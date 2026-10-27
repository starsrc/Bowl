-- +goose Up
-- +goose StatementBegin
ALTER TABLE entries
    ALTER COLUMN name TYPE BYTEA USING convert_to(name, 'UTF8');

DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.tables
        WHERE table_schema = current_schema() AND table_name = '_rollback_entries_name_backup'
    ) THEN
        IF EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_schema = current_schema()
              AND table_name = '_rollback_entries_name_backup'
              AND column_name = 'ino'
        ) THEN
            EXECUTE 'UPDATE entries e
                     SET name = b.raw_name
                     FROM _rollback_entries_name_backup b
                     WHERE e.parent_ino = b.parent_ino
                       AND e.name = convert_to(b.rollback_name, ''UTF8'')
                       AND e.ino = b.ino';
        ELSE
            EXECUTE 'UPDATE entries e
                     SET name = b.raw_name
                     FROM _rollback_entries_name_backup b
                     WHERE e.parent_ino = b.parent_ino
                       AND e.name = convert_to(b.rollback_name, ''UTF8'')';
        END IF;
        DROP TABLE _rollback_entries_name_backup;
    END IF;
END $$;

ALTER TABLE entries
    ADD CONSTRAINT chk_entries_name CHECK (
        octet_length(name) BETWEEN 1 AND 255
        AND position('\x00'::bytea IN name) = 0
        AND position('\x2f'::bytea IN name) = 0
    );
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
CREATE TABLE IF NOT EXISTS _rollback_entries_name_backup (
    parent_ino BIGINT NOT NULL,
    rollback_name VARCHAR(255) NOT NULL,
    ino BIGINT NOT NULL,
    raw_name BYTEA NOT NULL,
    PRIMARY KEY (parent_ino, rollback_name)
);
ALTER TABLE _rollback_entries_name_backup ADD COLUMN IF NOT EXISTS ino BIGINT;

CREATE OR REPLACE FUNCTION pg_temp.encode_entry_name_to_varchar(b bytea) RETURNS varchar(255) LANGUAGE plpgsql IMMUTABLE AS $$
DECLARE
    s text;
BEGIN
    IF b IS NULL THEN
        RETURN NULL;
    END IF;

    BEGIN
        s := convert_from(b, 'UTF8');
        IF length(s) <= 255 AND octet_length(b) <= 255 AND left(s, 10) <> '__bowl_rb_' THEN
            RETURN s::varchar(255);
        END IF;
    EXCEPTION WHEN others THEN
        NULL;
    END;

    RETURN ('__bowl_rb_' || encode(sha256(b), 'hex'))::varchar(255);
END;
$$;

INSERT INTO _rollback_entries_name_backup (parent_ino, rollback_name, ino, raw_name)
SELECT parent_ino, pg_temp.encode_entry_name_to_varchar(name), ino, name
FROM entries
WHERE convert_to(pg_temp.encode_entry_name_to_varchar(name), 'UTF8') <> name
ON CONFLICT (parent_ino, rollback_name) DO UPDATE SET ino = EXCLUDED.ino, raw_name = EXCLUDED.raw_name;

ALTER TABLE entries DROP CONSTRAINT IF EXISTS chk_entries_name;

ALTER TABLE entries
    ALTER COLUMN name TYPE VARCHAR(255) USING pg_temp.encode_entry_name_to_varchar(name);
-- +goose StatementEnd
