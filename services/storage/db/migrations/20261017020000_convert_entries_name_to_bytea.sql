-- +goose Up
-- +goose StatementBegin
CREATE OR REPLACE FUNCTION pg_temp.decode_entry_name_to_bytea(t text) RETURNS bytea LANGUAGE plpgsql IMMUTABLE AS $$
DECLARE
    res bytea := ''::bytea;
    i int;
    ch text;
    ch_code int;
BEGIN
    IF t IS NULL THEN
        RETURN NULL;
    END IF;
    IF t !~ '[\uE000-\uE0FF]' THEN
        RETURN convert_to(t, 'UTF8');
    END IF;

    FOR i IN 1 .. char_length(t) LOOP
        ch := substr(t, i, 1);
        ch_code := ascii(ch);
        IF ch_code BETWEEN 57344 AND 57599 THEN
            res := res || set_byte('\x00'::bytea, 0, ch_code - 57344);
        ELSE
            res := res || convert_to(ch, 'UTF8');
        END IF;
    END LOOP;
    RETURN res;
END;
$$;

ALTER TABLE entries
    ALTER COLUMN name TYPE BYTEA USING pg_temp.decode_entry_name_to_bytea(name);

ALTER TABLE entries
    ADD CONSTRAINT chk_entries_name CHECK (
        octet_length(name) BETWEEN 1 AND 255
        AND position('\x00'::bytea IN name) = 0
        AND position('\x2f'::bytea IN name) = 0
    );
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
CREATE OR REPLACE FUNCTION pg_temp.encode_entry_name_to_varchar(b bytea) RETURNS varchar(255) LANGUAGE plpgsql IMMUTABLE AS $$
DECLARE
    res text;
    i int;
    byte_val int;
BEGIN
    IF b IS NULL THEN
        RETURN NULL;
    END IF;
    BEGIN
        res := convert_from(b, 'UTF8');
        IF res !~ '[\uE000-\uE0FF]' THEN
            RETURN res::varchar(255);
        END IF;
    EXCEPTION WHEN character_not_in_repertoire THEN
        NULL;
    END;

    res := '';
    FOR i IN 0 .. octet_length(b) - 1 LOOP
        byte_val := get_byte(b, i);
        IF byte_val BETWEEN 1 AND 127 THEN
            res := res || chr(byte_val);
        ELSE
            res := res || chr(57344 + byte_val);
        END IF;
    END LOOP;
    RETURN res::varchar(255);
END;
$$;

ALTER TABLE entries DROP CONSTRAINT IF EXISTS chk_entries_name;

ALTER TABLE entries
    ALTER COLUMN name TYPE VARCHAR(255) USING pg_temp.encode_entry_name_to_varchar(name);
-- +goose StatementEnd
