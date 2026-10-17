-- +goose Up
-- +goose StatementBegin
ALTER TABLE entries
    ALTER COLUMN name TYPE BYTEA USING convert_to(name, 'UTF8');

ALTER TABLE entries
    ADD CONSTRAINT chk_entries_name CHECK (
        octet_length(name) BETWEEN 1 AND 255
        AND position('\x00'::bytea IN name) = 0
        AND position('\x2f'::bytea IN name) = 0
    );
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
ALTER TABLE entries DROP CONSTRAINT IF EXISTS chk_entries_name;

ALTER TABLE entries
    ALTER COLUMN name TYPE VARCHAR(255) USING convert_from(name, 'UTF8');
-- +goose StatementEnd
