-- +goose Up
-- +goose StatementBegin
CREATE SEQUENCE IF NOT EXISTS file_handle_id_seq START WITH 1;
-- +goose StatementEnd

CREATE TABLE IF NOT EXISTS file_handles (
    fh BIGINT PRIMARY KEY DEFAULT nextval('file_handle_id_seq'),
    ino BIGINT NOT NULL REFERENCES inodes(ino) ON DELETE CASCADE,
    flags INTEGER NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_file_handles_ino ON file_handles(ino);

-- +goose Down
DROP INDEX IF EXISTS idx_file_handles_ino;
DROP TABLE IF EXISTS file_handles;
DROP SEQUENCE IF EXISTS file_handle_id_seq;
