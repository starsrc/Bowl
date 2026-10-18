-- +goose Up
-- +goose StatementBegin
ALTER TABLE file_handles
    ADD COLUMN IF NOT EXISTS session_id BIGINT REFERENCES client_sessions(session_id) ON DELETE CASCADE;

CREATE INDEX IF NOT EXISTS idx_file_handles_session_id ON file_handles(session_id) WHERE session_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_inodes_orphan ON inodes(ino) WHERE nlink = 0;
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DROP INDEX IF EXISTS idx_inodes_orphan;
DROP INDEX IF EXISTS idx_file_handles_session_id;
ALTER TABLE file_handles DROP COLUMN IF EXISTS session_id;
-- +goose StatementEnd
