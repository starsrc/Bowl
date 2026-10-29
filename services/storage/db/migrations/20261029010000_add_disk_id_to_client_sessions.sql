-- +goose Up
-- +goose StatementBegin
ALTER TABLE client_sessions
    ADD COLUMN IF NOT EXISTS disk_id UUID REFERENCES disks(disk_id) ON DELETE CASCADE;

CREATE INDEX IF NOT EXISTS idx_client_sessions_disk_expires
    ON client_sessions (disk_id, expires_at);
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DROP INDEX IF EXISTS idx_client_sessions_disk_expires;
ALTER TABLE client_sessions DROP COLUMN IF EXISTS disk_id;
-- +goose StatementEnd
