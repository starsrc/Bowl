-- +goose Up
CREATE INDEX idx_disks_status_created_at ON disks (status, created_at DESC);
CREATE INDEX idx_disks_created_at ON disks (created_at DESC);

-- +goose Down
DROP INDEX IF EXISTS idx_disks_status_created_at;
DROP INDEX IF EXISTS idx_disks_created_at;
