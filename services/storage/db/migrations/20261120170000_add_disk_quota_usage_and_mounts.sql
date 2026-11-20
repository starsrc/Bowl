-- +goose Up
-- +goose StatementBegin
ALTER TABLE disks
    ADD COLUMN max_bytes BIGINT NOT NULL DEFAULT 1073741824 CONSTRAINT chk_disks_max_bytes CHECK (max_bytes > 0),
    ADD COLUMN used_bytes BIGINT NOT NULL DEFAULT 0 CONSTRAINT chk_disks_used_bytes CHECK (used_bytes >= 0),
    ADD COLUMN used_inodes BIGINT NOT NULL DEFAULT 1 CONSTRAINT chk_disks_used_inodes CHECK (used_inodes >= 1);

CREATE TABLE disk_mounts (
    disk_id UUID NOT NULL REFERENCES disks(disk_id) ON DELETE CASCADE,
    mount_id UUID NOT NULL,
    read_only BOOLEAN NOT NULL,
    epoch BIGINT NOT NULL,
    expires_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (disk_id, mount_id),
    CONSTRAINT chk_disk_mounts_epoch CHECK (
        (read_only AND epoch = 0) OR (NOT read_only AND epoch > 0)
    )
);

CREATE INDEX idx_disk_mounts_disk_expires ON disk_mounts (disk_id, expires_at);
CREATE INDEX idx_disk_mounts_expires_at ON disk_mounts (expires_at);
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DROP INDEX IF EXISTS idx_disk_mounts_expires_at;
DROP INDEX IF EXISTS idx_disk_mounts_disk_expires;
DROP TABLE IF EXISTS disk_mounts;

ALTER TABLE disks
    DROP CONSTRAINT IF EXISTS chk_disks_used_inodes,
    DROP CONSTRAINT IF EXISTS chk_disks_used_bytes,
    DROP CONSTRAINT IF EXISTS chk_disks_max_bytes,
    DROP COLUMN IF EXISTS used_inodes,
    DROP COLUMN IF EXISTS used_bytes,
    DROP COLUMN IF EXISTS max_bytes;
-- +goose StatementEnd
