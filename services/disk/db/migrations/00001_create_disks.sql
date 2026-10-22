-- +goose Up
-- +goose StatementBegin
CREATE TABLE IF NOT EXISTS disks (
    disk_id UUID PRIMARY KEY,
    name VARCHAR(255) NOT NULL DEFAULT '',
    root_ino BIGINT NOT NULL UNIQUE,
    max_bytes BIGINT NOT NULL,
    used_bytes BIGINT NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    CONSTRAINT chk_disks_root_ino CHECK (root_ino > 0),
    CONSTRAINT chk_disks_capacity CHECK (max_bytes > 0 AND used_bytes >= 0 AND used_bytes <= max_bytes)
);
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DROP TABLE IF EXISTS disks;
-- +goose StatementEnd
