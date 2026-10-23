-- +goose Up
-- +goose StatementBegin
-- 1. inodes 表新增 root_ino 列并回填根节点，建立索引
ALTER TABLE inodes ADD COLUMN IF NOT EXISTS root_ino BIGINT NOT NULL DEFAULT 0;
UPDATE inodes SET root_ino = 1 WHERE ino = 1 AND root_ino = 0;
CREATE INDEX IF NOT EXISTS idx_inodes_root_ino ON inodes(root_ino);

-- 2. entries 表外键 fk_entries_ino 调整为 ON DELETE CASCADE，支持按 root_ino 清理目录树时自动级联
ALTER TABLE entries DROP CONSTRAINT IF EXISTS fk_entries_ino;
ALTER TABLE entries ADD CONSTRAINT fk_entries_ino FOREIGN KEY (ino) REFERENCES inodes(ino) ON DELETE CASCADE;

-- 3. 创建或升级 disks 表
CREATE TABLE IF NOT EXISTS disks (
    disk_id UUID PRIMARY KEY,
    name VARCHAR(255) NOT NULL DEFAULT '',
    root_ino BIGINT NOT NULL UNIQUE,
    max_bytes BIGINT NOT NULL,
    used_bytes BIGINT NOT NULL DEFAULT 0,
    used_inodes BIGINT NOT NULL DEFAULT 1,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    CONSTRAINT chk_disks_root_ino CHECK (root_ino > 0),
    CONSTRAINT chk_disks_capacity CHECK (max_bytes > 0 AND used_bytes >= 0 AND used_bytes <= max_bytes AND used_inodes >= 0)
);

ALTER TABLE disks ADD COLUMN IF NOT EXISTS used_inodes BIGINT NOT NULL DEFAULT 1;
ALTER TABLE disks DROP CONSTRAINT IF EXISTS chk_disks_capacity;
ALTER TABLE disks ADD CONSTRAINT chk_disks_capacity CHECK (max_bytes > 0 AND used_bytes >= 0 AND used_bytes <= max_bytes AND used_inodes >= 0);
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DROP TABLE IF EXISTS disks;

ALTER TABLE entries DROP CONSTRAINT IF EXISTS fk_entries_ino;
ALTER TABLE entries ADD CONSTRAINT fk_entries_ino FOREIGN KEY (ino) REFERENCES inodes(ino) ON DELETE RESTRICT;

DROP INDEX IF EXISTS idx_inodes_root_ino;
ALTER TABLE inodes DROP COLUMN IF EXISTS root_ino;
-- +goose StatementEnd
