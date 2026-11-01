-- +goose Up
-- +goose StatementBegin
-- 1. 移除 disks 表的 used_bytes 与 used_inodes 冗余列，重建 chk_disks_capacity 约束
ALTER TABLE disks DROP CONSTRAINT IF EXISTS chk_disks_capacity;
ALTER TABLE disks DROP COLUMN IF EXISTS used_bytes, DROP COLUMN IF EXISTS used_inodes;
ALTER TABLE disks ADD CONSTRAINT chk_disks_capacity CHECK (max_bytes > 0);

-- 2. 建立包含 (mode, size) 的覆盖索引，并移除原单列索引
CREATE INDEX IF NOT EXISTS idx_inodes_root_ino_quota ON inodes (root_ino) INCLUDE (mode, size);
DROP INDEX IF EXISTS idx_inodes_root_ino;
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
-- 1. 恢复 idx_inodes_root_ino 单列索引并移除覆盖索引
CREATE INDEX IF NOT EXISTS idx_inodes_root_ino ON inodes (root_ino);
DROP INDEX IF EXISTS idx_inodes_root_ino_quota;

-- 2. 恢复 disks.used_bytes 与 disks.used_inodes 列并从 inodes 表回填统计值
ALTER TABLE disks DROP CONSTRAINT IF EXISTS chk_disks_capacity;
ALTER TABLE disks
    ADD COLUMN IF NOT EXISTS used_bytes BIGINT NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS used_inodes BIGINT NOT NULL DEFAULT 1;

UPDATE disks d
SET
    used_bytes = COALESCE((
        SELECT SUM(CASE WHEN (i.mode & 61440) = 32768 THEN i.size ELSE 0 END)::bigint
        FROM inodes i
        WHERE i.root_ino = d.root_ino
    ), 0),
    used_inodes = GREATEST(COALESCE((
        SELECT COUNT(*)::bigint
        FROM inodes i
        WHERE i.root_ino = d.root_ino
    ), 1), 1);

ALTER TABLE disks ADD CONSTRAINT chk_disks_capacity CHECK (max_bytes > 0 AND used_bytes >= 0 AND used_inodes >= 1);
-- +goose StatementEnd
