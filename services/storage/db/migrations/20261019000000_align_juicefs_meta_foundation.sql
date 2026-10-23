-- +goose Up
-- +goose StatementBegin
-- 1. inodes 表新增 flags 与 parent_ino 列，并回填存量目录的 parent_ino（非目录节点保持为 0）
ALTER TABLE inodes ADD COLUMN IF NOT EXISTS flags INTEGER NOT NULL DEFAULT 0;
ALTER TABLE inodes ADD COLUMN IF NOT EXISTS parent_ino BIGINT NOT NULL DEFAULT 0;

UPDATE inodes SET parent_ino = 1 WHERE ino = 1;

UPDATE inodes i
SET parent_ino = e.parent_ino
FROM entries e
WHERE e.ino = i.ino AND (i.mode & 61440) = 16384;

-- 2. inodes 表时间戳列默认值调整为 clock_timestamp()
ALTER TABLE inodes ALTER COLUMN atime SET DEFAULT clock_timestamp();
ALTER TABLE inodes ALTER COLUMN mtime SET DEFAULT clock_timestamp();
ALTER TABLE inodes ALTER COLUMN ctime SET DEFAULT clock_timestamp();

-- 3. 放宽 chk_inodes_symlink_target 约束以兼容 mknod 创建 S_IFLNK
ALTER TABLE inodes DROP CONSTRAINT IF EXISTS chk_inodes_symlink_target;
ALTER TABLE inodes ADD CONSTRAINT chk_inodes_symlink_target CHECK (
    CASE
        WHEN (mode & 61440) = 40960 THEN
            symlink_target IS NULL
            OR (
                octet_length(symlink_target) BETWEEN 1 AND 4095
                AND position('\x00'::bytea IN symlink_target) = 0
            )
        ELSE
            symlink_target IS NULL
    END
);

-- 4. 清理存量不合规数据并添加 chk_inodes_rdev 与 chk_inodes_size 约束
UPDATE inodes SET rdev = 0 WHERE (mode & 61440) NOT IN (8192, 24576) AND rdev <> 0;
UPDATE inodes SET size = 0 WHERE (mode & 61440) IN (4096, 8192, 24576, 49152) AND size <> 0;
UPDATE inodes SET size = 4096 WHERE (mode & 61440) = 16384 AND size = 0;

ALTER TABLE inodes ADD CONSTRAINT chk_inodes_rdev CHECK (
    ((mode & 61440) IN (8192, 24576)) OR rdev = 0
);
ALTER TABLE inodes ADD CONSTRAINT chk_inodes_size CHECK (
    size >= 0 AND (((mode & 61440) NOT IN (4096, 8192, 24576, 49152)) OR size = 0)
);

-- 5. file_handles 表 created_at 默认值调整为 clock_timestamp()
ALTER TABLE file_handles ALTER COLUMN created_at SET DEFAULT clock_timestamp();

-- 6. client_sessions 表新增 created_at 列
ALTER TABLE client_sessions ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp();
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
ALTER TABLE client_sessions DROP COLUMN IF EXISTS created_at;

ALTER TABLE file_handles ALTER COLUMN created_at SET DEFAULT NOW();

ALTER TABLE inodes DROP CONSTRAINT IF EXISTS chk_inodes_size;
ALTER TABLE inodes DROP CONSTRAINT IF EXISTS chk_inodes_rdev;

ALTER TABLE inodes DROP CONSTRAINT IF EXISTS chk_inodes_symlink_target;
ALTER TABLE inodes ADD CONSTRAINT chk_inodes_symlink_target CHECK (
    CASE
        WHEN (mode & 61440) = 40960 THEN
            symlink_target IS NOT NULL
            AND octet_length(symlink_target) BETWEEN 1 AND 4095
            AND position('\x00'::bytea IN symlink_target) = 0
        ELSE
            symlink_target IS NULL
    END
);

ALTER TABLE inodes ALTER COLUMN atime SET DEFAULT NOW();
ALTER TABLE inodes ALTER COLUMN mtime SET DEFAULT NOW();
ALTER TABLE inodes ALTER COLUMN ctime SET DEFAULT NOW();
ALTER TABLE inodes DROP COLUMN IF EXISTS parent_ino;
ALTER TABLE inodes DROP COLUMN IF EXISTS flags;
-- +goose StatementEnd
