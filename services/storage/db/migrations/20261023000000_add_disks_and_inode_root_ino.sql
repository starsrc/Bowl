-- +goose Up
-- +goose StatementBegin
-- 1. inodes 表新增 root_ino 列并建立索引
ALTER TABLE inodes ADD COLUMN IF NOT EXISTS root_ino BIGINT NOT NULL DEFAULT 0;
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

-- 4. 递归回填存量 inodes.root_ino（从磁盘根与自引用根向下递归覆盖整棵树，遇子盘根截断）
WITH RECURSIVE disk_trees AS (
    SELECT i.ino, i.ino AS root_ino
    FROM inodes i
    WHERE i.ino = 1
       OR i.parent_ino = i.ino
       OR EXISTS (SELECT 1 FROM disks d WHERE d.root_ino = i.ino)
    UNION
    SELECT c.ino, dt.root_ino
    FROM inodes c
    JOIN entries e ON e.ino = c.ino
    JOIN disk_trees dt ON e.parent_ino = dt.ino
    WHERE c.ino <> dt.ino
      AND c.ino <> 1
      AND c.parent_ino <> c.ino
      AND NOT EXISTS (SELECT 1 FROM disks d WHERE d.root_ino = c.ino)
),
unambiguous_trees AS (
    SELECT ino, MIN(root_ino) AS root_ino
    FROM disk_trees
    GROUP BY ino
    HAVING COUNT(DISTINCT root_ino) = 1
)
UPDATE inodes i
SET root_ino = ut.root_ino
FROM unambiguous_trees ut
WHERE i.ino = ut.ino AND i.root_ino = 0;

-- 若存在根据 parent_ino 链可确定父目录归属的多层孤儿目录节点，递归继承父节点 root_ino
WITH RECURSIVE orphan_dir_trees AS (
    SELECT i.ino, p.root_ino
    FROM inodes i
    JOIN inodes p ON p.ino = i.parent_ino
    WHERE i.root_ino = 0 AND i.parent_ino > 0 AND p.root_ino > 0
    UNION
    SELECT c.ino, odt.root_ino
    FROM inodes c
    JOIN orphan_dir_trees odt ON c.parent_ino = odt.ino
    WHERE c.root_ino = 0 AND c.parent_ino > 0 AND c.ino <> odt.ino
)
UPDATE inodes i
SET root_ino = odt.root_ino
FROM orphan_dir_trees odt
WHERE i.ino = odt.ino AND i.root_ino = 0;

-- 仅当全局仅存在唯一根（单盘/单根环境）时，将剩余无 entries 的存量孤儿节点安全归入该唯一根
WITH candidate_roots AS (
    SELECT root_ino FROM disks
    UNION
    SELECT root_ino FROM inodes WHERE root_ino > 0
)
UPDATE inodes
SET root_ino = (SELECT root_ino FROM candidate_roots)
WHERE root_ino = 0
  AND (SELECT COUNT(*) FROM candidate_roots) = 1;

-- 完整性校验：若仍存在 root_ino = 0 的节点（含跨盘硬链接冲突或多盘下归属不明的孤儿），阻止迁移
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM inodes WHERE root_ino = 0) THEN
        RAISE EXCEPTION 'migration blocked: found inodes with ambiguous or unresolvable root_ino ownership';
    END IF;
END $$;

-- 5. 根据回填后的 inodes 同步初始化存量 disks 的 used_inodes
UPDATE disks d
SET used_inodes = GREATEST(sub.cnt, 1)
FROM (
    SELECT root_ino, COUNT(*)::bigint AS cnt
    FROM inodes
    WHERE root_ino > 0
    GROUP BY root_ino
) sub
WHERE d.root_ino = sub.root_ino;
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
-- 1. 保留 disks 表与存量数据，仅还原本次新增的 used_inodes 列与容量约束
ALTER TABLE disks DROP CONSTRAINT IF EXISTS chk_disks_capacity;
ALTER TABLE disks DROP COLUMN IF EXISTS used_inodes;
ALTER TABLE disks ADD CONSTRAINT chk_disks_capacity CHECK (max_bytes > 0 AND used_bytes >= 0 AND used_bytes <= max_bytes);

-- 2. 还原 entries 外键约束为 RESTRICT
ALTER TABLE entries DROP CONSTRAINT IF EXISTS fk_entries_ino;
ALTER TABLE entries ADD CONSTRAINT fk_entries_ino FOREIGN KEY (ino) REFERENCES inodes(ino) ON DELETE RESTRICT;

-- 3. 删除 idx_inodes_root_ino 索引与 inodes.root_ino 列
DROP INDEX IF EXISTS idx_inodes_root_ino;
ALTER TABLE inodes DROP COLUMN IF EXISTS root_ino;
-- +goose StatementEnd
