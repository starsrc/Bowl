-- +goose Up
-- +goose StatementBegin
ALTER TABLE inodes DROP CONSTRAINT IF EXISTS chk_inodes_symlink_target;

ALTER TABLE inodes
    ALTER COLUMN symlink_target TYPE BYTEA USING convert_to(symlink_target, 'UTF8');

-- 若存在 Down 阶段备份的原始 bytea 辅助表，从辅助表中 100% 字节级无损还原
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = '_rollback_symlink_target_backup') THEN
        UPDATE inodes i
        SET symlink_target = b.raw_target,
            size = octet_length(b.raw_target)
        FROM _rollback_symlink_target_backup b
        WHERE i.ino = b.ino;
        DROP TABLE _rollback_symlink_target_backup;
    END IF;
END $$;

-- 将旧版允许的 4096 字节符号链接目标截断收敛至 4095 字节（PATH_MAX - 1），避免存量合法数据导致迁移失败
UPDATE inodes
SET symlink_target = substring(symlink_target FROM 1 FOR 4095),
    size = LEAST(size, 4095)
WHERE (mode & 61440) = 40960
  AND symlink_target IS NOT NULL
  AND octet_length(symlink_target) > 4095;

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
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
CREATE TABLE IF NOT EXISTS _rollback_symlink_target_backup (
    ino BIGINT PRIMARY KEY,
    raw_target BYTEA NOT NULL
);

INSERT INTO _rollback_symlink_target_backup (ino, raw_target)
SELECT ino, symlink_target FROM inodes WHERE symlink_target IS NOT NULL
ON CONFLICT (ino) DO UPDATE SET raw_target = EXCLUDED.raw_target;

CREATE OR REPLACE FUNCTION pg_temp.encode_symlink_target_to_text(b bytea) RETURNS text LANGUAGE plpgsql IMMUTABLE AS $$
DECLARE
    res text;
BEGIN
    IF b IS NULL THEN
        RETURN NULL;
    END IF;
    BEGIN
        res := convert_from(b, 'UTF8');
        IF octet_length(res) <= 4096 AND length(res) <= 4096 THEN
            RETURN res;
        END IF;
    EXCEPTION WHEN character_not_in_repertoire THEN
        NULL;
    END;

    RETURN left(encode(b, 'escape'), 4096);
END;
$$;

ALTER TABLE inodes DROP CONSTRAINT IF EXISTS chk_inodes_symlink_target;

ALTER TABLE inodes
    ALTER COLUMN symlink_target TYPE TEXT USING pg_temp.encode_symlink_target_to_text(symlink_target);

ALTER TABLE inodes ADD CONSTRAINT chk_inodes_symlink_target CHECK (
    ((mode & 61440) = 40960 AND symlink_target IS NOT NULL AND length(symlink_target) BETWEEN 1 AND 4096 AND octet_length(symlink_target) BETWEEN 1 AND 4096)
    OR
    ((mode & 61440) <> 40960 AND symlink_target IS NULL)
);
-- +goose StatementEnd
