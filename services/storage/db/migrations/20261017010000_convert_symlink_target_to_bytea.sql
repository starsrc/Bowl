-- +goose Up
ALTER TABLE inodes DROP CONSTRAINT IF EXISTS chk_inodes_symlink_target;
ALTER TABLE inodes ALTER COLUMN symlink_target TYPE BYTEA USING convert_to(symlink_target, 'UTF8');
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

-- +goose Down
ALTER TABLE inodes DROP CONSTRAINT IF EXISTS chk_inodes_symlink_target;
ALTER TABLE inodes ALTER COLUMN symlink_target TYPE TEXT USING convert_from(symlink_target, 'UTF8');
ALTER TABLE inodes ADD CONSTRAINT chk_inodes_symlink_target CHECK (
    ((mode & 61440) = 40960 AND symlink_target IS NOT NULL AND length(symlink_target) BETWEEN 1 AND 4096 AND octet_length(symlink_target) BETWEEN 1 AND 4096)
    OR
    ((mode & 61440) <> 40960 AND symlink_target IS NULL)
);
