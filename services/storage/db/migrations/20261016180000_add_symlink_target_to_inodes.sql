-- +goose Up
ALTER TABLE inodes ADD COLUMN symlink_target TEXT;
ALTER TABLE inodes ADD CONSTRAINT chk_inodes_symlink_target CHECK (
    ((mode & 61440) = 40960 AND symlink_target IS NOT NULL AND length(symlink_target) BETWEEN 1 AND 4096 AND octet_length(symlink_target) BETWEEN 1 AND 4096)
    OR
    ((mode & 61440) <> 40960 AND symlink_target IS NULL)
);

-- +goose Down
ALTER TABLE inodes DROP CONSTRAINT IF EXISTS chk_inodes_symlink_target;
ALTER TABLE inodes DROP COLUMN IF EXISTS symlink_target;
