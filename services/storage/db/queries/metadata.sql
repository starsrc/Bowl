-- name: GetInode :one
SELECT ino, generation, mode, size, nlink, uid, gid, rdev, atime, mtime, ctime, symlink_target
FROM inodes
WHERE ino = $1;

-- name: GetInodeByEntry :one
SELECT i.ino, i.generation, i.mode, i.size, i.nlink, i.uid, i.gid, i.rdev, i.atime, i.mtime, i.ctime, i.symlink_target
FROM entries e
JOIN inodes i ON e.ino = i.ino
WHERE e.parent_ino = $1 AND e.name = $2;

-- name: GetInodeForUpdate :one
SELECT ino, generation, mode, size, nlink, uid, gid, rdev, atime, mtime, ctime, symlink_target
FROM inodes
WHERE ino = $1
FOR UPDATE;

-- name: UpdateInodeAttr :one
UPDATE inodes
SET
  mode = $2,
  size = $3,
  uid = $4,
  gid = $5,
  atime = CASE WHEN @set_atime_now::boolean THEN clock_timestamp() ELSE @atime::timestamptz END,
  mtime = CASE WHEN @set_mtime_now::boolean THEN clock_timestamp() ELSE @mtime::timestamptz END,
  ctime = clock_timestamp()
WHERE ino = $1
RETURNING ino, generation, mode, size, nlink, uid, gid, rdev, atime, mtime, ctime, symlink_target;

-- name: CreateInode :one
INSERT INTO inodes (mode, nlink, uid, gid, rdev)
VALUES ($1, $2, $3, $4, $5)
RETURNING ino, generation, mode, size, nlink, uid, gid, rdev, atime, mtime, ctime, symlink_target;

-- name: CreateEntry :exec
INSERT INTO entries (parent_ino, name, ino)
VALUES ($1, $2, $3);

-- name: TouchInodeTimestamps :exec
UPDATE inodes
SET mtime = NOW(), ctime = NOW()
WHERE ino = $1;

-- name: IncrementInodeNlinkAndTouch :exec
UPDATE inodes
SET nlink = nlink + 1,
    mtime = NOW(),
    ctime = NOW()
WHERE ino = $1;

-- name: CreateSymlinkInode :one
INSERT INTO inodes (mode, size, nlink, uid, gid, rdev, symlink_target)
VALUES ($1, $2, $3, $4, $5, $6, $7)
RETURNING ino, generation, mode, size, nlink, uid, gid, rdev, atime, mtime, ctime, symlink_target;

-- name: GetSymlinkTarget :one
SELECT mode, symlink_target
FROM inodes
WHERE ino = $1;

-- name: IncrementInodeNlinkAndTouchCtime :one
UPDATE inodes
SET nlink = nlink + 1,
    ctime = NOW()
WHERE ino = $1
RETURNING ino, generation, mode, size, nlink, uid, gid, rdev, atime, mtime, ctime, symlink_target;

-- name: GetEntry :one
SELECT parent_ino, name, ino
FROM entries
WHERE parent_ino = $1 AND name = $2;

-- name: DeleteEntry :exec
DELETE FROM entries
WHERE parent_ino = $1 AND name = $2;

-- name: HasChildEntries :one
SELECT EXISTS(
    SELECT 1 FROM entries WHERE parent_ino = $1
);

-- name: DecrementInodeNlinkAndTouchCtime :one
UPDATE inodes
SET nlink = nlink - 1,
    ctime = NOW()
WHERE ino = $1
RETURNING ino, generation, mode, size, nlink, uid, gid, rdev, atime, mtime, ctime, symlink_target;

-- name: ClearInodeNlinkAndTouchCtime :one
UPDATE inodes
SET nlink = 0,
    ctime = NOW()
WHERE ino = $1
RETURNING ino, generation, mode, size, nlink, uid, gid, rdev, atime, mtime, ctime, symlink_target;

-- name: DecrementInodeNlinkAndTouch :exec
UPDATE inodes
SET nlink = nlink - 1,
    mtime = NOW(),
    ctime = NOW()
WHERE ino = $1;

-- name: UpdateEntry :exec
UPDATE entries
SET parent_ino = @new_parent_ino,
    name = @new_name
WHERE parent_ino = @old_parent_ino AND name = @old_name;

-- name: TouchInodeCtime :exec
UPDATE inodes
SET ctime = NOW()
WHERE ino = $1;

-- name: IsSubdirectory :one
WITH RECURSIVE ancestors AS (
    SELECT parent_ino, 1 AS depth
    FROM entries
    WHERE ino = @child_ino::bigint
    UNION ALL
    SELECT e.parent_ino, a.depth + 1
    FROM entries e
    JOIN ancestors a ON e.ino = a.parent_ino
    WHERE a.depth < 1024 AND a.parent_ino > 1
)
SELECT COALESCE((@child_ino::bigint = @ancestor_ino::bigint) OR EXISTS (
    SELECT 1 FROM ancestors WHERE parent_ino = @ancestor_ino::bigint
), false)::boolean AS is_sub;


