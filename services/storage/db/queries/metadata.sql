-- name: GetInode :one
SELECT ino, generation, mode, size, nlink, uid, gid, rdev, atime, mtime, ctime
FROM inodes
WHERE ino = $1;

-- name: GetInodeByEntry :one
SELECT i.ino, i.generation, i.mode, i.size, i.nlink, i.uid, i.gid, i.rdev, i.atime, i.mtime, i.ctime
FROM entries e
JOIN inodes i ON e.ino = i.ino
WHERE e.parent_ino = $1 AND e.name = $2;

-- name: GetInodeForUpdate :one
SELECT ino, generation, mode, size, nlink, uid, gid, rdev, atime, mtime, ctime
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
  atime = CASE WHEN @set_atime_now::boolean THEN NOW() ELSE @atime::timestamptz END,
  mtime = CASE WHEN @set_mtime_now::boolean THEN NOW() ELSE @mtime::timestamptz END,
  ctime = NOW()
WHERE ino = $1
RETURNING ino, generation, mode, size, nlink, uid, gid, rdev, atime, mtime, ctime;

