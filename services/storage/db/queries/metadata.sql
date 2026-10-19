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
SET mtime = clock_timestamp(), ctime = clock_timestamp()
WHERE ino = $1;

-- name: IncrementInodeNlinkAndTouch :exec
UPDATE inodes
SET nlink = nlink + 1,
    mtime = clock_timestamp(),
    ctime = clock_timestamp()
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
    ctime = clock_timestamp()
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
    ctime = clock_timestamp()
WHERE ino = $1
RETURNING ino, generation, mode, size, nlink, uid, gid, rdev, atime, mtime, ctime, symlink_target;

-- name: ClearInodeNlinkAndTouchCtime :one
UPDATE inodes
SET nlink = 0,
    ctime = clock_timestamp()
WHERE ino = $1
RETURNING ino, generation, mode, size, nlink, uid, gid, rdev, atime, mtime, ctime, symlink_target;

-- name: DecrementInodeNlinkAndTouch :exec
UPDATE inodes
SET nlink = nlink - 1,
    mtime = clock_timestamp(),
    ctime = clock_timestamp()
WHERE ino = $1;

-- name: UpdateEntry :exec
UPDATE entries
SET parent_ino = @new_parent_ino,
    name = @new_name
WHERE parent_ino = @old_parent_ino AND name = @old_name;

-- name: UpdateEntryIno :exec
UPDATE entries
SET ino = $3
WHERE parent_ino = $1 AND name = $2;

-- name: TouchInodeCtime :exec
UPDATE inodes
SET ctime = clock_timestamp()
WHERE ino = $1;

-- name: IsSubdirectory :one
WITH RECURSIVE ancestors AS (
    SELECT
        parent_ino,
        ARRAY[@child_ino::bigint, parent_ino] AS visited,
        (parent_ino = @child_ino::bigint) AS has_cycle
    FROM entries
    WHERE ino = @child_ino::bigint
    UNION ALL
    SELECT
        e.parent_ino,
        a.visited || e.parent_ino,
        (e.parent_ino = ANY(a.visited)) AS has_cycle
    FROM entries e
    JOIN ancestors a ON e.ino = a.parent_ino
    WHERE NOT a.has_cycle
)
SELECT COALESCE((@child_ino::bigint = @ancestor_ino::bigint) OR EXISTS (
    SELECT 1 FROM ancestors WHERE parent_ino = @ancestor_ino::bigint OR has_cycle
), false)::boolean AS is_sub;

-- name: CreateFileHandle :one
INSERT INTO file_handles (ino, flags, session_id)
VALUES ($1, $2, $3)
RETURNING *;

-- name: HasFileHandlesByIno :one
SELECT EXISTS (
    SELECT 1
    FROM file_handles fh
    LEFT JOIN client_sessions s ON fh.session_id = s.session_id
    WHERE fh.ino = $1
      AND (fh.session_id IS NULL OR s.expires_at > clock_timestamp())
);

-- name: GetFileHandle :one
SELECT fh.*
FROM file_handles fh
LEFT JOIN client_sessions s ON fh.session_id = s.session_id
WHERE fh.fh = $1
  AND fh.ino = $2
  AND fh.session_id IS NOT DISTINCT FROM sqlc.narg('session_id')
  AND (sqlc.narg('session_id') IS NULL OR s.expires_at > clock_timestamp());

-- name: HasLinkableTmpFileHandleByIno :one
-- 4194304 = 0o20000000 (O_TMPFILE), 128 = 0o200 (O_EXCL)
SELECT EXISTS (
    SELECT 1
    FROM file_handles fh
    LEFT JOIN client_sessions s ON fh.session_id = s.session_id
    WHERE fh.ino = $1
      AND (fh.session_id IS NULL OR s.expires_at > clock_timestamp())
      AND (fh.flags & 4194304) <> 0 -- 4194304 = 0o20000000 (O_TMPFILE)
      AND (fh.flags & 128) = 0      -- 128 = 0o200 (O_EXCL)
);

-- name: ClearTmpFileFlagByIno :exec
-- 4194304 = 0o20000000 (O_TMPFILE)
UPDATE file_handles
SET flags = flags & ~4194304 -- 4194304 = 0o20000000 (O_TMPFILE)
WHERE ino = $1;

-- name: DeleteFileHandle :one
DELETE FROM file_handles fh
WHERE fh.fh = $1
  AND fh.ino = $2
  AND fh.session_id IS NOT DISTINCT FROM sqlc.narg('session_id')
  AND (
      sqlc.narg('session_id') IS NULL
      OR EXISTS (
          SELECT 1
          FROM client_sessions s
          WHERE s.session_id = fh.session_id
              AND s.expires_at > clock_timestamp()
      )
  )
RETURNING fh.fh;

-- name: DeleteInode :exec
DELETE FROM inodes
WHERE ino = $1;

-- name: GetParentInode :one
SELECT i.*
FROM entries e
JOIN inodes i ON e.parent_ino = i.ino
WHERE e.ino = $1
LIMIT 1;

-- name: ListDirEntries :many
SELECT
    e.name,
    i.ino,
    i.generation,
    i.mode,
    i.size,
    i.nlink,
    i.uid,
    i.gid,
    i.rdev,
    i.atime,
    i.mtime,
    i.ctime,
    i.symlink_target
FROM entries e
JOIN inodes i ON e.ino = i.ino
WHERE e.parent_ino = $1
ORDER BY e.name ASC
LIMIT $2 OFFSET sqlc.arg('offset')::bigint;

-- name: GetInodeXattr :one
SELECT ino, name, value
FROM inode_xattrs
WHERE ino = $1 AND name = $2;

-- name: UpsertInodeXattr :exec
INSERT INTO inode_xattrs (ino, name, value)
VALUES ($1, $2, $3)
ON CONFLICT (ino, name) DO UPDATE
SET value = EXCLUDED.value;

-- name: GetInodeXattrNamesTotalBytes :one
SELECT COALESCE(SUM(octet_length(name) + 1), 0)::bigint AS total_bytes
FROM inode_xattrs
WHERE ino = $1;

-- name: ListInodeXattrNames :many
SELECT name
FROM inode_xattrs
WHERE ino = $1
ORDER BY name ASC;

-- name: DeleteInodeXattr :execrows
DELETE FROM inode_xattrs
WHERE ino = $1 AND name = $2;

-- name: DeleteExpiredClientSessions :exec
DELETE FROM client_sessions
WHERE expires_at <= clock_timestamp();

-- name: UpsertClientSession :one
INSERT INTO client_sessions (session_id, expires_at, updated_at)
VALUES ($1, $2, clock_timestamp())
ON CONFLICT (session_id) DO UPDATE
SET expires_at = EXCLUDED.expires_at,
    updated_at = clock_timestamp()
RETURNING session_id, expires_at, updated_at;

-- name: DeleteUnreferencedOrphanInodes :exec
DELETE FROM inodes
WHERE ino IN (
    SELECT i.ino
    FROM inodes i
    WHERE i.nlink = 0
      AND NOT EXISTS (
          SELECT 1
          FROM file_handles fh
          LEFT JOIN client_sessions s ON fh.session_id = s.session_id
          WHERE fh.ino = i.ino
            AND (fh.session_id IS NULL OR s.expires_at > clock_timestamp())
      )
    FOR UPDATE SKIP LOCKED
);

-- name: GetFirstConflictingFileLock :one
SELECT l.id, l.session_id, l.ino, l.fh, l.lock_owner, l.lk_flags, l.start_offset, l.end_offset, l.lock_type, l.pid, l.created_at
FROM file_locks l
JOIN client_sessions s ON l.session_id = s.session_id
WHERE l.ino = @ino
  AND l.lk_flags = @lk_flags
  AND s.expires_at > clock_timestamp()
  AND l.start_offset <= @end_offset
  AND l.end_offset >= @start_offset
  AND (@req_type::smallint = 1 OR l.lock_type = 1)
  AND (
    (@lk_flags = 0 AND NOT (l.session_id = @session_id AND l.lock_owner = @lock_owner))
    OR
    (@lk_flags = 1 AND NOT (l.session_id = @session_id AND l.fh = @fh))
  )
ORDER BY l.start_offset ASC, l.end_offset ASC, l.id ASC
LIMIT 1;

-- name: ListActiveFileLocksByInoAndFlags :many
SELECT l.id, l.session_id, l.ino, l.fh, l.lock_owner, l.lk_flags, l.start_offset, l.end_offset, l.lock_type, l.pid, l.created_at
FROM file_locks l
JOIN client_sessions s ON l.session_id = s.session_id
WHERE l.ino = @ino
  AND l.lk_flags = @lk_flags
  AND s.expires_at > clock_timestamp()
ORDER BY l.start_offset ASC, l.end_offset ASC, l.id ASC;

-- name: CreateFileLock :one
INSERT INTO file_locks (
    session_id, ino, fh, lock_owner, lk_flags, start_offset, end_offset, lock_type, pid
)
VALUES (
    @session_id, @ino, @fh, @lock_owner, @lk_flags, @start_offset, @end_offset, @lock_type, @pid
)
RETURNING id, session_id, ino, fh, lock_owner, lk_flags, start_offset, end_offset, lock_type, pid, created_at;

-- name: DeleteFileLocksByIDs :exec
DELETE FROM file_locks
WHERE id = ANY(@ids::bigint[]);

-- name: DeletePosixFileLocksBySessionAndOwner :execrows
DELETE FROM file_locks
WHERE session_id = @session_id
  AND ino = @ino
  AND lock_owner = @lock_owner
  AND lk_flags = 0;

-- name: DeletePosixFileLocksByOwner :execrows
DELETE FROM file_locks
WHERE ino = @ino
  AND lock_owner = @lock_owner
  AND lk_flags = 0;

-- name: DeleteFlockFileLocksBySessionAndFh :execrows
DELETE FROM file_locks
WHERE session_id = @session_id
  AND ino = @ino
  AND fh = @fh
  AND lk_flags = 1;

-- name: DeleteFlockFileLocksByFh :execrows
DELETE FROM file_locks
WHERE ino = @ino
  AND fh = @fh
  AND lk_flags = 1;

-- name: GetFileLockWaiter :one
SELECT session_id, request_id, ino, fh, lock_owner, lk_flags, start_offset, end_offset, lock_type, pid, status, created_at
FROM file_lock_waiters
WHERE session_id = @session_id AND request_id = @request_id;

-- name: ListPendingFileLockWaitersByInoAndFlags :many
SELECT w.session_id, w.request_id, w.ino, w.fh, w.lock_owner, w.lk_flags, w.start_offset, w.end_offset, w.lock_type, w.pid, w.status, w.created_at
FROM file_lock_waiters w
JOIN client_sessions s ON w.session_id = s.session_id
WHERE w.ino = @ino
  AND w.lk_flags = @lk_flags
  AND w.status = 1
  AND s.expires_at > clock_timestamp()
ORDER BY w.created_at ASC, w.session_id ASC, w.request_id ASC;

-- name: UpsertFileLockWaiter :one
INSERT INTO file_lock_waiters (
    session_id, request_id, ino, fh, lock_owner, lk_flags, start_offset, end_offset, lock_type, pid, status
)
VALUES (
    @session_id, @request_id, @ino, @fh, @lock_owner, @lk_flags, @start_offset, @end_offset, @lock_type, @pid, @status
)
ON CONFLICT (session_id, request_id) DO UPDATE
SET start_offset = EXCLUDED.start_offset,
    end_offset = EXCLUDED.end_offset,
    lock_type = EXCLUDED.lock_type,
    pid = EXCLUDED.pid,
    status = EXCLUDED.status
RETURNING session_id, request_id, ino, fh, lock_owner, lk_flags, start_offset, end_offset, lock_type, pid, status, created_at;

-- name: DeleteFileLockWaiter :execrows
DELETE FROM file_lock_waiters
WHERE session_id = @session_id AND request_id = @request_id;
