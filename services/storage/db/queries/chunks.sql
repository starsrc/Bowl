-- name: NextSliceID :one
SELECT nextval('slice_id_seq')::bigint;

-- name: GetChunk :one
SELECT ino, indx, slices
FROM chunks
WHERE ino = @ino::bigint AND indx = @indx::bigint;

-- name: GetChunkForUpdate :one
SELECT ino, indx, slices
FROM chunks
WHERE ino = @ino::bigint AND indx = @indx::bigint
FOR UPDATE;

-- name: GetChunksInRange :many
SELECT ino, indx, slices
FROM chunks
WHERE ino = @ino::bigint AND indx >= @start_indx::bigint AND indx <= @end_indx::bigint
ORDER BY indx ASC;

-- name: AppendChunkSlice :one
INSERT INTO chunks (ino, indx, slices)
VALUES (@ino::bigint, @indx::bigint, @slices::bytea)
ON CONFLICT (ino, indx)
DO UPDATE SET slices = chunks.slices || EXCLUDED.slices
RETURNING octet_length(slices)::bigint;

-- name: AppendExistingChunkSlice :one
WITH updated AS (
    UPDATE chunks
    SET slices = slices || @slices::bytea
    WHERE ino = @ino::bigint AND indx = @indx::bigint
    RETURNING octet_length(slices)::bigint AS octet_len
)
SELECT COALESCE((SELECT octet_len FROM updated), 0)::bigint AS octet_len;

-- name: UpdateChunkSlices :exec
UPDATE chunks
SET slices = @slices::bytea
WHERE ino = @ino::bigint AND indx = @indx::bigint;

-- name: DeleteChunk :many
DELETE FROM chunks
WHERE ino = @ino::bigint AND indx = @indx::bigint
RETURNING slices;

-- name: DeleteChunksByIno :many
DELETE FROM chunks
WHERE ino = @ino::bigint
RETURNING slices;

-- name: DeleteChunksByInos :many
DELETE FROM chunks
WHERE ino = ANY(@inos::bigint[])
RETURNING slices;

-- name: DeleteChunksFromIndx :many
DELETE FROM chunks
WHERE ino = @ino::bigint AND indx >= @from_indx::bigint
RETURNING slices;

-- name: DeleteChunksAfterIndx :many
DELETE FROM chunks
WHERE ino = @ino::bigint AND indx > @after_indx::bigint
RETURNING slices;

-- name: InsertSliceRef :exec
INSERT INTO slice_refs (slice_id, size, refs)
VALUES (@slice_id::bigint, @size::integer, @refs::integer);

-- name: AddSliceRefCount :exec
UPDATE slice_refs
SET refs = refs + @delta::integer
WHERE slice_id = @slice_id::bigint;

-- name: GetSliceRef :one
SELECT slice_id, size, refs, created_at
FROM slice_refs
WHERE slice_id = @slice_id::bigint;

-- name: ListGarbageSliceRefs :many
SELECT slice_id, size, refs, created_at
FROM slice_refs
WHERE refs <= 0
ORDER BY slice_id ASC
LIMIT @limit_count::integer
FOR UPDATE SKIP LOCKED;

-- name: DeleteSliceRef :exec
DELETE FROM slice_refs
WHERE slice_id = @slice_id::bigint;

-- name: UpdateInodeSizeAndTouchForWrite :one
UPDATE inodes
SET
    size = $2,
    mode = $3,
    mtime = ts.now,
    ctime = ts.now
FROM (SELECT clock_timestamp() AS now) AS ts
WHERE ino = $1
RETURNING ino, generation, mode, size, nlink, uid, gid, rdev, atime, mtime, ctime, symlink_target, flags, parent_ino, root_ino;

-- name: GetInodeSizeAndChunksInRange :many
SELECT
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
    i.symlink_target,
    i.flags,
    i.parent_ino,
    i.root_ino,
    c.indx AS chunk_indx,
    c.slices AS chunk_slices
FROM inodes i
LEFT JOIN chunks c ON c.ino = i.ino AND c.indx >= @start_indx::bigint AND c.indx <= @end_indx::bigint
WHERE i.ino = @ino::bigint
ORDER BY c.indx ASC;

-- name: LockValidSliceRefsKeyShare :many
SELECT slice_id
FROM slice_refs
WHERE slice_id = ANY(@slice_ids::bigint[])
  AND refs > 0
ORDER BY slice_id ASC
FOR KEY SHARE;


