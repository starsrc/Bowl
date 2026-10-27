-- name: NextSliceID :one
SELECT nextval('slice_id_seq')::bigint;

-- name: GetChunk :one
SELECT ino, indx, slices
FROM chunks
WHERE ino = @ino::bigint AND indx = @indx::integer;

-- name: GetChunkForUpdate :one
SELECT ino, indx, slices
FROM chunks
WHERE ino = @ino::bigint AND indx = @indx::integer
FOR UPDATE;

-- name: GetChunksInRange :many
SELECT ino, indx, slices
FROM chunks
WHERE ino = @ino::bigint AND indx >= @start_indx::integer AND indx <= @end_indx::integer
ORDER BY indx ASC;

-- name: AppendChunkSlice :exec
INSERT INTO chunks (ino, indx, slices)
VALUES (@ino::bigint, @indx::integer, @slices::bytea)
ON CONFLICT (ino, indx)
DO UPDATE SET slices = chunks.slices || EXCLUDED.slices;

-- name: AppendExistingChunkSlice :execrows
UPDATE chunks
SET slices = slices || @slices::bytea
WHERE ino = @ino::bigint AND indx = @indx::integer;

-- name: UpdateChunkSlices :exec
UPDATE chunks
SET slices = @slices::bytea
WHERE ino = @ino::bigint AND indx = @indx::integer;

-- name: DeleteChunk :many
DELETE FROM chunks
WHERE ino = @ino::bigint AND indx = @indx::integer
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
WHERE ino = @ino::bigint AND indx >= @from_indx::integer
RETURNING slices;

-- name: DeleteChunksAfterIndx :many
DELETE FROM chunks
WHERE ino = @ino::bigint AND indx > @after_indx::integer
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

