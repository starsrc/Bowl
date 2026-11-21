-- name: CreateDiskLineage :one
INSERT INTO disk_lineages (lineage_id, next_ino, next_pack_id, created_at)
VALUES ($1, 2, 1, clock_timestamp())
RETURNING lineage_id, next_ino, next_pack_id, created_at;

-- name: LockDiskLineage :one
SELECT lineage_id, next_ino, next_pack_id, created_at
FROM disk_lineages
WHERE lineage_id = $1
FOR UPDATE;

-- name: ReserveInodeSegment :one
UPDATE disk_lineages
SET next_ino = GREATEST(next_ino, @min_next::bigint) + @count::bigint
WHERE lineage_id = @lineage_id::uuid
  AND @count::bigint > 0
RETURNING
    (next_ino - @count::bigint)::bigint AS first_ino,
    (next_ino - 1)::bigint AS last_ino;

-- name: ReservePackSegment :one
UPDATE disk_lineages
SET next_pack_id = GREATEST(next_pack_id, @min_next::bigint) + @count::bigint
WHERE lineage_id = @lineage_id::uuid
  AND @count::bigint > 0
RETURNING
    (next_pack_id - @count::bigint)::bigint AS first_pack_id,
    (next_pack_id - 1)::bigint AS last_pack_id;

-- name: CreateDiskSnapshot :one
INSERT INTO disk_snapshots (
    snapshot_id,
    lineage_id,
    source_disk_id,
    parent_snapshot_id,
    merge_parent_snapshot_id,
    created_at,
    kind
)
VALUES ($1, $2, $3, $4, $5, clock_timestamp(), $6)
RETURNING snapshot_id, lineage_id, source_disk_id, parent_snapshot_id, merge_parent_snapshot_id, created_at, kind;

-- name: ListLineageSnapshots :many
SELECT snapshot_id, lineage_id, source_disk_id, parent_snapshot_id, merge_parent_snapshot_id, created_at, kind
FROM disk_snapshots
WHERE lineage_id = $1
ORDER BY created_at ASC, snapshot_id ASC;

-- name: CreateDisk :one
INSERT INTO disks (
    disk_id,
    name,
    lineage_id,
    parent_disk_id,
    base_snapshot_id,
    max_bytes,
    used_bytes,
    used_inodes,
    created_at,
    updated_at
)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, clock_timestamp(), clock_timestamp())
RETURNING disk_id, name, created_at, updated_at, lineage_id, parent_disk_id, base_snapshot_id, max_bytes, used_bytes, used_inodes;

-- name: GetDisk :one
SELECT disk_id, name, created_at, updated_at, lineage_id, parent_disk_id, base_snapshot_id, max_bytes, used_bytes, used_inodes
FROM disks
WHERE disk_id = $1;

-- name: ListDisks :many
SELECT disk_id, name, created_at, updated_at, lineage_id, parent_disk_id, base_snapshot_id, max_bytes, used_bytes, used_inodes
FROM disks
WHERE (NOT @has_cursor::boolean)
   OR (created_at, disk_id) < (@cursor_created_at::timestamptz, @cursor_disk_id::uuid)
ORDER BY created_at DESC, disk_id DESC
LIMIT @page_limit::int;

-- name: ListLineageDisks :many
SELECT disk_id, name, created_at, updated_at, lineage_id, parent_disk_id, base_snapshot_id, max_bytes, used_bytes, used_inodes
FROM disks
WHERE lineage_id = $1
ORDER BY disk_id ASC;

-- name: ListLineageDisksForUpdate :many
SELECT disk_id, name, created_at, updated_at, lineage_id, parent_disk_id, base_snapshot_id, max_bytes, used_bytes, used_inodes
FROM disks
WHERE lineage_id = $1
ORDER BY disk_id ASC
FOR UPDATE;

-- name: GetDiskForUpdate :one
SELECT disk_id, name, created_at, updated_at, lineage_id, parent_disk_id, base_snapshot_id, max_bytes, used_bytes, used_inodes
FROM disks
WHERE disk_id = $1
FOR UPDATE;

-- name: UpdateDisk :one
UPDATE disks
SET name = COALESCE(sqlc.narg('name')::text, name),
    max_bytes = COALESCE(sqlc.narg('max_bytes')::bigint, max_bytes),
    updated_at = clock_timestamp()
WHERE disk_id = $1
RETURNING disk_id, name, created_at, updated_at, lineage_id, parent_disk_id, base_snapshot_id, max_bytes, used_bytes, used_inodes;

-- name: UpdateDiskUsage :execrows
UPDATE disks
SET used_bytes = $2,
    used_inodes = $3
WHERE disk_id = $1;

-- name: SetDiskBaseSnapshot :exec
UPDATE disks
SET base_snapshot_id = $2
WHERE disk_id = $1;

-- name: SetDiskBaseSnapshotAndTouch :one
UPDATE disks
SET base_snapshot_id = $2,
    max_bytes = COALESCE(sqlc.narg('max_bytes')::bigint, max_bytes),
    used_bytes = COALESCE(sqlc.narg('used_bytes')::bigint, used_bytes),
    used_inodes = COALESCE(sqlc.narg('used_inodes')::bigint, used_inodes),
    updated_at = clock_timestamp()
WHERE disk_id = $1
RETURNING disk_id, name, created_at, updated_at, lineage_id, parent_disk_id, base_snapshot_id, max_bytes, used_bytes, used_inodes;

-- name: ClearDisksBaseSnapshotByIDs :exec
UPDATE disks
SET base_snapshot_id = NULL
WHERE lineage_id = @lineage_id::uuid
  AND base_snapshot_id = ANY(@snapshot_ids::uuid[]);

-- name: ClearSnapshotParentByIDs :exec
UPDATE disk_snapshots
SET parent_snapshot_id = NULL
WHERE lineage_id = @lineage_id::uuid
  AND parent_snapshot_id = ANY(@snapshot_ids::uuid[]);

-- name: ClearSnapshotMergeParentByIDs :exec
UPDATE disk_snapshots
SET merge_parent_snapshot_id = NULL
WHERE lineage_id = @lineage_id::uuid
  AND merge_parent_snapshot_id = ANY(@snapshot_ids::uuid[]);

-- name: DeleteSnapshotsByIDs :many
DELETE FROM disk_snapshots
WHERE lineage_id = @lineage_id::uuid
  AND snapshot_id = ANY(@snapshot_ids::uuid[])
RETURNING snapshot_id;

-- name: DeleteDisksByIDs :execrows
DELETE FROM disks
WHERE lineage_id = @lineage_id::uuid
  AND disk_id = ANY(@disk_ids::uuid[]);

-- name: DeleteDiskLineage :exec
DELETE FROM disk_lineages
WHERE lineage_id = $1;

-- name: CreateMountLease :one
INSERT INTO disk_mounts (
    disk_id,
    mount_id,
    read_only,
    epoch,
    expires_at,
    created_at,
    updated_at
)
VALUES ($1, $2, $3, $4, $5, clock_timestamp(), clock_timestamp())
RETURNING disk_id, mount_id, read_only, epoch, expires_at, created_at, updated_at;

-- name: GetMountLeaseForUpdate :one
SELECT disk_id, mount_id, read_only, epoch, expires_at, created_at, updated_at
FROM disk_mounts
WHERE disk_id = $1
  AND mount_id = $2
FOR UPDATE;

-- name: RenewMountLease :one
UPDATE disk_mounts
SET expires_at = @expires_at::timestamptz,
    updated_at = clock_timestamp()
WHERE disk_id = @disk_id::uuid
  AND mount_id = @mount_id::uuid
  AND read_only = @read_only::boolean
  AND epoch = @epoch::bigint
  AND expires_at > @now::timestamptz
RETURNING disk_id, mount_id, read_only, epoch, expires_at, created_at, updated_at;

-- name: DeleteMountLease :execrows
DELETE FROM disk_mounts
WHERE disk_id = $1
  AND mount_id = $2;

-- name: DeleteExpiredDiskMountLeases :exec
DELETE FROM disk_mounts
WHERE disk_id = $1
  AND expires_at <= $2;

-- name: DeleteSupersededRWMountLeases :exec
DELETE FROM disk_mounts
WHERE disk_id = $1
  AND NOT read_only
  AND epoch < $2;

-- name: DeleteDiskMountLeases :many
DELETE FROM disk_mounts
WHERE disk_id = $1
RETURNING expires_at;

-- name: ListActiveMountLeases :many
SELECT disk_id, mount_id, read_only, epoch, expires_at, created_at, updated_at
FROM disk_mounts
WHERE disk_id = $1
  AND expires_at > $2
ORDER BY mount_id ASC;

-- name: PruneExpiredMountLeases :execrows
DELETE FROM disk_mounts
WHERE expires_at <= $1;
