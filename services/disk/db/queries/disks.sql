-- name: CreateDisk :one
INSERT INTO disks (
    disk_id,
    name,
    capacity_bytes,
    inodes_quota,
    status
) VALUES (
    $1, $2, $3, $4, $5
)
RETURNING *;

-- name: GetDisk :one
SELECT * FROM disks
WHERE disk_id = $1;

-- name: UpdateDiskStatus :one
UPDATE disks
SET status = $2
WHERE disk_id = $1
RETURNING *;

-- name: UpdateDisk :one
UPDATE disks
SET name = $2,
    capacity_bytes = $3,
    inodes_quota = $4,
    updated_at = now()
WHERE disk_id = $1
RETURNING *;

-- name: ListDisks :many
SELECT * FROM disks
WHERE (@status_filter::smallint = 0 OR status = @status_filter::smallint)
ORDER BY created_at DESC
LIMIT @limit_val OFFSET @offset_val;

-- name: CountDisks :one
SELECT COUNT(*) FROM disks
WHERE (@status_filter::smallint = 0 OR status = @status_filter::smallint);

-- name: DeleteDisk :execrows
DELETE FROM disks
WHERE disk_id = $1;

