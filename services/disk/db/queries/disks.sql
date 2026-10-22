-- name: CreateDisk :one
INSERT INTO disks (
    disk_id,
    name,
    root_ino,
    max_bytes,
    used_bytes,
    created_at,
    updated_at
) VALUES (
    $1,
    $2,
    $3,
    $4,
    0,
    clock_timestamp(),
    clock_timestamp()
)
RETURNING disk_id, name, root_ino, max_bytes, used_bytes, created_at, updated_at;

-- name: GetDisk :one
SELECT disk_id, name, root_ino, max_bytes, used_bytes, created_at, updated_at
FROM disks
WHERE disk_id = $1;

-- name: GetDiskForUpdate :one
SELECT disk_id, name, root_ino, max_bytes, used_bytes, created_at, updated_at
FROM disks
WHERE disk_id = $1
FOR UPDATE;

-- name: UpdateDisk :one
UPDATE disks
SET
    name = $2,
    max_bytes = $3,
    updated_at = clock_timestamp()
WHERE disk_id = $1
RETURNING disk_id, name, root_ino, max_bytes, used_bytes, created_at, updated_at;

-- name: UpdateDiskUsedBytes :one
UPDATE disks
SET
    used_bytes = $2,
    updated_at = clock_timestamp()
WHERE disk_id = $1
RETURNING disk_id, name, root_ino, max_bytes, used_bytes, created_at, updated_at;

-- name: DeleteDisk :execrows
DELETE FROM disks
WHERE disk_id = $1;
