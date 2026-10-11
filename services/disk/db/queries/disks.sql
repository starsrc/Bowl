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
