-- name: CreateDiskAttachment :one
INSERT INTO disk_attachments (
    id,
    disk_id,
    client_id,
    read_only
) VALUES (
    $1, $2, $3, $4
)
RETURNING *;

-- name: DeleteDiskAttachment :execrows
DELETE FROM disk_attachments
WHERE disk_id = $1 AND client_id = $2;

-- name: ListDiskAttachmentsByDiskID :many
SELECT * FROM disk_attachments
WHERE disk_id = $1
ORDER BY attached_at ASC;

-- name: CountDiskAttachmentsByDiskID :one
SELECT COUNT(*) FROM disk_attachments
WHERE disk_id = $1;

-- name: DeleteDiskAttachmentsByDiskID :execrows
DELETE FROM disk_attachments
WHERE disk_id = $1;
