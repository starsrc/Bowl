-- name: CreateCleanupTask :one
INSERT INTO disk_cleanup_tasks (
    task_id,
    disk_id,
    status
) VALUES (
    $1, $2, $3
)
RETURNING *;

-- name: GetCleanupTask :one
SELECT * FROM disk_cleanup_tasks
WHERE task_id = $1;

-- name: GetNextPendingCleanupTask :one
SELECT * FROM disk_cleanup_tasks
WHERE status = 1
ORDER BY created_at ASC
LIMIT 1;

-- name: GetPendingCleanupTasks :many
SELECT * FROM disk_cleanup_tasks
WHERE status = 1
ORDER BY created_at ASC
LIMIT $1;

-- name: UpdateCleanupTaskStatus :one
UPDATE disk_cleanup_tasks
SET status = $2,
    updated_at = NOW()
WHERE task_id = $1
RETURNING *;

-- name: RecordCleanupTaskAttempt :one
UPDATE disk_cleanup_tasks
SET attempts = attempts + 1,
    last_error = $2,
    status = $3,
    updated_at = NOW()
WHERE task_id = $1
RETURNING *;

-- name: FailCleanupTask :one
UPDATE disk_cleanup_tasks
SET status = 4,
    attempts = attempts + 1,
    last_error = $2,
    updated_at = NOW()
WHERE task_id = $1
RETURNING *;

-- name: GetCleanupTaskByDiskID :one
SELECT * FROM disk_cleanup_tasks
WHERE disk_id = $1
ORDER BY created_at DESC
LIMIT 1;

-- name: ListCleanupTasksByDiskID :many
SELECT * FROM disk_cleanup_tasks
WHERE disk_id = $1
ORDER BY created_at DESC;

-- name: ListCleanupTasks :many
SELECT * FROM disk_cleanup_tasks
WHERE (@status_filter::smallint = 0 OR status = @status_filter::smallint)
ORDER BY created_at DESC
LIMIT @limit_val OFFSET @offset_val;

-- name: DeleteCleanupTask :execrows
DELETE FROM disk_cleanup_tasks
WHERE task_id = $1;
