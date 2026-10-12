-- +goose Up
CREATE TABLE disk_cleanup_tasks (
    task_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    disk_id UUID NOT NULL,
    status SMALLINT NOT NULL DEFAULT 1,
    attempts INT NOT NULL DEFAULT 0,
    last_error TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_disk_cleanup_tasks_status ON disk_cleanup_tasks(status);

CREATE TRIGGER set_disk_cleanup_tasks_updated_at
BEFORE UPDATE ON disk_cleanup_tasks
FOR EACH ROW
EXECUTE FUNCTION trigger_set_updated_at();

-- +goose Down
DROP TRIGGER IF EXISTS set_disk_cleanup_tasks_updated_at ON disk_cleanup_tasks;
DROP INDEX IF EXISTS idx_disk_cleanup_tasks_status;
DROP TABLE IF EXISTS disk_cleanup_tasks;
