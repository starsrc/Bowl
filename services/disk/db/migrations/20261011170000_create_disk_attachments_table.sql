-- +goose Up
CREATE TABLE disk_attachments (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    disk_id UUID NOT NULL REFERENCES disks(disk_id) ON DELETE CASCADE,
    client_id TEXT NOT NULL,
    read_only BOOLEAN NOT NULL,
    attached_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE UNIQUE INDEX idx_disk_attachments_writer ON disk_attachments (disk_id) WHERE read_only = false;
CREATE UNIQUE INDEX idx_disk_attachments_client ON disk_attachments (disk_id, client_id);

-- +goose Down
DROP TABLE IF EXISTS disk_attachments;
