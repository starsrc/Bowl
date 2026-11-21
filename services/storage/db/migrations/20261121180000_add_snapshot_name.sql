-- +goose Up
-- +goose StatementBegin
ALTER TABLE disk_snapshots ADD COLUMN name TEXT NOT NULL DEFAULT '';
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
ALTER TABLE disk_snapshots DROP COLUMN IF EXISTS name;
-- +goose StatementEnd
