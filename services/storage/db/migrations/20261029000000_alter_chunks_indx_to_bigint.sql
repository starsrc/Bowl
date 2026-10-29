-- +goose Up
-- +goose StatementBegin
ALTER TABLE chunks ALTER COLUMN indx TYPE BIGINT;
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
ALTER TABLE chunks ALTER COLUMN indx TYPE INTEGER;
-- +goose StatementEnd
