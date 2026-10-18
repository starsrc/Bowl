-- +goose Up
-- +goose StatementBegin
CREATE TABLE IF NOT EXISTS client_sessions (
    session_id BIGINT PRIMARY KEY,
    expires_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);
CREATE INDEX IF NOT EXISTS idx_client_sessions_expires_at ON client_sessions(expires_at);

CREATE TABLE IF NOT EXISTS file_locks (
    id BIGSERIAL PRIMARY KEY,
    session_id BIGINT NOT NULL REFERENCES client_sessions(session_id) ON DELETE CASCADE,
    ino BIGINT NOT NULL REFERENCES inodes(ino) ON DELETE CASCADE,
    fh BIGINT NOT NULL,
    lock_owner BIGINT NOT NULL,
    lk_flags INTEGER NOT NULL DEFAULT 0,
    start_offset BIGINT NOT NULL,
    end_offset BIGINT NOT NULL,
    lock_type SMALLINT NOT NULL,
    pid INTEGER NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    CONSTRAINT chk_file_locks_range CHECK (start_offset >= 0 AND end_offset >= start_offset),
    CONSTRAINT chk_file_locks_type CHECK (lock_type IN (0, 1)),
    CONSTRAINT chk_file_locks_flags CHECK (lk_flags IN (0, 1))
);
CREATE INDEX IF NOT EXISTS idx_file_locks_ino_flags_range ON file_locks(ino, lk_flags, start_offset, end_offset);
CREATE INDEX IF NOT EXISTS idx_file_locks_session_ino_owner ON file_locks(session_id, ino, lock_owner);
CREATE INDEX IF NOT EXISTS idx_file_locks_session_ino_fh ON file_locks(session_id, ino, fh);

CREATE TABLE IF NOT EXISTS file_lock_waiters (
    session_id BIGINT NOT NULL REFERENCES client_sessions(session_id) ON DELETE CASCADE,
    request_id BIGINT NOT NULL,
    ino BIGINT NOT NULL REFERENCES inodes(ino) ON DELETE CASCADE,
    fh BIGINT NOT NULL,
    lock_owner BIGINT NOT NULL,
    lk_flags INTEGER NOT NULL DEFAULT 0,
    start_offset BIGINT NOT NULL,
    end_offset BIGINT NOT NULL,
    lock_type SMALLINT NOT NULL,
    pid INTEGER NOT NULL DEFAULT 0,
    status SMALLINT NOT NULL DEFAULT 1,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (session_id, request_id),
    CONSTRAINT chk_file_lock_waiters_range CHECK (start_offset >= 0 AND end_offset >= start_offset),
    CONSTRAINT chk_file_lock_waiters_type CHECK (lock_type IN (0, 1)),
    CONSTRAINT chk_file_lock_waiters_flags CHECK (lk_flags IN (0, 1)),
    CONSTRAINT chk_file_lock_waiters_status CHECK (status IN (1, 2, 3))
);
CREATE INDEX IF NOT EXISTS idx_file_lock_waiters_ino_flags_status ON file_lock_waiters(ino, lk_flags, status, created_at);
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DROP TABLE IF EXISTS file_lock_waiters;
DROP TABLE IF EXISTS file_locks;
DROP TABLE IF EXISTS client_sessions;
-- +goose StatementEnd
