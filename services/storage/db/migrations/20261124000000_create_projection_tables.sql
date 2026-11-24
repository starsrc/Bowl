-- +goose Up
CREATE TABLE IF NOT EXISTS disks (
    disk_id        UUID        PRIMARY KEY,
    name           TEXT        NOT NULL DEFAULT '',
    default_branch TEXT        NOT NULL DEFAULT 'main',
    max_bytes      BIGINT      NOT NULL CHECK (max_bytes > 0),
    used_bytes     BIGINT      NOT NULL DEFAULT 0 CHECK (used_bytes >= 0),
    created_at     TIMESTAMPTZ NOT NULL,
    updated_at     TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_disks_created_at_disk_id
    ON disks (created_at DESC, disk_id DESC);

CREATE TABLE IF NOT EXISTS disk_branches (
    disk_id        UUID        NOT NULL REFERENCES disks(disk_id) ON DELETE CASCADE,
    name           TEXT        NOT NULL,
    head_commit_id BIGINT      NOT NULL CHECK (head_commit_id >= 0),
    writer_epoch   BIGINT      NOT NULL DEFAULT 0 CHECK (writer_epoch >= 0),
    created_at     TIMESTAMPTZ NOT NULL,
    updated_at     TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (disk_id, name)
);

CREATE TABLE IF NOT EXISTS disk_commits (
    disk_id         UUID        NOT NULL REFERENCES disks(disk_id) ON DELETE CASCADE,
    commit_id       BIGINT      NOT NULL CHECK (commit_id > 0),
    parent_id       BIGINT      NOT NULL DEFAULT 0 CHECK (parent_id >= 0),
    merge_parent_id BIGINT      NOT NULL DEFAULT 0 CHECK (merge_parent_id >= 0),
    kind            TEXT        NOT NULL CHECK (kind IN ('init', 'manual', 'fork', 'merge')),
    message         TEXT        NOT NULL DEFAULT '',
    created_at      TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (disk_id, commit_id)
);

CREATE INDEX IF NOT EXISTS idx_disk_commits_disk_id_commit_id_desc
    ON disk_commits (disk_id, commit_id DESC);

CREATE TABLE IF NOT EXISTS disk_mounts (
    disk_id      UUID        NOT NULL REFERENCES disks(disk_id) ON DELETE CASCADE,
    mount_id     UUID        NOT NULL,
    branch_name  TEXT        NOT NULL,
    read_only    BOOLEAN     NOT NULL DEFAULT FALSE,
    writer_epoch BIGINT      NOT NULL DEFAULT 0 CHECK (writer_epoch >= 0),
    mounted_at   TIMESTAMPTZ NOT NULL,
    expires_at   TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (disk_id, mount_id)
);

CREATE INDEX IF NOT EXISTS idx_disk_mounts_disk_id_expires_at
    ON disk_mounts (disk_id, expires_at);

-- +goose Down
DROP INDEX IF EXISTS idx_disk_mounts_disk_id_expires_at;
DROP TABLE IF EXISTS disk_mounts;
DROP INDEX IF EXISTS idx_disk_commits_disk_id_commit_id_desc;
DROP TABLE IF EXISTS disk_commits;
DROP TABLE IF EXISTS disk_branches;
DROP INDEX IF EXISTS idx_disks_created_at_disk_id;
DROP TABLE IF EXISTS disks;
