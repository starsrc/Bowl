-- +goose Up
-- +goose StatementBegin
ALTER TABLE disks DROP CONSTRAINT IF EXISTS fk_disks_base_commit;
ALTER TABLE disk_commits
    DROP CONSTRAINT IF EXISTS fk_disk_commits_parent,
    DROP CONSTRAINT IF EXISTS fk_disk_commits_merge_parent,
    DROP CONSTRAINT IF EXISTS chk_disk_commits_not_self_parent,
    DROP CONSTRAINT IF EXISTS uq_disk_commits_lineage_commit;
DROP INDEX IF EXISTS idx_disk_commits_lineage_id;

ALTER TABLE disk_commits RENAME TO disk_snapshots;
ALTER TABLE disk_snapshots RENAME COLUMN commit_id TO snapshot_id;
ALTER TABLE disk_snapshots RENAME COLUMN parent_commit_id TO parent_snapshot_id;
ALTER TABLE disk_snapshots RENAME COLUMN merge_parent_commit_id TO merge_parent_snapshot_id;

ALTER TABLE disk_snapshots
    ADD COLUMN kind TEXT NOT NULL DEFAULT 'fork';

UPDATE disk_snapshots
SET kind = 'merge'
WHERE merge_parent_snapshot_id IS NOT NULL;

ALTER TABLE disk_snapshots
    ALTER COLUMN kind DROP DEFAULT,
    ADD CONSTRAINT chk_disk_snapshots_kind CHECK (kind IN ('fork', 'merge', 'manual')),
    ADD CONSTRAINT uq_disk_snapshots_lineage_snapshot UNIQUE (lineage_id, snapshot_id),
    ADD CONSTRAINT fk_disk_snapshots_parent FOREIGN KEY (lineage_id, parent_snapshot_id)
        REFERENCES disk_snapshots(lineage_id, snapshot_id),
    ADD CONSTRAINT fk_disk_snapshots_merge_parent FOREIGN KEY (lineage_id, merge_parent_snapshot_id)
        REFERENCES disk_snapshots(lineage_id, snapshot_id),
    ADD CONSTRAINT chk_disk_snapshots_not_self_parent CHECK (
        snapshot_id IS DISTINCT FROM parent_snapshot_id
        AND snapshot_id IS DISTINCT FROM merge_parent_snapshot_id
    );

CREATE INDEX idx_disk_snapshots_lineage_id ON disk_snapshots (lineage_id);

ALTER TABLE disks RENAME COLUMN base_commit_id TO base_snapshot_id;
ALTER TABLE disks
    ADD CONSTRAINT fk_disks_base_snapshot FOREIGN KEY (lineage_id, base_snapshot_id)
        REFERENCES disk_snapshots(lineage_id, snapshot_id);
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
ALTER TABLE disks DROP CONSTRAINT IF EXISTS fk_disks_base_snapshot;
ALTER TABLE disks RENAME COLUMN base_snapshot_id TO base_commit_id;

DROP INDEX IF EXISTS idx_disk_snapshots_lineage_id;
ALTER TABLE disk_snapshots
    DROP CONSTRAINT IF EXISTS chk_disk_snapshots_not_self_parent,
    DROP CONSTRAINT IF EXISTS fk_disk_snapshots_merge_parent,
    DROP CONSTRAINT IF EXISTS fk_disk_snapshots_parent,
    DROP CONSTRAINT IF EXISTS uq_disk_snapshots_lineage_snapshot,
    DROP CONSTRAINT IF EXISTS chk_disk_snapshots_kind,
    DROP COLUMN IF EXISTS kind;

ALTER TABLE disk_snapshots RENAME COLUMN merge_parent_snapshot_id TO merge_parent_commit_id;
ALTER TABLE disk_snapshots RENAME COLUMN parent_snapshot_id TO parent_commit_id;
ALTER TABLE disk_snapshots RENAME COLUMN snapshot_id TO commit_id;
ALTER TABLE disk_snapshots RENAME TO disk_commits;

ALTER TABLE disk_commits
    ADD CONSTRAINT uq_disk_commits_lineage_commit UNIQUE (lineage_id, commit_id),
    ADD CONSTRAINT fk_disk_commits_parent FOREIGN KEY (lineage_id, parent_commit_id)
        REFERENCES disk_commits(lineage_id, commit_id),
    ADD CONSTRAINT fk_disk_commits_merge_parent FOREIGN KEY (lineage_id, merge_parent_commit_id)
        REFERENCES disk_commits(lineage_id, commit_id),
    ADD CONSTRAINT chk_disk_commits_not_self_parent CHECK (
        commit_id IS DISTINCT FROM parent_commit_id
        AND commit_id IS DISTINCT FROM merge_parent_commit_id
    );
CREATE INDEX idx_disk_commits_lineage_id ON disk_commits (lineage_id);

ALTER TABLE disks
    ADD CONSTRAINT fk_disks_base_commit FOREIGN KEY (lineage_id, base_commit_id)
        REFERENCES disk_commits(lineage_id, commit_id);
-- +goose StatementEnd
