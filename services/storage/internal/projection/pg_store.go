package projection

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
)

func toPGUUIDs(ids []uuid.UUID) []pgtype.UUID {
	out := make([]pgtype.UUID, len(ids))
	for i, id := range ids {
		out[i] = pgtype.UUID{Bytes: id, Valid: true}
	}
	return out
}

type PGStore struct {
	pool *pgxpool.Pool
}

func NewPGStore(pool *pgxpool.Pool) *PGStore {
	return &PGStore{pool: pool}
}

func mapPGError(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrNotFound
	}
	if pgErr, ok := errors.AsType[*pgconn.PgError](err); ok {
		if pgErr.Code == "23503" {
			return fmt.Errorf("%w: %w", ErrNotFound, err)
		}
	}
	if IsUnavailable(err) {
		if errors.Is(err, ErrUnavailable) {
			return err
		}
		return fmt.Errorf("%w: %w", ErrUnavailable, err)
	}
	return err
}

func (s *PGStore) requirePool() error {
	if s == nil || s.pool == nil {
		return fmt.Errorf("%w: nil postgres pool", ErrUnavailable)
	}
	return nil
}

func (s *PGStore) withTx(ctx context.Context, opts pgx.TxOptions, fn func(pgx.Tx) error) error {
	if err := s.requirePool(); err != nil {
		return err
	}
	tx, err := s.pool.BeginTx(ctx, opts)
	if err != nil {
		return mapPGError(err)
	}
	defer func() {
		_ = tx.Rollback(ctx)
	}()
	if fnErr := fn(tx); fnErr != nil {
		return fnErr
	}
	if commitErr := tx.Commit(ctx); commitErr != nil {
		return mapPGError(commitErr)
	}
	return nil
}

func checkDiskExistsTx(ctx context.Context, tx pgx.Tx, diskID uuid.UUID) error {
	var exists bool
	err := tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM disks WHERE disk_id = $1)`, diskID).Scan(&exists)
	if err != nil {
		return mapPGError(err)
	}
	if !exists {
		return ErrNotFound
	}
	return nil
}

func (s *PGStore) GetDisk(ctx context.Context, diskID uuid.UUID) (DiskRecord, error) {
	if diskID == uuid.Nil {
		return DiskRecord{}, fmt.Errorf("%w: disk_id cannot be nil", ErrInvalidArgument)
	}
	if err := s.requirePool(); err != nil {
		return DiskRecord{}, err
	}
	var (
		rec       DiskRecord
		maxBytes  int64
		usedBytes int64
	)
	err := s.pool.QueryRow(ctx, `
		SELECT disk_id, name, default_branch, max_bytes, used_bytes, created_at, updated_at
		FROM disks
		WHERE disk_id = $1
	`, diskID).Scan(
		&rec.DiskID,
		&rec.Name,
		&rec.DefaultBranch,
		&maxBytes,
		&usedBytes,
		&rec.CreatedAt,
		&rec.UpdatedAt,
	)
	if err != nil {
		return DiskRecord{}, mapPGError(err)
	}
	rec.MaxBytes = int64ToUint64(maxBytes)
	rec.UsedBytes = int64ToUint64(usedBytes)
	rec.CreatedAt = rec.CreatedAt.UTC()
	rec.UpdatedAt = rec.UpdatedAt.UTC()
	return rec, nil
}

func (s *PGStore) ListDisks(ctx context.Context, q ListDisksQuery) (ListDisksPage, error) {
	if err := s.requirePool(); err != nil {
		return ListDisksPage{}, err
	}
	limit, err := NormalizePageSize(q.PageSize)
	if err != nil {
		return ListDisksPage{}, err
	}
	var (
		rows pgx.Rows
	)
	if q.PageToken == "" {
		rows, err = s.pool.Query(ctx, `
			SELECT disk_id, name, default_branch, max_bytes, used_bytes, created_at, updated_at
			FROM disks
			ORDER BY created_at DESC, disk_id DESC
			LIMIT $1
		`, int(limit)+1)
	} else {
		var (
			cursorTime time.Time
			cursorID   uuid.UUID
		)
		cursorTime, cursorID, err = DecodeDiskPageCursor(q.PageToken)
		if err != nil {
			return ListDisksPage{}, err
		}
		rows, err = s.pool.Query(ctx, `
			SELECT disk_id, name, default_branch, max_bytes, used_bytes, created_at, updated_at
			FROM disks
			WHERE (created_at, disk_id) < ($1, $2)
			ORDER BY created_at DESC, disk_id DESC
			LIMIT $3
		`, cursorTime, cursorID, int(limit)+1)
	}
	if err != nil {
		return ListDisksPage{}, mapPGError(err)
	}
	defer rows.Close()

	disks := make([]DiskRecord, 0, min(int(limit)+1, 64))
	for rows.Next() {
		var (
			rec       DiskRecord
			maxBytes  int64
			usedBytes int64
		)
		if err = rows.Scan(
			&rec.DiskID,
			&rec.Name,
			&rec.DefaultBranch,
			&maxBytes,
			&usedBytes,
			&rec.CreatedAt,
			&rec.UpdatedAt,
		); err != nil {
			return ListDisksPage{}, mapPGError(err)
		}
		rec.MaxBytes = int64ToUint64(maxBytes)
		rec.UsedBytes = int64ToUint64(usedBytes)
		rec.CreatedAt = rec.CreatedAt.UTC()
		rec.UpdatedAt = rec.UpdatedAt.UTC()
		disks = append(disks, rec)
	}
	if err = rows.Err(); err != nil {
		return ListDisksPage{}, mapPGError(err)
	}

	page := ListDisksPage{}
	if len(disks) > int(limit) {
		disks = disks[:limit]
		last := disks[len(disks)-1]
		page.NextPageToken = EncodeDiskPageCursor(last.CreatedAt, last.DiskID)
	}
	page.Disks = disks
	return page, nil
}

func (s *PGStore) ListBranches(ctx context.Context, diskID uuid.UUID) ([]BranchRecord, error) {
	if diskID == uuid.Nil {
		return nil, fmt.Errorf("%w: disk_id cannot be nil", ErrInvalidArgument)
	}
	var out []BranchRecord
	err := s.withTx(ctx, pgx.TxOptions{AccessMode: pgx.ReadOnly}, func(tx pgx.Tx) error {
		if err := checkDiskExistsTx(ctx, tx, diskID); err != nil {
			return err
		}
		rows, err := tx.Query(ctx, `
			SELECT disk_id, name, head_commit_id, writer_epoch, created_at, updated_at
			FROM disk_branches
			WHERE disk_id = $1
			ORDER BY name ASC
		`, diskID)
		if err != nil {
			return mapPGError(err)
		}
		defer rows.Close()

		for rows.Next() {
			var (
				rec   BranchRecord
				head  int64
				epoch int64
			)
			if err = rows.Scan(
				&rec.DiskID,
				&rec.Name,
				&head,
				&epoch,
				&rec.CreatedAt,
				&rec.UpdatedAt,
			); err != nil {
				return mapPGError(err)
			}
			rec.HeadCommitID = int64ToUint64(head)
			rec.WriterEpoch = int64ToUint64(epoch)
			rec.CreatedAt = rec.CreatedAt.UTC()
			rec.UpdatedAt = rec.UpdatedAt.UTC()
			out = append(out, rec)
		}
		return mapPGError(rows.Err())
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (s *PGStore) ListCommits(ctx context.Context, q ListCommitsQuery) (ListCommitsPage, error) {
	if q.DiskID == uuid.Nil {
		return ListCommitsPage{}, fmt.Errorf("%w: disk_id cannot be nil", ErrInvalidArgument)
	}
	limit, err := NormalizePageSize(q.PageSize)
	if err != nil {
		return ListCommitsPage{}, err
	}
	if q.Kind != "" && !validCommitKind(q.Kind) {
		return ListCommitsPage{}, fmt.Errorf("%w: invalid commit kind %q", ErrInvalidArgument, q.Kind)
	}
	var (
		hasCursor bool
		cursorID  int64
	)
	if q.PageToken != "" {
		rawID, decErr := DecodeCommitPageCursor(q.PageToken)
		if decErr != nil {
			return ListCommitsPage{}, decErr
		}
		cursorID, err = uint64ToInt64(rawID)
		if err != nil {
			return ListCommitsPage{}, err
		}
		hasCursor = true
	}

	var commits []CommitRecord
	err = s.withTx(ctx, pgx.TxOptions{AccessMode: pgx.ReadOnly}, func(tx pgx.Tx) error {
		if txErr := checkDiskExistsTx(ctx, tx, q.DiskID); txErr != nil {
			return txErr
		}
		var (
			rows  pgx.Rows
			qErr  error
			fetch = int(limit) + 1
		)
		switch {
		case q.Kind == "" && !hasCursor:
			rows, qErr = tx.Query(ctx, `
				SELECT disk_id, commit_id, parent_id, merge_parent_id, kind, message, created_at
				FROM disk_commits
				WHERE disk_id = $1
				ORDER BY commit_id DESC
				LIMIT $2
			`, q.DiskID, fetch)
		case q.Kind != "" && !hasCursor:
			rows, qErr = tx.Query(ctx, `
				SELECT disk_id, commit_id, parent_id, merge_parent_id, kind, message, created_at
				FROM disk_commits
				WHERE disk_id = $1 AND kind = $2
				ORDER BY commit_id DESC
				LIMIT $3
			`, q.DiskID, q.Kind, fetch)
		case q.Kind == "" && hasCursor:
			rows, qErr = tx.Query(ctx, `
				SELECT disk_id, commit_id, parent_id, merge_parent_id, kind, message, created_at
				FROM disk_commits
				WHERE disk_id = $1 AND commit_id < $2
				ORDER BY commit_id DESC
				LIMIT $3
			`, q.DiskID, cursorID, fetch)
		default:
			rows, qErr = tx.Query(ctx, `
				SELECT disk_id, commit_id, parent_id, merge_parent_id, kind, message, created_at
				FROM disk_commits
				WHERE disk_id = $1 AND kind = $2 AND commit_id < $3
				ORDER BY commit_id DESC
				LIMIT $4
			`, q.DiskID, q.Kind, cursorID, fetch)
		}
		if qErr != nil {
			return mapPGError(qErr)
		}
		defer rows.Close()

		for rows.Next() {
			var (
				rec           CommitRecord
				commitID      int64
				parentID      int64
				mergeParentID int64
			)
			if scanErr := rows.Scan(
				&rec.DiskID,
				&commitID,
				&parentID,
				&mergeParentID,
				&rec.Kind,
				&rec.Message,
				&rec.CreatedAt,
			); scanErr != nil {
				return mapPGError(scanErr)
			}
			rec.CommitID = int64ToUint64(commitID)
			rec.ParentID = int64ToUint64(parentID)
			rec.MergeParentID = int64ToUint64(mergeParentID)
			rec.CreatedAt = rec.CreatedAt.UTC()
			commits = append(commits, rec)
		}
		return mapPGError(rows.Err())
	})
	if err != nil {
		return ListCommitsPage{}, err
	}

	page := ListCommitsPage{}
	if len(commits) > int(limit) {
		commits = commits[:limit]
		page.NextPageToken = EncodeCommitPageCursor(commits[len(commits)-1].CommitID)
	}
	page.Commits = commits
	return page, nil
}

func (s *PGStore) ListMounts(ctx context.Context, diskID uuid.UUID, branchName string, now time.Time) ([]MountRecord, error) {
	if diskID == uuid.Nil {
		return nil, fmt.Errorf("%w: disk_id cannot be nil", ErrInvalidArgument)
	}
	cutoff := normalizeTimestamp(now)
	var mounts []MountRecord
	err := s.withTx(ctx, pgx.TxOptions{AccessMode: pgx.ReadOnly}, func(tx pgx.Tx) error {
		if txErr := checkDiskExistsTx(ctx, tx, diskID); txErr != nil {
			return txErr
		}
		var (
			rows pgx.Rows
			qErr error
		)
		if branchName == "" {
			rows, qErr = tx.Query(ctx, `
				SELECT disk_id, mount_id, branch_name, read_only, writer_epoch, mounted_at, expires_at
				FROM disk_mounts
				WHERE disk_id = $1 AND expires_at > $2
				ORDER BY mount_id ASC
			`, diskID, cutoff)
		} else {
			rows, qErr = tx.Query(ctx, `
				SELECT disk_id, mount_id, branch_name, read_only, writer_epoch, mounted_at, expires_at
				FROM disk_mounts
				WHERE disk_id = $1 AND branch_name = $2 AND expires_at > $3
				ORDER BY mount_id ASC
			`, diskID, branchName, cutoff)
		}
		if qErr != nil {
			return mapPGError(qErr)
		}
		defer rows.Close()

		for rows.Next() {
			var (
				rec   MountRecord
				epoch int64
			)
			if scanErr := rows.Scan(
				&rec.DiskID,
				&rec.MountID,
				&rec.BranchName,
				&rec.ReadOnly,
				&epoch,
				&rec.MountedAt,
				&rec.ExpiresAt,
			); scanErr != nil {
				return mapPGError(scanErr)
			}
			rec.WriterEpoch = int64ToUint64(epoch)
			rec.MountedAt = rec.MountedAt.UTC()
			rec.ExpiresAt = rec.ExpiresAt.UTC()
			mounts = append(mounts, rec)
		}
		return mapPGError(rows.Err())
	})
	if err != nil {
		return nil, err
	}
	return mounts, nil
}

func upsertDiskTx(ctx context.Context, tx pgx.Tx, disk DiskRecord) error {
	maxBytes, err := uint64ToInt64(disk.MaxBytes)
	if err != nil {
		return err
	}
	usedBytes, err := uint64ToInt64(disk.UsedBytes)
	if err != nil {
		return err
	}
	_, err = tx.Exec(ctx, `
		INSERT INTO disks (disk_id, name, default_branch, max_bytes, used_bytes, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
		ON CONFLICT (disk_id) DO UPDATE SET
			name = EXCLUDED.name,
			default_branch = EXCLUDED.default_branch,
			max_bytes = EXCLUDED.max_bytes,
			used_bytes = EXCLUDED.used_bytes,
			created_at = EXCLUDED.created_at,
			updated_at = EXCLUDED.updated_at
	`,
		disk.DiskID,
		disk.Name,
		disk.DefaultBranch,
		maxBytes,
		usedBytes,
		normalizeTimestamp(disk.CreatedAt),
		normalizeTimestamp(disk.UpdatedAt),
	)
	return mapPGError(err)
}

func upsertBranchTx(ctx context.Context, tx pgx.Tx, b BranchRecord) error {
	head, err := uint64ToInt64(b.HeadCommitID)
	if err != nil {
		return err
	}
	epoch, err := uint64ToInt64(b.WriterEpoch)
	if err != nil {
		return err
	}
	_, err = tx.Exec(ctx, `
		INSERT INTO disk_branches (disk_id, name, head_commit_id, writer_epoch, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6)
		ON CONFLICT (disk_id, name) DO UPDATE SET
			head_commit_id = EXCLUDED.head_commit_id,
			writer_epoch = EXCLUDED.writer_epoch,
			created_at = EXCLUDED.created_at,
			updated_at = EXCLUDED.updated_at
	`,
		b.DiskID,
		b.Name,
		head,
		epoch,
		normalizeTimestamp(b.CreatedAt),
		normalizeTimestamp(b.UpdatedAt),
	)
	return mapPGError(err)
}

func upsertCommitTx(ctx context.Context, tx pgx.Tx, c CommitRecord) error {
	commitID, err := uint64ToInt64(c.CommitID)
	if err != nil {
		return err
	}
	parentID, err := uint64ToInt64(c.ParentID)
	if err != nil {
		return err
	}
	mergeParentID, err := uint64ToInt64(c.MergeParentID)
	if err != nil {
		return err
	}
	_, err = tx.Exec(ctx, `
		INSERT INTO disk_commits (disk_id, commit_id, parent_id, merge_parent_id, kind, message, created_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
		ON CONFLICT (disk_id, commit_id) DO UPDATE SET
			parent_id = EXCLUDED.parent_id,
			merge_parent_id = EXCLUDED.merge_parent_id,
			kind = EXCLUDED.kind,
			message = EXCLUDED.message,
			created_at = EXCLUDED.created_at
	`,
		c.DiskID,
		commitID,
		parentID,
		mergeParentID,
		c.Kind,
		c.Message,
		normalizeTimestamp(c.CreatedAt),
	)
	return mapPGError(err)
}

func upsertMountTx(ctx context.Context, tx pgx.Tx, m MountRecord) error {
	epoch, err := uint64ToInt64(m.WriterEpoch)
	if err != nil {
		return err
	}
	_, err = tx.Exec(ctx, `
		INSERT INTO disk_mounts (disk_id, mount_id, branch_name, read_only, writer_epoch, mounted_at, expires_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
		ON CONFLICT (disk_id, mount_id) DO UPDATE SET
			branch_name = EXCLUDED.branch_name,
			read_only = EXCLUDED.read_only,
			writer_epoch = EXCLUDED.writer_epoch,
			mounted_at = EXCLUDED.mounted_at,
			expires_at = EXCLUDED.expires_at
	`,
		m.DiskID,
		m.MountID,
		m.BranchName,
		m.ReadOnly,
		epoch,
		normalizeTimestamp(m.MountedAt),
		normalizeTimestamp(m.ExpiresAt),
	)
	return mapPGError(err)
}

func (s *PGStore) UpsertDisk(ctx context.Context, disk DiskRecord, initialBranch *BranchRecord, initialCommit *CommitRecord) error {
	if err := validateDiskRecord(disk, true); err != nil {
		return err
	}
	if initialBranch != nil {
		if err := validateBranchRecord(*initialBranch); err != nil {
			return err
		}
		if initialBranch.DiskID != disk.DiskID {
			return fmt.Errorf("%w: initial branch disk_id mismatch", ErrInvalidArgument)
		}
	}
	if initialCommit != nil {
		if err := validateCommitRecord(*initialCommit); err != nil {
			return err
		}
		if initialCommit.DiskID != disk.DiskID {
			return fmt.Errorf("%w: initial commit disk_id mismatch", ErrInvalidArgument)
		}
	}
	return s.withTx(ctx, pgx.TxOptions{}, func(tx pgx.Tx) error {
		if err := upsertDiskTx(ctx, tx, disk); err != nil {
			return err
		}
		if initialBranch != nil {
			if err := upsertBranchTx(ctx, tx, *initialBranch); err != nil {
				return err
			}
		}
		if initialCommit != nil {
			if err := upsertCommitTx(ctx, tx, *initialCommit); err != nil {
				return err
			}
		}
		return nil
	})
}

func (s *PGStore) UpdateDisk(ctx context.Context, disk DiskRecord) error {
	if err := validateDiskRecord(disk, false); err != nil {
		return err
	}
	if err := s.requirePool(); err != nil {
		return err
	}
	maxBytes, err := uint64ToInt64(disk.MaxBytes)
	if err != nil {
		return err
	}
	usedBytes, err := uint64ToInt64(disk.UsedBytes)
	if err != nil {
		return err
	}
	tag, err := s.pool.Exec(ctx, `
		UPDATE disks
		SET name = $2,
			default_branch = COALESCE(NULLIF($3, ''), default_branch),
			max_bytes = $4,
			used_bytes = $5,
			updated_at = $6
		WHERE disk_id = $1
	`,
		disk.DiskID,
		disk.Name,
		disk.DefaultBranch,
		maxBytes,
		usedBytes,
		normalizeTimestamp(disk.UpdatedAt),
	)
	if err != nil {
		return mapPGError(err)
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

func (s *PGStore) DeleteDisk(ctx context.Context, diskID uuid.UUID) error {
	if diskID == uuid.Nil {
		return fmt.Errorf("%w: disk_id cannot be nil", ErrInvalidArgument)
	}
	if err := s.requirePool(); err != nil {
		return err
	}
	_, err := s.pool.Exec(ctx, `DELETE FROM disks WHERE disk_id = $1`, diskID)
	return mapPGError(err)
}

func (s *PGStore) UpsertBranchAndCommit(ctx context.Context, branches []BranchRecord, commits []CommitRecord, deletedMountIDs []uuid.UUID) error {
	if len(branches) == 0 && len(commits) == 0 && len(deletedMountIDs) == 0 {
		return nil
	}
	var sharedDiskID uuid.UUID
	for _, b := range branches {
		if err := validateBranchRecord(b); err != nil {
			return err
		}
		if sharedDiskID == uuid.Nil {
			sharedDiskID = b.DiskID
		}
	}
	for _, c := range commits {
		if err := validateCommitRecord(c); err != nil {
			return err
		}
		if sharedDiskID == uuid.Nil {
			sharedDiskID = c.DiskID
		}
	}
	if slices.Contains(deletedMountIDs, uuid.Nil) {
		return fmt.Errorf("%w: deleted mount_id cannot be nil", ErrInvalidArgument)
	}
	return s.withTx(ctx, pgx.TxOptions{}, func(tx pgx.Tx) error {
		for _, b := range branches {
			if err := upsertBranchTx(ctx, tx, b); err != nil {
				return err
			}
		}
		for _, c := range commits {
			if err := upsertCommitTx(ctx, tx, c); err != nil {
				return err
			}
		}
		if len(deletedMountIDs) > 0 {
			pgIDs := toPGUUIDs(deletedMountIDs)
			var err error
			if sharedDiskID != uuid.Nil {
				_, err = tx.Exec(ctx, `DELETE FROM disk_mounts WHERE disk_id = $1 AND mount_id = ANY($2::uuid[])`, sharedDiskID, pgIDs)
			} else {
				_, err = tx.Exec(ctx, `DELETE FROM disk_mounts WHERE mount_id = ANY($1::uuid[])`, pgIDs)
			}
			if err != nil {
				return mapPGError(err)
			}
		}
		return nil
	})
}

func (s *PGStore) DeleteBranch(ctx context.Context, diskID uuid.UUID, branchName string) error {
	if diskID == uuid.Nil || branchName == "" {
		return fmt.Errorf("%w: disk_id and branch_name are required", ErrInvalidArgument)
	}
	if err := s.requirePool(); err != nil {
		return err
	}
	_, err := s.pool.Exec(ctx, `DELETE FROM disk_branches WHERE disk_id = $1 AND name = $2`, diskID, branchName)
	return mapPGError(err)
}

func (s *PGStore) DeleteCommits(ctx context.Context, diskID uuid.UUID, commitIDs []uint64) error {
	if diskID == uuid.Nil {
		return fmt.Errorf("%w: disk_id cannot be nil", ErrInvalidArgument)
	}
	if len(commitIDs) == 0 {
		return nil
	}
	if err := s.requirePool(); err != nil {
		return err
	}
	ids := make([]int64, len(commitIDs))
	for i, cid := range commitIDs {
		if cid == 0 {
			return fmt.Errorf("%w: commit_id cannot be zero", ErrInvalidArgument)
		}
		v, err := uint64ToInt64(cid)
		if err != nil {
			return err
		}
		ids[i] = v
	}
	_, err := s.pool.Exec(ctx, `DELETE FROM disk_commits WHERE disk_id = $1 AND commit_id = ANY($2::bigint[])`, diskID, ids)
	return mapPGError(err)
}

func (s *PGStore) UpsertMount(ctx context.Context, mount MountRecord, updatedBranch *BranchRecord, deletedMountIDs []uuid.UUID) error {
	if err := validateMountRecord(mount); err != nil {
		return err
	}
	if updatedBranch != nil {
		if err := validateBranchRecord(*updatedBranch); err != nil {
			return err
		}
		if updatedBranch.DiskID != mount.DiskID {
			return fmt.Errorf("%w: updated branch disk_id mismatch", ErrInvalidArgument)
		}
	}
	if slices.Contains(deletedMountIDs, uuid.Nil) {
		return fmt.Errorf("%w: deleted mount_id cannot be nil", ErrInvalidArgument)
	}
	return s.withTx(ctx, pgx.TxOptions{}, func(tx pgx.Tx) error {
		if len(deletedMountIDs) > 0 {
			if _, err := tx.Exec(ctx, `DELETE FROM disk_mounts WHERE disk_id = $1 AND mount_id = ANY($2::uuid[])`, mount.DiskID, toPGUUIDs(deletedMountIDs)); err != nil {
				return mapPGError(err)
			}
		}
		if updatedBranch != nil {
			if err := upsertBranchTx(ctx, tx, *updatedBranch); err != nil {
				return err
			}
		}
		return upsertMountTx(ctx, tx, mount)
	})
}

func (s *PGStore) DeleteMounts(ctx context.Context, diskID uuid.UUID, mountIDs []uuid.UUID, updatedBranches []BranchRecord) error {
	if diskID == uuid.Nil {
		return fmt.Errorf("%w: disk_id cannot be nil", ErrInvalidArgument)
	}
	if slices.Contains(mountIDs, uuid.Nil) {
		return fmt.Errorf("%w: mount_id cannot be nil", ErrInvalidArgument)
	}
	for _, b := range updatedBranches {
		if err := validateBranchRecord(b); err != nil {
			return err
		}
		if b.DiskID != diskID {
			return fmt.Errorf("%w: updated branch disk_id mismatch", ErrInvalidArgument)
		}
	}
	if len(mountIDs) == 0 && len(updatedBranches) == 0 {
		return nil
	}
	return s.withTx(ctx, pgx.TxOptions{}, func(tx pgx.Tx) error {
		if len(mountIDs) > 0 {
			if _, err := tx.Exec(ctx, `DELETE FROM disk_mounts WHERE disk_id = $1 AND mount_id = ANY($2::uuid[])`, diskID, toPGUUIDs(mountIDs)); err != nil {
				return mapPGError(err)
			}
		}
		for _, b := range updatedBranches {
			if err := upsertBranchTx(ctx, tx, b); err != nil {
				return err
			}
		}
		return nil
	})
}

func (s *PGStore) BatchUpdateDiskUsage(ctx context.Context, updates []DiskUsageUpdate) error {
	if len(updates) == 0 {
		return nil
	}
	if err := s.requirePool(); err != nil {
		return err
	}
	dedup := make(map[uuid.UUID]int64, len(updates))
	order := make([]uuid.UUID, 0, len(updates))
	for _, u := range updates {
		if u.DiskID == uuid.Nil {
			return fmt.Errorf("%w: disk_id cannot be nil", ErrInvalidArgument)
		}
		v, err := uint64ToInt64(u.UsedBytes)
		if err != nil {
			return err
		}
		if _, exists := dedup[u.DiskID]; !exists {
			order = append(order, u.DiskID)
		}
		dedup[u.DiskID] = v
	}
	diskIDs := make([]uuid.UUID, len(order))
	usedBytes := make([]int64, len(order))
	for i, id := range order {
		diskIDs[i] = id
		usedBytes[i] = dedup[id]
	}
	_, err := s.pool.Exec(ctx, `
		UPDATE disks AS d
		SET used_bytes = u.used_bytes
		FROM UNNEST($1::uuid[], $2::bigint[]) AS u(disk_id, used_bytes)
		WHERE d.disk_id = u.disk_id
	`, toPGUUIDs(diskIDs), usedBytes)
	return mapPGError(err)
}

func (s *PGStore) BatchRenewMounts(ctx context.Context, updates []MountRenewUpdate) error {
	if len(updates) == 0 {
		return nil
	}
	if err := s.requirePool(); err != nil {
		return err
	}
	type renewKey struct {
		diskID  uuid.UUID
		mountID uuid.UUID
	}
	dedup := make(map[renewKey]time.Time, len(updates))
	order := make([]renewKey, 0, len(updates))
	for _, u := range updates {
		if u.DiskID == uuid.Nil || u.MountID == uuid.Nil {
			return fmt.Errorf("%w: disk_id and mount_id are required", ErrInvalidArgument)
		}
		k := renewKey{diskID: u.DiskID, mountID: u.MountID}
		if _, exists := dedup[k]; !exists {
			order = append(order, k)
		}
		dedup[k] = normalizeTimestamp(u.ExpiresAt)
	}
	diskIDs := make([]uuid.UUID, len(order))
	mountIDs := make([]uuid.UUID, len(order))
	expiresAt := make([]time.Time, len(order))
	for i, k := range order {
		diskIDs[i] = k.diskID
		mountIDs[i] = k.mountID
		expiresAt[i] = dedup[k]
	}
	_, err := s.pool.Exec(ctx, `
		UPDATE disk_mounts AS m
		SET expires_at = u.expires_at
		FROM UNNEST($1::uuid[], $2::uuid[], $3::timestamptz[]) AS u(disk_id, mount_id, expires_at)
		WHERE m.disk_id = u.disk_id AND m.mount_id = u.mount_id
	`, toPGUUIDs(diskIDs), toPGUUIDs(mountIDs), expiresAt)
	return mapPGError(err)
}

func (s *PGStore) ReplaceDiskSnapshot(ctx context.Context, snap DiskSnapshot) error {
	if err := validateDiskRecord(snap.Disk, true); err != nil {
		return err
	}
	for _, b := range snap.Branches {
		if err := validateBranchRecord(b); err != nil {
			return err
		}
		if b.DiskID != snap.Disk.DiskID {
			return fmt.Errorf("%w: snapshot branch disk_id mismatch", ErrInvalidArgument)
		}
	}
	for _, c := range snap.Commits {
		if err := validateCommitRecord(c); err != nil {
			return err
		}
		if c.DiskID != snap.Disk.DiskID {
			return fmt.Errorf("%w: snapshot commit disk_id mismatch", ErrInvalidArgument)
		}
	}
	for _, m := range snap.Mounts {
		if err := validateMountRecord(m); err != nil {
			return err
		}
		if m.DiskID != snap.Disk.DiskID {
			return fmt.Errorf("%w: snapshot mount disk_id mismatch", ErrInvalidArgument)
		}
	}
	return s.withTx(ctx, pgx.TxOptions{}, func(tx pgx.Tx) error {
		if err := upsertDiskTx(ctx, tx, snap.Disk); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `DELETE FROM disk_branches WHERE disk_id = $1`, snap.Disk.DiskID); err != nil {
			return mapPGError(err)
		}
		if _, err := tx.Exec(ctx, `DELETE FROM disk_commits WHERE disk_id = $1`, snap.Disk.DiskID); err != nil {
			return mapPGError(err)
		}
		if _, err := tx.Exec(ctx, `DELETE FROM disk_mounts WHERE disk_id = $1`, snap.Disk.DiskID); err != nil {
			return mapPGError(err)
		}
		for _, b := range snap.Branches {
			if err := upsertBranchTx(ctx, tx, b); err != nil {
				return err
			}
		}
		for _, c := range snap.Commits {
			if err := upsertCommitTx(ctx, tx, c); err != nil {
				return err
			}
		}
		for _, m := range snap.Mounts {
			if err := upsertMountTx(ctx, tx, m); err != nil {
				return err
			}
		}
		return nil
	})
}

func (s *PGStore) ListAllDiskIDs(ctx context.Context) ([]uuid.UUID, error) {
	if err := s.requirePool(); err != nil {
		return nil, err
	}
	rows, err := s.pool.Query(ctx, `SELECT disk_id FROM disks ORDER BY disk_id ASC`)
	if err != nil {
		return nil, mapPGError(err)
	}
	defer rows.Close()

	var ids []uuid.UUID
	for rows.Next() {
		var id uuid.UUID
		if err = rows.Scan(&id); err != nil {
			return nil, mapPGError(err)
		}
		ids = append(ids, id)
	}
	if err = rows.Err(); err != nil {
		return nil, mapPGError(err)
	}
	return ids, nil
}

func (s *PGStore) PruneExpiredMounts(ctx context.Context, now time.Time) (int64, error) {
	if err := s.requirePool(); err != nil {
		return 0, err
	}
	cutoff := normalizeTimestamp(now)
	tag, err := s.pool.Exec(ctx, `DELETE FROM disk_mounts WHERE expires_at <= $1`, cutoff)
	if err != nil {
		return 0, mapPGError(err)
	}
	return tag.RowsAffected(), nil
}
