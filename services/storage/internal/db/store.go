package db

import (
	"context"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.uber.org/fx"

	"github.com/starsrc/pkg/observability"
	"github.com/starsrc/pkg/tx"

	"github.com/starsrc/bowl/services/storage/internal/config"
)

var (
	ErrNotFound        = errors.New("db: not found")
	ErrDiskNotFound    = ErrNotFound
	ErrInvalidArgument = errors.New("db: invalid argument")
	ErrCrossLineage    = errors.New("db: cross lineage")
	ErrStaleBaseCommit = errors.New("db: stale base commit")
	ErrStaleEpoch      = errors.New("db: stale epoch")
)

type SegmentKind uint8

const (
	SegmentKindInode SegmentKind = iota + 1
	SegmentKindPack
)

type DiskRecord struct {
	DiskID       uuid.UUID
	ParentDiskID uuid.UUID
	LineageID    uuid.UUID
	BaseCommitID uuid.UUID
	Name         string
	MaxBytes     uint64
	UsedBytes    uint64
	UsedInodes   uint64
	CreatedAt    time.Time
	UpdatedAt    time.Time
}

type CommitRecord struct {
	LineageID           uuid.UUID
	CommitID            uuid.UUID
	ParentCommitID      uuid.UUID
	MergeParentCommitID uuid.UUID
	BaseDiskID          uuid.UUID
	CreatedAt           time.Time
}

type MountLeaseRecord struct {
	DiskID    uuid.UUID
	MountID   uuid.UUID
	ReadOnly  bool
	Epoch     uint64
	ExpiresAt time.Time
}

type ForkDiskInput struct {
	SourceDiskID uuid.UUID
	ChildDiskID  uuid.UUID
	CommitID     uuid.UUID
	Name         string
	MaxBytes     uint64
	UsedBytes    uint64
	UsedInodes   uint64
}

type UpdateDiskInput struct {
	DiskID   uuid.UUID
	Name     *string
	MaxBytes *uint64
}

type MergeDiskInput struct {
	LineageID                  uuid.UUID
	SourceDiskID               uuid.UUID
	TargetDiskID               uuid.UUID
	ExpectedSourceBaseCommitID uuid.UUID
	ExpectedTargetBaseCommitID uuid.UUID
	NewSourceCommitID          uuid.UUID
	NewMergeCommitID           uuid.UUID
	SkipSourceCommit           bool
	TargetMaxBytes             uint64
	TargetUsedBytes            uint64
	TargetUsedInodes           uint64
}

type CreateMountLeaseInput struct {
	DiskID    uuid.UUID
	MountID   uuid.UUID
	ReadOnly  bool
	Epoch     uint64
	Now       time.Time
	ExpiresAt time.Time
}

type RenewMountLeaseInput struct {
	DiskID    uuid.UUID
	MountID   uuid.UUID
	ReadOnly  bool
	Epoch     uint64
	Now       time.Time
	ExpiresAt time.Time
}

type ListDisksQuery struct {
	PageSize  uint32
	PageToken string
}

type ListDisksPage struct {
	Disks         []DiskRecord
	NextPageToken string
}

type SegmentRange struct {
	First int64
	Last  int64
}

type Store interface {
	CreateDisk(ctx context.Context, diskID uuid.UUID, name string, maxBytes uint64) (DiskRecord, error)
	ForkDisk(ctx context.Context, arg ForkDiskInput) (DiskRecord, error)
	GetDisk(ctx context.Context, diskID uuid.UUID) (DiskRecord, error)
	ListDisks(ctx context.Context, query ListDisksQuery) (ListDisksPage, error)
	ListLineageCommits(ctx context.Context, lineageID uuid.UUID) ([]CommitRecord, error)
	UpdateDisk(ctx context.Context, input UpdateDiskInput) (DiskRecord, error)
	UpdateDiskUsage(ctx context.Context, diskID uuid.UUID, usedBytes, usedInodes uint64) error
	MergeDisk(ctx context.Context, input *MergeDiskInput) (DiskRecord, error)
	DeleteDisk(ctx context.Context, diskID uuid.UUID) ([]uuid.UUID, error)
	ReserveSegment(ctx context.Context, kind SegmentKind, lineageID uuid.UUID, minNext, count int64) (SegmentRange, error)
	CreateMountLease(ctx context.Context, arg CreateMountLeaseInput) (MountLeaseRecord, error)
	RenewMountLease(ctx context.Context, arg RenewMountLeaseInput) (MountLeaseRecord, error)
	DeleteMountLease(ctx context.Context, diskID, mountID uuid.UUID) (bool, error)
	DeleteDiskMountLeases(ctx context.Context, diskID uuid.UUID, now time.Time) (uint32, error)
	ListActiveMountLeases(ctx context.Context, diskID uuid.UUID, now time.Time) ([]MountLeaseRecord, error)
	PruneExpiredMountLeases(ctx context.Context, now time.Time) (int64, error)
}

var _ Store = (*SQLStore)(nil)

type SQLStore struct {
	queries *Queries
	tx      *tx.PGXTransactor[*Queries]
}

func NewStore(pool *pgxpool.Pool) Store {
	return &SQLStore{
		queries: New(pool),
		tx: tx.NewPGXTransactor(pool, func(rawTx pgx.Tx) *Queries {
			return New(rawTx)
		}),
	}
}

func toPGUUID(id uuid.UUID) pgtype.UUID {
	if id == uuid.Nil {
		return pgtype.UUID{}
	}
	return pgtype.UUID{Bytes: id, Valid: true}
}

func fromPGUUID(id pgtype.UUID) uuid.UUID {
	if !id.Valid {
		return uuid.Nil
	}
	return uuid.UUID(id.Bytes)
}

func toPGTime(t time.Time) pgtype.Timestamptz {
	return pgtype.Timestamptz{Time: t.UTC().Truncate(time.Microsecond), Valid: true}
}

func fromPGTime(t pgtype.Timestamptz) time.Time {
	return t.Time.UTC()
}

func toDiskRecord(d *Disk) DiskRecord {
	return DiskRecord{
		DiskID:       fromPGUUID(d.DiskID),
		ParentDiskID: fromPGUUID(d.ParentDiskID),
		LineageID:    fromPGUUID(d.LineageID),
		BaseCommitID: fromPGUUID(d.BaseCommitID),
		Name:         d.Name,
		MaxBytes:     uint64(max(0, d.MaxBytes)),
		UsedBytes:    uint64(max(0, d.UsedBytes)),
		UsedInodes:   uint64(max(0, d.UsedInodes)),
		CreatedAt:    d.CreatedAt.Time.UTC(),
		UpdatedAt:    d.UpdatedAt.Time.UTC(),
	}
}

func toCommitRecord(c *DiskCommit) CommitRecord {
	return CommitRecord{
		LineageID:           fromPGUUID(c.LineageID),
		CommitID:            fromPGUUID(c.CommitID),
		ParentCommitID:      fromPGUUID(c.ParentCommitID),
		MergeParentCommitID: fromPGUUID(c.MergeParentCommitID),
		BaseDiskID:          fromPGUUID(c.SourceDiskID),
		CreatedAt:           c.CreatedAt.Time.UTC(),
	}
}

func toMountLeaseRecord(m *DiskMount) MountLeaseRecord {
	return MountLeaseRecord{
		DiskID:    fromPGUUID(m.DiskID),
		MountID:   fromPGUUID(m.MountID),
		ReadOnly:  m.ReadOnly,
		Epoch:     uint64(max(0, m.Epoch)),
		ExpiresAt: fromPGTime(m.ExpiresAt),
	}
}

func validateForkDiskInput(arg ForkDiskInput) error {
	if arg.SourceDiskID == uuid.Nil || arg.ChildDiskID == uuid.Nil || arg.CommitID == uuid.Nil || arg.SourceDiskID == arg.ChildDiskID {
		return fmt.Errorf("%w: invalid fork disk identifiers", ErrInvalidArgument)
	}
	if arg.MaxBytes > math.MaxInt64 || arg.UsedBytes > math.MaxInt64 || arg.UsedInodes > math.MaxInt64 {
		return fmt.Errorf("%w: fork disk quota or usage overflows int64", ErrInvalidArgument)
	}
	if arg.UsedBytes > 0 && arg.UsedInodes == 0 {
		return fmt.Errorf("%w: fork disk used inodes must be >= 1 when used bytes > 0", ErrInvalidArgument)
	}
	return nil
}

func validateUpdateDiskInput(input UpdateDiskInput) error {
	if input.DiskID == uuid.Nil {
		return fmt.Errorf("%w: disk id cannot be nil uuid", ErrInvalidArgument)
	}
	if input.Name == nil && input.MaxBytes == nil {
		return fmt.Errorf("%w: update disk requires name or max_bytes", ErrInvalidArgument)
	}
	if input.MaxBytes != nil && (*input.MaxBytes == 0 || *input.MaxBytes > math.MaxInt64) {
		return fmt.Errorf("%w: max_bytes must be within [1, MaxInt64]", ErrInvalidArgument)
	}
	return nil
}

func validateUsageParams(diskID uuid.UUID, usedBytes, usedInodes uint64) error {
	if diskID == uuid.Nil {
		return fmt.Errorf("%w: disk id cannot be nil uuid", ErrInvalidArgument)
	}
	if usedBytes > math.MaxInt64 || usedInodes == 0 || usedInodes > math.MaxInt64 {
		return fmt.Errorf("%w: invalid disk usage values", ErrInvalidArgument)
	}
	return nil
}

func validateMergeDiskInput(input *MergeDiskInput) error {
	if input == nil {
		return fmt.Errorf("%w: merge disk input cannot be nil", ErrInvalidArgument)
	}
	if input.LineageID == uuid.Nil || input.SourceDiskID == uuid.Nil || input.TargetDiskID == uuid.Nil {
		return fmt.Errorf("%w: lineage, source and target disk ids cannot be nil uuid", ErrInvalidArgument)
	}
	if input.SourceDiskID == input.TargetDiskID {
		return fmt.Errorf("%w: source and target disk ids must differ", ErrInvalidArgument)
	}
	if input.ExpectedSourceBaseCommitID == uuid.Nil || input.ExpectedTargetBaseCommitID == uuid.Nil {
		return fmt.Errorf("%w: expected source and target base commit ids cannot be nil uuid", ErrInvalidArgument)
	}
	if input.NewMergeCommitID == uuid.Nil {
		return fmt.Errorf("%w: new merge commit id cannot be nil uuid", ErrInvalidArgument)
	}
	if !input.SkipSourceCommit {
		if input.NewSourceCommitID == uuid.Nil {
			return fmt.Errorf("%w: new source commit id cannot be nil uuid", ErrInvalidArgument)
		}
		if input.NewSourceCommitID == input.NewMergeCommitID {
			return fmt.Errorf("%w: new source and merge commit ids must differ", ErrInvalidArgument)
		}
	}
	if input.TargetMaxBytes > math.MaxInt64 || input.TargetUsedBytes > math.MaxInt64 || input.TargetUsedInodes > math.MaxInt64 {
		return fmt.Errorf("%w: merge target quota or usage overflows int64", ErrInvalidArgument)
	}
	if input.TargetUsedBytes > 0 && input.TargetUsedInodes == 0 {
		return fmt.Errorf("%w: merge target used inodes must be >= 1 when used bytes > 0", ErrInvalidArgument)
	}
	return nil
}

func validateMountLeaseParams(diskID, mountID uuid.UUID, readOnly bool, epoch uint64, now, expiresAt time.Time) (nowUTC, expUTC time.Time, err error) {
	if diskID == uuid.Nil || mountID == uuid.Nil {
		return time.Time{}, time.Time{}, fmt.Errorf("%w: disk id and mount id cannot be nil uuid", ErrInvalidArgument)
	}
	if readOnly && epoch != 0 {
		return time.Time{}, time.Time{}, fmt.Errorf("%w: read-only mount epoch must be 0", ErrInvalidArgument)
	}
	if !readOnly && (epoch == 0 || epoch > math.MaxInt64) {
		return time.Time{}, time.Time{}, fmt.Errorf("%w: read-write mount epoch must be within [1, MaxInt64]", ErrInvalidArgument)
	}
	if now.IsZero() || expiresAt.IsZero() {
		return time.Time{}, time.Time{}, fmt.Errorf("%w: now and expires_at must be non-zero", ErrInvalidArgument)
	}
	nowUTC = now.UTC().Truncate(time.Microsecond)
	expUTC = expiresAt.UTC().Truncate(time.Microsecond)
	if !expUTC.After(nowUTC) {
		return time.Time{}, time.Time{}, fmt.Errorf("%w: expires_at must be after now", ErrInvalidArgument)
	}
	return nowUTC, expUTC, nil
}

func mapPGXErr(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrNotFound
	}
	return err
}

func (s *SQLStore) CreateDisk(ctx context.Context, diskID uuid.UUID, name string, maxBytes uint64) (DiskRecord, error) {
	if diskID == uuid.Nil {
		return DiskRecord{}, fmt.Errorf("%w: disk id cannot be nil uuid", ErrInvalidArgument)
	}
	if maxBytes == 0 || maxBytes > math.MaxInt64 {
		return DiskRecord{}, fmt.Errorf("%w: max_bytes must be within [1, MaxInt64]", ErrInvalidArgument)
	}
	pgID := toPGUUID(diskID)
	var row Disk
	err := s.tx.ExecTxWithOptions(ctx, func(q *Queries) error {
		if _, linErr := q.CreateDiskLineage(ctx, pgID); linErr != nil {
			return linErr
		}
		created, createErr := q.CreateDisk(ctx, CreateDiskParams{
			DiskID:     pgID,
			Name:       name,
			LineageID:  pgID,
			MaxBytes:   int64(maxBytes),
			UsedBytes:  0,
			UsedInodes: 1,
		})
		if createErr != nil {
			return createErr
		}
		row = created
		return nil
	}, tx.WithDefaultRetry())
	if err != nil {
		return DiskRecord{}, mapPGXErr(err)
	}
	return toDiskRecord(&row), nil
}

func (s *SQLStore) ForkDisk(ctx context.Context, arg ForkDiskInput) (DiskRecord, error) {
	if err := validateForkDiskInput(arg); err != nil {
		return DiskRecord{}, err
	}
	pgSource := toPGUUID(arg.SourceDiskID)
	pgChild := toPGUUID(arg.ChildDiskID)
	pgCommit := toPGUUID(arg.CommitID)

	var childRow Disk
	err := s.tx.ExecTxWithOptions(ctx, func(q *Queries) error {
		srcSnapshot, getErr := q.GetDisk(ctx, pgSource)
		if getErr != nil {
			return mapPGXErr(getErr)
		}
		if _, lockErr := q.LockDiskLineage(ctx, srcSnapshot.LineageID); lockErr != nil {
			return mapPGXErr(lockErr)
		}
		lockedSource, lockSrcErr := q.GetDiskForUpdate(ctx, pgSource)
		if lockSrcErr != nil {
			return mapPGXErr(lockSrcErr)
		}
		if _, commitErr := q.CreateDiskCommit(ctx, CreateDiskCommitParams{
			CommitID:       pgCommit,
			LineageID:      lockedSource.LineageID,
			SourceDiskID:   pgSource,
			ParentCommitID: lockedSource.BaseCommitID,
		}); commitErr != nil {
			return commitErr
		}
		if setErr := q.SetDiskBaseCommit(ctx, SetDiskBaseCommitParams{
			DiskID:       pgSource,
			BaseCommitID: pgCommit,
		}); setErr != nil {
			return setErr
		}

		childMaxBytes := lockedSource.MaxBytes
		if arg.MaxBytes > 0 {
			childMaxBytes = int64(arg.MaxBytes)
		}
		childUsedBytes := lockedSource.UsedBytes
		childUsedInodes := lockedSource.UsedInodes
		if arg.UsedInodes > 0 {
			childUsedBytes = int64(arg.UsedBytes)
			childUsedInodes = int64(arg.UsedInodes)
		}

		created, createErr := q.CreateDisk(ctx, CreateDiskParams{
			DiskID:       pgChild,
			Name:         arg.Name,
			LineageID:    lockedSource.LineageID,
			ParentDiskID: pgSource,
			BaseCommitID: pgCommit,
			MaxBytes:     childMaxBytes,
			UsedBytes:    childUsedBytes,
			UsedInodes:   childUsedInodes,
		})
		if createErr != nil {
			return createErr
		}
		childRow = created
		return nil
	}, tx.WithDefaultRetry())
	if err != nil {
		return DiskRecord{}, mapPGXErr(err)
	}
	return toDiskRecord(&childRow), nil
}

func (s *SQLStore) GetDisk(ctx context.Context, diskID uuid.UUID) (DiskRecord, error) {
	if diskID == uuid.Nil {
		return DiskRecord{}, fmt.Errorf("%w: disk id cannot be nil uuid", ErrInvalidArgument)
	}
	row, err := s.queries.GetDisk(ctx, toPGUUID(diskID))
	if err != nil {
		return DiskRecord{}, mapPGXErr(err)
	}
	return toDiskRecord(&row), nil
}

func (s *SQLStore) ListDisks(ctx context.Context, query ListDisksQuery) (ListDisksPage, error) {
	pageSize := normalizePageSize(query.PageSize)

	var (
		hasCursor       bool
		cursorCreatedAt pgtype.Timestamptz
		cursorDiskID    pgtype.UUID
	)
	if query.PageToken != "" {
		ts, id, decErr := decodeDiskPageCursor(query.PageToken)
		if decErr != nil {
			return ListDisksPage{}, decErr
		}
		hasCursor = true
		cursorCreatedAt = pgtype.Timestamptz{Time: ts, Valid: true}
		cursorDiskID = toPGUUID(id)
	}

	rows, err := s.queries.ListDisks(ctx, ListDisksParams{
		HasCursor:       hasCursor,
		CursorCreatedAt: cursorCreatedAt,
		CursorDiskID:    cursorDiskID,
		PageLimit:       int32(pageSize + 1),
	})
	if err != nil {
		return ListDisksPage{}, err
	}

	var nextPageToken string
	if len(rows) > int(pageSize) {
		rows = rows[:pageSize]
		lastRow := rows[pageSize-1]
		nextPageToken = encodeDiskPageCursor(lastRow.CreatedAt.Time, fromPGUUID(lastRow.DiskID))
	}

	disks := make([]DiskRecord, 0, len(rows))
	for i := range rows {
		disks = append(disks, toDiskRecord(&rows[i]))
	}
	return ListDisksPage{
		Disks:         disks,
		NextPageToken: nextPageToken,
	}, nil
}

func (s *SQLStore) ListLineageCommits(ctx context.Context, lineageID uuid.UUID) ([]CommitRecord, error) {
	if lineageID == uuid.Nil {
		return nil, fmt.Errorf("%w: lineage id cannot be nil uuid", ErrInvalidArgument)
	}
	rows, err := s.queries.ListLineageCommits(ctx, toPGUUID(lineageID))
	if err != nil {
		return nil, mapPGXErr(err)
	}
	commits := make([]CommitRecord, 0, len(rows))
	for i := range rows {
		commits = append(commits, toCommitRecord(&rows[i]))
	}
	return commits, nil
}

func (s *SQLStore) UpdateDisk(ctx context.Context, input UpdateDiskInput) (DiskRecord, error) {
	if err := validateUpdateDiskInput(input); err != nil {
		return DiskRecord{}, err
	}
	params := UpdateDiskParams{
		DiskID: toPGUUID(input.DiskID),
	}
	if input.Name != nil {
		params.Name = pgtype.Text{String: *input.Name, Valid: true}
	}
	if input.MaxBytes != nil {
		params.MaxBytes = pgtype.Int8{Int64: int64(*input.MaxBytes), Valid: true}
	}
	var row Disk
	err := s.tx.ExecTxWithOptions(ctx, func(q *Queries) error {
		updated, updateErr := q.UpdateDisk(ctx, params)
		if updateErr != nil {
			return mapPGXErr(updateErr)
		}
		row = updated
		return nil
	}, tx.WithDefaultRetry())
	if err != nil {
		return DiskRecord{}, mapPGXErr(err)
	}
	return toDiskRecord(&row), nil
}

func (s *SQLStore) UpdateDiskUsage(ctx context.Context, diskID uuid.UUID, usedBytes, usedInodes uint64) error {
	if err := validateUsageParams(diskID, usedBytes, usedInodes); err != nil {
		return err
	}
	pgID := toPGUUID(diskID)
	err := tx.Retry(ctx, func(attemptCtx context.Context) error {
		affected, uErr := s.queries.UpdateDiskUsage(attemptCtx, UpdateDiskUsageParams{
			DiskID:     pgID,
			UsedBytes:  int64(usedBytes),
			UsedInodes: int64(usedInodes),
		})
		if uErr != nil {
			return uErr
		}
		if affected == 0 {
			return ErrNotFound
		}
		return nil
	}, tx.WithDefaultRetry())
	return mapPGXErr(err)
}

func (s *SQLStore) MergeDisk(ctx context.Context, input *MergeDiskInput) (DiskRecord, error) {
	if err := validateMergeDiskInput(input); err != nil {
		return DiskRecord{}, err
	}

	pgLineage := toPGUUID(input.LineageID)
	pgSource := toPGUUID(input.SourceDiskID)
	pgTarget := toPGUUID(input.TargetDiskID)

	var targetRow Disk
	err := s.tx.ExecTxWithOptions(ctx, func(q *Queries) error {
		if _, lockErr := q.LockDiskLineage(ctx, pgLineage); lockErr != nil {
			return mapPGXErr(lockErr)
		}

		firstID, secondID := pgSource, pgTarget
		sourceFirst := true
		if input.TargetDiskID.String() < input.SourceDiskID.String() {
			firstID, secondID = pgTarget, pgSource
			sourceFirst = false
		}

		firstRow, lockFirstErr := q.GetDiskForUpdate(ctx, firstID)
		if lockFirstErr != nil {
			return mapPGXErr(lockFirstErr)
		}
		secondRow, lockSecondErr := q.GetDiskForUpdate(ctx, secondID)
		if lockSecondErr != nil {
			return mapPGXErr(lockSecondErr)
		}

		sourceRow, curTargetRow := firstRow, secondRow
		if !sourceFirst {
			sourceRow, curTargetRow = secondRow, firstRow
		}

		if fromPGUUID(sourceRow.LineageID) != input.LineageID || fromPGUUID(curTargetRow.LineageID) != input.LineageID {
			return ErrCrossLineage
		}
		if fromPGUUID(sourceRow.BaseCommitID) != input.ExpectedSourceBaseCommitID ||
			fromPGUUID(curTargetRow.BaseCommitID) != input.ExpectedTargetBaseCommitID {
			return ErrStaleBaseCommit
		}

		srcFinalCommitID := input.ExpectedSourceBaseCommitID
		if !input.SkipSourceCommit {
			pgNewSrc := toPGUUID(input.NewSourceCommitID)
			if _, srcCommitErr := q.CreateDiskCommit(ctx, CreateDiskCommitParams{
				CommitID:       pgNewSrc,
				LineageID:      pgLineage,
				SourceDiskID:   pgSource,
				ParentCommitID: toPGUUID(input.ExpectedSourceBaseCommitID),
			}); srcCommitErr != nil {
				return srcCommitErr
			}
			if setSrcErr := q.SetDiskBaseCommit(ctx, SetDiskBaseCommitParams{
				DiskID:       pgSource,
				BaseCommitID: pgNewSrc,
			}); setSrcErr != nil {
				return setSrcErr
			}
			srcFinalCommitID = input.NewSourceCommitID
		}

		pgNewMerge := toPGUUID(input.NewMergeCommitID)
		if _, mergeCommitErr := q.CreateDiskCommit(ctx, CreateDiskCommitParams{
			CommitID:            pgNewMerge,
			LineageID:           pgLineage,
			SourceDiskID:        pgTarget,
			ParentCommitID:      toPGUUID(input.ExpectedTargetBaseCommitID),
			MergeParentCommitID: toPGUUID(srcFinalCommitID),
		}); mergeCommitErr != nil {
			return mergeCommitErr
		}

		touchParams := SetDiskBaseCommitAndTouchParams{
			DiskID:       pgTarget,
			BaseCommitID: pgNewMerge,
		}
		if input.TargetMaxBytes > 0 {
			touchParams.MaxBytes = pgtype.Int8{Int64: int64(input.TargetMaxBytes), Valid: true}
		}
		if input.TargetUsedInodes > 0 {
			touchParams.UsedBytes = pgtype.Int8{Int64: int64(input.TargetUsedBytes), Valid: true}
			touchParams.UsedInodes = pgtype.Int8{Int64: int64(input.TargetUsedInodes), Valid: true}
		}
		updatedTarget, touchErr := q.SetDiskBaseCommitAndTouch(ctx, touchParams)
		if touchErr != nil {
			return touchErr
		}
		targetRow = updatedTarget
		return nil
	}, tx.WithDefaultRetry())
	if err != nil {
		return DiskRecord{}, mapPGXErr(err)
	}
	return toDiskRecord(&targetRow), nil
}

func (s *SQLStore) DeleteDisk(ctx context.Context, diskID uuid.UUID) ([]uuid.UUID, error) {
	if diskID == uuid.Nil {
		return nil, fmt.Errorf("%w: disk id cannot be nil uuid", ErrInvalidArgument)
	}
	pgID := toPGUUID(diskID)

	var commitsToClean []uuid.UUID
	err := s.tx.ExecTxWithOptions(ctx, func(q *Queries) error {
		commitsToClean = nil

		diskRow, getErr := q.GetDisk(ctx, pgID)
		if getErr != nil {
			return mapPGXErr(getErr)
		}
		lineageID := diskRow.LineageID

		if _, lockErr := q.LockDiskLineage(ctx, lineageID); lockErr != nil {
			return mapPGXErr(lockErr)
		}
		if _, lockDiskErr := q.GetDiskForUpdate(ctx, pgID); lockDiskErr != nil {
			return mapPGXErr(lockDiskErr)
		}

		count, delErr := q.DeleteDisk(ctx, pgID)
		if delErr != nil {
			return delErr
		}
		if count == 0 {
			return ErrNotFound
		}

		remaining, cntErr := q.CountDisksByLineage(ctx, lineageID)
		if cntErr != nil {
			return cntErr
		}
		switch remaining {
		case 1:
			if clearErr := q.ClearLineageBaseCommits(ctx, lineageID); clearErr != nil {
				return clearErr
			}
			deletedCommits, cleanErr := q.DeleteLineageCommits(ctx, lineageID)
			if cleanErr != nil {
				return cleanErr
			}
			commitsToClean = toUUIDSlice(deletedCommits)
		case 0:
			deletedCommits, cleanErr := q.DeleteLineageCommits(ctx, lineageID)
			if cleanErr != nil {
				return cleanErr
			}
			commitsToClean = toUUIDSlice(deletedCommits)
			if delLinErr := q.DeleteDiskLineage(ctx, lineageID); delLinErr != nil {
				return delLinErr
			}
		}
		return nil
	}, tx.WithDefaultRetry())
	if err != nil {
		return nil, mapPGXErr(err)
	}
	return commitsToClean, nil
}

func toUUIDSlice(items []pgtype.UUID) []uuid.UUID {
	if len(items) == 0 {
		return nil
	}
	out := make([]uuid.UUID, 0, len(items))
	for _, item := range items {
		if id := fromPGUUID(item); id != uuid.Nil {
			out = append(out, id)
		}
	}
	return out
}

func (s *SQLStore) ReserveSegment(ctx context.Context, kind SegmentKind, lineageID uuid.UUID, minNext, count int64) (SegmentRange, error) {
	if lineageID == uuid.Nil || count <= 0 {
		return SegmentRange{}, fmt.Errorf("%w: invalid segment reserve parameters", ErrInvalidArgument)
	}
	if kind != SegmentKindInode && kind != SegmentKindPack {
		return SegmentRange{}, fmt.Errorf("%w: unknown segment kind %d", ErrInvalidArgument, kind)
	}

	pgLineage := toPGUUID(lineageID)
	var seg SegmentRange
	err := tx.Retry(ctx, func(attemptCtx context.Context) error {
		switch kind {
		case SegmentKindInode:
			row, rErr := s.queries.ReserveInodeSegment(attemptCtx, ReserveInodeSegmentParams{
				MinNext:   minNext,
				Count:     count,
				LineageID: pgLineage,
			})
			if rErr != nil {
				return rErr
			}
			seg = SegmentRange{First: row.FirstIno, Last: row.LastIno}
			return nil
		case SegmentKindPack:
			row, rErr := s.queries.ReservePackSegment(attemptCtx, ReservePackSegmentParams{
				MinNext:   minNext,
				Count:     count,
				LineageID: pgLineage,
			})
			if rErr != nil {
				return rErr
			}
			seg = SegmentRange{First: row.FirstPackID, Last: row.LastPackID}
			return nil
		default:
			return ErrInvalidArgument
		}
	}, tx.WithDefaultRetry())
	if err != nil {
		return SegmentRange{}, mapPGXErr(err)
	}
	return seg, nil
}

func (s *SQLStore) CreateMountLease(ctx context.Context, arg CreateMountLeaseInput) (MountLeaseRecord, error) {
	nowUTC, expUTC, err := validateMountLeaseParams(arg.DiskID, arg.MountID, arg.ReadOnly, arg.Epoch, arg.Now, arg.ExpiresAt)
	if err != nil {
		return MountLeaseRecord{}, err
	}
	pgDisk := toPGUUID(arg.DiskID)
	pgMount := toPGUUID(arg.MountID)
	pgNow := toPGTime(nowUTC)
	pgExp := toPGTime(expUTC)

	var row DiskMount
	err = s.tx.ExecTxWithOptions(ctx, func(q *Queries) error {
		if _, lockErr := q.GetDiskForUpdate(ctx, pgDisk); lockErr != nil {
			return mapPGXErr(lockErr)
		}
		if pruneErr := q.DeleteExpiredDiskMountLeases(ctx, DeleteExpiredDiskMountLeasesParams{
			DiskID:    pgDisk,
			ExpiresAt: pgNow,
		}); pruneErr != nil {
			return pruneErr
		}
		if !arg.ReadOnly {
			if supErr := q.DeleteSupersededRWMountLeases(ctx, DeleteSupersededRWMountLeasesParams{
				DiskID: pgDisk,
				Epoch:  int64(arg.Epoch),
			}); supErr != nil {
				return supErr
			}
		}
		created, createErr := q.CreateMountLease(ctx, CreateMountLeaseParams{
			DiskID:    pgDisk,
			MountID:   pgMount,
			ReadOnly:  arg.ReadOnly,
			Epoch:     int64(arg.Epoch),
			ExpiresAt: pgExp,
		})
		if createErr != nil {
			return createErr
		}
		row = created
		return nil
	}, tx.WithDefaultRetry())
	if err != nil {
		return MountLeaseRecord{}, mapPGXErr(err)
	}
	return toMountLeaseRecord(&row), nil
}

func (s *SQLStore) RenewMountLease(ctx context.Context, arg RenewMountLeaseInput) (MountLeaseRecord, error) {
	nowUTC, expUTC, err := validateMountLeaseParams(arg.DiskID, arg.MountID, arg.ReadOnly, arg.Epoch, arg.Now, arg.ExpiresAt)
	if err != nil {
		return MountLeaseRecord{}, err
	}
	pgDisk := toPGUUID(arg.DiskID)
	pgMount := toPGUUID(arg.MountID)
	pgNow := toPGTime(nowUTC)
	pgExp := toPGTime(expUTC)

	var row DiskMount
	err = s.tx.ExecTxWithOptions(ctx, func(q *Queries) error {
		existing, getErr := q.GetMountLeaseForUpdate(ctx, GetMountLeaseForUpdateParams{
			DiskID:  pgDisk,
			MountID: pgMount,
		})
		if getErr != nil {
			return mapPGXErr(getErr)
		}
		if !fromPGTime(existing.ExpiresAt).After(nowUTC) {
			if _, delErr := q.DeleteMountLease(ctx, DeleteMountLeaseParams{
				DiskID:  pgDisk,
				MountID: pgMount,
			}); delErr != nil {
				return delErr
			}
			return ErrNotFound
		}
		if existing.ReadOnly != arg.ReadOnly {
			return fmt.Errorf("%w: mount mode mismatch for mount_id %s", ErrInvalidArgument, arg.MountID)
		}
		if !arg.ReadOnly && uint64(max(0, existing.Epoch)) != arg.Epoch {
			return fmt.Errorf("%w: mount epoch %d != lease epoch %d", ErrStaleEpoch, arg.Epoch, existing.Epoch)
		}
		updated, renewErr := q.RenewMountLease(ctx, RenewMountLeaseParams{
			ExpiresAt: pgExp,
			DiskID:    pgDisk,
			MountID:   pgMount,
			ReadOnly:  arg.ReadOnly,
			Epoch:     int64(arg.Epoch),
			Now:       pgNow,
		})
		if renewErr != nil {
			return mapPGXErr(renewErr)
		}
		row = updated
		return nil
	}, tx.WithDefaultRetry())
	if err != nil {
		return MountLeaseRecord{}, mapPGXErr(err)
	}
	return toMountLeaseRecord(&row), nil
}

func (s *SQLStore) DeleteMountLease(ctx context.Context, diskID, mountID uuid.UUID) (bool, error) {
	if diskID == uuid.Nil || mountID == uuid.Nil {
		return false, fmt.Errorf("%w: disk id and mount id cannot be nil uuid", ErrInvalidArgument)
	}
	var affected int64
	err := tx.Retry(ctx, func(attemptCtx context.Context) error {
		n, delErr := s.queries.DeleteMountLease(attemptCtx, DeleteMountLeaseParams{
			DiskID:  toPGUUID(diskID),
			MountID: toPGUUID(mountID),
		})
		if delErr != nil {
			return delErr
		}
		affected = n
		return nil
	}, tx.WithDefaultRetry())
	if err != nil {
		return false, mapPGXErr(err)
	}
	return affected > 0, nil
}

func (s *SQLStore) DeleteDiskMountLeases(ctx context.Context, diskID uuid.UUID, now time.Time) (uint32, error) {
	if diskID == uuid.Nil || now.IsZero() {
		return 0, fmt.Errorf("%w: disk id and now are required", ErrInvalidArgument)
	}
	nowUTC := now.UTC().Truncate(time.Microsecond)
	var activeCount uint32
	err := tx.Retry(ctx, func(attemptCtx context.Context) error {
		expirations, delErr := s.queries.DeleteDiskMountLeases(attemptCtx, toPGUUID(diskID))
		if delErr != nil {
			return delErr
		}
		var count uint32
		for _, exp := range expirations {
			if fromPGTime(exp).After(nowUTC) {
				count++
			}
		}
		activeCount = count
		return nil
	}, tx.WithDefaultRetry())
	if err != nil {
		return 0, mapPGXErr(err)
	}
	return activeCount, nil
}

func (s *SQLStore) ListActiveMountLeases(ctx context.Context, diskID uuid.UUID, now time.Time) ([]MountLeaseRecord, error) {
	if diskID == uuid.Nil || now.IsZero() {
		return nil, fmt.Errorf("%w: disk id and now are required", ErrInvalidArgument)
	}
	rows, err := s.queries.ListActiveMountLeases(ctx, ListActiveMountLeasesParams{
		DiskID:    toPGUUID(diskID),
		ExpiresAt: toPGTime(now),
	})
	if err != nil {
		return nil, mapPGXErr(err)
	}
	out := make([]MountLeaseRecord, 0, len(rows))
	for i := range rows {
		out = append(out, toMountLeaseRecord(&rows[i]))
	}
	return out, nil
}

func (s *SQLStore) PruneExpiredMountLeases(ctx context.Context, now time.Time) (int64, error) {
	if now.IsZero() {
		return 0, fmt.Errorf("%w: now must be non-zero", ErrInvalidArgument)
	}
	var pruned int64
	err := tx.Retry(ctx, func(attemptCtx context.Context) error {
		n, delErr := s.queries.PruneExpiredMountLeases(attemptCtx, toPGTime(now))
		if delErr != nil {
			return delErr
		}
		pruned = n
		return nil
	}, tx.WithDefaultRetry())
	if err != nil {
		return 0, mapPGXErr(err)
	}
	return pruned, nil
}

func OpenPool(cfg config.DBPoolConfig) (*pgxpool.Pool, error) {
	pool, err := tx.OpenPool(cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to parse dbpool config: %w", err)
	}
	return pool, nil
}

type PoolParams struct {
	fx.In
	Ready     observability.Ready `optional:"true"`
	Lifecycle fx.Lifecycle
	Config    config.DBPoolConfig
}

func ProvidePool(params PoolParams) (*pgxpool.Pool, error) {
	pool, err := OpenPool(params.Config)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize dbpool: %w", err)
	}
	tx.RegisterPoolLifecycle(params.Lifecycle, pool)
	return pool, nil
}

var Module = fx.Module("db", fx.Provide(ProvidePool, NewStore))
