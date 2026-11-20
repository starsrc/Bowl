package db

import (
	"context"
	"errors"
	"fmt"
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

type ForkDiskInput struct {
	SourceDiskID uuid.UUID
	ChildDiskID  uuid.UUID
	CommitID     uuid.UUID
	Name         string
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
	CreateDisk(ctx context.Context, diskID uuid.UUID, name string) (DiskRecord, error)
	ForkDisk(ctx context.Context, arg ForkDiskInput) (DiskRecord, error)
	GetDisk(ctx context.Context, diskID uuid.UUID) (DiskRecord, error)
	ListDisks(ctx context.Context, query ListDisksQuery) (ListDisksPage, error)
	ListLineageCommits(ctx context.Context, lineageID uuid.UUID) ([]CommitRecord, error)
	UpdateDiskName(ctx context.Context, diskID uuid.UUID, name string) (DiskRecord, error)
	MergeDisk(ctx context.Context, input *MergeDiskInput) (DiskRecord, error)
	DeleteDisk(ctx context.Context, diskID uuid.UUID) ([]uuid.UUID, error)
	ReserveSegment(ctx context.Context, kind SegmentKind, lineageID uuid.UUID, minNext, count int64) (SegmentRange, error)
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

func toDiskRecord(d *Disk) DiskRecord {
	return DiskRecord{
		DiskID:       fromPGUUID(d.DiskID),
		ParentDiskID: fromPGUUID(d.ParentDiskID),
		LineageID:    fromPGUUID(d.LineageID),
		BaseCommitID: fromPGUUID(d.BaseCommitID),
		Name:         d.Name,
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
	return nil
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

func (s *SQLStore) CreateDisk(ctx context.Context, diskID uuid.UUID, name string) (DiskRecord, error) {
	if diskID == uuid.Nil {
		return DiskRecord{}, fmt.Errorf("%w: disk id cannot be nil uuid", ErrInvalidArgument)
	}
	pgID := toPGUUID(diskID)
	var row Disk
	err := s.tx.ExecTxWithOptions(ctx, func(q *Queries) error {
		if _, linErr := q.CreateDiskLineage(ctx, pgID); linErr != nil {
			return linErr
		}
		created, createErr := q.CreateDisk(ctx, CreateDiskParams{
			DiskID:    pgID,
			Name:      name,
			LineageID: pgID,
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
	if arg.SourceDiskID == uuid.Nil || arg.ChildDiskID == uuid.Nil || arg.CommitID == uuid.Nil || arg.SourceDiskID == arg.ChildDiskID {
		return DiskRecord{}, fmt.Errorf("%w: invalid fork disk identifiers", ErrInvalidArgument)
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
		created, createErr := q.CreateDisk(ctx, CreateDiskParams{
			DiskID:       pgChild,
			Name:         arg.Name,
			LineageID:    lockedSource.LineageID,
			ParentDiskID: pgSource,
			BaseCommitID: pgCommit,
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

func (s *SQLStore) UpdateDiskName(ctx context.Context, diskID uuid.UUID, name string) (DiskRecord, error) {
	if diskID == uuid.Nil {
		return DiskRecord{}, fmt.Errorf("%w: disk id cannot be nil uuid", ErrInvalidArgument)
	}
	pgID := toPGUUID(diskID)
	var row Disk
	err := s.tx.ExecTxWithOptions(ctx, func(q *Queries) error {
		updated, updateErr := q.UpdateDisk(ctx, UpdateDiskParams{
			DiskID: pgID,
			Name:   name,
		})
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

		updatedTarget, touchErr := q.SetDiskBaseCommitAndTouch(ctx, SetDiskBaseCommitAndTouchParams{
			DiskID:       pgTarget,
			BaseCommitID: pgNewMerge,
		})
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
