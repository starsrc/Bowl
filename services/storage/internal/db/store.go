// Package db 封装 Storage 控制面 PostgreSQL 连接池、事务与领域持久化接缝。
package db

import (
	"context"
	"errors"
	"fmt"
	"slices"
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

// 领域哨兵错误（吞并底层驱动细节，避免 pgx.ErrNoRows 泄露至调用方）。
var (
	ErrNotFound        = errors.New("db: not found")
	ErrDiskNotFound    = ErrNotFound
	ErrInvalidArgument = errors.New("db: invalid argument")
	ErrCrossLineage    = errors.New("db: cross lineage")
	ErrStaleBaseCommit = errors.New("db: stale base commit")
)

// SegmentKind 标识血缘单调号段资源类别。
type SegmentKind uint8

// 血缘单调号段资源类别枚举。
const (
	SegmentKindInode SegmentKind = iota + 1
	SegmentKindPack
)

// DiskRecord 描述控制面持久化的单盘领域记录。
type DiskRecord struct {
	DiskID       uuid.UUID
	ParentDiskID uuid.UUID
	LineageID    uuid.UUID
	BaseCommitID uuid.UUID
	Name         string
	CreatedAt    time.Time
	UpdatedAt    time.Time
}

// CommitRecord 描述血缘 DAG 中的不可变切面提交记录。
type CommitRecord struct {
	LineageID           uuid.UUID
	CommitID            uuid.UUID
	ParentCommitID      uuid.UUID
	MergeParentCommitID uuid.UUID
	BaseDiskID          uuid.UUID
	CreatedAt           time.Time
}

// ForkDiskInput 描述派生子盘事务入参（64B，满足 gocritic hugeParam <= 80B）。
type ForkDiskInput struct {
	SourceDiskID uuid.UUID
	ChildDiskID  uuid.UUID
	CommitID     uuid.UUID
	Name         string
}

// MergeDiskInput 描述原子合并事务入参（按指针传递以满足 gocritic hugeParam <= 80B）。
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

// ListDisksQuery 描述游标分页查询参数。
type ListDisksQuery struct {
	PageSize  uint32
	PageToken string
}

// ListDisksPage 描述游标分页查询结果。
type ListDisksPage struct {
	Disks         []DiskRecord
	NextPageToken string
}

// SegmentRange 描述号段分配闭区间 [First, Last]。
type SegmentRange struct {
	First int64
	Last  int64
}

// Store 定义控制面粗粒度持久化接缝接口。
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

const defaultRollbackTimeout = 5 * time.Second

// SQLStore 提供 PostgreSQL 控制面持久化适配器。
type SQLStore struct {
	queries   *Queries
	beginner  tx.Beginner
	pgxTxOpts pgx.TxOptions
	retryOpts []tx.Option
}

// NewStore 基于连接池创建控制面访问器。
func NewStore(pool *pgxpool.Pool) Store {
	return newSQLStore(pool, pool, tx.WithDefaultRetry())
}

func newSQLStore(dbtx DBTX, beginner tx.Beginner, opts ...tx.Option) *SQLStore {
	options := tx.ApplyOptions(opts...)
	return &SQLStore{
		queries:   New(dbtx),
		beginner:  beginner,
		pgxTxOpts: tx.ToPGXTxOptions(options),
		retryOpts: slices.Clone(opts),
	}
}

func (s *SQLStore) execQuery(ctx context.Context, fn func(attemptCtx context.Context, q *Queries) error) error {
	return tx.Retry(ctx, func(attemptCtx context.Context) error {
		return fn(attemptCtx, s.queries)
	}, s.retryOpts...)
}

func (s *SQLStore) execTx(ctx context.Context, fn func(attemptCtx context.Context, q *Queries) error) error {
	return tx.Retry(ctx, func(attemptCtx context.Context) error {
		return s.execSingleTx(attemptCtx, fn)
	}, s.retryOpts...)
}

func (s *SQLStore) execSingleTx(attemptCtx context.Context, fn func(attemptCtx context.Context, q *Queries) error) (txErr error) {
	rawTx, err := s.beginner.BeginTx(attemptCtx, s.pgxTxOpts)
	if err != nil {
		return err
	}

	defer func() {
		if p := recover(); p != nil {
			_ = rollbackTx(attemptCtx, rawTx)
			panic(p)
		}

		if txErr != nil {
			if rbErr := rollbackTx(attemptCtx, rawTx); rbErr != nil && !errors.Is(rbErr, pgx.ErrTxClosed) {
				txErr = errors.Join(txErr, rbErr)
			}
			return
		}

		if commitErr := rawTx.Commit(attemptCtx); commitErr != nil {
			txErr = commitErr
		}
	}()

	txErr = fn(attemptCtx, New(rawTx))
	return txErr
}

func rollbackTx(ctx context.Context, rawTx pgx.Tx) error {
	rollbackCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), defaultRollbackTimeout)
	defer cancel()
	return rawTx.Rollback(rollbackCtx)
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

// CreateDisk 原子写入新血缘行与根磁盘记录。
func (s *SQLStore) CreateDisk(ctx context.Context, diskID uuid.UUID, name string) (DiskRecord, error) {
	if diskID == uuid.Nil {
		return DiskRecord{}, fmt.Errorf("%w: disk id cannot be nil uuid", ErrInvalidArgument)
	}
	pgID := toPGUUID(diskID)
	var row Disk
	err := s.execTx(ctx, func(attemptCtx context.Context, q *Queries) error {
		if _, linErr := q.CreateDiskLineage(attemptCtx, pgID); linErr != nil {
			return linErr
		}
		created, createErr := q.CreateDisk(attemptCtx, CreateDiskParams{
			DiskID:    pgID,
			Name:      name,
			LineageID: pgID,
		})
		if createErr != nil {
			return createErr
		}
		row = created
		return nil
	})
	if err != nil {
		return DiskRecord{}, mapPGXErr(err)
	}
	return toDiskRecord(&row), nil
}

// ForkDisk 在单次可重试事务内按 disk_lineages -> disks 锁序派生子盘并写入基准切面。
func (s *SQLStore) ForkDisk(ctx context.Context, arg ForkDiskInput) (DiskRecord, error) {
	if arg.SourceDiskID == uuid.Nil || arg.ChildDiskID == uuid.Nil || arg.CommitID == uuid.Nil || arg.SourceDiskID == arg.ChildDiskID {
		return DiskRecord{}, fmt.Errorf("%w: invalid fork disk identifiers", ErrInvalidArgument)
	}
	pgSource := toPGUUID(arg.SourceDiskID)
	pgChild := toPGUUID(arg.ChildDiskID)
	pgCommit := toPGUUID(arg.CommitID)

	var childRow Disk
	err := s.execTx(ctx, func(attemptCtx context.Context, q *Queries) error {
		srcSnapshot, getErr := q.GetDisk(attemptCtx, pgSource)
		if getErr != nil {
			return getErr
		}
		if _, lockErr := q.LockDiskLineage(attemptCtx, srcSnapshot.LineageID); lockErr != nil {
			return lockErr
		}
		lockedSource, lockSrcErr := q.GetDiskForUpdate(attemptCtx, pgSource)
		if lockSrcErr != nil {
			return lockSrcErr
		}
		if _, commitErr := q.CreateDiskCommit(attemptCtx, CreateDiskCommitParams{
			CommitID:       pgCommit,
			LineageID:      lockedSource.LineageID,
			SourceDiskID:   pgSource,
			ParentCommitID: lockedSource.BaseCommitID,
		}); commitErr != nil {
			return commitErr
		}
		if setErr := q.SetDiskBaseCommit(attemptCtx, SetDiskBaseCommitParams{
			DiskID:       pgSource,
			BaseCommitID: pgCommit,
		}); setErr != nil {
			return setErr
		}
		created, createErr := q.CreateDisk(attemptCtx, CreateDiskParams{
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
	})
	if err != nil {
		return DiskRecord{}, mapPGXErr(err)
	}
	return toDiskRecord(&childRow), nil
}

// GetDisk 按标识查询单盘元数据。
func (s *SQLStore) GetDisk(ctx context.Context, diskID uuid.UUID) (DiskRecord, error) {
	if diskID == uuid.Nil {
		return DiskRecord{}, fmt.Errorf("%w: disk id cannot be nil uuid", ErrInvalidArgument)
	}
	pgID := toPGUUID(diskID)
	var row Disk
	err := s.execQuery(ctx, func(attemptCtx context.Context, q *Queries) error {
		got, getErr := q.GetDisk(attemptCtx, pgID)
		if getErr != nil {
			return getErr
		}
		row = got
		return nil
	})
	if err != nil {
		return DiskRecord{}, mapPGXErr(err)
	}
	return toDiskRecord(&row), nil
}

// ListDisks 按 (created_at DESC, disk_id DESC) 稳定游标分页查询磁盘。
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

	var rows []Disk
	err := s.execQuery(ctx, func(attemptCtx context.Context, q *Queries) error {
		got, listErr := q.ListDisks(attemptCtx, ListDisksParams{
			HasCursor:       hasCursor,
			CursorCreatedAt: cursorCreatedAt,
			CursorDiskID:    cursorDiskID,
			PageLimit:       int32(pageSize + 1),
		})
		if listErr != nil {
			return listErr
		}
		rows = got
		return nil
	})
	if err != nil {
		return ListDisksPage{}, mapPGXErr(err)
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

// ListLineageCommits 按 (created_at ASC, commit_id ASC) 查询指定血缘的全部切面节点。
func (s *SQLStore) ListLineageCommits(ctx context.Context, lineageID uuid.UUID) ([]CommitRecord, error) {
	if lineageID == uuid.Nil {
		return nil, fmt.Errorf("%w: lineage id cannot be nil uuid", ErrInvalidArgument)
	}
	pgLineage := toPGUUID(lineageID)
	var rows []DiskCommit
	err := s.execQuery(ctx, func(attemptCtx context.Context, q *Queries) error {
		got, listErr := q.ListLineageCommits(attemptCtx, pgLineage)
		if listErr != nil {
			return listErr
		}
		rows = got
		return nil
	})
	if err != nil {
		return nil, mapPGXErr(err)
	}
	commits := make([]CommitRecord, 0, len(rows))
	for i := range rows {
		commits = append(commits, toCommitRecord(&rows[i]))
	}
	return commits, nil
}

// UpdateDiskName 更新磁盘名称。
func (s *SQLStore) UpdateDiskName(ctx context.Context, diskID uuid.UUID, name string) (DiskRecord, error) {
	if diskID == uuid.Nil {
		return DiskRecord{}, fmt.Errorf("%w: disk id cannot be nil uuid", ErrInvalidArgument)
	}
	pgID := toPGUUID(diskID)
	var row Disk
	err := s.execQuery(ctx, func(attemptCtx context.Context, q *Queries) error {
		updated, updateErr := q.UpdateDisk(attemptCtx, UpdateDiskParams{
			DiskID: pgID,
			Name:   name,
		})
		if updateErr != nil {
			return updateErr
		}
		row = updated
		return nil
	})
	if err != nil {
		return DiskRecord{}, mapPGXErr(err)
	}
	return toDiskRecord(&row), nil
}

// MergeDisk 在单次可重试事务内按严格锁序与 CAS 切面校验原子提交合并节点。
func (s *SQLStore) MergeDisk(ctx context.Context, input *MergeDiskInput) (DiskRecord, error) {
	if err := validateMergeDiskInput(input); err != nil {
		return DiskRecord{}, err
	}

	pgLineage := toPGUUID(input.LineageID)
	pgSource := toPGUUID(input.SourceDiskID)
	pgTarget := toPGUUID(input.TargetDiskID)

	var targetRow Disk
	err := s.execTx(ctx, func(attemptCtx context.Context, q *Queries) error {
		if _, lockErr := q.LockDiskLineage(attemptCtx, pgLineage); lockErr != nil {
			return lockErr
		}

		firstID, secondID := pgSource, pgTarget
		sourceFirst := true
		if input.TargetDiskID.String() < input.SourceDiskID.String() {
			firstID, secondID = pgTarget, pgSource
			sourceFirst = false
		}

		firstRow, lockFirstErr := q.GetDiskForUpdate(attemptCtx, firstID)
		if lockFirstErr != nil {
			return lockFirstErr
		}
		secondRow, lockSecondErr := q.GetDiskForUpdate(attemptCtx, secondID)
		if lockSecondErr != nil {
			return lockSecondErr
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
			if _, srcCommitErr := q.CreateDiskCommit(attemptCtx, CreateDiskCommitParams{
				CommitID:       pgNewSrc,
				LineageID:      pgLineage,
				SourceDiskID:   pgSource,
				ParentCommitID: toPGUUID(input.ExpectedSourceBaseCommitID),
			}); srcCommitErr != nil {
				return srcCommitErr
			}
			if setSrcErr := q.SetDiskBaseCommit(attemptCtx, SetDiskBaseCommitParams{
				DiskID:       pgSource,
				BaseCommitID: pgNewSrc,
			}); setSrcErr != nil {
				return setSrcErr
			}
			srcFinalCommitID = input.NewSourceCommitID
		}

		pgNewMerge := toPGUUID(input.NewMergeCommitID)
		if _, mergeCommitErr := q.CreateDiskCommit(attemptCtx, CreateDiskCommitParams{
			CommitID:            pgNewMerge,
			LineageID:           pgLineage,
			SourceDiskID:        pgTarget,
			ParentCommitID:      toPGUUID(input.ExpectedTargetBaseCommitID),
			MergeParentCommitID: toPGUUID(srcFinalCommitID),
		}); mergeCommitErr != nil {
			return mergeCommitErr
		}

		updatedTarget, touchErr := q.SetDiskBaseCommitAndTouch(attemptCtx, SetDiskBaseCommitAndTouchParams{
			DiskID:       pgTarget,
			BaseCommitID: pgNewMerge,
		})
		if touchErr != nil {
			return touchErr
		}
		targetRow = updatedTarget
		return nil
	})
	if err != nil {
		return DiskRecord{}, mapPGXErr(err)
	}
	return toDiskRecord(&targetRow), nil
}

// DeleteDisk 在单次可重试事务内按 disk_lineages -> disks 锁序删盘并执行三态血缘切面回收。
func (s *SQLStore) DeleteDisk(ctx context.Context, diskID uuid.UUID) ([]uuid.UUID, error) {
	if diskID == uuid.Nil {
		return nil, fmt.Errorf("%w: disk id cannot be nil uuid", ErrInvalidArgument)
	}
	pgID := toPGUUID(diskID)

	var commitsToClean []uuid.UUID
	err := s.execTx(ctx, func(attemptCtx context.Context, q *Queries) error {
		commitsToClean = nil

		diskRow, getErr := q.GetDisk(attemptCtx, pgID)
		if getErr != nil {
			return getErr
		}
		lineageID := diskRow.LineageID

		// 统一遵守 disk_lineages -> disks 的全局加锁顺序，消除并发 ForkDisk 与 DeleteDisk 死锁风险。
		if _, lockErr := q.LockDiskLineage(attemptCtx, lineageID); lockErr != nil {
			return lockErr
		}
		if _, lockDiskErr := q.GetDiskForUpdate(attemptCtx, pgID); lockDiskErr != nil {
			return lockDiskErr
		}

		count, delErr := q.DeleteDisk(attemptCtx, pgID)
		if delErr != nil {
			return delErr
		}
		if count == 0 {
			return ErrNotFound
		}

		remaining, cntErr := q.CountDisksByLineage(attemptCtx, lineageID)
		if cntErr != nil {
			return cntErr
		}
		switch remaining {
		case 1:
			if clearErr := q.ClearLineageBaseCommits(attemptCtx, lineageID); clearErr != nil {
				return clearErr
			}
			deletedCommits, cleanErr := q.DeleteLineageCommits(attemptCtx, lineageID)
			if cleanErr != nil {
				return cleanErr
			}
			commitsToClean = toUUIDSlice(deletedCommits)
		case 0:
			deletedCommits, cleanErr := q.DeleteLineageCommits(attemptCtx, lineageID)
			if cleanErr != nil {
				return cleanErr
			}
			commitsToClean = toUUIDSlice(deletedCommits)
			if delLinErr := q.DeleteDiskLineage(attemptCtx, lineageID); delLinErr != nil {
				return delLinErr
			}
		}
		return nil
	})
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

// ReserveSegment 为指定血缘预留单调递增的 Inode 或 Pack 号段。
func (s *SQLStore) ReserveSegment(ctx context.Context, kind SegmentKind, lineageID uuid.UUID, minNext, count int64) (SegmentRange, error) {
	if lineageID == uuid.Nil || count <= 0 {
		return SegmentRange{}, fmt.Errorf("%w: invalid segment reserve parameters", ErrInvalidArgument)
	}
	if kind != SegmentKindInode && kind != SegmentKindPack {
		return SegmentRange{}, fmt.Errorf("%w: unknown segment kind %d", ErrInvalidArgument, kind)
	}

	pgLineage := toPGUUID(lineageID)
	var seg SegmentRange
	err := s.execQuery(ctx, func(attemptCtx context.Context, q *Queries) error {
		switch kind {
		case SegmentKindInode:
			row, rErr := q.ReserveInodeSegment(attemptCtx, ReserveInodeSegmentParams{
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
			row, rErr := q.ReservePackSegment(attemptCtx, ReservePackSegmentParams{
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
	})
	if err != nil {
		return SegmentRange{}, mapPGXErr(err)
	}
	return seg, nil
}

// OpenPool 根据配置创建连接池。
func OpenPool(cfg config.DBPoolConfig) (*pgxpool.Pool, error) {
	pool, err := tx.OpenPool(cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to parse dbpool config: %w", err)
	}
	return pool, nil
}

// PoolParams 包含连接池所需依赖。
type PoolParams struct {
	fx.In
	Ready     observability.Ready `optional:"true"`
	Lifecycle fx.Lifecycle
	Config    config.DBPoolConfig
}

// ProvidePool 创建连接池并注册生命周期。
func ProvidePool(params PoolParams) (*pgxpool.Pool, error) {
	pool, err := OpenPool(params.Config)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize dbpool: %w", err)
	}
	tx.RegisterPoolLifecycle(params.Lifecycle, pool)
	return pool, nil
}

// Module 提供控制面数据库依赖。
var Module = fx.Module("db", fx.Provide(ProvidePool, NewStore))
