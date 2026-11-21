package db

import (
	"context"
	"errors"
	"fmt"
	"math"
	"slices"
	"strings"
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
	ErrNotFound          = errors.New("db: not found")
	ErrDiskNotFound      = ErrNotFound
	ErrInvalidArgument   = errors.New("db: invalid argument")
	ErrCrossLineage      = errors.New("db: cross lineage")
	ErrStaleBaseSnapshot = errors.New("db: stale base snapshot")
	ErrStaleEpoch        = errors.New("db: stale epoch")
	ErrSnapshotInUse     = errors.New("db: snapshot is protected by active lineage")
)

type SegmentKind uint8

const (
	SegmentKindInode SegmentKind = iota + 1
	SegmentKindPack
)

type SnapshotKind string

const (
	SnapshotKindFork   SnapshotKind = "fork"
	SnapshotKindMerge  SnapshotKind = "merge"
	SnapshotKindManual SnapshotKind = "manual"
)

type DiskRecord struct {
	DiskID         uuid.UUID
	ParentDiskID   uuid.UUID
	LineageID      uuid.UUID
	BaseSnapshotID uuid.UUID
	Name           string
	MaxBytes       uint64
	UsedBytes      uint64
	UsedInodes     uint64
	CreatedAt      time.Time
	UpdatedAt      time.Time
}

type SnapshotRecord struct {
	LineageID             uuid.UUID
	SnapshotID            uuid.UUID
	ParentSnapshotID      uuid.UUID
	MergeParentSnapshotID uuid.UUID
	SourceDiskID          uuid.UUID
	Kind                  SnapshotKind
	Name                  string
	Protected             bool
	CreatedAt             time.Time
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
	SnapshotID   uuid.UUID
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
	LineageID                    uuid.UUID
	SourceDiskID                 uuid.UUID
	TargetDiskID                 uuid.UUID
	ExpectedSourceBaseSnapshotID uuid.UUID
	ExpectedTargetBaseSnapshotID uuid.UUID
	NewSourceSnapshotID          uuid.UUID
	NewMergeSnapshotID           uuid.UUID
	SkipSourceSnapshot           bool
	FastForward                  bool
	TargetMaxBytes               uint64
	TargetUsedBytes              uint64
	TargetUsedInodes             uint64
}

type DeleteDiskResult struct {
	DeletedDiskIDs       []uuid.UUID
	ReclaimedSnapshotIDs []uuid.UUID
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

type CreateSnapshotInput struct {
	DiskID     uuid.UUID
	SnapshotID uuid.UUID
	Name       string
}

type ListSnapshotsQuery struct {
	DiskID    uuid.UUID
	Kind      SnapshotKind
	PageSize  uint32
	PageToken string
}

type ListSnapshotsPage struct {
	Snapshots     []SnapshotRecord
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
	ListLineageDisks(ctx context.Context, lineageID uuid.UUID) ([]DiskRecord, error)
	ListLineageSnapshots(ctx context.Context, lineageID uuid.UUID) ([]SnapshotRecord, error)
	CreateSnapshot(ctx context.Context, arg CreateSnapshotInput) (SnapshotRecord, error)
	ListSnapshots(ctx context.Context, query ListSnapshotsQuery) (ListSnapshotsPage, error)
	DeleteSnapshot(ctx context.Context, diskID, snapshotID uuid.UUID) (SnapshotRecord, error)
	PruneSnapshots(ctx context.Context, diskID uuid.UUID, dryRun bool) ([]SnapshotRecord, error)
	UpdateDisk(ctx context.Context, input UpdateDiskInput) (DiskRecord, error)
	UpdateDiskUsage(ctx context.Context, diskID uuid.UUID, usedBytes, usedInodes uint64) error
	MergeDisk(ctx context.Context, input *MergeDiskInput) (DiskRecord, error)
	DeleteDisk(ctx context.Context, diskID uuid.UUID, recursive bool) (DeleteDiskResult, error)
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

func toPGUUIDSlice(ids []uuid.UUID) []pgtype.UUID {
	out := make([]pgtype.UUID, 0, len(ids))
	for _, id := range ids {
		if id != uuid.Nil {
			out = append(out, toPGUUID(id))
		}
	}
	return out
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
		DiskID:         fromPGUUID(d.DiskID),
		ParentDiskID:   fromPGUUID(d.ParentDiskID),
		LineageID:      fromPGUUID(d.LineageID),
		BaseSnapshotID: fromPGUUID(d.BaseSnapshotID),
		Name:           d.Name,
		MaxBytes:       uint64(max(0, d.MaxBytes)),
		UsedBytes:      uint64(max(0, d.UsedBytes)),
		UsedInodes:     uint64(max(0, d.UsedInodes)),
		CreatedAt:      d.CreatedAt.Time.UTC(),
		UpdatedAt:      d.UpdatedAt.Time.UTC(),
	}
}

func toSnapshotRecord(s *DiskSnapshot) SnapshotRecord {
	return SnapshotRecord{
		LineageID:             fromPGUUID(s.LineageID),
		SnapshotID:            fromPGUUID(s.SnapshotID),
		ParentSnapshotID:      fromPGUUID(s.ParentSnapshotID),
		MergeParentSnapshotID: fromPGUUID(s.MergeParentSnapshotID),
		SourceDiskID:          fromPGUUID(s.SourceDiskID),
		Kind:                  SnapshotKind(s.Kind),
		Name:                  s.Name,
		CreatedAt:             s.CreatedAt.Time.UTC(),
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
	if arg.SourceDiskID == uuid.Nil || arg.ChildDiskID == uuid.Nil || arg.SnapshotID == uuid.Nil || arg.SourceDiskID == arg.ChildDiskID {
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
	if input.ExpectedSourceBaseSnapshotID == uuid.Nil || input.ExpectedTargetBaseSnapshotID == uuid.Nil {
		return fmt.Errorf("%w: expected source and target base snapshot ids cannot be nil uuid", ErrInvalidArgument)
	}
	if input.FastForward {
		if input.SkipSourceSnapshot {
			if input.NewSourceSnapshotID != uuid.Nil {
				return fmt.Errorf("%w: fast-forward with skip source snapshot must not pass new source snapshot id", ErrInvalidArgument)
			}
			if input.NewMergeSnapshotID != input.ExpectedSourceBaseSnapshotID {
				return fmt.Errorf("%w: fast-forward reuse requires new merge snapshot id equal expected source base snapshot id", ErrInvalidArgument)
			}
		} else {
			if input.NewSourceSnapshotID == uuid.Nil {
				return fmt.Errorf("%w: fast-forward requires new source snapshot id", ErrInvalidArgument)
			}
			if input.NewMergeSnapshotID != input.NewSourceSnapshotID {
				return fmt.Errorf("%w: fast-forward requires new merge snapshot id equal new source snapshot id", ErrInvalidArgument)
			}
		}
	} else {
		if input.NewMergeSnapshotID == uuid.Nil {
			return fmt.Errorf("%w: new merge snapshot id cannot be nil uuid", ErrInvalidArgument)
		}
		if !input.SkipSourceSnapshot {
			if input.NewSourceSnapshotID == uuid.Nil {
				return fmt.Errorf("%w: new source snapshot id cannot be nil uuid", ErrInvalidArgument)
			}
			if input.NewSourceSnapshotID == input.NewMergeSnapshotID {
				return fmt.Errorf("%w: new source and merge snapshot ids must differ", ErrInvalidArgument)
			}
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

func collectSubtreeDiskIDs(disks []DiskRecord, rootID uuid.UUID, recursive bool) ([]uuid.UUID, bool) {
	foundRoot := false
	childrenByParent := make(map[uuid.UUID][]uuid.UUID)
	for i := range disks {
		d := disks[i]
		if d.DiskID == rootID {
			foundRoot = true
		}
		if recursive && d.ParentDiskID != uuid.Nil {
			childrenByParent[d.ParentDiskID] = append(childrenByParent[d.ParentDiskID], d.DiskID)
		}
	}
	if !foundRoot {
		return nil, false
	}
	if !recursive {
		return []uuid.UUID{rootID}, true
	}

	visited := map[uuid.UUID]struct{}{rootID: {}}
	queue := []uuid.UUID{rootID}
	out := []uuid.UUID{rootID}
	for len(queue) > 0 {
		cur := queue[0]
		queue = queue[1:]
		for _, childID := range childrenByParent[cur] {
			if _, seen := visited[childID]; seen {
				continue
			}
			visited[childID] = struct{}{}
			out = append(out, childID)
			queue = append(queue, childID)
		}
	}
	slices.SortFunc(out, func(a, b uuid.UUID) int {
		return strings.Compare(a.String(), b.String())
	})
	return out, true
}

func computeReclaimableSnapshots(survivingDisks []DiskRecord, snapshots []SnapshotRecord) (toDelete []uuid.UUID, remainingSnapshots int) {
	if len(snapshots) == 0 {
		return nil, 0
	}

	survivingDiskSet := make(map[uuid.UUID]struct{}, len(survivingDisks))
	for i := range survivingDisks {
		survivingDiskSet[survivingDisks[i].DiskID] = struct{}{}
	}

	if len(survivingDisks) >= 2 {
		byID := make(map[uuid.UUID]SnapshotRecord, len(snapshots))
		for i := range snapshots {
			byID[snapshots[i].SnapshotID] = snapshots[i]
		}
		reachable := make(map[uuid.UUID]struct{})
		queue := make([]uuid.UUID, 0, len(survivingDisks))
		for i := range survivingDisks {
			if baseID := survivingDisks[i].BaseSnapshotID; baseID != uuid.Nil {
				queue = append(queue, baseID)
			}
		}
		for len(queue) > 0 {
			cur := queue[0]
			queue = queue[1:]
			if cur == uuid.Nil {
				continue
			}
			if _, seen := reachable[cur]; seen {
				continue
			}
			rec, ok := byID[cur]
			if !ok {
				continue
			}
			reachable[cur] = struct{}{}
			if rec.ParentSnapshotID != uuid.Nil {
				queue = append(queue, rec.ParentSnapshotID)
			}
			if rec.MergeParentSnapshotID != uuid.Nil {
				queue = append(queue, rec.MergeParentSnapshotID)
			}
		}
		for i := range snapshots {
			snap := snapshots[i]
			if snap.Kind == SnapshotKindManual {
				if _, owned := survivingDiskSet[snap.SourceDiskID]; owned {
					remainingSnapshots++
					continue
				}
				toDelete = append(toDelete, snap.SnapshotID)
				continue
			}
			if _, ok := reachable[snap.SnapshotID]; ok {
				remainingSnapshots++
				continue
			}
			toDelete = append(toDelete, snap.SnapshotID)
		}
		return toDelete, remainingSnapshots
	}

	for i := range snapshots {
		snap := snapshots[i]
		if snap.Kind == SnapshotKindManual {
			if _, owned := survivingDiskSet[snap.SourceDiskID]; owned {
				remainingSnapshots++
				continue
			}
		}
		toDelete = append(toDelete, snap.SnapshotID)
	}
	return toDelete, remainingSnapshots
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
	pgSnapshot := toPGUUID(arg.SnapshotID)

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
		if _, snapErr := q.CreateDiskSnapshot(ctx, CreateDiskSnapshotParams{
			SnapshotID:       pgSnapshot,
			LineageID:        lockedSource.LineageID,
			SourceDiskID:     pgSource,
			ParentSnapshotID: lockedSource.BaseSnapshotID,
			Kind:             string(SnapshotKindFork),
		}); snapErr != nil {
			return snapErr
		}
		if setErr := q.SetDiskBaseSnapshot(ctx, SetDiskBaseSnapshotParams{
			DiskID:         pgSource,
			BaseSnapshotID: pgSnapshot,
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
			DiskID:         pgChild,
			Name:           arg.Name,
			LineageID:      lockedSource.LineageID,
			ParentDiskID:   pgSource,
			BaseSnapshotID: pgSnapshot,
			MaxBytes:       childMaxBytes,
			UsedBytes:      childUsedBytes,
			UsedInodes:     childUsedInodes,
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

func (s *SQLStore) ListLineageDisks(ctx context.Context, lineageID uuid.UUID) ([]DiskRecord, error) {
	if lineageID == uuid.Nil {
		return nil, fmt.Errorf("%w: lineage id cannot be nil uuid", ErrInvalidArgument)
	}
	rows, err := s.queries.ListLineageDisks(ctx, toPGUUID(lineageID))
	if err != nil {
		return nil, mapPGXErr(err)
	}
	disks := make([]DiskRecord, 0, len(rows))
	for i := range rows {
		disks = append(disks, toDiskRecord(&rows[i]))
	}
	return disks, nil
}

func (s *SQLStore) ListLineageSnapshots(ctx context.Context, lineageID uuid.UUID) ([]SnapshotRecord, error) {
	if lineageID == uuid.Nil {
		return nil, fmt.Errorf("%w: lineage id cannot be nil uuid", ErrInvalidArgument)
	}
	rows, err := s.queries.ListLineageSnapshots(ctx, toPGUUID(lineageID))
	if err != nil {
		return nil, mapPGXErr(err)
	}
	snapshots := make([]SnapshotRecord, 0, len(rows))
	for i := range rows {
		snapshots = append(snapshots, toSnapshotRecord(&rows[i]))
	}
	return snapshots, nil
}

func (s *SQLStore) CreateSnapshot(ctx context.Context, arg CreateSnapshotInput) (SnapshotRecord, error) {
	if arg.DiskID == uuid.Nil || arg.SnapshotID == uuid.Nil {
		return SnapshotRecord{}, fmt.Errorf("%w: disk id and snapshot id cannot be nil uuid", ErrInvalidArgument)
	}
	pgDisk := toPGUUID(arg.DiskID)
	pgSnap := toPGUUID(arg.SnapshotID)

	var created DiskSnapshot
	err := s.tx.ExecTxWithOptions(ctx, func(q *Queries) error {
		diskRow, getErr := q.GetDisk(ctx, pgDisk)
		if getErr != nil {
			return mapPGXErr(getErr)
		}
		if _, lockErr := q.LockDiskLineage(ctx, diskRow.LineageID); lockErr != nil {
			return mapPGXErr(lockErr)
		}
		lockedDisk, lockDiskErr := q.GetDiskForUpdate(ctx, pgDisk)
		if lockDiskErr != nil {
			return mapPGXErr(lockDiskErr)
		}
		row, snapErr := q.CreateDiskSnapshot(ctx, CreateDiskSnapshotParams{
			SnapshotID:       pgSnap,
			LineageID:        lockedDisk.LineageID,
			SourceDiskID:     pgDisk,
			ParentSnapshotID: lockedDisk.BaseSnapshotID,
			Kind:             string(SnapshotKindManual),
			Name:             arg.Name,
		})
		if snapErr != nil {
			return snapErr
		}
		created = row
		return nil
	}, tx.WithDefaultRetry())
	if err != nil {
		return SnapshotRecord{}, mapPGXErr(err)
	}
	return toSnapshotRecord(&created), nil
}

func (s *SQLStore) ListSnapshots(ctx context.Context, query ListSnapshotsQuery) (ListSnapshotsPage, error) {
	if query.DiskID == uuid.Nil {
		return ListSnapshotsPage{}, fmt.Errorf("%w: disk id cannot be nil uuid", ErrInvalidArgument)
	}
	if err := validateSnapshotKindFilter(query.Kind); err != nil {
		return ListSnapshotsPage{}, err
	}
	pageSize, err := normalizeSnapshotPageSize(query.PageSize)
	if err != nil {
		return ListSnapshotsPage{}, err
	}

	var (
		hasCursor    bool
		cursorTime   time.Time
		cursorSnapID uuid.UUID
	)
	if query.PageToken != "" {
		ts, id, decErr := decodeSnapshotPageCursor(query.PageToken)
		if decErr != nil {
			return ListSnapshotsPage{}, decErr
		}
		hasCursor = true
		cursorTime = ts
		cursorSnapID = id
	}

	pgDisk := toPGUUID(query.DiskID)
	var page ListSnapshotsPage
	err = s.tx.ExecTxWithOptions(ctx, func(q *Queries) error {
		diskRow, getErr := q.GetDisk(ctx, pgDisk)
		if getErr != nil {
			return mapPGXErr(getErr)
		}
		diskRows, disksErr := q.ListLineageDisks(ctx, diskRow.LineageID)
		if disksErr != nil {
			return mapPGXErr(disksErr)
		}
		snapRows, snapsErr := q.ListLineageSnapshots(ctx, diskRow.LineageID)
		if snapsErr != nil {
			return mapPGXErr(snapsErr)
		}

		disks := make([]DiskRecord, 0, len(diskRows))
		for i := range diskRows {
			disks = append(disks, toDiskRecord(&diskRows[i]))
		}
		snaps := make([]SnapshotRecord, 0, len(snapRows))
		for i := range snapRows {
			snaps = append(snaps, toSnapshotRecord(&snapRows[i]))
		}
		page = buildListSnapshotsPage(disks, snaps, query.Kind, pageSize, hasCursor, cursorTime, cursorSnapID)
		return nil
	}, tx.WithDefaultRetry())
	if err != nil {
		return ListSnapshotsPage{}, mapPGXErr(err)
	}
	return page, nil
}

func (s *SQLStore) DeleteSnapshot(ctx context.Context, diskID, snapshotID uuid.UUID) (SnapshotRecord, error) {
	if diskID == uuid.Nil || snapshotID == uuid.Nil {
		return SnapshotRecord{}, fmt.Errorf("%w: disk id and snapshot id cannot be nil uuid", ErrInvalidArgument)
	}
	pgDisk := toPGUUID(diskID)
	pgSnap := toPGUUID(snapshotID)

	var deleted SnapshotRecord
	err := s.tx.ExecTxWithOptions(ctx, func(q *Queries) error {
		diskRow, getErr := q.GetDisk(ctx, pgDisk)
		if getErr != nil {
			return mapPGXErr(getErr)
		}
		lineageID := diskRow.LineageID

		if _, lockErr := q.LockDiskLineage(ctx, lineageID); lockErr != nil {
			return mapPGXErr(lockErr)
		}
		lockedDisks, lockDisksErr := q.ListLineageDisksForUpdate(ctx, lineageID)
		if lockDisksErr != nil {
			return mapPGXErr(lockDisksErr)
		}
		diskRecords := make([]DiskRecord, 0, len(lockedDisks))
		diskPresent := false
		for i := range lockedDisks {
			rec := toDiskRecord(&lockedDisks[i])
			if rec.DiskID == diskID {
				diskPresent = true
			}
			diskRecords = append(diskRecords, rec)
		}
		if !diskPresent {
			return ErrNotFound
		}

		targetRow, snapErr := q.GetSnapshotByID(ctx, pgSnap)
		if snapErr != nil {
			return mapPGXErr(snapErr)
		}
		if fromPGUUID(targetRow.LineageID) != fromPGUUID(lineageID) {
			return ErrCrossLineage
		}

		snapRows, listErr := q.ListLineageSnapshots(ctx, lineageID)
		if listErr != nil {
			return listErr
		}
		snapRecords := make([]SnapshotRecord, 0, len(snapRows))
		for i := range snapRows {
			snapRecords = append(snapRecords, toSnapshotRecord(&snapRows[i]))
		}

		hull := ComputeActiveLCAHull(diskRecords, snapRecords)
		if _, protected := hull[snapshotID]; protected {
			return ErrSnapshotInUse
		}

		pgSnapIDs := []pgtype.UUID{pgSnap}
		if clearErr := q.ClearSnapshotParentByIDs(ctx, ClearSnapshotParentByIDsParams{
			LineageID:   lineageID,
			SnapshotIds: pgSnapIDs,
		}); clearErr != nil {
			return clearErr
		}
		if clearErr := q.ClearSnapshotMergeParentByIDs(ctx, ClearSnapshotMergeParentByIDsParams{
			LineageID:   lineageID,
			SnapshotIds: pgSnapIDs,
		}); clearErr != nil {
			return clearErr
		}
		deletedIDs, delErr := q.DeleteSnapshotsByIDs(ctx, DeleteSnapshotsByIDsParams{
			LineageID:   lineageID,
			SnapshotIds: pgSnapIDs,
		})
		if delErr != nil {
			return delErr
		}
		if len(deletedIDs) == 0 {
			return ErrNotFound
		}

		deleted = toSnapshotRecord(&targetRow)
		deleted.Protected = false
		return nil
	}, tx.WithDefaultRetry())
	if err != nil {
		return SnapshotRecord{}, mapPGXErr(err)
	}
	return deleted, nil
}

func (s *SQLStore) PruneSnapshots(ctx context.Context, diskID uuid.UUID, dryRun bool) ([]SnapshotRecord, error) {
	if diskID == uuid.Nil {
		return nil, fmt.Errorf("%w: disk id cannot be nil uuid", ErrInvalidArgument)
	}
	pgDisk := toPGUUID(diskID)

	var result []SnapshotRecord
	err := s.tx.ExecTxWithOptions(ctx, func(q *Queries) error {
		result = nil
		diskRow, getErr := q.GetDisk(ctx, pgDisk)
		if getErr != nil {
			return mapPGXErr(getErr)
		}
		lineageID := diskRow.LineageID

		if _, lockErr := q.LockDiskLineage(ctx, lineageID); lockErr != nil {
			return mapPGXErr(lockErr)
		}
		lockedDisks, lockDisksErr := q.ListLineageDisksForUpdate(ctx, lineageID)
		if lockDisksErr != nil {
			return mapPGXErr(lockDisksErr)
		}
		diskRecords := make([]DiskRecord, 0, len(lockedDisks))
		diskPresent := false
		for i := range lockedDisks {
			rec := toDiskRecord(&lockedDisks[i])
			if rec.DiskID == diskID {
				diskPresent = true
			}
			diskRecords = append(diskRecords, rec)
		}
		if !diskPresent {
			return ErrNotFound
		}

		snapRows, listErr := q.ListLineageSnapshots(ctx, lineageID)
		if listErr != nil {
			return listErr
		}
		snapRecords := make([]SnapshotRecord, 0, len(snapRows))
		for i := range snapRows {
			snapRecords = append(snapRecords, toSnapshotRecord(&snapRows[i]))
		}

		candidates := selectPrunableSnapshots(diskRecords, snapRecords)
		if dryRun || len(candidates) == 0 {
			result = candidates
			return nil
		}

		ids := make([]uuid.UUID, 0, len(candidates))
		for i := range candidates {
			ids = append(ids, candidates[i].SnapshotID)
		}
		pgSnapIDs := toPGUUIDSlice(ids)
		if clearErr := q.ClearSnapshotParentByIDs(ctx, ClearSnapshotParentByIDsParams{
			LineageID:   lineageID,
			SnapshotIds: pgSnapIDs,
		}); clearErr != nil {
			return clearErr
		}
		if clearErr := q.ClearSnapshotMergeParentByIDs(ctx, ClearSnapshotMergeParentByIDsParams{
			LineageID:   lineageID,
			SnapshotIds: pgSnapIDs,
		}); clearErr != nil {
			return clearErr
		}
		if _, delErr := q.DeleteSnapshotsByIDs(ctx, DeleteSnapshotsByIDsParams{
			LineageID:   lineageID,
			SnapshotIds: pgSnapIDs,
		}); delErr != nil {
			return delErr
		}

		result = candidates
		return nil
	}, tx.WithDefaultRetry())
	if err != nil {
		return nil, mapPGXErr(err)
	}
	return result, nil
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
		if fromPGUUID(sourceRow.BaseSnapshotID) != input.ExpectedSourceBaseSnapshotID ||
			fromPGUUID(curTargetRow.BaseSnapshotID) != input.ExpectedTargetBaseSnapshotID {
			return ErrStaleBaseSnapshot
		}

		var pgTargetBase pgtype.UUID
		if input.FastForward {
			pgTargetBase = toPGUUID(input.NewMergeSnapshotID)
			if !input.SkipSourceSnapshot {
				if _, srcSnapErr := q.CreateDiskSnapshot(ctx, CreateDiskSnapshotParams{
					SnapshotID:       pgTargetBase,
					LineageID:        pgLineage,
					SourceDiskID:     pgSource,
					ParentSnapshotID: toPGUUID(input.ExpectedSourceBaseSnapshotID),
					Kind:             string(SnapshotKindMerge),
				}); srcSnapErr != nil {
					return srcSnapErr
				}
				if setSrcErr := q.SetDiskBaseSnapshot(ctx, SetDiskBaseSnapshotParams{
					DiskID:         pgSource,
					BaseSnapshotID: pgTargetBase,
				}); setSrcErr != nil {
					return setSrcErr
				}
			}
		} else {
			srcFinalSnapshotID := input.ExpectedSourceBaseSnapshotID
			if !input.SkipSourceSnapshot {
				pgNewSrc := toPGUUID(input.NewSourceSnapshotID)
				if _, srcSnapErr := q.CreateDiskSnapshot(ctx, CreateDiskSnapshotParams{
					SnapshotID:       pgNewSrc,
					LineageID:        pgLineage,
					SourceDiskID:     pgSource,
					ParentSnapshotID: toPGUUID(input.ExpectedSourceBaseSnapshotID),
					Kind:             string(SnapshotKindMerge),
				}); srcSnapErr != nil {
					return srcSnapErr
				}
				if setSrcErr := q.SetDiskBaseSnapshot(ctx, SetDiskBaseSnapshotParams{
					DiskID:         pgSource,
					BaseSnapshotID: pgNewSrc,
				}); setSrcErr != nil {
					return setSrcErr
				}
				srcFinalSnapshotID = input.NewSourceSnapshotID
			}

			pgTargetBase = toPGUUID(input.NewMergeSnapshotID)
			if _, mergeSnapErr := q.CreateDiskSnapshot(ctx, CreateDiskSnapshotParams{
				SnapshotID:            pgTargetBase,
				LineageID:             pgLineage,
				SourceDiskID:          pgTarget,
				ParentSnapshotID:      toPGUUID(input.ExpectedTargetBaseSnapshotID),
				MergeParentSnapshotID: toPGUUID(srcFinalSnapshotID),
				Kind:                  string(SnapshotKindMerge),
			}); mergeSnapErr != nil {
				return mergeSnapErr
			}
		}

		touchParams := SetDiskBaseSnapshotAndTouchParams{
			DiskID:         pgTarget,
			BaseSnapshotID: pgTargetBase,
		}
		if input.TargetMaxBytes > 0 {
			touchParams.MaxBytes = pgtype.Int8{Int64: int64(input.TargetMaxBytes), Valid: true}
		}
		if input.TargetUsedInodes > 0 {
			touchParams.UsedBytes = pgtype.Int8{Int64: int64(input.TargetUsedBytes), Valid: true}
			touchParams.UsedInodes = pgtype.Int8{Int64: int64(input.TargetUsedInodes), Valid: true}
		}
		updatedTarget, touchErr := q.SetDiskBaseSnapshotAndTouch(ctx, touchParams)
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

func (s *SQLStore) DeleteDisk(ctx context.Context, diskID uuid.UUID, recursive bool) (DeleteDiskResult, error) {
	if diskID == uuid.Nil {
		return DeleteDiskResult{}, fmt.Errorf("%w: disk id cannot be nil uuid", ErrInvalidArgument)
	}
	pgID := toPGUUID(diskID)

	var result DeleteDiskResult
	err := s.tx.ExecTxWithOptions(ctx, func(q *Queries) error {
		result = DeleteDiskResult{}

		diskRow, getErr := q.GetDisk(ctx, pgID)
		if getErr != nil {
			return mapPGXErr(getErr)
		}
		lineageID := diskRow.LineageID

		if _, lockErr := q.LockDiskLineage(ctx, lineageID); lockErr != nil {
			return mapPGXErr(lockErr)
		}
		lockedDisks, lockDisksErr := q.ListLineageDisksForUpdate(ctx, lineageID)
		if lockDisksErr != nil {
			return mapPGXErr(lockDisksErr)
		}
		lineageRecords := make([]DiskRecord, 0, len(lockedDisks))
		for i := range lockedDisks {
			lineageRecords = append(lineageRecords, toDiskRecord(&lockedDisks[i]))
		}

		toDeleteDiskIDs, found := collectSubtreeDiskIDs(lineageRecords, diskID, recursive)
		if !found {
			return ErrNotFound
		}

		deletedCount, delErr := q.DeleteDisksByIDs(ctx, DeleteDisksByIDsParams{
			LineageID: lineageID,
			DiskIds:   toPGUUIDSlice(toDeleteDiskIDs),
		})
		if delErr != nil {
			return delErr
		}
		if deletedCount == 0 {
			return ErrNotFound
		}

		deletedSet := make(map[uuid.UUID]struct{}, len(toDeleteDiskIDs))
		for _, id := range toDeleteDiskIDs {
			deletedSet[id] = struct{}{}
		}
		survivingDisks := make([]DiskRecord, 0, len(lineageRecords)-len(toDeleteDiskIDs))
		for i := range lineageRecords {
			if _, removed := deletedSet[lineageRecords[i].DiskID]; !removed {
				survivingDisks = append(survivingDisks, lineageRecords[i])
			}
		}

		snapRows, listErr := q.ListLineageSnapshots(ctx, lineageID)
		if listErr != nil {
			return listErr
		}
		snapRecords := make([]SnapshotRecord, 0, len(snapRows))
		for i := range snapRows {
			snapRecords = append(snapRecords, toSnapshotRecord(&snapRows[i]))
		}

		toDeleteSnapIDs, remainingSnaps := computeReclaimableSnapshots(survivingDisks, snapRecords)
		var reclaimed []uuid.UUID
		if len(toDeleteSnapIDs) > 0 {
			pgSnapIDs := toPGUUIDSlice(toDeleteSnapIDs)
			if clearErr := q.ClearDisksBaseSnapshotByIDs(ctx, ClearDisksBaseSnapshotByIDsParams{
				LineageID:   lineageID,
				SnapshotIds: pgSnapIDs,
			}); clearErr != nil {
				return clearErr
			}
			if clearErr := q.ClearSnapshotParentByIDs(ctx, ClearSnapshotParentByIDsParams{
				LineageID:   lineageID,
				SnapshotIds: pgSnapIDs,
			}); clearErr != nil {
				return clearErr
			}
			if clearErr := q.ClearSnapshotMergeParentByIDs(ctx, ClearSnapshotMergeParentByIDsParams{
				LineageID:   lineageID,
				SnapshotIds: pgSnapIDs,
			}); clearErr != nil {
				return clearErr
			}
			deletedSnaps, cleanErr := q.DeleteSnapshotsByIDs(ctx, DeleteSnapshotsByIDsParams{
				LineageID:   lineageID,
				SnapshotIds: pgSnapIDs,
			})
			if cleanErr != nil {
				return cleanErr
			}
			reclaimed = toUUIDSlice(deletedSnaps)
		}

		if len(survivingDisks) == 0 && remainingSnaps == 0 {
			if delLinErr := q.DeleteDiskLineage(ctx, lineageID); delLinErr != nil {
				return delLinErr
			}
		}

		result = DeleteDiskResult{
			DeletedDiskIDs:       toDeleteDiskIDs,
			ReclaimedSnapshotIDs: reclaimed,
		}
		return nil
	}, tx.WithDefaultRetry())
	if err != nil {
		return DeleteDiskResult{}, mapPGXErr(err)
	}
	return result, nil
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
