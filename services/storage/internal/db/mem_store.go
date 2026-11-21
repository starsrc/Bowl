package db

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"maps"
	"math"
	"slices"
	"sync"
	"time"

	"github.com/google/uuid"
)

var _ Store = (*MemStore)(nil)

type MemDiskView struct {
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

func (v *MemDiskView) toRecord() DiskRecord {
	return DiskRecord{
		DiskID:         v.DiskID,
		ParentDiskID:   v.ParentDiskID,
		LineageID:      v.LineageID,
		BaseSnapshotID: v.BaseSnapshotID,
		Name:           v.Name,
		MaxBytes:       v.MaxBytes,
		UsedBytes:      v.UsedBytes,
		UsedInodes:     v.UsedInodes,
		CreatedAt:      v.CreatedAt,
		UpdatedAt:      v.UpdatedAt,
	}
}

type MemLineageView struct {
	LineageID  uuid.UUID
	NextIno    int64
	NextPackID int64
	CreatedAt  time.Time
}

type MemSnapshotView struct {
	SnapshotID            uuid.UUID
	LineageID             uuid.UUID
	SourceDiskID          uuid.UUID
	ParentSnapshotID      uuid.UUID
	MergeParentSnapshotID uuid.UUID
	Kind                  SnapshotKind
	Name                  string
	CreatedAt             time.Time
}

func (v *MemSnapshotView) toRecord() SnapshotRecord {
	return SnapshotRecord{
		LineageID:             v.LineageID,
		SnapshotID:            v.SnapshotID,
		ParentSnapshotID:      v.ParentSnapshotID,
		MergeParentSnapshotID: v.MergeParentSnapshotID,
		SourceDiskID:          v.SourceDiskID,
		Kind:                  v.Kind,
		Name:                  v.Name,
		CreatedAt:             v.CreatedAt,
	}
}

type MemState struct {
	Disks             map[uuid.UUID]MemDiskView
	Lineages          map[uuid.UUID]MemLineageView
	Snapshots         map[uuid.UUID]MemSnapshotView
	Mounts            map[uuid.UUID]map[uuid.UUID]MountLeaseRecord
	InodeReserveCalls int
	PackReserveCalls  int
	UpdateUsageCalls  int
}

type MemStore struct {
	mu                sync.Mutex
	disks             map[uuid.UUID]MemDiskView
	lineages          map[uuid.UUID]MemLineageView
	snapshots         map[uuid.UUID]MemSnapshotView
	mounts            map[uuid.UUID]map[uuid.UUID]MountLeaseRecord
	lastTS            time.Time
	failNextWrite     error
	inodeReserveCalls int
	packReserveCalls  int
	updateUsageCalls  int
}

func NewMemStore() *MemStore {
	return &MemStore{
		disks:     make(map[uuid.UUID]MemDiskView),
		lineages:  make(map[uuid.UUID]MemLineageView),
		snapshots: make(map[uuid.UUID]MemSnapshotView),
		mounts:    make(map[uuid.UUID]map[uuid.UUID]MountLeaseRecord),
	}
}

func (m *MemStore) SetFailNextWrite(err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.failNextWrite = err
}

func (m *MemStore) SeedSnapshot(rec SnapshotRecord, setBaseOnDiskID uuid.UUID) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if rec.SnapshotID == uuid.Nil || rec.LineageID == uuid.Nil || rec.SourceDiskID == uuid.Nil {
		return ErrInvalidArgument
	}
	if _, linOK := m.lineages[rec.LineageID]; !linOK {
		return ErrNotFound
	}
	if _, dup := m.snapshots[rec.SnapshotID]; dup {
		return errors.New("db: snapshot already exists")
	}
	now := rec.CreatedAt
	if now.IsZero() {
		now = m.nextNowLocked()
	}
	m.snapshots[rec.SnapshotID] = MemSnapshotView{
		SnapshotID:            rec.SnapshotID,
		LineageID:             rec.LineageID,
		SourceDiskID:          rec.SourceDiskID,
		ParentSnapshotID:      rec.ParentSnapshotID,
		MergeParentSnapshotID: rec.MergeParentSnapshotID,
		Kind:                  rec.Kind,
		Name:                  rec.Name,
		CreatedAt:             now,
	}
	if setBaseOnDiskID != uuid.Nil {
		d, ok := m.disks[setBaseOnDiskID]
		if !ok {
			return ErrNotFound
		}
		d.BaseSnapshotID = rec.SnapshotID
		m.disks[setBaseOnDiskID] = d
	}
	return nil
}

func (m *MemStore) Inspect() MemState {
	m.mu.Lock()
	defer m.mu.Unlock()
	mountsCopy := make(map[uuid.UUID]map[uuid.UUID]MountLeaseRecord, len(m.mounts))
	for diskID, diskMounts := range m.mounts {
		mountsCopy[diskID] = maps.Clone(diskMounts)
	}
	return MemState{
		Disks:             maps.Clone(m.disks),
		Lineages:          maps.Clone(m.lineages),
		Snapshots:         maps.Clone(m.snapshots),
		Mounts:            mountsCopy,
		InodeReserveCalls: m.inodeReserveCalls,
		PackReserveCalls:  m.packReserveCalls,
		UpdateUsageCalls:  m.updateUsageCalls,
	}
}

func (m *MemStore) consumeFailNextWriteLocked() error {
	if m.failNextWrite == nil {
		return nil
	}
	err := m.failNextWrite
	m.failNextWrite = nil
	return err
}

func nowMicroUTC() time.Time {
	return time.Now().UTC().Truncate(time.Microsecond)
}

func (m *MemStore) nextNowLocked() time.Time {
	now := nowMicroUTC()
	if !now.After(m.lastTS) {
		now = m.lastTS.Add(time.Microsecond)
	}
	m.lastTS = now
	return now
}

func (m *MemStore) CreateDisk(ctx context.Context, diskID uuid.UUID, name string, maxBytes uint64) (DiskRecord, error) {
	if err := ctx.Err(); err != nil {
		return DiskRecord{}, err
	}
	if diskID == uuid.Nil {
		return DiskRecord{}, fmt.Errorf("%w: disk id cannot be nil uuid", ErrInvalidArgument)
	}
	if maxBytes == 0 || maxBytes > math.MaxInt64 {
		return DiskRecord{}, fmt.Errorf("%w: max_bytes must be within [1, MaxInt64]", ErrInvalidArgument)
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.consumeFailNextWriteLocked(); err != nil {
		return DiskRecord{}, err
	}
	if _, exists := m.disks[diskID]; exists {
		return DiskRecord{}, errors.New("db: disk already exists")
	}

	now := m.nextNowLocked()
	m.lineages[diskID] = MemLineageView{
		LineageID:  diskID,
		NextIno:    2,
		NextPackID: 1,
		CreatedAt:  now,
	}
	row := MemDiskView{
		DiskID:     diskID,
		LineageID:  diskID,
		Name:       name,
		MaxBytes:   maxBytes,
		UsedBytes:  0,
		UsedInodes: 1,
		CreatedAt:  now,
		UpdatedAt:  now,
	}
	m.disks[diskID] = row
	return row.toRecord(), nil
}

func (m *MemStore) ForkDisk(ctx context.Context, arg ForkDiskInput) (DiskRecord, error) {
	if err := ctx.Err(); err != nil {
		return DiskRecord{}, err
	}
	if err := validateForkDiskInput(arg); err != nil {
		return DiskRecord{}, err
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.consumeFailNextWriteLocked(); err != nil {
		return DiskRecord{}, err
	}

	srcRow, ok := m.disks[arg.SourceDiskID]
	if !ok {
		return DiskRecord{}, ErrNotFound
	}
	if _, linOK := m.lineages[srcRow.LineageID]; !linOK {
		return DiskRecord{}, ErrNotFound
	}
	if _, childExists := m.disks[arg.ChildDiskID]; childExists {
		return DiskRecord{}, errors.New("db: child disk already exists")
	}
	if _, snapExists := m.snapshots[arg.SnapshotID]; snapExists {
		return DiskRecord{}, errors.New("db: snapshot already exists")
	}

	childMaxBytes := srcRow.MaxBytes
	if arg.MaxBytes > 0 {
		childMaxBytes = arg.MaxBytes
	}
	childUsedBytes := srcRow.UsedBytes
	childUsedInodes := srcRow.UsedInodes
	if arg.UsedInodes > 0 {
		childUsedBytes = arg.UsedBytes
		childUsedInodes = arg.UsedInodes
	}

	now := m.nextNowLocked()
	m.snapshots[arg.SnapshotID] = MemSnapshotView{
		SnapshotID:       arg.SnapshotID,
		LineageID:        srcRow.LineageID,
		SourceDiskID:     arg.SourceDiskID,
		ParentSnapshotID: srcRow.BaseSnapshotID,
		Kind:             SnapshotKindFork,
		CreatedAt:        now,
	}
	srcRow.BaseSnapshotID = arg.SnapshotID
	m.disks[arg.SourceDiskID] = srcRow

	childRow := MemDiskView{
		DiskID:         arg.ChildDiskID,
		ParentDiskID:   arg.SourceDiskID,
		LineageID:      srcRow.LineageID,
		BaseSnapshotID: arg.SnapshotID,
		Name:           arg.Name,
		MaxBytes:       childMaxBytes,
		UsedBytes:      childUsedBytes,
		UsedInodes:     childUsedInodes,
		CreatedAt:      now,
		UpdatedAt:      now,
	}
	m.disks[arg.ChildDiskID] = childRow
	return childRow.toRecord(), nil
}

func (m *MemStore) GetDisk(ctx context.Context, diskID uuid.UUID) (DiskRecord, error) {
	if err := ctx.Err(); err != nil {
		return DiskRecord{}, err
	}
	if diskID == uuid.Nil {
		return DiskRecord{}, fmt.Errorf("%w: disk id cannot be nil uuid", ErrInvalidArgument)
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	row, ok := m.disks[diskID]
	if !ok {
		return DiskRecord{}, ErrNotFound
	}
	return row.toRecord(), nil
}

func (m *MemStore) ListDisks(ctx context.Context, query ListDisksQuery) (ListDisksPage, error) {
	if err := ctx.Err(); err != nil {
		return ListDisksPage{}, err
	}
	pageSize := normalizePageSize(query.PageSize)

	var (
		hasCursor    bool
		cursorTime   time.Time
		cursorDiskID uuid.UUID
	)
	if query.PageToken != "" {
		ts, id, decErr := decodeDiskPageCursor(query.PageToken)
		if decErr != nil {
			return ListDisksPage{}, decErr
		}
		hasCursor = true
		cursorTime = ts
		cursorDiskID = id
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	ordered := make([]MemDiskView, 0, len(m.disks))
	for id := range m.disks {
		row := m.disks[id]
		if hasCursor {
			beforeTime := row.CreatedAt.Before(cursorTime)
			sameTimeSmallerID := row.CreatedAt.Equal(cursorTime) && bytes.Compare(row.DiskID[:], cursorDiskID[:]) < 0
			if !beforeTime && !sameTimeSmallerID {
				continue
			}
		}
		ordered = append(ordered, row)
	}

	slices.SortFunc(ordered, func(a, b MemDiskView) int {
		if cmp := b.CreatedAt.Compare(a.CreatedAt); cmp != 0 {
			return cmp
		}
		return bytes.Compare(b.DiskID[:], a.DiskID[:])
	})

	var nextPageToken string
	if len(ordered) > int(pageSize) {
		ordered = ordered[:pageSize]
		lastRow := ordered[pageSize-1]
		nextPageToken = encodeDiskPageCursor(lastRow.CreatedAt, lastRow.DiskID)
	}

	disks := make([]DiskRecord, 0, len(ordered))
	for i := range ordered {
		disks = append(disks, ordered[i].toRecord())
	}
	return ListDisksPage{
		Disks:         disks,
		NextPageToken: nextPageToken,
	}, nil
}

func (m *MemStore) ListLineageDisks(ctx context.Context, lineageID uuid.UUID) ([]DiskRecord, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if lineageID == uuid.Nil {
		return nil, fmt.Errorf("%w: lineage id cannot be nil uuid", ErrInvalidArgument)
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	ordered := make([]MemDiskView, 0)
	for id := range m.disks {
		d := m.disks[id]
		if d.LineageID == lineageID {
			ordered = append(ordered, d)
		}
	}
	slices.SortFunc(ordered, func(a, b MemDiskView) int {
		return bytes.Compare(a.DiskID[:], b.DiskID[:])
	})

	disks := make([]DiskRecord, 0, len(ordered))
	for i := range ordered {
		disks = append(disks, ordered[i].toRecord())
	}
	return disks, nil
}

func (m *MemStore) ListLineageSnapshots(ctx context.Context, lineageID uuid.UUID) ([]SnapshotRecord, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if lineageID == uuid.Nil {
		return nil, fmt.Errorf("%w: lineage id cannot be nil uuid", ErrInvalidArgument)
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	ordered := make([]MemSnapshotView, 0)
	for sid := range m.snapshots {
		s := m.snapshots[sid]
		if s.LineageID == lineageID {
			ordered = append(ordered, s)
		}
	}
	slices.SortFunc(ordered, func(a, b MemSnapshotView) int {
		if cmp := a.CreatedAt.Compare(b.CreatedAt); cmp != 0 {
			return cmp
		}
		return bytes.Compare(a.SnapshotID[:], b.SnapshotID[:])
	})

	snapshots := make([]SnapshotRecord, 0, len(ordered))
	for i := range ordered {
		snapshots = append(snapshots, ordered[i].toRecord())
	}
	return snapshots, nil
}

func (m *MemStore) collectLineageStateLocked(lineageID uuid.UUID) ([]DiskRecord, []SnapshotRecord) {
	disks := make([]DiskRecord, 0)
	for id := range m.disks {
		d := m.disks[id]
		if d.LineageID == lineageID {
			disks = append(disks, d.toRecord())
		}
	}
	snaps := make([]SnapshotRecord, 0)
	for sid := range m.snapshots {
		s := m.snapshots[sid]
		if s.LineageID == lineageID {
			snaps = append(snaps, s.toRecord())
		}
	}
	return disks, snaps
}

func (m *MemStore) deleteSnapshotsLocked(lineageID uuid.UUID, snapIDs []uuid.UUID) {
	delSet := make(map[uuid.UUID]struct{}, len(snapIDs))
	for _, sid := range snapIDs {
		delSet[sid] = struct{}{}
	}
	for sid := range m.snapshots {
		if _, deleting := delSet[sid]; deleting {
			continue
		}
		s := m.snapshots[sid]
		if s.LineageID != lineageID {
			continue
		}
		changed := false
		if _, cleared := delSet[s.ParentSnapshotID]; cleared {
			s.ParentSnapshotID = uuid.Nil
			changed = true
		}
		if _, cleared := delSet[s.MergeParentSnapshotID]; cleared {
			s.MergeParentSnapshotID = uuid.Nil
			changed = true
		}
		if changed {
			m.snapshots[sid] = s
		}
	}
	for _, sid := range snapIDs {
		delete(m.snapshots, sid)
	}
}

func (m *MemStore) CreateSnapshot(ctx context.Context, arg CreateSnapshotInput) (SnapshotRecord, error) {
	if err := ctx.Err(); err != nil {
		return SnapshotRecord{}, err
	}
	if arg.DiskID == uuid.Nil || arg.SnapshotID == uuid.Nil {
		return SnapshotRecord{}, fmt.Errorf("%w: disk id and snapshot id cannot be nil uuid", ErrInvalidArgument)
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.consumeFailNextWriteLocked(); err != nil {
		return SnapshotRecord{}, err
	}

	diskRow, ok := m.disks[arg.DiskID]
	if !ok {
		return SnapshotRecord{}, ErrNotFound
	}
	if _, linOK := m.lineages[diskRow.LineageID]; !linOK {
		return SnapshotRecord{}, ErrNotFound
	}
	if _, exists := m.snapshots[arg.SnapshotID]; exists {
		return SnapshotRecord{}, errors.New("db: snapshot already exists")
	}

	view := MemSnapshotView{
		SnapshotID:       arg.SnapshotID,
		LineageID:        diskRow.LineageID,
		SourceDiskID:     arg.DiskID,
		ParentSnapshotID: diskRow.BaseSnapshotID,
		Kind:             SnapshotKindManual,
		Name:             arg.Name,
		CreatedAt:        m.nextNowLocked(),
	}
	m.snapshots[arg.SnapshotID] = view
	return view.toRecord(), nil
}

func (m *MemStore) ListSnapshots(ctx context.Context, query ListSnapshotsQuery) (ListSnapshotsPage, error) {
	if err := ctx.Err(); err != nil {
		return ListSnapshotsPage{}, err
	}
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

	m.mu.Lock()
	defer m.mu.Unlock()

	diskRow, ok := m.disks[query.DiskID]
	if !ok {
		return ListSnapshotsPage{}, ErrNotFound
	}
	disks, snaps := m.collectLineageStateLocked(diskRow.LineageID)
	return buildListSnapshotsPage(disks, snaps, query.Kind, pageSize, hasCursor, cursorTime, cursorSnapID), nil
}

func (m *MemStore) DeleteSnapshot(ctx context.Context, diskID, snapshotID uuid.UUID) (SnapshotRecord, error) {
	if err := ctx.Err(); err != nil {
		return SnapshotRecord{}, err
	}
	if diskID == uuid.Nil || snapshotID == uuid.Nil {
		return SnapshotRecord{}, fmt.Errorf("%w: disk id and snapshot id cannot be nil uuid", ErrInvalidArgument)
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.consumeFailNextWriteLocked(); err != nil {
		return SnapshotRecord{}, err
	}

	diskRow, ok := m.disks[diskID]
	if !ok {
		return SnapshotRecord{}, ErrNotFound
	}
	targetView, snapOK := m.snapshots[snapshotID]
	if !snapOK {
		return SnapshotRecord{}, ErrNotFound
	}
	if targetView.LineageID != diskRow.LineageID {
		return SnapshotRecord{}, ErrCrossLineage
	}

	disks, snaps := m.collectLineageStateLocked(diskRow.LineageID)
	hull := ComputeActiveLCAHull(disks, snaps)
	if _, protected := hull[snapshotID]; protected {
		return SnapshotRecord{}, ErrSnapshotInUse
	}

	m.deleteSnapshotsLocked(diskRow.LineageID, []uuid.UUID{snapshotID})
	rec := targetView.toRecord()
	rec.Protected = false
	return rec, nil
}

func (m *MemStore) PruneSnapshots(ctx context.Context, diskID uuid.UUID, dryRun bool) ([]SnapshotRecord, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if diskID == uuid.Nil {
		return nil, fmt.Errorf("%w: disk id cannot be nil uuid", ErrInvalidArgument)
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	if !dryRun {
		if err := m.consumeFailNextWriteLocked(); err != nil {
			return nil, err
		}
	}

	diskRow, ok := m.disks[diskID]
	if !ok {
		return nil, ErrNotFound
	}
	disks, snaps := m.collectLineageStateLocked(diskRow.LineageID)
	candidates := selectPrunableSnapshots(disks, snaps)
	if dryRun || len(candidates) == 0 {
		return candidates, nil
	}

	ids := make([]uuid.UUID, 0, len(candidates))
	for i := range candidates {
		ids = append(ids, candidates[i].SnapshotID)
	}
	m.deleteSnapshotsLocked(diskRow.LineageID, ids)
	return candidates, nil
}

func (m *MemStore) UpdateDisk(ctx context.Context, input UpdateDiskInput) (DiskRecord, error) {
	if err := ctx.Err(); err != nil {
		return DiskRecord{}, err
	}
	if err := validateUpdateDiskInput(input); err != nil {
		return DiskRecord{}, err
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.consumeFailNextWriteLocked(); err != nil {
		return DiskRecord{}, err
	}

	row, ok := m.disks[input.DiskID]
	if !ok {
		return DiskRecord{}, ErrNotFound
	}
	if input.Name != nil {
		row.Name = *input.Name
	}
	if input.MaxBytes != nil {
		row.MaxBytes = *input.MaxBytes
	}
	row.UpdatedAt = m.nextNowLocked()
	m.disks[input.DiskID] = row
	return row.toRecord(), nil
}

func (m *MemStore) UpdateDiskUsage(ctx context.Context, diskID uuid.UUID, usedBytes, usedInodes uint64) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := validateUsageParams(diskID, usedBytes, usedInodes); err != nil {
		return err
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.consumeFailNextWriteLocked(); err != nil {
		return err
	}

	row, ok := m.disks[diskID]
	if !ok {
		return ErrNotFound
	}
	row.UsedBytes = usedBytes
	row.UsedInodes = usedInodes
	m.disks[diskID] = row
	m.updateUsageCalls++
	return nil
}

func (m *MemStore) MergeDisk(ctx context.Context, input *MergeDiskInput) (DiskRecord, error) {
	if err := ctx.Err(); err != nil {
		return DiskRecord{}, err
	}
	if err := validateMergeDiskInput(input); err != nil {
		return DiskRecord{}, err
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.consumeFailNextWriteLocked(); err != nil {
		return DiskRecord{}, err
	}

	if _, linOK := m.lineages[input.LineageID]; !linOK {
		return DiskRecord{}, ErrDiskNotFound
	}
	srcRow, srcOK := m.disks[input.SourceDiskID]
	if !srcOK {
		return DiskRecord{}, ErrDiskNotFound
	}
	tgtRow, tgtOK := m.disks[input.TargetDiskID]
	if !tgtOK {
		return DiskRecord{}, ErrDiskNotFound
	}
	if srcRow.LineageID != input.LineageID || tgtRow.LineageID != input.LineageID {
		return DiskRecord{}, ErrCrossLineage
	}
	if srcRow.BaseSnapshotID != input.ExpectedSourceBaseSnapshotID ||
		tgtRow.BaseSnapshotID != input.ExpectedTargetBaseSnapshotID {
		return DiskRecord{}, ErrStaleBaseSnapshot
	}

	if input.FastForward {
		ffSnapID := input.NewMergeSnapshotID
		if !input.SkipSourceSnapshot {
			if _, exists := m.snapshots[ffSnapID]; exists {
				return DiskRecord{}, errors.New("db: snapshot already exists")
			}
			ffNow := m.nextNowLocked()
			m.snapshots[ffSnapID] = MemSnapshotView{
				SnapshotID:       ffSnapID,
				LineageID:        input.LineageID,
				SourceDiskID:     input.SourceDiskID,
				ParentSnapshotID: input.ExpectedSourceBaseSnapshotID,
				Kind:             SnapshotKindMerge,
				CreatedAt:        ffNow,
			}
			srcRow.BaseSnapshotID = ffSnapID
			m.disks[input.SourceDiskID] = srcRow
		}
		tgtRow.BaseSnapshotID = ffSnapID
		if input.TargetMaxBytes > 0 {
			tgtRow.MaxBytes = input.TargetMaxBytes
		}
		if input.TargetUsedInodes > 0 {
			tgtRow.UsedBytes = input.TargetUsedBytes
			tgtRow.UsedInodes = input.TargetUsedInodes
		}
		tgtRow.UpdatedAt = m.nextNowLocked()
		m.disks[input.TargetDiskID] = tgtRow
		return tgtRow.toRecord(), nil
	}

	if !input.SkipSourceSnapshot {
		if _, exists := m.snapshots[input.NewSourceSnapshotID]; exists {
			return DiskRecord{}, errors.New("db: snapshot already exists")
		}
	}
	if _, exists := m.snapshots[input.NewMergeSnapshotID]; exists {
		return DiskRecord{}, errors.New("db: snapshot already exists")
	}

	srcFinalSnapshotID := input.ExpectedSourceBaseSnapshotID
	if !input.SkipSourceSnapshot {
		srcNow := m.nextNowLocked()
		m.snapshots[input.NewSourceSnapshotID] = MemSnapshotView{
			SnapshotID:       input.NewSourceSnapshotID,
			LineageID:        input.LineageID,
			SourceDiskID:     input.SourceDiskID,
			ParentSnapshotID: input.ExpectedSourceBaseSnapshotID,
			Kind:             SnapshotKindMerge,
			CreatedAt:        srcNow,
		}
		srcRow.BaseSnapshotID = input.NewSourceSnapshotID
		m.disks[input.SourceDiskID] = srcRow
		srcFinalSnapshotID = input.NewSourceSnapshotID
	}

	mergeNow := m.nextNowLocked()
	m.snapshots[input.NewMergeSnapshotID] = MemSnapshotView{
		SnapshotID:            input.NewMergeSnapshotID,
		LineageID:             input.LineageID,
		SourceDiskID:          input.TargetDiskID,
		ParentSnapshotID:      input.ExpectedTargetBaseSnapshotID,
		MergeParentSnapshotID: srcFinalSnapshotID,
		Kind:                  SnapshotKindMerge,
		CreatedAt:             mergeNow,
	}
	tgtRow.BaseSnapshotID = input.NewMergeSnapshotID
	if input.TargetMaxBytes > 0 {
		tgtRow.MaxBytes = input.TargetMaxBytes
	}
	if input.TargetUsedInodes > 0 {
		tgtRow.UsedBytes = input.TargetUsedBytes
		tgtRow.UsedInodes = input.TargetUsedInodes
	}
	tgtRow.UpdatedAt = mergeNow
	m.disks[input.TargetDiskID] = tgtRow
	return tgtRow.toRecord(), nil
}

func (m *MemStore) DeleteDisk(ctx context.Context, diskID uuid.UUID, recursive bool) (DeleteDiskResult, error) {
	if err := ctx.Err(); err != nil {
		return DeleteDiskResult{}, err
	}
	if diskID == uuid.Nil {
		return DeleteDiskResult{}, fmt.Errorf("%w: disk id cannot be nil uuid", ErrInvalidArgument)
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.consumeFailNextWriteLocked(); err != nil {
		return DeleteDiskResult{}, err
	}

	row, ok := m.disks[diskID]
	if !ok {
		return DeleteDiskResult{}, ErrNotFound
	}
	lineageID := row.LineageID
	if _, linOK := m.lineages[lineageID]; !linOK {
		return DeleteDiskResult{}, ErrNotFound
	}

	lineageDisks := make([]DiskRecord, 0)
	for id := range m.disks {
		d := m.disks[id]
		if d.LineageID == lineageID {
			lineageDisks = append(lineageDisks, d.toRecord())
		}
	}

	toDeleteDiskIDs, found := collectSubtreeDiskIDs(lineageDisks, diskID, recursive)
	if !found {
		return DeleteDiskResult{}, ErrNotFound
	}

	deletedDiskSet := make(map[uuid.UUID]struct{}, len(toDeleteDiskIDs))
	for _, id := range toDeleteDiskIDs {
		deletedDiskSet[id] = struct{}{}
		delete(m.disks, id)
		delete(m.mounts, id)
	}

	survivingDisks := make([]DiskRecord, 0, len(lineageDisks)-len(toDeleteDiskIDs))
	for i := range lineageDisks {
		if _, removed := deletedDiskSet[lineageDisks[i].DiskID]; !removed {
			survivingDisks = append(survivingDisks, lineageDisks[i])
		}
	}

	lineageSnaps := make([]SnapshotRecord, 0)
	for sid := range m.snapshots {
		s := m.snapshots[sid]
		if s.LineageID == lineageID {
			lineageSnaps = append(lineageSnaps, s.toRecord())
		}
	}

	toDeleteSnapIDs, remainingSnaps := computeReclaimableSnapshots(survivingDisks, lineageSnaps)
	if len(toDeleteSnapIDs) > 0 {
		delSnapSet := make(map[uuid.UUID]struct{}, len(toDeleteSnapIDs))
		for _, sid := range toDeleteSnapIDs {
			delSnapSet[sid] = struct{}{}
		}
		for id := range m.disks {
			d := m.disks[id]
			if d.LineageID != lineageID {
				continue
			}
			if _, cleared := delSnapSet[d.BaseSnapshotID]; cleared {
				d.BaseSnapshotID = uuid.Nil
				m.disks[id] = d
			}
		}
		for sid := range m.snapshots {
			if _, deleting := delSnapSet[sid]; deleting {
				continue
			}
			s := m.snapshots[sid]
			if s.LineageID != lineageID {
				continue
			}
			changed := false
			if _, cleared := delSnapSet[s.ParentSnapshotID]; cleared {
				s.ParentSnapshotID = uuid.Nil
				changed = true
			}
			if _, cleared := delSnapSet[s.MergeParentSnapshotID]; cleared {
				s.MergeParentSnapshotID = uuid.Nil
				changed = true
			}
			if changed {
				m.snapshots[sid] = s
			}
		}
		for _, sid := range toDeleteSnapIDs {
			delete(m.snapshots, sid)
		}
	}

	if len(survivingDisks) == 0 && remainingSnaps == 0 {
		delete(m.lineages, lineageID)
	}
	return DeleteDiskResult{
		DeletedDiskIDs:       toDeleteDiskIDs,
		ReclaimedSnapshotIDs: toDeleteSnapIDs,
	}, nil
}

func (m *MemStore) ReserveSegment(ctx context.Context, kind SegmentKind, lineageID uuid.UUID, minNext, count int64) (SegmentRange, error) {
	if err := ctx.Err(); err != nil {
		return SegmentRange{}, err
	}
	if lineageID == uuid.Nil || count <= 0 {
		return SegmentRange{}, fmt.Errorf("%w: invalid segment reserve parameters", ErrInvalidArgument)
	}
	if kind != SegmentKindInode && kind != SegmentKindPack {
		return SegmentRange{}, fmt.Errorf("%w: unknown segment kind %d", ErrInvalidArgument, kind)
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	lin, ok := m.lineages[lineageID]
	if !ok {
		return SegmentRange{}, ErrNotFound
	}

	switch kind {
	case SegmentKindInode:
		m.inodeReserveCalls++
		first := max(lin.NextIno, minNext)
		lin.NextIno = first + count
		m.lineages[lineageID] = lin
		return SegmentRange{First: first, Last: lin.NextIno - 1}, nil
	case SegmentKindPack:
		m.packReserveCalls++
		first := max(lin.NextPackID, minNext)
		lin.NextPackID = first + count
		m.lineages[lineageID] = lin
		return SegmentRange{First: first, Last: lin.NextPackID - 1}, nil
	default:
		return SegmentRange{}, ErrInvalidArgument
	}
}

func (m *MemStore) CreateMountLease(ctx context.Context, arg CreateMountLeaseInput) (MountLeaseRecord, error) {
	if err := ctx.Err(); err != nil {
		return MountLeaseRecord{}, err
	}
	nowUTC, expUTC, err := validateMountLeaseParams(arg.DiskID, arg.MountID, arg.ReadOnly, arg.Epoch, arg.Now, arg.ExpiresAt)
	if err != nil {
		return MountLeaseRecord{}, err
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.consumeFailNextWriteLocked(); err != nil {
		return MountLeaseRecord{}, err
	}
	if _, ok := m.disks[arg.DiskID]; !ok {
		return MountLeaseRecord{}, ErrNotFound
	}

	leases := m.mounts[arg.DiskID]
	for id, lease := range leases {
		if !lease.ExpiresAt.After(nowUTC) {
			delete(leases, id)
			continue
		}
		if !arg.ReadOnly && !lease.ReadOnly && lease.Epoch < arg.Epoch {
			delete(leases, id)
		}
	}
	if leases == nil {
		leases = make(map[uuid.UUID]MountLeaseRecord)
		m.mounts[arg.DiskID] = leases
	}
	if _, dup := leases[arg.MountID]; dup {
		return MountLeaseRecord{}, errors.New("db: mount lease already exists")
	}
	rec := MountLeaseRecord{
		DiskID:    arg.DiskID,
		MountID:   arg.MountID,
		ReadOnly:  arg.ReadOnly,
		Epoch:     arg.Epoch,
		ExpiresAt: expUTC,
	}
	leases[arg.MountID] = rec
	return rec, nil
}

func (m *MemStore) RenewMountLease(ctx context.Context, arg RenewMountLeaseInput) (MountLeaseRecord, error) {
	if err := ctx.Err(); err != nil {
		return MountLeaseRecord{}, err
	}
	nowUTC, expUTC, err := validateMountLeaseParams(arg.DiskID, arg.MountID, arg.ReadOnly, arg.Epoch, arg.Now, arg.ExpiresAt)
	if err != nil {
		return MountLeaseRecord{}, err
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.consumeFailNextWriteLocked(); err != nil {
		return MountLeaseRecord{}, err
	}

	leases := m.mounts[arg.DiskID]
	existing, ok := leases[arg.MountID]
	if !ok {
		return MountLeaseRecord{}, ErrNotFound
	}
	if !existing.ExpiresAt.After(nowUTC) {
		return MountLeaseRecord{}, ErrNotFound
	}
	if existing.ReadOnly != arg.ReadOnly {
		return MountLeaseRecord{}, fmt.Errorf("%w: mount mode mismatch for mount_id %s", ErrInvalidArgument, arg.MountID)
	}
	if !arg.ReadOnly && existing.Epoch != arg.Epoch {
		return MountLeaseRecord{}, fmt.Errorf("%w: mount epoch %d != lease epoch %d", ErrStaleEpoch, arg.Epoch, existing.Epoch)
	}
	existing.ExpiresAt = expUTC
	leases[arg.MountID] = existing
	return existing, nil
}

func (m *MemStore) DeleteMountLease(ctx context.Context, diskID, mountID uuid.UUID) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	if diskID == uuid.Nil || mountID == uuid.Nil {
		return false, fmt.Errorf("%w: disk id and mount id cannot be nil uuid", ErrInvalidArgument)
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.consumeFailNextWriteLocked(); err != nil {
		return false, err
	}

	leases := m.mounts[diskID]
	if leases == nil {
		return false, nil
	}
	if _, ok := leases[mountID]; !ok {
		return false, nil
	}
	delete(leases, mountID)
	if len(leases) == 0 {
		delete(m.mounts, diskID)
	}
	return true, nil
}

func (m *MemStore) DeleteDiskMountLeases(ctx context.Context, diskID uuid.UUID, now time.Time) (uint32, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if diskID == uuid.Nil || now.IsZero() {
		return 0, fmt.Errorf("%w: disk id and now are required", ErrInvalidArgument)
	}
	nowUTC := now.UTC().Truncate(time.Microsecond)

	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.consumeFailNextWriteLocked(); err != nil {
		return 0, err
	}

	leases := m.mounts[diskID]
	var activeCount uint32
	for _, lease := range leases {
		if lease.ExpiresAt.After(nowUTC) {
			activeCount++
		}
	}
	delete(m.mounts, diskID)
	return activeCount, nil
}

func (m *MemStore) ListActiveMountLeases(ctx context.Context, diskID uuid.UUID, now time.Time) ([]MountLeaseRecord, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if diskID == uuid.Nil || now.IsZero() {
		return nil, fmt.Errorf("%w: disk id and now are required", ErrInvalidArgument)
	}
	nowUTC := now.UTC().Truncate(time.Microsecond)

	m.mu.Lock()
	defer m.mu.Unlock()

	leases := m.mounts[diskID]
	out := make([]MountLeaseRecord, 0, len(leases))
	for _, lease := range leases {
		if lease.ExpiresAt.After(nowUTC) {
			out = append(out, lease)
		}
	}
	slices.SortFunc(out, func(a, b MountLeaseRecord) int {
		return bytes.Compare(a.MountID[:], b.MountID[:])
	})
	return out, nil
}

func (m *MemStore) PruneExpiredMountLeases(ctx context.Context, now time.Time) (int64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if now.IsZero() {
		return 0, fmt.Errorf("%w: now must be non-zero", ErrInvalidArgument)
	}
	nowUTC := now.UTC().Truncate(time.Microsecond)

	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.consumeFailNextWriteLocked(); err != nil {
		return 0, err
	}

	var pruned int64
	for diskID, leases := range m.mounts {
		for mountID, lease := range leases {
			if !lease.ExpiresAt.After(nowUTC) {
				delete(leases, mountID)
				pruned++
			}
		}
		if len(leases) == 0 {
			delete(m.mounts, diskID)
		}
	}
	return pruned, nil
}
