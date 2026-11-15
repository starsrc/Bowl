package db

import (
	"bytes"
	"context"
	"fmt"
	"maps"
	"slices"
	"sync"
	"time"

	"github.com/google/uuid"
)

var _ Store = (*MemStore)(nil)

// MemDiskView 描述 MemStore 中持久化磁盘的只读快照视图（供测试观测使用）。
type MemDiskView struct {
	DiskID       uuid.UUID
	ParentDiskID uuid.UUID
	LineageID    uuid.UUID
	BaseCommitID uuid.UUID
	Name         string
	CreatedAt    time.Time
	UpdatedAt    time.Time
}

func (v *MemDiskView) toRecord() DiskRecord {
	return DiskRecord{
		DiskID:       v.DiskID,
		ParentDiskID: v.ParentDiskID,
		LineageID:    v.LineageID,
		BaseCommitID: v.BaseCommitID,
		Name:         v.Name,
		CreatedAt:    v.CreatedAt,
		UpdatedAt:    v.UpdatedAt,
	}
}

// MemLineageView 描述 MemStore 中血缘状态的只读快照视图（供测试观测使用）。
type MemLineageView struct {
	LineageID  uuid.UUID
	NextIno    int64
	NextPackID int64
	CreatedAt  time.Time
}

// MemCommitView 描述 MemStore 中基准切面 DAG 记录的只读快照视图（供测试观测使用）。
type MemCommitView struct {
	CommitID            uuid.UUID
	LineageID           uuid.UUID
	SourceDiskID        uuid.UUID
	ParentCommitID      uuid.UUID
	MergeParentCommitID uuid.UUID
	CreatedAt           time.Time
}

func (v *MemCommitView) toRecord() CommitRecord {
	return CommitRecord{
		LineageID:           v.LineageID,
		CommitID:            v.CommitID,
		ParentCommitID:      v.ParentCommitID,
		MergeParentCommitID: v.MergeParentCommitID,
		BaseDiskID:          v.SourceDiskID,
		CreatedAt:           v.CreatedAt,
	}
}

// MemSnapshot 描述 MemStore 在某一时刻的深拷贝只读状态快照。
type MemSnapshot struct {
	Disks             map[uuid.UUID]MemDiskView
	Lineages          map[uuid.UUID]MemLineageView
	Commits           map[uuid.UUID]MemCommitView
	InodeReserveCalls int
	PackReserveCalls  int
}

// MemStore 提供与 SQLStore 契约一致的进程内控制面持久化适配器。
type MemStore struct {
	mu                sync.Mutex
	disks             map[uuid.UUID]MemDiskView
	lineages          map[uuid.UUID]MemLineageView
	commits           map[uuid.UUID]MemCommitView
	lastTS            time.Time
	failNextWrite     error
	inodeReserveCalls int
	packReserveCalls  int
}

// NewMemStore 创建共享进程内测试适配器实例。
func NewMemStore() *MemStore {
	return &MemStore{
		disks:    make(map[uuid.UUID]MemDiskView),
		lineages: make(map[uuid.UUID]MemLineageView),
		commits:  make(map[uuid.UUID]MemCommitView),
	}
}

// SetFailNextWrite 注入下一次写事务失败错误（触发一次后自动清空，且不产生部分状态变更）。
func (m *MemStore) SetFailNextWrite(err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.failNextWrite = err
}

// Inspect 返回当前内存表状态的深拷贝只读快照（仅在具体 *MemStore 上提供，不泄露至 Store 接口）。
func (m *MemStore) Inspect() MemSnapshot {
	m.mu.Lock()
	defer m.mu.Unlock()
	return MemSnapshot{
		Disks:             maps.Clone(m.disks),
		Lineages:          maps.Clone(m.lineages),
		Commits:           maps.Clone(m.commits),
		InodeReserveCalls: m.inodeReserveCalls,
		PackReserveCalls:  m.packReserveCalls,
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

// CreateDisk 原子创建新血缘行与根磁盘记录。
func (m *MemStore) CreateDisk(ctx context.Context, diskID uuid.UUID, name string) (DiskRecord, error) {
	if err := ctx.Err(); err != nil {
		return DiskRecord{}, err
	}
	if diskID == uuid.Nil {
		return DiskRecord{}, fmt.Errorf("%w: disk id cannot be nil uuid", ErrInvalidArgument)
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.consumeFailNextWriteLocked(); err != nil {
		return DiskRecord{}, err
	}
	if _, exists := m.disks[diskID]; exists {
		return DiskRecord{}, fmt.Errorf("%w: disk already exists", ErrAlreadyExists)
	}
	if _, linExists := m.lineages[diskID]; linExists {
		return DiskRecord{}, fmt.Errorf("%w: lineage already exists", ErrAlreadyExists)
	}

	now := m.nextNowLocked()
	m.lineages[diskID] = MemLineageView{
		LineageID:  diskID,
		NextIno:    2,
		NextPackID: 1,
		CreatedAt:  now,
	}
	row := MemDiskView{
		DiskID:    diskID,
		LineageID: diskID,
		Name:      name,
		CreatedAt: now,
		UpdatedAt: now,
	}
	m.disks[diskID] = row
	return row.toRecord(), nil
}

// ForkDisk 原子写入派生切面、更新源盘基准切面并创建子盘记录。
func (m *MemStore) ForkDisk(ctx context.Context, arg ForkDiskInput) (DiskRecord, error) {
	if err := ctx.Err(); err != nil {
		return DiskRecord{}, err
	}
	if arg.SourceDiskID == uuid.Nil || arg.ChildDiskID == uuid.Nil || arg.CommitID == uuid.Nil || arg.SourceDiskID == arg.ChildDiskID {
		return DiskRecord{}, fmt.Errorf("%w: invalid fork disk identifiers", ErrInvalidArgument)
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
	if srcRow.BaseCommitID != uuid.Nil && arg.CommitID == srcRow.BaseCommitID {
		return DiskRecord{}, fmt.Errorf("%w: commit id cannot equal source base commit id", ErrInvalidArgument)
	}
	if _, childExists := m.disks[arg.ChildDiskID]; childExists {
		return DiskRecord{}, fmt.Errorf("%w: child disk already exists", ErrAlreadyExists)
	}
	if _, commitExists := m.commits[arg.CommitID]; commitExists {
		return DiskRecord{}, fmt.Errorf("%w: commit already exists", ErrAlreadyExists)
	}

	now := m.nextNowLocked()
	m.commits[arg.CommitID] = MemCommitView{
		CommitID:       arg.CommitID,
		LineageID:      srcRow.LineageID,
		SourceDiskID:   arg.SourceDiskID,
		ParentCommitID: srcRow.BaseCommitID,
		CreatedAt:      now,
	}
	srcRow.BaseCommitID = arg.CommitID
	m.disks[arg.SourceDiskID] = srcRow

	childRow := MemDiskView{
		DiskID:       arg.ChildDiskID,
		ParentDiskID: arg.SourceDiskID,
		LineageID:    srcRow.LineageID,
		BaseCommitID: arg.CommitID,
		Name:         arg.Name,
		CreatedAt:    now,
		UpdatedAt:    now,
	}
	m.disks[arg.ChildDiskID] = childRow
	return childRow.toRecord(), nil
}

// GetDisk 查询单盘元数据记录。
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

// ListDisks 按 (created_at DESC, disk_id DESC) 稳定游标分页查询磁盘列表。
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

// ListLineageCommits 按 (created_at ASC, commit_id ASC) 查询指定血缘的全部切面节点。
func (m *MemStore) ListLineageCommits(ctx context.Context, lineageID uuid.UUID) ([]CommitRecord, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if lineageID == uuid.Nil {
		return nil, fmt.Errorf("%w: lineage id cannot be nil uuid", ErrInvalidArgument)
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	ordered := make([]MemCommitView, 0)
	for cid := range m.commits {
		c := m.commits[cid]
		if c.LineageID == lineageID {
			ordered = append(ordered, c)
		}
	}
	slices.SortFunc(ordered, func(a, b MemCommitView) int {
		if cmp := a.CreatedAt.Compare(b.CreatedAt); cmp != 0 {
			return cmp
		}
		return bytes.Compare(a.CommitID[:], b.CommitID[:])
	})

	commits := make([]CommitRecord, 0, len(ordered))
	for i := range ordered {
		commits = append(commits, ordered[i].toRecord())
	}
	return commits, nil
}

// UpdateDiskName 更新磁盘名称。
func (m *MemStore) UpdateDiskName(ctx context.Context, diskID uuid.UUID, name string) (DiskRecord, error) {
	if err := ctx.Err(); err != nil {
		return DiskRecord{}, err
	}
	if diskID == uuid.Nil {
		return DiskRecord{}, fmt.Errorf("%w: disk id cannot be nil uuid", ErrInvalidArgument)
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.consumeFailNextWriteLocked(); err != nil {
		return DiskRecord{}, err
	}

	row, ok := m.disks[diskID]
	if !ok {
		return DiskRecord{}, ErrNotFound
	}
	row.Name = name
	row.UpdatedAt = m.nextNowLocked()
	m.disks[diskID] = row
	return row.toRecord(), nil
}

// MergeDisk 原子校验血缘与 CAS 切面并写入合并 Commit 节点（失败零副作用回滚）。
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
		return DiskRecord{}, ErrNotFound
	}
	srcRow, srcOK := m.disks[input.SourceDiskID]
	if !srcOK {
		return DiskRecord{}, ErrNotFound
	}
	tgtRow, tgtOK := m.disks[input.TargetDiskID]
	if !tgtOK {
		return DiskRecord{}, ErrNotFound
	}
	if srcRow.LineageID != input.LineageID || tgtRow.LineageID != input.LineageID {
		return DiskRecord{}, ErrCrossLineage
	}
	if srcRow.BaseCommitID != input.ExpectedSourceBaseCommitID ||
		tgtRow.BaseCommitID != input.ExpectedTargetBaseCommitID {
		return DiskRecord{}, ErrStaleBaseCommit
	}
	if !input.SkipSourceCommit {
		if _, exists := m.commits[input.NewSourceCommitID]; exists {
			return DiskRecord{}, fmt.Errorf("%w: commit already exists", ErrAlreadyExists)
		}
	}
	if _, exists := m.commits[input.NewMergeCommitID]; exists {
		return DiskRecord{}, fmt.Errorf("%w: commit already exists", ErrAlreadyExists)
	}

	srcFinalCommitID := input.ExpectedSourceBaseCommitID
	if !input.SkipSourceCommit {
		srcNow := m.nextNowLocked()
		m.commits[input.NewSourceCommitID] = MemCommitView{
			CommitID:       input.NewSourceCommitID,
			LineageID:      input.LineageID,
			SourceDiskID:   input.SourceDiskID,
			ParentCommitID: input.ExpectedSourceBaseCommitID,
			CreatedAt:      srcNow,
		}
		srcRow.BaseCommitID = input.NewSourceCommitID
		m.disks[input.SourceDiskID] = srcRow
		srcFinalCommitID = input.NewSourceCommitID
	}

	mergeNow := m.nextNowLocked()
	m.commits[input.NewMergeCommitID] = MemCommitView{
		CommitID:            input.NewMergeCommitID,
		LineageID:           input.LineageID,
		SourceDiskID:        input.TargetDiskID,
		ParentCommitID:      input.ExpectedTargetBaseCommitID,
		MergeParentCommitID: srcFinalCommitID,
		CreatedAt:           mergeNow,
	}
	tgtRow.BaseCommitID = input.NewMergeCommitID
	tgtRow.UpdatedAt = mergeNow
	m.disks[input.TargetDiskID] = tgtRow
	return tgtRow.toRecord(), nil
}

// DeleteDisk 删盘并按同血缘剩余盘数执行三态切面回收。
func (m *MemStore) DeleteDisk(ctx context.Context, diskID uuid.UUID) ([]uuid.UUID, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if diskID == uuid.Nil {
		return nil, fmt.Errorf("%w: disk id cannot be nil uuid", ErrInvalidArgument)
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.consumeFailNextWriteLocked(); err != nil {
		return nil, err
	}

	row, ok := m.disks[diskID]
	if !ok {
		return nil, ErrNotFound
	}
	lineageID := row.LineageID
	if _, linOK := m.lineages[lineageID]; !linOK {
		return nil, ErrNotFound
	}

	delete(m.disks, diskID)

	var remaining int
	for id := range m.disks {
		if m.disks[id].LineageID == lineageID {
			remaining++
		}
	}

	var commitsToClean []uuid.UUID
	switch remaining {
	case 1:
		for id := range m.disks {
			d := m.disks[id]
			if d.LineageID == lineageID && d.BaseCommitID != uuid.Nil {
				d.BaseCommitID = uuid.Nil
				m.disks[id] = d
			}
		}
		commitsToClean = m.deleteLineageCommitsLocked(lineageID)
	case 0:
		commitsToClean = m.deleteLineageCommitsLocked(lineageID)
		delete(m.lineages, lineageID)
	}
	return commitsToClean, nil
}

func (m *MemStore) deleteLineageCommitsLocked(lineageID uuid.UUID) []uuid.UUID {
	var deleted []uuid.UUID
	for cid := range m.commits {
		if m.commits[cid].LineageID == lineageID {
			deleted = append(deleted, cid)
			delete(m.commits, cid)
		}
	}
	return deleted
}

// ReserveSegment 为指定血缘单调预留 Inode 或 Pack 号段。
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
	if err := m.consumeFailNextWriteLocked(); err != nil {
		return SegmentRange{}, err
	}
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
