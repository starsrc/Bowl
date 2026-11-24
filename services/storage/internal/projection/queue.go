package projection

import (
	"context"
	"maps"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
)

const (
	DefaultDiskQueueSize = 64
	DefaultFlushInterval = 5 * time.Second
	DefaultOpTimeout     = 10 * time.Second
)

type QueueConfig struct {
	DiskQueueSize int
	FlushInterval time.Duration
	OpTimeout     time.Duration
}

type diskEvent struct {
	diskID     uuid.UUID
	diskSeq    uint64
	catalogSeq uint64
	apply      func(ctx context.Context, store Store) error
}

type diskLane struct {
	diskID      uuid.UUID
	ch          chan diskEvent
	enqueuedSeq uint64
	flushedSeq  uint64
	notify      chan struct{}
}

type mountBatchKey struct {
	diskID  uuid.UUID
	mountID uuid.UUID
}

type Queue struct {
	store         Store
	diskQueueSize int
	flushInterval time.Duration
	opTimeout     time.Duration

	mu                 sync.Mutex
	closed             bool
	disks              map[uuid.UUID]*diskLane
	dirty              map[uuid.UUID]uint64
	dirtyGen           uint64
	catalogEnqueuedSeq uint64
	catalogFlushedSeq  uint64
	catalogDone        map[uint64]struct{}
	catalogNotify      chan struct{}

	batchMu  sync.Mutex
	usageBuf map[uuid.UUID]uint64
	renewBuf map[mountBatchKey]time.Time

	isClosed    atomic.Bool
	drainCtx    context.Context
	drainCancel context.CancelFunc
	stopBatcher chan struct{}
	batchDone   chan struct{}
	workerWG    sync.WaitGroup
	closeDone   chan struct{}
	closeErr    error
}

func NewQueue(store Store, cfg QueueConfig) *Queue {
	queueSize := cfg.DiskQueueSize
	if queueSize <= 0 {
		queueSize = DefaultDiskQueueSize
	}
	flushInterval := cfg.FlushInterval
	if flushInterval <= 0 {
		flushInterval = DefaultFlushInterval
	}
	opTimeout := cfg.OpTimeout
	if opTimeout <= 0 {
		opTimeout = DefaultOpTimeout
	}

	drainCtx, drainCancel := context.WithCancel(context.Background())
	q := &Queue{
		store:         store,
		diskQueueSize: queueSize,
		flushInterval: flushInterval,
		opTimeout:     opTimeout,
		disks:         make(map[uuid.UUID]*diskLane),
		dirty:         make(map[uuid.UUID]uint64),
		catalogDone:   make(map[uint64]struct{}),
		catalogNotify: make(chan struct{}),
		usageBuf:      make(map[uuid.UUID]uint64),
		renewBuf:      make(map[mountBatchKey]time.Time),
		drainCtx:      drainCtx,
		drainCancel:   drainCancel,
		stopBatcher:   make(chan struct{}),
		batchDone:     make(chan struct{}),
		closeDone:     make(chan struct{}),
	}
	go q.runBatcher()
	return q
}

func cloneBranchPtr(b *BranchRecord) *BranchRecord {
	if b == nil {
		return nil
	}
	cp := *b
	return &cp
}

func cloneCommitPtr(c *CommitRecord) *CommitRecord {
	if c == nil {
		return nil
	}
	cp := *c
	return &cp
}

func (q *Queue) EnqueueUpsertDisk(disk DiskRecord, initialBranch *BranchRecord, initialCommit *CommitRecord) {
	b := cloneBranchPtr(initialBranch)
	c := cloneCommitPtr(initialCommit)
	q.enqueue(disk.DiskID, true, func(ctx context.Context, store Store) error {
		return store.UpsertDisk(ctx, disk, b, c)
	})
}

func (q *Queue) EnqueueUpdateDisk(disk DiskRecord) {
	q.enqueue(disk.DiskID, true, func(ctx context.Context, store Store) error {
		return store.UpdateDisk(ctx, disk)
	})
}

func (q *Queue) EnqueueDeleteDisk(diskID uuid.UUID) {
	q.enqueue(diskID, true, func(ctx context.Context, store Store) error {
		return store.DeleteDisk(ctx, diskID)
	})
}

func (q *Queue) EnqueueUpsertBranchAndCommit(diskID uuid.UUID, branches []BranchRecord, commits []CommitRecord, deletedMountIDs []uuid.UUID) {
	bs := slices.Clone(branches)
	cs := slices.Clone(commits)
	ms := slices.Clone(deletedMountIDs)
	q.enqueue(diskID, false, func(ctx context.Context, store Store) error {
		return store.UpsertBranchAndCommit(ctx, bs, cs, ms)
	})
}

func (q *Queue) EnqueueDeleteBranch(diskID uuid.UUID, branchName string) {
	q.enqueue(diskID, false, func(ctx context.Context, store Store) error {
		return store.DeleteBranch(ctx, diskID, branchName)
	})
}

func (q *Queue) EnqueueDeleteCommits(diskID uuid.UUID, commitIDs []uint64) {
	ids := slices.Clone(commitIDs)
	q.enqueue(diskID, false, func(ctx context.Context, store Store) error {
		return store.DeleteCommits(ctx, diskID, ids)
	})
}

func (q *Queue) EnqueueUpsertMount(mount MountRecord, updatedBranch *BranchRecord, deletedMountIDs []uuid.UUID) {
	b := cloneBranchPtr(updatedBranch)
	ms := slices.Clone(deletedMountIDs)
	q.enqueue(mount.DiskID, false, func(ctx context.Context, store Store) error {
		return store.UpsertMount(ctx, mount, b, ms)
	})
}

func (q *Queue) EnqueueDeleteMounts(diskID uuid.UUID, mountIDs []uuid.UUID, updatedBranches []BranchRecord) {
	ms := slices.Clone(mountIDs)
	bs := slices.Clone(updatedBranches)
	q.enqueue(diskID, false, func(ctx context.Context, store Store) error {
		return store.DeleteMounts(ctx, diskID, ms, bs)
	})
}

func (q *Queue) enqueue(diskID uuid.UUID, isCatalog bool, apply func(context.Context, Store) error) {
	if diskID == uuid.Nil {
		return
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed {
		return
	}
	lane, ok := q.disks[diskID]
	if !ok {
		lane = &diskLane{
			diskID: diskID,
			ch:     make(chan diskEvent, q.diskQueueSize),
			notify: make(chan struct{}),
		}
		q.disks[diskID] = lane
		q.workerWG.Add(1)
		go q.runDiskWorker(lane)
	}

	nextDiskSeq := lane.enqueuedSeq + 1
	var nextCatalogSeq uint64
	if isCatalog {
		nextCatalogSeq = q.catalogEnqueuedSeq + 1
	}
	ev := diskEvent{
		diskID:     diskID,
		diskSeq:    nextDiskSeq,
		catalogSeq: nextCatalogSeq,
		apply:      apply,
	}
	select {
	case lane.ch <- ev:
		lane.enqueuedSeq = nextDiskSeq
		if isCatalog {
			q.catalogEnqueuedSeq = nextCatalogSeq
		}
	default:
		q.markDirtyLocked(diskID)
	}
}

func (q *Queue) runDiskWorker(lane *diskLane) {
	defer q.workerWG.Done()
	for ev := range lane.ch {
		var err error
		if q.store == nil {
			err = ErrUnavailable
		} else {
			opCtx, cancel := context.WithTimeout(q.drainCtx, q.opTimeout)
			err = ev.apply(opCtx, q.store)
			cancel()
		}

		q.mu.Lock()
		if err != nil {
			q.markDirtyLocked(ev.diskID)
		}
		lane.flushedSeq = ev.diskSeq
		q.broadcastLaneLocked(lane)
		if ev.catalogSeq > 0 {
			q.markCatalogDoneLocked(ev.catalogSeq)
		}
		q.mu.Unlock()
	}
}

func (q *Queue) broadcastLaneLocked(lane *diskLane) {
	close(lane.notify)
	lane.notify = make(chan struct{})
}

func (q *Queue) broadcastCatalogLocked() {
	close(q.catalogNotify)
	q.catalogNotify = make(chan struct{})
}

func (q *Queue) markCatalogDoneLocked(seq uint64) {
	q.catalogDone[seq] = struct{}{}
	advanced := false
	for {
		next := q.catalogFlushedSeq + 1
		if _, ok := q.catalogDone[next]; !ok {
			break
		}
		delete(q.catalogDone, next)
		q.catalogFlushedSeq = next
		advanced = true
	}
	if advanced {
		q.broadcastCatalogLocked()
	}
}

func (q *Queue) markDirtyLocked(diskID uuid.UUID) uint64 {
	q.dirtyGen++
	q.dirty[diskID] = q.dirtyGen
	if lane, ok := q.disks[diskID]; ok {
		q.broadcastLaneLocked(lane)
	}
	return q.dirtyGen
}

func (q *Queue) MarkDirty(diskID uuid.UUID) uint64 {
	if diskID == uuid.Nil {
		return 0
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.markDirtyLocked(diskID)
}

func (q *Queue) IsDirty(diskID uuid.UUID) bool {
	if diskID == uuid.Nil {
		return false
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	_, ok := q.dirty[diskID]
	return ok
}

func (q *Queue) HasDirtyDisks() bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.dirty) > 0
}

func (q *Queue) TakeDirtyDisks() map[uuid.UUID]uint64 {
	q.mu.Lock()
	defer q.mu.Unlock()
	out := make(map[uuid.UUID]uint64, len(q.dirty))
	maps.Copy(out, q.dirty)
	return out
}

func (q *Queue) ClearDirty(diskID uuid.UUID, gen uint64) bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	cur, ok := q.dirty[diskID]
	if !ok || cur != gen {
		return false
	}
	delete(q.dirty, diskID)
	return true
}

func (q *Queue) RecordUsage(diskID uuid.UUID, usedBytes uint64) {
	if diskID == uuid.Nil || q.isClosed.Load() {
		return
	}
	q.batchMu.Lock()
	q.usageBuf[diskID] = usedBytes
	q.batchMu.Unlock()
}

func (q *Queue) RecordMountRenew(diskID, mountID uuid.UUID, expiresAt time.Time) {
	if diskID == uuid.Nil || mountID == uuid.Nil || q.isClosed.Load() {
		return
	}
	ts := normalizeTimestamp(expiresAt)
	key := mountBatchKey{diskID: diskID, mountID: mountID}
	q.batchMu.Lock()
	if prev, ok := q.renewBuf[key]; !ok || ts.After(prev) {
		q.renewBuf[key] = ts
	}
	q.batchMu.Unlock()
}

func (q *Queue) runBatcher() {
	defer close(q.batchDone)
	ticker := time.NewTicker(q.flushInterval)
	defer ticker.Stop()
	for {
		select {
		case <-q.stopBatcher:
			return
		case <-ticker.C:
			opCtx, cancel := context.WithTimeout(q.drainCtx, q.opTimeout)
			q.flushBatches(opCtx)
			cancel()
		}
	}
}

func (q *Queue) FlushBatches(ctx context.Context) {
	q.flushBatches(ctx)
}

func (q *Queue) flushBatches(ctx context.Context) {
	q.batchMu.Lock()
	usageMap := q.usageBuf
	if len(usageMap) > 0 {
		q.usageBuf = make(map[uuid.UUID]uint64)
	}
	renewMap := q.renewBuf
	if len(renewMap) > 0 {
		q.renewBuf = make(map[mountBatchKey]time.Time)
	}
	q.batchMu.Unlock()

	if len(usageMap) > 0 {
		updates := make([]DiskUsageUpdate, 0, len(usageMap))
		for id, used := range usageMap {
			updates = append(updates, DiskUsageUpdate{
				DiskID:    id,
				UsedBytes: used,
			})
		}
		var err error
		if q.store == nil {
			err = ErrUnavailable
		} else {
			err = q.store.BatchUpdateDiskUsage(ctx, updates)
		}
		if err != nil {
			q.mu.Lock()
			for _, u := range updates {
				q.markDirtyLocked(u.DiskID)
			}
			q.mu.Unlock()
		}
	}

	if len(renewMap) > 0 {
		updates := make([]MountRenewUpdate, 0, len(renewMap))
		for k, exp := range renewMap {
			updates = append(updates, MountRenewUpdate{
				DiskID:    k.diskID,
				MountID:   k.mountID,
				ExpiresAt: exp,
			})
		}
		var err error
		if q.store == nil {
			err = ErrUnavailable
		} else {
			err = q.store.BatchRenewMounts(ctx, updates)
		}
		if err != nil {
			q.mu.Lock()
			for _, u := range updates {
				q.markDirtyLocked(u.DiskID)
			}
			q.mu.Unlock()
		}
	}
}

func (q *Queue) WaitDisk(ctx context.Context, diskID uuid.UUID, timeout time.Duration) error {
	if diskID == uuid.Nil {
		return nil
	}
	q.mu.Lock()
	if _, dirty := q.dirty[diskID]; dirty {
		q.mu.Unlock()
		return ErrDiskDirty
	}
	lane := q.disks[diskID]
	if lane == nil || lane.flushedSeq >= lane.enqueuedSeq {
		q.mu.Unlock()
		return nil
	}
	targetSeq := lane.enqueuedSeq
	notify := lane.notify
	q.mu.Unlock()

	if timeout <= 0 {
		return ErrBarrierTimeout
	}
	timer := time.NewTimer(timeout)
	defer timer.Stop()

	for {
		select {
		case <-notify:
			q.mu.Lock()
			if _, dirty := q.dirty[diskID]; dirty {
				q.mu.Unlock()
				return ErrDiskDirty
			}
			if lane.flushedSeq >= targetSeq {
				q.mu.Unlock()
				return nil
			}
			notify = lane.notify
			q.mu.Unlock()
		case <-timer.C:
			return ErrBarrierTimeout
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (q *Queue) WaitCatalog(ctx context.Context, timeout time.Duration) error {
	q.mu.Lock()
	if q.catalogFlushedSeq >= q.catalogEnqueuedSeq {
		q.mu.Unlock()
		return nil
	}
	targetSeq := q.catalogEnqueuedSeq
	notify := q.catalogNotify
	q.mu.Unlock()

	if timeout <= 0 {
		return ErrBarrierTimeout
	}
	timer := time.NewTimer(timeout)
	defer timer.Stop()

	for {
		select {
		case <-notify:
			q.mu.Lock()
			if q.catalogFlushedSeq >= targetSeq {
				q.mu.Unlock()
				return nil
			}
			notify = q.catalogNotify
			q.mu.Unlock()
		case <-timer.C:
			return ErrBarrierTimeout
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (q *Queue) Close(ctx context.Context) error {
	q.mu.Lock()
	if q.closed {
		q.mu.Unlock()
		<-q.closeDone
		return q.closeErr
	}
	q.closed = true
	q.isClosed.Store(true)
	close(q.stopBatcher)
	for _, lane := range q.disks {
		close(lane.ch)
	}
	q.mu.Unlock()

	<-q.batchDone

	workersDone := make(chan struct{})
	go func() {
		q.workerWG.Wait()
		close(workersDone)
	}()

	var closeErr error
	select {
	case <-workersDone:
	case <-ctx.Done():
		closeErr = ctx.Err()
		q.drainCancel()
		<-workersDone
	}

	if closeErr == nil {
		q.flushBatches(ctx)
	}

	q.mu.Lock()
	for _, lane := range q.disks {
		q.broadcastLaneLocked(lane)
	}
	q.broadcastCatalogLocked()
	q.closeErr = closeErr
	q.mu.Unlock()

	q.drainCancel()
	close(q.closeDone)
	return closeErr
}
