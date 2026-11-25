package projection

import (
	"context"
	"sync"
	"time"

	"github.com/google/uuid"
)

const (
	DefaultFlushInterval = 5 * time.Second
	DefaultOpTimeout     = 10 * time.Second
)

type QueueConfig struct {
	FlushInterval time.Duration
	OpTimeout     time.Duration
}

type dirtyState struct {
	gen     uint64
	catalog bool
}

type mountBatchKey struct {
	diskID  uuid.UUID
	mountID uuid.UUID
}

type Queue struct {
	store         Store
	flushInterval time.Duration
	opTimeout     time.Duration

	mu       sync.Mutex
	closed   bool
	dirty    map[uuid.UUID]dirtyState
	dirtyGen uint64
	usageBuf map[uuid.UUID]uint64
	renewBuf map[mountBatchKey]time.Time

	drainCtx    context.Context
	drainCancel context.CancelFunc
	stopBatcher chan struct{}
	batchDone   chan struct{}
	closeOnce   sync.Once
	closeErr    error
}

func NewQueue(store Store, cfg QueueConfig) *Queue {
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
		flushInterval: flushInterval,
		opTimeout:     opTimeout,
		dirty:         make(map[uuid.UUID]dirtyState),
		usageBuf:      make(map[uuid.UUID]uint64),
		renewBuf:      make(map[mountBatchKey]time.Time),
		drainCtx:      drainCtx,
		drainCancel:   drainCancel,
		stopBatcher:   make(chan struct{}),
		batchDone:     make(chan struct{}),
	}
	go q.runBatcher()
	return q
}

func (q *Queue) markDirtyLocked(diskID uuid.UUID, isCatalog bool) uint64 {
	q.dirtyGen++
	existing := q.dirty[diskID]
	q.dirty[diskID] = dirtyState{
		gen:     q.dirtyGen,
		catalog: existing.catalog || isCatalog,
	}
	return q.dirtyGen
}

func (q *Queue) MarkDirty(diskID uuid.UUID, isCatalog bool) uint64 {
	if diskID == uuid.Nil {
		return 0
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.markDirtyLocked(diskID, isCatalog)
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

func (q *Queue) DirtyGeneration(diskID uuid.UUID) (uint64, bool) {
	if diskID == uuid.Nil {
		return 0, false
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	st, ok := q.dirty[diskID]
	return st.gen, ok
}

func (q *Queue) dirtyInfo(diskID uuid.UUID) (dirtyState, bool) {
	if diskID == uuid.Nil {
		return dirtyState{}, false
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	st, ok := q.dirty[diskID]
	return st, ok
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
	for id, st := range q.dirty {
		out[id] = st.gen
	}
	return out
}

func (q *Queue) TakeDirtyCatalogDisks() map[uuid.UUID]uint64 {
	q.mu.Lock()
	defer q.mu.Unlock()
	var out map[uuid.UUID]uint64
	for id, st := range q.dirty {
		if !st.catalog {
			continue
		}
		if out == nil {
			out = make(map[uuid.UUID]uint64)
		}
		out[id] = st.gen
	}
	return out
}

func (q *Queue) ClearDirty(diskID uuid.UUID, gen uint64) bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	st, ok := q.dirty[diskID]
	if !ok || st.gen != gen {
		return false
	}
	delete(q.dirty, diskID)
	return true
}

func (q *Queue) RecordUsage(diskID uuid.UUID, usedBytes uint64) {
	if diskID == uuid.Nil {
		return
	}
	q.mu.Lock()
	if !q.closed {
		q.usageBuf[diskID] = usedBytes
	}
	q.mu.Unlock()
}

func (q *Queue) RecordMountRenew(diskID, mountID uuid.UUID, expiresAt time.Time) {
	if diskID == uuid.Nil || mountID == uuid.Nil {
		return
	}
	ts := normalizeTimestamp(expiresAt)
	key := mountBatchKey{diskID: diskID, mountID: mountID}
	q.mu.Lock()
	if !q.closed {
		if prev, ok := q.renewBuf[key]; !ok || ts.After(prev) {
			q.renewBuf[key] = ts
		}
	}
	q.mu.Unlock()
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
	q.mu.Lock()
	var usageMap map[uuid.UUID]uint64
	if len(q.usageBuf) > 0 {
		usageMap = q.usageBuf
		q.usageBuf = make(map[uuid.UUID]uint64)
	}
	var renewMap map[mountBatchKey]time.Time
	if len(q.renewBuf) > 0 {
		renewMap = q.renewBuf
		q.renewBuf = make(map[mountBatchKey]time.Time)
	}
	q.mu.Unlock()

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
				q.markDirtyLocked(u.DiskID, false)
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
				q.markDirtyLocked(u.DiskID, false)
			}
			q.mu.Unlock()
		}
	}
}

func (q *Queue) Close(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	q.closeOnce.Do(func() {
		q.mu.Lock()
		q.closed = true
		close(q.stopBatcher)
		q.mu.Unlock()

		var closeErr error
		select {
		case <-q.batchDone:
		case <-ctx.Done():
			closeErr = ctx.Err()
			q.drainCancel()
			<-q.batchDone
		}

		if closeErr == nil {
			q.flushBatches(ctx)
			if ctx.Err() != nil {
				closeErr = ctx.Err()
			}
		}

		q.drainCancel()
		q.closeErr = closeErr
	})
	return q.closeErr
}
