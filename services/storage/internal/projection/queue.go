package projection

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"

	"github.com/starsrc/bowl/services/storage/internal/engine"
)

func isShutdownCancellation(ctx context.Context, err error) bool {
	return ctx != nil && ctx.Err() != nil &&
		(errors.Is(err, context.Canceled) || errors.Is(err, engine.ErrClosed))
}

const (
	defaultFlushInterval = 5 * time.Second
	defaultOpTimeout     = 10 * time.Second
)

type queueConfig struct {
	flushInterval time.Duration
	opTimeout     time.Duration
	logger        *slog.Logger
}

type dirtyState struct {
	gen     uint64
	catalog bool
}

type mountBatchKey struct {
	diskID  uuid.UUID
	mountID uuid.UUID
}

type queue struct {
	store         Store
	logger        *slog.Logger
	flushInterval time.Duration
	opTimeout     time.Duration

	mu       sync.Mutex
	closed   bool
	dirty    map[uuid.UUID]dirtyState
	dirtyGen uint64
	usageBuf map[uuid.UUID]uint64
	renewBuf map[mountBatchKey]time.Time

	drainCtx         context.Context
	drainCancel      context.CancelFunc
	stopBatcher      chan struct{}
	batchDone        chan struct{}
	closeOnce        sync.Once
	closeErr         error
	batchFlushErrors atomic.Uint64
}

func newQueue(store Store, cfg queueConfig) *queue {
	flushInterval := cfg.flushInterval
	if flushInterval <= 0 {
		flushInterval = defaultFlushInterval
	}
	opTimeout := cfg.opTimeout
	if opTimeout <= 0 {
		opTimeout = defaultOpTimeout
	}
	logger := cfg.logger
	if logger == nil {
		logger = slog.Default()
	}

	drainCtx, drainCancel := context.WithCancel(context.Background())
	q := &queue{
		store:         store,
		logger:        logger,
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

func (q *queue) markDirtyLocked(diskID uuid.UUID, isCatalog bool) uint64 {
	q.dirtyGen++
	existing := q.dirty[diskID]
	q.dirty[diskID] = dirtyState{
		gen:     q.dirtyGen,
		catalog: existing.catalog || isCatalog,
	}
	return q.dirtyGen
}

func (q *queue) markDirty(diskID uuid.UUID, isCatalog bool) uint64 {
	if diskID == uuid.Nil {
		return 0
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.markDirtyLocked(diskID, isCatalog)
}

func (q *queue) isDirty(diskID uuid.UUID) bool {
	if diskID == uuid.Nil {
		return false
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	_, ok := q.dirty[diskID]
	return ok
}

func (q *queue) dirtyGeneration(diskID uuid.UUID) (uint64, bool) {
	if diskID == uuid.Nil {
		return 0, false
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	st, ok := q.dirty[diskID]
	return st.gen, ok
}

func (q *queue) dirtyInfo(diskID uuid.UUID) (dirtyState, bool) {
	if diskID == uuid.Nil {
		return dirtyState{}, false
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	st, ok := q.dirty[diskID]
	return st, ok
}

func (q *queue) hasDirtyDisks() bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.dirty) > 0
}

func (q *queue) dirtyCounts() (total, catalog int) {
	q.mu.Lock()
	defer q.mu.Unlock()
	total = len(q.dirty)
	for _, st := range q.dirty {
		if st.catalog {
			catalog++
		}
	}
	return total, catalog
}

func (q *queue) takeDirtyDisks() map[uuid.UUID]uint64 {
	q.mu.Lock()
	defer q.mu.Unlock()
	out := make(map[uuid.UUID]uint64, len(q.dirty))
	for id, st := range q.dirty {
		out[id] = st.gen
	}
	return out
}

func (q *queue) takeDirtyCatalogDisks() map[uuid.UUID]uint64 {
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

func (q *queue) clearDirty(diskID uuid.UUID, gen uint64) bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	st, ok := q.dirty[diskID]
	if !ok || st.gen != gen {
		return false
	}
	delete(q.dirty, diskID)
	return true
}

func (q *queue) recordUsage(diskID uuid.UUID, usedBytes uint64) {
	if diskID == uuid.Nil {
		return
	}
	q.mu.Lock()
	if !q.closed {
		q.usageBuf[diskID] = usedBytes
	}
	q.mu.Unlock()
}

func (q *queue) recordMountRenew(diskID, mountID uuid.UUID, expiresAt time.Time) {
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

func (q *queue) runBatcher() {
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

func (q *queue) flushBatches(ctx context.Context) {
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
		if q.store == nil {
			q.mu.Lock()
			for _, u := range updates {
				q.markDirtyLocked(u.DiskID, false)
			}
			q.mu.Unlock()
		} else {
			start := time.Now()
			if err := q.store.BatchUpdateDiskUsage(ctx, updates); err != nil {
				q.mu.Lock()
				for _, u := range updates {
					q.markDirtyLocked(u.DiskID, false)
				}
				q.mu.Unlock()
				if !isShutdownCancellation(q.drainCtx, err) {
					q.batchFlushErrors.Add(1)
					q.logger.WarnContext(ctx, "projection batch flush failed",
						"batch_type", "usage",
						"batch_size", len(updates),
						"duration", time.Since(start),
						"error", err,
					)
				}
			}
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
		if q.store == nil {
			q.mu.Lock()
			for _, u := range updates {
				q.markDirtyLocked(u.DiskID, false)
			}
			q.mu.Unlock()
		} else {
			start := time.Now()
			if err := q.store.BatchRenewMounts(ctx, updates); err != nil {
				q.mu.Lock()
				for _, u := range updates {
					q.markDirtyLocked(u.DiskID, false)
				}
				q.mu.Unlock()
				if !isShutdownCancellation(q.drainCtx, err) {
					q.batchFlushErrors.Add(1)
					q.logger.WarnContext(ctx, "projection batch flush failed",
						"batch_type", "renew",
						"batch_size", len(updates),
						"duration", time.Since(start),
						"error", err,
					)
				}
			}
		}
	}
}

func (q *queue) close(ctx context.Context) error {
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
