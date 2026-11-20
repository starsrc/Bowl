package control

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"

	"github.com/starsrc/bowl/services/storage/internal/db"
	"github.com/starsrc/bowl/services/storage/internal/store"
)

const (
	DefaultUsageSyncInterval  = 5 * time.Second
	maxActorIdleSweepInterval = 30 * time.Second
	closeUsageFlushTimeout    = 5 * time.Second
)

type actorEntry struct {
	diskUUID       uuid.UUID
	actor          *store.StoreActor
	lastUsedAtNano atomic.Int64

	syncMu           sync.Mutex
	syncedUsedBytes  uint64
	syncedUsedInodes uint64
	hasSynced        bool
}

func newActorEntry(diskUUID uuid.UUID, actor *store.StoreActor, nowNano int64, usedBytes, usedInodes uint64) *actorEntry {
	e := &actorEntry{
		diskUUID:         diskUUID,
		actor:            actor,
		syncedUsedBytes:  usedBytes,
		syncedUsedInodes: usedInodes,
		hasSynced:        true,
	}
	e.lastUsedAtNano.Store(nowNano)
	return e
}

func (e *actorEntry) touch(nowNano int64) {
	e.lastUsedAtNano.Store(nowNano)
}

func idleSweepInterval(ttl time.Duration) time.Duration {
	if ttl <= 0 {
		return 0
	}
	interval := min(ttl/2, maxActorIdleSweepInterval)
	if interval < time.Millisecond {
		return time.Millisecond
	}
	return interval
}

func (m *manager) getWarmEntry(diskID string) *actorEntry {
	m.poolMu.RLock()
	defer m.poolMu.RUnlock()
	if m.closed {
		return nil
	}
	return m.actors[diskID]
}

func (m *manager) flushEntryUsage(ctx context.Context, entry *actorEntry) error {
	if entry == nil || entry.actor == nil || m.store == nil {
		return nil
	}
	entry.syncMu.Lock()
	defer entry.syncMu.Unlock()
	stat, err := entry.actor.StatFS(ctx)
	if err != nil {
		return err
	}
	if entry.hasSynced && entry.syncedUsedBytes == stat.UsedBytes && entry.syncedUsedInodes == stat.Files {
		return nil
	}
	if err := m.store.UpdateDiskUsage(ctx, entry.diskUUID, stat.UsedBytes, stat.Files); err != nil {
		if errors.Is(err, db.ErrNotFound) {
			return nil
		}
		return err
	}
	entry.syncedUsedBytes = stat.UsedBytes
	entry.syncedUsedInodes = stat.Files
	entry.hasSynced = true
	return nil
}

func (m *manager) startBackgroundLoops() {
	m.bgCtx, m.bgCancel = context.WithCancel(context.Background())
	if m.usageSyncInterval > 0 {
		m.bgWG.Go(func() {
			m.runUsageSyncLoop(m.bgCtx)
		})
	}
	if m.actorIdleTTL > 0 {
		m.bgWG.Go(func() {
			m.runIdleEvictionLoop(m.bgCtx)
		})
	}
}

func (m *manager) runUsageSyncLoop(ctx context.Context) {
	ticker := time.NewTicker(m.usageSyncInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			m.syncDirtyUsage(ctx)
		}
	}
}

func (m *manager) syncDirtyUsage(ctx context.Context) {
	if m.store == nil {
		return
	}
	m.poolMu.RLock()
	if m.closed || len(m.actors) == 0 {
		m.poolMu.RUnlock()
		return
	}
	diskIDs := make([]string, 0, len(m.actors))
	for diskID, entry := range m.actors {
		if entry != nil {
			diskIDs = append(diskIDs, diskID)
		}
	}
	m.poolMu.RUnlock()

	for _, diskID := range diskIDs {
		if ctx.Err() != nil {
			return
		}
		m.syncDirtyDisk(ctx, diskID)
	}
}

func (m *manager) syncDirtyDisk(ctx context.Context, diskID string) {
	unlockDisk := m.lockDisk(ctx, diskID)
	defer unlockDisk()

	m.poolMu.RLock()
	if m.closed {
		m.poolMu.RUnlock()
		return
	}
	entry := m.actors[diskID]
	m.poolMu.RUnlock()
	if entry == nil {
		return
	}

	if err := m.flushEntryUsage(ctx, entry); err != nil && ctx.Err() == nil && !errors.Is(err, store.ErrClosed) {
		m.logger.WarnContext(ctx, "background usage sync failed",
			"disk_id", diskID,
			"err", err,
		)
	}
}

func (m *manager) runIdleEvictionLoop(ctx context.Context) {
	interval := idleSweepInterval(m.actorIdleTTL)
	if interval <= 0 {
		return
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			m.sweepIdleActors(ctx)
		}
	}
}

func (m *manager) sweepIdleActors(ctx context.Context) {
	if m.actorIdleTTL <= 0 || m.store == nil {
		return
	}
	if _, pruneErr := m.store.PruneExpiredMountLeases(ctx, m.currentTime()); pruneErr != nil && ctx.Err() == nil {
		m.logger.WarnContext(ctx, "prune expired mount leases failed", "err", pruneErr)
	}

	nowNano := m.currentTime().UnixNano()
	idleTTLNs := m.actorIdleTTL.Nanoseconds()

	m.poolMu.RLock()
	if m.closed || len(m.actors) == 0 {
		m.poolMu.RUnlock()
		return
	}
	var candidates []string
	for diskID, entry := range m.actors {
		if entry != nil && nowNano-entry.lastUsedAtNano.Load() >= idleTTLNs {
			candidates = append(candidates, diskID)
		}
	}
	m.poolMu.RUnlock()

	for _, diskID := range candidates {
		if ctx.Err() != nil {
			return
		}
		m.evictIdleActor(ctx, diskID)
	}
}

func (m *manager) evictIdleActor(ctx context.Context, diskID string) {
	unlockDisk := m.lockDisk(ctx, diskID)
	defer unlockDisk()

	idleTTLNs := m.actorIdleTTL.Nanoseconds()

	m.poolMu.RLock()
	if m.closed {
		m.poolMu.RUnlock()
		return
	}
	entry := m.actors[diskID]
	if entry == nil || m.currentTime().UnixNano()-entry.lastUsedAtNano.Load() < idleTTLNs {
		m.poolMu.RUnlock()
		return
	}
	m.poolMu.RUnlock()

	leases, err := m.store.ListActiveMountLeases(ctx, entry.diskUUID, m.currentTime())
	if err != nil {
		if ctx.Err() == nil {
			m.logger.WarnContext(ctx, "idle eviction mount lease check failed",
				"disk_id", diskID,
				"err", err,
			)
		}
		return
	}
	if len(leases) > 0 {
		return
	}

	if flushErr := m.flushEntryUsage(ctx, entry); flushErr != nil {
		if ctx.Err() == nil && !errors.Is(flushErr, store.ErrClosed) {
			m.logger.WarnContext(ctx, "idle eviction usage flush failed",
				"disk_id", diskID,
				"err", flushErr,
			)
		}
		return
	}

	m.poolMu.Lock()
	if m.closed || m.actors[diskID] != entry || m.currentTime().UnixNano()-entry.lastUsedAtNano.Load() < idleTTLNs {
		m.poolMu.Unlock()
		return
	}
	delete(m.actors, diskID)
	m.poolMu.Unlock()

	mode := actorModeLabel(entry.actor.ReadOnly())
	if closeErr := entry.actor.Close(); closeErr != nil {
		m.collector.incCleanupError()
		m.logger.WarnContext(ctx, "idle eviction actor close failed",
			"disk_id", diskID,
			"mode", mode,
			"err", closeErr,
		)
	}
	m.collector.incActorEviction()
	m.logger.InfoContext(ctx, "idle actor evicted",
		"disk_id", diskID,
		"mode", mode,
	)
}
