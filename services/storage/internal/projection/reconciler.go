package projection

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/google/uuid"

	"github.com/starsrc/bowl/services/storage/internal/engine"
)

func (m *ControlManager) ReconcileDisk(ctx context.Context, diskID uuid.UUID) error {
	return m.reconcileDisk(ctx, diskID, false, false)
}

func (m *ControlManager) reconcileDisk(ctx context.Context, diskID uuid.UUID, onlyIfDirty, onlyIfCatalog bool) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if diskID == uuid.Nil {
		return fmt.Errorf("%w: disk_id cannot be nil", ErrInvalidArgument)
	}
	if m.store == nil {
		return ErrUnavailable
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	slot := m.getSlot(diskID)
	slot.opMu.Lock()
	defer slot.opMu.Unlock()

	if err := ctx.Err(); err != nil {
		return err
	}

	st, hasDirty := m.queue.dirtyInfo(diskID)
	if onlyIfDirty && (!hasDirty || (onlyIfCatalog && !st.catalog)) {
		return nil
	}
	gen := st.gen

	if m.closed.Load() {
		return engine.ErrClosed
	}
	entry := lookupOpenEntry(slot, diskID)

	if entry == nil && !diskExistsOnFS(m.diskDir(diskID)) {
		if err := m.store.DeleteDisk(ctx, diskID); err != nil {
			return err
		}
		if hasDirty {
			m.queue.ClearDirty(diskID, gen)
		}
		return nil
	}

	var (
		rawSnap engine.ControlSnapshot
		err     error
	)
	now := m.now()
	if entry != nil {
		rawSnap, err = entry.store.SnapshotControlMeta(now)
	} else {
		err = m.withTempDisk(diskID, func(ds *engine.DiskStore) error {
			var sErr error
			rawSnap, sErr = ds.SnapshotControlMeta(now)
			return sErr
		})
	}
	if err != nil {
		return err
	}

	snap := controlSnapshotToDiskSnapshot(diskID, rawSnap)
	if repErr := m.store.ReplaceDiskSnapshot(ctx, snap); repErr != nil {
		return repErr
	}
	if hasDirty {
		m.queue.ClearDirty(diskID, gen)
	}
	return nil
}

func (m *ControlManager) ReconcileAll(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if m.store == nil {
		return ErrUnavailable
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	entries, err := os.ReadDir(m.disksDir)
	if err != nil {
		return err
	}

	var errs []error
	for _, dirEntry := range entries {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return ctxErr
		}
		if !dirEntry.IsDir() {
			continue
		}
		diskID, parseErr := uuid.Parse(dirEntry.Name())
		if parseErr != nil || diskID == uuid.Nil {
			continue
		}
		if !diskExistsOnFS(m.diskDir(diskID)) {
			continue
		}
		if recErr := m.ReconcileDisk(ctx, diskID); recErr != nil {
			errs = append(errs, recErr)
		}
	}

	pgIDs, listErr := m.store.ListAllDiskIDs(ctx)
	if listErr != nil {
		errs = append(errs, listErr)
	} else {
		for _, id := range pgIDs {
			if ctxErr := ctx.Err(); ctxErr != nil {
				return ctxErr
			}
			slot := m.getSlot(id)
			slot.opMu.Lock()
			if m.closed.Load() {
				slot.opMu.Unlock()
				return engine.ErrClosed
			}
			exists := lookupOpenEntry(slot, id) != nil || diskExistsOnFS(m.diskDir(id))
			if !exists {
				gen, hasDirty := m.queue.DirtyGeneration(id)
				if delErr := m.store.DeleteDisk(ctx, id); delErr != nil {
					errs = append(errs, delErr)
				} else if hasDirty {
					m.queue.ClearDirty(id, gen)
				}
			}
			slot.opMu.Unlock()
		}
	}

	if _, pruneErr := m.store.PruneExpiredMounts(ctx, m.now()); pruneErr != nil {
		errs = append(errs, pruneErr)
	}

	return errors.Join(errs...)
}

func (m *ControlManager) ReconcileDirtyDisks(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if m.store == nil {
		return nil
	}
	dirty := m.queue.TakeDirtyDisks()
	var errs []error
	for diskID := range dirty {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := m.reconcileDisk(ctx, diskID, true, false); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

func (m *ControlManager) SampleOpenDiskUsage(ctx context.Context) {
	if ctx == nil {
		ctx = context.Background()
	}
	for i := range m.slots {
		if ctx.Err() != nil {
			return
		}
		if entries := m.slots[i].disks.Load(); entries == nil || len(*entries) == 0 {
			continue
		}
		m.slots[i].opMu.RLock()
		if !m.closed.Load() {
			if entries := m.slots[i].disks.Load(); entries != nil {
				for _, item := range *entries {
					used := item.entry.store.UsedBytes()
					usedInt64 := int64(used)
					if item.entry.lastSampledUsed.Load() != usedInt64 {
						item.entry.lastSampledUsed.Store(usedInt64)
						m.queue.RecordUsage(item.id, used)
					}
				}
			}
		}
		m.slots[i].opMu.RUnlock()
	}
}

func (m *ControlManager) EvictIdleDisks(ctx context.Context) int {
	if m.actorIdleTTL <= 0 {
		return 0
	}
	if ctx == nil {
		ctx = context.Background()
	}

	evicted := 0
	ttlNano := m.actorIdleTTL.Nanoseconds()
	var (
		hasNow  bool
		nowNano int64
	)
	for i := range m.slots {
		if ctx.Err() != nil {
			break
		}
		entries := m.slots[i].disks.Load()
		if entries == nil || len(*entries) == 0 {
			continue
		}
		if !hasNow {
			nowNano = m.now().UnixNano()
			hasNow = true
		}
		for _, item := range *entries {
			if ctx.Err() != nil {
				break
			}
			if nowNano-item.entry.lastUsedNano.Load() < ttlNano {
				continue
			}
			if m.tryEvictIdleDisk(ctx, &m.slots[i], item.id, ttlNano) {
				evicted++
			}
		}
	}
	return evicted
}

func (m *ControlManager) tryEvictIdleDisk(ctx context.Context, slot *diskSlot, diskID uuid.UUID, ttlNano int64) bool {
	slot.opMu.Lock()
	defer slot.opMu.Unlock()

	if m.closed.Load() {
		return false
	}
	entry := lookupOpenEntry(slot, diskID)
	if entry == nil {
		return false
	}

	now := m.now()
	if now.UnixNano()-entry.lastUsedNano.Load() < ttlNano {
		return false
	}

	mounted, err := entry.store.HasActiveMounts("", now)
	if err != nil || mounted {
		entry.store.EvictIdleActors(m.actorIdleTTL)
		return false
	}

	m.queue.RecordUsage(diskID, entry.store.UsedBytes())
	flushCtx, cancel := context.WithTimeout(ctx, m.syncWriteTimeout)
	m.queue.FlushBatches(flushCtx)
	cancel()

	slot.remove(diskID)
	_ = entry.store.Close()
	return true
}

func (m *ControlManager) runMaintenanceLoop() {
	reconcileTicker := time.NewTicker(m.reconcileInterval)
	defer reconcileTicker.Stop()

	var evictCh <-chan time.Time
	if m.actorIdleTTL > 0 {
		interval := min(max(m.actorIdleTTL/2, 5*time.Millisecond), time.Second)
		evictTicker := time.NewTicker(interval)
		defer evictTicker.Stop()
		evictCh = evictTicker.C
	}

	for {
		select {
		case <-m.bgCtx.Done():
			return
		case <-reconcileTicker.C:
			opCtx, cancel := context.WithTimeout(m.bgCtx, m.opTimeout)
			m.SampleOpenDiskUsage(opCtx)
			_ = m.ReconcileDirtyDisks(opCtx)
			if m.store != nil {
				_, _ = m.store.PruneExpiredMounts(opCtx, m.now())
			}
			cancel()
		case <-evictCh:
			opCtx, cancel := context.WithTimeout(m.bgCtx, m.opTimeout)
			m.EvictIdleDisks(opCtx)
			cancel()
		}
	}
}
