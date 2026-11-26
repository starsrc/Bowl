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

func (m *ControlManager) isShutdownCancellation(err error) bool {
	return isShutdownCancellation(m.bgCtx, err)
}

func (m *ControlManager) recordReconcileErr(err error) error {
	if err != nil && !m.isShutdownCancellation(err) {
		m.reconcileErrors.Add(1)
	}
	return err
}

func (m *ControlManager) reconcileDisk(ctx context.Context, diskID uuid.UUID, onlyIfDirty, onlyIfCatalog bool) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if diskID == uuid.Nil {
		return m.recordReconcileErr(fmt.Errorf("%w: disk_id cannot be nil", ErrInvalidArgument))
	}
	if m.store == nil {
		return ErrUnavailable
	}
	if err := ctx.Err(); err != nil {
		return m.recordReconcileErr(err)
	}

	slot := m.getSlot(diskID)
	slot.opMu.Lock()
	defer slot.opMu.Unlock()

	if err := ctx.Err(); err != nil {
		return m.recordReconcileErr(err)
	}

	st, hasDirty := m.queue.dirtyInfo(diskID)
	if onlyIfDirty && (!hasDirty || (onlyIfCatalog && !st.catalog)) {
		return nil
	}
	gen := st.gen

	if m.closed.Load() {
		return m.recordReconcileErr(engine.ErrClosed)
	}
	entry := lookupOpenEntry(slot, diskID)

	if entry == nil && !diskExistsOnFS(m.diskDir(diskID)) {
		if err := m.store.DeleteDisk(ctx, diskID); err != nil {
			return m.recordReconcileErr(err)
		}
		if hasDirty {
			m.queue.clearDirty(diskID, gen)
		}
		m.reconcileDisks.Add(1)
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
		return m.recordReconcileErr(err)
	}

	snap := m.controlSnapshotToDiskSnapshot(ctx, diskID, rawSnap)
	if repErr := m.store.ReplaceDiskSnapshot(ctx, snap); repErr != nil {
		return m.recordReconcileErr(repErr)
	}
	if hasDirty {
		m.queue.clearDirty(diskID, gen)
	}
	m.reconcileDisks.Add(1)
	return nil
}

func (m *ControlManager) reconcileAll(ctx context.Context) error {
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
		if recErr := m.reconcileDisk(ctx, diskID, false, false); recErr != nil {
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
				gen, hasDirty := m.queue.dirtyGeneration(id)
				if delErr := m.store.DeleteDisk(ctx, id); delErr != nil {
					errs = append(errs, delErr)
				} else if hasDirty {
					m.queue.clearDirty(id, gen)
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

func (m *ControlManager) reconcileDirtyDisks(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if m.store == nil {
		return nil
	}
	dirty := m.queue.takeDirtyDisks()
	if len(dirty) == 0 {
		return nil
	}
	start := time.Now()
	var (
		errs         []error
		firstErr     error
		sampleDiskID uuid.UUID
		failedDisks  int
	)
	for diskID := range dirty {
		if err := ctx.Err(); err != nil {
			if !m.isShutdownCancellation(err) {
				if firstErr == nil {
					firstErr = err
					sampleDiskID = diskID
				}
				failedDisks++
				m.logger.WarnContext(ctx, "projection dirty reconcile cycle failed",
					"dirty_disks", len(dirty),
					"failed_disks", failedDisks,
					"sample_disk_id", sampleDiskID.String(),
					"duration", time.Since(start),
					"error", firstErr,
				)
			}
			return err
		}
		if err := m.reconcileDisk(ctx, diskID, true, false); err != nil {
			if m.isShutdownCancellation(err) {
				return err
			}
			errs = append(errs, err)
			failedDisks++
			if firstErr == nil {
				firstErr = err
				sampleDiskID = diskID
			}
			m.logger.DebugContext(ctx, "projection dirty disk reconcile failed",
				"op", "reconcile_dirty_disk",
				"disk_id", diskID.String(),
				"error", err,
			)
		}
	}
	if failedDisks > 0 {
		m.logger.WarnContext(ctx, "projection dirty reconcile cycle failed",
			"dirty_disks", len(dirty),
			"failed_disks", failedDisks,
			"sample_disk_id", sampleDiskID.String(),
			"duration", time.Since(start),
			"error", firstErr,
		)
	}
	return errors.Join(errs...)
}

func (m *ControlManager) sampleOpenDiskUsage(ctx context.Context) {
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
						m.queue.recordUsage(item.id, used)
					}
				}
			}
		}
		m.slots[i].opMu.RUnlock()
	}
}

func (m *ControlManager) evictIdleDisks(ctx context.Context) int {
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

	mounted, err := m.hasActiveMounts(entry.store, "", now)
	if err != nil {
		m.logger.WarnContext(ctx, "projection idle eviction active mount check failed",
			"op", "evict_idle_disk",
			"disk_id", diskID.String(),
			"error", err,
		)
		entry.store.EvictIdleActors(m.actorIdleTTL)
		return false
	}
	if mounted {
		entry.store.EvictIdleActors(m.actorIdleTTL)
		return false
	}

	m.queue.recordUsage(diskID, entry.store.UsedBytes())
	flushCtx, cancel := context.WithTimeout(ctx, m.syncWriteTimeout)
	m.queue.flushBatches(flushCtx)
	cancel()

	slot.remove(diskID)
	if closeErr := m.closeDisk(entry.store); closeErr != nil {
		m.logger.WarnContext(ctx, "projection idle eviction disk close failed",
			"op", "evict_idle_disk",
			"disk_id", diskID.String(),
			"error", closeErr,
		)
	}
	m.idleEvictions.Add(1)
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
			m.sampleOpenDiskUsage(opCtx)
			_ = m.reconcileDirtyDisks(opCtx)
			if m.store != nil {
				pruneStart := time.Now()
				if _, pruneErr := m.store.PruneExpiredMounts(opCtx, m.now()); pruneErr != nil && !m.isShutdownCancellation(pruneErr) {
					m.logger.WarnContext(opCtx, "projection prune expired mounts failed",
						"op", "prune_expired_mounts",
						"duration", time.Since(pruneStart),
						"error", pruneErr,
					)
				}
			}
			cancel()
		case <-evictCh:
			opCtx, cancel := context.WithTimeout(m.bgCtx, m.opTimeout)
			m.evictIdleDisks(opCtx)
			cancel()
		}
	}
}
