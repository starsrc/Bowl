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

	unlock := m.lockDisk(diskID)
	defer unlock()

	if err := m.queue.WaitLaneIdle(ctx, diskID, m.opTimeout); err != nil {
		return err
	}

	gen, hasDirty := m.queue.DirtyGeneration(diskID)

	m.poolMu.RLock()
	if m.closed {
		m.poolMu.RUnlock()
		return engine.ErrClosed
	}
	entry, isOpen := m.disks[diskID]
	m.poolMu.RUnlock()

	if !isOpen && !diskExistsOnFS(m.diskDir(diskID)) {
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
	if isOpen {
		rawSnap, err = entry.store.SnapshotControlMeta(now)
	} else {
		var ds *engine.DiskStore
		ds, err = m.openDiskRaw(diskID, "", 0, false)
		if err == nil {
			rawSnap, err = ds.SnapshotControlMeta(now)
			closeErr := ds.Close()
			if err == nil {
				err = closeErr
			}
		}
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
			unlock := m.lockDisk(id)
			m.poolMu.RLock()
			_, isOpen := m.disks[id]
			m.poolMu.RUnlock()
			exists := isOpen || diskExistsOnFS(m.diskDir(id))
			if !exists {
				gen, hasDirty := m.queue.DirtyGeneration(id)
				if delErr := m.store.DeleteDisk(ctx, id); delErr != nil {
					errs = append(errs, delErr)
				} else if hasDirty {
					m.queue.ClearDirty(id, gen)
				}
			}
			unlock()
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
		if err := m.ReconcileDisk(ctx, diskID); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

func (m *ControlManager) SampleOpenDiskUsage(ctx context.Context) {
	if ctx == nil {
		ctx = context.Background()
	}
	m.poolMu.RLock()
	ids := make([]uuid.UUID, 0, len(m.disks))
	for id := range m.disks {
		ids = append(ids, id)
	}
	m.poolMu.RUnlock()

	for _, diskID := range ids {
		if ctx.Err() != nil {
			return
		}
		unlock := m.rlockDisk(diskID)
		m.poolMu.RLock()
		entry, ok := m.disks[diskID]
		m.poolMu.RUnlock()
		if ok {
			if st, stErr := entry.store.StatFS(ctx); stErr == nil {
				if !entry.hasSampled.Load() || entry.lastSampledUsed.Load() != st.UsedBytes {
					entry.lastSampledUsed.Store(st.UsedBytes)
					entry.hasSampled.Store(true)
					m.queue.RecordUsage(diskID, st.UsedBytes)
				}
			}
		}
		unlock()
	}
}

func (m *ControlManager) EvictIdleDisks(ctx context.Context) int {
	if m.actorIdleTTL <= 0 {
		return 0
	}
	if ctx == nil {
		ctx = context.Background()
	}

	m.poolMu.RLock()
	ids := make([]uuid.UUID, 0, len(m.disks))
	for id := range m.disks {
		ids = append(ids, id)
	}
	m.poolMu.RUnlock()

	evicted := 0
	ttlNano := m.actorIdleTTL.Nanoseconds()
	for _, diskID := range ids {
		if ctx.Err() != nil {
			break
		}
		if m.tryEvictIdleDisk(ctx, diskID, ttlNano) {
			evicted++
		}
	}
	return evicted
}

func (m *ControlManager) tryEvictIdleDisk(ctx context.Context, diskID uuid.UUID, ttlNano int64) bool {
	unlock := m.lockDisk(diskID)
	defer unlock()

	m.poolMu.RLock()
	entry, ok := m.disks[diskID]
	m.poolMu.RUnlock()
	if !ok {
		return false
	}
	if entry.inFlight.Load() > 0 {
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

	if st, stErr := entry.store.StatFS(ctx); stErr == nil {
		_ = m.queue.WaitDisk(ctx, diskID, m.barrierTimeout)
		m.queue.RecordUsage(diskID, st.UsedBytes)
		m.queue.FlushBatches(ctx)
	}

	m.poolMu.Lock()
	delete(m.disks, diskID)
	m.poolMu.Unlock()

	_ = entry.store.Close()
	return true
}

func (m *ControlManager) runReconcileLoop() {
	ticker := time.NewTicker(m.reconcileInterval)
	defer ticker.Stop()

	for {
		select {
		case <-m.bgCtx.Done():
			return
		case <-ticker.C:
			opCtx, cancel := context.WithTimeout(m.bgCtx, m.opTimeout)
			m.SampleOpenDiskUsage(opCtx)
			_ = m.ReconcileDirtyDisks(opCtx)
			if m.store != nil {
				_, _ = m.store.PruneExpiredMounts(opCtx, m.now())
			}
			cancel()
		}
	}
}

func (m *ControlManager) runEvictLoop() {
	interval := min(max(m.actorIdleTTL/2, 5*time.Millisecond), time.Second)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-m.bgCtx.Done():
			return
		case <-ticker.C:
			opCtx, cancel := context.WithTimeout(m.bgCtx, m.opTimeout)
			m.EvictIdleDisks(opCtx)
			cancel()
		}
	}
}
