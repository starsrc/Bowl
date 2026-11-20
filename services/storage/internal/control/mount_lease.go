package control

import (
	"cmp"
	"context"
	"fmt"
	"math"
	"slices"
	"time"

	"github.com/google/uuid"
)

type mountLease struct {
	readOnly  bool
	epoch     uint64
	expiresAt time.Time
}

func (m *manager) currentTime() time.Time {
	if m.now != nil {
		return m.now()
	}
	return time.Now()
}

func (m *manager) leaseTTL() time.Duration {
	if m.mountLeaseTTL > 0 {
		return m.mountLeaseTTL
	}
	return DefaultMountLeaseTTL
}

func (m *manager) leaseTTLSeconds() uint32 {
	ttl := m.leaseTTL()
	secs := int64(ttl / time.Second)
	if ttl%time.Second != 0 {
		secs++
	}
	if secs <= 0 {
		return 1
	}
	return uint32(secs)
}

func parseMountID(mountID string) (uuid.UUID, error) {
	id, err := uuid.Parse(mountID)
	if err != nil {
		return uuid.Nil, fmt.Errorf("%w: invalid mount id %q", ErrInvalidArgument, mountID)
	}
	if id == uuid.Nil {
		return uuid.Nil, fmt.Errorf("%w: mount id cannot be nil uuid", ErrInvalidArgument)
	}
	return id, nil
}

func (m *manager) pruneExpiredDiskLeasesLocked(diskID string, now time.Time) map[string]mountLease {
	leases := m.activeMounts[diskID]
	if len(leases) == 0 {
		return nil
	}
	for id, lease := range leases {
		if !lease.expiresAt.After(now) {
			delete(leases, id)
		}
	}
	if len(leases) == 0 {
		delete(m.activeMounts, diskID)
		return nil
	}
	return leases
}

func (m *manager) upsertMountLease(diskKey, mountKey string, readOnly bool, epoch uint64, isRenew bool) (uint32, error) {
	now := m.currentTime()
	ttl := m.leaseTTL()

	m.mountsMu.Lock()
	defer m.mountsMu.Unlock()

	leases := m.pruneExpiredDiskLeasesLocked(diskKey, now)
	if isRenew {
		if existing, ok := leases[mountKey]; ok && existing.readOnly != readOnly {
			return 0, fmt.Errorf("%w: mount mode mismatch for mount_id %s", ErrInvalidArgument, mountKey)
		}
	}
	if !readOnly && len(leases) > 0 {
		for id, lease := range leases {
			if !lease.readOnly && lease.epoch < epoch {
				delete(leases, id)
			}
			if !lease.readOnly && lease.epoch > epoch {
				if isRenew {
					return 0, fmt.Errorf("%w: mount epoch %d superseded by %d", ErrStaleEpoch, epoch, lease.epoch)
				}

				return m.leaseTTLSeconds(), nil
			}
		}
	}
	if leases == nil {
		leases = make(map[string]mountLease)
		m.activeMounts[diskKey] = leases
	}
	leases[mountKey] = mountLease{
		readOnly:  readOnly,
		epoch:     epoch,
		expiresAt: now.Add(ttl),
	}
	return m.leaseTTLSeconds(), nil
}

func (m *manager) issueMountLease(diskID string, readOnly bool, epoch uint64) (mountID string, ttlSeconds uint32, err error) {
	mountUUID, err := uuid.NewV7()
	if err != nil {
		return "", 0, err
	}
	mountID = mountUUID.String()
	ttlSeconds, err = m.upsertMountLease(diskID, mountID, readOnly, epoch, false)
	if err != nil {
		return "", 0, err
	}
	return mountID, ttlSeconds, nil
}

func (m *manager) UnmountDisk(_ context.Context, params UnmountDiskParams) error {
	diskUUID, err := parseDiskID(params.DiskID)
	if err != nil {
		return err
	}
	mountUUID, err := parseMountID(params.MountID)
	if err != nil {
		return err
	}
	if openErr := m.ensureOpen(); openErr != nil {
		return openErr
	}

	diskKey := diskUUID.String()
	mountKey := mountUUID.String()

	m.mountsMu.Lock()
	if leases := m.activeMounts[diskKey]; leases != nil {
		delete(leases, mountKey)
		if len(leases) == 0 {
			delete(m.activeMounts, diskKey)
		}
	}
	m.mountsMu.Unlock()
	return nil
}

func (m *manager) RenewMount(ctx context.Context, params RenewMountParams) (*RenewMountResult, error) {
	diskUUID, err := parseDiskID(params.DiskID)
	if err != nil {
		return nil, err
	}
	mountUUID, err := parseMountID(params.MountID)
	if err != nil {
		return nil, err
	}
	if params.ReadOnly && params.Epoch != 0 {
		return nil, fmt.Errorf("%w: read-only mount epoch must be 0", ErrInvalidArgument)
	}
	if !params.ReadOnly && (params.Epoch == 0 || params.Epoch > math.MaxInt64) {
		return nil, fmt.Errorf("%w: read-write mount epoch must be within [1, MaxInt64]", ErrInvalidArgument)
	}
	if openErr := m.ensureOpen(); openErr != nil {
		return nil, openErr
	}

	diskKey := diskUUID.String()
	mountKey := mountUUID.String()

	unlockDisk := m.lockDisk(ctx, diskKey)
	defer unlockDisk()

	actor, err := m.actorLocked(ctx, diskUUID)
	if err != nil {
		return nil, err
	}
	if !params.ReadOnly {
		if currentEpoch := actor.WriterEpoch(); params.Epoch != currentEpoch {
			return nil, fmt.Errorf("%w: mount epoch %d != current writer epoch %d", ErrStaleEpoch, params.Epoch, currentEpoch)
		}
	}

	ttlSeconds, err := m.upsertMountLease(diskKey, mountKey, params.ReadOnly, params.Epoch, true)
	if err != nil {
		return nil, err
	}
	return &RenewMountResult{
		LeaseTTLSeconds: ttlSeconds,
	}, nil
}

func (m *manager) clearDiskMounts(diskID string) {
	m.mountsMu.Lock()
	delete(m.activeMounts, diskID)
	m.mountsMu.Unlock()
}

func (m *manager) checkDeleteMountGate(diskID string) error {
	now := m.currentTime()

	m.mountsMu.Lock()
	defer m.mountsMu.Unlock()

	if leases := m.pruneExpiredDiskLeasesLocked(diskID, now); len(leases) > 0 {
		return fmt.Errorf("%w: disk %s has active mount lease(s)", ErrDiskMounted, diskID)
	}
	return nil
}

func (m *manager) checkRWMountGate(diskID string) error {
	now := m.currentTime()

	m.mountsMu.Lock()
	defer m.mountsMu.Unlock()

	if leases := m.pruneExpiredDiskLeasesLocked(diskID, now); len(leases) > 0 {
		for _, lease := range leases {
			if !lease.readOnly {
				return fmt.Errorf("%w: disk %s has active read-write mount lease", ErrDiskMounted, diskID)
			}
		}
	}
	return nil
}

func (m *manager) checkMergeMountGate(sourceKey, targetKey string) error {
	now := m.currentTime()

	m.mountsMu.Lock()
	defer m.mountsMu.Unlock()

	if targetLeases := m.pruneExpiredDiskLeasesLocked(targetKey, now); len(targetLeases) > 0 {
		return fmt.Errorf("%w: target disk %s has active mount lease(s)", ErrDiskMounted, targetKey)
	}
	if sourceLeases := m.pruneExpiredDiskLeasesLocked(sourceKey, now); len(sourceLeases) > 0 {
		for _, lease := range sourceLeases {
			if !lease.readOnly {
				return fmt.Errorf("%w: source disk %s has active read-write mount lease", ErrDiskMounted, sourceKey)
			}
		}
	}
	return nil
}

func (m *manager) ListMounts(ctx context.Context, diskID string) ([]MountLeaseInfo, error) {
	id, err := parseDiskID(diskID)
	if err != nil {
		return nil, err
	}
	if openErr := m.ensureOpen(); openErr != nil {
		return nil, openErr
	}

	diskKey := id.String()
	unlockDisk := m.lockDisk(ctx, diskKey)
	defer unlockDisk()

	if openErr := m.ensureOpen(); openErr != nil {
		return nil, openErr
	}
	if _, err = m.store.GetDisk(ctx, id); err != nil {
		return nil, mapDBErr(err)
	}

	now := m.currentTime()
	m.mountsMu.Lock()
	leases := m.pruneExpiredDiskLeasesLocked(diskKey, now)
	out := make([]MountLeaseInfo, 0, len(leases))
	for mountID, lease := range leases {
		out = append(out, MountLeaseInfo{
			MountID:   mountID,
			ReadOnly:  lease.readOnly,
			Epoch:     lease.epoch,
			ExpiresAt: lease.expiresAt,
		})
	}
	m.mountsMu.Unlock()

	slices.SortFunc(out, func(a, b MountLeaseInfo) int {
		return cmp.Compare(a.MountID, b.MountID)
	})
	return out, nil
}

func (m *manager) EvictMounts(ctx context.Context, params EvictMountsParams) (*EvictMountsResult, error) {
	diskUUID, err := parseDiskID(params.DiskID)
	if err != nil {
		return nil, err
	}
	var mountKey string
	if params.MountID != "" {
		mountUUID, mountErr := parseMountID(params.MountID)
		if mountErr != nil {
			return nil, mountErr
		}
		mountKey = mountUUID.String()
	}
	if openErr := m.ensureOpen(); openErr != nil {
		return nil, openErr
	}

	diskKey := diskUUID.String()
	unlockDisk := m.lockDisk(ctx, diskKey)
	defer unlockDisk()

	if openErr := m.ensureOpen(); openErr != nil {
		return nil, openErr
	}
	if _, err = m.store.GetDisk(ctx, diskUUID); err != nil {
		return nil, mapDBErr(err)
	}

	if mountKey != "" {
		m.mountsMu.Lock()
		leases := m.pruneExpiredDiskLeasesLocked(diskKey, m.currentTime())
		lease, ok := leases[mountKey]
		if !ok {
			m.mountsMu.Unlock()
			return &EvictMountsResult{}, nil
		}
		if lease.readOnly {
			delete(leases, mountKey)
			if len(leases) == 0 {
				delete(m.activeMounts, diskKey)
			}
			m.mountsMu.Unlock()
			m.logger.InfoContext(ctx, "mount lease evicted",
				"disk_id", diskKey,
				"mount_id", mountKey,
				"mode", actorModeRO,
				"evicted_count", uint32(1),
			)
			return &EvictMountsResult{EvictedCount: 1}, nil
		}
		m.mountsMu.Unlock()

		actor, actorErr := m.actorLocked(ctx, diskUUID)
		if actorErr != nil {
			return nil, actorErr
		}
		epoch, _, advErr := actor.AdvanceEpoch(ctx)
		if advErr != nil {
			return nil, advErr
		}

		m.mountsMu.Lock()
		if currentLeases := m.activeMounts[diskKey]; currentLeases != nil {
			delete(currentLeases, mountKey)
			if len(currentLeases) == 0 {
				delete(m.activeMounts, diskKey)
			}
		}
		m.mountsMu.Unlock()

		m.logger.InfoContext(ctx, "mount lease evicted",
			"disk_id", diskKey,
			"mount_id", mountKey,
			"mode", actorModeRW,
			"evicted_count", uint32(1),
			"writer_epoch", epoch,
		)
		return &EvictMountsResult{
			EvictedCount: 1,
			WriterEpoch:  epoch,
		}, nil
	}

	actor, actorErr := m.actorLocked(ctx, diskUUID)
	if actorErr != nil {
		return nil, actorErr
	}
	epoch, _, advErr := actor.AdvanceEpoch(ctx)
	if advErr != nil {
		return nil, advErr
	}

	m.mountsMu.Lock()
	leases := m.pruneExpiredDiskLeasesLocked(diskKey, m.currentTime())
	evictedCount := uint32(len(leases))
	delete(m.activeMounts, diskKey)
	m.mountsMu.Unlock()

	m.logger.InfoContext(ctx, "disk mounts evicted",
		"disk_id", diskKey,
		"evicted_count", evictedCount,
		"writer_epoch", epoch,
	)
	return &EvictMountsResult{
		EvictedCount: evictedCount,
		WriterEpoch:  epoch,
	}, nil
}
