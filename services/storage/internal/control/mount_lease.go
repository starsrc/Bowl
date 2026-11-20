package control

import (
	"context"
	"fmt"
	"math"
	"time"

	"github.com/google/uuid"

	"github.com/starsrc/bowl/services/storage/internal/db"
)

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

func (m *manager) issueMountLease(ctx context.Context, diskUUID uuid.UUID, readOnly bool, epoch uint64) (mountID string, ttlSeconds uint32, err error) {
	mountUUID, err := uuid.NewV7()
	if err != nil {
		return "", 0, err
	}
	now := m.currentTime()
	if _, err = m.store.CreateMountLease(ctx, db.CreateMountLeaseInput{
		DiskID:    diskUUID,
		MountID:   mountUUID,
		ReadOnly:  readOnly,
		Epoch:     epoch,
		Now:       now,
		ExpiresAt: now.Add(m.leaseTTL()),
	}); err != nil {
		return "", 0, mapDBErr(err)
	}
	return mountUUID.String(), m.leaseTTLSeconds(), nil
}

func (m *manager) UnmountDisk(ctx context.Context, params UnmountDiskParams) error {
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
	unlockDisk := m.lockDisk(ctx, diskKey)
	defer unlockDisk()

	if openErr := m.ensureOpen(); openErr != nil {
		return openErr
	}
	if entry := m.getWarmEntry(diskKey); entry != nil {
		if flushErr := m.flushEntryUsage(ctx, entry); flushErr != nil {
			m.logger.WarnContext(ctx, "unmount disk usage flush failed",
				"disk_id", diskKey,
				"err", flushErr,
			)
		}
	}
	if _, delErr := m.store.DeleteMountLease(ctx, diskUUID, mountUUID); delErr != nil {
		return mapDBErr(delErr)
	}
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
	unlockDisk := m.lockDisk(ctx, diskKey)
	defer unlockDisk()

	if openErr := m.ensureOpen(); openErr != nil {
		return nil, openErr
	}
	entry, err := m.actorEntryLocked(ctx, diskUUID)
	if err != nil {
		return nil, err
	}
	if !params.ReadOnly {
		if currentEpoch := entry.actor.WriterEpoch(); params.Epoch != currentEpoch {
			return nil, fmt.Errorf("%w: mount epoch %d != current writer epoch %d", ErrStaleEpoch, params.Epoch, currentEpoch)
		}
	}

	now := m.currentTime()
	if _, err = m.store.RenewMountLease(ctx, db.RenewMountLeaseInput{
		DiskID:    diskUUID,
		MountID:   mountUUID,
		ReadOnly:  params.ReadOnly,
		Epoch:     params.Epoch,
		Now:       now,
		ExpiresAt: now.Add(m.leaseTTL()),
	}); err != nil {
		return nil, mapDBErr(err)
	}
	return &RenewMountResult{
		LeaseTTLSeconds: m.leaseTTLSeconds(),
	}, nil
}

func (m *manager) clearDiskMounts(ctx context.Context, diskUUID uuid.UUID) (uint32, error) {
	count, err := m.store.DeleteDiskMountLeases(ctx, diskUUID, m.currentTime())
	if err != nil {
		return 0, mapDBErr(err)
	}
	return count, nil
}

func (m *manager) checkDeleteMountGate(ctx context.Context, diskUUID uuid.UUID) error {
	leases, err := m.store.ListActiveMountLeases(ctx, diskUUID, m.currentTime())
	if err != nil {
		return mapDBErr(err)
	}
	if len(leases) > 0 {
		return fmt.Errorf("%w: disk %s has active mount lease(s)", ErrDiskMounted, diskUUID.String())
	}
	return nil
}

func (m *manager) checkRWMountGate(ctx context.Context, diskUUID uuid.UUID) error {
	leases, err := m.store.ListActiveMountLeases(ctx, diskUUID, m.currentTime())
	if err != nil {
		return mapDBErr(err)
	}
	for _, lease := range leases {
		if !lease.ReadOnly {
			return fmt.Errorf("%w: disk %s has active read-write mount lease", ErrDiskMounted, diskUUID.String())
		}
	}
	return nil
}

func (m *manager) checkMergeMountGate(ctx context.Context, sourceUUID, targetUUID uuid.UUID) error {
	now := m.currentTime()
	targetLeases, err := m.store.ListActiveMountLeases(ctx, targetUUID, now)
	if err != nil {
		return mapDBErr(err)
	}
	if len(targetLeases) > 0 {
		return fmt.Errorf("%w: target disk %s has active mount lease(s)", ErrDiskMounted, targetUUID.String())
	}
	sourceLeases, err := m.store.ListActiveMountLeases(ctx, sourceUUID, now)
	if err != nil {
		return mapDBErr(err)
	}
	for _, lease := range sourceLeases {
		if !lease.ReadOnly {
			return fmt.Errorf("%w: source disk %s has active read-write mount lease", ErrDiskMounted, sourceUUID.String())
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

	leases, err := m.store.ListActiveMountLeases(ctx, id, m.currentTime())
	if err != nil {
		return nil, mapDBErr(err)
	}
	out := make([]MountLeaseInfo, 0, len(leases))
	for _, lease := range leases {
		out = append(out, MountLeaseInfo{
			MountID:   lease.MountID.String(),
			ReadOnly:  lease.ReadOnly,
			Epoch:     lease.Epoch,
			ExpiresAt: lease.ExpiresAt,
		})
	}
	return out, nil
}

func (m *manager) EvictMounts(ctx context.Context, params EvictMountsParams) (*EvictMountsResult, error) {
	diskUUID, err := parseDiskID(params.DiskID)
	if err != nil {
		return nil, err
	}
	var mountUUID uuid.UUID
	if params.MountID != "" {
		parsedMountID, mountErr := parseMountID(params.MountID)
		if mountErr != nil {
			return nil, mountErr
		}
		mountUUID = parsedMountID
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

	if mountUUID != uuid.Nil {
		leases, listErr := m.store.ListActiveMountLeases(ctx, diskUUID, m.currentTime())
		if listErr != nil {
			return nil, mapDBErr(listErr)
		}
		var targetLease *db.MountLeaseRecord
		for i := range leases {
			if leases[i].MountID == mountUUID {
				targetLease = &leases[i]
				break
			}
		}
		if targetLease == nil {
			return &EvictMountsResult{}, nil
		}
		if targetLease.ReadOnly {
			if _, delErr := m.store.DeleteMountLease(ctx, diskUUID, mountUUID); delErr != nil {
				return nil, mapDBErr(delErr)
			}
			m.logger.InfoContext(ctx, "mount lease evicted",
				"disk_id", diskKey,
				"mount_id", mountUUID.String(),
				"mode", actorModeRO,
				"evicted_count", uint32(1),
			)
			return &EvictMountsResult{EvictedCount: 1}, nil
		}

		entry, actorErr := m.actorEntryLocked(ctx, diskUUID)
		if actorErr != nil {
			return nil, actorErr
		}
		epoch, _, advErr := entry.actor.AdvanceEpoch(ctx)
		if advErr != nil {
			return nil, advErr
		}
		if flushErr := m.flushEntryUsage(ctx, entry); flushErr != nil {
			m.logger.WarnContext(ctx, "evict rw mount usage flush failed",
				"disk_id", diskKey,
				"err", flushErr,
			)
		}
		if _, delErr := m.store.DeleteMountLease(ctx, diskUUID, mountUUID); delErr != nil {
			return nil, mapDBErr(delErr)
		}

		m.logger.InfoContext(ctx, "mount lease evicted",
			"disk_id", diskKey,
			"mount_id", mountUUID.String(),
			"mode", actorModeRW,
			"evicted_count", uint32(1),
			"writer_epoch", epoch,
		)
		return &EvictMountsResult{
			EvictedCount: 1,
			WriterEpoch:  epoch,
		}, nil
	}

	entry, actorErr := m.actorEntryLocked(ctx, diskUUID)
	if actorErr != nil {
		return nil, actorErr
	}
	epoch, _, advErr := entry.actor.AdvanceEpoch(ctx)
	if advErr != nil {
		return nil, advErr
	}
	if flushErr := m.flushEntryUsage(ctx, entry); flushErr != nil {
		m.logger.WarnContext(ctx, "evict disk mounts usage flush failed",
			"disk_id", diskKey,
			"err", flushErr,
		)
	}

	evictedCount, delErr := m.clearDiskMounts(ctx, diskUUID)
	if delErr != nil {
		return nil, delErr
	}

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
