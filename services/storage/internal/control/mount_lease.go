package control

import (
	"context"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/starsrc/bowl/services/storage/internal/db"
	"github.com/starsrc/bowl/services/storage/internal/store"
)

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

func validateLeaseEpoch(epoch uint64) error {
	if epoch == 0 || epoch > math.MaxInt64 {
		return fmt.Errorf("%w: writer epoch must be within [1, MaxInt64]", ErrInvalidArgument)
	}
	return nil
}

// withLiveActor 在快路径执行 actor 回调；仅当遇到因并发 MergeDisk/DeleteDisk 逐出旧实例导致的 store.ErrClosed 且 Manager 未关闭时重试一次。
func (m *manager) withLiveActor(ctx context.Context, diskID string, fn func(*store.StoreActor) error) error {
	for attempt := range 2 {
		actor, err := m.Actor(ctx, diskID)
		if err != nil {
			if attempt == 0 && errors.Is(err, store.ErrClosed) && m.ensureOpen() == nil {
				continue
			}
			return err
		}
		err = fn(actor)
		if err != nil && attempt == 0 && errors.Is(err, store.ErrClosed) && m.ensureOpen() == nil {
			continue
		}
		return err
	}
	return ErrClosed
}

func (m *manager) UnmountDisk(ctx context.Context, params UnmountDiskParams) error {
	diskUUID, err := parseDiskID(params.DiskID)
	if err != nil {
		return err
	}
	if err := validateLeaseEpoch(params.Epoch); err != nil {
		return err
	}
	if openErr := m.ensureOpen(); openErr != nil {
		return openErr
	}

	return m.withLiveActor(ctx, diskUUID.String(), func(actor *store.StoreActor) error {
		return actor.ReleaseWriterLease(ctx, params.Epoch)
	})
}

func (m *manager) RenewMount(ctx context.Context, params RenewMountParams) (*RenewMountResult, error) {
	diskUUID, err := parseDiskID(params.DiskID)
	if err != nil {
		return nil, err
	}
	if err := validateLeaseEpoch(params.Epoch); err != nil {
		return nil, err
	}
	if openErr := m.ensureOpen(); openErr != nil {
		return nil, openErr
	}

	if err := m.withLiveActor(ctx, diskUUID.String(), func(actor *store.StoreActor) error {
		return actor.RenewWriterLease(ctx, params.Epoch, m.leaseTTL())
	}); err != nil {
		return nil, err
	}
	return &RenewMountResult{
		LeaseTTLSeconds: m.leaseTTLSeconds(),
	}, nil
}

func (m *manager) checkMergeMountGate(ctx context.Context, sourceRow, targetRow *db.DiskRecord) error {
	targetKey := targetRow.DiskID.String()
	targetActor, err := m.getOrOpenActorWithLineageLocked(ctx, targetKey, targetRow.LineageID)
	if err != nil {
		return err
	}
	if targetActor.HasActiveWriterLease(ctx) {
		return fmt.Errorf("%w: target disk %s has active writer lease", ErrDiskMounted, targetKey)
	}

	sourceKey := sourceRow.DiskID.String()
	sourceActor, err := m.getOrOpenActorWithLineageLocked(ctx, sourceKey, sourceRow.LineageID)
	if err != nil {
		return err
	}
	if sourceActor.HasActiveWriterLease(ctx) {
		return fmt.Errorf("%w: source disk %s has active writer lease", ErrDiskMounted, sourceKey)
	}
	return nil
}
