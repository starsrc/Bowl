package db_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"go.uber.org/fx"

	"github.com/starsrc/bowl/services/storage/internal/config"
	"github.com/starsrc/bowl/services/storage/internal/db"
)

func TestMemStore_GivenInjectedWriteError_WhenForkDiskCalled_ThenRollsBackAtomically(t *testing.T) {
	ctx := context.Background()
	mem := db.NewMemStore()

	rootID := mustNewUUIDv7(t)
	_, err := mem.CreateDisk(ctx, rootID, "root", testDefaultMaxBytes)
	require.NoError(t, err)

	injected := errors.New("injected write error")
	mem.SetFailNextWrite(injected)
	_, err = mem.ForkDisk(ctx, db.ForkDiskInput{
		SourceDiskID: rootID,
		ChildDiskID:  mustNewUUIDv7(t),
		SnapshotID:   mustNewUUIDv7(t),
		Name:         "fail-child",
	})
	require.ErrorIs(t, err, injected)

	snapAfterFail := mem.Inspect()
	require.Len(t, snapAfterFail.Disks, 1, "disk count after injected failure want=1, got=%d", len(snapAfterFail.Disks))
	require.Empty(t, snapAfterFail.Snapshots, "injected failure must not leave snapshot records")

	childID := mustNewUUIDv7(t)
	_, err = mem.ForkDisk(ctx, db.ForkDiskInput{
		SourceDiskID: rootID,
		ChildDiskID:  childID,
		SnapshotID:   mustNewUUIDv7(t),
		Name:         "recovered-child",
	})
	require.NoError(t, err)

	snapRecovered := mem.Inspect()
	require.Len(t, snapRecovered.Disks, 2, "disk count after recovery want=2, got=%d", len(snapRecovered.Disks))
	require.Len(t, snapRecovered.Snapshots, 1, "snapshot count after recovery want=1, got=%d", len(snapRecovered.Snapshots))
}

func TestMemStore_GivenInjectedWriteError_WhenMergeDiskCalled_ThenRollsBackAtomically(t *testing.T) {
	ctx := context.Background()
	mem := db.NewMemStore()

	rootID := mustNewUUIDv7(t)
	root, err := mem.CreateDisk(ctx, rootID, "root", testDefaultMaxBytes)
	require.NoError(t, err)

	childID, snapFork := mustNewUUIDv7(t), mustNewUUIDv7(t)
	_, err = mem.ForkDisk(ctx, db.ForkDiskInput{
		SourceDiskID: rootID,
		ChildDiskID:  childID,
		SnapshotID:   snapFork,
		Name:         "child",
	})
	require.NoError(t, err)

	injected := errors.New("injected merge write error")
	mem.SetFailNextWrite(injected)
	_, err = mem.MergeDisk(ctx, &db.MergeDiskInput{
		LineageID:                    root.LineageID,
		SourceDiskID:                 childID,
		TargetDiskID:                 rootID,
		ExpectedSourceBaseSnapshotID: snapFork,
		ExpectedTargetBaseSnapshotID: snapFork,
		NewSourceSnapshotID:          mustNewUUIDv7(t),
		NewMergeSnapshotID:           mustNewUUIDv7(t),
	})
	require.ErrorIs(t, err, injected)

	snapAfterFail := mem.Inspect()
	require.Len(t, snapAfterFail.Snapshots, 1, "injected failure must not leave new snapshots")
	require.Equal(t, snapFork, snapAfterFail.Disks[rootID].BaseSnapshotID)
	require.Equal(t, snapFork, snapAfterFail.Disks[childID].BaseSnapshotID)
}

func TestMemStore_GivenCanceledContext_WhenMethodsCalled_ThenFailsFast(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	mem := db.NewMemStore()
	id := mustNewUUIDv7(t)
	mntID := mustNewUUIDv7(t)
	now := time.Date(2026, 11, 20, 12, 0, 0, 0, time.UTC)
	name := "x"

	_, err := mem.CreateDisk(ctx, id, "canceled", testDefaultMaxBytes)
	require.ErrorIs(t, err, context.Canceled)
	_, err = mem.ForkDisk(ctx, db.ForkDiskInput{SourceDiskID: id, ChildDiskID: mustNewUUIDv7(t), SnapshotID: mustNewUUIDv7(t), Name: "c"})
	require.ErrorIs(t, err, context.Canceled)
	_, err = mem.GetDisk(ctx, id)
	require.ErrorIs(t, err, context.Canceled)
	_, err = mem.ListDisks(ctx, db.ListDisksQuery{})
	require.ErrorIs(t, err, context.Canceled)
	_, err = mem.ListLineageDisks(ctx, id)
	require.ErrorIs(t, err, context.Canceled)
	_, err = mem.UpdateDisk(ctx, db.UpdateDiskInput{DiskID: id, Name: &name})
	require.ErrorIs(t, err, context.Canceled)
	err = mem.UpdateDiskUsage(ctx, id, 0, 1)
	require.ErrorIs(t, err, context.Canceled)
	_, err = mem.DeleteDisk(ctx, id, false)
	require.ErrorIs(t, err, context.Canceled)
	_, err = mem.ListLineageSnapshots(ctx, id)
	require.ErrorIs(t, err, context.Canceled)
	_, err = mem.MergeDisk(ctx, &db.MergeDiskInput{
		LineageID:           id,
		SourceDiskID:        id,
		TargetDiskID:        mustNewUUIDv7(t),
		NewSourceSnapshotID: mustNewUUIDv7(t),
		NewMergeSnapshotID:  mustNewUUIDv7(t),
	})
	require.ErrorIs(t, err, context.Canceled)
	_, err = mem.ReserveSegment(ctx, db.SegmentKindInode, id, 2, 10)
	require.ErrorIs(t, err, context.Canceled)
	_, err = mem.CreateMountLease(ctx, db.CreateMountLeaseInput{DiskID: id, MountID: mntID, ReadOnly: true, Now: now, ExpiresAt: now.Add(time.Second)})
	require.ErrorIs(t, err, context.Canceled)
	_, err = mem.RenewMountLease(ctx, db.RenewMountLeaseInput{DiskID: id, MountID: mntID, ReadOnly: true, Now: now, ExpiresAt: now.Add(time.Second)})
	require.ErrorIs(t, err, context.Canceled)
	_, err = mem.DeleteMountLease(ctx, id, mntID)
	require.ErrorIs(t, err, context.Canceled)
	_, err = mem.DeleteDiskMountLeases(ctx, id, now)
	require.ErrorIs(t, err, context.Canceled)
	_, err = mem.ListActiveMountLeases(ctx, id, now)
	require.ErrorIs(t, err, context.Canceled)
	_, err = mem.PruneExpiredMountLeases(ctx, now)
	require.ErrorIs(t, err, context.Canceled)
}

func TestMemStore_GivenReserveAndUsageCalls_WhenInspected_ThenTracksCountersAndMounts(t *testing.T) {
	ctx := context.Background()
	mem := db.NewMemStore()
	now := time.Date(2026, 11, 20, 12, 0, 0, 0, time.UTC)

	rootID := mustNewUUIDv7(t)
	_, err := mem.CreateDisk(ctx, rootID, "root", testDefaultMaxBytes)
	require.NoError(t, err)

	_, err = mem.ReserveSegment(ctx, db.SegmentKindInode, rootID, 2, 10)
	require.NoError(t, err)
	_, err = mem.ReserveSegment(ctx, db.SegmentKindPack, rootID, 1, 10)
	require.NoError(t, err)
	require.NoError(t, mem.UpdateDiskUsage(ctx, rootID, 1024, 2))
	mntID := mustNewUUIDv7(t)
	_, err = mem.CreateMountLease(ctx, db.CreateMountLeaseInput{
		DiskID:    rootID,
		MountID:   mntID,
		ReadOnly:  true,
		Now:       now,
		ExpiresAt: now.Add(30 * time.Second),
	})
	require.NoError(t, err)

	snap := mem.Inspect()
	require.Equal(t, 1, snap.InodeReserveCalls, "InodeReserveCalls want=1, got=%d", snap.InodeReserveCalls)
	require.Equal(t, 1, snap.PackReserveCalls, "PackReserveCalls want=1, got=%d", snap.PackReserveCalls)
	require.Equal(t, 1, snap.UpdateUsageCalls, "UpdateUsageCalls want=1, got=%d", snap.UpdateUsageCalls)
	require.Len(t, snap.Mounts[rootID], 1)
}

func TestMemStore_GivenTriStateDelete_WhenInspected_ThenClearsSurvivorBaseSnapshot(t *testing.T) {
	ctx := context.Background()
	mem := db.NewMemStore()

	rootID := mustNewUUIDv7(t)
	_, err := mem.CreateDisk(ctx, rootID, "root", testDefaultMaxBytes)
	require.NoError(t, err)

	childID, snapID := mustNewUUIDv7(t), mustNewUUIDv7(t)
	_, err = mem.ForkDisk(ctx, db.ForkDiskInput{
		SourceDiskID: rootID,
		ChildDiskID:  childID,
		SnapshotID:   snapID,
		Name:         "child",
	})
	require.NoError(t, err)
	require.Equal(t, snapID, mem.Inspect().Disks[childID].BaseSnapshotID)

	_, err = mem.DeleteDisk(ctx, rootID, false)
	require.NoError(t, err)
	snapAfterDelete := mem.Inspect()
	require.Equal(t, uuid.Nil, snapAfterDelete.Disks[childID].BaseSnapshotID, "remaining==1 must clear survivor BaseSnapshotID")
	require.Empty(t, snapAfterDelete.Snapshots)
}

func TestDBModule_GivenInvalidOrValidPoolConfig_WhenOpenPoolOrFxValidated_ThenMatchesContract(t *testing.T) {
	_, err := db.OpenPool(config.DBPoolConfig{DSN: "://bad-dsn"})
	require.Error(t, err, "invalid DSN must return error")

	poolCfg := config.DBPoolConfig{
		DSN:            resolveMetaTestDSN(),
		TracingEnabled: true,
		MetricsEnabled: false,
	}
	require.NoError(t, fx.ValidateApp(fx.Supply(poolCfg), db.Module), "db.Module fx validation should succeed")
}
