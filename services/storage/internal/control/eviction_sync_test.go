package control

import (
	"context"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/starsrc/bowl/services/storage/internal/config"
	"github.com/starsrc/bowl/services/storage/internal/db"
	"github.com/starsrc/bowl/services/storage/internal/store"
)

func gatherCounterLabelValue(t *testing.T, reg *prometheus.Registry, metricName, labelKey, labelVal string) float64 {
	t.Helper()
	fams, err := reg.Gather()
	require.NoError(t, err)
	for _, fam := range fams {
		if fam.GetName() != metricName {
			continue
		}
		for _, m := range fam.GetMetric() {
			if hasMetricLabel(m, labelKey, labelVal) && m.GetCounter() != nil {
				return m.GetCounter().GetValue()
			}
		}
	}
	return 0
}

func TestActorEviction_GivenIdleUnmountedActor_WhenTTLElapses_ThenFlushesDirtyUsageEvictsAndIncrementsMetric(t *testing.T) {
	ctx := context.Background()
	clk := newFakeClock(time.Date(2026, 11, 20, 10, 0, 0, 0, time.UTC))
	memDB := db.NewMemStore()
	mgr := newManager(
		memDB,
		WithStorageRoot(t.TempDir()),
		WithClock(clk.Now),
		WithActorIdleTTL(40*time.Millisecond),
		WithUsageSyncInterval(0),
	)
	t.Cleanup(func() { _ = mgr.Close() })

	reg := prometheus.NewRegistry()
	require.NoError(t, reg.Register(mgr.Collector()))

	disk, err := mgr.CreateDisk(ctx, CreateDiskParams{Name: "evict-dirty", MaxBytes: 16 << 20})
	require.NoError(t, err)
	mnt, err := mgr.MountDisk(ctx, MountDiskParams{DiskID: disk.DiskID})
	require.NoError(t, err)
	actor, err := mgr.Actor(ctx, disk.DiskID)
	require.NoError(t, err)
	writeRegularFile(t, ctx, actor, mnt.Epoch, "a.bin", make([]byte, 4096))

	require.NoError(t, mgr.UnmountDisk(ctx, UnmountDiskParams{DiskID: disk.DiskID, MountID: mnt.MountID}))
	writeRegularFile(t, ctx, actor, mnt.Epoch, "b.bin", make([]byte, 2048))

	beforeEvict, err := mgr.GetDisk(ctx, disk.DiskID)
	require.NoError(t, err)
	require.Equal(t, uint64(4096), beforeEvict.UsedBytes)

	clk.Advance(50 * time.Millisecond)

	require.EventuallyWithT(t, func(c *assert.CollectT) {
		assert.InDelta(c, 1.0, gatherCounterValue(t, reg, "bowl_storage_control_actor_evictions_total"), 1e-9)
		assert.InDelta(c, 0.0, gatherMetricLabelValue(t, reg, "bowl_storage_disks_loaded", "mode", "rw"), 1e-9)
	}, 2*time.Second, 10*time.Millisecond)

	afterEvict, err := mgr.GetDisk(ctx, disk.DiskID)
	require.NoError(t, err)
	require.Equal(t, uint64(6144), afterEvict.UsedBytes)
	require.Equal(t, uint64(3), afterEvict.UsedInodes)
	require.InDelta(t, 0.0, gatherMetricLabelValue(t, reg, "bowl_storage_disks_loaded", "mode", "rw"), 1e-9)

	reopened, err := mgr.Actor(ctx, disk.DiskID)
	require.NoError(t, err)
	require.Equal(t, mnt.Epoch, reopened.WriterEpoch())
	require.InDelta(t, 1.0, gatherMetricLabelValue(t, reg, "bowl_storage_disks_loaded", "mode", "rw"), 1e-9)
}

func TestActorEviction_GivenMountedOrRecentlyAccessedActor_WhenSwept_ThenRetainsProtectedActorsAndEvictsGetDiskOnlyActor(t *testing.T) {
	ctx := context.Background()
	clk := newFakeClock(time.Date(2026, 11, 20, 10, 0, 0, 0, time.UTC))
	memDB := db.NewMemStore()
	mgr := newManager(
		memDB,
		WithStorageRoot(t.TempDir()),
		WithClock(clk.Now),
		WithActorIdleTTL(60*time.Millisecond),
		WithUsageSyncInterval(0),
	)
	t.Cleanup(func() { _ = mgr.Close() })

	reg := prometheus.NewRegistry()
	require.NoError(t, reg.Register(mgr.Collector()))

	mountedDisk, err := mgr.CreateDisk(ctx, CreateDiskParams{Name: "mounted", MaxBytes: 8 << 20})
	require.NoError(t, err)
	_, err = mgr.MountDisk(ctx, MountDiskParams{DiskID: mountedDisk.DiskID, ReadOnly: true})
	require.NoError(t, err)

	activeDisk, err := mgr.CreateDisk(ctx, CreateDiskParams{Name: "active-actor", MaxBytes: 8 << 20})
	require.NoError(t, err)

	metadataOnlyDisk, err := mgr.CreateDisk(ctx, CreateDiskParams{Name: "getdisk-only", MaxBytes: 8 << 20})
	require.NoError(t, err)

	clk.Advance(40 * time.Millisecond)
	_, err = mgr.Actor(ctx, activeDisk.DiskID)
	require.NoError(t, err)
	_, err = mgr.GetDisk(ctx, metadataOnlyDisk.DiskID)
	require.NoError(t, err)
	_, err = mgr.ListDisks(ctx, ListDisksParams{})
	require.NoError(t, err)

	clk.Advance(30 * time.Millisecond)

	require.EventuallyWithT(t, func(c *assert.CollectT) {
		assert.InDelta(c, 1.0, gatherCounterValue(t, reg, "bowl_storage_control_actor_evictions_total"), 1e-9)
		assert.InDelta(c, 2.0, gatherMetricLabelValue(t, reg, "bowl_storage_disks_loaded", "mode", "rw"), 1e-9)
	}, 2*time.Second, 10*time.Millisecond)

	mgr.poolMu.RLock()
	_, hasMounted := mgr.actors[mountedDisk.DiskID]
	_, hasActive := mgr.actors[activeDisk.DiskID]
	_, hasMetaOnly := mgr.actors[metadataOnlyDisk.DiskID]
	mgr.poolMu.RUnlock()

	require.True(t, hasMounted, "mounted actor must not be evicted")
	require.True(t, hasActive, "recently accessed actor must not be evicted")
	require.False(t, hasMetaOnly, "GetDisk and ListDisks must not refresh lastUsedAt")
}

func TestActorEviction_GivenZeroIdleTTL_WhenClockAdvances_ThenDisablesEviction(t *testing.T) {
	ctx := context.Background()
	clk := newFakeClock(time.Date(2026, 11, 20, 10, 0, 0, 0, time.UTC))
	mgr := newManager(
		db.NewMemStore(),
		WithStorageRoot(t.TempDir()),
		WithClock(clk.Now),
		WithActorIdleTTL(0),
		WithUsageSyncInterval(0),
	)
	t.Cleanup(func() { _ = mgr.Close() })

	reg := prometheus.NewRegistry()
	require.NoError(t, reg.Register(mgr.Collector()))

	disk, err := mgr.CreateDisk(ctx, CreateDiskParams{Name: "no-evict", MaxBytes: 8 << 20})
	require.NoError(t, err)

	clk.Advance(24 * time.Hour)
	mgr.sweepIdleActors(ctx)

	require.InDelta(t, 0.0, gatherCounterValue(t, reg, "bowl_storage_control_actor_evictions_total"), 1e-9)
	require.InDelta(t, 1.0, gatherMetricLabelValue(t, reg, "bowl_storage_disks_loaded", "mode", "rw"), 1e-9)
	mgr.poolMu.RLock()
	_, loaded := mgr.actors[disk.DiskID]
	mgr.poolMu.RUnlock()
	require.True(t, loaded)
}

func TestManagerModule_GivenMetaConfigActorIdleTTL_WhenConstructed_ThenWiresIdleTTL(t *testing.T) {
	dir := t.TempDir()
	mgr := newManagerFromParams(ManagerParams{
		Store: db.NewMemStore(),
		MetaCfg: &config.MetaConfig{
			DataDirVal:      dir,
			ActorIdleTTLVal: 2 * time.Minute,
		},
	})
	t.Cleanup(func() { _ = mgr.Close() })

	impl, ok := mgr.(*manager)
	require.True(t, ok)
	require.Equal(t, 2*time.Minute, impl.actorIdleTTL)
	require.Equal(t, 30*time.Second, idleSweepInterval(impl.actorIdleTTL))

	disabled := newManagerFromParams(ManagerParams{
		Store: db.NewMemStore(),
		MetaCfg: &config.MetaConfig{
			DataDirVal:      t.TempDir(),
			ActorIdleTTLVal: 0,
		},
	})
	t.Cleanup(func() { _ = disabled.Close() })
	disabledImpl, ok := disabled.(*manager)
	require.True(t, ok)
	require.Zero(t, disabledImpl.actorIdleTTL)
}

func TestGetDiskAndListDisks_GivenColdDisksAfterRestart_WhenQueried_ThenReadPGWithoutOpeningPebble(t *testing.T) {
	ctx := context.Background()
	rootDir := t.TempDir()
	memDB := db.NewMemStore()

	mgr1 := newManager(memDB, WithStorageRoot(rootDir), WithUsageSyncInterval(0))
	d1, err := mgr1.CreateDisk(ctx, CreateDiskParams{Name: "cold-1", MaxBytes: 8 << 20})
	require.NoError(t, err)
	mnt1, err := mgr1.MountDisk(ctx, MountDiskParams{DiskID: d1.DiskID})
	require.NoError(t, err)
	a1, err := mgr1.Actor(ctx, d1.DiskID)
	require.NoError(t, err)
	writeRegularFile(t, ctx, a1, mnt1.Epoch, "one.bin", make([]byte, 3072))

	d2, err := mgr1.CreateDisk(ctx, CreateDiskParams{Name: "cold-2", MaxBytes: 12 << 20})
	require.NoError(t, err)
	require.NoError(t, mgr1.Close())

	mgr2 := newManager(memDB, WithStorageRoot(rootDir), WithUsageSyncInterval(0))
	t.Cleanup(func() { _ = mgr2.Close() })
	reg := prometheus.NewRegistry()
	require.NoError(t, reg.Register(mgr2.Collector()))

	got1, err := mgr2.GetDisk(ctx, d1.DiskID)
	require.NoError(t, err)
	require.Equal(t, uint64(8<<20), got1.MaxBytes)
	require.Equal(t, uint64(3072), got1.UsedBytes)
	require.Equal(t, uint64(2), got1.UsedInodes)

	listed, err := mgr2.ListDisks(ctx, ListDisksParams{PageSize: 10})
	require.NoError(t, err)
	require.Len(t, listed.Disks, 2)
	byID := make(map[string]*Disk, len(listed.Disks))
	for _, d := range listed.Disks {
		byID[d.DiskID] = d
	}
	require.Equal(t, uint64(3072), byID[d1.DiskID].UsedBytes)
	require.Equal(t, uint64(12<<20), byID[d2.DiskID].MaxBytes)
	require.Equal(t, uint64(1), byID[d2.DiskID].UsedInodes)

	require.InDelta(t, 0.0, gatherMetricLabelValue(t, reg, "bowl_storage_disks_loaded", "mode", "rw"), 1e-9)
	require.InDelta(t, 0.0, gatherCounterLabelValue(t, reg, "bowl_storage_control_actor_loads_total", "mode", "rw"), 1e-9)
}

func TestUsageSync_GivenBackgroundSyncLoop_WhenDiskMutatedAndIdle_ThenFlushesDirtyOnly(t *testing.T) {
	ctx := context.Background()
	memDB := db.NewMemStore()
	mgr := newManager(
		memDB,
		WithStorageRoot(t.TempDir()),
		WithActorIdleTTL(0),
		WithUsageSyncInterval(20*time.Millisecond),
	)
	t.Cleanup(func() { _ = mgr.Close() })

	disk, err := mgr.CreateDisk(ctx, CreateDiskParams{Name: "bg-sync", MaxBytes: 8 << 20})
	require.NoError(t, err)

	time.Sleep(70 * time.Millisecond)
	require.Zero(t, memDB.Inspect().UpdateUsageCalls, "clean disk must not trigger redundant UpdateDiskUsage writes")

	mnt, err := mgr.MountDisk(ctx, MountDiskParams{DiskID: disk.DiskID})
	require.NoError(t, err)
	actor, err := mgr.Actor(ctx, disk.DiskID)
	require.NoError(t, err)
	writeRegularFile(t, ctx, actor, mnt.Epoch, "dirty.bin", make([]byte, 5120))

	require.EventuallyWithT(t, func(c *assert.CollectT) {
		got, getErr := mgr.GetDisk(ctx, disk.DiskID)
		require.NoError(c, getErr)
		assert.Equal(c, uint64(5120), got.UsedBytes)
		assert.Equal(c, uint64(2), got.UsedInodes)
	}, 2*time.Second, 10*time.Millisecond)

	callsAfterSync := memDB.Inspect().UpdateUsageCalls
	require.Equal(t, 1, callsAfterSync, "dirty write should trigger exactly 1 UpdateDiskUsage call")

	time.Sleep(70 * time.Millisecond)
	require.Equal(t, callsAfterSync, memDB.Inspect().UpdateUsageCalls, "subsequent ticks without mutations must skip UpdateDiskUsage")
}

func TestUpdateDisk_GivenColdDisk_WhenResizedOrShrunk_ThenUpdatesPGWithoutOpeningPebbleAndAlignsOnOpen(t *testing.T) {
	ctx := context.Background()
	rootDir := t.TempDir()
	memDB := db.NewMemStore()

	mgr1 := newManager(memDB, WithStorageRoot(rootDir), WithUsageSyncInterval(0))
	disk, err := mgr1.CreateDisk(ctx, CreateDiskParams{Name: "cold-resize", MaxBytes: 8 << 20})
	require.NoError(t, err)
	mnt, err := mgr1.MountDisk(ctx, MountDiskParams{DiskID: disk.DiskID})
	require.NoError(t, err)
	actor1, err := mgr1.Actor(ctx, disk.DiskID)
	require.NoError(t, err)
	writeRegularFile(t, ctx, actor1, mnt.Epoch, "data.bin", make([]byte, 8192))
	require.NoError(t, mgr1.Close())

	mgr2 := newManager(memDB, WithStorageRoot(rootDir), WithUsageSyncInterval(0))
	t.Cleanup(func() { _ = mgr2.Close() })
	reg := prometheus.NewRegistry()
	require.NoError(t, reg.Register(mgr2.Collector()))

	tooSmall := uint64(4096)
	_, err = mgr2.UpdateDisk(ctx, UpdateDiskParams{DiskID: disk.DiskID, MaxBytes: &tooSmall})
	require.ErrorIs(t, err, ErrInvalidShrink)
	require.InDelta(t, 0.0, gatherMetricLabelValue(t, reg, "bowl_storage_disks_loaded", "mode", "rw"), 1e-9)

	newName := "cold-resized"
	newQuota := uint64(24 << 20)
	updated, err := mgr2.UpdateDisk(ctx, UpdateDiskParams{
		DiskID:   disk.DiskID,
		Name:     &newName,
		MaxBytes: &newQuota,
	})
	require.NoError(t, err)
	require.Equal(t, newName, updated.Name)
	require.Equal(t, newQuota, updated.MaxBytes)
	require.Equal(t, uint64(8192), updated.UsedBytes)
	require.InDelta(t, 0.0, gatherMetricLabelValue(t, reg, "bowl_storage_disks_loaded", "mode", "rw"), 1e-9)
	require.InDelta(t, 0.0, gatherCounterLabelValue(t, reg, "bowl_storage_control_actor_loads_total", "mode", "rw"), 1e-9)

	actor2, err := mgr2.Actor(ctx, disk.DiskID)
	require.NoError(t, err)
	stat, err := actor2.StatFS(ctx)
	require.NoError(t, err)
	require.Equal(t, newQuota, stat.TotalBytes, "opening cold-resized disk must align Pebble MaxBytes to PG")
	require.Equal(t, uint64(8192), stat.UsedBytes)
}

func TestUpdateDisk_GivenWarmDiskWithUnflushedWrites_WhenShrunkOrExpanded_ThenValidatesLiveUsageAndUpdatesBoth(t *testing.T) {
	ctx := context.Background()
	memDB := db.NewMemStore()
	mgr := newManager(memDB, WithStorageRoot(t.TempDir()), WithUsageSyncInterval(0))
	t.Cleanup(func() { _ = mgr.Close() })

	disk, err := mgr.CreateDisk(ctx, CreateDiskParams{Name: "warm-resize", MaxBytes: 8 << 20})
	require.NoError(t, err)
	mnt, err := mgr.MountDisk(ctx, MountDiskParams{DiskID: disk.DiskID})
	require.NoError(t, err)
	actor, err := mgr.Actor(ctx, disk.DiskID)
	require.NoError(t, err)
	writeRegularFile(t, ctx, actor, mnt.Epoch, "live.bin", make([]byte, 8192))

	pgBefore, err := mgr.GetDisk(ctx, disk.DiskID)
	require.NoError(t, err)
	require.Zero(t, pgBefore.UsedBytes)

	tooSmall := uint64(4096)
	_, err = mgr.UpdateDisk(ctx, UpdateDiskParams{DiskID: disk.DiskID, MaxBytes: &tooSmall})
	require.ErrorIs(t, err, ErrInvalidShrink)

	pgAfterReject, err := mgr.GetDisk(ctx, disk.DiskID)
	require.NoError(t, err)
	require.Equal(t, uint64(8<<20), pgAfterReject.MaxBytes)

	expanded := uint64(20 << 20)
	res, err := mgr.UpdateDisk(ctx, UpdateDiskParams{DiskID: disk.DiskID, MaxBytes: &expanded})
	require.NoError(t, err)
	require.Equal(t, expanded, res.MaxBytes)
	require.Equal(t, uint64(8192), res.UsedBytes)

	st, err := actor.StatFS(ctx)
	require.NoError(t, err)
	require.Equal(t, expanded, st.TotalBytes)
}

func TestRenewMount_GivenStrictPGLeases_WhenEvictedUnmountedExpiredOrModeMismatched_ThenNeverResurrects(t *testing.T) {
	ctx := context.Background()
	clk := newFakeClock(time.Date(2026, 11, 20, 12, 0, 0, 0, time.UTC))
	memDB := db.NewMemStore()
	mgr := newManager(memDB, WithStorageRoot(t.TempDir()), WithClock(clk.Now))
	t.Cleanup(func() { _ = mgr.Close() })

	disk, err := mgr.CreateDisk(ctx, CreateDiskParams{Name: "strict-renew", MaxBytes: 16 << 20})
	require.NoError(t, err)

	rwMnt, err := mgr.MountDisk(ctx, MountDiskParams{DiskID: disk.DiskID, ReadOnly: false})
	require.NoError(t, err)
	roMnt, err := mgr.MountDisk(ctx, MountDiskParams{DiskID: disk.DiskID, ReadOnly: true})
	require.NoError(t, err)

	_, err = mgr.RenewMount(ctx, RenewMountParams{
		DiskID:   disk.DiskID,
		MountID:  rwMnt.MountID,
		ReadOnly: true,
		Epoch:    0,
	})
	require.ErrorIs(t, err, ErrInvalidArgument)

	_, err = mgr.RenewMount(ctx, RenewMountParams{
		DiskID:   disk.DiskID,
		MountID:  roMnt.MountID,
		ReadOnly: false,
		Epoch:    rwMnt.Epoch,
	})
	require.ErrorIs(t, err, ErrInvalidArgument)

	evictRes, err := mgr.EvictMounts(ctx, EvictMountsParams{DiskID: disk.DiskID, MountID: roMnt.MountID})
	require.NoError(t, err)
	require.Equal(t, uint32(1), evictRes.EvictedCount)

	_, err = mgr.RenewMount(ctx, RenewMountParams{
		DiskID:   disk.DiskID,
		MountID:  roMnt.MountID,
		ReadOnly: true,
		Epoch:    0,
	})
	require.ErrorIs(t, err, ErrNotFound)

	require.NoError(t, mgr.UnmountDisk(ctx, UnmountDiskParams{DiskID: disk.DiskID, MountID: rwMnt.MountID}))
	_, err = mgr.RenewMount(ctx, RenewMountParams{
		DiskID:   disk.DiskID,
		MountID:  rwMnt.MountID,
		ReadOnly: false,
		Epoch:    rwMnt.Epoch,
	})
	require.ErrorIs(t, err, ErrNotFound)

	rw2, err := mgr.MountDisk(ctx, MountDiskParams{DiskID: disk.DiskID, ReadOnly: false})
	require.NoError(t, err)
	clk.Advance(DefaultMountLeaseTTL + time.Second)

	_, err = mgr.RenewMount(ctx, RenewMountParams{
		DiskID:   disk.DiskID,
		MountID:  rw2.MountID,
		ReadOnly: false,
		Epoch:    rw2.Epoch,
	})
	require.ErrorIs(t, err, ErrNotFound)

	active, err := mgr.ListMounts(ctx, disk.DiskID)
	require.NoError(t, err)
	require.Empty(t, active, "failed RenewMount calls must never resurrect leases")
}

type hookUpdateDiskStore struct {
	*db.MemStore
	onUpdateDisk func()
}

func (h *hookUpdateDiskStore) UpdateDisk(ctx context.Context, input db.UpdateDiskInput) (db.DiskRecord, error) {
	rec, err := h.MemStore.UpdateDisk(ctx, input)
	if err == nil && h.onUpdateDisk != nil {
		fn := h.onUpdateDisk
		h.onUpdateDisk = nil
		fn()
	}
	return rec, err
}

func TestUpdateDisk_GivenConcurrentWriteDuringWarmResize_WhenActorRejectsShrink_ThenRollsBackPG(t *testing.T) {
	ctx := context.Background()
	memDB := db.NewMemStore()
	hookStore := &hookUpdateDiskStore{MemStore: memDB}
	mgr := newManager(hookStore, WithStorageRoot(t.TempDir()), WithUsageSyncInterval(0))
	t.Cleanup(func() { _ = mgr.Close() })

	disk, err := mgr.CreateDisk(ctx, CreateDiskParams{Name: "orig-name", MaxBytes: 16 << 20})
	require.NoError(t, err)
	mnt, err := mgr.MountDisk(ctx, MountDiskParams{DiskID: disk.DiskID})
	require.NoError(t, err)
	actor, err := mgr.Actor(ctx, disk.DiskID)
	require.NoError(t, err)

	hookStore.onUpdateDisk = func() {
		writeRegularFile(t, ctx, actor, mnt.Epoch, "concurrent.bin", make([]byte, 16384))
	}

	newName := "should-rollback"
	shrinkTarget := uint64(8192)
	_, err = mgr.UpdateDisk(ctx, UpdateDiskParams{
		DiskID:   disk.DiskID,
		Name:     &newName,
		MaxBytes: &shrinkTarget,
	})
	require.ErrorIs(t, err, ErrInvalidShrink)

	after, err := mgr.GetDisk(ctx, disk.DiskID)
	require.NoError(t, err)
	require.Equal(t, "orig-name", after.Name, "PG name must roll back after actor shrink rejection")
	require.Equal(t, uint64(16<<20), after.MaxBytes, "PG max_bytes must roll back after actor shrink rejection")
}

func TestUsageSync_GivenBackgroundSyncDisabled_WhenUnmountForkMergeAndCloseRun_ThenFlushesSynchronously(t *testing.T) {
	ctx := context.Background()
	rootDir := t.TempDir()
	memDB := db.NewMemStore()

	mgr1 := newManager(memDB, WithStorageRoot(rootDir), WithActorIdleTTL(0), WithUsageSyncInterval(0))
	base, err := mgr1.CreateDisk(ctx, CreateDiskParams{Name: "base", MaxBytes: 16 << 20})
	require.NoError(t, err)
	mnt, err := mgr1.MountDisk(ctx, MountDiskParams{DiskID: base.DiskID})
	require.NoError(t, err)
	baseActor, err := mgr1.Actor(ctx, base.DiskID)
	require.NoError(t, err)
	writeRegularFile(t, ctx, baseActor, mnt.Epoch, "f1.bin", make([]byte, 4096))

	beforeUnmount, err := mgr1.GetDisk(ctx, base.DiskID)
	require.NoError(t, err)
	require.Zero(t, beforeUnmount.UsedBytes)

	require.NoError(t, mgr1.UnmountDisk(ctx, UnmountDiskParams{DiskID: base.DiskID, MountID: mnt.MountID}))
	afterUnmount, err := mgr1.GetDisk(ctx, base.DiskID)
	require.NoError(t, err)
	require.Equal(t, uint64(4096), afterUnmount.UsedBytes)
	require.Equal(t, uint64(2), afterUnmount.UsedInodes)

	child, err := mgr1.ForkDisk(ctx, ForkDiskParams{SourceDiskID: base.DiskID, Name: "child"})
	require.NoError(t, err)
	gotChild, err := mgr1.GetDisk(ctx, child.DiskID)
	require.NoError(t, err)
	require.Equal(t, uint64(4096), gotChild.UsedBytes)
	require.Equal(t, uint64(2), gotChild.UsedInodes)

	childMnt, err := mgr1.MountDisk(ctx, MountDiskParams{DiskID: child.DiskID})
	require.NoError(t, err)
	childActor, err := mgr1.Actor(ctx, child.DiskID)
	require.NoError(t, err)
	writeRegularFile(t, ctx, childActor, childMnt.Epoch, "f2.bin", make([]byte, 2048))
	require.NoError(t, mgr1.UnmountDisk(ctx, UnmountDiskParams{DiskID: child.DiskID, MountID: childMnt.MountID}))

	mergeQuota := uint64(24 << 20)
	_, err = mgr1.MergeDisk(ctx, &MergeDiskParams{
		SourceDiskID: child.DiskID,
		TargetDiskID: base.DiskID,
		Strategy:     store.MergeStrategyFail,
		MaxBytes:     &mergeQuota,
	})
	require.NoError(t, err)
	afterMerge, err := mgr1.GetDisk(ctx, base.DiskID)
	require.NoError(t, err)
	require.Equal(t, mergeQuota, afterMerge.MaxBytes)
	require.Equal(t, uint64(6144), afterMerge.UsedBytes)
	require.Equal(t, uint64(3), afterMerge.UsedInodes)

	mnt2, err := mgr1.MountDisk(ctx, MountDiskParams{DiskID: base.DiskID})
	require.NoError(t, err)
	baseActor2, err := mgr1.Actor(ctx, base.DiskID)
	require.NoError(t, err)
	writeRegularFile(t, ctx, baseActor2, mnt2.Epoch, "f3.bin", make([]byte, 1024))
	require.NoError(t, mgr1.Close())

	mgr2 := newManager(memDB, WithStorageRoot(rootDir), WithActorIdleTTL(0), WithUsageSyncInterval(0))
	t.Cleanup(func() { _ = mgr2.Close() })
	afterClose, err := mgr2.GetDisk(ctx, base.DiskID)
	require.NoError(t, err)
	require.Equal(t, uint64(7168), afterClose.UsedBytes)
	require.Equal(t, uint64(4), afterClose.UsedInodes)
}
