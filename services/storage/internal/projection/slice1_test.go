package projection

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"

	"github.com/starsrc/bowl/services/storage/internal/engine"
)

type slice1Clock struct {
	mu    sync.RWMutex
	now   time.Time
	calls atomic.Int64
}

func newSlice1Clock(start time.Time) *slice1Clock {
	return &slice1Clock{now: start.UTC()}
}

func (c *slice1Clock) Now() time.Time {
	c.calls.Add(1)
	c.mu.RLock()
	t := c.now
	c.mu.RUnlock()
	return t
}

func (c *slice1Clock) Advance(d time.Duration) time.Time {
	c.mu.Lock()
	c.now = c.now.Add(d)
	t := c.now
	c.mu.Unlock()
	return t
}

func TestWithDisk_GivenHotOpenDisk_WhenInvoked_ThenZeroAllocs(t *testing.T) {
	ctx := context.Background()
	mgr, err := NewControlManager(ManagerOptions{
		StorageRoot:             t.TempDir(),
		Store:                   NewMemStore(),
		FlushInterval:           time.Hour,
		ReconcileInterval:       time.Hour,
		DisableStartupReconcile: true,
		DisableGC:               true,
	})
	require.NoError(t, err)
	defer func() { require.NoError(t, mgr.Close(ctx)) }()

	disk, err := mgr.CreateDisk(ctx, "hot-zero-alloc", 16<<20)
	require.NoError(t, err)

	noop := func(*engine.DiskStore) error { return nil }
	require.NoError(t, mgr.WithDisk(ctx, disk.DiskID, noop))

	allocs := testing.AllocsPerRun(100, func() {
		if wErr := mgr.WithDisk(ctx, disk.DiskID, noop); wErr != nil {
			t.Fatalf("unexpected WithDisk error: %v", wErr)
		}
	})
	require.Zero(t, allocs, "hot WithDisk must perform 0 allocs/op")
}

func TestWithDiskReadOnly_GivenCollidingColdFallback_WhenHotDiskRuns_ThenDoesNotBlock(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	mgr, err := NewControlManager(ManagerOptions{
		StorageRoot:             root,
		Store:                   NewMemStore(),
		FlushInterval:           time.Hour,
		ReconcileInterval:       time.Hour,
		DisableStartupReconcile: true,
		DisableGC:               true,
	})
	require.NoError(t, err)
	defer func() { require.NoError(t, mgr.Close(ctx)) }()

	diskA, diskB := findCollidingUUIDs(t)
	require.Equal(t, diskSlotIndex(diskA), diskSlotIndex(diskB))

	seedColdDisk(t, root, diskA, "cold-a")
	seedColdDisk(t, root, diskB, "hot-b")

	require.NoError(t, mgr.WithDisk(ctx, diskB, func(*engine.DiskStore) error { return nil }))
	require.False(t, mgr.IsOpen(diskA))
	require.True(t, mgr.IsOpen(diskB))

	coldEntered := make(chan struct{})
	allowColdExit := make(chan struct{})
	coldErrCh := make(chan error, 1)

	go func() {
		coldErrCh <- mgr.withDiskReadOnly(diskA, func(*engine.DiskStore) error {
			close(coldEntered)
			<-allowColdExit
			return nil
		})
	}()

	select {
	case <-coldEntered:
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for cold withDiskReadOnly(diskA) to enter callback")
	}

	hotDone := make(chan error, 1)
	go func() {
		hotDone <- mgr.WithDisk(ctx, diskB, func(ds *engine.DiskStore) error {
			_ = ds.UsedBytes()
			return nil
		})
	}()

	select {
	case hotErr := <-hotDone:
		require.NoError(t, hotErr)
	case <-time.After(300 * time.Millisecond):
		close(allowColdExit)
		coldErr := <-coldErrCh
		t.Fatalf("hot WithDisk(diskB) blocked behind colliding cold withDiskReadOnly(diskA) (cold err: %v)", coldErr)
	}

	close(allowColdExit)
	require.NoError(t, <-coldErrCh)
	require.False(t, mgr.IsOpen(diskA))
	require.True(t, mgr.IsOpen(diskB))
}

func TestEvictIdleDisks_GivenStalledPG_WhenEvictingIdleDisk_ThenBoundsFlushBySyncWriteTimeoutAndMarksDirty(t *testing.T) {
	ctx := context.Background()
	store := NewMemStore()
	clk := newSlice1Clock(time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC))

	const syncTimeout = 60 * time.Millisecond
	mgr, err := NewControlManager(ManagerOptions{
		StorageRoot:             t.TempDir(),
		Store:                   store,
		Now:                     clk.Now,
		SyncWriteTimeout:        syncTimeout,
		FlushInterval:           time.Hour,
		ReconcileInterval:       time.Hour,
		ActorIdleTTL:            20 * time.Millisecond,
		OpTimeout:               5 * time.Second,
		DisableStartupReconcile: true,
		DisableGC:               true,
	})
	require.NoError(t, err)
	defer func() { require.NoError(t, mgr.Close(ctx)) }()

	disk, err := mgr.CreateDisk(ctx, "stalled-evict", 16<<20)
	require.NoError(t, err)

	require.NoError(t, mgr.WithDisk(ctx, disk.DiskID, func(ds *engine.DiskStore) error {
		f, cErr := ds.Create(ctx, engine.DefaultBranchName, engine.RootIno, "data.bin", 0o644, 0, 0)
		if cErr != nil {
			return cErr
		}
		_, wErr := ds.Write(ctx, engine.DefaultBranchName, f.Ino, 0, make([]byte, 4096))
		return wErr
	}))

	store.SetHooks(MemStoreHooks{
		BeforeBatch: func(hookCtx context.Context, _ string) error {
			<-hookCtx.Done()
			return hookCtx.Err()
		},
	})

	clk.Advance(time.Second)

	opCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()

	start := time.Now()
	evicted := mgr.EvictIdleDisks(opCtx)
	elapsed := time.Since(start)

	require.Equal(t, 1, evicted)
	require.Less(t, elapsed, 400*time.Millisecond, "tryEvictIdleDisk must bound flush by syncWriteTimeout instead of waiting for opTimeout")
	require.False(t, mgr.IsOpen(disk.DiskID))
	require.True(t, mgr.Queue().IsDirty(disk.DiskID))
}

func TestLastUsedNano_GivenActorIdleTTLDisabled_WhenHotWithDiskRuns_ThenZeroClockReads(t *testing.T) {
	ctx := context.Background()
	clk := newSlice1Clock(time.Date(2026, 6, 2, 12, 0, 0, 0, time.UTC))

	mgr, err := NewControlManager(ManagerOptions{
		StorageRoot:             t.TempDir(),
		Store:                   NewMemStore(),
		Now:                     clk.Now,
		FlushInterval:           time.Hour,
		ReconcileInterval:       time.Hour,
		ActorIdleTTL:            0,
		DisableStartupReconcile: true,
		DisableGC:               true,
	})
	require.NoError(t, err)
	defer func() { require.NoError(t, mgr.Close(ctx)) }()

	disk, err := mgr.CreateDisk(ctx, "no-ttl-clock", 16<<20)
	require.NoError(t, err)

	clk.calls.Store(0)
	for range 10 {
		require.NoError(t, mgr.WithDisk(ctx, disk.DiskID, func(*engine.DiskStore) error { return nil }))
	}
	require.Zero(t, clk.calls.Load(), "hot WithDisk must make zero m.now() calls when actorIdleTTL <= 0")
}

func TestLastUsedNano_GivenActorIdleTTLEnabled_WhenHotWithDiskRuns_ThenExitOnlyAndThrottled(t *testing.T) {
	ctx := context.Background()
	start := time.Date(2026, 6, 3, 12, 0, 0, 0, time.UTC)
	clk := newSlice1Clock(start)

	const idleTTL = 800 * time.Millisecond
	mgr, err := NewControlManager(ManagerOptions{
		StorageRoot:             t.TempDir(),
		Store:                   NewMemStore(),
		Now:                     clk.Now,
		FlushInterval:           time.Hour,
		ReconcileInterval:       time.Hour,
		ActorIdleTTL:            idleTTL,
		DisableStartupReconcile: true,
		DisableGC:               true,
	})
	require.NoError(t, err)
	defer func() { require.NoError(t, mgr.Close(ctx)) }()

	diskA, err := mgr.CreateDisk(ctx, "throttled-ttl-a", 16<<20)
	require.NoError(t, err)

	clk.Advance(50 * time.Millisecond)
	clk.calls.Store(0)
	require.NoError(t, mgr.WithDisk(ctx, diskA.DiskID, func(*engine.DiskStore) error { return nil }))
	require.Equal(t, int64(1), clk.calls.Load(), "WithDisk must read clock only once on exit when actorIdleTTL > 0")

	clk.Advance(760 * time.Millisecond)
	require.Equal(t, 1, mgr.EvictIdleDisks(ctx), "throttled touch (< actorIdleTTL/8) must not extend lastUsedNano")
	require.False(t, mgr.IsOpen(diskA.DiskID))

	diskB, err := mgr.CreateDisk(ctx, "throttled-ttl-b", 16<<20)
	require.NoError(t, err)

	clk.Advance(110 * time.Millisecond)
	clk.calls.Store(0)
	require.NoError(t, mgr.WithDisk(ctx, diskB.DiskID, func(*engine.DiskStore) error { return nil }))
	require.Equal(t, int64(1), clk.calls.Load(), "WithDisk must read clock only once on exit when actorIdleTTL > 0")

	clk.Advance(750 * time.Millisecond)
	require.Zero(t, mgr.EvictIdleDisks(ctx), "unthrottled touch (>= actorIdleTTL/8) must update lastUsedNano on exit")
	require.True(t, mgr.IsOpen(diskB.DiskID))

	require.NoError(t, mgr.WithDisk(ctx, diskB.DiskID, func(*engine.DiskStore) error {
		clk.Advance(2 * time.Second)
		return nil
	}))
	require.Zero(t, mgr.EvictIdleDisks(ctx), "long RPC exit touch must prevent immediate eviction")
	require.True(t, mgr.IsOpen(diskB.DiskID))
}

func TestStorageMetricsCollector_GivenConcurrentDeleteEvictAndClose_WhenCollecting_ThenRaceFree(t *testing.T) {
	ctx := context.Background()
	store := NewMemStore()
	mgr, err := NewControlManager(ManagerOptions{
		StorageRoot:             t.TempDir(),
		Store:                   store,
		FlushInterval:           time.Hour,
		ReconcileInterval:       time.Hour,
		ActorIdleTTL:            15 * time.Millisecond,
		DisableStartupReconcile: true,
		DisableGC:               true,
	})
	require.NoError(t, err)

	collector := NewStorageMetricsCollector(mgr)

	stop := make(chan struct{})
	var wg sync.WaitGroup

	for range 4 {
		wg.Go(func() {
			for {
				select {
				case <-stop:
					return
				default:
					ch := make(chan prometheus.Metric, 64)
					collector.Collect(ch)
					close(ch)
					for range ch {
					}
				}
			}
		})
	}

	wg.Go(func() {
		for i := range 12 {
			d, cErr := mgr.CreateDisk(ctx, fmt.Sprintf("race-disk-%d", i), 8<<20)
			if cErr != nil {
				return
			}
			_ = mgr.WithDisk(ctx, d.DiskID, func(ds *engine.DiskStore) error {
				f, fErr := ds.Create(ctx, engine.DefaultBranchName, engine.RootIno, "f.bin", 0o644, 0, 0)
				if fErr != nil {
					return fErr
				}
				_, wErr := ds.Write(ctx, engine.DefaultBranchName, f.Ino, 0, []byte("payload"))
				return wErr
			})
			if i%2 == 0 {
				_ = mgr.DeleteDisk(ctx, d.DiskID)
			} else {
				mgr.EvictIdleDisks(ctx)
			}
		}
	})

	time.Sleep(80 * time.Millisecond)
	require.NoError(t, mgr.Close(ctx))
	close(stop)
	wg.Wait()

	reg := prometheus.NewPedanticRegistry()
	require.NoError(t, reg.Register(collector))
	fams, err := reg.Gather()
	require.NoError(t, err)
	found := false
	for _, fam := range fams {
		if fam.GetName() == "bowl_storage_disks_loaded" {
			found = true
			require.Len(t, fam.GetMetric(), 1)
			require.Zero(t, fam.GetMetric()[0].GetGauge().GetValue())
		}
	}
	require.True(t, found)
}

func TestQueue_GivenConcurrentRecordAndClose_WhenClosed_ThenRaceFreeAndNoPostCloseWrites(t *testing.T) {
	ctx := context.Background()
	store := NewMemStore()
	var afterCloseWrites atomic.Int64
	var closedFlag atomic.Bool

	store.SetHooks(MemStoreHooks{
		BeforeBatch: func(_ context.Context, _ string) error {
			if closedFlag.Load() {
				afterCloseWrites.Add(1)
			}
			return nil
		},
	})

	q := NewQueue(store, QueueConfig{
		FlushInterval: 5 * time.Millisecond,
	})

	stop := make(chan struct{})
	var wg sync.WaitGroup
	for range 4 {
		wg.Go(func() {
			diskID := uuid.New()
			mountID := uuid.New()
			var n uint64
			for {
				select {
				case <-stop:
					return
				default:
					n++
					q.RecordUsage(diskID, n)
					q.RecordMountRenew(diskID, mountID, time.Now().Add(time.Minute))
				}
			}
		})
	}

	time.Sleep(25 * time.Millisecond)
	require.NoError(t, q.Close(ctx))
	closedFlag.Store(true)
	close(stop)
	wg.Wait()

	q.RecordUsage(uuid.New(), 999)
	q.RecordMountRenew(uuid.New(), uuid.New(), time.Now().Add(time.Minute))
	q.FlushBatches(ctx)

	require.Zero(t, afterCloseWrites.Load(), "closed Queue must reject post-close RecordUsage and RecordMountRenew writes")
}

func TestWithTempDisk_GivenCloseError_WhenReadSucceedsOrFails_ThenPropagatesCorrectError(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	store := NewMemStore()
	mgr, err := NewControlManager(ManagerOptions{
		StorageRoot:             root,
		Store:                   store,
		FlushInterval:           time.Hour,
		ReconcileInterval:       time.Hour,
		DisableStartupReconcile: true,
		DisableGC:               true,
	})
	require.NoError(t, err)
	defer func() { require.NoError(t, mgr.Close(ctx)) }()

	diskID := uuid.New()
	seedColdDisk(t, root, diskID, "close-err-disk")

	sentinelCloseErr := errors.New("synthetic temp disk close failure")
	origClose := mgr.closeDisk
	mgr.closeDisk = func(ds *engine.DiskStore) error {
		_ = origClose(ds)
		return sentinelCloseErr
	}

	err = mgr.withDiskReadOnly(diskID, func(*engine.DiskStore) error {
		return nil
	})
	require.ErrorIs(t, err, sentinelCloseErr)

	sentinelReadErr := errors.New("synthetic read failure")
	err = mgr.withDiskReadOnly(diskID, func(*engine.DiskStore) error {
		return sentinelReadErr
	})
	require.ErrorIs(t, err, sentinelReadErr)
	require.NotErrorIs(t, err, sentinelCloseErr)

	err = mgr.DeleteDisk(ctx, diskID)
	require.ErrorIs(t, err, sentinelCloseErr)

	err = mgr.ReconcileDisk(ctx, diskID)
	require.ErrorIs(t, err, sentinelCloseErr)
}

func TestClose_GivenConcurrentWithDiskAndLifecycleOps_WhenClosing_ThenDrainsAllStripesRaceFree(t *testing.T) {
	ctx := context.Background()
	store := NewMemStore()
	mgr, err := NewControlManager(ManagerOptions{
		StorageRoot:             t.TempDir(),
		Store:                   store,
		FlushInterval:           time.Hour,
		ReconcileInterval:       time.Hour,
		ActorIdleTTL:            30 * time.Second,
		DisableStartupReconcile: true,
		DisableGC:               true,
	})
	require.NoError(t, err)

	disks := make([]uuid.UUID, 8)
	for i := range disks {
		d, cErr := mgr.CreateDisk(ctx, fmt.Sprintf("close-drain-%d", i), 8<<20)
		require.NoError(t, cErr)
		disks[i] = d.DiskID
	}

	stop := make(chan struct{})
	var wg sync.WaitGroup
	for worker := range 4 {
		wg.Go(func() {
			idx := worker
			for {
				select {
				case <-stop:
					return
				default:
					id := disks[idx%len(disks)]
					idx++
					wErr := mgr.WithDisk(ctx, id, func(ds *engine.DiskStore) error {
						_ = ds.UsedBytes()
						return nil
					})
					if wErr != nil && !errors.Is(wErr, engine.ErrClosed) {
						t.Errorf("unexpected WithDisk error during Close: %v", wErr)
						return
					}
				}
			}
		})
	}

	time.Sleep(30 * time.Millisecond)
	require.NoError(t, mgr.Close(ctx))
	close(stop)
	wg.Wait()

	for _, id := range disks {
		require.False(t, mgr.IsOpen(id))
		require.ErrorIs(t, mgr.WithDisk(ctx, id, func(*engine.DiskStore) error { return nil }), engine.ErrClosed)
	}
}

func TestPanicSafety_GivenCallbackPanicInWithDiskOrColdReadOnly_WhenRecovered_ThenReleasesStripeLocks(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	mgr, err := NewControlManager(ManagerOptions{
		StorageRoot:             root,
		Store:                   NewMemStore(),
		FlushInterval:           time.Hour,
		ReconcileInterval:       time.Hour,
		DisableStartupReconcile: true,
		DisableGC:               true,
	})
	require.NoError(t, err)
	defer func() { require.NoError(t, mgr.Close(ctx)) }()

	hotDisk, err := mgr.CreateDisk(ctx, "panic-hot", 16<<20)
	require.NoError(t, err)

	func() {
		defer func() {
			require.Equal(t, "boom-hot", recover())
		}()
		_ = mgr.WithDisk(ctx, hotDisk.DiskID, func(*engine.DiskStore) error {
			panic("boom-hot")
		})
	}()

	hotDone := make(chan error, 1)
	go func() {
		renamed := "panic-hot-after"
		_, uErr := mgr.UpdateDisk(ctx, hotDisk.DiskID, &renamed, nil)
		hotDone <- uErr
	}()
	select {
	case uErr := <-hotDone:
		require.NoError(t, uErr)
	case <-time.After(500 * time.Millisecond):
		t.Fatal("UpdateDisk deadlocked after panic inside WithDisk callback")
	}

	coldID := uuid.New()
	seedColdDisk(t, root, coldID, "panic-cold")
	require.False(t, mgr.IsOpen(coldID))

	func() {
		defer func() {
			require.Equal(t, "boom-cold", recover())
		}()
		_ = mgr.withDiskReadOnly(coldID, func(*engine.DiskStore) error {
			panic("boom-cold")
		})
	}()

	coldDone := make(chan error, 1)
	go func() {
		coldDone <- mgr.withDiskReadOnly(coldID, func(*engine.DiskStore) error {
			return nil
		})
	}()
	select {
	case rErr := <-coldDone:
		require.NoError(t, rErr)
	case <-time.After(500 * time.Millisecond):
		t.Fatal("cold withDiskReadOnly deadlocked after panic inside cold callback")
	}
}
