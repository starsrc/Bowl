package store

import (
	"bytes"
	"context"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"
)

func blobSyncSampleCountInternal(t *testing.T, reg *prometheus.Registry, target, trigger string) uint64 {
	t.Helper()
	fams, err := reg.Gather()
	require.NoError(t, err)
	for _, fam := range fams {
		if fam.GetName() != "bowl_storage_blob_sync_duration_seconds" {
			continue
		}
		for _, m := range fam.GetMetric() {
			if metricMatchesLabelsInternal(m, map[string]string{"target": target, "trigger": trigger}) {
				return m.GetHistogram().GetSampleCount()
			}
		}
	}
	return 0
}

func metricMatchesLabelsInternal(m *dto.Metric, want map[string]string) bool {
	matched := 0
	for _, lp := range m.GetLabel() {
		if v, ok := want[lp.GetName()]; ok && lp.GetValue() == v {
			matched++
		}
	}
	return matched == len(want)
}

type eventRecorder struct {
	mu     sync.Mutex
	events []string
}

func (r *eventRecorder) record(ev string) {
	r.mu.Lock()
	r.events = append(r.events, ev)
	r.mu.Unlock()
}

func (r *eventRecorder) snapshot() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]string, len(r.events))
	copy(out, r.events)
	return out
}

func (r *eventRecorder) reset() {
	r.mu.Lock()
	r.events = r.events[:0]
	r.mu.Unlock()
}

func TestGivenDirtyPackWhenFsyncBarrierRunsThenPackAndPacksDirSyncFinishBeforeWALSync(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	metrics := NewMetrics()
	reg := prometheus.NewRegistry()
	require.NoError(t, reg.Register(metrics))

	var (
		rec            eventRecorder
		walSawDirty    atomic.Bool
		walSawPackData atomic.Uint64
		walSawPacksDir atomic.Uint64
		walHookCalls   atomic.Int32
		packErrArmed   atomic.Bool
		tickHookArmed  atomic.Bool
		tickEntered    = make(chan struct{})
		releaseTick    = make(chan struct{})
		actor          *StoreActor
		err            error
	)

	actor, err = Open(Options{
		Dir:         dir,
		DiskID:      "disk-step24-fsync",
		MaxBytes:    64 << 20,
		MaxPackSize: 2 * BlobSize,
		Metrics:     metrics,
		syncFileHook: func(_ uint64) {
			rec.record("pack_sync")
		},
		syncFileErrHook: func(_ uint64) error {
			if packErrArmed.Load() {
				return ErrIO
			}
			return nil
		},
		beforeWALSyncHook: func() {
			walHookCalls.Add(1)
			rec.record("wal_sync")
			if actor.blobs.hasDirty() {
				walSawDirty.Store(true)
			}
			walSawPackData.Store(blobSyncSampleCountInternal(t, reg, "pack_data", "fsync_barrier"))
			walSawPacksDir.Store(blobSyncSampleCountInternal(t, reg, "packs_dir", "fsync_barrier"))
		},
		beforeTickHook: makeOnceArmedTickHook(&tickHookArmed, tickEntered, releaseTick),
	})
	require.NoError(t, err)
	defer func() {
		packErrArmed.Store(false)
		require.NoError(t, actor.Close())
	}()

	f1 := createAndWriteInternal(t, actor, "f1.bin", 0x11, BlobSize)
	require.True(t, actor.blobs.hasDirty())
	require.True(t, actor.WALDirty())

	require.NoError(t, actor.Fsync(ctx, f1.Ino))
	require.Equal(t, int32(1), walHookCalls.Load())
	require.False(t, walSawDirty.Load())
	require.GreaterOrEqual(t, walSawPackData.Load(), uint64(1))
	require.GreaterOrEqual(t, walSawPacksDir.Load(), uint64(1))
	require.Equal(t, []string{"pack_sync", "wal_sync"}, rec.snapshot())
	require.False(t, actor.blobs.hasDirty())
	require.False(t, actor.WALDirty())

	rec.reset()
	walSawDirty.Store(false)
	walHookCalls.Store(0)

	f2 := createAndWriteInternal(t, actor, "f2.bin", 0x22, BlobSize)
	require.True(t, actor.blobs.hasDirty())

	var (
		wg       sync.WaitGroup
		c1Err    error
		c2Err    error
		fsyncErr error
	)
	tickHookArmed.Store(true)
	wg.Go(func() {
		_, c1Err = actor.Create(ctx, CreateReq{ParentIno: RootIno, Name: []byte("gate.bin"), Mode: S_IFREG | 0o644})
	})
	awaitValueInternal(t, tickEntered, 2*time.Second, "beforeTickHook did not fire", func() {
		close(releaseTick)
	})
	wg.Go(func() {
		_, c2Err = actor.Create(ctx, CreateReq{ParentIno: RootIno, Name: []byte("coalesced.bin"), Mode: S_IFREG | 0o644})
	})
	wg.Go(func() {
		fsyncErr = actor.Fsync(ctx, f2.Ino)
	})
	waitMailboxActiveAtLeast(t, actor, 2, 2*time.Second)
	close(releaseTick)
	wg.Wait()
	require.NoError(t, c1Err)
	require.NoError(t, c2Err)
	require.NoError(t, fsyncErr)
	require.Equal(t, int32(1), walHookCalls.Load())
	require.False(t, walSawDirty.Load())
	require.Equal(t, []string{"pack_sync", "wal_sync"}, rec.snapshot())
	require.False(t, actor.blobs.hasDirty())

	_ = createAndWriteInternal(t, actor, "f3.bin", 0x33, BlobSize)
	require.NoError(t, actor.Unlink(ctx, RootIno, []byte("f1.bin"), false))
	require.NoError(t, actor.Unlink(ctx, RootIno, []byte("f2.bin"), false))
	require.Equal(t, 1, actor.MetricsSnapshot().PendingDeadPacks)
	require.True(t, actor.WALDirty())

	walHookCalls.Store(0)
	packErrArmed.Store(true)
	err = actor.Fsync(ctx, f1.Ino)
	require.ErrorIs(t, err, ErrNotFound)

	f3Attr, _, err := actor.Lookup(ctx, RootIno, []byte("f3.bin"))
	require.NoError(t, err)
	err = actor.Fsync(ctx, f3Attr.Ino)
	require.ErrorIs(t, err, ErrIO)
	require.Equal(t, int32(0), walHookCalls.Load())
	require.True(t, actor.WALDirty())
	require.Equal(t, 1, actor.MetricsSnapshot().PendingDeadPacks)
	_, statErr := os.Stat(packFilePath(actor.blobs.dir, 1))
	require.NoError(t, statErr)

	packErrArmed.Store(false)
	require.NoError(t, actor.Fsync(ctx, f3Attr.Ino))
	actor.blobs.waitPendingReclaims()
	require.False(t, actor.WALDirty())
	require.Equal(t, 0, actor.MetricsSnapshot().PendingDeadPacks)
	_, statErr = os.Stat(packFilePath(actor.blobs.dir, 1))
	require.ErrorIs(t, statErr, os.ErrNotExist)
}

func TestGivenDirtyPackFromPriorWriteWhenFsyncDirAdvanceEpochOrUpdateMaxBytesRunsThenPackAndPacksDirSyncFinishBeforeWALSync(t *testing.T) {
	cases := []struct {
		name string
		run  func(t *testing.T, ctx context.Context, actor *StoreActor)
	}{
		{
			name: "FsyncDir",
			run: func(t *testing.T, ctx context.Context, actor *StoreActor) {
				require.NoError(t, actor.FsyncDir(ctx, RootIno))
			},
		},
		{
			name: "AdvanceEpoch",
			run: func(t *testing.T, ctx context.Context, actor *StoreActor) {
				ep, _, err := actor.AdvanceEpoch(ctx)
				require.NoError(t, err)
				require.Equal(t, uint64(1), ep)
			},
		},
		{
			name: "UpdateMaxBytes",
			run: func(t *testing.T, ctx context.Context, actor *StoreActor) {
				require.NoError(t, actor.UpdateMaxBytes(ctx, 128<<20))
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			dir := t.TempDir()
			metrics := NewMetrics()
			reg := prometheus.NewRegistry()
			require.NoError(t, reg.Register(metrics))

			var (
				rec            eventRecorder
				walSawDirty    atomic.Bool
				walSawPackData atomic.Uint64
				walSawPacksDir atomic.Uint64
				walHookCalls   atomic.Int32
				actor          *StoreActor
				err            error
			)

			actor, err = Open(Options{
				Dir:      dir,
				DiskID:   "disk-step24-" + tc.name,
				MaxBytes: 64 << 20,
				Metrics:  metrics,
				syncFileHook: func(_ uint64) {
					rec.record("pack_sync")
				},
				beforeWALSyncHook: func() {
					walHookCalls.Add(1)
					rec.record("wal_sync")
					if actor.blobs.hasDirty() {
						walSawDirty.Store(true)
					}
					walSawPackData.Store(blobSyncSampleCountInternal(t, reg, "pack_data", "fsync_barrier"))
					walSawPacksDir.Store(blobSyncSampleCountInternal(t, reg, "packs_dir", "fsync_barrier"))
				},
			})
			require.NoError(t, err)
			defer func() { require.NoError(t, actor.Close()) }()

			_ = createAndWriteInternal(t, actor, "prior.bin", 0x5A, BlobSize)
			require.True(t, actor.blobs.hasDirty())

			tc.run(t, ctx, actor)

			require.Equal(t, int32(1), walHookCalls.Load())
			require.False(t, walSawDirty.Load())
			require.GreaterOrEqual(t, walSawPackData.Load(), uint64(1))
			require.GreaterOrEqual(t, walSawPacksDir.Load(), uint64(1))
			require.Equal(t, []string{"pack_sync", "wal_sync"}, rec.snapshot())
			require.False(t, actor.blobs.hasDirty())
			require.False(t, actor.WALDirty())
		})
	}
}

func TestGivenLowWatermarkPackWhenCompactPacksRunsThenRelocatedPackSyncsBeforeRelocateCommitAndEIOAbortsCleanly(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()

	actor1, err := Open(Options{
		Dir:         dir,
		DiskID:      "disk-step24-compact",
		MaxBytes:    64 << 20,
		MaxPackSize: 2 * BlobSize,
	})
	require.NoError(t, err)
	_ = createAndWriteInternal(t, actor1, "dead.bin", 0xAA, BlobSize)
	keep := createAndWriteInternal(t, actor1, "keep.bin", 0xBB, BlobSize)
	require.NoError(t, actor1.Close())

	metrics := NewMetrics()
	reg := prometheus.NewRegistry()
	require.NoError(t, reg.Register(metrics))

	var (
		rec               eventRecorder
		relocateHookCalls atomic.Int32
		relocateSawDirty  atomic.Bool
		relocatePackData  atomic.Uint64
		relocatePacksDir  atomic.Uint64
		packErrArmed      atomic.Bool
		actor2            *StoreActor
	)

	actor2, err = Open(Options{
		Dir:         dir,
		DiskID:      "disk-step24-compact",
		MaxBytes:    64 << 20,
		MaxPackSize: 2 * BlobSize,
		Metrics:     metrics,
		syncFileHook: func(_ uint64) {
			rec.record("pack_sync")
		},
		syncFileErrHook: func(_ uint64) error {
			if packErrArmed.Load() {
				return ErrIO
			}
			return nil
		},
		beforeCompactRelocateHook: func() {
			relocateHookCalls.Add(1)
			rec.record("before_compact_relocate")
			if actor2.blobs.hasDirty() {
				relocateSawDirty.Store(true)
			}
			relocatePackData.Store(blobSyncSampleCountInternal(t, reg, "pack_data", "flush_wal"))
			relocatePacksDir.Store(blobSyncSampleCountInternal(t, reg, "packs_dir", "flush_wal"))
		},
	})
	require.NoError(t, err)
	defer func() {
		packErrArmed.Store(false)
		require.NoError(t, actor2.Close())
	}()

	require.NoError(t, actor2.Unlink(ctx, RootIno, []byte("dead.bin"), false))
	require.NoError(t, actor2.FlushWAL())
	require.Equal(t, 1, actor2.MetricsSnapshot().LowWatermarkPacks)

	packErrArmed.Store(true)
	err = actor2.CompactPacks(ctx)
	require.ErrorIs(t, err, ErrIO)
	require.Equal(t, int32(0), relocateHookCalls.Load())
	require.Equal(t, 1, actor2.MetricsSnapshot().LowWatermarkPacks)
	_, statErr := os.Stat(packFilePath(actor2.blobs.dir, 1))
	require.NoError(t, statErr)

	actor2.inFlightMu.Lock()
	pinnedCount := len(actor2.inFlightPacks)
	actor2.inFlightMu.Unlock()
	require.Zero(t, pinnedCount)

	rec.reset()
	packErrArmed.Store(false)
	require.NoError(t, actor2.CompactPacks(ctx))
	require.Equal(t, int32(1), relocateHookCalls.Load())
	require.False(t, relocateSawDirty.Load())
	require.GreaterOrEqual(t, relocatePackData.Load(), uint64(1))
	require.GreaterOrEqual(t, relocatePacksDir.Load(), uint64(1))
	require.Equal(t, []string{"pack_sync", "before_compact_relocate"}, rec.snapshot())
	require.Equal(t, 0, actor2.MetricsSnapshot().LowWatermarkPacks)
	_, statErr = os.Stat(packFilePath(actor2.blobs.dir, 1))
	require.ErrorIs(t, statErr, os.ErrNotExist)

	got, err := actor2.Read(ctx, keep.Ino, 0, BlobSize)
	require.NoError(t, err)
	require.Equal(t, bytes.Repeat([]byte{0xBB}, BlobSize), got)
}

func TestGivenCoalescedCreateWriteFsyncWhenPackSyncOrWALApplyReturnsEIOThenStateAndDirtyAtimeRollBackAndRetrySucceeds(t *testing.T) {
	cases := []struct {
		name      string
		injectWAL bool
	}{
		{name: "PackSyncEIO", injectWAL: false},
		{name: "WALApplyEIO", injectWAL: true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			dir := t.TempDir()

			var (
				packErrArmed  atomic.Bool
				walErrArmed   atomic.Bool
				failNextWAL   atomic.Bool
				tickHookStage atomic.Int32
				tickEntered   = make(chan struct{})
				releaseTick   = make(chan struct{})
				futureAtime   = time.Now().Add(2 * time.Hour).Truncate(time.Nanosecond)
				olderAtime    = time.Now().Add(1 * time.Hour).Truncate(time.Nanosecond)
				newerAtime    = time.Now().Add(3 * time.Hour).Truncate(time.Nanosecond)
				anchorIno     atomic.Uint64
				seedIno       atomic.Uint64
				actor         *StoreActor
				err           error
			)

			actor, err = Open(Options{
				Dir:         dir,
				DiskID:      "disk-step25-" + tc.name,
				MaxBytes:    64 << 20,
				MaxPackSize: 2 * BlobSize,
				AtimeMode:   AtimeModeStrictAtime,
				syncFileErrHook: func(packID uint64) error {
					if packErrArmed.Load() {
						if tickHookStage.Load() == 3 && packID == actor.blobs.activePackID() {
							actor.MarkAtimeDirty(seedIno.Load(), newerAtime)
						}
						return ErrIO
					}
					return nil
				},
				beforeWALSyncHook: func() {
					if failNextWAL.Swap(false) {
						actor.MarkAtimeDirty(seedIno.Load(), newerAtime)
						walErrArmed.Store(true)
					}
				},
				walApplyErrHook: func() error {
					if walErrArmed.Swap(false) {
						return ErrIO
					}
					return nil
				},
				beforeTickHook: func() {
					switch tickHookStage.Load() {
					case 1:
						if tickHookStage.CompareAndSwap(1, 2) {
							close(tickEntered)
							<-releaseTick
						}
					case 2:
						if tickHookStage.CompareAndSwap(2, 3) {
							actor.MarkAtimeDirty(anchorIno.Load(), futureAtime)
							actor.MarkAtimeDirty(seedIno.Load(), olderAtime)
						}
					}
				},
			})
			require.NoError(t, err)

			seed := createAndWriteInternal(t, actor, "seed.bin", 0x11, 2*BlobSize)
			anchor := createAndWriteInternal(t, actor, "anchor.bin", 0x22, BlobSize)
			seedIno.Store(seed.Ino)
			anchorIno.Store(anchor.Ino)
			require.NoError(t, actor.FlushWAL())

			var (
				wg       sync.WaitGroup
				gateErr  error
				cErr     error
				wErr     error
				fsyncErr error
			)
			tickHookStage.Store(1)
			wg.Go(func() {
				_, gateErr = actor.Create(ctx, CreateReq{ParentIno: RootIno, Name: []byte("gate.bin"), Mode: S_IFREG | 0o644})
			})
			awaitValueInternal(t, tickEntered, 2*time.Second, "beforeTickHook did not fire", func() {
				close(releaseTick)
			})

			newPayload := bytes.Repeat([]byte{0x77}, 2*BlobSize)
			wg.Go(func() {
				_, cErr = actor.Create(ctx, CreateReq{ParentIno: RootIno, Name: []byte("co-created.bin"), Mode: S_IFREG | 0o644})
			})
			wg.Go(func() {
				_, _, wErr = actor.Write(ctx, seed.Ino, BlobSize, newPayload)
			})
			wg.Go(func() {
				fsyncErr = actor.Fsync(ctx, anchor.Ino)
			})
			waitMailboxActiveAtLeast(t, actor, 3, 2*time.Second)

			if tc.injectWAL {
				failNextWAL.Store(true)
			} else {
				packErrArmed.Store(true)
			}
			close(releaseTick)
			wg.Wait()

			require.NoError(t, gateErr)
			require.ErrorIs(t, cErr, ErrIO)
			require.ErrorIs(t, wErr, ErrIO)
			require.ErrorIs(t, fsyncErr, ErrIO)

			packErrArmed.Store(false)
			walErrArmed.Store(false)
			failNextWAL.Store(false)

			afterFailStatFS, err := actor.StatFS(ctx)
			require.NoError(t, err)
			require.Equal(t, uint64(3*BlobSize), afterFailStatFS.UsedBytes)
			require.Equal(t, uint64(4), afterFailStatFS.Files)
			require.Equal(t, uint64(64<<20), afterFailStatFS.TotalBytes)

			afterFailSnap := actor.MetricsSnapshot()
			require.Equal(t, uint64(3*BlobSize), afterFailSnap.PackPhysicalBytes)
			require.Equal(t, uint64(3*BlobSize), afterFailSnap.PackLiveBytes)
			require.Equal(t, 0, afterFailSnap.LowWatermarkPacks)

			_, _, err = actor.Lookup(ctx, RootIno, []byte("co-created.bin"))
			require.ErrorIs(t, err, ErrNotFound)

			seedAttr, err := actor.GetAttr(ctx, seed.Ino)
			require.NoError(t, err)
			require.Equal(t, uint64(2*BlobSize), seedAttr.Size)
			seedData, err := actor.Read(ctx, seed.Ino, 0, 2*BlobSize)
			require.NoError(t, err)
			require.Equal(t, bytes.Repeat([]byte{0x11}, 2*BlobSize), seedData)

			actor.atimeMu.Lock()
			restoredAtime := actor.dirtyAtime[anchor.Ino]
			mergedSeedAtime := actor.dirtyAtime[seed.Ino]
			actor.atimeMu.Unlock()
			require.Equal(t, futureAtime.UnixNano(), restoredAtime)
			require.Equal(t, newerAtime.UnixNano(), mergedSeedAtime)

			anchorAttr, err := actor.GetAttr(ctx, anchor.Ino)
			require.NoError(t, err)
			require.Equal(t, futureAtime.UnixNano(), anchorAttr.Atime)

			_, err = actor.Create(ctx, CreateReq{ParentIno: RootIno, Name: []byte("co-created.bin"), Mode: S_IFREG | 0o644})
			require.NoError(t, err)
			_, _, err = actor.Write(ctx, seed.Ino, BlobSize, newPayload)
			require.NoError(t, err)
			require.NoError(t, actor.Fsync(ctx, anchor.Ino))

			retryStatFS, err := actor.StatFS(ctx)
			require.NoError(t, err)
			require.Equal(t, uint64(4*BlobSize), retryStatFS.UsedBytes)
			require.Equal(t, uint64(5), retryStatFS.Files)

			retrySnap := actor.MetricsSnapshot()
			require.Equal(t, 2, retrySnap.LowWatermarkPacks)
			require.Equal(t, uint64(4*BlobSize), retrySnap.PackLiveBytes)

			gotSeedTail, err := actor.Read(ctx, seed.Ino, BlobSize, 2*BlobSize)
			require.NoError(t, err)
			require.Equal(t, newPayload, gotSeedTail)

			require.NoError(t, actor.Close())

			reopened, err := Open(Options{
				Dir:         dir,
				DiskID:      "disk-step25-" + tc.name,
				MaxPackSize: 2 * BlobSize,
			})
			require.NoError(t, err)
			defer func() { require.NoError(t, reopened.Close()) }()

			reopenedAnchor, err := reopened.GetAttr(ctx, anchor.Ino)
			require.NoError(t, err)
			require.Equal(t, futureAtime.UnixNano(), reopenedAnchor.Atime)
		})
	}
}

func TestGivenAdvanceEpochAndUpdateMaxBytesWhenBarrierReturnsEIOThenEpochAndQuotaRollBackAndRetrySucceeds(t *testing.T) {
	cases := []struct {
		name      string
		injectWAL bool
	}{
		{name: "PackSyncEIO", injectWAL: false},
		{name: "WALApplyEIO", injectWAL: true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			dir := t.TempDir()

			var (
				packErrArmed atomic.Bool
				walErrArmed  atomic.Bool
				failNextWAL  atomic.Bool
			)

			actor, err := Open(Options{
				Dir:      dir,
				DiskID:   "disk-step25-meta-" + tc.name,
				MaxBytes: 64 << 20,
				syncFileErrHook: func(_ uint64) error {
					if packErrArmed.Load() {
						return ErrIO
					}
					return nil
				},
				beforeWALSyncHook: func() {
					if failNextWAL.Swap(false) {
						walErrArmed.Store(true)
					}
				},
				walApplyErrHook: func() error {
					if walErrArmed.Swap(false) {
						return ErrIO
					}
					return nil
				},
			})
			require.NoError(t, err)

			_ = createAndWriteInternal(t, actor, "dirty1.bin", 0x31, BlobSize)
			if tc.injectWAL {
				failNextWAL.Store(true)
			} else {
				packErrArmed.Store(true)
			}
			_, _, err = actor.AdvanceEpoch(ctx)
			require.ErrorIs(t, err, ErrIO)
			packErrArmed.Store(false)
			walErrArmed.Store(false)
			failNextWAL.Store(false)

			require.Equal(t, uint64(0), actor.WriterEpoch())

			ep, _, err := actor.AdvanceEpoch(ctx)
			require.NoError(t, err)
			require.Equal(t, uint64(1), ep)
			require.Equal(t, uint64(1), actor.WriterEpoch())

			f2, err := actor.Create(ctx, CreateReq{ParentIno: RootIno, Name: []byte("dirty2.bin"), Mode: S_IFREG | 0o644, Epoch: 1})
			require.NoError(t, err)
			_, _, err = actor.Write(ctx, f2.Ino, 0, bytes.Repeat([]byte{0x32}, BlobSize), 1)
			require.NoError(t, err)

			if tc.injectWAL {
				failNextWAL.Store(true)
			} else {
				packErrArmed.Store(true)
			}
			err = actor.UpdateMaxBytes(ctx, 128<<20)
			require.ErrorIs(t, err, ErrIO)
			packErrArmed.Store(false)
			walErrArmed.Store(false)
			failNextWAL.Store(false)

			require.Equal(t, uint64(64<<20), actor.MaxBytes())
			st, err := actor.StatFS(ctx)
			require.NoError(t, err)
			require.Equal(t, uint64(64<<20), st.TotalBytes)

			require.NoError(t, actor.Close())

			reopened, err := Open(Options{
				Dir:    dir,
				DiskID: "disk-step25-meta-" + tc.name,
			})
			require.NoError(t, err)
			require.Equal(t, uint64(64<<20), reopened.MaxBytes())
			require.Equal(t, uint64(1), reopened.WriterEpoch())

			require.NoError(t, reopened.UpdateMaxBytes(ctx, 96<<20))
			require.Equal(t, uint64(96<<20), reopened.MaxBytes())
			require.NoError(t, reopened.Close())
		})
	}
}

func TestGivenNonBarrierWriteTickWhenWALApplyReturnsEIOThenInMemoryStateRollsBackAndRetrySucceeds(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()

	var walErrArmed atomic.Bool
	actor, err := Open(Options{
		Dir:         dir,
		DiskID:      "disk-step25-nosync",
		MaxBytes:    64 << 20,
		MaxPackSize: 2 * BlobSize,
		AtimeMode:   AtimeModeStrictAtime,
		walApplyErrHook: func() error {
			if walErrArmed.Swap(false) {
				return ErrIO
			}
			return nil
		},
	})
	require.NoError(t, err)
	defer func() {
		walErrArmed.Store(false)
		require.NoError(t, actor.Close())
	}()

	seed := createAndWriteInternal(t, actor, "seed.bin", 0x41, 2*BlobSize)
	anchor := createAndWriteInternal(t, actor, "anchor.bin", 0x42, BlobSize)
	require.NoError(t, actor.FlushWAL())
	require.False(t, actor.WALDirty())

	futureAtime := time.Now().Add(3 * time.Hour).Truncate(time.Nanosecond)
	actor.MarkAtimeDirty(anchor.Ino, futureAtime)

	walErrArmed.Store(true)
	_, _, err = actor.Write(ctx, seed.Ino, BlobSize, bytes.Repeat([]byte{0x99}, 2*BlobSize))
	require.ErrorIs(t, err, ErrIO)
	require.False(t, actor.WALDirty())

	st, err := actor.StatFS(ctx)
	require.NoError(t, err)
	require.Equal(t, uint64(3*BlobSize), st.UsedBytes)
	require.Equal(t, uint64(3), st.Files)

	snap := actor.MetricsSnapshot()
	require.Equal(t, uint64(3*BlobSize), snap.PackPhysicalBytes)
	require.Equal(t, uint64(3*BlobSize), snap.PackLiveBytes)
	require.Equal(t, 0, snap.LowWatermarkPacks)

	anchorAttr, err := actor.GetAttr(ctx, anchor.Ino)
	require.NoError(t, err)
	require.Equal(t, futureAtime.UnixNano(), anchorAttr.Atime)

	_, _, err = actor.Write(ctx, seed.Ino, BlobSize, bytes.Repeat([]byte{0x99}, 2*BlobSize))
	require.NoError(t, err)

	stAfter, err := actor.StatFS(ctx)
	require.NoError(t, err)
	require.Equal(t, uint64(4*BlobSize), stAfter.UsedBytes)
	require.Equal(t, 2, actor.MetricsSnapshot().LowWatermarkPacks)
}

func TestGivenDirtyPackWhenFlushWALRunsThenPackAndPacksDirSyncBeforeWALAndEIOPreservesDeadPacksAndWALDirty(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	metrics := NewMetrics()
	reg := prometheus.NewRegistry()
	require.NoError(t, reg.Register(metrics))

	var (
		rec            eventRecorder
		walHookCalls   atomic.Int32
		walSawDirty    atomic.Bool
		walSawPackData atomic.Uint64
		walSawPacksDir atomic.Uint64
		packErrArmed   atomic.Bool
		actor          *StoreActor
		err            error
	)

	actor, err = Open(Options{
		Dir:         dir,
		DiskID:      "disk-step26-flushwal",
		MaxBytes:    64 << 20,
		MaxPackSize: 2 * BlobSize,
		Metrics:     metrics,
		syncFileHook: func(_ uint64) {
			rec.record("pack_sync")
		},
		syncFileErrHook: func(_ uint64) error {
			if packErrArmed.Load() {
				return ErrIO
			}
			return nil
		},
		beforeWALSyncHook: func() {
			walHookCalls.Add(1)
			rec.record("wal_sync")
			if actor.blobs.hasDirty() {
				walSawDirty.Store(true)
			}
			walSawPackData.Store(blobSyncSampleCountInternal(t, reg, "pack_data", "flush_wal"))
			walSawPacksDir.Store(blobSyncSampleCountInternal(t, reg, "packs_dir", "flush_wal"))
		},
	})
	require.NoError(t, err)
	defer func() {
		packErrArmed.Store(false)
		require.NoError(t, actor.Close())
	}()

	_ = createAndWriteInternal(t, actor, "f1.bin", 0x51, BlobSize)
	require.True(t, actor.blobs.hasDirty())
	require.True(t, actor.WALDirty())

	require.NoError(t, actor.FlushWAL())
	require.Equal(t, int32(1), walHookCalls.Load())
	require.False(t, walSawDirty.Load())
	require.GreaterOrEqual(t, walSawPackData.Load(), uint64(1))
	require.GreaterOrEqual(t, walSawPacksDir.Load(), uint64(1))
	require.Equal(t, []string{"pack_sync", "wal_sync"}, rec.snapshot())
	require.False(t, actor.blobs.hasDirty())
	require.False(t, actor.WALDirty())

	walHookCalls.Store(0)
	require.NoError(t, actor.FlushWAL())
	require.Equal(t, int32(0), walHookCalls.Load())

	_ = createAndWriteInternal(t, actor, "f2.bin", 0x52, BlobSize)
	_ = createAndWriteInternal(t, actor, "f3.bin", 0x53, BlobSize)
	require.NoError(t, actor.Unlink(ctx, RootIno, []byte("f1.bin"), false))
	require.NoError(t, actor.Unlink(ctx, RootIno, []byte("f2.bin"), false))
	require.Equal(t, 1, actor.MetricsSnapshot().PendingDeadPacks)
	require.True(t, actor.WALDirty())
	require.True(t, actor.blobs.hasDirty())

	walHookCalls.Store(0)
	packErrArmed.Store(true)
	err = actor.FlushWAL()
	require.ErrorIs(t, err, ErrIO)
	require.Equal(t, int32(0), walHookCalls.Load())
	require.True(t, actor.WALDirty())
	require.Equal(t, 1, actor.MetricsSnapshot().PendingDeadPacks)
	_, statErr := os.Stat(packFilePath(actor.blobs.dir, 1))
	require.NoError(t, statErr)

	packErrArmed.Store(false)
	require.NoError(t, actor.FlushWAL())
	require.Equal(t, int32(1), walHookCalls.Load())
	require.False(t, actor.WALDirty())
	actor.blobs.mu.Lock()
	dirtyPackCount := len(actor.blobs.dirtyPacks)
	actor.blobs.mu.Unlock()
	require.Zero(t, dirtyPackCount)
	require.Equal(t, 0, actor.MetricsSnapshot().PendingDeadPacks)
	_, statErr = os.Stat(packFilePath(actor.blobs.dir, 1))
	require.ErrorIs(t, statErr, os.ErrNotExist)

	require.True(t, actor.blobs.hasDirty())
	walHookCalls.Store(0)
	require.NoError(t, actor.FlushWAL())
	require.Equal(t, int32(1), walHookCalls.Load())
	require.False(t, actor.blobs.hasDirty())
}

func TestGivenDirtyPackWhenCloseRunsThenPackAndPacksDirSyncBeforeWALWithFlushWALTrigger(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	metrics := NewMetrics()
	reg := prometheus.NewRegistry()
	require.NoError(t, reg.Register(metrics))

	var (
		rec            eventRecorder
		walHookCalls   atomic.Int32
		walSawDirty    atomic.Bool
		walSawPackData atomic.Uint64
		walSawPacksDir atomic.Uint64
		actor          *StoreActor
		err            error
	)

	actor, err = Open(Options{
		Dir:      dir,
		DiskID:   "disk-step26-close",
		MaxBytes: 64 << 20,
		Metrics:  metrics,
		syncFileHook: func(_ uint64) {
			rec.record("pack_sync")
		},
		beforeWALSyncHook: func() {
			walHookCalls.Add(1)
			rec.record("wal_sync")
			if actor.blobs.hasDirty() {
				walSawDirty.Store(true)
			}
			walSawPackData.Store(blobSyncSampleCountInternal(t, reg, "pack_data", "flush_wal"))
			walSawPacksDir.Store(blobSyncSampleCountInternal(t, reg, "packs_dir", "flush_wal"))
		},
	})
	require.NoError(t, err)

	f := createAndWriteInternal(t, actor, "close.bin", 0x61, BlobSize)
	require.True(t, actor.blobs.hasDirty())
	require.True(t, actor.WALDirty())

	require.NoError(t, actor.Close())
	require.Equal(t, int32(1), walHookCalls.Load())
	require.False(t, walSawDirty.Load())
	require.GreaterOrEqual(t, walSawPackData.Load(), uint64(1))
	require.GreaterOrEqual(t, walSawPacksDir.Load(), uint64(1))
	require.Equal(t, []string{"pack_sync", "wal_sync"}, rec.snapshot())

	reopened, err := Open(Options{
		Dir:    dir,
		DiskID: "disk-step26-close",
	})
	require.NoError(t, err)
	defer func() { require.NoError(t, reopened.Close()) }()

	got, err := reopened.Read(ctx, f.Ino, 0, BlobSize)
	require.NoError(t, err)
	require.Equal(t, bytes.Repeat([]byte{0x61}, BlobSize), got)
}

func TestGivenDirtyPackAndWALWhenCloseDiscardRunsThenPackAndWALFlushSkipped(t *testing.T) {
	dir := t.TempDir()

	var (
		packSyncCalls atomic.Int32
		walSyncCalls  atomic.Int32
	)

	actor, err := Open(Options{
		Dir:      dir,
		DiskID:   "disk-step26-discard",
		MaxBytes: 64 << 20,
		syncFileHook: func(_ uint64) {
			packSyncCalls.Add(1)
		},
		beforeWALSyncHook: func() {
			walSyncCalls.Add(1)
		},
	})
	require.NoError(t, err)

	_ = createAndWriteInternal(t, actor, "discard.bin", 0x71, BlobSize)
	require.True(t, actor.blobs.hasDirty())
	require.True(t, actor.WALDirty())

	require.NoError(t, actor.CloseDiscard())
	require.Equal(t, int32(0), packSyncCalls.Load())
	require.Equal(t, int32(0), walSyncCalls.Load())
}

func TestGivenPriorPendingDeadPackAndAbortedBatchDeadPackWhenBarrierWALApplyFailsThenOnlyPriorPendingDeadPackRestored(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()

	var (
		failNextWAL   atomic.Bool
		walErrArmed   atomic.Bool
		tickHookArmed atomic.Bool
		tickEntered   = make(chan struct{})
		releaseTick   = make(chan struct{})
	)

	actor, err := Open(Options{
		Dir:         dir,
		DiskID:      "disk-step24-deadpack-isolation",
		MaxBytes:    64 << 20,
		MaxPackSize: BlobSize,
		beforeWALSyncHook: func() {
			if failNextWAL.Swap(false) {
				walErrArmed.Store(true)
			}
		},
		walApplyErrHook: func() error {
			if walErrArmed.Swap(false) {
				return ErrIO
			}
			return nil
		},
		beforeTickHook: makeOnceArmedTickHook(&tickHookArmed, tickEntered, releaseTick),
	})
	require.NoError(t, err)
	defer func() {
		failNextWAL.Store(false)
		walErrArmed.Store(false)
		require.NoError(t, actor.Close())
	}()

	_ = createAndWriteInternal(t, actor, "p1.bin", 0x11, BlobSize)
	_ = createAndWriteInternal(t, actor, "p2.bin", 0x22, BlobSize)
	p3 := createAndWriteInternal(t, actor, "p3.bin", 0x33, BlobSize)

	require.NoError(t, actor.Unlink(ctx, RootIno, []byte("p1.bin"), false))
	require.Equal(t, 1, actor.MetricsSnapshot().PendingDeadPacks)

	var (
		wg        sync.WaitGroup
		gateErr   error
		unlinkErr error
		fsyncErr  error
	)
	tickHookArmed.Store(true)
	wg.Go(func() {
		_, gateErr = actor.Create(ctx, CreateReq{ParentIno: RootIno, Name: []byte("gate.bin"), Mode: S_IFREG | 0o644})
	})
	awaitValueInternal(t, tickEntered, 2*time.Second, "beforeTickHook did not fire", func() {
		close(releaseTick)
	})

	wg.Go(func() {
		unlinkErr = actor.Unlink(ctx, RootIno, []byte("p2.bin"), false)
	})
	wg.Go(func() {
		fsyncErr = actor.Fsync(ctx, p3.Ino)
	})
	waitMailboxActiveAtLeast(t, actor, 2, 2*time.Second)

	failNextWAL.Store(true)
	close(releaseTick)
	wg.Wait()

	require.NoError(t, gateErr)
	require.ErrorIs(t, unlinkErr, ErrIO)
	require.ErrorIs(t, fsyncErr, ErrIO)

	actor.deadPacksMu.Lock()
	pending := append([]uint64(nil), actor.pendingDeadPacks...)
	actor.deadPacksMu.Unlock()
	require.Equal(t, []uint64{1}, pending)

	_, statErr := os.Stat(packFilePath(actor.blobs.dir, 1))
	require.NoError(t, statErr)
	_, statErr = os.Stat(packFilePath(actor.blobs.dir, 2))
	require.NoError(t, statErr)

	p2Attr, _, err := actor.Lookup(ctx, RootIno, []byte("p2.bin"))
	require.NoError(t, err)
	gotP2, err := actor.Read(ctx, p2Attr.Ino, 0, BlobSize)
	require.NoError(t, err)
	require.Equal(t, bytes.Repeat([]byte{0x22}, BlobSize), gotP2)

	require.NoError(t, actor.FlushWAL())
	_, statErr = os.Stat(packFilePath(actor.blobs.dir, 1))
	require.ErrorIs(t, statErr, os.ErrNotExist)
	_, statErr = os.Stat(packFilePath(actor.blobs.dir, 2))
	require.NoError(t, statErr)
}
