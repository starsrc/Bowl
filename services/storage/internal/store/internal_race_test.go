package store

import (
	"bytes"
	"context"
	"os"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"
)

func createAndWriteInternal(t *testing.T, actor *StoreActor, name string, fill byte, size int) Attribute {
	t.Helper()
	ctx := context.Background()
	attr, err := actor.Create(ctx, CreateReq{
		ParentIno: RootIno,
		Name:      []byte(name),
		Mode:      S_IFREG | 0o644,
	})
	require.NoError(t, err)
	_, _, err = actor.Write(ctx, attr.Ino, 0, bytes.Repeat([]byte{fill}, size))
	require.NoError(t, err)
	return attr
}

func gatherHistogramMapInternal(t *testing.T, reg *prometheus.Registry) map[string][]*dto.Metric {
	t.Helper()
	fams, err := reg.Gather()
	require.NoError(t, err)
	out := make(map[string][]*dto.Metric, len(fams))
	for _, fam := range fams {
		out[fam.GetName()] = fam.GetMetric()
	}
	return out
}

func findMetricByLabelInternal(metrics []*dto.Metric, labelName, labelVal string) *dto.Metric {
	for _, m := range metrics {
		for _, lp := range m.GetLabel() {
			if lp.GetName() == labelName && lp.GetValue() == labelVal {
				return m
			}
		}
	}
	return nil
}

func awaitValueInternal[T any](t *testing.T, ch <-chan T, timeout time.Duration, msg string, onTimeout func()) T {
	t.Helper()
	select {
	case v := <-ch:
		return v
	case <-time.After(timeout):
		if onTimeout != nil {
			onTimeout()
		}
		t.Fatal(msg)
		var zero T
		return zero
	}
}

func requireChanPendingInternal(t *testing.T, ch <-chan error, msg string) {
	t.Helper()
	select {
	case err := <-ch:
		t.Fatalf("%s: unexpected early completion err=%v", msg, err)
	default:
	}
}

func waitMailboxActiveAtLeast(t *testing.T, actor *StoreActor, want int, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		actor.mb.mu.Lock()
		n := len(actor.mb.active)
		actor.mb.mu.Unlock()
		if n >= want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("等待邮箱排队请求超时: want>=%d got=%d", want, n)
		}
		runtime.Gosched()
	}
}

func makeDualRemovePackHook(
	entered1 chan<- uint64, release1 <-chan struct{},
	entered2 chan<- uint64, release2 <-chan struct{},
) func(uint64) {
	return func(packID uint64) {
		switch packID {
		case 1:
			entered1 <- packID
			<-release1
		case 2:
			entered2 <- packID
			<-release2
		}
	}
}

func makeOnceArmedSyncHook(armed *atomic.Bool, calls *atomic.Int32, entered chan struct{}, release <-chan struct{}) func(uint64) {
	var once sync.Once
	return func(_ uint64) {
		if !armed.Load() {
			return
		}
		calls.Add(1)
		once.Do(func() {
			close(entered)
			<-release
		})
	}
}

func makeOnceArmedTickHook(armed *atomic.Bool, entered chan struct{}, release <-chan struct{}) func() {
	var once sync.Once
	return func() {
		if !armed.Load() {
			return
		}
		once.Do(func() {
			close(entered)
			<-release
		})
	}
}

func TestCompactPacksPhaseACASFenceAgainstConcurrentOverwriteTruncateAndUnlink(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	actor1, err := Open(Options{
		Dir:      dir,
		DiskID:   "disk-compact-cas",
		MaxBytes: 128 << 20,
	})
	require.NoError(t, err)

	_ = createAndWriteInternal(t, actor1, "dead.bin", 0xAA, BlobSize)
	fKeep := createAndWriteInternal(t, actor1, "keep.bin", 0xBB, 32768)
	fOverwrite := createAndWriteInternal(t, actor1, "overwrite.bin", 0xCC, 32768)
	fTruncate := createAndWriteInternal(t, actor1, "truncate.bin", 0xDD, 32768)
	fUnlink := createAndWriteInternal(t, actor1, "unlink.bin", 0xEE, 32768)
	require.Equal(t, uint64(1), actor1.blobs.activePackID())
	require.NoError(t, actor1.Close())

	newOverwritePayload := bytes.Repeat([]byte{0xFE}, 32768)
	var hookTriggered bool
	var actor2 *StoreActor
	actor2, err = Open(Options{
		Dir:      dir,
		DiskID:   "disk-compact-cas",
		MaxBytes: 128 << 20,
		beforeCompactRelocateHook: func() {
			hookTriggered = true
			_, _, hErr := actor2.Write(ctx, fOverwrite.Ino, 0, newOverwritePayload)
			require.NoError(t, hErr)

			_, hErr = actor2.SetAttr(ctx, fTruncate.Ino, SetAttrReq{Size: new(uint64(0))})
			require.NoError(t, hErr)

			hErr = actor2.Unlink(ctx, RootIno, []byte("unlink.bin"), false)
			require.NoError(t, hErr)
		},
	})
	require.NoError(t, err)
	defer func() { require.NoError(t, actor2.Close()) }()

	require.NoError(t, actor2.Unlink(ctx, RootIno, []byte("dead.bin"), false))

	require.NoError(t, actor2.CompactPacks(ctx))
	require.True(t, hookTriggered, "必须在搬迁读取追加后、提交 opCompactRelocate 前触发并发写钩子")

	_, statErr := os.Stat(packFilePath(actor2.blobs.dir, 1))
	require.ErrorIs(t, statErr, os.ErrNotExist)

	gotKeep, err := actor2.Read(ctx, fKeep.Ino, 0, 32768)
	require.NoError(t, err)
	require.Equal(t, bytes.Repeat([]byte{0xBB}, 32768), gotKeep)

	gotOverwrite, err := actor2.Read(ctx, fOverwrite.Ino, 0, 32768)
	require.NoError(t, err)
	require.Equal(t, newOverwritePayload, gotOverwrite)

	truncAttr, err := actor2.GetAttr(ctx, fTruncate.Ino)
	require.NoError(t, err)
	require.Zero(t, truncAttr.Size)
	gotTrunc, err := actor2.Read(ctx, fTruncate.Ino, 0, 32768)
	require.NoError(t, err)
	require.Empty(t, gotTrunc)

	_, err = actor2.GetAttr(ctx, fUnlink.Ino)
	require.ErrorIs(t, err, ErrNotFound)

	snap := actor2.MetricsSnapshot()
	require.Equal(t, uint64(65536), snap.PackLiveBytes)
	require.Equal(t, uint64(5*32768), snap.PackPhysicalBytes)
}

func TestAsyncDeadPackReclaimNonBlockingFsyncAndDrainOnClose(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()

	removeEntered1 := make(chan uint64, 2)
	releaseRemove1 := make(chan struct{})
	var release1Once sync.Once

	removeEntered2 := make(chan uint64, 2)
	releaseRemove2 := make(chan struct{})
	var release2Once sync.Once

	actor, err := Open(Options{
		Dir:            dir,
		DiskID:         "disk-async-reclaim",
		MaxBytes:       64 << 20,
		MaxPackSize:    2 * BlobSize,
		removePackHook: makeDualRemovePackHook(removeEntered1, releaseRemove1, removeEntered2, releaseRemove2),
	})
	require.NoError(t, err)

	f, err := actor.Create(ctx, CreateReq{
		ParentIno: RootIno,
		Name:      []byte("reclaim.bin"),
		Mode:      S_IFREG | 0o644,
	})
	require.NoError(t, err)

	_, _, err = actor.Write(ctx, f.Ino, 0, bytes.Repeat([]byte{0x11}, 2*BlobSize))
	require.NoError(t, err)
	_, _, err = actor.Write(ctx, f.Ino, 0, bytes.Repeat([]byte{0x22}, 2*BlobSize))
	require.NoError(t, err)

	_, _, err = actor.Write(ctx, f.Ino, 2*BlobSize, bytes.Repeat([]byte{0x33}, BlobSize))
	require.NoError(t, err)
	_, statErr := os.Stat(packFilePath(actor.blobs.dir, 1))
	require.NoError(t, statErr)

	fsyncDone := make(chan error, 1)
	go func() {
		fsyncDone <- actor.Fsync(ctx, f.Ino)
	}()

	fsyncErr := awaitValueInternal(t, fsyncDone, 2*time.Second, "前台 Fsync 被后台死包删除同步阻塞", func() {
		release1Once.Do(func() { close(releaseRemove1) })
		release2Once.Do(func() { close(releaseRemove2) })
	})
	require.NoError(t, fsyncErr)

	id1 := awaitValueInternal(t, removeEntered1, 2*time.Second, "后台死包回收协程未收到待删死包 1", func() {
		release1Once.Do(func() { close(releaseRemove1) })
		release2Once.Do(func() { close(releaseRemove2) })
	})
	require.Equal(t, uint64(1), id1)
	release1Once.Do(func() { close(releaseRemove1) })
	actor.blobs.waitPendingReclaims()
	_, statErr = os.Stat(packFilePath(actor.blobs.dir, 1))
	require.ErrorIs(t, statErr, os.ErrNotExist, "waitPendingReclaims 返回后死包 1 必须已物理删除")

	_, _, err = actor.Write(ctx, f.Ino, 0, bytes.Repeat([]byte{0x44}, 2*BlobSize))
	require.NoError(t, err)
	require.NoError(t, actor.Fsync(ctx, f.Ino))

	id2 := awaitValueInternal(t, removeEntered2, 2*time.Second, "后台死包回收协程未收到待删死包 2", func() {
		release2Once.Do(func() { close(releaseRemove2) })
	})
	require.Equal(t, uint64(2), id2)

	closeDone := make(chan error, 1)
	go func() {
		closeDone <- actor.Close()
	}()
	requireChanPendingInternal(t, closeDone, "后台死包回收未释放前 Close 不得提前返回")

	release2Once.Do(func() { close(releaseRemove2) })
	closeErr := awaitValueInternal(t, closeDone, 2*time.Second, "释放死包回收钩子后 Close 未能排空返回", nil)
	require.NoError(t, closeErr)
	_, statErr = os.Stat(packFilePath(actor.blobs.dir, 2))
	require.ErrorIs(t, statErr, os.ErrNotExist, "Close 返回前必须排空后台死包回收队列并删净 Pack 2")
}

func TestSyncBarrierCoalescingAndParallelPackSync(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	metrics := NewMetrics()
	reg := prometheus.NewRegistry()
	require.NoError(t, reg.Register(metrics))

	var (
		syncHookArmed atomic.Bool
		syncHookCalls atomic.Int32
		syncEntered   = make(chan struct{})
		releaseSync   = make(chan struct{})

		tickHookArmed atomic.Bool
		tickEntered   = make(chan struct{})
		releaseTick   = make(chan struct{})
	)

	actor, err := Open(Options{
		Dir:            dir,
		DiskID:         "disk-metrics-sync",
		MaxBytes:       64 << 20,
		Metrics:        metrics,
		syncFileHook:   makeOnceArmedSyncHook(&syncHookArmed, &syncHookCalls, syncEntered, releaseSync),
		beforeTickHook: makeOnceArmedTickHook(&tickHookArmed, tickEntered, releaseTick),
	})
	require.NoError(t, err)
	defer func() { require.NoError(t, actor.Close()) }()

	dirAttr, err := actor.Mkdir(ctx, MkdirReq{
		ParentIno: RootIno,
		Name:      []byte("sub"),
		Mode:      S_IFDIR | 0o755,
	})
	require.NoError(t, err)
	f, err := actor.Create(ctx, CreateReq{
		ParentIno: dirAttr.Ino,
		Name:      []byte("data.bin"),
		Mode:      S_IFREG | 0o644,
	})
	require.NoError(t, err)

	_, _, err = actor.Write(ctx, f.Ino, 0, bytes.Repeat([]byte{0x31}, BlobSize))
	require.NoError(t, err)

	syncHookArmed.Store(true)
	fsyncDone := make(chan error, 1)
	go func() {
		fsyncDone <- actor.Fsync(ctx, f.Ino)
	}()
	awaitValueInternal(t, syncEntered, 2*time.Second, "Fsync 屏障未触发 .pack syncFileHook", func() {
		close(releaseSync)
	})
	close(releaseSync)
	require.NoError(t, awaitValueInternal(t, fsyncDone, 2*time.Second, "释放 syncFileHook 后 Fsync 未完成", nil))
	require.Equal(t, int32(1), syncHookCalls.Load())
	syncHookArmed.Store(false)

	f2, err := actor.Create(ctx, CreateReq{
		ParentIno: dirAttr.Ino,
		Name:      []byte("merged.bin"),
		Mode:      S_IFREG | 0o644,
	})
	require.NoError(t, err)
	_, _, err = actor.Write(ctx, f2.Ino, 0, bytes.Repeat([]byte{0x42}, BlobSize))
	require.NoError(t, err)

	var (
		wg   sync.WaitGroup
		mErr error
		cErr error
		fErr error
	)
	tickHookArmed.Store(true)

	wg.Go(func() {
		_, mErr = actor.Mkdir(ctx, MkdirReq{
			ParentIno: dirAttr.Ino,
			Name:      []byte("co-batch-dir"),
			Mode:      S_IFDIR | 0o755,
		})
	})
	awaitValueInternal(t, tickEntered, 2*time.Second, "beforeTickHook 未能截获首轮 tick", func() {
		close(releaseTick)
	})

	wg.Go(func() {
		_, cErr = actor.Mkdir(ctx, MkdirReq{
			ParentIno: dirAttr.Ino,
			Name:      []byte("co-batch-dir-2"),
			Mode:      S_IFDIR | 0o755,
		})
	})
	wg.Go(func() {
		fErr = actor.Fsync(ctx, f2.Ino)
	})
	waitMailboxActiveAtLeast(t, actor, 2, 2*time.Second)

	close(releaseTick)
	wg.Wait()
	require.NoError(t, mErr)
	require.NoError(t, cErr)
	require.NoError(t, fErr)
	tickHookArmed.Store(false)

	gathered := gatherHistogramMapInternal(t, reg)
	phaseMetrics := gathered["bowl_storage_actor_tick_phase_duration_seconds"]
	walSyncMetric := findMetricByLabelInternal(phaseMetrics, "phase", "wal_apply_sync")
	require.NotNil(t, walSyncMetric, "合并同步批次后必须记录 wal_apply_sync 直方图")
	require.GreaterOrEqual(t, walSyncMetric.GetHistogram().GetSampleCount(), uint64(2), "两次同步屏障应均记录 wal_apply_sync")
}
