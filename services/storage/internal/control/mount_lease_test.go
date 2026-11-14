package control

import (
	"context"
	"math"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"

	"github.com/starsrc/bowl/services/storage/internal/db"
	"github.com/starsrc/bowl/services/storage/internal/store"
)

type fakeClock struct {
	unixNano atomic.Int64
}

func newFakeClock(start time.Time) *fakeClock {
	c := &fakeClock{}
	c.unixNano.Store(start.UnixNano())
	return c
}

func (c *fakeClock) Now() time.Time {
	return time.Unix(0, c.unixNano.Load()).UTC()
}

func (c *fakeClock) Advance(d time.Duration) {
	c.unixNano.Add(d.Nanoseconds())
}

func requireDiskMountedStatus(t *testing.T, err error) {
	t.Helper()
	require.ErrorIs(t, err, ErrDiskMounted)
	st := ToStatus(err)
	require.NotNil(t, st)
	require.Equal(t, codes.Aborted, st.Code())
	require.Len(t, st.Details(), 1)
	info, ok := st.Details()[0].(*errdetails.ErrorInfo)
	require.True(t, ok)
	require.Equal(t, "EBUSY", info.GetReason())
	require.Equal(t, ErrorDomain, info.GetDomain())
}

func TestMountLease_RWBlocksMerge_ROConcurrentAndDoesNotBlockMerge(t *testing.T) {
	ctx := context.Background()
	mgr := New(db.NewMemStore(), WithStorageRoot(t.TempDir()))
	t.Cleanup(func() { _ = mgr.Close() })

	target, err := mgr.CreateDisk(ctx, CreateDiskParams{Name: "target", MaxBytes: 32 << 20})
	require.NoError(t, err)
	tActor, err := mgr.Actor(ctx, target.DiskID)
	require.NoError(t, err)
	writeRegularFile(t, ctx, tActor, 0, "base.txt", []byte("base"))

	source, err := mgr.ForkDisk(ctx, ForkDiskParams{SourceDiskID: target.DiskID, Name: "source"})
	require.NoError(t, err)
	sActor, err := mgr.Actor(ctx, source.DiskID)
	require.NoError(t, err)
	writeRegularFile(t, ctx, sActor, 0, "feat.txt", []byte("feat"))

	// 非法组合或非法 epoch 参数必须返回 ErrInvalidArgument。
	_, err = mgr.MountDisk(ctx, MountDiskParams{DiskID: target.DiskID, ReadOnly: true, Force: true})
	require.ErrorIs(t, err, ErrInvalidArgument)
	_, err = mgr.RenewMount(ctx, RenewMountParams{DiskID: target.DiskID, Epoch: 0})
	require.ErrorIs(t, err, ErrInvalidArgument)
	_, err = mgr.RenewMount(ctx, RenewMountParams{DiskID: target.DiskID, Epoch: uint64(math.MaxInt64) + 1})
	require.ErrorIs(t, err, ErrInvalidArgument)
	require.ErrorIs(t, mgr.UnmountDisk(ctx, UnmountDiskParams{DiskID: target.DiskID, Epoch: 0}), ErrInvalidArgument)
	require.ErrorIs(t, mgr.UnmountDisk(ctx, UnmountDiskParams{DiskID: target.DiskID, Epoch: uint64(math.MaxInt64) + 1}), ErrInvalidArgument)

	// RW 挂载返回递增 Epoch 与 30s TTL，且拦截非强制二次 RW 挂载与目标盘 MergeDisk。
	rwMnt, err := mgr.MountDisk(ctx, MountDiskParams{DiskID: target.DiskID, ReadOnly: false})
	require.NoError(t, err)
	require.Equal(t, uint64(1), rwMnt.Epoch)
	require.Equal(t, uint32(30), rwMnt.LeaseTTLSeconds)

	_, err = mgr.MountDisk(ctx, MountDiskParams{DiskID: target.DiskID, ReadOnly: false})
	requireDiskMountedStatus(t, err)

	_, err = mgr.MergeDisk(ctx, &MergeDiskParams{
		SourceDiskID: source.DiskID,
		TargetDiskID: target.DiskID,
		Strategy:     store.MergeStrategyFail,
	})
	requireDiskMountedStatus(t, err)

	// 并发大量 RO 挂载始终无状态放行（epoch=0, lease_ttl_seconds=0），且不得阻塞 RW。
	const roConcurrency = 16
	var wg sync.WaitGroup
	roErrs := make([]error, roConcurrency)
	roResults := make([]*MountDiskResult, roConcurrency)
	for i := range roConcurrency {
		diskID := target.DiskID
		if i%2 == 1 {
			diskID = source.DiskID
		}
		wg.Go(func() {
			roResults[i], roErrs[i] = mgr.MountDisk(ctx, MountDiskParams{DiskID: diskID, ReadOnly: true})
		})
	}
	wg.Wait()
	for i := range roConcurrency {
		require.NoError(t, roErrs[i])
		require.Equal(t, uint64(0), roResults[i].Epoch)
		require.Equal(t, uint32(0), roResults[i].LeaseTTLSeconds)
		require.Equal(t, RootIno, roResults[i].RootAttr.Ino)
	}

	// 释放 RW 租约（重复 UnmountDisk 幂等返回 nil）。
	require.NoError(t, mgr.UnmountDisk(ctx, UnmountDiskParams{DiskID: target.DiskID, Epoch: rwMnt.Epoch}))
	require.NoError(t, mgr.UnmountDisk(ctx, UnmountDiskParams{DiskID: target.DiskID, Epoch: rwMnt.Epoch}))

	// 虽仍存在活跃 RO 挂载场景（RO 无租约状态），但 RW 已释放，MergeDisk 必须直接放行并成功提交。
	res, err := mgr.MergeDisk(ctx, &MergeDiskParams{
		SourceDiskID: source.DiskID,
		TargetDiskID: target.DiskID,
		Strategy:     store.MergeStrategyFail,
	})
	require.NoError(t, err)
	require.NotEmpty(t, res.MergeCommitID)
}

func TestMountLease_SourceRWRejects_DryRunAllowsWithoutClearing(t *testing.T) {
	ctx := context.Background()
	mgr := New(db.NewMemStore(), WithStorageRoot(t.TempDir()))
	t.Cleanup(func() { _ = mgr.Close() })

	target, err := mgr.CreateDisk(ctx, CreateDiskParams{Name: "target", MaxBytes: 32 << 20})
	require.NoError(t, err)
	tActor, err := mgr.Actor(ctx, target.DiskID)
	require.NoError(t, err)
	writeRegularFile(t, ctx, tActor, 0, "base.txt", []byte("base"))

	source, err := mgr.ForkDisk(ctx, ForkDiskParams{SourceDiskID: target.DiskID, Name: "source"})
	require.NoError(t, err)
	sActor, err := mgr.Actor(ctx, source.DiskID)
	require.NoError(t, err)
	writeRegularFile(t, ctx, sActor, 0, "feat.txt", []byte("feat"))

	// 1. 源盘 RW + 目标盘 RW 下，DryRun=true 放行且不清除租约。
	srcRW, err := mgr.MountDisk(ctx, MountDiskParams{DiskID: source.DiskID, ReadOnly: false})
	require.NoError(t, err)
	tgtRW, err := mgr.MountDisk(ctx, MountDiskParams{DiskID: target.DiskID, ReadOnly: false})
	require.NoError(t, err)

	dryRes, err := mgr.MergeDisk(ctx, &MergeDiskParams{
		SourceDiskID: source.DiskID,
		TargetDiskID: target.DiskID,
		Strategy:     store.MergeStrategyFail,
		DryRun:       true,
	})
	require.NoError(t, err)
	require.Empty(t, dryRes.MergeCommitID)

	// DryRun 不得清除活跃租约，立即调用正式合并仍被拦截。
	_, err = mgr.MergeDisk(ctx, &MergeDiskParams{
		SourceDiskID: source.DiskID,
		TargetDiskID: target.DiskID,
		Strategy:     store.MergeStrategyFail,
	})
	requireDiskMountedStatus(t, err)

	// 释放目标盘后，仅源盘存在未过期 RW 租约，正式合并仍被拒绝且错误包含 "source disk <id>"。
	require.NoError(t, mgr.UnmountDisk(ctx, UnmountDiskParams{DiskID: target.DiskID, Epoch: tgtRW.Epoch}))
	_, err = mgr.MergeDisk(ctx, &MergeDiskParams{
		SourceDiskID: source.DiskID,
		TargetDiskID: target.DiskID,
		Strategy:     store.MergeStrategyFail,
	})
	requireDiskMountedStatus(t, err)
	require.Contains(t, err.Error(), "source disk "+source.DiskID)

	// 2. 释放源盘 RW 并挂源盘 RO 后，正式合并成功放行。
	require.NoError(t, mgr.UnmountDisk(ctx, UnmountDiskParams{DiskID: source.DiskID, Epoch: srcRW.Epoch}))
	srcRO, err := mgr.MountDisk(ctx, MountDiskParams{DiskID: source.DiskID, ReadOnly: true})
	require.NoError(t, err)
	require.Equal(t, uint64(0), srcRO.Epoch)

	res, err := mgr.MergeDisk(ctx, &MergeDiskParams{
		SourceDiskID: source.DiskID,
		TargetDiskID: target.DiskID,
		Strategy:     store.MergeStrategyFail,
	})
	require.NoError(t, err)
	require.NotEmpty(t, res.MergeCommitID)
}

func TestMountLease_RenewAndVirtualClockExpiration(t *testing.T) {
	ctx := context.Background()
	clk := newFakeClock(time.Date(2026, 11, 14, 0, 0, 0, 0, time.UTC))
	mgr := New(db.NewMemStore(), WithStorageRoot(t.TempDir()), WithClock(clk.Now))
	t.Cleanup(func() { _ = mgr.Close() })

	target, err := mgr.CreateDisk(ctx, CreateDiskParams{Name: "target", MaxBytes: 32 << 20})
	require.NoError(t, err)
	tActor, err := mgr.Actor(ctx, target.DiskID)
	require.NoError(t, err)
	writeRegularFile(t, ctx, tActor, 0, "base.txt", []byte("base"))

	source, err := mgr.ForkDisk(ctx, ForkDiskParams{SourceDiskID: target.DiskID, Name: "source"})
	require.NoError(t, err)
	sActor, err := mgr.Actor(ctx, source.DiskID)
	require.NoError(t, err)
	writeRegularFile(t, ctx, sActor, 0, "feat.txt", []byte("feat"))

	mnt, err := mgr.MountDisk(ctx, MountDiskParams{DiskID: target.DiskID, ReadOnly: false})
	require.NoError(t, err)

	// 推进 +20s 续期。
	clk.Advance(20 * time.Second)
	renewRes, err := mgr.RenewMount(ctx, RenewMountParams{
		DiskID: target.DiskID,
		Epoch:  mnt.Epoch,
	})
	require.NoError(t, err)
	require.Equal(t, uint32(30), renewRes.LeaseTTLSeconds)

	// 再推进 +20s（距上次续期 20s < 30s，虽距首次挂载已 40s），仍拦截 MergeDisk 与非强制 MountDisk。
	clk.Advance(20 * time.Second)
	_, err = mgr.MountDisk(ctx, MountDiskParams{DiskID: target.DiskID, ReadOnly: false})
	requireDiskMountedStatus(t, err)

	_, err = mgr.MergeDisk(ctx, &MergeDiskParams{
		SourceDiskID: source.DiskID,
		TargetDiskID: target.DiskID,
		Strategy:     store.MergeStrategyFail,
	})
	requireDiskMountedStatus(t, err)

	// 再推进 +11s（距上次续期 31s > 30s），租约自动过期，旧 epoch 续约与写入被拒，MergeDisk 自动放行。
	clk.Advance(11 * time.Second)
	_, err = mgr.RenewMount(ctx, RenewMountParams{DiskID: target.DiskID, Epoch: mnt.Epoch})
	require.ErrorIs(t, err, ErrStaleEpoch)

	res, err := mgr.MergeDisk(ctx, &MergeDiskParams{
		SourceDiskID: source.DiskID,
		TargetDiskID: target.DiskID,
		Strategy:     store.MergeStrategyFail,
	})
	require.NoError(t, err)
	require.NotEmpty(t, res.MergeCommitID)
}

func TestMountLease_RestartPersistsGate_ForcePreemptionAndExpiryRelease(t *testing.T) {
	ctx := context.Background()
	rootDir := t.TempDir()
	memDB := db.NewMemStore()
	clk := newFakeClock(time.Date(2026, 11, 14, 0, 0, 0, 0, time.UTC))

	mgr1 := New(memDB, WithStorageRoot(rootDir), WithClock(clk.Now))
	target, err := mgr1.CreateDisk(ctx, CreateDiskParams{Name: "target", MaxBytes: 32 << 20})
	require.NoError(t, err)
	tActor, err := mgr1.Actor(ctx, target.DiskID)
	require.NoError(t, err)
	writeRegularFile(t, ctx, tActor, 0, "base.txt", []byte("base"))

	source, err := mgr1.ForkDisk(ctx, ForkDiskParams{SourceDiskID: target.DiskID, Name: "source"})
	require.NoError(t, err)
	sActor, err := mgr1.Actor(ctx, source.DiskID)
	require.NoError(t, err)
	writeRegularFile(t, ctx, sActor, 0, "feat.txt", []byte("feat"))

	tgtMnt, err := mgr1.MountDisk(ctx, MountDiskParams{DiskID: target.DiskID, ReadOnly: false})
	require.NoError(t, err)
	epoch0 := tgtMnt.Epoch

	srcMnt, err := mgr1.MountDisk(ctx, MountDiskParams{DiskID: source.DiskID, ReadOnly: false})
	require.NoError(t, err)

	// 模拟 storage 进程重启（不调用 UnmountDisk，不推进时钟）。
	require.NoError(t, mgr1.Close())

	mgr2 := New(memDB, WithStorageRoot(rootDir), WithClock(clk.Now))
	t.Cleanup(func() { _ = mgr2.Close() })

	// 1. 重启后无需任何重启重建或 RenewMount：Pebble 持久化 WriterEpoch + WriterExpiresAt 直接门禁非强制 MountDisk 与 MergeDisk。
	_, err = mgr2.MountDisk(ctx, MountDiskParams{DiskID: target.DiskID, ReadOnly: false})
	requireDiskMountedStatus(t, err)

	_, err = mgr2.MergeDisk(ctx, &MergeDiskParams{
		SourceDiskID: source.DiskID,
		TargetDiskID: target.DiskID,
		Strategy:     store.MergeStrategyFail,
	})
	requireDiskMountedStatus(t, err)

	// epoch 不匹配时 RenewMount 返回 ErrStaleEpoch；匹配的 epoch 可正常续约。
	_, err = mgr2.RenewMount(ctx, RenewMountParams{DiskID: target.DiskID, Epoch: epoch0 + 99})
	require.ErrorIs(t, err, ErrStaleEpoch)
	renewRes, err := mgr2.RenewMount(ctx, RenewMountParams{DiskID: target.DiskID, Epoch: epoch0})
	require.NoError(t, err)
	require.Equal(t, uint32(30), renewRes.LeaseTTLSeconds)

	// 2. 使用 Force=true 抢占目标盘 RW 挂载：新写者拿到 epoch0+1，旧写者写入与 RenewMount 均返回 ErrStaleEpoch。
	preemptMnt, err := mgr2.MountDisk(ctx, MountDiskParams{DiskID: target.DiskID, ReadOnly: false, Force: true})
	require.NoError(t, err)
	require.Equal(t, epoch0+1, preemptMnt.Epoch)

	tActor2, err := mgr2.Actor(ctx, target.DiskID)
	require.NoError(t, err)
	_, staleWriteErr := tActor2.Create(ctx, store.CreateReq{
		ParentIno: RootIno,
		Name:      []byte("stale-writer.txt"),
		Mode:      store.S_IFREG | 0o644,
		Epoch:     epoch0,
	})
	require.ErrorIs(t, staleWriteErr, ErrStaleEpoch)

	_, staleRenewErr := mgr2.RenewMount(ctx, RenewMountParams{DiskID: target.DiskID, Epoch: epoch0})
	require.ErrorIs(t, staleRenewErr, ErrStaleEpoch)
	st := ToStatus(staleRenewErr)
	require.Equal(t, codes.FailedPrecondition, st.Code())

	// 旧写者调用 UnmountDisk(epoch0) 幂等返回 nil，不得释放新写者的 epoch0+1 租约。
	require.NoError(t, mgr2.UnmountDisk(ctx, UnmountDiskParams{DiskID: target.DiskID, Epoch: epoch0}))
	require.True(t, tActor2.HasActiveWriterLease(ctx))

	// 新写者可正常续约与写入。
	_, err = mgr2.RenewMount(ctx, RenewMountParams{DiskID: target.DiskID, Epoch: preemptMnt.Epoch})
	require.NoError(t, err)
	writeRegularFile(t, ctx, tActor2, preemptMnt.Epoch, "owner2.txt", []byte("owner2"))

	// 3. 验证 Force=true MergeDisk 在双盘均持未过期租约时可直接合并，且合并后目标盘 epoch 再递增且无活跃租约。
	mergeRes, err := mgr2.MergeDisk(ctx, &MergeDiskParams{
		SourceDiskID: source.DiskID,
		TargetDiskID: target.DiskID,
		Strategy:     store.MergeStrategyFail,
		Force:        true,
	})
	require.NoError(t, err)
	require.NotEmpty(t, mergeRes.MergeCommitID)

	newTActor, err := mgr2.Actor(ctx, target.DiskID)
	require.NoError(t, err)
	require.Equal(t, preemptMnt.Epoch+1, newTActor.WriterEpoch())
	require.False(t, newTActor.HasActiveWriterLease(ctx))
	_, err = mgr2.RenewMount(ctx, RenewMountParams{DiskID: target.DiskID, Epoch: preemptMnt.Epoch})
	require.ErrorIs(t, err, ErrStaleEpoch)

	// 4. 源盘不再卸载，推进时钟超过 30s 后租约自动过期，无需 Force 即可再次合入或重新无 Force 挂载。
	clk.Advance(31 * time.Second)
	_, err = mgr2.RenewMount(ctx, RenewMountParams{DiskID: source.DiskID, Epoch: srcMnt.Epoch})
	require.ErrorIs(t, err, ErrStaleEpoch)

	cleanFork, err := mgr2.ForkDisk(ctx, ForkDiskParams{SourceDiskID: target.DiskID, Name: "clean"})
	require.NoError(t, err)
	_, err = mgr2.MergeDisk(ctx, &MergeDiskParams{
		SourceDiskID: cleanFork.DiskID,
		TargetDiskID: target.DiskID,
		Strategy:     store.MergeStrategyFail,
	})
	require.NoError(t, err)
}

func TestMountLease_DeleteDiskReturnsNotFoundAndCustomTTLApplies(t *testing.T) {
	ctx := context.Background()
	clk := newFakeClock(time.Date(2026, 11, 14, 0, 0, 0, 0, time.UTC))
	mgr := New(db.NewMemStore(), WithStorageRoot(t.TempDir()), WithClock(clk.Now), WithMountLeaseTTL(5*time.Second))
	t.Cleanup(func() { _ = mgr.Close() })

	disk, err := mgr.CreateDisk(ctx, CreateDiskParams{Name: "ephemeral", MaxBytes: 16 << 20})
	require.NoError(t, err)
	mnt, err := mgr.MountDisk(ctx, MountDiskParams{DiskID: disk.DiskID, ReadOnly: false})
	require.NoError(t, err)
	require.Equal(t, uint32(5), mnt.LeaseTTLSeconds)

	// 到期边界前（+4s）仍活跃，超过 5s 后自动失效放行再次无 Force 挂载。
	clk.Advance(4 * time.Second)
	_, err = mgr.MountDisk(ctx, MountDiskParams{DiskID: disk.DiskID, ReadOnly: false})
	requireDiskMountedStatus(t, err)

	clk.Advance(2 * time.Second)
	mnt2, err := mgr.MountDisk(ctx, MountDiskParams{DiskID: disk.DiskID, ReadOnly: false})
	require.NoError(t, err)
	require.Equal(t, mnt.Epoch+1, mnt2.Epoch)

	require.NoError(t, mgr.DeleteDisk(ctx, disk.DiskID))

	// DeleteDisk 销毁磁盘后，RenewMount 与 UnmountDisk 返回 ErrNotFound。
	_, err = mgr.RenewMount(ctx, RenewMountParams{
		DiskID: disk.DiskID,
		Epoch:  mnt2.Epoch,
	})
	require.ErrorIs(t, err, ErrNotFound)
	require.ErrorIs(t, mgr.UnmountDisk(ctx, UnmountDiskParams{
		DiskID: disk.DiskID,
		Epoch:  mnt2.Epoch,
	}), ErrNotFound)
}

func TestMountLease_ConcurrentMergeEvictionRetryOnce(t *testing.T) {
	ctx := context.Background()
	mgr := New(db.NewMemStore(), WithStorageRoot(t.TempDir()))
	t.Cleanup(func() { _ = mgr.Close() })

	target, err := mgr.CreateDisk(ctx, CreateDiskParams{Name: "target", MaxBytes: 32 << 20})
	require.NoError(t, err)
	tActor, err := mgr.Actor(ctx, target.DiskID)
	require.NoError(t, err)
	writeRegularFile(t, ctx, tActor, 0, "base.txt", []byte("base"))

	source, err := mgr.ForkDisk(ctx, ForkDiskParams{SourceDiskID: target.DiskID, Name: "source"})
	require.NoError(t, err)
	sActor, err := mgr.Actor(ctx, source.DiskID)
	require.NoError(t, err)
	writeRegularFile(t, ctx, sActor, 0, "feat.txt", []byte("feat"))

	mnt, err := mgr.MountDisk(ctx, MountDiskParams{DiskID: target.DiskID, ReadOnly: false})
	require.NoError(t, err)

	var (
		wg        sync.WaitGroup
		mergeDone atomic.Bool
		mergeErr  error
		roMu      sync.Mutex
		roErrs    []error
		roCalls   atomic.Int64
	)
	wg.Go(func() {
		defer mergeDone.Store(true)
		_, mergeErr = mgr.MergeDisk(ctx, &MergeDiskParams{
			SourceDiskID: source.DiskID,
			TargetDiskID: target.DiskID,
			Strategy:     store.MergeStrategyFail,
			Force:        true,
		})
	})

	wg.Go(func() {
		for !mergeDone.Load() {
			_, roErr := mgr.MountDisk(ctx, MountDiskParams{DiskID: target.DiskID, ReadOnly: true})
			roCalls.Add(1)
			if roErr != nil {
				roMu.Lock()
				roErrs = append(roErrs, roErr)
				roMu.Unlock()
			}
		}
	})

	wg.Wait()
	require.NoError(t, mergeErr)
	require.Positive(t, roCalls.Load())
	require.Empty(t, roErrs, "并发 MergeDisk Phase 2 替换 actor 期间，RO MountDisk 应通过重试一次收敛而非泄露 store.ErrClosed")

	// 验证目标盘新实例 epoch 已推进且旧 epoch 被拒绝（不再是 ErrClosed）。
	_, renewErr := mgr.RenewMount(ctx, RenewMountParams{DiskID: target.DiskID, Epoch: mnt.Epoch})
	require.ErrorIs(t, renewErr, ErrStaleEpoch)
}
