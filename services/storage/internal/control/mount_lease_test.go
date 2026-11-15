package control

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"

	"github.com/starsrc/bowl/pkg/rpcerr"
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

func requireValidNonNilUUID(t *testing.T, raw string) {
	t.Helper()
	parsed, err := uuid.Parse(raw)
	require.NoError(t, err, "MountID 必须为合法 UUID: got=%q", raw)
	require.NotEqual(t, uuid.Nil, parsed, "MountID 不得为全零 UUID")
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
	require.Equal(t, rpcerr.Domain, info.GetDomain())
}

func TestMountLease_IssueUnmountAndRejectOnlineTargetMerge(t *testing.T) {
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

	// RW 挂载返回合法非零 UUID MountID 与 30s TTL，且拦截目标盘 MergeDisk
	rwMnt, err := mgr.MountDisk(ctx, MountDiskParams{DiskID: target.DiskID, ReadOnly: false})
	require.NoError(t, err)
	requireValidNonNilUUID(t, rwMnt.MountID)
	require.Equal(t, uint32(30), rwMnt.LeaseTTLSeconds)

	_, err = mgr.MergeDisk(ctx, &MergeDiskParams{
		SourceDiskID: source.DiskID,
		TargetDiskID: target.DiskID,
		Strategy:     store.MergeStrategyFail,
	})
	requireDiskMountedStatus(t, err)

	// 同时存在 RO 挂载，释放 RW 后仍因 RO 活跃租约拦截 MergeDisk
	roMnt, err := mgr.MountDisk(ctx, MountDiskParams{DiskID: target.DiskID, ReadOnly: true})
	require.NoError(t, err)
	requireValidNonNilUUID(t, roMnt.MountID)
	require.Equal(t, uint32(30), roMnt.LeaseTTLSeconds)

	require.NoError(t, mgr.UnmountDisk(ctx, UnmountDiskParams{DiskID: target.DiskID, MountID: rwMnt.MountID}))
	// 重复调用 UnmountDisk 必须幂等返回 nil
	require.NoError(t, mgr.UnmountDisk(ctx, UnmountDiskParams{DiskID: target.DiskID, MountID: rwMnt.MountID}))

	_, err = mgr.MergeDisk(ctx, &MergeDiskParams{
		SourceDiskID: source.DiskID,
		TargetDiskID: target.DiskID,
		Strategy:     store.MergeStrategyFail,
	})
	requireDiskMountedStatus(t, err)

	// 释放 RO 租约后，MergeDisk 成功提交
	require.NoError(t, mgr.UnmountDisk(ctx, UnmountDiskParams{DiskID: target.DiskID, MountID: roMnt.MountID}))
	res, err := mgr.MergeDisk(ctx, &MergeDiskParams{
		SourceDiskID: source.DiskID,
		TargetDiskID: target.DiskID,
		Strategy:     store.MergeStrategyFail,
	})
	require.NoError(t, err)
	require.NotEmpty(t, res.MergeCommitID)
}

func TestMountLease_SourceRWRejects_SourceROAllows_DryRunAllowsWithoutClearing(t *testing.T) {
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

	// 1. 源盘 RW + 目标盘 RW 下，DryRun=true 放行且不清除租约
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

	// DryRun 不得清除活跃租约，立即调用正式合并仍被拦截
	_, err = mgr.MergeDisk(ctx, &MergeDiskParams{
		SourceDiskID: source.DiskID,
		TargetDiskID: target.DiskID,
		Strategy:     store.MergeStrategyFail,
	})
	requireDiskMountedStatus(t, err)

	// 释放目标盘后，仅源盘存在未过期 RW 租约，正式合并仍被拒绝且错误包含 "source disk <id>"
	require.NoError(t, mgr.UnmountDisk(ctx, UnmountDiskParams{DiskID: target.DiskID, MountID: tgtRW.MountID}))
	_, err = mgr.MergeDisk(ctx, &MergeDiskParams{
		SourceDiskID: source.DiskID,
		TargetDiskID: target.DiskID,
		Strategy:     store.MergeStrategyFail,
	})
	requireDiskMountedStatus(t, err)
	require.Contains(t, err.Error(), "source disk "+source.DiskID)

	// 2. 释放源盘 RW 后改挂源盘 RO，目标盘未挂载时，正式合并成功放行
	require.NoError(t, mgr.UnmountDisk(ctx, UnmountDiskParams{DiskID: source.DiskID, MountID: srcRW.MountID}))
	srcRO, err := mgr.MountDisk(ctx, MountDiskParams{DiskID: source.DiskID, ReadOnly: true})
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = mgr.UnmountDisk(ctx, UnmountDiskParams{DiskID: source.DiskID, MountID: srcRO.MountID})
	})

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

	// 推进 +20s 续期
	clk.Advance(20 * time.Second)
	renewRes, err := mgr.RenewMount(ctx, RenewMountParams{
		DiskID:   target.DiskID,
		MountID:  mnt.MountID,
		ReadOnly: false,
		Epoch:    mnt.Epoch,
	})
	require.NoError(t, err)
	require.Equal(t, uint32(30), renewRes.LeaseTTLSeconds)

	// 再推进 +20s（距上次续期 20s < 30s，虽距首次挂载已 40s），仍拦截 MergeDisk
	clk.Advance(20 * time.Second)
	_, err = mgr.MergeDisk(ctx, &MergeDiskParams{
		SourceDiskID: source.DiskID,
		TargetDiskID: target.DiskID,
		Strategy:     store.MergeStrategyFail,
	})
	requireDiskMountedStatus(t, err)

	// 再推进 +11s（距上次续期 31s > 30s），租约自动过期，MergeDisk 自动放行
	clk.Advance(11 * time.Second)
	res, err := mgr.MergeDisk(ctx, &MergeDiskParams{
		SourceDiskID: source.DiskID,
		TargetDiskID: target.DiskID,
		Strategy:     store.MergeStrategyFail,
	})
	require.NoError(t, err)
	require.NotEmpty(t, res.MergeCommitID)
}

func TestMountLease_RestartRebuildAndForcePreemption(t *testing.T) {
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

	roDisk, err := mgr1.CreateDisk(ctx, CreateDiskParams{Name: "ro-disk", MaxBytes: 16 << 20})
	require.NoError(t, err)

	tgtMnt, err := mgr1.MountDisk(ctx, MountDiskParams{DiskID: target.DiskID, ReadOnly: false})
	require.NoError(t, err)
	epoch0 := tgtMnt.Epoch

	srcMnt, err := mgr1.MountDisk(ctx, MountDiskParams{DiskID: source.DiskID, ReadOnly: false})
	require.NoError(t, err)

	roMnt, err := mgr1.MountDisk(ctx, MountDiskParams{DiskID: roDisk.DiskID, ReadOnly: true})
	require.NoError(t, err)

	// 模拟 storage 进程重启
	require.NoError(t, mgr1.Close())

	mgr2 := New(memDB, WithStorageRoot(rootDir), WithClock(clk.Now))
	t.Cleanup(func() { _ = mgr2.Close() })

	// 1. 重启后调用 RenewMount（epoch 匹配当前 WriterEpoch）自动重建 RW 租约并拦截 MergeDisk
	_, err = mgr2.RenewMount(ctx, RenewMountParams{
		DiskID:   target.DiskID,
		MountID:  tgtMnt.MountID,
		ReadOnly: false,
		Epoch:    epoch0,
	})
	require.NoError(t, err)

	_, err = mgr2.RenewMount(ctx, RenewMountParams{
		DiskID:   source.DiskID,
		MountID:  srcMnt.MountID,
		ReadOnly: false,
		Epoch:    srcMnt.Epoch,
	})
	require.NoError(t, err)

	// RO 续期重建同样校验磁盘存在并重建 RO 租约
	_, err = mgr2.RenewMount(ctx, RenewMountParams{
		DiskID:   roDisk.DiskID,
		MountID:  roMnt.MountID,
		ReadOnly: true,
		Epoch:    0,
	})
	require.NoError(t, err)

	// epoch 不匹配当前 WriterEpoch 时返回 ErrStaleEpoch
	_, err = mgr2.RenewMount(ctx, RenewMountParams{
		DiskID:   target.DiskID,
		MountID:  tgtMnt.MountID,
		ReadOnly: false,
		Epoch:    epoch0 + 99,
	})
	require.ErrorIs(t, err, ErrStaleEpoch)

	// 重建后，未传 Force 的 MergeDisk 再次被拦截
	_, err = mgr2.MergeDisk(ctx, &MergeDiskParams{
		SourceDiskID: source.DiskID,
		TargetDiskID: target.DiskID,
		Strategy:     store.MergeStrategyFail,
	})
	requireDiskMountedStatus(t, err)

	// 2. 双盘均处于未过期 RW 租约时，MergeDisk(Force: true) 成功提交，目标盘 WriterEpoch 递增且旧租约清除
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
	require.Equal(t, epoch0+1, newTActor.WriterEpoch())

	// 被抢占的旧目标盘续期返回 ErrStaleEpoch
	_, err = mgr2.RenewMount(ctx, RenewMountParams{
		DiskID:   target.DiskID,
		MountID:  tgtMnt.MountID,
		ReadOnly: false,
		Epoch:    epoch0,
	})
	require.ErrorIs(t, err, ErrStaleEpoch)

	// 释放源盘 RW 租约后，目标盘旧租约已清除，无需 Force 即可再次合入 cleanFork
	require.NoError(t, mgr2.UnmountDisk(ctx, UnmountDiskParams{DiskID: source.DiskID, MountID: srcMnt.MountID}))
	cleanFork, err := mgr2.ForkDisk(ctx, ForkDiskParams{SourceDiskID: target.DiskID, Name: "clean"})
	require.NoError(t, err)
	_, err = mgr2.MergeDisk(ctx, &MergeDiskParams{
		SourceDiskID: cleanFork.DiskID,
		TargetDiskID: target.DiskID,
		Strategy:     store.MergeStrategyFail,
	})
	require.NoError(t, err)
}

func TestMountLease_DeleteDiskClearsLeasesAndCustomTTLApplies(t *testing.T) {
	ctx := context.Background()
	clk := newFakeClock(time.Date(2026, 11, 14, 0, 0, 0, 0, time.UTC))
	mgr := New(db.NewMemStore(), WithStorageRoot(t.TempDir()), WithClock(clk.Now), WithMountLeaseTTL(5*time.Second))
	t.Cleanup(func() { _ = mgr.Close() })

	disk, err := mgr.CreateDisk(ctx, CreateDiskParams{Name: "ephemeral", MaxBytes: 16 << 20})
	require.NoError(t, err)
	mnt, err := mgr.MountDisk(ctx, MountDiskParams{DiskID: disk.DiskID, ReadOnly: false})
	require.NoError(t, err)
	require.Equal(t, uint32(5), mnt.LeaseTTLSeconds)

	require.NoError(t, mgr.DeleteDisk(ctx, disk.DiskID))

	// DeleteDisk 已清除租约且磁盘已销毁，后续 RenewMount 返回 ErrNotFound
	_, err = mgr.RenewMount(ctx, RenewMountParams{
		DiskID:   disk.DiskID,
		MountID:  mnt.MountID,
		ReadOnly: false,
		Epoch:    mnt.Epoch,
	})
	require.ErrorIs(t, err, ErrNotFound)
}
