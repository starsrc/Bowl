package store

import (
	"bytes"
	"context"
	"math"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/v2"
	"github.com/stretchr/testify/require"
)

func TestBranchStatCodec_49BytesAndWriterExpiresAt(t *testing.T) {
	require.Equal(t, 49, BranchStatSize)

	want := BranchStat{
		LogicalBytes:    123456,
		Inodes:          78,
		NextWriteSeq:    99,
		WriterEpoch:     12,
		MaxBytes:        64 << 20,
		WriterExpiresAt: math.MaxInt64 - 42,
	}
	encoded := EncodeBranchStat(want)
	require.Len(t, encoded, BranchStatSize)

	got, err := DecodeBranchStat(encoded)
	require.NoError(t, err)
	require.Equal(t, want, got)

	// 拒绝 41 字节旧格式
	_, err = DecodeBranchStat(encoded[:41])
	require.ErrorIs(t, err, ErrInvalidValue)
}

func TestWriterLease_AcquireRenewReleaseAndPersistence(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()

	cur := time.Date(2026, 5, 1, 10, 0, 0, 0, time.FixedZone("UTC+8", 8*3600))
	var clockMu sync.Mutex
	nowFn := func() time.Time {
		clockMu.Lock()
		defer clockMu.Unlock()
		return cur
	}
	advanceClock := func(d time.Duration) {
		clockMu.Lock()
		cur = cur.Add(d)
		clockMu.Unlock()
	}

	actor, err := Open(Options{
		Dir:      dir,
		DiskID:   "lease-lifecycle",
		MaxBytes: 64 << 20,
		Now:      nowFn,
	})
	require.NoError(t, err)

	// 1. 获取 30s UTC 租约并写入文件，校验跨重启恢复后租约状态完整保留
	ep1, rootAttr, err := actor.AcquireWriterLease(ctx, DefaultWriterLeaseTTL, false)
	require.NoError(t, err)
	require.Equal(t, uint64(1), ep1)
	require.Equal(t, RootIno, rootAttr.Ino)
	wantExp1 := cur.UTC().Add(DefaultWriterLeaseTTL).UnixNano()
	require.Equal(t, wantExp1, actor.WriterExpiresAt())
	require.True(t, actor.HasActiveWriterLease(ctx))

	f, err := actor.Create(ctx, CreateReq{
		ParentIno: RootIno,
		Name:      []byte("lease.bin"),
		Mode:      S_IFREG | 0o644,
		Epoch:     ep1,
	})
	require.NoError(t, err)
	_, _, err = actor.Write(ctx, f.Ino, 0, []byte("v1"), ep1)
	require.NoError(t, err)
	seqAfterWrite := actor.stat.NextWriteSeq

	// 非 force 在租约活跃期内必须被拒绝
	_, _, err = actor.AcquireWriterLease(ctx, DefaultWriterLeaseTTL, false)
	require.ErrorIs(t, err, ErrWriterLeaseActive)
	require.Equal(t, ep1, actor.WriterEpoch())

	require.NoError(t, actor.Close())

	// 重开 Actor 验证 49B BranchStat 从 Pebble 恢复
	reopened, err := Open(Options{
		Dir:    dir,
		DiskID: "lease-lifecycle",
		Now:    nowFn,
	})
	require.NoError(t, err)
	defer func() { _ = reopened.Close() }()
	require.Equal(t, ep1, reopened.WriterEpoch())
	require.Equal(t, wantExp1, reopened.WriterExpiresAt())
	require.True(t, reopened.HasActiveWriterLease(ctx))

	// 2. 停滞时钟下 RenewWriterLease 仍必须成功落盘且不递增 WriterEpoch / NextWriteSeq
	require.NoError(t, reopened.RenewWriterLease(ctx, ep1, DefaultWriterLeaseTTL))
	require.Equal(t, ep1, reopened.WriterEpoch())
	require.Equal(t, wantExp1, reopened.WriterExpiresAt())
	require.Equal(t, seqAfterWrite, reopened.stat.NextWriteSeq)

	// 推进时钟 10s 后续约，过期时间向后顺延
	advanceClock(10 * time.Second)
	require.NoError(t, reopened.RenewWriterLease(ctx, ep1, DefaultWriterLeaseTTL))
	wantExp2 := nowFn().UTC().Add(DefaultWriterLeaseTTL).UnixNano()
	require.Equal(t, wantExp2, reopened.WriterExpiresAt())
	require.Equal(t, ep1, reopened.WriterEpoch())
	require.Equal(t, seqAfterWrite, reopened.stat.NextWriteSeq)

	// 旧 epoch 或 epoch=0 续约被拒
	require.ErrorIs(t, reopened.RenewWriterLease(ctx, 0, DefaultWriterLeaseTTL), ErrStaleEpoch)
	require.ErrorIs(t, reopened.RenewWriterLease(ctx, ep1+1, DefaultWriterLeaseTTL), ErrStaleEpoch)

	// 3. 活跃期 force=true 抢占成功推进至 epoch=2，旧 epoch=1 续约与写入均返回 ErrStaleEpoch
	ep2, _, err := reopened.AcquireWriterLease(ctx, DefaultWriterLeaseTTL, true)
	require.NoError(t, err)
	require.Equal(t, uint64(2), ep2)
	require.ErrorIs(t, reopened.RenewWriterLease(ctx, ep1, DefaultWriterLeaseTTL), ErrStaleEpoch)
	_, _, err = reopened.Write(ctx, f.Ino, 0, []byte("stale"), ep1)
	require.ErrorIs(t, err, ErrStaleEpoch)

	// 4. 租约精确到期（now == expiresAt）：HasActiveWriterLease 为假，!force 可接管，续约与旧写被拒
	advanceClock(DefaultWriterLeaseTTL)
	require.False(t, reopened.HasActiveWriterLease(ctx))
	require.ErrorIs(t, reopened.RenewWriterLease(ctx, ep2, DefaultWriterLeaseTTL), ErrStaleEpoch)
	_, _, err = reopened.Write(ctx, f.Ino, 0, []byte("expired"), ep2)
	require.ErrorIs(t, err, ErrStaleEpoch)

	ep3, _, err := reopened.AcquireWriterLease(ctx, DefaultWriterLeaseTTL, false)
	require.NoError(t, err)
	require.Equal(t, uint64(3), ep3)
	require.True(t, reopened.HasActiveWriterLease(ctx))

	// 5. ReleaseWriterLease 幂等清零 WriterExpiresAt（WriterEpoch 保持 3 不回退）
	require.NoError(t, reopened.ReleaseWriterLease(ctx, ep1)) // 旧 epoch 幂等忽略
	require.True(t, reopened.HasActiveWriterLease(ctx))

	require.NoError(t, reopened.ReleaseWriterLease(ctx, ep3))
	require.False(t, reopened.HasActiveWriterLease(ctx))
	require.Equal(t, ep3, reopened.WriterEpoch())
	require.Equal(t, int64(0), reopened.WriterExpiresAt())
	require.NoError(t, reopened.ReleaseWriterLease(ctx, ep3)) // 重复释放幂等

	_, _, err = reopened.Write(ctx, f.Ino, 0, []byte("after-release"), ep3)
	require.ErrorIs(t, err, ErrStaleEpoch)

	// 释放后 !force 可立即获取 epoch=4，并验证孤儿清理仅在存在孤儿时递增 NextWriteSeq
	ep4, _, err := reopened.AcquireWriterLease(ctx, DefaultWriterLeaseTTL, false)
	require.NoError(t, err)
	require.Equal(t, uint64(4), ep4)

	// 无孤儿再次 force 获取，NextWriteSeq 不变；制造孤儿后再抢占，NextWriteSeq +1
	seqBeforeNoOrphan := reopened.stat.NextWriteSeq
	ep5, _, err := reopened.AcquireWriterLease(ctx, DefaultWriterLeaseTTL, true)
	require.NoError(t, err)
	require.Equal(t, uint64(5), ep5)
	require.Equal(t, seqBeforeNoOrphan, reopened.stat.NextWriteSeq)

	require.NoError(t, reopened.Unlink(ctx, RootIno, []byte("lease.bin"), true, ep5))
	seqBeforeOrphan := reopened.stat.NextWriteSeq
	ep6, _, err := reopened.AcquireWriterLease(ctx, DefaultWriterLeaseTTL, true)
	require.NoError(t, err)
	require.Equal(t, uint64(6), ep6)
	require.Equal(t, seqBeforeOrphan+1, reopened.stat.NextWriteSeq)
}

func TestWriterLease_WritePathExpiryFencing(t *testing.T) {
	ctx := context.Background()
	cur := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)
	var clockMu sync.Mutex
	nowFn := func() time.Time {
		clockMu.Lock()
		defer clockMu.Unlock()
		return cur
	}

	actor, err := Open(Options{
		Dir:      t.TempDir(),
		DiskID:   "write-fence",
		MaxBytes: 64 << 20,
		Now:      nowFn,
	})
	require.NoError(t, err)
	defer func() { _ = actor.Close() }()

	ep, _, err := actor.AcquireWriterLease(ctx, 5*time.Second, false)
	require.NoError(t, err)
	f, err := actor.Create(ctx, CreateReq{ParentIno: RootIno, Name: []byte("f"), Mode: S_IFREG | 0o644, Epoch: ep})
	require.NoError(t, err)

	// 推进时钟使租约过期，验证各类外部写与 Fsync/FsyncDir 均被围栏返回 ErrStaleEpoch
	clockMu.Lock()
	cur = cur.Add(5 * time.Second)
	clockMu.Unlock()

	_, _, err = actor.Write(ctx, f.Ino, 0, []byte("x"), ep)
	require.ErrorIs(t, err, ErrStaleEpoch)
	_, err = actor.Create(ctx, CreateReq{ParentIno: RootIno, Name: []byte("f2"), Mode: S_IFREG | 0o644, Epoch: ep})
	require.ErrorIs(t, err, ErrStaleEpoch)
	_, err = actor.Mkdir(ctx, MkdirReq{ParentIno: RootIno, Name: []byte("d"), Mode: S_IFDIR | 0o755, Epoch: ep})
	require.ErrorIs(t, err, ErrStaleEpoch)
	size := uint64(1)
	_, err = actor.SetAttr(ctx, f.Ino, SetAttrReq{Size: &size, Epoch: ep}, ep)
	require.ErrorIs(t, err, ErrStaleEpoch)
	require.ErrorIs(t, actor.Fallocate(ctx, f.Ino, 0, 4096, 0, ep), ErrStaleEpoch)
	require.ErrorIs(t, actor.SetXattr(ctx, f.Ino, []byte("user.k"), []byte("v"), 0, ep), ErrStaleEpoch)
	require.ErrorIs(t, actor.Fsync(ctx, f.Ino, ep), ErrStaleEpoch)
	require.ErrorIs(t, actor.FsyncDir(ctx, RootIno, ep), ErrStaleEpoch)
	require.ErrorIs(t, actor.PurgeOrphanInode(ctx, f.Ino, ep), ErrStaleEpoch)
}

func TestWriterLease_ResetEpochAndMergeTreesZeroLease(t *testing.T) {
	ctx := context.Background()
	cur := time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC)
	nowFn := func() time.Time { return cur }

	srcDir := t.TempDir()
	src, err := Open(Options{Dir: srcDir, DiskID: "src", MaxBytes: 64 << 20, Now: nowFn})
	require.NoError(t, err)
	ep, _, err := src.AcquireWriterLease(ctx, DefaultWriterLeaseTTL, false)
	require.NoError(t, err)
	require.Positive(t, src.WriterExpiresAt())

	cpDir := filepath.Join(t.TempDir(), "cp")
	_, err = src.Checkpoint(ctx, cpDir)
	require.NoError(t, err)
	require.NoError(t, src.Close())

	childDir := filepath.Join(t.TempDir(), "child")
	require.NoError(t, CloneCheckpointDir(ctx, cpDir, childDir))
	_, err = SanitizeCheckpoint(ctx, SanitizeOptions{Dir: childDir, CommitID: "c1"})
	require.NoError(t, err)

	child, err := Open(Options{Dir: childDir, DiskID: "child", ResetEpoch: true, Now: nowFn})
	require.NoError(t, err)
	defer func() { _ = child.Close() }()
	require.Equal(t, uint64(0), child.WriterEpoch())
	require.Equal(t, int64(0), child.WriterExpiresAt())
	require.False(t, child.HasActiveWriterLease(ctx))

	// 验证未授权的 epoch=1 在 ResetEpoch 后被拦截，需重新获取租约
	_, err = child.Create(ctx, CreateReq{ParentIno: RootIno, Name: []byte("bad"), Mode: S_IFREG | 0o644, Epoch: ep})
	require.ErrorIs(t, err, ErrStaleEpoch)
	require.NoError(t, child.Close())

	// 验证 MergeTrees 校验 ExpectedWriterEpoch == math.MaxUint64 溢出
	maxSrcDir := t.TempDir()
	maxActor, err := Open(Options{Dir: maxSrcDir, DiskID: "max-merge", MaxBytes: 64 << 20})
	require.NoError(t, err)
	require.NoError(t, maxActor.Close())
	maxDB, err := pebble.Open(maxSrcDir, NewPebbleOptions(nil, false))
	require.NoError(t, err)
	v, c, err := maxDB.Get(StatKey())
	require.NoError(t, err)
	maxSt, err := DecodeBranchStat(v)
	require.NoError(t, err)
	require.NoError(t, c.Close())
	maxSt.WriterEpoch = math.MaxUint64
	require.NoError(t, maxDB.Set(StatKey(), EncodeBranchStat(maxSt), pebble.Sync))
	require.NoError(t, maxDB.Close())

	_, err = MergeTrees(ctx, &MergeOptions{
		BaseRoot:            cpDir,
		TargetRoot:          maxSrcDir,
		SourceRoot:          childDir,
		StagingDir:          filepath.Join(t.TempDir(), "staging"),
		ExpectedWriterEpoch: math.MaxUint64,
	})
	require.ErrorIs(t, err, ErrNoSpace)
}

func TestWriterLease_BoundariesOverflowAndZeroClock(t *testing.T) {
	ctx := context.Background()
	var clockMu sync.Mutex
	cur := time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC)
	nowFn := func() time.Time {
		clockMu.Lock()
		defer clockMu.Unlock()
		return cur
	}
	setClock := func(ts time.Time) {
		clockMu.Lock()
		cur = ts
		clockMu.Unlock()
	}

	dir := t.TempDir()
	actor, err := Open(Options{Dir: dir, DiskID: "bounds", MaxBytes: 64 << 20, Now: nowFn})
	require.NoError(t, err)

	// 非正 TTL 拒绝
	_, _, err = actor.AcquireWriterLease(ctx, 0, false)
	require.ErrorIs(t, err, ErrInvalid)
	_, _, err = actor.AcquireWriterLease(ctx, -time.Second, true)
	require.ErrorIs(t, err, ErrInvalid)
	require.ErrorIs(t, actor.RenewWriterLease(ctx, 1, 0), ErrInvalid)
	require.ErrorIs(t, actor.RenewWriterLease(ctx, 1, -time.Millisecond), ErrInvalid)

	// 2262 年 UnixNano 上界溢出保护
	setClock(time.Unix(0, math.MaxInt64-int64(time.Second)).UTC())
	_, _, err = actor.AcquireWriterLease(ctx, DefaultWriterLeaseTTL, false)
	require.ErrorIs(t, err, ErrInvalid)

	// 正常时间获取租约后，验证续约溢出保护
	setClock(time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC))
	ep, _, err := actor.AcquireWriterLease(ctx, DefaultWriterLeaseTTL, false)
	require.NoError(t, err)
	setClock(time.Unix(0, math.MaxInt64-int64(time.Second)).UTC())
	require.ErrorIs(t, actor.RenewWriterLease(ctx, ep, DefaultWriterLeaseTTL), ErrInvalid)

	// 释放租约并切换至零值时间（time.Time{}），验证零值/负时间绝不绕过 WriterExpiresAt <= 0 检查
	require.NoError(t, actor.ReleaseWriterLease(ctx, ep))
	setClock(time.Time{})
	require.False(t, actor.HasActiveWriterLease(ctx))
	_, _, err = actor.Write(ctx, RootIno, 0, []byte("zero-clock"), ep)
	require.ErrorIs(t, err, ErrStaleEpoch)
	require.NoError(t, actor.Close())

	// 直接写入 WriterEpoch = math.MaxUint64，验证 AcquireWriterLease 溢出拒绝
	db, err := pebble.Open(dir, NewPebbleOptions(nil, false))
	require.NoError(t, err)
	val, closer, err := db.Get(StatKey())
	require.NoError(t, err)
	st, err := DecodeBranchStat(val)
	require.NoError(t, err)
	require.NoError(t, closer.Close())
	st.WriterEpoch = math.MaxUint64
	st.WriterExpiresAt = 0
	require.NoError(t, db.Set(StatKey(), EncodeBranchStat(st), pebble.Sync))
	require.NoError(t, db.Close())

	setClock(time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC))
	maxActor, err := Open(Options{Dir: dir, DiskID: "bounds", Now: nowFn})
	require.NoError(t, err)
	defer func() { _ = maxActor.Close() }()
	_, _, err = maxActor.AcquireWriterLease(ctx, DefaultWriterLeaseTTL, true)
	require.ErrorIs(t, err, ErrNoSpace)
}

func TestWriterLease_ForceStaleAndConcurrentSafety(t *testing.T) {
	ctx := context.Background()
	var clockMu sync.Mutex
	cur := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
	nowFn := func() time.Time {
		clockMu.Lock()
		defer clockMu.Unlock()
		return cur
	}

	hookGate := make(chan struct{})
	hookEntered := make(chan struct{})
	var armed atomic.Bool

	actor, err := Open(Options{
		Dir:      t.TempDir(),
		DiskID:   "concurrent-stale",
		MaxBytes: 64 << 20,
		Now:      nowFn,
		beforeTickHook: func() {
			if armed.CompareAndSwap(true, false) {
				close(hookEntered)
				<-hookGate
			}
		},
	})
	require.NoError(t, err)
	defer func() { _ = actor.Close() }()

	// 1. 直接验证 submitStaleEpoch 置位 forceStale 后即使同批次/后续 epoch 推进也不触发 nil deref panic
	f, err := actor.Create(ctx, CreateReq{ParentIno: RootIno, Name: []byte("f"), Mode: S_IFREG | 0o644})
	require.NoError(t, err)

	armed.Store(true)
	staleDone := make(chan error, 1)
	go func() {
		staleDone <- actor.submitStaleEpoch(ctx, opWrite, 1)
	}()
	// 确保 opWrite(epoch=1, req=nil, forceStale=true) 已与 AdvanceEpoch 同批或在其之后执行，不会因 epoch 变为 1 而崩溃
	<-hookEntered
	acqDone := make(chan error, 1)
	go func() {
		_, _, aErr := actor.AcquireWriterLease(ctx, DefaultWriterLeaseTTL, true)
		acqDone <- aErr
	}()
	time.Sleep(10 * time.Millisecond)
	close(hookGate)
	require.ErrorIs(t, <-staleDone, ErrStaleEpoch)
	require.NoError(t, <-acqDone)

	// 2. 并发 RenewWriterLease、Write 与边界时钟抖动压测
	ep := actor.WriterEpoch()
	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() {
			for j := range 20 {
				_, _, wErr := actor.Write(ctx, f.Ino, 0, []byte("concurrent"), ep)
				if wErr != nil {
					require.ErrorIs(t, wErr, ErrStaleEpoch)
				}
				if j%5 == 0 {
					rErr := actor.RenewWriterLease(ctx, ep, DefaultWriterLeaseTTL)
					if rErr != nil {
						require.ErrorIs(t, rErr, ErrStaleEpoch)
					}
				}
			}
		})
	}
	wg.Go(func() {
		clockMu.Lock()
		cur = cur.Add(15 * time.Second)
		clockMu.Unlock()
	})
	wg.Wait()
}

func TestCommitWithSyncBarrier_SyncsDirtyPacksBeforeDeadPackRemovalAndRollback(t *testing.T) {
	ctx := context.Background()
	cur := time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)
	nowFn := func() time.Time { return cur }

	var (
		eventsMu sync.Mutex
		events   []string
	)
	recordEvent := func(ev string) {
		eventsMu.Lock()
		events = append(events, ev)
		eventsMu.Unlock()
	}

	actor, err := Open(Options{
		Dir:         t.TempDir(),
		DiskID:      "barrier-order",
		MaxBytes:    64 << 20,
		MaxPackSize: BlobSize, // 每块独立成包，便于覆写制造死包
		Now:         nowFn,
		syncFileHook: func(uint64) {
			recordEvent("sync")
		},
		removePackHook: func(uint64) {
			recordEvent("remove")
		},
	})
	require.NoError(t, err)
	defer func() { _ = actor.Close() }()

	ep, _, err := actor.AcquireWriterLease(ctx, DefaultWriterLeaseTTL, false)
	require.NoError(t, err)
	f, err := actor.Create(ctx, CreateReq{ParentIno: RootIno, Name: []byte("pack.bin"), Mode: S_IFREG | 0o644, Epoch: ep})
	require.NoError(t, err)

	// 写入 Pack 1 并持久化，再以 NoSync 覆写到 Pack 2 使 Pack 1 进入 pendingDeadPacks
	_, _, err = actor.Write(ctx, f.Ino, 0, bytes.Repeat([]byte{0x11}, BlobSize), ep)
	require.NoError(t, err)
	require.NoError(t, actor.FlushWAL())
	eventsMu.Lock()
	events = nil
	eventsMu.Unlock()

	_, _, err = actor.Write(ctx, f.Ino, 0, bytes.Repeat([]byte{0x22}, BlobSize), ep)
	require.NoError(t, err)
	require.NotEmpty(t, actor.pendingDeadPacks)

	// 触发 RenewWriterLease 同步屏障：必须先 sync 活跃新包再 remove 死包
	cur = cur.Add(time.Second)
	require.NoError(t, actor.RenewWriterLease(ctx, ep, DefaultWriterLeaseTTL))
	actor.blobs.waitPendingReclaims()

	eventsMu.Lock()
	syncIdx := -1
	removeIdx := -1
	for i, ev := range events {
		if ev == "sync" && syncIdx == -1 {
			syncIdx = i
		}
		if ev == "remove" && removeIdx == -1 {
			removeIdx = i
		}
	}
	eventsMu.Unlock()
	require.GreaterOrEqual(t, syncIdx, 0, "同步屏障必须触发新活跃 .pack 刷盘")
	require.Greater(t, removeIdx, syncIdx, "活跃 .pack 刷盘必须严格早于死包删除: events=%v", events)

	// 测试 pack sync 失败时回滚 s.stat、保留 pendingDeadPacks 与 walDirty、绝不触发死包删除
	_, _, err = actor.Write(ctx, f.Ino, 0, bytes.Repeat([]byte{0x33}, BlobSize), ep)
	require.NoError(t, err)
	require.NotEmpty(t, actor.pendingDeadPacks)
	var dirtyPackID uint64
	actor.blobs.mu.Lock()
	for id := range actor.blobs.dirtyPacks {
		dirtyPackID = id
		break
	}
	actor.blobs.mu.Unlock()
	require.NotZero(t, dirtyPackID)

	actor.blobs.filesMu.Lock()
	if pf := actor.blobs.packFiles[dirtyPackID]; pf != nil {
		_ = pf.Close()
	}
	actor.blobs.filesMu.Unlock()

	eventsMu.Lock()
	events = nil
	eventsMu.Unlock()
	prevExpiresAt := actor.stat.WriterExpiresAt
	cur = cur.Add(5 * time.Second)
	err = actor.RenewWriterLease(ctx, ep, DefaultWriterLeaseTTL)
	require.Error(t, err)
	require.Equal(t, prevExpiresAt, actor.stat.WriterExpiresAt, "提交失败后内存 s.stat 必须回滚")
	require.Equal(t, prevExpiresAt, actor.WriterExpiresAt(), "提交失败后原子快照不得推进")
	require.NotEmpty(t, actor.pendingDeadPacks, "提交失败后必须恢复 pendingDeadPacks")

	eventsMu.Lock()
	for _, ev := range events {
		require.NotEqual(t, "remove", ev, "同步失败绝不能删除待回收死包")
	}
	eventsMu.Unlock()

	// 恢复 activePack 文件描述符以确保 Close 正常清理
	actor.blobs.filesMu.Lock()
	if _, ok := actor.blobs.packFiles[dirtyPackID]; ok {
		if reopenedFD, oErr := os.OpenFile(packFilePath(actor.blobs.dir, dirtyPackID), os.O_RDWR, 0o644); oErr == nil {
			actor.blobs.packFiles[dirtyPackID] = reopenedFD
		}
	}
	actor.blobs.filesMu.Unlock()
}
