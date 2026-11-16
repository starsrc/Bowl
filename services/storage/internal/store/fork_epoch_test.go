package store_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/starsrc/bowl/services/storage/internal/store"
)

type sharedSegmentPool struct {
	mu      sync.Mutex
	nextIno uint64
	nextPck uint64
}

func newSharedSegmentPool(startIno, startPack uint64) *sharedSegmentPool {
	return &sharedSegmentPool{nextIno: startIno, nextPck: startPack}
}

func (p *sharedSegmentPool) allocIno(_ context.Context, minNext uint64) (first, last uint64, err error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.nextIno < minNext {
		p.nextIno = minNext
	}
	first = p.nextIno
	last = first + 31
	p.nextIno = last + 1
	return first, last, nil
}

func (p *sharedSegmentPool) allocPack(_ context.Context, minNext uint64) (first, last uint64, err error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.nextPck < minNext {
		p.nextPck = minNext
	}
	first = p.nextPck
	last = first + store.PackSegment - 1
	p.nextPck = last + 1
	return first, last, nil
}

func fileNlink(t *testing.T, path string) uint64 {
	t.Helper()
	info, err := os.Stat(path)
	require.NoErrorf(t, err, "获取文件 Stat 失败: path=%s", path)
	st, ok := info.Sys().(*syscall.Stat_t)
	require.Truef(t, ok, "Stat_t 类型转换失败: path=%s", path)
	return uint64(st.Nlink) //nolint:unconvert,nolintlint // Nlink 在 linux/amd64 是 uint64，在 darwin 与 linux/arm64 更窄
}

func requireAllPackNlink(t *testing.T, dir string, wantNlink uint64) {
	t.Helper()
	packs := findPackFiles(t, dir)
	require.NotEmptyf(t, packs, "目录缺少 pack 文件: dir=%s", dir)
	for _, p := range packs {
		got := fileNlink(t, p)
		require.Equalf(t, wantNlink, got, "pack 硬链接数不符: path=%s want=%d got=%d", p, wantNlink, got)
	}
}

func requireMinPackCount(t *testing.T, dir string, minCount int) {
	t.Helper()
	packs := findPackFiles(t, dir)
	require.GreaterOrEqualf(t, len(packs), minCount, "pack 文件数不足: dir=%s want>=%d got=%d", dir, minCount, len(packs))
}

func findFirstFileWithPrefix(t *testing.T, dir, prefix string) string {
	t.Helper()
	entries, err := os.ReadDir(dir)
	require.NoErrorf(t, err, "读取目录失败: dir=%s", dir)
	for _, e := range entries {
		if !e.IsDir() && strings.HasPrefix(e.Name(), prefix) {
			return filepath.Join(dir, e.Name())
		}
	}
	t.Fatalf("目录中未找到前缀为 %q 的文件: dir=%s", prefix, dir)
	return ""
}

func createAndWriteManyFiles(t *testing.T, actor *store.StoreActor, prefix string, count, payloadSize int) []uint64 {
	t.Helper()
	ctx := context.Background()
	inos := make([]uint64, 0, count)
	for i := range count {
		name := []byte(fmt.Sprintf("%s-%03d.bin", prefix, i))
		f, err := actor.Create(ctx, store.CreateReq{ParentIno: store.RootIno, Name: name, Mode: store.S_IFREG | 0o644})
		require.NoErrorf(t, err, "创建文件失败: name=%s", string(name))
		_, _, err = actor.Write(ctx, f.Ino, 0, makePatternBytes(payloadSize, byte(i)))
		require.NoErrorf(t, err, "写入文件失败: name=%s", string(name))
		inos = append(inos, f.Ino)
	}
	return inos
}

func requireNoDuplicateUint64s(t *testing.T, a, b []uint64) {
	t.Helper()
	seen := make(map[uint64]struct{}, len(a))
	for _, v := range a {
		if _, dup := seen[v]; dup {
			t.Fatalf("分支 A 出现重复 inode: %d", v)
		}
		seen[v] = struct{}{}
	}
	for _, v := range b {
		if _, dup := seen[v]; dup {
			t.Fatalf("跨分支出现重复 inode: %d", v)
		}
		seen[v] = struct{}{}
	}
}

func verifyFilesPattern(t *testing.T, actor *store.StoreActor, inos []uint64, payloadSize int) {
	t.Helper()
	ctx := context.Background()
	for i, ino := range inos {
		got, err := actor.Read(ctx, ino, 0, uint32(payloadSize))
		require.NoErrorf(t, err, "回读跨分支文件失败: idx=%d ino=%d", i, ino)
		require.Equalf(t, makePatternBytes(payloadSize, byte(i)), got, "跨分支文件内容不符: idx=%d ino=%d", i, ino)
	}
}

func waitUntilAtLeast(done *atomic.Bool, counter *atomic.Int64, target int64) {
	for !done.Load() && counter.Load() < target {
		runtime.Gosched()
	}
}

func runConcurrentWritersForCheckpoint(ctx context.Context, actor *store.StoreActor, stop *atomic.Bool, writeOps *atomic.Int64, firstErr *atomic.Pointer[error]) {
	for i := 0; !stop.Load(); i++ {
		name := []byte(fmt.Sprintf("cw-%04d.bin", i))
		f, err := actor.Create(ctx, store.CreateReq{ParentIno: store.RootIno, Name: name, Mode: store.S_IFREG | 0o644})
		if err != nil {
			e := err
			firstErr.CompareAndSwap(nil, &e)
			return
		}
		if _, _, err = actor.Write(ctx, f.Ino, 0, makePatternBytes(64*1024, byte(i))); err != nil {
			e := err
			firstErr.CompareAndSwap(nil, &e)
			return
		}
		if i%3 == 0 {
			_ = actor.Unlink(ctx, store.RootIno, name, false)
		}
		writeOps.Add(1)
	}
}

func TestGivenWriterEpochWhenAdvancedOrResetThenStaleAndFutureEpochsFenced(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()

	// Given: 初始实例 WriterEpoch 必须为 0
	actor := openTestActor(t, &store.Options{Dir: dir, DiskID: "epoch-disk", MaxBytes: 16 << 20})
	require.Equalf(t, uint64(0), actor.WriterEpoch(), "初始 WriterEpoch 应为 0")

	f, err := actor.Create(ctx, store.CreateReq{ParentIno: store.RootIno, Name: []byte("ep.bin"), Mode: store.S_IFREG | 0o644})
	require.NoError(t, err)

	// When: 以当前 epoch=0（含省略参数）写入成功，传入未来 epoch=1 遭隔离
	_, _, err = actor.Write(ctx, f.Ino, 0, []byte("e0"))
	require.NoError(t, err)
	_, _, err = actor.Write(ctx, f.Ino, 2, []byte("e0-exp"), 0)
	require.NoError(t, err)
	_, _, err = actor.Write(ctx, f.Ino, 2, []byte("e1"), 1)
	require.ErrorIsf(t, err, store.ErrStaleEpoch, "未来 epoch=1 写入应返回 ErrStaleEpoch: got=%v", err)

	// When: AdvanceEpoch 推进至 1，旧 epoch=0 与省略参数（等价 epoch=0）全部返回 ErrStaleEpoch
	newEpoch, _, err := actor.AdvanceEpoch(ctx)
	require.NoError(t, err)
	require.Equalf(t, uint64(1), newEpoch, "首次 AdvanceEpoch 后应为 1")
	require.Equalf(t, uint64(1), actor.WriterEpoch(), "WriterEpoch() 应更新为 1")

	_, _, err = actor.Write(ctx, f.Ino, 2, []byte("stale-implicit"))
	require.ErrorIs(t, err, store.ErrStaleEpoch)
	_, _, err = actor.Write(ctx, f.Ino, 2, []byte("stale"), 0)
	require.ErrorIs(t, err, store.ErrStaleEpoch)
	require.ErrorIs(t, actor.Fallocate(ctx, f.Ino, 0, 4096, 0, 0), store.ErrStaleEpoch)
	sizeOne := uint64(1)
	_, err = actor.SetAttr(ctx, f.Ino, store.SetAttrReq{Size: &sizeOne}, 0)
	require.ErrorIs(t, err, store.ErrStaleEpoch)
	require.ErrorIs(t, actor.Fsync(ctx, f.Ino, 0), store.ErrStaleEpoch)
	require.ErrorIs(t, actor.FsyncDir(ctx, store.RootIno, 0), store.ErrStaleEpoch)

	// 新 epoch=1 可正常写入
	_, _, err = actor.Write(ctx, f.Ino, 2, []byte("-ok"), 1)
	require.NoError(t, err)

	// When: 关闭后用 ResetEpoch:true 且过小 MaxBytes 重开返回 ErrQuotaShrinkBelowUsed；合法 ResetEpoch 重开可将 epoch 复位为 0
	_, _, err = actor.Write(ctx, f.Ino, 0, makePatternBytes(2*store.BlobSize, 0x31), 1)
	require.NoError(t, err)
	require.NoError(t, actor.Close())

	_, err = store.Open(store.Options{Dir: dir, DiskID: "epoch-disk", MaxBytes: store.BlobSize, ResetEpoch: true})
	require.ErrorIsf(t, err, store.ErrQuotaShrinkBelowUsed, "ResetEpoch 缩容低于已用容量的 MaxBytes 应返回 ErrQuotaShrinkBelowUsed: got=%v", err)

	resetActor := openTestActor(t, &store.Options{Dir: dir, DiskID: "epoch-disk", MaxBytes: 16 << 20, ResetEpoch: true})
	require.Equalf(t, uint64(0), resetActor.WriterEpoch(), "ResetEpoch:true 重开应将 WriterEpoch 复位为 0")
}

func TestGiven36PacksWhenCheckpointAndCloneThenParallelHardlinksAndBranchIsolationHold(t *testing.T) {
	ctx := context.Background()
	srcDir := t.TempDir()
	cpDir := filepath.Join(t.TempDir(), "cp")
	childDir := filepath.Join(t.TempDir(), "child")

	// Given: 每个 pack 上限 1 块，写入 36 块触发 >= 35 个 pack 文件（覆盖并发硬链接阈值 32）
	src := openTestActor(t, &store.Options{Dir: srcDir, DiskID: "src-branch", MaxBytes: 64 << 20, MaxPackSize: store.BlobSize})
	f, err := src.Create(ctx, store.CreateReq{ParentIno: store.RootIno, Name: []byte("base.bin"), Mode: store.S_IFREG | 0o644})
	require.NoError(t, err)
	basePayload := makePatternBytes(36*store.BlobSize, 0x5A)
	_, _, err = src.Write(ctx, f.Ino, 0, basePayload)
	require.NoError(t, err)
	epochBefore := src.WriterEpoch()

	// When: 执行 Checkpoint 到 cpDir
	cpStats, err := src.Checkpoint(ctx, cpDir)
	require.NoError(t, err)

	// Then: 统计吻合，源 WriterEpoch 未变，cpDir 内全部 .pack 硬链接数 == 2
	require.GreaterOrEqualf(t, cpStats.LivePacks, 35, "Checkpoint 应包含至少 35 个活跃 pack: got=%d", cpStats.LivePacks)
	require.Equalf(t, uint64(36*store.BlobSize), cpStats.LogicalBytes, "Checkpoint LogicalBytes 不符")
	require.Falsef(t, cpStats.HasOrphans, "无孤儿时 HasOrphans 应为 false")
	require.Equalf(t, epochBefore, src.WriterEpoch(), "Checkpoint 不得扰动源实例 WriterEpoch")
	requireMinPackCount(t, cpDir, 35)
	requireAllPackNlink(t, cpDir, 2)

	// When: 在源实例继续写入后应开启新 pack，同时通过 CloneCheckpointDir 派生子分支
	fNew, err := src.Create(ctx, store.CreateReq{ParentIno: store.RootIno, Name: []byte("src-only.bin"), Mode: store.S_IFREG | 0o644})
	require.NoError(t, err)
	_, _, err = src.Write(ctx, fNew.Ino, 0, makePatternBytes(store.BlobSize, 0x6B))
	require.NoError(t, err)

	require.NoError(t, store.CloneCheckpointDir(ctx, cpDir, childDir))

	// Then: Clone 跳过 LOCK 与 pebble_events.log；元数据文件独立拷贝（nlink==1）；只读快照与子分支完全隔离
	_, err = os.Stat(filepath.Join(childDir, "LOCK"))
	require.ErrorIsf(t, err, os.ErrNotExist, "CloneCheckpointDir 不得复制 Pebble LOCK")
	_, err = os.Stat(filepath.Join(childDir, store.PebbleEventsLogFileName))
	require.ErrorIsf(t, err, os.ErrNotExist, "CloneCheckpointDir 不得复制 pebble_events.log")
	manifestPath := findFirstFileWithPrefix(t, childDir, "MANIFEST-")
	require.Equalf(t, uint64(1), fileNlink(t, manifestPath), "MANIFEST 元数据文件应为独立副本 (nlink=1)")

	roCommit := openTestActor(t, &store.Options{Dir: cpDir, DiskID: "cp-ro", ReadOnly: true})
	child := openTestActor(t, &store.Options{Dir: childDir, DiskID: "child-branch", MaxBytes: 64 << 20, MaxPackSize: store.BlobSize})

	childFile, err := child.Create(ctx, store.CreateReq{ParentIno: store.RootIno, Name: []byte("child-only.bin"), Mode: store.S_IFREG | 0o644})
	require.NoError(t, err)
	_, _, err = child.Write(ctx, childFile.Ino, 0, []byte("child-data"))
	require.NoError(t, err)

	_, _, err = roCommit.Lookup(ctx, store.RootIno, []byte("src-only.bin"))
	require.ErrorIs(t, err, store.ErrNotFound)
	_, _, err = roCommit.Lookup(ctx, store.RootIno, []byte("child-only.bin"))
	require.ErrorIs(t, err, store.ErrNotFound)
	_, _, err = src.Lookup(ctx, store.RootIno, []byte("child-only.bin"))
	require.ErrorIs(t, err, store.ErrNotFound)

	gotCommitBase, err := roCommit.Read(ctx, f.Ino, 0, 36*store.BlobSize)
	require.NoError(t, err)
	require.Equal(t, basePayload, gotCommitBase)
}

func TestGivenOrphanInCheckpointWhenSanitizedThenOrphanPacksDroppedWithoutAffectingSource(t *testing.T) {
	ctx := context.Background()
	srcDir := t.TempDir()
	cpDir := filepath.Join(t.TempDir(), "cp-orphan")
	sanDir := filepath.Join(t.TempDir(), "sanitized")

	// Given: 源实例包含 1 个正常文件（pack1）与 1 个 openHandle=true 孤儿文件（pack2）
	src := openTestActor(t, &store.Options{Dir: srcDir, DiskID: "src-orphan", MaxBytes: 16 << 20, MaxPackSize: store.BlobSize})
	liveFile, err := src.Create(ctx, store.CreateReq{ParentIno: store.RootIno, Name: []byte("live.bin"), Mode: store.S_IFREG | 0o644})
	require.NoError(t, err)
	livePayload := makePatternBytes(store.BlobSize, 0x12)
	_, _, err = src.Write(ctx, liveFile.Ino, 0, livePayload)
	require.NoError(t, err)

	orphanFile, err := src.Create(ctx, store.CreateReq{ParentIno: store.RootIno, Name: []byte("orph.bin"), Mode: store.S_IFREG | 0o644})
	require.NoError(t, err)
	orphanPayload := makePatternBytes(store.BlobSize, 0x24)
	_, _, err = src.Write(ctx, orphanFile.Ino, 0, orphanPayload)
	require.NoError(t, err)
	require.NoError(t, src.Unlink(ctx, store.RootIno, []byte("orph.bin"), true))

	stats, err := src.Checkpoint(ctx, cpDir)
	require.NoError(t, err)
	require.Truef(t, stats.HasOrphans, "带未关闭孤儿的 Checkpoint 应标记 HasOrphans=true")

	// When: 将 Checkpoint 克隆到 sanDir 后调用 SanitizeCheckpoint 净化
	require.NoError(t, store.CloneCheckpointDir(ctx, cpDir, sanDir))
	sanStatFS, err := store.SanitizeCheckpoint(ctx, store.SanitizeOptions{Dir: sanDir, CommitID: "commit-san-1"})
	require.NoError(t, err)

	// Then: 净化后已用空间仅含活文件；孤儿包被物理移除（源侧 pack2 硬链接数比 pack1 更低）；源孤儿句柄依然可读
	require.Equalf(t, uint64(store.BlobSize), sanStatFS.UsedBytes, "净化后 StatFS UsedBytes 应只剩活文件")
	require.Equalf(t, uint64(3), fileNlink(t, filepath.Join(srcDir, "packs", "0000000000000001.pack")), "活包 pack1 应被源/cp/sanDir 三处共享")
	require.Equalf(t, uint64(2), fileNlink(t, filepath.Join(srcDir, "packs", "0000000000000002.pack")), "孤儿包 pack2 在 sanDir 被删除后应仅剩源/cp 两处共享")

	sanActor := openTestActor(t, &store.Options{Dir: sanDir, DiskID: "san-ro", ReadOnly: true})
	sanHasOrphans, err := sanActor.HasOrphans()
	require.NoError(t, err)
	require.Falsef(t, sanHasOrphans, "SanitizeCheckpoint 后 HasOrphans 应为 false")

	gotOrphanOnSrc, err := src.Read(ctx, orphanFile.Ino, 0, store.BlobSize)
	require.NoError(t, err)
	require.Equal(t, orphanPayload, gotOrphanOnSrc)
}

func TestGivenCrossBranchSegmentAllocatorsWhen70FilesPerBranchThenInodesAndPacksCollisionFree(t *testing.T) {
	ctx := context.Background()
	pool := newSharedSegmentPool(100, 100)
	baseDir := t.TempDir()
	cpDir := filepath.Join(t.TempDir(), "cp-alloc")
	childDir := filepath.Join(t.TempDir(), "child-alloc")

	// Given: 源实例注入外部号段分配器并 Checkpoint 派生子分支
	src := openTestActor(t, &store.Options{
		Dir:           baseDir,
		DiskID:        "alloc-src",
		MaxBytes:      64 << 20,
		MaxPackSize:   store.BlobSize,
		InoAllocator:  pool.allocIno,
		PackAllocator: pool.allocPack,
	})
	seed, err := src.Create(ctx, store.CreateReq{ParentIno: store.RootIno, Name: []byte("seed.bin"), Mode: store.S_IFREG | 0o644})
	require.NoError(t, err)
	_, _, err = src.Write(ctx, seed.Ino, 0, makePatternBytes(store.BlobSize, 0x01))
	require.NoError(t, err)
	_, err = src.Checkpoint(ctx, cpDir)
	require.NoError(t, err)
	require.NoError(t, store.CloneCheckpointDir(ctx, cpDir, childDir))

	child := openTestActor(t, &store.Options{
		Dir:           childDir,
		DiskID:        "alloc-child",
		MaxBytes:      64 << 20,
		MaxPackSize:   store.BlobSize,
		InoAllocator:  pool.allocIno,
		PackAllocator: pool.allocPack,
	})

	// When: 源分支与子分支各新建并写入 70 个整块文件（超过单段 PackSegment=64 与单段 InoSegment=32）
	srcInos := createAndWriteManyFiles(t, src, "s", 70, store.BlobSize)
	childInos := createAndWriteManyFiles(t, child, "c", 70, store.BlobSize)

	// Then: 两分支分配到的 Inode 全集无碰撞，且 140 个文件全部可完整校验读回
	requireNoDuplicateUint64s(t, srcInos, childInos)
	verifyFilesPattern(t, src, srcInos, store.BlobSize)
	verifyFilesPattern(t, child, childInos, store.BlobSize)
}

func TestGivenConcurrentWritesAndCompactPacksWhenCheckpointCalledThenSnapshotConsistent(t *testing.T) {
	ctx := context.Background()
	srcDir := t.TempDir()
	cpDir := filepath.Join(t.TempDir(), "cp-concurrent")

	// Given: 并发持续写入和删除与前台 CompactPacks 交织运行
	actor := openTestActor(t, &store.Options{Dir: srcDir, DiskID: "cp-race", MaxBytes: 64 << 20, MaxPackSize: 2 * store.BlobSize})
	var stop atomic.Bool
	var writeOps atomic.Int64
	var writeErr atomic.Pointer[error]
	var wg sync.WaitGroup
	wg.Go(func() {
		runConcurrentWritersForCheckpoint(ctx, actor, &stop, &writeOps, &writeErr)
	})

	waitUntilAtLeast(&stop, &writeOps, 8)
	require.NoError(t, actor.CompactPacks(ctx))

	// When: 并发压测期间执行在线 Checkpoint
	stats, cpErr := actor.Checkpoint(ctx, cpDir)
	stop.Store(true)
	wg.Wait()

	// Then: 后台写入无错，Checkpoint 成功且只读重开无损坏
	require.Nilf(t, writeErr.Load(), "并发写入出现错误: %v", writeErr.Load())
	require.NoError(t, cpErr)
	roCP := openTestActor(t, &store.Options{Dir: cpDir, DiskID: "cp-race-ro", ReadOnly: true})
	st, err := roCP.StatFS(ctx)
	require.NoError(t, err)
	require.Equalf(t, stats.LogicalBytes, st.UsedBytes, "只读快照 StatFS UsedBytes 与 CheckpointStats 不一致")
}

func TestGivenInvalidForkInputsAndFailingAllocatorsWhenInvokedThenReturnsErrors(t *testing.T) {
	ctx := context.Background()
	existingDir := t.TempDir()
	actor := openTestActor(t, &store.Options{DiskID: "fork-err", MaxBytes: 16 << 20})

	// Given & When & Then: Checkpoint 目标为空、已存在或对只读实例调用均返回预期错误
	_, err := actor.Checkpoint(ctx, "")
	require.ErrorIsf(t, err, store.ErrInvalid, "Checkpoint 空路径应返回 ErrInvalid")
	_, err = actor.Checkpoint(ctx, existingDir)
	require.ErrorIsf(t, err, store.ErrExist, "Checkpoint 已存在目录应返回 ErrExist")

	cpDir := filepath.Join(t.TempDir(), "ro-cp")
	_, err = actor.Checkpoint(ctx, cpDir)
	require.NoError(t, err)
	ro := openTestActor(t, &store.Options{Dir: cpDir, DiskID: "ro-cp", ReadOnly: true})
	_, err = ro.Checkpoint(ctx, filepath.Join(t.TempDir(), "cp2"))
	require.ErrorIsf(t, err, store.ErrReadOnly, "只读实例调用 Checkpoint 应返回 ErrReadOnly")

	// When & Then: CloneCheckpointDir 非法参数返回对应错误
	require.ErrorIs(t, store.CloneCheckpointDir(ctx, "", filepath.Join(t.TempDir(), "d")), store.ErrInvalid)
	require.ErrorIs(t, store.CloneCheckpointDir(ctx, cpDir, cpDir), store.ErrInvalid)
	require.ErrorIs(t, store.CloneCheckpointDir(ctx, cpDir, existingDir), store.ErrExist)

	// When & Then: 外部号段分配器返回错误时透传失败，不产生脏状态
	boom := errors.New("allocator down")
	badInoActor := openTestActor(t, &store.Options{
		DiskID: "bad-ino-alloc",
		InoAllocator: func(context.Context, uint64) (uint64, uint64, error) {
			return 0, 0, boom
		},
	})
	_, err = badInoActor.Create(ctx, store.CreateReq{ParentIno: store.RootIno, Name: []byte("x"), Mode: store.S_IFREG | 0o644})
	require.ErrorIsf(t, err, boom, "InoAllocator 失败应透传错误")

	badPackActor := openTestActor(t, &store.Options{
		DiskID: "bad-pack-alloc",
		PackAllocator: func(context.Context, uint64) (uint64, uint64, error) {
			return 0, 0, boom
		},
	})
	f, err := badPackActor.Create(ctx, store.CreateReq{ParentIno: store.RootIno, Name: []byte("y"), Mode: store.S_IFREG | 0o644})
	require.NoError(t, err)
	_, _, err = badPackActor.Write(ctx, f.Ino, 0, []byte("hello"))
	require.ErrorIsf(t, err, boom, "PackAllocator 失败应透传错误")
}
