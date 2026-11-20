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
	return uint64(st.Nlink)
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

	actor := openTestActor(t, &store.Options{Dir: dir, DiskID: "epoch-disk", MaxBytes: 16 << 20})
	require.Equalf(t, uint64(0), actor.WriterEpoch(), "初始 WriterEpoch 应为 0")

	f, err := actor.Create(ctx, store.CreateReq{ParentIno: store.RootIno, Name: []byte("ep.bin"), Mode: store.S_IFREG | 0o644})
	require.NoError(t, err)

	_, _, err = actor.Write(ctx, f.Ino, 0, []byte("e0"))
	require.NoError(t, err)
	_, _, err = actor.Write(ctx, f.Ino, 2, []byte("e0-exp"), 0)
	require.NoError(t, err)
	_, _, err = actor.Write(ctx, f.Ino, 2, []byte("e1"), 1)
	require.ErrorIsf(t, err, store.ErrStaleEpoch, "未来 epoch=1 写入应返回 ErrStaleEpoch: got=%v", err)

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

	_, _, err = actor.Write(ctx, f.Ino, 2, []byte("-ok"), 1)
	require.NoError(t, err)

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

	src := openTestActor(t, &store.Options{Dir: srcDir, DiskID: "src-branch", MaxBytes: 64 << 20, MaxPackSize: store.BlobSize})
	f, err := src.Create(ctx, store.CreateReq{ParentIno: store.RootIno, Name: []byte("base.bin"), Mode: store.S_IFREG | 0o644})
	require.NoError(t, err)
	basePayload := makePatternBytes(36*store.BlobSize, 0x5A)
	_, _, err = src.Write(ctx, f.Ino, 0, basePayload)
	require.NoError(t, err)
	epochBefore := src.WriterEpoch()

	cpStats, err := src.Checkpoint(ctx, cpDir)
	require.NoError(t, err)

	require.GreaterOrEqualf(t, cpStats.LivePacks, 35, "Checkpoint 应包含至少 35 个活跃 pack: got=%d", cpStats.LivePacks)
	require.Equalf(t, uint64(36*store.BlobSize), cpStats.LogicalBytes, "Checkpoint LogicalBytes 不符")
	require.Falsef(t, cpStats.HasOrphans, "无孤儿时 HasOrphans 应为 false")
	require.Equalf(t, epochBefore, src.WriterEpoch(), "Checkpoint 不得扰动源实例 WriterEpoch")
	requireMinPackCount(t, cpDir, 35)
	requireAllPackNlink(t, cpDir, 2)

	fNew, err := src.Create(ctx, store.CreateReq{ParentIno: store.RootIno, Name: []byte("src-only.bin"), Mode: store.S_IFREG | 0o644})
	require.NoError(t, err)
	_, _, err = src.Write(ctx, fNew.Ino, 0, makePatternBytes(store.BlobSize, 0x6B))
	require.NoError(t, err)

	require.NoError(t, store.CloneCheckpointDir(ctx, cpDir, childDir))

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

	require.NoError(t, store.CloneCheckpointDir(ctx, cpDir, sanDir))
	sanStatFS, err := store.SanitizeCheckpoint(ctx, store.SanitizeOptions{Dir: sanDir, CommitID: "commit-san-1"})
	require.NoError(t, err)

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

	srcInos := createAndWriteManyFiles(t, src, "s", 70, store.BlobSize)
	childInos := createAndWriteManyFiles(t, child, "c", 70, store.BlobSize)

	requireNoDuplicateUint64s(t, srcInos, childInos)
	verifyFilesPattern(t, src, srcInos, store.BlobSize)
	verifyFilesPattern(t, child, childInos, store.BlobSize)
}

func TestGivenConcurrentWritesAndCompactPacksWhenCheckpointCalledThenSnapshotConsistent(t *testing.T) {
	ctx := context.Background()
	srcDir := t.TempDir()
	cpDir := filepath.Join(t.TempDir(), "cp-concurrent")

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

	stats, cpErr := actor.Checkpoint(ctx, cpDir)
	stop.Store(true)
	wg.Wait()

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

	require.ErrorIs(t, store.CloneCheckpointDir(ctx, "", filepath.Join(t.TempDir(), "d")), store.ErrInvalid)
	require.ErrorIs(t, store.CloneCheckpointDir(ctx, cpDir, cpDir), store.ErrInvalid)
	require.ErrorIs(t, store.CloneCheckpointDir(ctx, cpDir, existingDir), store.ErrExist)

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

func TestGivenExistingRWDiskWhenReopenedWithUpdatedMaxBytesThenAlignsAndPersistsQuota(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()

	initial, err := store.Open(store.Options{Dir: dir, DiskID: "align-max-bytes", MaxBytes: 4 << 20})
	require.NoError(t, err)
	epoch, _, err := initial.AdvanceEpoch(ctx)
	require.NoError(t, err)
	require.Equal(t, uint64(1), epoch)
	f, err := initial.Create(ctx, store.CreateReq{ParentIno: store.RootIno, Name: []byte("f.bin"), Mode: store.S_IFREG | 0o644, Epoch: epoch})
	require.NoError(t, err)
	_, _, err = initial.Write(ctx, f.Ino, 0, make([]byte, 4096), epoch)
	require.NoError(t, err)
	require.NoError(t, initial.Close())

	reopened, err := store.Open(store.Options{Dir: dir, DiskID: "align-max-bytes", MaxBytes: 12 << 20})
	require.NoError(t, err)
	require.Equal(t, uint64(12<<20), reopened.MaxBytes(), "reopen must align MaxBytes to opts.MaxBytes")
	st, err := reopened.StatFS(ctx)
	require.NoError(t, err)
	require.Equal(t, uint64(12<<20), st.TotalBytes, "StatFS TotalBytes must reflect aligned MaxBytes")
	require.Equal(t, uint64(1), reopened.WriterEpoch(), "reopen without ResetEpoch must preserve WriterEpoch")
	require.NoError(t, reopened.Close())

	persisted, err := store.Open(store.Options{Dir: dir, DiskID: "align-max-bytes"})
	require.NoError(t, err)
	t.Cleanup(func() { _ = persisted.Close() })
	require.Equal(t, uint64(12<<20), persisted.MaxBytes(), "aligned MaxBytes must persist in Pebble StatKey")
	require.NoError(t, persisted.Close())

	ro, err := store.Open(store.Options{Dir: dir, DiskID: "align-max-bytes", ReadOnly: true, MaxBytes: 64 << 20})
	require.NoError(t, err)
	t.Cleanup(func() { _ = ro.Close() })
	require.Equal(t, uint64(12<<20), ro.MaxBytes(), "read-only open must not overwrite persisted MaxBytes")
}
