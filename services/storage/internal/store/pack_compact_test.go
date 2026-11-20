package store_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/starsrc/bowl/services/storage/internal/store"
)

func requirePackFileCount(t *testing.T, dir string, want int) []string {
	t.Helper()
	files := findPackFiles(t, dir)
	require.Lenf(t, files, want, "packs 目录 .pack 文件数量不符: want=%d got=%d files=%v", want, len(files), files)
	return files
}

func TestGivenMaxPackSizeOptionsWhenOpenedThenBoundsEnforced(t *testing.T) {

	_, err := store.Open(store.Options{Dir: t.TempDir(), DiskID: "min-minus-1", MaxBytes: 64 << 20, MaxPackSize: store.BlobSize - 1})
	require.ErrorIsf(t, err, store.ErrInvalid, "MaxPackSize 小于 BlobSize 应返回 ErrInvalid: got=%v", err)

	_, err = store.Open(store.Options{Dir: t.TempDir(), DiskID: "max-plus-1", MaxBytes: 64 << 20, MaxPackSize: store.PackMaxSize + 1})
	require.ErrorIsf(t, err, store.ErrInvalid, "MaxPackSize 大于 PackMaxSize 应返回 ErrInvalid: got=%v", err)

	minActor := openTestActor(t, &store.Options{DiskID: "min-valid", MaxPackSize: store.BlobSize})
	require.NoError(t, minActor.Close())

	maxActor := openTestActor(t, &store.Options{DiskID: "max-valid", MaxPackSize: store.PackMaxSize})
	require.NoError(t, maxActor.Close())
}

func TestGivenPreexistingHighPackIDWhenOpenedThenNextPackAllocatesHigherID(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()

	initActor := openTestActor(t, &store.Options{Dir: dir, DiskID: "pack-id-recovery", MaxPackSize: 2 * store.BlobSize})
	require.NoError(t, initActor.Close())

	require.NoError(t, os.MkdirAll(filepath.Join(dir, "packs"), 0o755))
	preexisting := filepath.Join(dir, "packs", "0000000000000250.pack")
	require.NoError(t, os.WriteFile(preexisting, make([]byte, store.BlobSize), 0o600))

	actor := openTestActor(t, &store.Options{Dir: dir, DiskID: "pack-id-recovery", MaxPackSize: 2 * store.BlobSize})
	f, err := actor.Create(ctx, store.CreateReq{ParentIno: store.RootIno, Name: []byte("f.bin"), Mode: store.S_IFREG | 0o644})
	require.NoError(t, err)
	_, _, err = actor.Write(ctx, f.Ino, 0, makePatternBytes(store.BlobSize, 0x19))
	require.NoError(t, err)

	_, err = os.Stat(filepath.Join(dir, "packs", "0000000000000251.pack"))
	require.NoErrorf(t, err, "恢复后新建 pack 应从 251 起步: err=%v", err)
}

func TestGivenWritesRollingAcrossThreePacksWhenClosedAndReopenedThenSyncTargetsAndColdReadMatch(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()

	actor, reg := newMeteredActor(t, &store.Options{Dir: dir, DiskID: "rollover-3", MaxPackSize: 2 * store.BlobSize})
	f, err := actor.Create(ctx, store.CreateReq{ParentIno: store.RootIno, Name: []byte("roll.bin"), Mode: store.S_IFREG | 0o644})
	require.NoError(t, err)
	payload := makePatternBytes(5*store.BlobSize, 0x33)
	_, _, err = actor.Write(ctx, f.Ino, 0, payload)
	require.NoError(t, err)

	require.NoError(t, actor.Close())

	requirePackFileCount(t, dir, 3)
	require.GreaterOrEqualf(t, histogramSampleCount(t, reg, "bowl_storage_blob_sync_duration_seconds", map[string]string{"target": "pack_data", "trigger": "bg_rotate"}), uint64(2), "2 个已封口 pack 应在 Close 排空前完成 bg_rotate fdatasync")

	reopened, reg2 := newMeteredActor(t, &store.Options{Dir: dir, DiskID: "rollover-3", MaxPackSize: 2 * store.BlobSize})
	got, err := reopened.Read(ctx, f.Ino, 0, 5*store.BlobSize)
	require.NoError(t, err)
	require.Equalf(t, payload, got, "跨 3 个 pack 冷重开回读数据不符")

	fBarrier, err := reopened.Create(ctx, store.CreateReq{ParentIno: store.RootIno, Name: []byte("barrier.bin"), Mode: store.S_IFREG | 0o644})
	require.NoError(t, err)
	_, _, err = reopened.Write(ctx, fBarrier.Ino, 0, makePatternBytes(store.BlobSize, 0x34))
	require.NoError(t, err)
	require.NoError(t, reopened.Fsync(ctx, fBarrier.Ino))
	require.GreaterOrEqualf(t, histogramSampleCount(t, reg2, "bowl_storage_blob_sync_duration_seconds", map[string]string{"target": "packs_dir", "trigger": "fsync_barrier"}), uint64(1), "新活跃 pack Fsync 应触发 packs_dir fsync_barrier")
}

func TestGivenLowAndHighWatermarkPacksWhenCompactPacksThenLowWatermarkRelocatedAndHighKept(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()

	actor, reg := newMeteredActor(t, &store.Options{Dir: dir, DiskID: "compact-wm", MaxPackSize: 4 * store.BlobSize})

	fKeep1, err := actor.Create(ctx, store.CreateReq{ParentIno: store.RootIno, Name: []byte("keep1.bin"), Mode: store.S_IFREG | 0o644})
	require.NoError(t, err)
	keep1Data := makePatternBytes(store.BlobSize, 0x11)
	_, _, err = actor.Write(ctx, fKeep1.Ino, 0, keep1Data)
	require.NoError(t, err)

	fDrop1, err := actor.Create(ctx, store.CreateReq{ParentIno: store.RootIno, Name: []byte("drop1.bin"), Mode: store.S_IFREG | 0o644})
	require.NoError(t, err)
	_, _, err = actor.Write(ctx, fDrop1.Ino, 0, makePatternBytes(3*store.BlobSize, 0x22))
	require.NoError(t, err)

	fKeep2, err := actor.Create(ctx, store.CreateReq{ParentIno: store.RootIno, Name: []byte("keep2.bin"), Mode: store.S_IFREG | 0o644})
	require.NoError(t, err)
	keep2Data := makePatternBytes(3*store.BlobSize, 0x33)
	_, _, err = actor.Write(ctx, fKeep2.Ino, 0, keep2Data)
	require.NoError(t, err)

	fDrop2, err := actor.Create(ctx, store.CreateReq{ParentIno: store.RootIno, Name: []byte("drop2.bin"), Mode: store.S_IFREG | 0o644})
	require.NoError(t, err)
	_, _, err = actor.Write(ctx, fDrop2.Ino, 0, makePatternBytes(store.BlobSize, 0x44))
	require.NoError(t, err)

	fAnchor, err := actor.Create(ctx, store.CreateReq{ParentIno: store.RootIno, Name: []byte("anchor.bin"), Mode: store.S_IFREG | 0o644})
	require.NoError(t, err)
	_, _, err = actor.Write(ctx, fAnchor.Ino, 0, makePatternBytes(store.BlobSize, 0x55))
	require.NoError(t, err)

	require.NoError(t, actor.Unlink(ctx, store.RootIno, []byte("drop1.bin"), false))
	require.NoError(t, actor.Unlink(ctx, store.RootIno, []byte("drop2.bin"), false))
	require.NoError(t, actor.FlushWAL())

	beforeSnap := actor.MetricsSnapshot()
	require.Equalf(t, 1, beforeSnap.LowWatermarkPacks, "应仅有 pack1 处于低水位: got=%d", beforeSnap.LowWatermarkPacks)

	require.NoError(t, actor.CompactPacks(ctx))

	afterSnap := actor.MetricsSnapshot()
	require.Equalf(t, 0, afterSnap.LowWatermarkPacks, "CompactPacks 后低水位 pack 数应归零")
	require.Lessf(t, afterSnap.PackPhysicalBytes, beforeSnap.PackPhysicalBytes, "CompactPacks 后 PackPhysicalBytes 应下降")
	require.Falsef(t, actor.WALDirty(), "CompactPacks 结束后 WALDirty 应为 false")
	require.GreaterOrEqualf(t, histogramSampleCount(t, reg, "bowl_storage_blob_sync_duration_seconds", map[string]string{"trigger": "flush_wal"}), uint64(1), "CompactPacks 回收旧 pack 前应执行 flush_wal 同步")

	_, err = os.Stat(filepath.Join(dir, "packs", "0000000000000001.pack"))
	require.ErrorIsf(t, err, os.ErrNotExist, "低水位 pack1 应已被删除: err=%v", err)
	_, err = os.Stat(filepath.Join(dir, "packs", "0000000000000002.pack"))
	require.NoErrorf(t, err, "高水位 pack2 不得被 CompactPacks 误删: err=%v", err)

	require.NoError(t, actor.CompactPacks(ctx))
	got1, err := actor.Read(ctx, fKeep1.Ino, 0, store.BlobSize)
	require.NoError(t, err)
	require.Equal(t, keep1Data, got1)
	got2, err := actor.Read(ctx, fKeep2.Ino, 0, 3*store.BlobSize)
	require.NoError(t, err)
	require.Equal(t, keep2Data, got2)
}

func TestGivenActiveDeadPackAndCrashRecoveryWhenFlushedAndCleanedThenDeadPacksReclaimedSafely(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()

	actor := openTestActor(t, &store.Options{Dir: dir, DiskID: "dead-pack-lifecycle", MaxPackSize: 2 * store.BlobSize})
	f1, err := actor.Create(ctx, store.CreateReq{ParentIno: store.RootIno, Name: []byte("ephemeral.bin"), Mode: store.S_IFREG | 0o644})
	require.NoError(t, err)
	_, _, err = actor.Write(ctx, f1.Ino, 0, makePatternBytes(2*store.BlobSize, 0x0A))
	require.NoError(t, err)
	require.NoError(t, actor.Unlink(ctx, store.RootIno, []byte("ephemeral.bin"), false))
	require.NoError(t, actor.FlushWAL())

	_, err = os.Stat(filepath.Join(dir, "packs", "0000000000000001.pack"))
	require.NoErrorf(t, err, "活跃 pack 即使 LiveBytes==0 也不得在封口前被删除: err=%v", err)

	f2, err := actor.Create(ctx, store.CreateReq{ParentIno: store.RootIno, Name: []byte("live.bin"), Mode: store.S_IFREG | 0o644})
	require.NoError(t, err)
	_, _, err = actor.Write(ctx, f2.Ino, 0, makePatternBytes(2*store.BlobSize, 0x0B))
	require.NoError(t, err)
	require.NoError(t, actor.FlushWAL())

	_, err = os.Stat(filepath.Join(dir, "packs", "0000000000000001.pack"))
	require.ErrorIsf(t, err, os.ErrNotExist, "封口死包 pack1 应在 FlushWAL 后被回收: err=%v", err)

	orphanFile, err := actor.Create(ctx, store.CreateReq{ParentIno: store.RootIno, Name: []byte("orphan.bin"), Mode: store.S_IFREG | 0o644})
	require.NoError(t, err)
	_, _, err = actor.Write(ctx, orphanFile.Ino, 0, makePatternBytes(2*store.BlobSize, 0x0C))
	require.NoError(t, err)
	require.NoError(t, actor.FlushWAL())
	require.NoError(t, actor.Unlink(ctx, store.RootIno, []byte("orphan.bin"), true))

	require.NoError(t, actor.CloseDiscard())

	recovered := openTestActor(t, &store.Options{Dir: dir, DiskID: "dead-pack-lifecycle", MaxPackSize: 2 * store.BlobSize})
	require.NoError(t, recovered.CleanOrphans(ctx))
	require.NoError(t, recovered.CompactPacks(ctx))

	hasOrphans, err := recovered.HasOrphans()
	require.NoError(t, err)
	require.Falsef(t, hasOrphans, "恢复并 CleanOrphans 后不应残留孤儿")
	gotLive, err := recovered.Read(ctx, f2.Ino, 0, 2*store.BlobSize)
	require.NoError(t, err)
	require.Equal(t, makePatternBytes(2*store.BlobSize, 0x0B), gotLive)
}

func TestGivenQuotaLimitsWhenPrewriteFailsOrMaxBytesUpdatedThenSpaceRecoveredAndPersisted(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()

	actor := openTestActor(t, &store.Options{Dir: dir, DiskID: "quota-pack", MaxBytes: 4 * store.BlobSize, MaxPackSize: 2 * store.BlobSize})
	f, err := actor.Create(ctx, store.CreateReq{ParentIno: store.RootIno, Name: []byte("q.bin"), Mode: store.S_IFREG | 0o644})
	require.NoError(t, err)
	_, _, err = actor.Write(ctx, f.Ino, 0, makePatternBytes(3*store.BlobSize, 0x61))
	require.NoError(t, err)

	_, _, err = actor.Write(ctx, f.Ino, 3*store.BlobSize, makePatternBytes(2*store.BlobSize, 0x62))
	require.ErrorIsf(t, err, store.ErrNoSpace, "超额写入应返回 ErrNoSpace: got=%v", err)
	require.Equalf(t, uint64(3*store.BlobSize), actor.MetricsSnapshot().LogicalBytes, "失败写入不得泄露 LogicalBytes")

	require.ErrorIsf(t, actor.UpdateMaxBytes(ctx, 2*store.BlobSize), store.ErrQuotaShrinkBelowUsed, "缩容低于已用容量应返回 ErrQuotaShrinkBelowUsed")
	require.NoError(t, actor.UpdateMaxBytes(ctx, 8*store.BlobSize))
	require.Equalf(t, uint64(8*store.BlobSize), actor.MaxBytes(), "UpdateMaxBytes 后 MaxBytes() 未更新")

	_, _, err = actor.Write(ctx, f.Ino, 3*store.BlobSize, makePatternBytes(2*store.BlobSize, 0x63))
	require.NoError(t, err)
	require.NoError(t, actor.Close())

	reopened := openTestActor(t, &store.Options{Dir: dir, DiskID: "quota-pack", MaxBytes: 6 * store.BlobSize, MaxPackSize: 2 * store.BlobSize, ResetEpoch: true})
	require.Equalf(t, uint64(6*store.BlobSize), reopened.MaxBytes(), "ResetEpoch 重开后有效缩容 MaxBytes 应生效")
}
