package meta_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/starsrc/bowl/services/storage/internal/db"
	dbmock "github.com/starsrc/bowl/services/storage/internal/db/mock"
	"github.com/starsrc/bowl/services/storage/internal/meta"
)

// 已引用的首批物理块必须允许后续扫描推进到同目录、同桶跨二级目录及跨散列桶的未登记孤儿块，并在到达目录树末尾后回绕回收新老化的前缀孤儿块。
func TestReviewLogicR1_GCScanProgressPastReferencedPrefix(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	dir := t.TempDir()
	bs := meta.NewLocalFSBlockStore(dir)
	const (
		liveID               = uint64(256)     // chunks/00/0/256_0_4
		sameDirOrphanID      = uint64(512)     // chunks/00/0/512_0_4（首轮未满宽限期，验证游标越过后老化再回绕重扫）
		sameBucketL2OrphanID = uint64(1000192) // chunks/00/1/1000192_0_4（同桶不同二级目录 l2="1"，文件名 "1000192_0_4" < "256_0_4"）
		crossBucketOrphanID  = uint64(257)     // chunks/01/0/257_0_4
	)
	old := time.Now().Add(-2 * time.Hour)
	fresh := time.Now()
	for _, item := range []struct {
		id    uint64
		mtime time.Time
	}{
		{id: liveID, mtime: old},
		{id: sameDirOrphanID, mtime: fresh},
		{id: sameBucketL2OrphanID, mtime: old},
		{id: crossBucketOrphanID, mtime: old},
	} {
		require.NoError(t, bs.PutBlock(ctx, item.id, 0, []byte("data")))
		path := filepath.Join(dir, "chunks", fmt.Sprintf("%02X", item.id%256), fmt.Sprintf("%d", item.id/1000000), fmt.Sprintf("%d_0_4", item.id))
		require.NoError(t, os.Chtimes(path, item.mtime, item.mtime))
	}
	store := dbmock.NewMockStore(t)
	q := dbmock.NewMockQuerier(t)
	setupMockGCTxRepeated(store, q)
	q.EXPECT().ListGarbageSliceRefs(mock.Anything, int32(1)).Return(nil, nil)
	store.EXPECT().GetSliceRef(mock.Anything, int64(liveID)).Return(db.SliceRef{SliceID: int64(liveID), Size: 4, Refs: 1}, nil).Maybe()
	store.EXPECT().GetSliceRef(mock.Anything, int64(sameDirOrphanID)).Return(db.SliceRef{}, pgx.ErrNoRows).Maybe()
	store.EXPECT().GetSliceRef(mock.Anything, int64(sameBucketL2OrphanID)).Return(db.SliceRef{}, pgx.ErrNoRows).Maybe()
	store.EXPECT().GetSliceRef(mock.Anything, int64(crossBucketOrphanID)).Return(db.SliceRef{}, pgx.ErrNoRows).Maybe()
	mgr := meta.New(store, meta.WithBlockStore(bs))

	// 第一阶段（2 批次）：第 1 批扫描 liveID=256（游标停在 00/0/256_0_4）；
	// 第 2 批跳过尚未满宽限期的 sameDirOrphanID=512，跨二级目录扫描并删除 00/1/1000192_0_4（游标停在 00/1/1000192_0_4）。
	for batch := 0; batch < 2; batch++ {
		_, err := mgr.CollectGarbage(ctx, 1)
		require.NoError(t, err)
	}
	sameBucketL2Path := filepath.Join(dir, "chunks", "00", "1", "1000192_0_4")
	_, err := os.Stat(sameBucketL2Path)
	require.ErrorIs(t, err, os.ErrNotExist, "同一散列桶不同二级目录的孤儿块应被 GC 推进并回收")

	sameDirOrphanPath := filepath.Join(dir, "chunks", "00", "0", "512_0_4")
	_, err = os.Stat(sameDirOrphanPath)
	require.NoError(t, err, "未满宽限期的前缀孤儿块在首次越过时不应被提前删除")

	// 模拟游标已越过 00/0 后，前缀孤儿块 512_0_4 老化超过宽限期
	require.NoError(t, os.Chtimes(sameDirOrphanPath, old, old))

	// 第二阶段（3 批次）：第 3 批回收 01/0/257_0_4；随后游标到达目录树末尾复位并回绕至 00/0，经过 liveID=256 后回收 512_0_4。
	for batch := 0; batch < 3; batch++ {
		_, err := mgr.CollectGarbage(ctx, 1)
		require.NoError(t, err)
	}

	live := make([]byte, 4)
	require.NoError(t, bs.GetBlock(ctx, liveID, 0, 4, 0, live))
	require.Equal(t, []byte("data"), live)
	_, err = os.Stat(sameDirOrphanPath)
	require.ErrorIs(t, err, os.ErrNotExist, "游标到达末尾后必须回绕重扫并回收已老化的前缀孤儿块")
	orphanPath := filepath.Join(dir, "chunks", "01", "0", "257_0_4")
	_, err = os.Stat(orphanPath)
	require.ErrorIs(t, err, os.ErrNotExist, "已引用的首批块不应使后续未登记孤儿块永久逃过 GC")
}

// 同一会话在删盘探测期间完成流重连后，新流仍能响应探活，磁盘必须保留。
func TestReviewLogicR1_DeleteDiskPreservesReplacementStream(t *testing.T) {
	pool, ctx := setupMetaTestDB(t)
	mgr := meta.New(db.NewStore(pool), meta.WithSessionProbeTimeout(200*time.Millisecond))
	disk, err := mgr.CreateDisk(ctx, meta.CreateDiskParams{Name: t.Name(), MaxBytes: 1024})
	require.NoError(t, err)
	sid := uint64(time.Now().UnixNano() & 0x7fffffffffffffff)
	t.Cleanup(func() {
		mgr.WatchHub().Evict(sid)
		require.NoError(t, mgr.CloseSession(context.Background(), sid))
		_, err := pool.Exec(context.Background(), "DELETE FROM disks WHERE disk_id = $1", disk.DiskID)
		require.NoError(t, err)
		_, err = pool.Exec(context.Background(), "DELETE FROM inodes WHERE root_ino = $1", int64(disk.RootIno))
		require.NoError(t, err)
	})
	_, err = mgr.RenewSessionWithDisk(ctx, sid, &disk.DiskID)
	require.NoError(t, err)

	oldPing := make(chan struct{})
	replacementReady := make(chan struct{})
	_, unregisterOld := mgr.WatchHub().Register(sid, func(seq uint64) error {
		close(oldPing)
		select {
		case <-replacementReady:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	})
	defer unregisterOld()
	deleteDone := make(chan error, 1)
	go func() { deleteDone <- mgr.DeleteDisk(ctx, disk.DiskID) }()
	select {
	case <-oldPing:
	case <-ctx.Done():
		t.Fatal("未收到旧流探活")
	}
	var newStreamID uint64
	newStreamID, unregisterNew := mgr.WatchHub().Register(sid, func(seq uint64) error {
		mgr.WatchHub().HandlePong(sid, newStreamID, seq)
		return nil
	})
	defer unregisterNew()
	// 在旧探测返回前独立验证新流已经能够完成 Ping-Pong。
	require.True(t, mgr.WatchHub().Probe(ctx, sid, 100*time.Millisecond))
	close(replacementReady)
	select {
	case err := <-deleteDone:
		assert.ErrorIs(t, err, meta.ErrBusy, "替换旧流不能成为删除存活挂载磁盘的依据")
	case <-ctx.Done():
		t.Fatal("删盘未在上下文期限内完成")
	}
	_, err = mgr.GetDisk(ctx, disk.DiskID)
	assert.NoError(t, err, "已确认新流存活时磁盘必须保留")
	var sessions int
	require.NoError(t, pool.QueryRow(ctx, "SELECT count(*) FROM client_sessions WHERE session_id = $1", int64(sid)).Scan(&sessions))
	assert.Equal(t, 1, sessions, "新流所属会话必须保留")
}
