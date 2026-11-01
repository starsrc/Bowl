package meta_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/starsrc/bowl/services/storage/internal/db"
	dbmock "github.com/starsrc/bowl/services/storage/internal/db/mock"
	"github.com/starsrc/bowl/services/storage/internal/meta"
)

// TestDeleteAllChunksByInos 单元测试 deleteAllChunksByInos 批量清理函数
func TestDeleteAllChunksByInos(t *testing.T) {
	ctx := context.Background()

	t.Run("inos 为空时直接返回 nil 且无数据库调用", func(t *testing.T) {
		mockQuerier := dbmock.NewMockQuerier(t)
		err := meta.DeleteAllChunksByInos(ctx, mockQuerier, nil)
		require.NoError(t, err)

		err = meta.DeleteAllChunksByInos(ctx, mockQuerier, []int64{})
		require.NoError(t, err)
		mockQuerier.AssertNotCalled(t, "DeleteChunksByInos", mock.Anything, mock.Anything)
	})

	t.Run("DeleteChunksByInos 数据库错误透传", func(t *testing.T) {
		mockQuerier := dbmock.NewMockQuerier(t)
		inos := []int64{101, 102}
		expectedErr := errors.New("db error")

		mockQuerier.EXPECT().
			DeleteChunksByInos(mock.Anything, inos).
			Return(nil, expectedErr).
			Once()

		err := meta.DeleteAllChunksByInos(ctx, mockQuerier, inos)
		require.Error(t, err)
		assert.ErrorIs(t, err, expectedErr)
	})

	t.Run("DeleteChunksByInos 返回空 deletedSlices 时正常返回 nil", func(t *testing.T) {
		mockQuerier := dbmock.NewMockQuerier(t)
		inos := []int64{101, 102}

		mockQuerier.EXPECT().
			DeleteChunksByInos(mock.Anything, inos).
			Return(nil, nil).
			Once()

		err := meta.DeleteAllChunksByInos(ctx, mockQuerier, inos)
		require.NoError(t, err)
		mockQuerier.AssertNotCalled(t, "AddSliceRefCount", mock.Anything, mock.Anything)
	})

	t.Run("成功删除并按 slice_id 升序扣减 slice_refs", func(t *testing.T) {
		mockQuerier := dbmock.NewMockQuerier(t)
		inos := []int64{101, 102}

		// 构造切片数据：
		// slice 30 出现 1 次
		// slice 10 出现 2 次
		// slice 20 出现 1 次
		// hole (id=0) 出现 1 次
		chunk1 := meta.EncodeSlices([]meta.Slice{
			{Pos: 0, ID: 30, Size: 4096, Off: 0, Len: 1024},
			{Pos: 1024, ID: 10, Size: 4096, Off: 0, Len: 1024},
		})
		chunk2 := meta.EncodeSlices([]meta.Slice{
			{Pos: 0, ID: 0, Size: 0, Off: 0, Len: 1024}, // hole slice, id=0
			{Pos: 1024, ID: 10, Size: 4096, Off: 1024, Len: 1024},
			{Pos: 2048, ID: 20, Size: 4096, Off: 0, Len: 1024},
		})

		mockQuerier.EXPECT().
			DeleteChunksByInos(mock.Anything, inos).
			Return([][]byte{chunk1, chunk2}, nil).
			Once()

		var callOrder []int64
		// 必须按 slice_id 升序依次扣减: 10 (-2), 20 (-1), 30 (-1)
		mockQuerier.EXPECT().
			AddSliceRefCount(mock.Anything, db.AddSliceRefCountParams{
				SliceID: 10,
				Delta:   -2,
			}).
			Run(func(_ context.Context, arg db.AddSliceRefCountParams) {
				callOrder = append(callOrder, arg.SliceID)
			}).
			Return(nil).
			Once()

		mockQuerier.EXPECT().
			AddSliceRefCount(mock.Anything, db.AddSliceRefCountParams{
				SliceID: 20,
				Delta:   -1,
			}).
			Run(func(_ context.Context, arg db.AddSliceRefCountParams) {
				callOrder = append(callOrder, arg.SliceID)
			}).
			Return(nil).
			Once()

		mockQuerier.EXPECT().
			AddSliceRefCount(mock.Anything, db.AddSliceRefCountParams{
				SliceID: 30,
				Delta:   -1,
			}).
			Run(func(_ context.Context, arg db.AddSliceRefCountParams) {
				callOrder = append(callOrder, arg.SliceID)
			}).
			Return(nil).
			Once()

		err := meta.DeleteAllChunksByInos(ctx, mockQuerier, inos)
		require.NoError(t, err)
		assert.Equal(t, []int64{10, 20, 30}, callOrder, "必须按 slice_id 升序依次扣减以保证确定性锁顺序")
	})

	t.Run("AddSliceRefCount 出错时透传错误", func(t *testing.T) {
		mockQuerier := dbmock.NewMockQuerier(t)
		inos := []int64{101}
		chunk := meta.EncodeSlices([]meta.Slice{
			{Pos: 0, ID: 50, Size: 4096, Off: 0, Len: 1024},
		})

		mockQuerier.EXPECT().
			DeleteChunksByInos(mock.Anything, inos).
			Return([][]byte{chunk}, nil).
			Once()

		expectedErr := errors.New("ref decrement failed")
		mockQuerier.EXPECT().
			AddSliceRefCount(mock.Anything, db.AddSliceRefCountParams{
				SliceID: 50,
				Delta:   -1,
			}).
			Return(expectedErr).
			Once()

		err := meta.DeleteAllChunksByInos(ctx, mockQuerier, inos)
		require.Error(t, err)
		assert.ErrorIs(t, err, expectedErr)
	})
}

// TestRenewSession_DeleteChunksError 验证 RenewSession 在批量清理孤儿 chunks 出错时向上传播错误并中止
func TestRenewSession_DeleteChunksError(t *testing.T) {
	ctx := context.Background()
	sessID := uint64(55001)

	mockStore := dbmock.NewMockStore(t)
	mockQuerier := dbmock.NewMockQuerier(t)
	mgr := meta.New(mockStore)

	setupMockWriteTx(mockStore, mockQuerier)

	mockQuerier.EXPECT().
		DeleteExpiredClientSessions(mock.Anything).
		Return(int64(0), nil).
		Once()

	mockQuerier.EXPECT().
		UpsertClientSession(mock.Anything, mock.Anything).
		Return(db.ClientSession{
			SessionID: int64(sessID),
			ExpiresAt: pgtype.Timestamptz{Time: time.Now().Add(time.Minute), Valid: true},
		}, nil).
		Once()

	mockQuerier.EXPECT().
		DeleteUnreferencedOrphanInodes(mock.Anything).
		Return([]db.DeleteUnreferencedOrphanInodesRow{
			{Ino: 301, RootIno: 10, Mode: int32(meta.S_IFREG | 0o644), Size: 4096},
		}, nil).
		Once()

	expectedErr := errors.New("delete chunks failed")
	mockQuerier.EXPECT().
		DeleteChunksByInos(mock.Anything, []int64{301}).
		Return(nil, expectedErr).
		Once()

	expiresAt, err := mgr.RenewSession(ctx, sessID)
	require.Error(t, err)
	assert.ErrorIs(t, err, expectedErr)
	assert.True(t, expiresAt.IsZero())
}

// TestDeleteDisk_DeleteChunksError 验证 DeleteDisk 在批量清理磁盘 chunks 出错时向上传播错误
func TestDeleteDisk_DeleteChunksError(t *testing.T) {
	ctx := context.Background()
	diskIDStr := "11111111-1111-1111-1111-111111111111"
	var diskID pgtype.UUID
	_ = diskID.Scan(diskIDStr)
	rootIno := int64(1001)

	mockStore := dbmock.NewMockStore(t)
	mockQuerier := dbmock.NewMockQuerier(t)
	mgr := meta.New(mockStore)

	setupMockWriteTx(mockStore, mockQuerier)

	mockQuerier.EXPECT().
		GetDiskForUpdate(mock.Anything, diskID).
		Return(db.GetDiskForUpdateRow{
			DiskID:  diskID,
			RootIno: rootIno,
		}, nil).
		Once()

	mockQuerier.EXPECT().
		ListActiveSessionIDsByDisk(mock.Anything, db.ListActiveSessionIDsByDiskParams{
			DiskID:  diskID,
			RootIno: rootIno,
		}).
		Return([]int64(nil), nil).
		Once()

	mockQuerier.EXPECT().
		DeleteDisk(mock.Anything, diskID).
		Return(int64(1), nil).
		Once()

	mockQuerier.EXPECT().
		DeleteInodesByRootIno(mock.Anything, rootIno).
		Return([]int64{rootIno, 2001}, nil).
		Once()

	expectedErr := errors.New("delete disk chunks failed")
	mockQuerier.EXPECT().
		DeleteChunksByInos(mock.Anything, []int64{rootIno, 2001}).
		Return(nil, expectedErr).
		Once()

	err := mgr.DeleteDisk(ctx, diskIDStr)
	require.Error(t, err)
	assert.ErrorIs(t, err, expectedErr)
}

// TestBatchDestroyChunks_PostgresIntegration 在真实 PostgreSQL 环境下端到端验证批量销毁 chunks 与 slice_refs
func TestBatchDestroyChunks_PostgresIntegration(t *testing.T) {
	pool, ctx := setupMetaTestDB(t)
	if pool == nil {
		return
	}
	acquireTestSliceGCAdvisoryLock(t, pool)

	store := db.NewStore(pool)
	dataDir := t.TempDir()
	blockStore := meta.NewLocalFSBlockStore(dataDir)
	mgr := meta.New(store, meta.WithBlockStore(blockStore))

	t.Run("场景 1: Unlink + Release 多句柄与末个句柄生命周期验证", func(t *testing.T) {
		diskName := fmt.Sprintf("test_batch_destroy_unlink_release_%d", time.Now().UnixNano())
		disk, err := mgr.CreateDisk(ctx, meta.CreateDiskParams{
			Name:     diskName,
			MaxBytes: 100 * 1024 * 1024,
		})
		require.NoError(t, err)
		defer func() {
			_ = mgr.DeleteDisk(context.Background(), disk.DiskID)
		}()

		fileName := []byte("multi_handle_orphan.bin")
		createRes, err := mgr.Create(ctx, meta.CreateParams{
			ParentIno: disk.RootIno,
			Name:      fileName,
			Mode:      meta.S_IFREG | 0o644,
			UID:       1000,
			GID:       1000,
			Flags:     meta.O_RDWR,
		})
		require.NoError(t, err)
		ino := createRes.Inode.Ino
		fh1 := createRes.Fh

		// 写入数据并落盘
		payload := bytes.Repeat([]byte("A"), 8192)
		writeRes, err := mgr.Write(ctx, &meta.WriteRequest{
			Ino:    ino,
			Fh:     fh1,
			Offset: 0,
			Data:   payload,
		})
		require.NoError(t, err)
		assert.Equal(t, uint32(len(payload)), writeRes.Size)

		// 打开第二个句柄 fh2
		openRes, err := mgr.Open(ctx, meta.OpenParams{
			Ino:   ino,
			Flags: meta.O_RDWR,
		})
		require.NoError(t, err)
		fh2 := openRes.Fh

		// 查询初始 chunks 与 slice_id
		chunkRow, err := store.GetChunk(ctx, db.GetChunkParams{Ino: int64(ino), Indx: 0})
		require.NoError(t, err)
		slicesList, err := meta.DecodeSlices(chunkRow.Slices)
		require.NoError(t, err)
		require.NotEmpty(t, slicesList)
		sliceID := slicesList[0].ID
		defer func() {
			_ = store.DeleteSliceRef(context.Background(), int64(sliceID))
		}()

		refRow, err := store.GetSliceRef(ctx, int64(sliceID))
		require.NoError(t, err)
		assert.Equal(t, int32(1), refRow.Refs)

		// 1. 执行 Unlink，转为 Orphan Inode (nlink = 0)
		err = mgr.Unlink(ctx, disk.RootIno, fileName)
		require.NoError(t, err)

		// 断言: 因仍有活跃句柄 (fh1, fh2)，chunks 与 slice_refs 依然存在
		_, err = store.GetChunk(ctx, db.GetChunkParams{Ino: int64(ino), Indx: 0})
		require.NoError(t, err, "Orphan Inode 存在活跃句柄时 chunks 必须保留")
		refRowAfterUnlink, err := store.GetSliceRef(ctx, int64(sliceID))
		require.NoError(t, err)
		assert.Equal(t, int32(1), refRowAfterUnlink.Refs, "Orphan Inode 存在活跃句柄时 slice_refs 必须保留")

		// 2. 关闭第一个句柄 fh1
		err = mgr.Release(ctx, meta.ReleaseParams{
			Ino: ino,
			Fh:  fh1,
		})
		require.NoError(t, err)

		// 断言: 因仍有 fh2，chunks 与 slice_refs 依然存在
		_, err = store.GetChunk(ctx, db.GetChunkParams{Ino: int64(ino), Indx: 0})
		require.NoError(t, err, "关闭非末个句柄后 chunks 必须依然保留")
		refRowAfterFh1, err := store.GetSliceRef(ctx, int64(sliceID))
		require.NoError(t, err)
		assert.Equal(t, int32(1), refRowAfterFh1.Refs)

		// 3. 关闭最后一个句柄 fh2
		err = mgr.Release(ctx, meta.ReleaseParams{
			Ino: ino,
			Fh:  fh2,
		})
		require.NoError(t, err)

		// 断言: 此时无剩余句柄，在同一事务内 Inode、chunks 均被物理清除，slice_refs 引用归零
		_, err = store.GetInode(ctx, int64(ino))
		assert.ErrorIs(t, err, pgx.ErrNoRows, "末个句柄释放后 Inode 必须被物理回收")

		_, err = store.GetChunk(ctx, db.GetChunkParams{Ino: int64(ino), Indx: 0})
		assert.ErrorIs(t, err, pgx.ErrNoRows, "末个句柄释放后 chunks 表行必须被清空")

		refRowAfterFinal, err := store.GetSliceRef(ctx, int64(sliceID))
		require.NoError(t, err)
		assert.Equal(t, int32(0), refRowAfterFinal.Refs, "末个句柄释放后 slice_refs.refs 必须归零")
	})

	t.Run("场景 2: 会话超时后 RenewSession 触发多个超期孤儿 Inode 的批量 chunks 回收与 slice_refs 递减", func(t *testing.T) {
		diskName := fmt.Sprintf("test_batch_destroy_renew_session_%d", time.Now().UnixNano())
		disk, err := mgr.CreateDisk(ctx, meta.CreateDiskParams{
			Name:     diskName,
			MaxBytes: 100 * 1024 * 1024,
		})
		require.NoError(t, err)
		defer func() {
			_ = mgr.DeleteDisk(context.Background(), disk.DiskID)
		}()

		sessActive := uint64(99101)
		sessExpired := uint64(99102)
		defer func() {
			_, _ = pool.Exec(context.Background(), "DELETE FROM client_sessions WHERE session_id IN ($1, $2)", int64(sessActive), int64(sessExpired))
		}()

		// 激活两个会话
		_, err = mgr.RenewSession(ctx, sessActive)
		require.NoError(t, err)
		_, err = mgr.RenewSession(ctx, sessExpired)
		require.NoError(t, err)

		// 在 sessExpired 下创建两个文件
		f1Res, err := mgr.Create(ctx, meta.CreateParams{
			ParentIno: disk.RootIno,
			Name:      []byte("orphan1.txt"),
			Mode:      meta.S_IFREG | 0o644,
			UID:       1000,
			GID:       1000,
			Flags:     meta.O_RDWR,
			SessionID: &sessExpired,
		})
		require.NoError(t, err)

		f2Res, err := mgr.Create(ctx, meta.CreateParams{
			ParentIno: disk.RootIno,
			Name:      []byte("orphan2.txt"),
			Mode:      meta.S_IFREG | 0o644,
			UID:       1000,
			GID:       1000,
			Flags:     meta.O_RDWR,
			SessionID: &sessExpired,
		})
		require.NoError(t, err)

		// 分别写入数据
		payload1 := []byte("orphan1-payload-data")
		_, err = mgr.Write(ctx, &meta.WriteRequest{
			Ino:       f1Res.Inode.Ino,
			Fh:        f1Res.Fh,
			Offset:    0,
			Data:      payload1,
			SessionID: &sessExpired,
		})
		require.NoError(t, err)

		payload2 := []byte("orphan2-payload-data-longer-slice")
		_, err = mgr.Write(ctx, &meta.WriteRequest{
			Ino:       f2Res.Inode.Ino,
			Fh:        f2Res.Fh,
			Offset:    0,
			Data:      payload2,
			SessionID: &sessExpired,
		})
		require.NoError(t, err)

		// 获取两者的 slice_id
		chunk1, err := store.GetChunk(ctx, db.GetChunkParams{Ino: int64(f1Res.Inode.Ino), Indx: 0})
		require.NoError(t, err)
		slices1, err := meta.DecodeSlices(chunk1.Slices)
		require.NoError(t, err)
		sliceID1 := slices1[0].ID

		chunk2, err := store.GetChunk(ctx, db.GetChunkParams{Ino: int64(f2Res.Inode.Ino), Indx: 0})
		require.NoError(t, err)
		slices2, err := meta.DecodeSlices(chunk2.Slices)
		require.NoError(t, err)
		sliceID2 := slices2[0].ID
		defer func() {
			_ = store.DeleteSliceRef(context.Background(), int64(sliceID1))
			_ = store.DeleteSliceRef(context.Background(), int64(sliceID2))
		}()

		// 分别 Unlink 两文件（此时因为持有 sessExpired 句柄，成为 Orphan Inode）
		err = mgr.Unlink(ctx, disk.RootIno, []byte("orphan1.txt"))
		require.NoError(t, err)
		err = mgr.Unlink(ctx, disk.RootIno, []byte("orphan2.txt"))
		require.NoError(t, err)

		// 将 sessExpired 伪造为已超期 (10 秒前)
		pastTime := time.Now().Add(-10 * time.Second)
		_, err = pool.Exec(ctx, "UPDATE client_sessions SET expires_at = $1 WHERE session_id = $2", pastTime, int64(sessExpired))
		require.NoError(t, err)

		// 客户端调用活跃会话的 RenewSession，触发超期会话级联清理与超期孤儿批量回收
		_, err = mgr.RenewSession(ctx, sessActive)
		require.NoError(t, err)

		// 断言 1: 超期会话被清理
		var sessCount int
		err = pool.QueryRow(ctx, "SELECT count(*) FROM client_sessions WHERE session_id = $1", int64(sessExpired)).Scan(&sessCount)
		require.NoError(t, err)
		assert.Equal(t, 0, sessCount)

		// 断言 2: 两孤儿 Inode 被物理删除
		_, err = store.GetInode(ctx, int64(f1Res.Inode.Ino))
		assert.ErrorIs(t, err, pgx.ErrNoRows)
		_, err = store.GetInode(ctx, int64(f2Res.Inode.Ino))
		assert.ErrorIs(t, err, pgx.ErrNoRows)

		// 断言 3: 两孤儿 Inode 的 chunks 均被物理删除
		_, err = store.GetChunk(ctx, db.GetChunkParams{Ino: int64(f1Res.Inode.Ino), Indx: 0})
		assert.ErrorIs(t, err, pgx.ErrNoRows)
		_, err = store.GetChunk(ctx, db.GetChunkParams{Ino: int64(f2Res.Inode.Ino), Indx: 0})
		assert.ErrorIs(t, err, pgx.ErrNoRows)

		// 断言 4: 两者的 slice_refs 均被扣减归零
		ref1, err := store.GetSliceRef(ctx, int64(sliceID1))
		require.NoError(t, err)
		assert.Equal(t, int32(0), ref1.Refs)

		ref2, err := store.GetSliceRef(ctx, int64(sliceID2))
		require.NoError(t, err)
		assert.Equal(t, int32(0), ref2.Refs)
	})

	t.Run("场景 3: DeleteDisk 级联清空该磁盘下所有 Inode 的 chunks 并扣减 slice_refs，隔离保留其他磁盘", func(t *testing.T) {
		// 创建 diskA 与 diskB
		diskA, err := mgr.CreateDisk(ctx, meta.CreateDiskParams{
			Name:     fmt.Sprintf("disk_a_%d", time.Now().UnixNano()),
			MaxBytes: 100 * 1024 * 1024,
		})
		require.NoError(t, err)
		defer func() {
			_ = mgr.DeleteDisk(context.Background(), diskA.DiskID)
		}()

		diskB, err := mgr.CreateDisk(ctx, meta.CreateDiskParams{
			Name:     fmt.Sprintf("disk_b_%d", time.Now().UnixNano()),
			MaxBytes: 100 * 1024 * 1024,
		})
		require.NoError(t, err)
		defer func() {
			_ = mgr.DeleteDisk(context.Background(), diskB.DiskID)
		}()

		// diskA 下创建两个常规文件
		fileA1, err := mgr.Create(ctx, meta.CreateParams{
			ParentIno: diskA.RootIno,
			Name:      []byte("fileA1.bin"),
			Mode:      meta.S_IFREG | 0o644,
			UID:       1000,
			GID:       1000,
			Flags:     meta.O_RDWR,
		})
		require.NoError(t, err)

		fileA2, err := mgr.Create(ctx, meta.CreateParams{
			ParentIno: diskA.RootIno,
			Name:      []byte("fileA2.bin"),
			Mode:      meta.S_IFREG | 0o644,
			UID:       1000,
			GID:       1000,
			Flags:     meta.O_RDWR,
		})
		require.NoError(t, err)

		// diskB 下创建一个常规文件
		fileB, err := mgr.Create(ctx, meta.CreateParams{
			ParentIno: diskB.RootIno,
			Name:      []byte("fileB.bin"),
			Mode:      meta.S_IFREG | 0o644,
			UID:       1000,
			GID:       1000,
			Flags:     meta.O_RDWR,
		})
		require.NoError(t, err)

		// 写入数据：fileA1 多次写入 (多 slice)
		_, err = mgr.Write(ctx, &meta.WriteRequest{
			Ino:    fileA1.Inode.Ino,
			Fh:     fileA1.Fh,
			Offset: 0,
			Data:   []byte("fileA1-slice1-data"),
		})
		require.NoError(t, err)

		_, err = mgr.Write(ctx, &meta.WriteRequest{
			Ino:    fileA1.Inode.Ino,
			Fh:     fileA1.Fh,
			Offset: 1024,
			Data:   []byte("fileA1-slice2-data"),
		})
		require.NoError(t, err)

		// fileA2 单次写入
		_, err = mgr.Write(ctx, &meta.WriteRequest{
			Ino:    fileA2.Inode.Ino,
			Fh:     fileA2.Fh,
			Offset: 0,
			Data:   []byte("fileA2-slice-data"),
		})
		require.NoError(t, err)

		// fileB 写入
		_, err = mgr.Write(ctx, &meta.WriteRequest{
			Ino:    fileB.Inode.Ino,
			Fh:     fileB.Fh,
			Offset: 0,
			Data:   []byte("fileB-slice-data"),
		})
		require.NoError(t, err)

		// 收集 diskA 关联的 slice_ids
		chunkA1, err := store.GetChunk(ctx, db.GetChunkParams{Ino: int64(fileA1.Inode.Ino), Indx: 0})
		require.NoError(t, err)
		slicesA1, err := meta.DecodeSlices(chunkA1.Slices)
		require.NoError(t, err)
		require.Len(t, slicesA1, 2)

		chunkA2, err := store.GetChunk(ctx, db.GetChunkParams{Ino: int64(fileA2.Inode.Ino), Indx: 0})
		require.NoError(t, err)
		slicesA2, err := meta.DecodeSlices(chunkA2.Slices)
		require.NoError(t, err)
		require.Len(t, slicesA2, 1)

		var diskASliceIDs []uint64
		for _, s := range slicesA1 {
			diskASliceIDs = append(diskASliceIDs, s.ID)
		}
		for _, s := range slicesA2 {
			diskASliceIDs = append(diskASliceIDs, s.ID)
		}

		// 收集 diskB 关联的 slice_ids
		chunkB, err := store.GetChunk(ctx, db.GetChunkParams{Ino: int64(fileB.Inode.Ino), Indx: 0})
		require.NoError(t, err)
		slicesB, err := meta.DecodeSlices(chunkB.Slices)
		require.NoError(t, err)
		require.Len(t, slicesB, 1)
		diskBSliceID := slicesB[0].ID
		defer func() {
			for _, id := range diskASliceIDs {
				_ = store.DeleteSliceRef(context.Background(), int64(id))
			}
			_ = store.DeleteSliceRef(context.Background(), int64(diskBSliceID))
		}()

		// 执行 DeleteDisk 销毁 diskA
		err = mgr.DeleteDisk(ctx, diskA.DiskID)
		require.NoError(t, err)

		// 断言 1: diskA 及其 Inodes 被删除
		_, err = mgr.GetDisk(ctx, diskA.DiskID)
		assert.ErrorIs(t, err, meta.ErrNotFound)

		_, err = store.GetInode(ctx, int64(fileA1.Inode.Ino))
		assert.ErrorIs(t, err, pgx.ErrNoRows)
		_, err = store.GetInode(ctx, int64(fileA2.Inode.Ino))
		assert.ErrorIs(t, err, pgx.ErrNoRows)

		// 断言 2: diskA 下文件的 chunks 均被物理删除
		_, err = store.GetChunk(ctx, db.GetChunkParams{Ino: int64(fileA1.Inode.Ino), Indx: 0})
		assert.ErrorIs(t, err, pgx.ErrNoRows)
		_, err = store.GetChunk(ctx, db.GetChunkParams{Ino: int64(fileA2.Inode.Ino), Indx: 0})
		assert.ErrorIs(t, err, pgx.ErrNoRows)

		// 断言 3: diskA 下文件的 slice_refs 均被扣减归零
		for _, id := range diskASliceIDs {
			ref, err := store.GetSliceRef(ctx, int64(id))
			require.NoError(t, err)
			assert.Equal(t, int32(0), ref.Refs, "diskA 的切片引用计数必须归零")
		}

		// 断言 4: diskB 的 chunks 与 slice_refs 隔离保留，不受任何干扰
		chunkBAfter, err := store.GetChunk(ctx, db.GetChunkParams{Ino: int64(fileB.Inode.Ino), Indx: 0})
		require.NoError(t, err)
		assert.Equal(t, chunkB.Slices, chunkBAfter.Slices)

		refB, err := store.GetSliceRef(ctx, int64(diskBSliceID))
		require.NoError(t, err)
		assert.Equal(t, int32(1), refB.Refs, "diskB 的切片引用计数必须不受影响")
	})
}
