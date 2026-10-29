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

// TestUnlink_ChunksCleanup_Mock 验证在 Unlink 常规文件时清除 chunks 并扣减 slice_refs 的事务内行为（Mock 单元测试）。
func TestUnlink_ChunksCleanup_Mock(t *testing.T) {
	ctx := context.Background()
	now := time.Now().Truncate(time.Microsecond)
	parentIno := uint64(1001)
	childIno := uint64(2001)
	rootIno := int64(100)
	name := []byte("testfile.txt")

	parentRow := db.Inode{
		Ino:   int64(parentIno),
		Mode:  int32(meta.S_IFDIR | 0o755),
		Nlink: 2,
		Uid:   1000,
		Gid:   1000,
	}

	childRow := db.Inode{
		Ino:        int64(childIno),
		Generation: 1,
		Mode:       int32(meta.S_IFREG | 0o644),
		Size:       8192,
		Nlink:      1,
		Uid:        1000,
		Gid:        1000,
		RootIno:    rootIno,
		Atime:      pgtype.Timestamptz{Time: now, Valid: true},
		Mtime:      pgtype.Timestamptz{Time: now, Valid: true},
		Ctime:      pgtype.Timestamptz{Time: now, Valid: true},
	}

	entryRow := db.Entry{
		ParentIno: int64(parentIno),
		Name:      name,
		Ino:       int64(childIno),
	}

	t.Run("无活跃句柄时 Unlink 删除常规文件最后一个硬链接 (nlink == 0)，在事务内调用 DeleteChunksByIno 并扣减 slice_refs 随后物理删除 Inode", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockWriteTx(mockStore, mockQuerier)

		// 1. 升序加锁: parent(1001) < child(2001)
		mockQuerier.EXPECT().GetEntry(mock.Anything, db.GetEntryParams{
			ParentIno: int64(parentIno),
			Name:      name,
		}).Return(entryRow, nil).Once()

		mockQuerier.EXPECT().GetInodeForUpdate(mock.Anything, int64(parentIno)).
			Return(parentRow, nil).Once()
		mockQuerier.EXPECT().GetInodeForUpdate(mock.Anything, int64(childIno)).
			Return(childRow, nil).Once()

		// 二次校验目录项未漂移
		mockQuerier.EXPECT().GetEntry(mock.Anything, db.GetEntryParams{
			ParentIno: int64(parentIno),
			Name:      name,
		}).Return(entryRow, nil).Once()

		// 2. 删除目录项
		mockQuerier.EXPECT().DeleteEntry(mock.Anything, db.DeleteEntryParams{
			ParentIno: int64(parentIno),
			Name:      name,
		}).Return(nil).Once()

		// 3. 递减子节点硬链接并刷新 ctime
		childDecremented := childRow
		childDecremented.Nlink = 0
		mockQuerier.EXPECT().DecrementInodeNlinkAndTouchCtime(mock.Anything, int64(childIno)).
			Return(childDecremented, nil).Once()

		// 4. 刷新父目录时间戳
		mockQuerier.EXPECT().TouchInodeTimestamps(mock.Anything, int64(parentIno)).
			Return(nil).Once()

		// 5. 检查活跃句柄 (返回无活跃句柄)
		mockQuerier.EXPECT().HasFileHandlesByIno(mock.Anything, int64(childIno)).
			Return(false, nil).Once()

		// 6. 核心契约：必须在同一事务内清理 chunks 并扣减 slice_refs
		chunkBytes := meta.EncodeSlices([]meta.Slice{
			{Pos: 0, ID: 50, Size: 4096, Off: 0, Len: 4096},
			{Pos: 4096, ID: 60, Size: 4096, Off: 0, Len: 4096},
		})
		mockQuerier.EXPECT().DeleteChunksByIno(mock.Anything, int64(childIno)).
			Return([][]byte{chunkBytes}, nil).Once()

		mockQuerier.EXPECT().AddSliceRefCount(mock.Anything, db.AddSliceRefCountParams{
			SliceID: 50,
			Delta:   -1,
		}).Return(nil).Once()

		mockQuerier.EXPECT().AddSliceRefCount(mock.Anything, db.AddSliceRefCountParams{
			SliceID: 60,
			Delta:   -1,
		}).Return(nil).Once()

		// 7. 物理删除 Inode 并退还配额
		mockQuerier.EXPECT().DeleteInode(mock.Anything, int64(childIno)).
			Return(nil).Once()

		mockQuerier.EXPECT().AddDiskQuotaByRootIno(mock.Anything, db.AddDiskQuotaByRootInoParams{
			RootIno:     rootIno,
			DeltaBytes:  -8192,
			DeltaInodes: -1,
		}).Return(nil).Once()

		err := mgr.Unlink(ctx, parentIno, name)
		require.NoError(t, err)
	})

	t.Run("多硬链接文件 (nlink == 2) Unlink 其一，nlink 降为 1，不清理 chunks 且不扣减 slice_refs", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		multiChildRow := childRow
		multiChildRow.Nlink = 2

		setupMockWriteTx(mockStore, mockQuerier)

		mockQuerier.EXPECT().GetEntry(mock.Anything, db.GetEntryParams{
			ParentIno: int64(parentIno),
			Name:      name,
		}).Return(entryRow, nil).Once()

		mockQuerier.EXPECT().GetInodeForUpdate(mock.Anything, int64(parentIno)).
			Return(parentRow, nil).Once()
		mockQuerier.EXPECT().GetInodeForUpdate(mock.Anything, int64(childIno)).
			Return(multiChildRow, nil).Once()

		mockQuerier.EXPECT().GetEntry(mock.Anything, db.GetEntryParams{
			ParentIno: int64(parentIno),
			Name:      name,
		}).Return(entryRow, nil).Once()

		mockQuerier.EXPECT().DeleteEntry(mock.Anything, db.DeleteEntryParams{
			ParentIno: int64(parentIno),
			Name:      name,
		}).Return(nil).Once()

		childDecremented := multiChildRow
		childDecremented.Nlink = 1
		mockQuerier.EXPECT().DecrementInodeNlinkAndTouchCtime(mock.Anything, int64(childIno)).
			Return(childDecremented, nil).Once()

		mockQuerier.EXPECT().TouchInodeTimestamps(mock.Anything, int64(parentIno)).
			Return(nil).Once()

		// 核心断言：nlink > 0 时绝不调用 HasFileHandlesByIno、DeleteChunksByIno、DeleteInode 与配额退还
		mockQuerier.AssertNotCalled(t, "DeleteChunksByIno", mock.Anything, mock.Anything)
		mockQuerier.AssertNotCalled(t, "DeleteInode", mock.Anything, mock.Anything)

		err := mgr.Unlink(ctx, parentIno, name)
		require.NoError(t, err)
	})

	t.Run("存在活跃句柄时 Unlink 常规文件 (unlink-whilst-open)，保留孤儿 Inode，不清理 chunks 与 slice_refs", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockWriteTx(mockStore, mockQuerier)

		mockQuerier.EXPECT().GetEntry(mock.Anything, db.GetEntryParams{
			ParentIno: int64(parentIno),
			Name:      name,
		}).Return(entryRow, nil).Once()

		mockQuerier.EXPECT().GetInodeForUpdate(mock.Anything, int64(parentIno)).
			Return(parentRow, nil).Once()
		mockQuerier.EXPECT().GetInodeForUpdate(mock.Anything, int64(childIno)).
			Return(childRow, nil).Once()

		mockQuerier.EXPECT().GetEntry(mock.Anything, db.GetEntryParams{
			ParentIno: int64(parentIno),
			Name:      name,
		}).Return(entryRow, nil).Once()

		mockQuerier.EXPECT().DeleteEntry(mock.Anything, db.DeleteEntryParams{
			ParentIno: int64(parentIno),
			Name:      name,
		}).Return(nil).Once()

		childDecremented := childRow
		childDecremented.Nlink = 0
		mockQuerier.EXPECT().DecrementInodeNlinkAndTouchCtime(mock.Anything, int64(childIno)).
			Return(childDecremented, nil).Once()

		mockQuerier.EXPECT().TouchInodeTimestamps(mock.Anything, int64(parentIno)).
			Return(nil).Once()

		// 检查活跃句柄: 返回存在活跃句柄
		mockQuerier.EXPECT().HasFileHandlesByIno(mock.Anything, int64(childIno)).
			Return(true, nil).Once()

		// 核心断言：存在活跃句柄时绝不清理 chunks 和 Inode
		mockQuerier.AssertNotCalled(t, "DeleteChunksByIno", mock.Anything, mock.Anything)
		mockQuerier.AssertNotCalled(t, "DeleteInode", mock.Anything, mock.Anything)

		err := mgr.Unlink(ctx, parentIno, name)
		require.NoError(t, err)
	})

	t.Run("DeleteChunksByIno 底层失败时透传错误并中止事务", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockWriteTx(mockStore, mockQuerier)

		mockQuerier.EXPECT().GetEntry(mock.Anything, db.GetEntryParams{
			ParentIno: int64(parentIno),
			Name:      name,
		}).Return(entryRow, nil).Once()

		mockQuerier.EXPECT().GetInodeForUpdate(mock.Anything, int64(parentIno)).
			Return(parentRow, nil).Once()
		mockQuerier.EXPECT().GetInodeForUpdate(mock.Anything, int64(childIno)).
			Return(childRow, nil).Once()

		mockQuerier.EXPECT().GetEntry(mock.Anything, db.GetEntryParams{
			ParentIno: int64(parentIno),
			Name:      name,
		}).Return(entryRow, nil).Once()

		mockQuerier.EXPECT().DeleteEntry(mock.Anything, db.DeleteEntryParams{
			ParentIno: int64(parentIno),
			Name:      name,
		}).Return(nil).Once()

		childDecremented := childRow
		childDecremented.Nlink = 0
		mockQuerier.EXPECT().DecrementInodeNlinkAndTouchCtime(mock.Anything, int64(childIno)).
			Return(childDecremented, nil).Once()

		mockQuerier.EXPECT().TouchInodeTimestamps(mock.Anything, int64(parentIno)).
			Return(nil).Once()

		mockQuerier.EXPECT().HasFileHandlesByIno(mock.Anything, int64(childIno)).
			Return(false, nil).Once()

		delChunkErr := errors.New("db delete chunks fail")
		mockQuerier.EXPECT().DeleteChunksByIno(mock.Anything, int64(childIno)).
			Return(nil, delChunkErr).Once()

		err := mgr.Unlink(ctx, parentIno, name)
		require.Error(t, err)
		assert.ErrorIs(t, err, delChunkErr)
	})
}

// TestRename_ChunksCleanup_Mock 验证在 Rename 覆盖已存在的常规文件时清除目标文件 chunks 并扣减 slice_refs 的事务内行为（Mock 单元测试）。
func TestRename_ChunksCleanup_Mock(t *testing.T) {
	ctx := context.Background()
	now := time.Now().Truncate(time.Microsecond)
	oldParentIno := uint64(1001)
	newParentIno := uint64(1002)
	srcIno := uint64(2001)
	targetIno := uint64(3001)
	rootIno := int64(100)

	oldName := []byte("src.txt")
	newName := []byte("dst.txt")

	parentRow1 := db.Inode{Ino: int64(oldParentIno), Mode: int32(meta.S_IFDIR | 0o755), Nlink: 2, Uid: 1000, Gid: 1000, RootIno: rootIno}
	parentRow2 := db.Inode{Ino: int64(newParentIno), Mode: int32(meta.S_IFDIR | 0o755), Nlink: 2, Uid: 1000, Gid: 1000, RootIno: rootIno}

	srcRow := db.Inode{
		Ino:        int64(srcIno),
		Generation: 1,
		Mode:       int32(meta.S_IFREG | 0o644),
		Size:       4096,
		Nlink:      1,
		Uid:        1000,
		Gid:        1000,
		RootIno:    rootIno,
		Atime:      pgtype.Timestamptz{Time: now, Valid: true},
		Mtime:      pgtype.Timestamptz{Time: now, Valid: true},
		Ctime:      pgtype.Timestamptz{Time: now, Valid: true},
	}

	targetRow := db.Inode{
		Ino:        int64(targetIno),
		Generation: 1,
		Mode:       int32(meta.S_IFREG | 0o644),
		Size:       8192,
		Nlink:      1,
		Uid:        1000,
		Gid:        1000,
		RootIno:    rootIno,
		Atime:      pgtype.Timestamptz{Time: now, Valid: true},
		Mtime:      pgtype.Timestamptz{Time: now, Valid: true},
		Ctime:      pgtype.Timestamptz{Time: now, Valid: true},
	}

	srcEntry := db.Entry{ParentIno: int64(oldParentIno), Name: oldName, Ino: int64(srcIno)}
	targetEntry := db.Entry{ParentIno: int64(newParentIno), Name: newName, Ino: int64(targetIno)}

	t.Run("无打开句柄时 Rename 覆盖常规文件 (目标 nlink == 0)，立即清理目标文件 chunks 与 slice_refs，源文件 chunks 保留", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockWriteTx(mockStore, mockQuerier)

		// 预读目标项
		mockQuerier.EXPECT().GetEntry(mock.Anything, db.GetEntryParams{ParentIno: int64(oldParentIno), Name: oldName}).
			Return(srcEntry, nil).Once()
		mockQuerier.EXPECT().GetEntry(mock.Anything, db.GetEntryParams{ParentIno: int64(newParentIno), Name: newName}).
			Return(targetEntry, nil).Once()

		// 升序加锁: 1001, 1002, 2001, 3001
		mockQuerier.EXPECT().GetInodeForUpdate(mock.Anything, int64(oldParentIno)).Return(parentRow1, nil).Once()
		mockQuerier.EXPECT().GetInodeForUpdate(mock.Anything, int64(newParentIno)).Return(parentRow2, nil).Once()
		mockQuerier.EXPECT().GetInodeForUpdate(mock.Anything, int64(srcIno)).Return(srcRow, nil).Once()
		mockQuerier.EXPECT().GetInodeForUpdate(mock.Anything, int64(targetIno)).Return(targetRow, nil).Once()

		// 二次校验
		mockQuerier.EXPECT().GetEntry(mock.Anything, db.GetEntryParams{ParentIno: int64(oldParentIno), Name: oldName}).
			Return(srcEntry, nil).Once()
		mockQuerier.EXPECT().GetEntry(mock.Anything, db.GetEntryParams{ParentIno: int64(newParentIno), Name: newName}).
			Return(targetEntry, nil).Once()

		// 覆盖旧目标项
		mockQuerier.EXPECT().DeleteEntry(mock.Anything, db.DeleteEntryParams{ParentIno: int64(newParentIno), Name: newName}).
			Return(nil).Once()

		targetDecremented := targetRow
		targetDecremented.Nlink = 0
		mockQuerier.EXPECT().DecrementInodeNlinkAndTouchCtime(mock.Anything, int64(targetIno)).
			Return(targetDecremented, nil).Once()

		mockQuerier.EXPECT().HasFileHandlesByIno(mock.Anything, int64(targetIno)).
			Return(false, nil).Once()

		// 核心契约：必须清理目标文件的 chunks，绝不清理源文件的 chunks
		targetChunkBytes := meta.EncodeSlices([]meta.Slice{
			{Pos: 0, ID: 77, Size: 8192, Off: 0, Len: 8192},
		})
		mockQuerier.EXPECT().DeleteChunksByIno(mock.Anything, int64(targetIno)).
			Return([][]byte{targetChunkBytes}, nil).Once()

		mockQuerier.EXPECT().AddSliceRefCount(mock.Anything, db.AddSliceRefCountParams{
			SliceID: 77,
			Delta:   -1,
		}).Return(nil).Once()

		mockQuerier.EXPECT().DeleteInode(mock.Anything, int64(targetIno)).
			Return(nil).Once()

		mockQuerier.EXPECT().AddDiskQuotaByRootIno(mock.Anything, db.AddDiskQuotaByRootInoParams{
			RootIno:     rootIno,
			DeltaBytes:  -8192,
			DeltaInodes: -1,
		}).Return(nil).Once()

		// 更新源目录项至目标位置
		mockQuerier.EXPECT().UpdateEntry(mock.Anything, db.UpdateEntryParams{
			OldParentIno: int64(oldParentIno),
			OldName:      oldName,
			NewParentIno: int64(newParentIno),
			NewName:      newName,
		}).Return(nil).Once()

		mockQuerier.EXPECT().TouchInodeCtime(mock.Anything, int64(srcIno)).Return(nil).Once()
		mockQuerier.EXPECT().TouchInodeTimestamps(mock.Anything, int64(oldParentIno)).Return(nil).Once()
		mockQuerier.EXPECT().TouchInodeTimestamps(mock.Anything, int64(newParentIno)).Return(nil).Once()

		// 确保未对源文件执行 DeleteChunksByIno
		mockQuerier.AssertNotCalled(t, "DeleteChunksByIno", mock.Anything, int64(srcIno))

		err := mgr.Rename(ctx, meta.RenameParams{
			OldParentIno: oldParentIno,
			OldName:      oldName,
			NewParentIno: newParentIno,
			NewName:      newName,
		})
		require.NoError(t, err)
	})

	t.Run("目标常规文件存在活跃句柄时 Rename 覆盖 (rename-whilst-open)，不清理目标 chunks 与 Inode", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockWriteTx(mockStore, mockQuerier)

		mockQuerier.EXPECT().GetEntry(mock.Anything, db.GetEntryParams{ParentIno: int64(oldParentIno), Name: oldName}).
			Return(srcEntry, nil).Once()
		mockQuerier.EXPECT().GetEntry(mock.Anything, db.GetEntryParams{ParentIno: int64(newParentIno), Name: newName}).
			Return(targetEntry, nil).Once()

		mockQuerier.EXPECT().GetInodeForUpdate(mock.Anything, int64(oldParentIno)).Return(parentRow1, nil).Once()
		mockQuerier.EXPECT().GetInodeForUpdate(mock.Anything, int64(newParentIno)).Return(parentRow2, nil).Once()
		mockQuerier.EXPECT().GetInodeForUpdate(mock.Anything, int64(srcIno)).Return(srcRow, nil).Once()
		mockQuerier.EXPECT().GetInodeForUpdate(mock.Anything, int64(targetIno)).Return(targetRow, nil).Once()

		mockQuerier.EXPECT().GetEntry(mock.Anything, db.GetEntryParams{ParentIno: int64(oldParentIno), Name: oldName}).
			Return(srcEntry, nil).Once()
		mockQuerier.EXPECT().GetEntry(mock.Anything, db.GetEntryParams{ParentIno: int64(newParentIno), Name: newName}).
			Return(targetEntry, nil).Once()

		mockQuerier.EXPECT().DeleteEntry(mock.Anything, db.DeleteEntryParams{ParentIno: int64(newParentIno), Name: newName}).
			Return(nil).Once()

		targetDecremented := targetRow
		targetDecremented.Nlink = 0
		mockQuerier.EXPECT().DecrementInodeNlinkAndTouchCtime(mock.Anything, int64(targetIno)).
			Return(targetDecremented, nil).Once()

		// 检查活跃句柄: 返回存在活跃句柄
		mockQuerier.EXPECT().HasFileHandlesByIno(mock.Anything, int64(targetIno)).
			Return(true, nil).Once()

		// 更新目录项
		mockQuerier.EXPECT().UpdateEntry(mock.Anything, db.UpdateEntryParams{
			OldParentIno: int64(oldParentIno),
			OldName:      oldName,
			NewParentIno: int64(newParentIno),
			NewName:      newName,
		}).Return(nil).Once()

		mockQuerier.EXPECT().TouchInodeCtime(mock.Anything, int64(srcIno)).Return(nil).Once()
		mockQuerier.EXPECT().TouchInodeTimestamps(mock.Anything, int64(oldParentIno)).Return(nil).Once()
		mockQuerier.EXPECT().TouchInodeTimestamps(mock.Anything, int64(newParentIno)).Return(nil).Once()

		// 核心断言：绝不删除目标与源的 chunks 和 Inode
		mockQuerier.AssertNotCalled(t, "DeleteChunksByIno", mock.Anything, mock.Anything)
		mockQuerier.AssertNotCalled(t, "DeleteInode", mock.Anything, mock.Anything)

		err := mgr.Rename(ctx, meta.RenameParams{
			OldParentIno: oldParentIno,
			OldName:      oldName,
			NewParentIno: newParentIno,
			NewName:      newName,
		})
		require.NoError(t, err)
	})
}

// TestUnlink_Rename_Chunks_Integration 真实 PostgreSQL 与本地 BlockStore 集成测试
func TestUnlink_Rename_Chunks_Integration(t *testing.T) {
	pool, ctx := setupMetaTestDB(t)
	if pool == nil {
		return
	}
	acquireTestSliceGCAdvisoryLock(t, pool)

	store := db.NewStore(pool)
	dataDir := t.TempDir()
	bs := meta.NewLocalFSBlockStore(dataDir)
	mgr := meta.New(store, meta.WithBlockStore(bs))

	diskName := fmt.Sprintf("test_unlink_rename_%d", time.Now().UnixNano())
	disk, err := mgr.CreateDisk(ctx, meta.CreateDiskParams{
		Name:     diskName,
		MaxBytes: 10 * 1024 * 1024 * 1024,
	})
	require.NoError(t, err)
	rootIno := disk.RootIno
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		rows, qErr := pool.Query(cleanupCtx, "SELECT ino FROM entries WHERE parent_ino = $1", rootIno)
		if qErr == nil {
			var childInos []int64
			for rows.Next() {
				var cIno int64
				if scanErr := rows.Scan(&cIno); scanErr == nil {
					childInos = append(childInos, cIno)
				}
			}
			rows.Close()

			for _, cIno := range childInos {
				for indx := int64(0); indx < 16; indx++ {
					cRow, _ := store.GetChunk(cleanupCtx, db.GetChunkParams{Ino: cIno, Indx: indx})
					if len(cRow.Slices) > 0 {
						if sls, dErr := meta.DecodeSlices(cRow.Slices); dErr == nil {
							for _, s := range sls {
								if s.ID > 0 {
									_ = store.DeleteSliceRef(cleanupCtx, int64(s.ID))
								}
							}
						}
					}
				}
				_ = meta.DeleteAllChunksByInos(cleanupCtx, store, []int64{cIno})
				_ = store.DeleteInode(cleanupCtx, cIno)
			}
		}

		_ = mgr.DeleteDisk(cleanupCtx, disk.DiskID)

		var diskCount int
		_ = pool.QueryRow(cleanupCtx, "SELECT count(*) FROM disks WHERE disk_id = $1", disk.DiskID).Scan(&diskCount)
		assert.Equal(t, 0, diskCount, "测试磁盘必须已清除")

		_, rootErr := store.GetInode(cleanupCtx, int64(rootIno))
		assert.ErrorIs(t, rootErr, pgx.ErrNoRows, "rootIno 必须已清除")
	})

	t.Run("无句柄 Unlink 常规文件: 物理删除 Inode 并清空 chunks 行及扣减 slice_refs", func(t *testing.T) {
		name := []byte("unlink_clean.txt")
		created, err := mgr.Create(ctx, meta.CreateParams{
			ParentIno: rootIno,
			Name:      name,
			Mode:      0o644,
			UID:       1000,
			GID:       1000,
			Flags:     meta.O_RDWR,
		})
		require.NoError(t, err)
		ino := created.Inode.Ino
		fh := created.Fh

		payload := []byte("test-unlink-cleanup-payload-12345678")
		writeRes, err := mgr.Write(ctx, &meta.WriteRequest{
			Ino:    ino,
			Fh:     fh,
			Offset: 0,
			Data:   payload,
		})
		require.NoError(t, err)
		assert.Equal(t, uint32(len(payload)), writeRes.Size)

		// 检查此时数据库中存在 chunks 行
		chunkRow, err := store.GetChunk(ctx, db.GetChunkParams{Ino: int64(ino), Indx: 0})
		require.NoError(t, err)
		require.NotEmpty(t, chunkRow.Slices)

		slicesList, err := meta.DecodeSlices(chunkRow.Slices)
		require.NoError(t, err)
		require.NotEmpty(t, slicesList)
		sliceID := slicesList[0].ID

		// 检查 slice_refs 引用计数为 1
		refRow, err := store.GetSliceRef(ctx, int64(sliceID))
		require.NoError(t, err)
		assert.Equal(t, int32(1), refRow.Refs)

		// 关闭句柄使活跃句柄为 0
		err = mgr.Release(ctx, meta.ReleaseParams{Ino: ino, Fh: fh})
		require.NoError(t, err)

		// 执行 Unlink
		err = mgr.Unlink(ctx, rootIno, name)
		require.NoError(t, err)

		// 断言 1: Inode 已被物理删除
		_, err = store.GetInode(ctx, int64(ino))
		assert.ErrorIs(t, err, pgx.ErrNoRows, "无句柄常规文件 Unlink 后 Inode 必须被物理清理")

		// 断言 2: chunks 表中该 Inode 的记录已被物理删除
		_, err = store.GetChunk(ctx, db.GetChunkParams{Ino: int64(ino), Indx: 0})
		assert.ErrorIs(t, err, pgx.ErrNoRows, "无句柄常规文件 Unlink 后 chunks 表记录必须被物理清理")

		// 断言 3: slice_refs 计数已被原子扣减为 0
		refRowAfter, err := store.GetSliceRef(ctx, int64(sliceID))
		require.NoError(t, err)
		assert.Equal(t, int32(0), refRowAfter.Refs, "关联的 slice_refs.refs 必须扣减为 0")
	})

	t.Run("无句柄 Rename 覆盖目标常规文件: 立即清理目标 chunks 并扣减 slice_refs，源文件 chunks 完好保留", func(t *testing.T) {
		srcName := []byte("src_rename.txt")
		dstName := []byte("dst_rename.txt")

		// 创建源文件
		srcCreated, err := mgr.Create(ctx, meta.CreateParams{
			ParentIno: rootIno,
			Name:      srcName,
			Mode:      0o644,
			UID:       1000,
			GID:       1000,
			Flags:     meta.O_RDWR,
		})
		require.NoError(t, err)
		srcIno := srcCreated.Inode.Ino
		srcFh := srcCreated.Fh

		srcData := []byte("source-file-persistent-data")
		_, err = mgr.Write(ctx, &meta.WriteRequest{
			Ino:    srcIno,
			Fh:     srcFh,
			Offset: 0,
			Data:   srcData,
		})
		require.NoError(t, err)

		// 创建目标文件
		dstCreated, err := mgr.Create(ctx, meta.CreateParams{
			ParentIno: rootIno,
			Name:      dstName,
			Mode:      0o644,
			UID:       1000,
			GID:       1000,
			Flags:     meta.O_RDWR,
		})
		require.NoError(t, err)
		dstIno := dstCreated.Inode.Ino
		dstFh := dstCreated.Fh

		dstData := []byte("destination-file-to-be-overwritten")
		_, err = mgr.Write(ctx, &meta.WriteRequest{
			Ino:    dstIno,
			Fh:     dstFh,
			Offset: 0,
			Data:   dstData,
		})
		require.NoError(t, err)

		// 获取 dst 的 slice_id
		dstChunk, err := store.GetChunk(ctx, db.GetChunkParams{Ino: int64(dstIno), Indx: 0})
		require.NoError(t, err)
		dstSlices, err := meta.DecodeSlices(dstChunk.Slices)
		require.NoError(t, err)
		dstSliceID := dstSlices[0].ID

		// 关闭两者的活跃句柄
		require.NoError(t, mgr.Release(ctx, meta.ReleaseParams{Ino: srcIno, Fh: srcFh}))
		require.NoError(t, mgr.Release(ctx, meta.ReleaseParams{Ino: dstIno, Fh: dstFh}))

		// 执行 Rename 覆盖
		err = mgr.Rename(ctx, meta.RenameParams{
			OldParentIno: rootIno,
			OldName:      srcName,
			NewParentIno: rootIno,
			NewName:      dstName,
		})
		require.NoError(t, err)

		// 断言 1: 被覆盖的目标 Inode 已被物理删除
		_, err = store.GetInode(ctx, int64(dstIno))
		assert.ErrorIs(t, err, pgx.ErrNoRows, "被覆盖的目标文件 Inode 必须被删除")

		// 断言 2: 被覆盖的目标文件 chunks 已被物理删除
		_, err = store.GetChunk(ctx, db.GetChunkParams{Ino: int64(dstIno), Indx: 0})
		assert.ErrorIs(t, err, pgx.ErrNoRows, "被覆盖的目标文件 chunks 必须被删除")

		// 断言 3: 被覆盖的目标 slice_refs 引用计数扣减为 0
		refRowAfter, err := store.GetSliceRef(ctx, int64(dstSliceID))
		require.NoError(t, err)
		assert.Equal(t, int32(0), refRowAfter.Refs)

		// 断言 4: 源文件的 chunks 完好无损，且打开新名称读取数据完全一致
		openRes, err := mgr.Open(ctx, meta.OpenParams{Ino: srcIno, Flags: meta.O_RDONLY})
		require.NoError(t, err)
		defer func() { _ = mgr.Release(ctx, meta.ReleaseParams{Ino: srcIno, Fh: openRes.Fh}) }()

		readRes, err := mgr.Read(ctx, &meta.ReadRequest{
			Ino:    srcIno,
			Fh:     openRes.Fh,
			Offset: 0,
			Size:   uint32(len(srcData)),
		})
		require.NoError(t, err)
		assert.True(t, bytes.Equal(srcData, readRes.Data))
	})

	t.Run("多硬链接文件 (nlink > 1) Unlink 其一: chunks 与 slice_refs 完整保留且仍可正常 Read", func(t *testing.T) {
		origName := []byte("orig_link.txt")
		hardlinkName := []byte("hardlink.txt")

		created, err := mgr.Create(ctx, meta.CreateParams{
			ParentIno: rootIno,
			Name:      origName,
			Mode:      0o644,
			UID:       1000,
			GID:       1000,
			Flags:     meta.O_RDWR,
		})
		require.NoError(t, err)
		ino := created.Inode.Ino
		fh := created.Fh

		payload := []byte("shared-hardlink-bytes-data")
		_, err = mgr.Write(ctx, &meta.WriteRequest{
			Ino:    ino,
			Fh:     fh,
			Offset: 0,
			Data:   payload,
		})
		require.NoError(t, err)

		// 创建硬链接
		linkInode, err := mgr.Link(ctx, meta.LinkParams{
			Ino:          ino,
			NewParentIno: rootIno,
			NewName:      hardlinkName,
		})
		require.NoError(t, err)
		assert.Equal(t, uint32(2), linkInode.Attr.Nlink)

		// 关闭当前句柄
		require.NoError(t, mgr.Release(ctx, meta.ReleaseParams{Ino: ino, Fh: fh}))

		// Unlink 原始名称 origName
		err = mgr.Unlink(ctx, rootIno, origName)
		require.NoError(t, err)

		// 断言 1: Inode 依然存在，nlink 变为 1
		inodeRow, err := store.GetInode(ctx, int64(ino))
		require.NoError(t, err)
		assert.Equal(t, int32(1), inodeRow.Nlink)

		// 断言 2: chunks 记录依然完整存在
		chunkRow, err := store.GetChunk(ctx, db.GetChunkParams{Ino: int64(ino), Indx: 0})
		require.NoError(t, err)
		require.NotEmpty(t, chunkRow.Slices)

		// 断言 3: 打开 hardlink 读取，数据逐字节完全匹配
		openRes, err := mgr.Open(ctx, meta.OpenParams{Ino: ino, Flags: meta.O_RDONLY})
		require.NoError(t, err)
		defer func() { _ = mgr.Release(ctx, meta.ReleaseParams{Ino: ino, Fh: openRes.Fh}) }()

		readRes, err := mgr.Read(ctx, &meta.ReadRequest{
			Ino:    ino,
			Fh:     openRes.Fh,
			Offset: 0,
			Size:   uint32(len(payload)),
		})
		require.NoError(t, err)
		assert.True(t, bytes.Equal(payload, readRes.Data))
	})

	t.Run("打开句柄后执行 Unlink 转为 Orphan Inode，期间持续 Write 追加/覆写和 Read 读取，数据完全一致", func(t *testing.T) {
		name := []byte("orphan_read_write.txt")
		created, err := mgr.Create(ctx, meta.CreateParams{
			ParentIno: rootIno,
			Name:      name,
			Mode:      0o644,
			UID:       1000,
			GID:       1000,
			Flags:     meta.O_RDWR,
		})
		require.NoError(t, err)
		ino := created.Inode.Ino
		fh := created.Fh
		defer func() {
			_ = mgr.Release(context.Background(), meta.ReleaseParams{Ino: ino, Fh: fh})
		}()

		part1 := []byte("hello world first block data; ")
		_, err = mgr.Write(ctx, &meta.WriteRequest{
			Ino:    ino,
			Fh:     fh,
			Offset: 0,
			Data:   part1,
		})
		require.NoError(t, err)

		// 在句柄打开期间执行 Unlink (unlink-whilst-open)
		err = mgr.Unlink(ctx, rootIno, name)
		require.NoError(t, err)

		// 验证目录项已被删除
		_, err = mgr.Lookup(ctx, rootIno, name)
		assert.ErrorIs(t, err, meta.ErrNotFound, "Unlink 后 Lookup 必须返回 NotFound")

		// 验证孤儿 Inode 与 chunks 依然保留
		inodeRow, err := store.GetInode(ctx, int64(ino))
		require.NoError(t, err)
		assert.Equal(t, int32(0), inodeRow.Nlink, "转为孤儿 Inode (nlink == 0)")

		_, err = store.GetChunk(ctx, db.GetChunkParams{Ino: int64(ino), Indx: 0})
		require.NoError(t, err, "孤儿文件 chunks 行必须完整保留")

		// 核心契约：通过已打开句柄继续 Write 追加
		part2 := []byte("second block data after unlink!")
		_, err = mgr.Write(ctx, &meta.WriteRequest{
			Ino:    ino,
			Fh:     fh,
			Offset: uint64(len(part1)),
			Data:   part2,
		})
		require.NoError(t, err, "Orphan Inode 必须支持通过已打开句柄继续写入")

		// 核心契约：通过已打开句柄继续 Read 读取全量内容
		expectedTotal := append(part1, part2...)
		readRes, err := mgr.Read(ctx, &meta.ReadRequest{
			Ino:    ino,
			Fh:     fh,
			Offset: 0,
			Size:   uint32(len(expectedTotal)),
		})
		require.NoError(t, err, "Orphan Inode 必须支持通过已打开句柄继续读取")
		assert.True(t, bytes.Equal(expectedTotal, readRes.Data), "读取的数据必须逐字节匹配全量拼接数据")
	})

	t.Run("打开句柄后被 Rename 覆盖转为 Orphan Inode，期间持续 Write 追加/覆写和 Read 读取，数据完全一致", func(t *testing.T) {
		targetName := []byte("target_overwrite.txt")
		srcName := []byte("source_winner.txt")

		// 1. 创建目标文件并保持句柄打开
		targetCreated, err := mgr.Create(ctx, meta.CreateParams{
			ParentIno: rootIno,
			Name:      targetName,
			Mode:      0o644,
			UID:       1000,
			GID:       1000,
			Flags:     meta.O_RDWR,
		})
		require.NoError(t, err)
		targetIno := targetCreated.Inode.Ino
		targetFh := targetCreated.Fh
		defer func() {
			_ = mgr.Release(context.Background(), meta.ReleaseParams{Ino: targetIno, Fh: targetFh})
		}()

		targetPart1 := []byte("target original contents before rename overwrite; ")
		_, err = mgr.Write(ctx, &meta.WriteRequest{
			Ino:    targetIno,
			Fh:     targetFh,
			Offset: 0,
			Data:   targetPart1,
		})
		require.NoError(t, err)

		// 2. 创建源文件并写入
		srcCreated, err := mgr.Create(ctx, meta.CreateParams{
			ParentIno: rootIno,
			Name:      srcName,
			Mode:      0o644,
			UID:       1000,
			GID:       1000,
			Flags:     meta.O_RDWR,
		})
		require.NoError(t, err)
		srcIno := srcCreated.Inode.Ino
		srcFh := srcCreated.Fh
		defer func() {
			_ = mgr.Release(context.Background(), meta.ReleaseParams{Ino: srcIno, Fh: srcFh})
		}()

		srcData := []byte("source winner file contents")
		_, err = mgr.Write(ctx, &meta.WriteRequest{
			Ino:    srcIno,
			Fh:     srcFh,
			Offset: 0,
			Data:   srcData,
		})
		require.NoError(t, err)

		// 3. 在 targetFh 依然打开的情况下，执行 Rename 覆盖
		err = mgr.Rename(ctx, meta.RenameParams{
			OldParentIno: rootIno,
			OldName:      srcName,
			NewParentIno: rootIno,
			NewName:      targetName,
		})
		require.NoError(t, err)

		// 验证 target 变为孤儿 (nlink == 0)
		targetRow, err := store.GetInode(ctx, int64(targetIno))
		require.NoError(t, err)
		assert.Equal(t, int32(0), targetRow.Nlink)

		// 4. 目标文件转为孤儿后，继续通过 targetFh 进行 Write 追加
		targetPart2 := []byte("target appended contents after rename overwrite!")
		_, err = mgr.Write(ctx, &meta.WriteRequest{
			Ino:    targetIno,
			Fh:     targetFh,
			Offset: uint64(len(targetPart1)),
			Data:   targetPart2,
		})
		require.NoError(t, err)

		// 5. 目标文件转为孤儿后，继续通过 targetFh 进行 Read 读取
		expectedTargetTotal := append(targetPart1, targetPart2...)
		readResTarget, err := mgr.Read(ctx, &meta.ReadRequest{
			Ino:    targetIno,
			Fh:     targetFh,
			Offset: 0,
			Size:   uint32(len(expectedTargetTotal)),
		})
		require.NoError(t, err)
		assert.True(t, bytes.Equal(expectedTargetTotal, readResTarget.Data))

		// 6. 验证源文件通过 targetName 读取到的依然是源文件内容
		lookupRes, err := mgr.Lookup(ctx, rootIno, targetName)
		require.NoError(t, err)
		assert.Equal(t, srcIno, lookupRes.Ino)

		readResSrc, err := mgr.Read(ctx, &meta.ReadRequest{
			Ino:    srcIno,
			Fh:     srcFh,
			Offset: 0,
			Size:   uint32(len(srcData)),
		})
		require.NoError(t, err)
		assert.True(t, bytes.Equal(srcData, readResSrc.Data))
	})
}
