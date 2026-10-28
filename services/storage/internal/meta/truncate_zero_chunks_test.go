package meta_test

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/starsrc/bowl/services/storage/internal/db"
	dbmock "github.com/starsrc/bowl/services/storage/internal/db/mock"
	"github.com/starsrc/bowl/services/storage/internal/meta"
	metamock "github.com/starsrc/bowl/services/storage/internal/meta/mock"
)

// TestDecrementDeletedSlicesRefs 验证事务内按升序排序 slice_id 并累加扣减物理切片引用计数的公共辅助逻辑。
func TestDecrementDeletedSlicesRefs(t *testing.T) {
	ctx := context.Background()

	t.Run("空切片输入直接返回 nil 且不触发 Querier 操作", func(t *testing.T) {
		mockQuerier := dbmock.NewMockQuerier(t)
		err := meta.DecrementDeletedSlicesRefs(ctx, mockQuerier)
		require.NoError(t, err)

		err = meta.DecrementDeletedSlicesRefs(ctx, mockQuerier, []byte{})
		require.NoError(t, err)
	})

	t.Run("仅包含空洞切片 (id == 0) 不触发 AddSliceRefCount", func(t *testing.T) {
		mockQuerier := dbmock.NewMockQuerier(t)
		holeSlice := meta.EncodeSlice(meta.Slice{
			Pos:  0,
			ID:   0,
			Size: 0,
			Off:  0,
			Len:  4096,
		})
		err := meta.DecrementDeletedSlicesRefs(ctx, mockQuerier, holeSlice)
		require.NoError(t, err)
		mockQuerier.AssertNotCalled(t, "AddSliceRefCount", mock.Anything, mock.Anything)
	})

	t.Run("非 24 字节整数倍的损坏数据直接返回 ErrInvalidSliceBytes", func(t *testing.T) {
		mockQuerier := dbmock.NewMockQuerier(t)
		err := meta.DecrementDeletedSlicesRefs(ctx, mockQuerier, []byte{1, 2, 3})
		require.Error(t, err)
		require.ErrorIs(t, err, meta.ErrInvalidSliceBytes)
	})

	t.Run("同一 slice_id 出现多次且跨多行 chunks 时累加频次并按 slice_id 升序执行原子扣减", func(t *testing.T) {
		mockQuerier := dbmock.NewMockQuerier(t)

		// 模拟 3 行 chunks 的 raw slices:
		// row1: slice_id=20 (出现 1 次), slice_id=10 (出现 1 次)
		// row2: slice_id=5 (出现 2 次), slice_id=0 (空洞切片)
		// row3: slice_id=10 (出现 2 次)
		// 汇总频次:
		// slice 5 -> 2 次 (Delta: -2)
		// slice 10 -> 3 次 (Delta: -3)
		// slice 20 -> 1 次 (Delta: -1)
		// 死锁防护断言：遍历更新顺序必须严格单调升序: 5 -> 10 -> 20
		row1 := meta.EncodeSlices([]meta.Slice{
			{Pos: 0, ID: 20, Size: 4096, Off: 0, Len: 1024},
			{Pos: 1024, ID: 10, Size: 4096, Off: 0, Len: 1024},
		})
		row2 := meta.EncodeSlices([]meta.Slice{
			{Pos: 2048, ID: 5, Size: 4096, Off: 0, Len: 512},
			{Pos: 2560, ID: 5, Size: 4096, Off: 512, Len: 512},
			{Pos: 3072, ID: 0, Size: 0, Off: 0, Len: 1024},
		})
		row3 := meta.EncodeSlices([]meta.Slice{
			{Pos: 0, ID: 10, Size: 4096, Off: 1024, Len: 512},
			{Pos: 512, ID: 10, Size: 4096, Off: 1536, Len: 512},
		})

		var callOrder []int64

		mockQuerier.EXPECT().
			AddSliceRefCount(mock.Anything, db.AddSliceRefCountParams{
				SliceID: 5,
				Delta:   -2,
			}).
			RunAndReturn(func(ctx context.Context, arg db.AddSliceRefCountParams) error {
				callOrder = append(callOrder, arg.SliceID)
				return nil
			}).
			Once()

		mockQuerier.EXPECT().
			AddSliceRefCount(mock.Anything, db.AddSliceRefCountParams{
				SliceID: 10,
				Delta:   -3,
			}).
			RunAndReturn(func(ctx context.Context, arg db.AddSliceRefCountParams) error {
				callOrder = append(callOrder, arg.SliceID)
				return nil
			}).
			Once()

		mockQuerier.EXPECT().
			AddSliceRefCount(mock.Anything, db.AddSliceRefCountParams{
				SliceID: 20,
				Delta:   -1,
			}).
			RunAndReturn(func(ctx context.Context, arg db.AddSliceRefCountParams) error {
				callOrder = append(callOrder, arg.SliceID)
				return nil
			}).
			Once()

		err := meta.DecrementDeletedSlicesRefs(ctx, mockQuerier, row1, row2, row3)
		require.NoError(t, err)

		// 严格断言升序调用顺序防死锁
		assert.Equal(t, []int64{5, 10, 20}, callOrder)
	})

	t.Run("底层 AddSliceRefCount 返回错误时向上透传包装错误并中断后续调用", func(t *testing.T) {
		mockQuerier := dbmock.NewMockQuerier(t)
		row := meta.EncodeSlices([]meta.Slice{
			{Pos: 0, ID: 5, Size: 4096, Off: 0, Len: 1024},
			{Pos: 1024, ID: 10, Size: 4096, Off: 0, Len: 1024},
		})

		dbErr := errors.New("db error")
		mockQuerier.EXPECT().
			AddSliceRefCount(mock.Anything, db.AddSliceRefCountParams{
				SliceID: 5,
				Delta:   -1,
			}).
			Return(dbErr).
			Once()

		err := meta.DecrementDeletedSlicesRefs(ctx, mockQuerier, row)
		require.Error(t, err)
		assert.True(t, errors.Is(err, dbErr))
		mockQuerier.AssertNotCalled(t, "AddSliceRefCount", mock.Anything, db.AddSliceRefCountParams{
			SliceID: 10,
			Delta:   -1,
		})
	})
}

// TestDeleteAllChunksByIno 验证根据 Ino 全量删除 chunks 表记录并触发引用扣减。
func TestDeleteAllChunksByIno(t *testing.T) {
	ctx := context.Background()
	ino := int64(12345)

	t.Run("该 Inode 无 chunks 行时优雅返回 nil", func(t *testing.T) {
		mockQuerier := dbmock.NewMockQuerier(t)
		mockQuerier.EXPECT().
			DeleteChunksByIno(mock.Anything, ino).
			Return(nil, nil).
			Once()

		err := meta.DeleteAllChunksByIno(ctx, mockQuerier, ino)
		require.NoError(t, err)
		mockQuerier.AssertNotCalled(t, "AddSliceRefCount", mock.Anything, mock.Anything)
	})

	t.Run("DeleteChunksByIno 返回错误时向上透传", func(t *testing.T) {
		mockQuerier := dbmock.NewMockQuerier(t)
		dbErr := errors.New("delete chunks failed")
		mockQuerier.EXPECT().
			DeleteChunksByIno(mock.Anything, ino).
			Return(nil, dbErr).
			Once()

		err := meta.DeleteAllChunksByIno(ctx, mockQuerier, ino)
		require.Error(t, err)
		assert.True(t, errors.Is(err, dbErr))
	})

	t.Run("DeleteChunksByIno 成功后级联递减引用计数", func(t *testing.T) {
		mockQuerier := dbmock.NewMockQuerier(t)
		chunk1 := meta.EncodeSlices([]meta.Slice{
			{Pos: 0, ID: 100, Size: 4096, Off: 0, Len: 4096},
		})
		mockQuerier.EXPECT().
			DeleteChunksByIno(mock.Anything, ino).
			Return([][]byte{chunk1}, nil).
			Once()

		mockQuerier.EXPECT().
			AddSliceRefCount(mock.Anything, db.AddSliceRefCountParams{
				SliceID: 100,
				Delta:   -1,
			}).
			Return(nil).
			Once()

		err := meta.DeleteAllChunksByIno(ctx, mockQuerier, ino)
		require.NoError(t, err)
	})
}

// TestOpen_OTrunc_CleansChunksAndRefs 验证常规非空文件在 Open(O_TRUNC) 时清空 chunks 并扣减 slice_refs。
func TestOpen_OTrunc_CleansChunksAndRefs(t *testing.T) {
	ctx := context.Background()
	now := time.Now().Truncate(time.Microsecond)
	ino := uint64(2001)

	fileRow := db.Inode{
		Ino:        int64(ino),
		Generation: 1,
		Mode:       int32(meta.S_IFREG | 0o644),
		Size:       8192,
		Nlink:      1,
		Uid:        1000,
		Gid:        1000,
		Atime:      pgtype.Timestamptz{Time: now, Valid: true},
		Mtime:      pgtype.Timestamptz{Time: now, Valid: true},
		Ctime:      pgtype.Timestamptz{Time: now, Valid: true},
	}

	mockStore := dbmock.NewMockStore(t)
	mockQuerier := dbmock.NewMockQuerier(t)
	mgr := meta.New(mockStore)

	setupMockWriteTx(mockStore, mockQuerier)

	mockQuerier.EXPECT().
		GetInodeForUpdate(mock.Anything, int64(ino)).
		Return(fileRow, nil).
		Once()

	mockQuerier.EXPECT().
		UpdateInodeAttr(mock.Anything, mock.MatchedBy(func(p db.UpdateInodeAttrParams) bool {
			return p.Ino == int64(ino) && p.Size == 0
		})).
		Return(fileRow, nil).
		Once()

	chunkBytes := meta.EncodeSlices([]meta.Slice{
		{Pos: 0, ID: 88, Size: 8192, Off: 0, Len: 8192},
	})

	// 核心断言：在同一事务内调用 DeleteChunksByIno
	mockQuerier.EXPECT().
		DeleteChunksByIno(mock.Anything, int64(ino)).
		Return([][]byte{chunkBytes}, nil).
		Once()

	// 核心断言：调用 AddSliceRefCount 扣减 slice_id=88 的 refs
	mockQuerier.EXPECT().
		AddSliceRefCount(mock.Anything, db.AddSliceRefCountParams{
			SliceID: 88,
			Delta:   -1,
		}).
		Return(nil).
		Once()

	mockQuerier.EXPECT().
		CreateFileHandle(mock.Anything, db.CreateFileHandleParams{
			Ino:   int64(ino),
			Flags: int32(meta.O_RDWR | meta.O_TRUNC),
		}).
		Return(db.FileHandle{
			Fh:        101,
			Ino:       int64(ino),
			Flags:     int32(meta.O_RDWR | meta.O_TRUNC),
			CreatedAt: pgtype.Timestamptz{Time: now, Valid: true},
		}, nil).
		Once()

	res, err := mgr.Open(ctx, meta.OpenParams{
		Ino:   ino,
		Flags: meta.O_RDWR | meta.O_TRUNC,
	})
	require.NoError(t, err)
	require.NotNil(t, res)
	assert.Equal(t, uint64(101), res.Fh)
}

// TestSetAttr_SizeZero_CleansChunksAndRefs 验证常规非空文件在 SetAttr(size == 0) 时清空 chunks 并扣减 slice_refs。
func TestSetAttr_SizeZero_CleansChunksAndRefs(t *testing.T) {
	ctx := context.Background()
	now := time.Now().Truncate(time.Microsecond)
	ino := uint64(3001)

	fileRow := db.Inode{
		Ino:        int64(ino),
		Generation: 1,
		Mode:       int32(meta.S_IFREG | 0o644),
		Size:       4096,
		Nlink:      1,
		Uid:        1000,
		Gid:        1000,
		Atime:      pgtype.Timestamptz{Time: now, Valid: true},
		Mtime:      pgtype.Timestamptz{Time: now, Valid: true},
		Ctime:      pgtype.Timestamptz{Time: now, Valid: true},
	}

	mockStore := dbmock.NewMockStore(t)
	mockQuerier := dbmock.NewMockQuerier(t)
	mgr := meta.New(mockStore)

	setupMockWriteTx(mockStore, mockQuerier)

	mockQuerier.EXPECT().
		GetInodeForUpdate(mock.Anything, int64(ino)).
		Return(fileRow, nil).
		Once()

	chunkBytes := meta.EncodeSlices([]meta.Slice{
		{Pos: 0, ID: 99, Size: 4096, Off: 0, Len: 4096},
	})

	// 核心断言：在同一事务内调用 DeleteChunksByIno
	mockQuerier.EXPECT().
		DeleteChunksByIno(mock.Anything, int64(ino)).
		Return([][]byte{chunkBytes}, nil).
		Once()

	// 核心断言：调用 AddSliceRefCount 扣减 slice_id=99 的 refs
	mockQuerier.EXPECT().
		AddSliceRefCount(mock.Anything, db.AddSliceRefCountParams{
			SliceID: 99,
			Delta:   -1,
		}).
		Return(nil).
		Once()

	updatedRow := fileRow
	updatedRow.Size = 0
	mockQuerier.EXPECT().
		UpdateInodeAttr(mock.Anything, mock.MatchedBy(func(p db.UpdateInodeAttrParams) bool {
			return p.Ino == int64(ino) && p.Size == 0
		})).
		Return(updatedRow, nil).
		Once()

	zeroSize := uint64(0)
	inode, err := mgr.SetAttr(ctx, ino, meta.SetAttrInput{
		Size: &zeroSize,
	})
	require.NoError(t, err)
	require.NotNil(t, inode)
	assert.Equal(t, uint64(0), inode.Attr.Size)
}

// TestTruncateZero_ExpandAndReadZero 验证文件截断归零后再扩容，Read 绝不复活截断前历史数据，全部返回 0x00。
func TestTruncateZero_ExpandAndReadZero(t *testing.T) {
	ctx := context.Background()
	now := time.Now().Truncate(time.Microsecond)
	ino := uint64(4001)
	fh := uint64(401)

	fileRowBeforeTruncate := db.Inode{
		Ino:        int64(ino),
		Generation: 1,
		Mode:       int32(meta.S_IFREG | 0o644),
		Size:       4096,
		Nlink:      1,
		Uid:        1000,
		Gid:        1000,
		Atime:      pgtype.Timestamptz{Time: now, Valid: true},
		Mtime:      pgtype.Timestamptz{Time: now, Valid: true},
		Ctime:      pgtype.Timestamptz{Time: now, Valid: true},
	}

	t.Run("SetAttr(size=0) 清空 chunks 后再 SetAttr(size=N) 扩容，Read[0, N) 全零返回", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mockBS := metamock.NewMockBlockStore(t)
		mgr := meta.New(mockStore, meta.WithBlockStore(mockBS))

		// 1. 模拟 SetAttr(size=0) 阶段
		setupMockWriteTx(mockStore, mockQuerier)
		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(ino)).
			Return(fileRowBeforeTruncate, nil).
			Once()

		oldChunkBytes := meta.EncodeSlices([]meta.Slice{
			{Pos: 0, ID: 777, Size: 4096, Off: 0, Len: 4096},
		})
		mockQuerier.EXPECT().
			DeleteChunksByIno(mock.Anything, int64(ino)).
			Return([][]byte{oldChunkBytes}, nil).
			Once()

		mockQuerier.EXPECT().
			AddSliceRefCount(mock.Anything, db.AddSliceRefCountParams{
				SliceID: 777,
				Delta:   -1,
			}).
			Return(nil).
			Once()

		fileRowTruncated := fileRowBeforeTruncate
		fileRowTruncated.Size = 0
		mockQuerier.EXPECT().
			UpdateInodeAttr(mock.Anything, mock.MatchedBy(func(p db.UpdateInodeAttrParams) bool {
				return p.Ino == int64(ino) && p.Size == 0
			})).
			Return(fileRowTruncated, nil).
			Once()

		zeroSize := uint64(0)
		_, err := mgr.SetAttr(ctx, ino, meta.SetAttrInput{
			Size: &zeroSize,
		})
		require.NoError(t, err)

		// 2. 模拟 SetAttr(size=8192) 扩容阶段
		setupMockWriteTx(mockStore, mockQuerier)
		fileRowExpanded := fileRowTruncated
		fileRowExpanded.Size = 8192

		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(ino)).
			Return(fileRowTruncated, nil).
			Once()

		mockQuerier.EXPECT().
			UpdateInodeAttr(mock.Anything, mock.MatchedBy(func(p db.UpdateInodeAttrParams) bool {
				return p.Ino == int64(ino) && p.Size == 8192
			})).
			Return(fileRowExpanded, nil).
			Once()

		newSize := uint64(8192)
		_, err = mgr.SetAttr(ctx, ino, meta.SetAttrInput{
			Size: &newSize,
		})
		require.NoError(t, err)

		// 3. 模拟 Read[0, 8192) 阶段：因 chunks 表记录已被彻底清空，GetInodeSizeAndChunksInRange 返回 ChunkIndx 为 NULL，直接全零返回且无 BlockStore IO
		setupMockWriteTx(mockStore, mockQuerier)
		mockQuerier.EXPECT().
			GetInodeSizeAndChunksInRange(mock.Anything, db.GetInodeSizeAndChunksInRangeParams{
				Ino:       int64(ino),
				StartIndx: 0,
				EndIndx:   0,
			}).
			Return([]db.GetInodeSizeAndChunksInRangeRow{
				{
					Ino:       fileRowExpanded.Ino,
					Mode:      fileRowExpanded.Mode,
					Size:      fileRowExpanded.Size,
					Atime:     fileRowExpanded.Atime,
					Mtime:     fileRowExpanded.Mtime,
					Ctime:     fileRowExpanded.Ctime,
					ChunkIndx: pgtype.Int4{Valid: false},
				},
			}, nil).
			Once()

		mockQuerier.EXPECT().
			GetFileHandle(mock.Anything, db.GetFileHandleParams{
				Fh:  int64(fh),
				Ino: int64(ino),
			}).
			Return(db.FileHandle{
				Fh:    int64(fh),
				Ino:   int64(ino),
				Flags: int32(meta.O_RDONLY),
			}, nil).
			Once()

		readResp, err := mgr.Read(ctx, &meta.ReadRequest{
			Ino:    ino,
			Fh:     fh,
			Offset: 0,
			Size:   8192,
		})
		require.NoError(t, err)
		require.NotNil(t, readResp)
		assert.Equal(t, 8192, len(readResp.Data))
		assert.True(t, bytes.Equal(readResp.Data, make([]byte, 8192)), "必须全为 0x00，绝不复活历史数据")
		// 严防触发历史物理块 IO
		mockBS.AssertNotCalled(t, "GetBlock", mock.Anything, mock.Anything)
	})

	t.Run("Open(O_TRUNC) 清空 chunks 后再 Fallocate(mode=0) 扩容，Read[0, N) 全零返回", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mockBS := metamock.NewMockBlockStore(t)
		mgr := meta.New(mockStore, meta.WithBlockStore(mockBS))

		// 1. 模拟 Open(O_TRUNC) 阶段
		setupMockWriteTx(mockStore, mockQuerier)
		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(ino)).
			Return(fileRowBeforeTruncate, nil).
			Once()

		oldChunkBytes := meta.EncodeSlices([]meta.Slice{
			{Pos: 0, ID: 888, Size: 4096, Off: 0, Len: 4096},
		})
		mockQuerier.EXPECT().
			DeleteChunksByIno(mock.Anything, int64(ino)).
			Return([][]byte{oldChunkBytes}, nil).
			Once()

		mockQuerier.EXPECT().
			AddSliceRefCount(mock.Anything, db.AddSliceRefCountParams{
				SliceID: 888,
				Delta:   -1,
			}).
			Return(nil).
			Once()

		fileRowTruncated := fileRowBeforeTruncate
		fileRowTruncated.Size = 0
		mockQuerier.EXPECT().
			UpdateInodeAttr(mock.Anything, mock.MatchedBy(func(p db.UpdateInodeAttrParams) bool {
				return p.Ino == int64(ino) && p.Size == 0
			})).
			Return(fileRowTruncated, nil).
			Once()

		mockQuerier.EXPECT().
			CreateFileHandle(mock.Anything, db.CreateFileHandleParams{
				Ino:   int64(ino),
				Flags: int32(meta.O_RDWR | meta.O_TRUNC),
			}).
			Return(db.FileHandle{
				Fh:        int64(fh),
				Ino:       int64(ino),
				Flags:     int32(meta.O_RDWR | meta.O_TRUNC),
				CreatedAt: pgtype.Timestamptz{Time: now, Valid: true},
			}, nil).
			Once()

		openRes, err := mgr.Open(ctx, meta.OpenParams{
			Ino:   ino,
			Flags: meta.O_RDWR | meta.O_TRUNC,
		})
		require.NoError(t, err)
		assert.Equal(t, fh, openRes.Fh)

		// 2. 模拟 Fallocate(mode=0, offset=0, length=4096) 扩容阶段
		setupMockWriteTx(mockStore, mockQuerier)
		fileRowAllocated := fileRowTruncated
		fileRowAllocated.Size = 4096

		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(ino)).
			Return(fileRowTruncated, nil).
			Once()

		mockQuerier.EXPECT().
			GetFileHandle(mock.Anything, db.GetFileHandleParams{
				Fh:  int64(fh),
				Ino: int64(ino),
			}).
			Return(db.FileHandle{
				Fh:    int64(fh),
				Ino:   int64(ino),
				Flags: int32(meta.O_RDWR),
			}, nil).
			Once()

		mockQuerier.EXPECT().
			UpdateInodeAttr(mock.Anything, mock.MatchedBy(func(p db.UpdateInodeAttrParams) bool {
				return p.Ino == int64(ino) && p.Size == 4096
			})).
			Return(fileRowAllocated, nil).
			Once()

		err = mgr.Fallocate(ctx, meta.FallocateParams{
			Ino:    ino,
			Fh:     fh,
			Mode:   0,
			Offset: 0,
			Length: 4096,
		})
		require.NoError(t, err)

		// 3. 模拟 Read[0, 4096) 阶段
		setupMockWriteTx(mockStore, mockQuerier)
		mockQuerier.EXPECT().
			GetInodeSizeAndChunksInRange(mock.Anything, db.GetInodeSizeAndChunksInRangeParams{
				Ino:       int64(ino),
				StartIndx: 0,
				EndIndx:   0,
			}).
			Return([]db.GetInodeSizeAndChunksInRangeRow{
				{
					Ino:       fileRowAllocated.Ino,
					Mode:      fileRowAllocated.Mode,
					Size:      fileRowAllocated.Size,
					Atime:     fileRowAllocated.Atime,
					Mtime:     fileRowAllocated.Mtime,
					Ctime:     fileRowAllocated.Ctime,
					ChunkIndx: pgtype.Int4{Valid: false},
				},
			}, nil).
			Once()

		mockQuerier.EXPECT().
			GetFileHandle(mock.Anything, db.GetFileHandleParams{
				Fh:  int64(fh),
				Ino: int64(ino),
			}).
			Return(db.FileHandle{
				Fh:    int64(fh),
				Ino:   int64(ino),
				Flags: int32(meta.O_RDWR),
			}, nil).
			Once()

		readResp, err := mgr.Read(ctx, &meta.ReadRequest{
			Ino:    ino,
			Fh:     fh,
			Offset: 0,
			Size:   4096,
		})
		require.NoError(t, err)
		require.NotNil(t, readResp)
		assert.Equal(t, 4096, len(readResp.Data))
		assert.True(t, bytes.Equal(readResp.Data, make([]byte, 4096)), "必须全为 0x00")
		mockBS.AssertNotCalled(t, "GetBlock", mock.Anything, mock.Anything)
	})
}
