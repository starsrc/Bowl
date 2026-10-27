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

// TestTruncateChunksToSize 单元测试 truncateChunksToSize 辅助函数的边界与分支行为。
func TestTruncateChunksToSize(t *testing.T) {
	ctx := context.Background()
	ino := int64(1001)

	t.Run("跨多个 64 MiB Chunk 的对齐缩容 (borderPos == 0)", func(t *testing.T) {
		mockQuerier := dbmock.NewMockQuerier(t)
		// newSize = 128 MiB (2 * ChunkSize = 134217728), borderIndx = 2, borderPos = 0
		newSize := uint64(2 * meta.ChunkSize)

		// 模拟被删 indx=2 和 indx=3 的切片流，包含 slice_id=10 (2 次) 与 slice_id=20 (1 次)
		deletedChunk2 := meta.EncodeSlices([]meta.Slice{
			{Pos: 0, ID: 10, Size: 4096, Off: 0, Len: 1024},
			{Pos: 1024, ID: 20, Size: 4096, Off: 0, Len: 1024},
		})
		deletedChunk3 := meta.EncodeSlices([]meta.Slice{
			{Pos: 0, ID: 10, Size: 4096, Off: 1024, Len: 1024},
		})

		mockQuerier.EXPECT().
			DeleteChunksFromIndx(mock.Anything, db.DeleteChunksFromIndxParams{
				Ino:      ino,
				FromIndx: 2,
			}).
			Return([][]byte{deletedChunk2, deletedChunk3}, nil).
			Once()

		// 验证 slice_refs 升序扣减：slice 10 (-2), slice 20 (-1)
		mockQuerier.EXPECT().
			AddSliceRefCount(mock.Anything, db.AddSliceRefCountParams{
				SliceID: 10,
				Delta:   -2,
			}).
			Return(nil).
			Once()

		mockQuerier.EXPECT().
			AddSliceRefCount(mock.Anything, db.AddSliceRefCountParams{
				SliceID: 20,
				Delta:   -1,
			}).
			Return(nil).
			Once()

		err := meta.TruncateChunksToSize(ctx, mockQuerier, ino, newSize)
		require.NoError(t, err)

		// 断言未调用非对齐相关的 DB 方法
		mockQuerier.AssertNotCalled(t, "DeleteChunksAfterIndx", mock.Anything, mock.Anything)
		mockQuerier.AssertNotCalled(t, "AppendExistingChunkSlice", mock.Anything, mock.Anything)
	})

	t.Run("对齐缩容当无任何匹配 chunk 被删时正常返回 nil", func(t *testing.T) {
		mockQuerier := dbmock.NewMockQuerier(t)
		newSize := uint64(meta.ChunkSize) // borderIndx = 1, borderPos = 0

		mockQuerier.EXPECT().
			DeleteChunksFromIndx(mock.Anything, db.DeleteChunksFromIndxParams{
				Ino:      ino,
				FromIndx: 1,
			}).
			Return(nil, nil).
			Once()

		err := meta.TruncateChunksToSize(ctx, mockQuerier, ino, newSize)
		require.NoError(t, err)
		mockQuerier.AssertNotCalled(t, "AddSliceRefCount", mock.Anything, mock.Anything)
	})

	t.Run("跨块非对齐缩容 (borderPos > 0) 删除越界块并向边界块追加空洞切片", func(t *testing.T) {
		mockQuerier := dbmock.NewMockQuerier(t)
		// newSize = 64 MiB + 1024 (ChunkSize + 1024) -> borderIndx = 1, borderPos = 1024
		newSize := uint64(meta.ChunkSize + 1024)
		expectedHoleBytes := meta.EncodeSlice(meta.Slice{
			Pos:  1024,
			ID:   0,
			Size: 0,
			Off:  0,
			Len:  uint32(meta.ChunkSize) - 1024,
		})

		// 模拟被删 indx > 1 的块 (如 indx=2)
		deletedChunk2 := meta.EncodeSlices([]meta.Slice{
			{Pos: 0, ID: 30, Size: 4096, Off: 0, Len: 2048},
		})

		mockQuerier.EXPECT().
			DeleteChunksAfterIndx(mock.Anything, db.DeleteChunksAfterIndxParams{
				Ino:       ino,
				AfterIndx: 1,
			}).
			Return([][]byte{deletedChunk2}, nil).
			Once()

		mockQuerier.EXPECT().
			AddSliceRefCount(mock.Anything, db.AddSliceRefCountParams{
				SliceID: 30,
				Delta:   -1,
			}).
			Return(nil).
			Once()

		mockQuerier.EXPECT().
			AppendExistingChunkSlice(mock.Anything, db.AppendExistingChunkSliceParams{
				Ino:    ino,
				Indx:   1,
				Slices: expectedHoleBytes,
			}).
			Return(int64(1), nil).
			Once()

		err := meta.TruncateChunksToSize(ctx, mockQuerier, ino, newSize)
		require.NoError(t, err)
		mockQuerier.AssertNotCalled(t, "DeleteChunksFromIndx", mock.Anything, mock.Anything)
	})

	t.Run("单块内非对齐缩容 (borderIndx == 0, borderPos > 0)", func(t *testing.T) {
		mockQuerier := dbmock.NewMockQuerier(t)
		// newSize = 4096 字节 -> borderIndx = 0, borderPos = 4096
		newSize := uint64(4096)
		expectedHoleBytes := meta.EncodeSlice(meta.Slice{
			Pos:  4096,
			ID:   0,
			Size: 0,
			Off:  0,
			Len:  uint32(meta.ChunkSize) - 4096,
		})

		mockQuerier.EXPECT().
			DeleteChunksAfterIndx(mock.Anything, db.DeleteChunksAfterIndxParams{
				Ino:       ino,
				AfterIndx: 0,
			}).
			Return(nil, nil).
			Once()

		mockQuerier.EXPECT().
			AppendExistingChunkSlice(mock.Anything, db.AppendExistingChunkSliceParams{
				Ino:    ino,
				Indx:   0,
				Slices: expectedHoleBytes,
			}).
			Return(int64(1), nil).
			Once()

		err := meta.TruncateChunksToSize(ctx, mockQuerier, ino, newSize)
		require.NoError(t, err)
		mockQuerier.AssertNotCalled(t, "AddSliceRefCount", mock.Anything, mock.Anything)
	})

	t.Run("边界 Chunk 原本为稀疏未写入状态时非零缩容 (UPDATE 影响 0 行无需插入新行)", func(t *testing.T) {
		mockQuerier := dbmock.NewMockQuerier(t)
		newSize := uint64(8192)
		expectedHoleBytes := meta.EncodeSlice(meta.Slice{
			Pos:  8192,
			ID:   0,
			Size: 0,
			Off:  0,
			Len:  uint32(meta.ChunkSize) - 8192,
		})

		mockQuerier.EXPECT().
			DeleteChunksAfterIndx(mock.Anything, db.DeleteChunksAfterIndxParams{
				Ino:       ino,
				AfterIndx: 0,
			}).
			Return(nil, nil).
			Once()

		// 边界块不存在，UPDATE 匹配 0 行返回 rowsAffected = 0
		mockQuerier.EXPECT().
			AppendExistingChunkSlice(mock.Anything, db.AppendExistingChunkSliceParams{
				Ino:    ino,
				Indx:   0,
				Slices: expectedHoleBytes,
			}).
			Return(int64(0), nil).
			Once()

		err := meta.TruncateChunksToSize(ctx, mockQuerier, ino, newSize)
		require.NoError(t, err)
		mockQuerier.AssertNotCalled(t, "AppendChunkSlice", mock.Anything, mock.Anything)
	})

	t.Run("DeleteChunksFromIndx 失败向上透传错误", func(t *testing.T) {
		mockQuerier := dbmock.NewMockQuerier(t)
		dbErr := errors.New("db error in delete chunks from indx")

		mockQuerier.EXPECT().
			DeleteChunksFromIndx(mock.Anything, mock.Anything).
			Return(nil, dbErr).
			Once()

		err := meta.TruncateChunksToSize(ctx, mockQuerier, ino, uint64(meta.ChunkSize))
		require.Error(t, err)
		assert.ErrorIs(t, err, dbErr)
	})

	t.Run("DeleteChunksAfterIndx 失败向上透传错误", func(t *testing.T) {
		mockQuerier := dbmock.NewMockQuerier(t)
		dbErr := errors.New("db error in delete chunks after indx")

		mockQuerier.EXPECT().
			DeleteChunksAfterIndx(mock.Anything, mock.Anything).
			Return(nil, dbErr).
			Once()

		err := meta.TruncateChunksToSize(ctx, mockQuerier, ino, 1024)
		require.Error(t, err)
		assert.ErrorIs(t, err, dbErr)
	})

	t.Run("AppendExistingChunkSlice 失败向上透传错误", func(t *testing.T) {
		mockQuerier := dbmock.NewMockQuerier(t)
		dbErr := errors.New("db error in append existing chunk slice")

		mockQuerier.EXPECT().
			DeleteChunksAfterIndx(mock.Anything, mock.Anything).
			Return(nil, nil).
			Once()

		mockQuerier.EXPECT().
			AppendExistingChunkSlice(mock.Anything, mock.Anything).
			Return(int64(0), dbErr).
			Once()

		err := meta.TruncateChunksToSize(ctx, mockQuerier, ino, 1024)
		require.Error(t, err)
		assert.ErrorIs(t, err, dbErr)
	})
}

// TestSetAttr_TruncateNonZero 验证在 SetAttr 中触发非零缩容截断时的事务协调逻辑。
func TestSetAttr_TruncateNonZero(t *testing.T) {
	ctx := context.Background()
	now := time.Now().Truncate(time.Microsecond)
	ino := uint64(3001)

	fileRowOriginal := db.Inode{
		Ino:        int64(ino),
		Generation: 1,
		Mode:       int32(meta.S_IFREG | 0o644),
		Size:       128 * 1024 * 1024, // 128 MiB
		Nlink:      1,
		Uid:        1000,
		Gid:        1000,
		Atime:      pgtype.Timestamptz{Time: now, Valid: true},
		Mtime:      pgtype.Timestamptz{Time: now, Valid: true},
		Ctime:      pgtype.Timestamptz{Time: now, Valid: true},
	}

	t.Run("SetAttr 跨块对齐缩容 (128 MiB -> 64 MiB) 调用 DeleteChunksFromIndx 并在同事务更新属性", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockWriteTx(mockStore, mockQuerier)

		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(ino)).
			Return(fileRowOriginal, nil).
			Once()

		deletedSlices := meta.EncodeSlices([]meta.Slice{
			{Pos: 0, ID: 55, Size: 4096, Off: 0, Len: 4096},
		})

		// 64 MiB: borderIndx = 1, borderPos = 0
		mockQuerier.EXPECT().
			DeleteChunksFromIndx(mock.Anything, db.DeleteChunksFromIndxParams{
				Ino:      int64(ino),
				FromIndx: 1,
			}).
			Return([][]byte{deletedSlices}, nil).
			Once()

		mockQuerier.EXPECT().
			AddSliceRefCount(mock.Anything, db.AddSliceRefCountParams{
				SliceID: 55,
				Delta:   -1,
			}).
			Return(nil).
			Once()

		fileRowUpdated := fileRowOriginal
		fileRowUpdated.Size = int64(meta.ChunkSize)

		mockQuerier.EXPECT().
			UpdateInodeAttr(mock.Anything, mock.MatchedBy(func(p db.UpdateInodeAttrParams) bool {
				return p.Ino == int64(ino) && p.Size == int64(meta.ChunkSize)
			})).
			Return(fileRowUpdated, nil).
			Once()

		newSize := uint64(meta.ChunkSize)
		inode, err := mgr.SetAttr(ctx, ino, meta.SetAttrInput{
			Size: &newSize,
		})
		require.NoError(t, err)
		require.NotNil(t, inode)
		assert.Equal(t, uint64(meta.ChunkSize), inode.Attr.Size)
	})

	t.Run("SetAttr 非对齐缩容 (128 MiB -> 64 MiB + 4 MiB) 调用 DeleteChunksAfterIndx 并追加空洞切片", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockWriteTx(mockStore, mockQuerier)

		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(ino)).
			Return(fileRowOriginal, nil).
			Once()

		newSize := uint64(meta.ChunkSize + 4*1024*1024) // 68 MiB: borderIndx = 1, borderPos = 4 MiB

		deletedSlices := meta.EncodeSlices([]meta.Slice{
			{Pos: 0, ID: 66, Size: 4096, Off: 0, Len: 4096},
		})

		mockQuerier.EXPECT().
			DeleteChunksAfterIndx(mock.Anything, db.DeleteChunksAfterIndxParams{
				Ino:       int64(ino),
				AfterIndx: 1,
			}).
			Return([][]byte{deletedSlices}, nil).
			Once()

		mockQuerier.EXPECT().
			AddSliceRefCount(mock.Anything, db.AddSliceRefCountParams{
				SliceID: 66,
				Delta:   -1,
			}).
			Return(nil).
			Once()

		expectedHole := meta.EncodeSlice(meta.Slice{
			Pos:  4 * 1024 * 1024,
			ID:   0,
			Size: 0,
			Off:  0,
			Len:  uint32(meta.ChunkSize) - 4*1024*1024,
		})

		mockQuerier.EXPECT().
			AppendExistingChunkSlice(mock.Anything, db.AppendExistingChunkSliceParams{
				Ino:    int64(ino),
				Indx:   1,
				Slices: expectedHole,
			}).
			Return(int64(1), nil).
			Once()

		fileRowUpdated := fileRowOriginal
		fileRowUpdated.Size = int64(newSize)

		mockQuerier.EXPECT().
			UpdateInodeAttr(mock.Anything, mock.MatchedBy(func(p db.UpdateInodeAttrParams) bool {
				return p.Ino == int64(ino) && p.Size == int64(newSize)
			})).
			Return(fileRowUpdated, nil).
			Once()

		inode, err := mgr.SetAttr(ctx, ino, meta.SetAttrInput{
			Size: &newSize,
		})
		require.NoError(t, err)
		require.NotNil(t, inode)
		assert.Equal(t, newSize, inode.Attr.Size)
	})
}

// TestSetAttr_TruncateNonZero_ResurrectPrevention_Read 端到端防数据复活测试：
// 「先写满 -> 非零缩容截断 -> 再次 SetAttr 扩容回原大小 -> Read 校验截断点之前数据完好、截断点之后全为 0x00」。
func TestSetAttr_TruncateNonZero_ResurrectPrevention_Read(t *testing.T) {
	ctx := context.Background()
	now := time.Now().Truncate(time.Microsecond)
	ino := uint64(5001)
	fh := uint64(501)

	mockStore := dbmock.NewMockStore(t)
	mockQuerier := dbmock.NewMockQuerier(t)
	mockBS := metamock.NewMockBlockStore(t)
	mgr := meta.New(mockStore, meta.WithBlockStore(mockBS))

	// 初始状态：文件大小 4096 字节，Indx=0 有一条完整的原始切片 [0, 4096, ID=100]
	origSize := uint64(4096)
	truncSize := uint64(2048)

	origSlice := meta.Slice{
		Pos:  0,
		ID:   100,
		Size: uint32(origSize),
		Off:  0,
		Len:  uint32(origSize),
	}
	origSliceBytes := meta.EncodeSlice(origSlice)

	initialFileRow := db.Inode{
		Ino:        int64(ino),
		Generation: 1,
		Mode:       int32(meta.S_IFREG | 0o644),
		Size:       int64(origSize),
		Nlink:      1,
		Uid:        1000,
		Gid:        1000,
		Atime:      pgtype.Timestamptz{Time: now, Valid: true},
		Mtime:      pgtype.Timestamptz{Time: now, Valid: true},
		Ctime:      pgtype.Timestamptz{Time: now, Valid: true},
	}

	// 1. 模拟 SetAttr(size = 2048) 缩容截断
	setupMockWriteTx(mockStore, mockQuerier)

	mockQuerier.EXPECT().
		GetInodeForUpdate(mock.Anything, int64(ino)).
		Return(initialFileRow, nil).
		Once()

	// 2048 < ChunkSize -> borderIndx = 0, borderPos = 2048
	mockQuerier.EXPECT().
		DeleteChunksAfterIndx(mock.Anything, db.DeleteChunksAfterIndxParams{
			Ino:       int64(ino),
			AfterIndx: 0,
		}).
		Return(nil, nil).
		Once()

	holeSlice := meta.Slice{
		Pos:  uint32(truncSize),
		ID:   0,
		Size: 0,
		Off:  0,
		Len:  uint32(meta.ChunkSize) - uint32(truncSize),
	}
	holeSliceBytes := meta.EncodeSlice(holeSlice)

	mockQuerier.EXPECT().
		AppendExistingChunkSlice(mock.Anything, db.AppendExistingChunkSliceParams{
			Ino:    int64(ino),
			Indx:   0,
			Slices: holeSliceBytes,
		}).
		Return(int64(1), nil).
		Once()

	truncatedRow := initialFileRow
	truncatedRow.Size = int64(truncSize)

	mockQuerier.EXPECT().
		UpdateInodeAttr(mock.Anything, mock.MatchedBy(func(p db.UpdateInodeAttrParams) bool {
			return p.Ino == int64(ino) && p.Size == int64(truncSize)
		})).
		Return(truncatedRow, nil).
		Once()

	res, err := mgr.SetAttr(ctx, ino, meta.SetAttrInput{
		Size: &truncSize,
	})
	require.NoError(t, err)
	require.NotNil(t, res)
	assert.Equal(t, truncSize, res.Attr.Size)

	// 2. 模拟再次 SetAttr(size = 4096) 扩容回原大小
	setupMockWriteTx(mockStore, mockQuerier)

	mockQuerier.EXPECT().
		GetInodeForUpdate(mock.Anything, int64(ino)).
		Return(truncatedRow, nil).
		Once()

	expandedRow := initialFileRow
	expandedRow.Size = int64(origSize)

	mockQuerier.EXPECT().
		UpdateInodeAttr(mock.Anything, mock.MatchedBy(func(p db.UpdateInodeAttrParams) bool {
			return p.Ino == int64(ino) && p.Size == int64(origSize)
		})).
		Return(expandedRow, nil).
		Once()

	resExpanded, err := mgr.SetAttr(ctx, ino, meta.SetAttrInput{
		Size: &origSize,
	})
	require.NoError(t, err)
	require.NotNil(t, resExpanded)
	assert.Equal(t, origSize, resExpanded.Attr.Size)

	// 3. 模拟 Read(offset = 0, size = 4096) 读取全量数据
	// Indx=0 的 chunk.Slices 已经追加了 [origSlice, holeSlice] 两个切片
	combinedSlicesBytes := append(append([]byte(nil), origSliceBytes...), holeSliceBytes...)

	setupMockWriteTx(mockStore, mockQuerier)

	mockQuerier.EXPECT().
		GetInode(mock.Anything, int64(ino)).
		Return(expandedRow, nil).
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

	mockQuerier.EXPECT().
		GetChunk(mock.Anything, db.GetChunkParams{
			Ino:  int64(ino),
			Indx: 0,
		}).
		Return(db.Chunk{
			Ino:    int64(ino),
			Indx:   0,
			Slices: combinedSlicesBytes,
		}, nil).
		Once()

	// 截断点之前的 2048 字节数据 (来自 ID=100) 会调用 BlockStore.GetBlock 读取
	fakeData := bytes.Repeat([]byte{0xAB}, int(truncSize))
	mockBS.EXPECT().
		GetBlock(mock.Anything, uint64(100), uint32(0), uint32(origSize), uint32(0), mock.Anything).
		RunAndReturn(func(ctx context.Context, sliceID uint64, blockIdx uint32, blockSize uint32, offset uint32, dst []byte) error {
			copy(dst, fakeData)
			return nil
		}).
		Once()

	readResp, err := mgr.Read(ctx, &meta.ReadRequest{
		Ino:    ino,
		Fh:     fh,
		Offset: 0,
		Size:   uint32(origSize),
	})
	require.NoError(t, err)
	require.NotNil(t, readResp)
	assert.Equal(t, int(origSize), len(readResp.Data))

	// 核心断言：截断点之前 [0, 2048) 数据完好 (0xAB)，截断点之后 [2048, 4096) 全为 0x00
	assert.True(t, bytes.Equal(readResp.Data[:truncSize], fakeData), "截断点前数据必须完好")
	assert.True(t, bytes.Equal(readResp.Data[truncSize:], make([]byte, origSize-truncSize)), "截断点后数据必须全为 0x00，绝不复活")

	// 严防对 [2048, 4096) 触发 BlockStore 读取（空洞由 buildSlice 补齐为 id=0 直接内存填零）
	mockBS.AssertNumberOfCalls(t, "GetBlock", 1)
}
