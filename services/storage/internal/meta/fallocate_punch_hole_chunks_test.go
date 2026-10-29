package meta_test

import (
	"bytes"
	"context"
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
	metamock "github.com/starsrc/bowl/services/storage/internal/meta/mock"
)

// TestFallocatePunchHole_LocalChunk_ReadHoleZero 验证文件内部局部打洞后向 chunks 追加空洞 Slice，
// 且随后通过 Read 接口读取时，打洞区域返回 0x00，未打洞区域正常返回原始数据。
func TestFallocatePunchHole_LocalChunk_ReadHoleZero(t *testing.T) {
	ctx := context.Background()
	now := time.Now().Truncate(time.Microsecond)
	ino := uint64(5001)
	fh := uint64(501)
	fileSize := int64(4096)

	fileRow := db.Inode{
		Ino:        int64(ino),
		Generation: 1,
		Mode:       int32(meta.S_IFREG | 0o644),
		Size:       fileSize,
		Nlink:      1,
		Uid:        1000,
		Gid:        1000,
		Atime:      pgtype.Timestamptz{Time: now, Valid: true},
		Mtime:      pgtype.Timestamptz{Time: now, Valid: true},
		Ctime:      pgtype.Timestamptz{Time: now, Valid: true},
	}

	mockStore := dbmock.NewMockStore(t)
	mockQuerier := dbmock.NewMockQuerier(t)
	mockBS := metamock.NewMockBlockStore(t)
	mgr := meta.New(mockStore, meta.WithBlockStore(mockBS))

	// 1. 执行 Fallocate 打洞：在 [1024, 3072) 打洞，长度 2048
	setupMockWriteTx(mockStore, mockQuerier)

	mockQuerier.EXPECT().
		GetInodeForUpdate(mock.Anything, int64(ino)).
		Return(fileRow, nil).
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

	expectedHoleSlice := meta.Slice{
		Pos:  1024,
		ID:   0,
		Size: 0,
		Off:  0,
		Len:  2048,
	}

	mockQuerier.EXPECT().
		AppendExistingChunkSlice(mock.Anything, db.AppendExistingChunkSliceParams{
			Ino:    int64(ino),
			Indx:   0,
			Slices: meta.EncodeSlice(expectedHoleSlice),
		}).
		Return(int64(1), nil).
		Once()

	mockQuerier.EXPECT().
		UpdateInodeAttr(mock.Anything, mock.MatchedBy(func(p db.UpdateInodeAttrParams) bool {
			return p.Ino == int64(ino) && p.Size == fileSize && p.SetMtimeNow
		})).
		Return(fileRow, nil).
		Once()

	err := mgr.Fallocate(ctx, meta.FallocateParams{
		Ino:    ino,
		Fh:     fh,
		Mode:   meta.FallocFlPunchHole | meta.FallocFlKeepSize,
		Offset: 1024,
		Length: 2048,
	})
	require.NoError(t, err)

	// 2. 执行 Read(offset=0, size=4096) 验证
	// 模拟已存在切片 (ID: 100) + 打洞切片 (ID: 0)
	baseSlice := meta.Slice{Pos: 0, ID: 100, Size: 4096, Off: 0, Len: 4096}
	combinedSlices := meta.EncodeSlices([]meta.Slice{baseSlice, expectedHoleSlice})

	setupMockWriteTx(mockStore, mockQuerier)

	mockQuerier.EXPECT().
		GetInodeSizeAndChunksInRange(mock.Anything, db.GetInodeSizeAndChunksInRangeParams{
			Ino:       int64(ino),
			StartIndx: 0,
			EndIndx:   0,
		}).
		Return([]db.GetInodeSizeAndChunksInRangeRow{
			{
				Ino:         fileRow.Ino,
				Mode:        fileRow.Mode,
				Size:        fileRow.Size,
				Atime:       fileRow.Atime,
				Mtime:       fileRow.Mtime,
				Ctime:       fileRow.Ctime,
				ChunkIndx:   pgtype.Int8{Int64: 0, Valid: true},
				ChunkSlices: combinedSlices,
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

	mockQuerier.EXPECT().
		LockValidSliceRefsKeyShare(mock.Anything, []int64{100}).
		Return([]int64{100}, nil).
		Once()

	// 准备 BlockStore 物理数据：4096 字节，内容为 'A'
	// BuildSlice 视图为：
	// 1. [0, 1024): sliceID 100, off=0, len=1024
	// 2. [1024, 3072): 空洞切片 (ID: 0)，不调用 GetBlock
	// 3. [3072, 4096): sliceID 100, off=3072, len=1024
	mockBS.EXPECT().
		GetBlock(mock.Anything, uint64(100), uint32(0), uint32(4096), uint32(0), mock.Anything).
		RunAndReturn(func(ctx context.Context, sid uint64, bIdx, bSize, off uint32, dst []byte) error {
			require.Len(t, dst, 1024)
			copy(dst, bytes.Repeat([]byte("A"), 1024))
			return nil
		}).
		Once()

	mockBS.EXPECT().
		GetBlock(mock.Anything, uint64(100), uint32(0), uint32(4096), uint32(3072), mock.Anything).
		RunAndReturn(func(ctx context.Context, sid uint64, bIdx, bSize, off uint32, dst []byte) error {
			require.Len(t, dst, 1024)
			copy(dst, bytes.Repeat([]byte("A"), 1024))
			return nil
		}).
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

	// 验证：[0, 1024) 为 'A'，[1024, 3072) 全为 0x00，[3072, 4096) 为 'A'
	assert.True(t, bytes.Equal(readResp.Data[0:1024], bytes.Repeat([]byte("A"), 1024)))
	assert.True(t, bytes.Equal(readResp.Data[1024:3072], make([]byte, 2048)), "中间打洞区间必须为 0x00")
	assert.True(t, bytes.Equal(readResp.Data[3072:4096], bytes.Repeat([]byte("A"), 1024)))
}

// TestFallocatePunchHole_CrossChunkBoundary_ReadHoleZero 验证跨越 64 MiB Chunk 边界的打洞操作：
// 自动在相邻的 Chunk 0 尾部和 Chunk 1 头部追加局部空洞 Slice，Read 读取时跨界打洞区域全部返回 0x00。
func TestFallocatePunchHole_CrossChunkBoundary_ReadHoleZero(t *testing.T) {
	ctx := context.Background()
	now := time.Now().Truncate(time.Microsecond)
	ino := uint64(5002)
	fh := uint64(502)
	fileSize := int64(2 * meta.ChunkSize) // 128 MiB

	fileRow := db.Inode{
		Ino:        int64(ino),
		Generation: 1,
		Mode:       int32(meta.S_IFREG | 0o644),
		Size:       fileSize,
		Nlink:      1,
		Uid:        1000,
		Gid:        1000,
		Atime:      pgtype.Timestamptz{Time: now, Valid: true},
		Mtime:      pgtype.Timestamptz{Time: now, Valid: true},
		Ctime:      pgtype.Timestamptz{Time: now, Valid: true},
	}

	mockStore := dbmock.NewMockStore(t)
	mockQuerier := dbmock.NewMockQuerier(t)
	mockBS := metamock.NewMockBlockStore(t)
	mgr := meta.New(mockStore, meta.WithBlockStore(mockBS))

	setupMockWriteTx(mockStore, mockQuerier)

	mockQuerier.EXPECT().
		GetInodeForUpdate(mock.Anything, int64(ino)).
		Return(fileRow, nil).
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

	// 打洞区间：[ChunkSize - 1024, ChunkSize + 2048)，总长度 3072
	// Chunk 0: [ChunkSize - 1024, ChunkSize)，长度 1024
	expectedHoleChunk0 := meta.Slice{
		Pos:  uint32(meta.ChunkSize - 1024),
		ID:   0,
		Size: 0,
		Off:  0,
		Len:  1024,
	}
	mockQuerier.EXPECT().
		AppendExistingChunkSlice(mock.Anything, db.AppendExistingChunkSliceParams{
			Ino:    int64(ino),
			Indx:   0,
			Slices: meta.EncodeSlice(expectedHoleChunk0),
		}).
		Return(int64(1), nil).
		Once()

	// Chunk 1: [0, 2048)，长度 2048
	expectedHoleChunk1 := meta.Slice{
		Pos:  0,
		ID:   0,
		Size: 0,
		Off:  0,
		Len:  2048,
	}
	mockQuerier.EXPECT().
		AppendExistingChunkSlice(mock.Anything, db.AppendExistingChunkSliceParams{
			Ino:    int64(ino),
			Indx:   1,
			Slices: meta.EncodeSlice(expectedHoleChunk1),
		}).
		Return(int64(1), nil).
		Once()

	mockQuerier.EXPECT().
		UpdateInodeAttr(mock.Anything, mock.MatchedBy(func(p db.UpdateInodeAttrParams) bool {
			return p.Ino == int64(ino) && p.Size == fileSize && p.SetMtimeNow
		})).
		Return(fileRow, nil).
		Once()

	err := mgr.Fallocate(ctx, meta.FallocateParams{
		Ino:    ino,
		Fh:     fh,
		Mode:   meta.FallocFlPunchHole | meta.FallocFlKeepSize,
		Offset: uint64(meta.ChunkSize - 1024),
		Length: 3072,
	})
	require.NoError(t, err)

	// 2. 执行跨 Chunk 边界 Read 读取：
	// 读取区间 [ChunkSize - 2048, ChunkSize + 4096)，总长度 6144 字节
	// Chunk 0: 原始切片 (ID: 10) + 打洞切片 (ID: 0, Pos: ChunkSize - 1024, Len: 1024)
	baseSlice0 := meta.Slice{Pos: 0, ID: 10, Size: meta.ChunkSize, Off: 0, Len: meta.ChunkSize}
	chunk0Slices := meta.EncodeSlices([]meta.Slice{baseSlice0, expectedHoleChunk0})

	// Chunk 1: 原始切片 (ID: 20) + 打洞切片 (ID: 0, Pos: 0, Len: 2048)
	baseSlice1 := meta.Slice{Pos: 0, ID: 20, Size: meta.ChunkSize, Off: 0, Len: meta.ChunkSize}
	chunk1Slices := meta.EncodeSlices([]meta.Slice{baseSlice1, expectedHoleChunk1})

	setupMockWriteTx(mockStore, mockQuerier)

	mockQuerier.EXPECT().
		GetInodeSizeAndChunksInRange(mock.Anything, db.GetInodeSizeAndChunksInRangeParams{
			Ino:       int64(ino),
			StartIndx: 0,
			EndIndx:   1,
		}).
		Return([]db.GetInodeSizeAndChunksInRangeRow{
			{
				Ino:         fileRow.Ino,
				Mode:        fileRow.Mode,
				Size:        fileRow.Size,
				Atime:       fileRow.Atime,
				Mtime:       fileRow.Mtime,
				Ctime:       fileRow.Ctime,
				ChunkIndx:   pgtype.Int8{Int64: 0, Valid: true},
				ChunkSlices: chunk0Slices,
			},
			{
				Ino:         fileRow.Ino,
				Mode:        fileRow.Mode,
				Size:        fileRow.Size,
				Atime:       fileRow.Atime,
				Mtime:       fileRow.Mtime,
				Ctime:       fileRow.Ctime,
				ChunkIndx:   pgtype.Int8{Int64: 1, Valid: true},
				ChunkSlices: chunk1Slices,
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

	mockQuerier.EXPECT().
		LockValidSliceRefsKeyShare(mock.Anything, []int64{10, 20}).
		Return([]int64{10, 20}, nil).
		Once()

	// Chunk 0 未打洞区域读取 [ChunkSize - 2048, ChunkSize - 1024)，长度 1024
	mockBS.EXPECT().
		GetBlock(mock.Anything, uint64(10), uint32((meta.ChunkSize-2048)/meta.BlockSize), uint32(meta.BlockSize), uint32((meta.ChunkSize-2048)%meta.BlockSize), mock.Anything).
		RunAndReturn(func(ctx context.Context, sid uint64, bIdx, bSize, off uint32, dst []byte) error {
			require.Len(t, dst, 1024)
			copy(dst, bytes.Repeat([]byte("X"), 1024))
			return nil
		}).
		Once()

	// Chunk 1 未打洞区域读取 [2048, 4096)，长度 2048
	mockBS.EXPECT().
		GetBlock(mock.Anything, uint64(20), uint32(0), uint32(meta.BlockSize), uint32(2048), mock.Anything).
		RunAndReturn(func(ctx context.Context, sid uint64, bIdx, bSize, off uint32, dst []byte) error {
			require.Len(t, dst, 2048)
			copy(dst, bytes.Repeat([]byte("Y"), 2048))
			return nil
		}).
		Once()

	readResp, err := mgr.Read(ctx, &meta.ReadRequest{
		Ino:    ino,
		Fh:     fh,
		Offset: uint64(meta.ChunkSize - 2048),
		Size:   6144,
	})
	require.NoError(t, err)
	require.NotNil(t, readResp)
	assert.Equal(t, 6144, len(readResp.Data))

	// 断言：
	// [0, 1024) 为 'X'（Chunk 0 未打洞区域）
	// [1024, 4096) 共 3072 字节全为 0x00（跨界打洞区域）
	// [4096, 6144) 共 2048 字节为 'Y'（Chunk 1 未打洞区域）
	assert.True(t, bytes.Equal(readResp.Data[0:1024], bytes.Repeat([]byte("X"), 1024)))
	assert.True(t, bytes.Equal(readResp.Data[1024:4096], make([]byte, 3072)), "跨越 64 MiB 边界的打洞区间必须全为 0x00")
	assert.True(t, bytes.Equal(readResp.Data[4096:6144], bytes.Repeat([]byte("Y"), 2048)))
}

// TestFallocatePunchHole_FullChunk_DeletesChunkAndDecrementsRefs 验证整块 64 MiB 打洞时：
// 直接调用 DeleteChunk 删除整块 chunks 记录并级联递减引用计数 slice_refs.refs。
func TestFallocatePunchHole_FullChunk_DeletesChunkAndDecrementsRefs(t *testing.T) {
	ctx := context.Background()
	now := time.Now().Truncate(time.Microsecond)
	ino := uint64(5003)
	fh := uint64(503)
	fileSize := int64(3 * meta.ChunkSize) // 192 MiB

	fileRow := db.Inode{
		Ino:        int64(ino),
		Generation: 1,
		Mode:       int32(meta.S_IFREG | 0o644),
		Size:       fileSize,
		Nlink:      1,
		Uid:        1000,
		Gid:        1000,
		Atime:      pgtype.Timestamptz{Time: now, Valid: true},
		Mtime:      pgtype.Timestamptz{Time: now, Valid: true},
		Ctime:      pgtype.Timestamptz{Time: now, Valid: true},
	}

	t.Run("单整块打洞完全覆盖 Chunk 1", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockWriteTx(mockStore, mockQuerier)

		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(ino)).
			Return(fileRow, nil).
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

		deletedSliceBytes := meta.EncodeSlices([]meta.Slice{
			{Pos: 0, ID: 201, Size: meta.ChunkSize, Off: 0, Len: meta.ChunkSize},
		})

		// 核心断言：整块覆盖时调用 DeleteChunk
		mockQuerier.EXPECT().
			DeleteChunk(mock.Anything, db.DeleteChunkParams{
				Ino:  int64(ino),
				Indx: 1,
			}).
			Return([][]byte{deletedSliceBytes}, nil).
			Once()

		// 核心断言：递减 slice_refs.refs
		mockQuerier.EXPECT().
			AddSliceRefCount(mock.Anything, db.AddSliceRefCountParams{
				SliceID: 201,
				Delta:   -1,
			}).
			Return(nil).
			Once()

		mockQuerier.EXPECT().
			UpdateInodeAttr(mock.Anything, mock.MatchedBy(func(p db.UpdateInodeAttrParams) bool {
				return p.Ino == int64(ino) && p.Size == fileSize && p.SetMtimeNow
			})).
			Return(fileRow, nil).
			Once()

		// 在 Chunk 1 整块打洞：Offset = ChunkSize, Length = ChunkSize
		err := mgr.Fallocate(ctx, meta.FallocateParams{
			Ino:    ino,
			Fh:     fh,
			Mode:   meta.FallocFlPunchHole | meta.FallocFlKeepSize,
			Offset: uint64(meta.ChunkSize),
			Length: uint64(meta.ChunkSize),
		})
		require.NoError(t, err)

		// 严防整块覆盖时多余调用 AppendExistingChunkSlice
		mockQuerier.AssertNotCalled(t, "AppendExistingChunkSlice", mock.Anything, mock.Anything)
	})

	t.Run("跨多块打洞：头部局部追加空洞、中间整块删除且递减引用、尾部局部追加空洞", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockWriteTx(mockStore, mockQuerier)

		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(ino)).
			Return(fileRow, nil).
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

		// 打洞区间：从 Chunk 0 的 1024 处一直打洞到 Chunk 2 的 2048 处
		// punchStart = 1024, punchEnd = 2*ChunkSize + 2048
		// Chunk 0 (indx=0): [1024, ChunkSize) 局部打洞
		hole0 := meta.Slice{
			Pos:  1024,
			ID:   0,
			Size: 0,
			Off:  0,
			Len:  uint32(meta.ChunkSize - 1024),
		}
		mockQuerier.EXPECT().
			AppendExistingChunkSlice(mock.Anything, db.AppendExistingChunkSliceParams{
				Ino:    int64(ino),
				Indx:   0,
				Slices: meta.EncodeSlice(hole0),
			}).
			Return(int64(1), nil).
			Once()

		// Chunk 1 (indx=1): [0, ChunkSize) 整块覆盖 -> DeleteChunk
		deletedSlicesChunk1 := meta.EncodeSlices([]meta.Slice{
			{Pos: 0, ID: 301, Size: meta.ChunkSize, Off: 0, Len: meta.ChunkSize},
		})
		mockQuerier.EXPECT().
			DeleteChunk(mock.Anything, db.DeleteChunkParams{
				Ino:  int64(ino),
				Indx: 1,
			}).
			Return([][]byte{deletedSlicesChunk1}, nil).
			Once()

		mockQuerier.EXPECT().
			AddSliceRefCount(mock.Anything, db.AddSliceRefCountParams{
				SliceID: 301,
				Delta:   -1,
			}).
			Return(nil).
			Once()

		// Chunk 2 (indx=2): [0, 2048) 局部打洞
		hole2 := meta.Slice{
			Pos:  0,
			ID:   0,
			Size: 0,
			Off:  0,
			Len:  2048,
		}
		mockQuerier.EXPECT().
			AppendExistingChunkSlice(mock.Anything, db.AppendExistingChunkSliceParams{
				Ino:    int64(ino),
				Indx:   2,
				Slices: meta.EncodeSlice(hole2),
			}).
			Return(int64(1), nil).
			Once()

		mockQuerier.EXPECT().
			UpdateInodeAttr(mock.Anything, mock.MatchedBy(func(p db.UpdateInodeAttrParams) bool {
				return p.Ino == int64(ino) && p.Size == fileSize && p.SetMtimeNow
			})).
			Return(fileRow, nil).
			Once()

		err := mgr.Fallocate(ctx, meta.FallocateParams{
			Ino:    ino,
			Fh:     fh,
			Mode:   meta.FallocFlPunchHole | meta.FallocFlKeepSize,
			Offset: 1024,
			Length: 2*uint64(meta.ChunkSize) + 2048 - 1024,
		})
		require.NoError(t, err)
	})
}

// TestFallocatePunchHole_SparseAndOutOfBound_NoExtraChunks 验证在未写入的稀疏区间或超出 current.Size 处打洞时的边界与幂等性。
func TestFallocatePunchHole_SparseAndOutOfBound_NoExtraChunks(t *testing.T) {
	ctx := context.Background()
	now := time.Now().Truncate(time.Microsecond)
	ino := uint64(5004)
	fh := uint64(504)
	fileSize := int64(1024)

	fileRow := db.Inode{
		Ino:        int64(ino),
		Generation: 1,
		Mode:       int32(meta.S_IFREG | 0o644),
		Size:       fileSize,
		Nlink:      1,
		Uid:        1000,
		Gid:        1000,
		Atime:      pgtype.Timestamptz{Time: now, Valid: true},
		Mtime:      pgtype.Timestamptz{Time: now, Valid: true},
		Ctime:      pgtype.Timestamptz{Time: now, Valid: true},
	}

	t.Run("打洞起始偏移超过 current.Size 时直接成功且不触发任何 chunks 表写操作", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockWriteTx(mockStore, mockQuerier)

		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(ino)).
			Return(fileRow, nil).
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
				return p.Ino == int64(ino) && p.Size == fileSize && p.SetMtimeNow
			})).
			Return(fileRow, nil).
			Once()

		// Offset = 2048 >= fileSize (1024)
		err := mgr.Fallocate(ctx, meta.FallocateParams{
			Ino:    ino,
			Fh:     fh,
			Mode:   meta.FallocFlPunchHole | meta.FallocFlKeepSize,
			Offset: 2048,
			Length: 1024,
		})
		require.NoError(t, err)

		mockQuerier.AssertNotCalled(t, "DeleteChunk", mock.Anything, mock.Anything)
		mockQuerier.AssertNotCalled(t, "AppendExistingChunkSlice", mock.Anything, mock.Anything)
	})

	t.Run("文件大小为 0 时打洞不触发任何 chunks 表写操作", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockWriteTx(mockStore, mockQuerier)

		zeroFileRow := fileRow
		zeroFileRow.Size = 0

		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(ino)).
			Return(zeroFileRow, nil).
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
				return p.Ino == int64(ino) && p.Size == 0 && p.SetMtimeNow
			})).
			Return(zeroFileRow, nil).
			Once()

		err := mgr.Fallocate(ctx, meta.FallocateParams{
			Ino:    ino,
			Fh:     fh,
			Mode:   meta.FallocFlPunchHole | meta.FallocFlKeepSize,
			Offset: 0,
			Length: 1024,
		})
		require.NoError(t, err)

		mockQuerier.AssertNotCalled(t, "DeleteChunk", mock.Anything, mock.Anything)
		mockQuerier.AssertNotCalled(t, "AppendExistingChunkSlice", mock.Anything, mock.Anything)
	})

	t.Run("整块删除未写入的稀疏块时优雅容错", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockWriteTx(mockStore, mockQuerier)

		largeFileRow := fileRow
		largeFileRow.Size = int64(meta.ChunkSize)

		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(ino)).
			Return(largeFileRow, nil).
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

		// 模拟原本是稀疏块，DeleteChunk 返回 nil (0 行匹配) 或 pgx.ErrNoRows
		mockQuerier.EXPECT().
			DeleteChunk(mock.Anything, db.DeleteChunkParams{
				Ino:  int64(ino),
				Indx: 0,
			}).
			Return(nil, pgx.ErrNoRows).
			Once()

		mockQuerier.EXPECT().
			UpdateInodeAttr(mock.Anything, mock.MatchedBy(func(p db.UpdateInodeAttrParams) bool {
				return p.Ino == int64(ino) && p.Size == int64(meta.ChunkSize) && p.SetMtimeNow
			})).
			Return(largeFileRow, nil).
			Once()

		err := mgr.Fallocate(ctx, meta.FallocateParams{
			Ino:    ino,
			Fh:     fh,
			Mode:   meta.FallocFlPunchHole | meta.FallocFlKeepSize,
			Offset: 0,
			Length: uint64(meta.ChunkSize),
		})
		require.NoError(t, err)
		mockQuerier.AssertNotCalled(t, "AddSliceRefCount", mock.Anything, mock.Anything)
	})
}

// TestFallocatePunchHole_HighSparseChunkIndxBeyondInt32_PostgresIntegration 验证在超过 128 PiB
// (indx >= 1<<31) 的高稀疏偏移处执行 Fallocate 打洞时，startIndx/endIndx 保持 64 位精度，
// 精确清理或打洞目标高偏移 Chunk，不因 32 位截断回绕误删 indx=0 数据。
func TestFallocatePunchHole_HighSparseChunkIndxBeyondInt32_PostgresIntegration(t *testing.T) {
	pool, ctx := setupMetaTestDB(t)
	if pool == nil {
		return
	}

	store := db.NewStore(pool)
	dataDir := t.TempDir()
	bs := meta.NewLocalFSBlockStore(dataDir)
	mgr := meta.New(store, meta.WithBlockStore(bs))

	fileRow, err := store.CreateInode(ctx, db.CreateInodeParams{
		Mode:  int32(meta.S_IFREG | 0o644),
		Nlink: 1,
		Uid:   1000,
		Gid:   1000,
	})
	require.NoError(t, err)
	ino := uint64(fileRow.Ino)

	highIndx := int64(1) << 31 // 2147483648 (offset = 128 PiB = 1<<57)
	highOffset := uint64(highIndx) * uint64(meta.ChunkSize)

	t.Cleanup(func() {
		for _, indx := range []int64{0, highIndx} {
			chunkRow, _ := store.GetChunk(context.Background(), db.GetChunkParams{Ino: int64(ino), Indx: indx})
			if len(chunkRow.Slices) > 0 {
				if slices, err := meta.DecodeSlices(chunkRow.Slices); err == nil {
					for _, s := range slices {
						if s.ID > 0 {
							_ = store.DeleteSliceRef(context.Background(), int64(s.ID))
						}
					}
				}
			}
		}
		_ = meta.DeleteAllChunksByInos(context.Background(), store, []int64{int64(ino)})
		_ = store.DeleteInode(context.Background(), int64(ino))
	})

	openRes, err := mgr.Open(ctx, meta.OpenParams{
		Ino:   ino,
		Flags: meta.O_RDWR,
	})
	require.NoError(t, err)
	fh := openRes.Fh
	t.Cleanup(func() {
		_ = mgr.Release(context.Background(), meta.ReleaseParams{Ino: ino, Fh: fh})
	})

	lowData := bytes.Repeat([]byte("L"), 4096)
	highData := bytes.Repeat([]byte("H"), 4096)

	_, err = mgr.Write(ctx, &meta.WriteRequest{
		Ino:    ino,
		Fh:     fh,
		Offset: 0,
		Data:   lowData,
	})
	require.NoError(t, err)

	_, err = mgr.Write(ctx, &meta.WriteRequest{
		Ino:    ino,
		Fh:     fh,
		Offset: highOffset,
		Data:   highData,
	})
	require.NoError(t, err)

	// 在 highOffset + 1024 处打洞 2048 字节
	err = mgr.Fallocate(ctx, meta.FallocateParams{
		Ino:    ino,
		Fh:     fh,
		Mode:   meta.FallocFlPunchHole | meta.FallocFlKeepSize,
		Offset: highOffset + 1024,
		Length: 2048,
	})
	require.NoError(t, err)

	// 验证 indx=0 完好无损
	readLow, err := mgr.Read(ctx, &meta.ReadRequest{
		Ino:    ino,
		Fh:     fh,
		Offset: 0,
		Size:   4096,
	})
	require.NoError(t, err)
	assert.Equal(t, lowData, readLow.Data, "高偏移打洞绝不能因 32 位回绕污染 indx=0 的数据")

	// 验证 highIndx 中间 [1024, 3072) 已变为 0x00，首尾各 1024B 仍为 'H'
	readHigh, err := mgr.Read(ctx, &meta.ReadRequest{
		Ino:    ino,
		Fh:     fh,
		Offset: highOffset,
		Size:   4096,
	})
	require.NoError(t, err)
	expectedHigh := make([]byte, 4096)
	copy(expectedHigh[0:1024], highData[0:1024])
	copy(expectedHigh[3072:4096], highData[3072:4096])
	assert.Equal(t, expectedHigh, readHigh.Data, "highIndx 打洞区间 [1024, 3072) 必须读零")
}
