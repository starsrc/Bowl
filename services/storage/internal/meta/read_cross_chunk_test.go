package meta_test

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/starsrc/Aether/pkg/tx"
	"github.com/starsrc/bowl/services/storage/internal/db"
	dbmock "github.com/starsrc/bowl/services/storage/internal/db/mock"
	"github.com/starsrc/bowl/services/storage/internal/meta"
	metamock "github.com/starsrc/bowl/services/storage/internal/meta/mock"
)

// TestRead_CrossBlockBoundary 验证单次 Read 命中的有效 Slice 跨越 4 MiB 物理 Block 边界时的分块读取与拼接。
func TestRead_CrossBlockBoundary(t *testing.T) {
	ctx := context.Background()
	now := time.Now().Truncate(time.Microsecond)
	ino := uint64(801)
	fh := uint64(81)

	fileRow := db.Inode{
		Ino:   int64(ino),
		Mode:  int32(meta.S_IFREG | 0o644),
		Size:  8 * 1024 * 1024, // 8 MiB 文件大小
		Atime: pgtype.Timestamptz{Time: now, Valid: true},
		Mtime: pgtype.Timestamptz{Time: now, Valid: true},
		Ctime: pgtype.Timestamptz{Time: now, Valid: true},
	}

	t.Run("跨越 4 MiB Block 边界读取 2 MiB 数据拼接验证", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mockBS := metamock.NewMockBlockStore(t)
		mgr := meta.New(mockStore, meta.WithBlockStore(mockBS))

		setupMockWriteTx(mockStore, mockQuerier)

		sliceID := uint64(9001)
		// 构造一个 8 MiB 大小的单个 Slice，包含两个 4 MiB Block (Block 0 与 Block 1)
		sliceDesc := meta.Slice{
			Pos:  0,
			ID:   sliceID,
			Size: 8 * 1024 * 1024,
			Off:  0,
			Len:  8 * 1024 * 1024,
		}

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
					ChunkSlices: meta.EncodeSlice(sliceDesc),
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

		mockQuerier.EXPECT().
			LockValidSliceRefsKeyShare(mock.Anything, []int64{int64(sliceID)}).
			Return([]int64{int64(sliceID)}, nil).
			Once()

		// 读取区间 [3 MiB, 5 MiB)，长度 2 MiB:
		// 跨越 4 MiB Block 边界：
		// - Block 0 (BlockIdx=0, BlockSize=4 MiB): BlockOff = 3 MiB, Length = 1 MiB
		// - Block 1 (BlockIdx=1, BlockSize=4 MiB): BlockOff = 0, Length = 1 MiB
		part1 := bytes.Repeat([]byte{0xAA}, 1024*1024)
		part2 := bytes.Repeat([]byte{0xBB}, 1024*1024)

		mockBS.EXPECT().
			GetBlock(mock.Anything, sliceID, uint32(0), uint32(meta.BlockSize), uint32(3*1024*1024), mock.Anything).
			RunAndReturn(func(ctx context.Context, sid uint64, bIdx, bSize, off uint32, dst []byte) error {
				require.Len(t, dst, 1024*1024)
				copy(dst, part1)
				return nil
			}).
			Once()

		mockBS.EXPECT().
			GetBlock(mock.Anything, sliceID, uint32(1), uint32(meta.BlockSize), uint32(0), mock.Anything).
			RunAndReturn(func(ctx context.Context, sid uint64, bIdx, bSize, off uint32, dst []byte) error {
				require.Len(t, dst, 1024*1024)
				copy(dst, part2)
				return nil
			}).
			Once()

		resp, err := mgr.Read(ctx, &meta.ReadRequest{
			Ino:    ino,
			Fh:     fh,
			Offset: 3 * 1024 * 1024,
			Size:   2 * 1024 * 1024,
		})
		require.NoError(t, err)
		require.NotNil(t, resp)
		expected := append(part1, part2...)
		assert.Equal(t, expected, resp.Data)
	})
}

// TestRead_CrossChunkBoundary 验证单次 Read 跨越 64 MiB Chunk 边界 (startIndx != endIndx) 时的拼接。
func TestRead_CrossChunkBoundary(t *testing.T) {
	ctx := context.Background()
	now := time.Now().Truncate(time.Microsecond)
	ino := uint64(802)
	fh := uint64(82)

	// 文件总大小 128 MiB (覆盖 2 个 64 MiB Chunk)
	fileRow := db.Inode{
		Ino:   int64(ino),
		Mode:  int32(meta.S_IFREG | 0o644),
		Size:  128 * 1024 * 1024,
		Atime: pgtype.Timestamptz{Time: now, Valid: true},
		Mtime: pgtype.Timestamptz{Time: now, Valid: true},
		Ctime: pgtype.Timestamptz{Time: now, Valid: true},
	}

	t.Run("跨 64 MiB 边界读取 1 MiB (左右各 512 KiB) 正常拼接返回", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mockBS := metamock.NewMockBlockStore(t)
		mgr := meta.New(mockStore, meta.WithBlockStore(mockBS))

		setupMockWriteTx(mockStore, mockQuerier)

		sliceID1 := uint64(1001)
		sliceID2 := uint64(1002)

		// Chunk 0: 在 [ChunkSize-512KiB, ChunkSize) 处有切片
		slice0 := meta.Slice{
			Pos:  uint32(meta.ChunkSize - 512*1024),
			ID:   sliceID1,
			Size: 512 * 1024,
			Off:  0,
			Len:  512 * 1024,
		}

		// Chunk 1: 在 [0, 512KiB) 处有切片
		slice1 := meta.Slice{
			Pos:  0,
			ID:   sliceID2,
			Size: 512 * 1024,
			Off:  0,
			Len:  512 * 1024,
		}

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
					ChunkSlices: meta.EncodeSlice(slice0),
				},
				{
					Ino:         fileRow.Ino,
					Mode:        fileRow.Mode,
					Size:        fileRow.Size,
					Atime:       fileRow.Atime,
					Mtime:       fileRow.Mtime,
					Ctime:       fileRow.Ctime,
					ChunkIndx:   pgtype.Int8{Int64: 1, Valid: true},
					ChunkSlices: meta.EncodeSlice(slice1),
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

		mockQuerier.EXPECT().
			LockValidSliceRefsKeyShare(mock.Anything, []int64{int64(sliceID1), int64(sliceID2)}).
			Return([]int64{int64(sliceID1), int64(sliceID2)}, nil).
			Once()

		part1 := bytes.Repeat([]byte{0x11}, 512*1024)
		part2 := bytes.Repeat([]byte{0x22}, 512*1024)

		mockBS.EXPECT().
			GetBlock(mock.Anything, sliceID1, uint32(0), uint32(512*1024), uint32(0), mock.Anything).
			RunAndReturn(func(ctx context.Context, sid uint64, bIdx, bSize, off uint32, dst []byte) error {
				require.Len(t, dst, 512*1024)
				copy(dst, part1)
				return nil
			}).
			Once()

		mockBS.EXPECT().
			GetBlock(mock.Anything, sliceID2, uint32(0), uint32(512*1024), uint32(0), mock.Anything).
			RunAndReturn(func(ctx context.Context, sid uint64, bIdx, bSize, off uint32, dst []byte) error {
				require.Len(t, dst, 512*1024)
				copy(dst, part2)
				return nil
			}).
			Once()

		// 请求读取 [ChunkSize - 512 KiB, ChunkSize + 512 KiB)，共 1 MiB
		resp, err := mgr.Read(ctx, &meta.ReadRequest{
			Ino:    ino,
			Fh:     fh,
			Offset: uint64(meta.ChunkSize - 512*1024),
			Size:   1024 * 1024,
		})
		require.NoError(t, err)
		require.NotNil(t, resp)
		expected := append(part1, part2...)
		assert.Equal(t, expected, resp.Data)
	})

	t.Run("左空洞跨界读取：Chunk 0 不存在 (读零)，Chunk 1 有数据", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mockBS := metamock.NewMockBlockStore(t)
		mgr := meta.New(mockStore, meta.WithBlockStore(mockBS))

		setupMockWriteTx(mockStore, mockQuerier)

		sliceID2 := uint64(1003)
		slice1 := meta.Slice{
			Pos:  0,
			ID:   sliceID2,
			Size: 512 * 1024,
			Off:  0,
			Len:  512 * 1024,
		}

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
					ChunkIndx:   pgtype.Int8{Int64: 1, Valid: true},
					ChunkSlices: meta.EncodeSlice(slice1),
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

		mockQuerier.EXPECT().
			LockValidSliceRefsKeyShare(mock.Anything, []int64{int64(sliceID2)}).
			Return([]int64{int64(sliceID2)}, nil).
			Once()

		part2 := bytes.Repeat([]byte{0x33}, 512*1024)
		mockBS.EXPECT().
			GetBlock(mock.Anything, sliceID2, uint32(0), uint32(512*1024), uint32(0), mock.Anything).
			RunAndReturn(func(ctx context.Context, sid uint64, bIdx, bSize, off uint32, dst []byte) error {
				require.Len(t, dst, 512*1024)
				copy(dst, part2)
				return nil
			}).
			Once()

		resp, err := mgr.Read(ctx, &meta.ReadRequest{
			Ino:    ino,
			Fh:     fh,
			Offset: uint64(meta.ChunkSize - 512*1024),
			Size:   1024 * 1024,
		})
		require.NoError(t, err)
		require.NotNil(t, resp)
		expected := append(make([]byte, 512*1024), part2...)
		assert.Equal(t, expected, resp.Data)
	})

	t.Run("右空洞跨界读取：Chunk 0 有数据，Chunk 1 不存在 (读零)", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mockBS := metamock.NewMockBlockStore(t)
		mgr := meta.New(mockStore, meta.WithBlockStore(mockBS))

		setupMockWriteTx(mockStore, mockQuerier)

		// Chunk 0: [ChunkSize-512KiB, ChunkSize)
		sliceID1 := uint64(1004)
		slice0 := meta.Slice{
			Pos:  uint32(meta.ChunkSize - 512*1024),
			ID:   sliceID1,
			Size: 512 * 1024,
			Off:  0,
			Len:  512 * 1024,
		}

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
					ChunkSlices: meta.EncodeSlice(slice0),
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

		mockQuerier.EXPECT().
			LockValidSliceRefsKeyShare(mock.Anything, []int64{int64(sliceID1)}).
			Return([]int64{int64(sliceID1)}, nil).
			Once()

		part1 := bytes.Repeat([]byte{0x44}, 512*1024)
		mockBS.EXPECT().
			GetBlock(mock.Anything, sliceID1, uint32(0), uint32(512*1024), uint32(0), mock.Anything).
			RunAndReturn(func(ctx context.Context, sid uint64, bIdx, bSize, off uint32, dst []byte) error {
				require.Len(t, dst, 512*1024)
				copy(dst, part1)
				return nil
			}).
			Once()

		resp, err := mgr.Read(ctx, &meta.ReadRequest{
			Ino:    ino,
			Fh:     fh,
			Offset: uint64(meta.ChunkSize - 512*1024),
			Size:   1024 * 1024,
		})
		require.NoError(t, err)
		require.NotNil(t, resp)
		expected := append(part1, make([]byte, 512*1024)...)
		assert.Equal(t, expected, resp.Data)
	})

	t.Run("双侧均为稀疏空洞跨界读取全零", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mockBS := metamock.NewMockBlockStore(t)
		mgr := meta.New(mockStore, meta.WithBlockStore(mockBS))

		setupMockWriteTx(mockStore, mockQuerier)

		mockQuerier.EXPECT().
			GetInodeSizeAndChunksInRange(mock.Anything, db.GetInodeSizeAndChunksInRangeParams{
				Ino:       int64(ino),
				StartIndx: 0,
				EndIndx:   1,
			}).
			Return([]db.GetInodeSizeAndChunksInRangeRow{
				{
					Ino:       fileRow.Ino,
					Mode:      fileRow.Mode,
					Size:      fileRow.Size,
					Atime:     fileRow.Atime,
					Mtime:     fileRow.Mtime,
					Ctime:     fileRow.Ctime,
					ChunkIndx: pgtype.Int8{Valid: false},
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

		resp, err := mgr.Read(ctx, &meta.ReadRequest{
			Ino:    ino,
			Fh:     fh,
			Offset: uint64(meta.ChunkSize - 256*1024),
			Size:   512 * 1024,
		})
		require.NoError(t, err)
		require.NotNil(t, resp)
		assert.Equal(t, make([]byte, 512*1024), resp.Data)
	})

	t.Run("跨 64 MiB 边界叠加多次随机覆写精确读取", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mockBS := metamock.NewMockBlockStore(t)
		mgr := meta.New(mockStore, meta.WithBlockStore(mockBS))

		setupMockWriteTx(mockStore, mockQuerier)

		// Chunk 0 包含底图 [ChunkSize-512KiB, ChunkSize) 与后续尾部覆写 [ChunkSize-128KiB, ChunkSize)
		baseSliceID0 := uint64(2001)
		overSliceID0 := uint64(2002)
		chunk0Slices := []meta.Slice{
			{Pos: uint32(meta.ChunkSize - 512*1024), ID: baseSliceID0, Size: 512 * 1024, Off: 0, Len: 512 * 1024},
			{Pos: uint32(meta.ChunkSize - 128*1024), ID: overSliceID0, Size: 128 * 1024, Off: 0, Len: 128 * 1024},
		}

		// Chunk 1 包含底图 [0, 512KiB) 与后续头部覆写 [0, 64KiB)
		baseSliceID1 := uint64(2003)
		overSliceID1 := uint64(2004)
		chunk1Slices := []meta.Slice{
			{Pos: 0, ID: baseSliceID1, Size: 512 * 1024, Off: 0, Len: 512 * 1024},
			{Pos: 0, ID: overSliceID1, Size: 64 * 1024, Off: 0, Len: 64 * 1024},
		}

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
					ChunkSlices: meta.EncodeSlices(chunk0Slices),
				},
				{
					Ino:         fileRow.Ino,
					Mode:        fileRow.Mode,
					Size:        fileRow.Size,
					Atime:       fileRow.Atime,
					Mtime:       fileRow.Mtime,
					Ctime:       fileRow.Ctime,
					ChunkIndx:   pgtype.Int8{Int64: 1, Valid: true},
					ChunkSlices: meta.EncodeSlices(chunk1Slices),
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

		mockQuerier.EXPECT().
			LockValidSliceRefsKeyShare(mock.Anything, []int64{int64(baseSliceID0), int64(overSliceID0), int64(baseSliceID1), int64(overSliceID1)}).
			Return([]int64{int64(baseSliceID0), int64(overSliceID0), int64(baseSliceID1), int64(overSliceID1)}, nil).
			Once()

		dataBase0 := bytes.Repeat([]byte{0x55}, 384*1024)
		dataOver0 := bytes.Repeat([]byte{0x66}, 128*1024)
		dataOver1 := bytes.Repeat([]byte{0x77}, 64*1024)
		dataBase1 := bytes.Repeat([]byte{0x88}, 448*1024)

		// Chunk 0:
		// [ChunkSize-512KiB, ChunkSize-128KiB): 384 KiB 读 baseSliceID0 (off=0)
		mockBS.EXPECT().
			GetBlock(mock.Anything, baseSliceID0, uint32(0), uint32(512*1024), uint32(0), mock.Anything).
			RunAndReturn(func(ctx context.Context, sid uint64, bIdx, bSize, off uint32, dst []byte) error {
				require.Len(t, dst, 384*1024)
				copy(dst, dataBase0)
				return nil
			}).
			Once()

		// [ChunkSize-128KiB, ChunkSize): 128 KiB 读 overSliceID0 (off=0)
		mockBS.EXPECT().
			GetBlock(mock.Anything, overSliceID0, uint32(0), uint32(128*1024), uint32(0), mock.Anything).
			RunAndReturn(func(ctx context.Context, sid uint64, bIdx, bSize, off uint32, dst []byte) error {
				require.Len(t, dst, 128*1024)
				copy(dst, dataOver0)
				return nil
			}).
			Once()

		// Chunk 1:
		// [0, 64KiB): 64 KiB 读 overSliceID1 (off=0)
		mockBS.EXPECT().
			GetBlock(mock.Anything, overSliceID1, uint32(0), uint32(64*1024), uint32(0), mock.Anything).
			RunAndReturn(func(ctx context.Context, sid uint64, bIdx, bSize, off uint32, dst []byte) error {
				require.Len(t, dst, 64*1024)
				copy(dst, dataOver1)
				return nil
			}).
			Once()

		// [64KiB, 512KiB): 448 KiB 读 baseSliceID1 (off=64KiB)
		mockBS.EXPECT().
			GetBlock(mock.Anything, baseSliceID1, uint32(0), uint32(512*1024), uint32(64*1024), mock.Anything).
			RunAndReturn(func(ctx context.Context, sid uint64, bIdx, bSize, off uint32, dst []byte) error {
				require.Len(t, dst, 448*1024)
				copy(dst, dataBase1)
				return nil
			}).
			Once()

		resp, err := mgr.Read(ctx, &meta.ReadRequest{
			Ino:    ino,
			Fh:     fh,
			Offset: uint64(meta.ChunkSize - 512*1024),
			Size:   1024 * 1024,
		})
		require.NoError(t, err)
		require.NotNil(t, resp)

		var expected []byte
		expected = append(expected, dataBase0...)
		expected = append(expected, dataOver0...)
		expected = append(expected, dataOver1...)
		expected = append(expected, dataBase1...)
		assert.Equal(t, expected, resp.Data)
	})
}

// TestRead_CrossChunk_PostgresIntegration 真实 PostgreSQL + LocalFSBlockStore 集成测试。
func TestRead_CrossChunk_PostgresIntegration(t *testing.T) {
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
	defer func() {
		chunkRow, _ := store.GetChunk(context.Background(), db.GetChunkParams{Ino: fileRow.Ino, Indx: 0})
		if len(chunkRow.Slices) > 0 {
			slices, _ := meta.DecodeSlices(chunkRow.Slices)
			for _, s := range slices {
				_ = store.DeleteSliceRef(context.Background(), int64(s.ID))
			}
		}
		_ = meta.DeleteAllChunksByInos(context.Background(), store, []int64{fileRow.Ino})
		_ = store.DeleteInode(context.Background(), fileRow.Ino)
	}()

	openRes, err := mgr.Open(ctx, meta.OpenParams{
		Ino:   ino,
		Flags: meta.O_RDWR,
	})
	require.NoError(t, err)
	fh := openRes.Fh
	defer func() {
		_ = mgr.Release(context.Background(), meta.ReleaseParams{Ino: ino, Fh: fh})
	}()

	// 1. 在 64 MiB - 512 KiB 处跨界写入 1 MiB 数据 (跨越 Chunk 0 和 Chunk 1)
	crossOffset := uint64(meta.ChunkSize - 512*1024)
	payload := make([]byte, 1024*1024)
	for i := range payload {
		payload[i] = byte((i*7 + 13) % 256)
	}

	writeRes, err := mgr.Write(ctx, &meta.WriteRequest{
		Ino:    ino,
		Fh:     fh,
		Offset: crossOffset,
		Data:   payload,
	})
	require.NoError(t, err)
	require.NotNil(t, writeRes)
	assert.Equal(t, uint32(len(payload)), writeRes.Size)

	// 2. 从 64 MiB - 512 KiB 处完整读取 1 MiB，逐字节校验
	readRes, err := mgr.Read(ctx, &meta.ReadRequest{
		Ino:    ino,
		Fh:     fh,
		Offset: crossOffset,
		Size:   uint32(len(payload)),
	})
	require.NoError(t, err)
	require.NotNil(t, readRes)
	assert.Equal(t, payload, readRes.Data)

	// 3. 从 64 MiB - 256 KiB 处跨界读取 512 KiB (局部跨界读取)
	readSub, err := mgr.Read(ctx, &meta.ReadRequest{
		Ino:    ino,
		Fh:     fh,
		Offset: crossOffset + 256*1024,
		Size:   512 * 1024,
	})
	require.NoError(t, err)
	require.NotNil(t, readSub)
	assert.Equal(t, payload[256*1024:256*1024+512*1024], readSub.Data)

	// 4. 左空洞读取：从 64 MiB - 768 KiB 处读取 512 KiB
	// [ChunkSize-768KiB, ChunkSize-512KiB) 为 256 KiB 空洞全零
	// [ChunkSize-512KiB, ChunkSize-256KiB) 为 payload 的前 256 KiB
	readLeftHole, err := mgr.Read(ctx, &meta.ReadRequest{
		Ino:    ino,
		Fh:     fh,
		Offset: crossOffset - 256*1024,
		Size:   512 * 1024,
	})
	require.NoError(t, err)
	require.NotNil(t, readLeftHole)
	expectedLeftHole := append(make([]byte, 256*1024), payload[:256*1024]...)
	assert.Equal(t, expectedLeftHole, readLeftHole.Data)

	// 5. 右空洞读取验证：新建一个只有 Chunk 0 尾部数据、Chunk 1 全空的 Inode
	rightHoleFile, err := store.CreateInode(ctx, db.CreateInodeParams{
		Mode:  int32(meta.S_IFREG | 0o644),
		Nlink: 1,
		Uid:   1000,
		Gid:   1000,
	})
	require.NoError(t, err)
	rightHoleIno := uint64(rightHoleFile.Ino)
	defer func() {
		chunkRow, _ := store.GetChunk(context.Background(), db.GetChunkParams{Ino: rightHoleFile.Ino, Indx: 0})
		if len(chunkRow.Slices) > 0 {
			slices, _ := meta.DecodeSlices(chunkRow.Slices)
			for _, s := range slices {
				_ = store.DeleteSliceRef(context.Background(), int64(s.ID))
			}
		}
		_ = meta.DeleteAllChunksByInos(context.Background(), store, []int64{rightHoleFile.Ino})
		_ = store.DeleteInode(context.Background(), rightHoleFile.Ino)
	}()

	rhOpen, err := mgr.Open(ctx, meta.OpenParams{Ino: rightHoleIno, Flags: meta.O_RDWR})
	require.NoError(t, err)
	defer func() {
		_ = mgr.Release(context.Background(), meta.ReleaseParams{Ino: rightHoleIno, Fh: rhOpen.Fh})
	}()

	// 仅在 Chunk 0 尾部 [ChunkSize-256KiB, ChunkSize) 写入 256 KiB
	tailPayload := bytes.Repeat([]byte{0x7A}, 256*1024)
	_, err = mgr.Write(ctx, &meta.WriteRequest{
		Ino:    rightHoleIno,
		Fh:     rhOpen.Fh,
		Offset: uint64(meta.ChunkSize - 256*1024),
		Data:   tailPayload,
	})
	require.NoError(t, err)

	// 人为扩容文件大小到 ChunkSize + 256 KiB，以允许跨界读取且不会被 EOF 截断
	_, err = store.UpdateInodeSizeAndTouchForWrite(ctx, db.UpdateInodeSizeAndTouchForWriteParams{
		Ino:  rightHoleFile.Ino,
		Size: int64(meta.ChunkSize + 256*1024),
		Mode: int32(meta.S_IFREG | 0o644),
	})
	require.NoError(t, err)

	// 跨界读取 [ChunkSize-256KiB, ChunkSize+256KiB) 共 512 KiB：
	// 前 256 KiB 为真实写入的 tailPayload，后 256 KiB 为 Chunk 1 稀疏空洞 0x00
	readRightHole, err := mgr.Read(ctx, &meta.ReadRequest{
		Ino:    rightHoleIno,
		Fh:     rhOpen.Fh,
		Offset: uint64(meta.ChunkSize - 256*1024),
		Size:   512 * 1024,
	})
	require.NoError(t, err)
	require.NotNil(t, readRightHole)
	expectedRightHole := append(tailPayload, make([]byte, 256*1024)...)
	assert.Equal(t, expectedRightHole, readRightHole.Data)

	// 6. 跨 64 MiB 边界叠加多次随机覆写后的跨界读取
	// 在 ino 上的原 payload 基础上：
	// 在 Chunk 0 尾部 [ChunkSize-128KiB, ChunkSize) 覆写 128 KiB
	overChunk0 := bytes.Repeat([]byte{0xC0}, 128*1024)
	_, err = mgr.Write(ctx, &meta.WriteRequest{
		Ino:    ino,
		Fh:     fh,
		Offset: uint64(meta.ChunkSize - 128*1024),
		Data:   overChunk0,
	})
	require.NoError(t, err)

	// 在 Chunk 1 头部 [ChunkSize, ChunkSize+64KiB) 覆写 64 KiB
	overChunk1 := bytes.Repeat([]byte{0xC1}, 64*1024)
	_, err = mgr.Write(ctx, &meta.WriteRequest{
		Ino:    ino,
		Fh:     fh,
		Offset: uint64(meta.ChunkSize),
		Data:   overChunk1,
	})
	require.NoError(t, err)

	// 重新跨界读取完整 1 MiB [ChunkSize-512KiB, ChunkSize+512KiB)
	readOverwritten, err := mgr.Read(ctx, &meta.ReadRequest{
		Ino:    ino,
		Fh:     fh,
		Offset: crossOffset,
		Size:   uint32(len(payload)),
	})
	require.NoError(t, err)
	require.NotNil(t, readOverwritten)

	expectedOverwritten := make([]byte, len(payload))
	copy(expectedOverwritten, payload)
	// Chunk 0 尾部 128 KiB: 在 [512KiB-128KiB, 512KiB) = [384KiB, 512KiB)
	copy(expectedOverwritten[384*1024:512*1024], overChunk0)
	// Chunk 1 头部 64 KiB: 在 [512KiB, 512KiB+64KiB) = [512KiB, 576KiB)
	copy(expectedOverwritten[512*1024:576*1024], overChunk1)
	assert.Equal(t, expectedOverwritten, readOverwritten.Data)
}

// -----------------------------------------------------------------------------
// Wave 2 节点 W2-N1 复现测试：
// 1. TestRead_CrossChunk_ConcurrentWriteAtomicSnapshot_NoTornRead (Ch1-5)
// 2. TestRead_InFlightProtectedAgainstConcurrentCompactAndGC (Ch1-6 / Ch2-5)
// -----------------------------------------------------------------------------

type tornReadHookStore struct {
	db.Store
	onAfterGetChunk func(ctx context.Context, arg db.GetChunkParams)
}

func (s *tornReadHookStore) ExecTx(ctx context.Context, fn func(db.Querier) error) error {
	return s.ExecTxWithOptions(ctx, fn)
}

func (s *tornReadHookStore) ExecTxWithOptions(ctx context.Context, fn func(db.Querier) error, opts ...tx.Option) error {
	return s.Store.ExecTxWithOptions(ctx, func(q db.Querier) error {
		return fn(&tornReadHookQuerier{Querier: q, onAfterGetChunk: s.onAfterGetChunk})
	}, opts...)
}

func (s *tornReadHookStore) Pool() *pgxpool.Pool {
	if pp, ok := s.Store.(interface{ Pool() *pgxpool.Pool }); ok {
		return pp.Pool()
	}
	return nil
}

type tornReadHookQuerier struct {
	db.Querier
	onAfterGetChunk func(ctx context.Context, arg db.GetChunkParams)
}

func (q *tornReadHookQuerier) GetChunk(ctx context.Context, arg db.GetChunkParams) (db.Chunk, error) {
	chunk, err := q.Querier.GetChunk(ctx, arg)
	if q.onAfterGetChunk != nil {
		q.onAfterGetChunk(ctx, arg)
	}
	return chunk, err
}

// TestRead_CrossChunk_ConcurrentWriteAtomicSnapshot_NoTornRead 验证跨 Chunk 原子写被并发 Read 读撕裂的缺陷（Ch1-5）。
func TestRead_CrossChunk_ConcurrentWriteAtomicSnapshot_NoTornRead(t *testing.T) {
	pool, ctx := setupMetaTestDB(t)
	if pool == nil {
		return
	}

	baseStore := db.NewStore(pool)
	dataDir := t.TempDir()
	bs := meta.NewLocalFSBlockStore(dataDir)
	baseMgr := meta.New(baseStore, meta.WithBlockStore(bs))

	disk, err := baseMgr.CreateDisk(ctx, meta.CreateDiskParams{
		Name:     "torn-read-disk",
		MaxBytes: 10 * 1024 * 1024 * 1024,
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = baseMgr.DeleteDisk(context.Background(), disk.DiskID)
	})

	fileRow, err := baseStore.CreateInode(ctx, db.CreateInodeParams{
		Mode:  int32(meta.S_IFREG | 0o644),
		Nlink: 1,
		Uid:   1000,
		Gid:   1000,
	})
	require.NoError(t, err)
	ino := uint64(fileRow.Ino)
	t.Cleanup(func() {
		for indx := int64(0); indx < 2; indx++ {
			chunkRow, _ := baseStore.GetChunk(context.Background(), db.GetChunkParams{Ino: fileRow.Ino, Indx: indx})
			if len(chunkRow.Slices) > 0 {
				slices, _ := meta.DecodeSlices(chunkRow.Slices)
				for _, s := range slices {
					_ = baseStore.DeleteSliceRef(context.Background(), int64(s.ID))
				}
			}
		}
		_ = meta.DeleteAllChunksByInos(context.Background(), baseStore, []int64{fileRow.Ino})
		_ = baseStore.DeleteInode(context.Background(), fileRow.Ino)
	})

	openRes, err := baseMgr.Open(ctx, meta.OpenParams{
		Ino:   ino,
		Flags: meta.O_RDWR,
	})
	require.NoError(t, err)
	fh := openRes.Fh
	t.Cleanup(func() {
		_ = baseMgr.Release(context.Background(), meta.ReleaseParams{Ino: ino, Fh: fh})
	})

	// 在跨 64 MiB 边界的 [meta.ChunkSize-1, meta.ChunkSize+1) 写入初始 2 字节 "AB"
	crossOffset := uint64(meta.ChunkSize - 1)
	_, err = baseMgr.Write(ctx, &meta.WriteRequest{
		Ino:    ino,
		Fh:     fh,
		Offset: crossOffset,
		Data:   []byte("AB"),
	})
	require.NoError(t, err)

	t.Run("确定性拦截跨 Chunk 读快照窗口复现读撕裂", func(t *testing.T) {
		subCtx, subCancel := context.WithCancel(ctx)
		defer subCancel()

		var getChunkCalls int32
		writeTriggered := make(chan struct{})
		var writeOnce sync.Once
		writeDone := make(chan struct{})
		writerExited := make(chan struct{})
		var asyncWriteErr error
		var hookTimeoutErr error

		hookedStore := &tornReadHookStore{
			Store: baseStore,
			onAfterGetChunk: func(callCtx context.Context, arg db.GetChunkParams) {
				if arg.Ino != int64(ino) {
					return
				}
				// 仅当 GetChunk 返回且为第 1 次时触发并发写
				if atomic.AddInt32(&getChunkCalls, 1) == 1 {
					writeOnce.Do(func() {
						close(writeTriggered)
					})
					// 等待并发写提交完成（带超时防卡死）
					select {
					case <-writeDone:
					case <-time.After(5 * time.Second):
						hookTimeoutErr = errors.New("等待并发写提交超时")
					case <-callCtx.Done():
					}
				}
			},
		}

		hookedMgr := meta.New(hookedStore, meta.WithBlockStore(bs))

		// 启动并发写协程，等待 writeTriggered 信号
		go func() {
			defer close(writerExited)
			select {
			case <-writeTriggered:
				_, wErr := baseMgr.Write(subCtx, &meta.WriteRequest{
					Ino:    ino,
					Fh:     fh,
					Offset: crossOffset,
					Data:   []byte("CD"),
				})
				if wErr != nil {
					asyncWriteErr = wErr
				}
				close(writeDone)
			case <-subCtx.Done():
			}
		}()

		// 主协程执行跨 Chunk 读取 2 字节
		readResp, rErr := hookedMgr.Read(subCtx, &meta.ReadRequest{
			Ino:    ino,
			Fh:     fh,
			Offset: crossOffset,
			Size:   2,
		})
		require.NoError(t, rErr)
		require.NotNil(t, readResp)

		readStr := string(readResp.Data)

		subCancel()
		select {
		case <-writerExited:
		case <-time.After(5 * time.Second):
			t.Fatal("等待并发写协程退出超时")
		}

		require.NoError(t, hookTimeoutErr)
		require.NoError(t, asyncWriteErr)

		// 快照隔离一致性断言：读出的 2 字节必须为原子快照 'AB' 或 'CD'
		assert.NotEqual(t, "AD", readStr, "跨 Chunk 读遭遇并发写撕裂！读出未修复的中间状态 'AD'")
		assert.NotEqual(t, "CB", readStr, "跨 Chunk 读遭遇并发写撕裂！读出未修复的中间状态 'CB'")
		assert.True(t, readStr == "AB" || readStr == "CD", "跨 Chunk 读取必须为原子快照 'AB' 或 'CD'，但读到撕裂数据: %q", readStr)
	})

	t.Run("并发跨 Chunk 原子写与多协程读循环校验无撕裂", func(t *testing.T) {
		stopWriterCh := make(chan struct{})
		var writeErr error
		var totalReads atomic.Int64
		var invalidReads atomic.Int64

		var readErrsMu sync.Mutex
		var readErrs []error

		// 启动 1 个写协程在 "AB" 与 "CD" 之间交替覆写 30 次
		go func() {
			defer close(stopWriterCh)
			patterns := [][]byte{[]byte("CD"), []byte("AB")}
			for i := 0; i < 30; i++ {
				data := patterns[i%2]
				_, err := baseMgr.Write(ctx, &meta.WriteRequest{
					Ino:    ino,
					Fh:     fh,
					Offset: crossOffset,
					Data:   data,
				})
				if err != nil {
					writeErr = err
					return
				}
			}
		}()

		// 启动 4 个并发读协程
		var wg sync.WaitGroup
		for r := 0; r < 4; r++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for {
					select {
					case <-stopWriterCh:
						return
					case <-ctx.Done():
						return
					default:
					}

					readResp, err := baseMgr.Read(ctx, &meta.ReadRequest{
						Ino:    ino,
						Fh:     fh,
						Offset: crossOffset,
						Size:   2,
					})
					if err != nil {
						if !errors.Is(err, context.Canceled) {
							readErrsMu.Lock()
							readErrs = append(readErrs, err)
							readErrsMu.Unlock()
						}
						continue
					}
					totalReads.Add(1)
					str := string(readResp.Data)
					if str != "AB" && str != "CD" {
						invalidReads.Add(1)
					}
				}
			}()
		}

		wg.Wait()
		require.NoError(t, writeErr)
		require.Empty(t, readErrs, "并发 Read 不得出现任何非 context.Canceled 错误")
		assert.Greater(t, totalReads.Load(), int64(0), "并发读必须有成功读取样本")
		assert.Equal(t, int64(0), invalidReads.Load(), "每次成功 Read 必须严格满足 str == 'AB' || str == 'CD'")
	})
}

type readBarrierBlockStore struct {
	meta.BlockStore
	onBeforeGetBlock func(ctx context.Context, sliceID uint64, blockIdx uint32)
}

func (b *readBarrierBlockStore) GetBlock(ctx context.Context, sliceID uint64, blockIdx, blockSize, offset uint32, dst []byte) error {
	if b.onBeforeGetBlock != nil {
		b.onBeforeGetBlock(ctx, sliceID, blockIdx)
	}
	return b.BlockStore.GetBlock(ctx, sliceID, blockIdx, blockSize, offset, dst)
}

// TestRead_InFlightProtectedAgainstConcurrentCompactAndGC 验证在途 Read 遭遇并发 CompactChunk+CollectGarbage 物理删块报 EIO 的缺陷（Ch1-6 / Ch2-5）。
func TestRead_InFlightProtectedAgainstConcurrentCompactAndGC(t *testing.T) {
	pool, ctx := setupMetaTestDB(t)
	if pool == nil {
		return
	}
	acquireTestSliceGCAdvisoryLock(t, pool)

	store := db.NewStore(pool)
	dataDir := t.TempDir()
	realBS := meta.NewLocalFSBlockStore(dataDir)

	barrierBS := &readBarrierBlockStore{
		BlockStore: realBS,
	}
	mgr := meta.New(store, meta.WithBlockStore(barrierBS))

	fileRow, err := store.CreateInode(ctx, db.CreateInodeParams{
		Mode:  int32(meta.S_IFREG | 0o644),
		Nlink: 1,
		Uid:   1000,
		Gid:   1000,
	})
	require.NoError(t, err)
	ino := uint64(fileRow.Ino)
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		var sliceIDs []int64
		for indx := int64(0); indx < 16; indx++ {
			chunkRow, cErr := store.GetChunk(cleanupCtx, db.GetChunkParams{Ino: fileRow.Ino, Indx: indx})
			if cErr == nil && len(chunkRow.Slices) > 0 {
				if sls, dErr := meta.DecodeSlices(chunkRow.Slices); dErr == nil {
					for _, s := range sls {
						if s.ID > 0 {
							sliceIDs = append(sliceIDs, int64(s.ID))
						}
					}
				}
			}
		}
		_ = meta.DeleteAllChunksByInos(cleanupCtx, store, []int64{fileRow.Ino})
		for _, sid := range sliceIDs {
			_ = store.DeleteSliceRef(cleanupCtx, sid)
		}
		_ = store.DeleteInode(cleanupCtx, fileRow.Ino)
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

	// 写入测试数据前排空共享库历史可能残留的可回收垃圾
	for i := 0; i < 5; i++ {
		reclaimed, _ := mgr.CollectGarbage(ctx, 500)
		if reclaimed == 0 {
			break
		}
	}

	// 向同一文件 Chunk 0 连续写入/覆写 3 次（产生 3 条 Slice）
	data1 := bytes.Repeat([]byte{0x11}, 128*1024)
	_, err = mgr.Write(ctx, &meta.WriteRequest{Ino: ino, Fh: fh, Offset: 0, Data: data1})
	require.NoError(t, err)

	data2 := bytes.Repeat([]byte{0x22}, 128*1024)
	_, err = mgr.Write(ctx, &meta.WriteRequest{Ino: ino, Fh: fh, Offset: 64 * 1024, Data: data2})
	require.NoError(t, err)

	data3 := bytes.Repeat([]byte{0x33}, 128*1024)
	_, err = mgr.Write(ctx, &meta.WriteRequest{Ino: ino, Fh: fh, Offset: 128 * 1024, Data: data3})
	require.NoError(t, err)

	// 从 store.GetChunk 解码记录本用例生成的 3 个 oldSliceIDs
	chunkRow, err := store.GetChunk(ctx, db.GetChunkParams{Ino: int64(ino), Indx: 0})
	require.NoError(t, err)
	initSlices, err := meta.DecodeSlices(chunkRow.Slices)
	require.NoError(t, err)
	var oldSliceIDs []uint64
	for _, s := range initSlices {
		if s.ID > 0 {
			oldSliceIDs = append(oldSliceIDs, s.ID)
		}
	}
	require.Len(t, oldSliceIDs, 3, "写入 3 次应生成 3 个旧切片")

	expectedPayload := make([]byte, 256*1024)
	copy(expectedPayload[0:64*1024], bytes.Repeat([]byte{0x11}, 64*1024))
	copy(expectedPayload[64*1024:128*1024], bytes.Repeat([]byte{0x22}, 64*1024))
	copy(expectedPayload[128*1024:256*1024], bytes.Repeat([]byte{0x33}, 128*1024))

	// 设置屏障：当读者协程首次进入 GetBlock 时触发通知并阻塞
	enteredGetBlockCh := make(chan struct{})
	proceedCh := make(chan struct{})
	var readerBlocked atomic.Bool
	var proceedOnce sync.Once

	t.Cleanup(func() {
		proceedOnce.Do(func() {
			close(proceedCh)
		})
	})

	barrierBS.onBeforeGetBlock = func(bCtx context.Context, sliceID uint64, blockIdx uint32) {
		if readerBlocked.CompareAndSwap(false, true) {
			close(enteredGetBlockCh)
			select {
			case <-proceedCh:
			case <-bCtx.Done():
			case <-ctx.Done():
			}
		}
	}

	type readResult struct {
		resp *meta.ReadResponse
		err  error
	}
	readResCh := make(chan readResult, 1)

	// 启动读者协程 R
	go func() {
		resp, rErr := mgr.Read(ctx, &meta.ReadRequest{
			Ino:    ino,
			Fh:     fh,
			Offset: 0,
			Size:   256 * 1024,
		})
		readResCh <- readResult{resp: resp, err: rErr}
	}()

	// 主测试协程等待读者协程 R 进入 GetBlock
	select {
	case <-enteredGetBlockCh:
	case <-time.After(10 * time.Second):
		t.Fatal("超时等待读者进入 GetBlock 屏障")
	}

	// 主测试协程顺序调用 CompactChunk 与 CollectGarbage
	err = mgr.CompactChunk(ctx, ino, 0)
	require.NoError(t, err)

	firstReclaimed, err := mgr.CollectGarbage(ctx, 500)
	require.NoError(t, err)
	t.Logf("第一次 CollectGarbage 回收切片数: %d", firstReclaimed)

	// 读者阻塞在 GetBlock 屏障期间：显式断言目标 oldSliceIDs 因持有 FOR KEY SHARE 读锁而依然存在于 slice_refs 表中
	for _, sid := range oldSliceIDs {
		refRow, refErr := store.GetSliceRef(ctx, int64(sid))
		require.NoError(t, refErr, "读者持锁保护期间，旧 sliceID %d 必须依然存在于 slice_refs 表中", sid)
		assert.Equal(t, int32(0), refRow.Refs, "旧 sliceID %d 引用计数已被 CompactChunk 扣减为 0", sid)
	}

	// 放行读者协程继续执行 GetBlock
	proceedOnce.Do(func() {
		close(proceedCh)
	})

	// 等待读者协程返回
	var rRes readResult
	select {
	case rRes = <-readResCh:
	case <-time.After(10 * time.Second):
		t.Fatal("超时等待读者协程 Read 返回")
	}

	// 断言：在途 Read 必须受保护，成功返回预期载荷且 err == nil，绝不报 ErrIO
	require.NoError(t, rRes.err, "在途 Read 应受读保护免受并发 Compact+GC 物理删块影响，但报出错误: %v", rRes.err)
	require.NotNil(t, rRes.resp)
	assert.Equal(t, expectedPayload, rRes.resp.Data)

	// 待读者 R 返回（释放读保护）后，再次调用 CollectGarbage 能够将旧 Slice 物理块完整回收干净
	secondReclaimed, err := mgr.CollectGarbage(ctx, 500)
	require.NoError(t, err)
	t.Logf("读者退出后第二次 CollectGarbage 回收切片数: %d", secondReclaimed)

	// 显式断言全部 3 个 oldSliceIDs 均已从 slice_refs 表中彻底删除
	for _, sid := range oldSliceIDs {
		_, refErr := store.GetSliceRef(ctx, int64(sid))
		assert.ErrorIs(t, refErr, pgx.ErrNoRows, "读者释放读保护后，旧 sliceID %d 必须被彻底删除", sid)
	}

	// 且 dataDir/chunks 下仅剩合并后的 1 个新物理块文件！
	var remainingBlockFiles []string
	err = filepath.WalkDir(filepath.Join(dataDir, "chunks"), func(path string, d os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if !d.IsDir() {
			remainingBlockFiles = append(remainingBlockFiles, path)
		}
		return nil
	})
	require.NoError(t, err)
	assert.Len(t, remainingBlockFiles, 1, "全部旧物理块回收后，dataDir/chunks 下仅能剩 1 个合并后的新物理块文件")
}
