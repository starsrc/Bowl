package meta_test

import (
	"bytes"
	"context"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

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

	setupBaseMocks := func(mockStore *dbmock.MockStore, mockQuerier *dbmock.MockQuerier) {
		setupMockWriteTx(mockStore, mockQuerier)
		mockQuerier.EXPECT().
			GetInode(mock.Anything, int64(ino)).
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
				Flags: int32(meta.O_RDONLY),
			}, nil).
			Once()
	}

	t.Run("跨越 4 MiB Block 边界读取 2 MiB 数据拼接验证", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mockBS := metamock.NewMockBlockStore(t)
		mgr := meta.New(mockStore, meta.WithBlockStore(mockBS))

		setupBaseMocks(mockStore, mockQuerier)

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
			GetChunk(mock.Anything, db.GetChunkParams{Ino: int64(ino), Indx: 0}).
			Return(db.Chunk{
				Ino:    int64(ino),
				Indx:   0,
				Slices: meta.EncodeSlice(sliceDesc),
			}, nil).
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

	setupBaseMocks := func(mockStore *dbmock.MockStore, mockQuerier *dbmock.MockQuerier) {
		setupMockWriteTx(mockStore, mockQuerier)
		mockQuerier.EXPECT().
			GetInode(mock.Anything, int64(ino)).
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
				Flags: int32(meta.O_RDONLY),
			}, nil).
			Once()
	}

	t.Run("跨 64 MiB 边界读取 1 MiB (左右各 512 KiB) 正常拼接返回", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mockBS := metamock.NewMockBlockStore(t)
		mgr := meta.New(mockStore, meta.WithBlockStore(mockBS))

		setupBaseMocks(mockStore, mockQuerier)

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
		mockQuerier.EXPECT().
			GetChunk(mock.Anything, db.GetChunkParams{Ino: int64(ino), Indx: 0}).
			Return(db.Chunk{Ino: int64(ino), Indx: 0, Slices: meta.EncodeSlice(slice0)}, nil).
			Once()

		// Chunk 1: 在 [0, 512KiB) 处有切片
		slice1 := meta.Slice{
			Pos:  0,
			ID:   sliceID2,
			Size: 512 * 1024,
			Off:  0,
			Len:  512 * 1024,
		}
		mockQuerier.EXPECT().
			GetChunk(mock.Anything, db.GetChunkParams{Ino: int64(ino), Indx: 1}).
			Return(db.Chunk{Ino: int64(ino), Indx: 1, Slices: meta.EncodeSlice(slice1)}, nil).
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

		setupBaseMocks(mockStore, mockQuerier)

		// Chunk 0 不存在 (pgx.ErrNoRows)
		mockQuerier.EXPECT().
			GetChunk(mock.Anything, db.GetChunkParams{Ino: int64(ino), Indx: 0}).
			Return(db.Chunk{}, pgx.ErrNoRows).
			Once()

		// Chunk 1: [0, 512 KiB)
		sliceID2 := uint64(1003)
		slice1 := meta.Slice{
			Pos:  0,
			ID:   sliceID2,
			Size: 512 * 1024,
			Off:  0,
			Len:  512 * 1024,
		}
		mockQuerier.EXPECT().
			GetChunk(mock.Anything, db.GetChunkParams{Ino: int64(ino), Indx: 1}).
			Return(db.Chunk{Ino: int64(ino), Indx: 1, Slices: meta.EncodeSlice(slice1)}, nil).
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

		setupBaseMocks(mockStore, mockQuerier)

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
			GetChunk(mock.Anything, db.GetChunkParams{Ino: int64(ino), Indx: 0}).
			Return(db.Chunk{Ino: int64(ino), Indx: 0, Slices: meta.EncodeSlice(slice0)}, nil).
			Once()

		// Chunk 1 不存在 (pgx.ErrNoRows)
		mockQuerier.EXPECT().
			GetChunk(mock.Anything, db.GetChunkParams{Ino: int64(ino), Indx: 1}).
			Return(db.Chunk{}, pgx.ErrNoRows).
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

		setupBaseMocks(mockStore, mockQuerier)

		mockQuerier.EXPECT().
			GetChunk(mock.Anything, db.GetChunkParams{Ino: int64(ino), Indx: 0}).
			Return(db.Chunk{}, pgx.ErrNoRows).
			Once()

		mockQuerier.EXPECT().
			GetChunk(mock.Anything, db.GetChunkParams{Ino: int64(ino), Indx: 1}).
			Return(db.Chunk{}, pgx.ErrNoRows).
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

		setupBaseMocks(mockStore, mockQuerier)

		// Chunk 0 包含底图 [ChunkSize-512KiB, ChunkSize) 与后续尾部覆写 [ChunkSize-128KiB, ChunkSize)
		baseSliceID0 := uint64(2001)
		overSliceID0 := uint64(2002)
		chunk0Slices := []meta.Slice{
			{Pos: uint32(meta.ChunkSize - 512*1024), ID: baseSliceID0, Size: 512 * 1024, Off: 0, Len: 512 * 1024},
			{Pos: uint32(meta.ChunkSize - 128*1024), ID: overSliceID0, Size: 128 * 1024, Off: 0, Len: 128 * 1024},
		}
		mockQuerier.EXPECT().
			GetChunk(mock.Anything, db.GetChunkParams{Ino: int64(ino), Indx: 0}).
			Return(db.Chunk{Ino: int64(ino), Indx: 0, Slices: meta.EncodeSlices(chunk0Slices)}, nil).
			Once()

		// Chunk 1 包含底图 [0, 512KiB) 与后续头部覆写 [0, 64KiB)
		baseSliceID1 := uint64(2003)
		overSliceID1 := uint64(2004)
		chunk1Slices := []meta.Slice{
			{Pos: 0, ID: baseSliceID1, Size: 512 * 1024, Off: 0, Len: 512 * 1024},
			{Pos: 0, ID: overSliceID1, Size: 64 * 1024, Off: 0, Len: 64 * 1024},
		}
		mockQuerier.EXPECT().
			GetChunk(mock.Anything, db.GetChunkParams{Ino: int64(ino), Indx: 1}).
			Return(db.Chunk{Ino: int64(ino), Indx: 1, Slices: meta.EncodeSlices(chunk1Slices)}, nil).
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
	dsn := os.Getenv("STORAGE_TEST_DSN")
	if dsn == "" {
		dsn = "postgres://postgres:postgres@localhost:9432/bowl?sslmode=disable"
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		// 备选尝试 5432
		dsn = "postgres://postgres:postgres@localhost:5432/bowl?sslmode=disable"
		pool, err = pgxpool.New(ctx, dsn)
		if err != nil {
			t.Skipf("跳过 PostgreSQL 测试 (连接初始化失败): %v", err)
		}
	}
	defer pool.Close()

	if err := pool.Ping(ctx); err != nil {
		dsn = "postgres://postgres:postgres@localhost:5432/bowl?sslmode=disable"
		var err2 error
		pool, err2 = pgxpool.New(ctx, dsn)
		if err2 != nil || pool.Ping(ctx) != nil {
			t.Skipf("跳过 PostgreSQL 测试 (ping 失败): %v", err)
		}
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
		_ = store.DeleteInode(context.Background(), fileRow.Ino)
		_, _ = store.DeleteChunksByIno(context.Background(), fileRow.Ino)
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
		_ = store.DeleteInode(context.Background(), rightHoleFile.Ino)
		_, _ = store.DeleteChunksByIno(context.Background(), rightHoleFile.Ino)
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
