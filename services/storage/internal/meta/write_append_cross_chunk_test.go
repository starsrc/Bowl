package meta_test

import (
	"context"
	"fmt"
	"math"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

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

// TestWrite_CrossChunkBoundary 测试写入跨越 64 MiB Chunk 边界时自动切分为两个 Chunk 且 refs = 2。
func TestWrite_CrossChunkBoundary(t *testing.T) {
	ctx := context.Background()
	now := time.Now().Truncate(time.Microsecond)
	ino := uint64(201)
	fh := uint64(21)
	sliceID := int64(1001)

	// 跨越 64 MiB 边界：从 64 MiB - 1024 处写入 4096 字节
	// actualOffset = ChunkSize - 1024
	// startIndx = 0, endIndx = 1
	// firstPos = ChunkSize - 1024, firstLen = 1024
	// secondPos = 0, secondOff = 1024, secondLen = 3072
	offset := uint64(meta.ChunkSize - 1024)
	payload := make([]byte, 4096)
	for i := range payload {
		payload[i] = byte(i % 251)
	}

	baseFileRow := db.Inode{
		Ino:        int64(ino),
		Generation: 1,
		Mode:       int32(meta.S_IFREG | 0o644),
		Size:       int64(offset), // 初始大小刚好等于写入起始偏移
		Nlink:      1,
		Uid:        1000,
		Gid:        1000,
		Rdev:       0,
		Atime:      pgtype.Timestamptz{Time: now.Add(-time.Hour), Valid: true},
		Mtime:      pgtype.Timestamptz{Time: now.Add(-time.Hour), Valid: true},
		Ctime:      pgtype.Timestamptz{Time: now.Add(-time.Hour), Valid: true},
	}

	t.Run("普通跨 64 MiB 边界写入透明拆分至 indx 与 indx+1 且 slice_refs.refs 设为 2", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mockBS := metamock.NewMockBlockStore(t)
		mgr := meta.New(mockStore, meta.WithBlockStore(mockBS))

		// 阶段 1：行锁外申请 slice_id 与落盘物理块 (payload <= 4 MiB 拆为 1 个 block)
		mockStore.EXPECT().
			NextSliceID(mock.Anything).
			Return(sliceID, nil).
			Once()

		mockBS.EXPECT().
			PutBlock(mock.Anything, uint64(sliceID), uint32(0), payload).
			Return(nil).
			Once()

		// 阶段 2：事务提交
		setupMockWriteTx(mockStore, mockQuerier)

		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(ino)).
			Return(baseFileRow, nil).
			Once()

		mockQuerier.EXPECT().
			GetFileHandle(mock.Anything, db.GetFileHandleParams{
				Fh:  int64(fh),
				Ino: int64(ino),
			}).
			Return(db.FileHandle{
				Fh:    int64(fh),
				Ino:   int64(ino),
				Flags: int32(meta.O_WRONLY), // 非 O_APPEND
			}, nil).
			Once()

		// 第一个 Chunk (indx = 0)：[ChunkSize-1024, ChunkSize)，长度 1024
		firstSliceExpected := meta.EncodeSlice(meta.Slice{
			Pos:  uint32(meta.ChunkSize - 1024),
			ID:   uint64(sliceID),
			Size: uint32(len(payload)),
			Off:  0,
			Len:  1024,
		})
		mockQuerier.EXPECT().
			AppendChunkSlice(mock.Anything, db.AppendChunkSliceParams{
				Ino:    int64(ino),
				Indx:   0,
				Slices: firstSliceExpected,
			}).
			Return(nil).
			Once()

		// 第二个 Chunk (indx = 1)：[0, 3072)，长度 3072
		secondSliceExpected := meta.EncodeSlice(meta.Slice{
			Pos:  0,
			ID:   uint64(sliceID),
			Size: uint32(len(payload)),
			Off:  1024,
			Len:  3072,
		})
		mockQuerier.EXPECT().
			AppendChunkSlice(mock.Anything, db.AppendChunkSliceParams{
				Ino:    int64(ino),
				Indx:   1,
				Slices: secondSliceExpected,
			}).
			Return(nil).
			Once()

		// 跨界拆分时 slice_refs 记录 refs = 2
		mockQuerier.EXPECT().
			InsertSliceRef(mock.Anything, db.InsertSliceRefParams{
				SliceID: sliceID,
				Size:    int32(len(payload)),
				Refs:    2,
			}).
			Return(nil).
			Once()

		expectedNewSize := int64(offset) + int64(len(payload))
		updatedRow := baseFileRow
		updatedRow.Size = expectedNewSize
		updatedRow.Mtime = pgtype.Timestamptz{Time: now, Valid: true}
		updatedRow.Ctime = pgtype.Timestamptz{Time: now, Valid: true}

		mockQuerier.EXPECT().
			UpdateInodeSizeAndTouchForWrite(mock.Anything, db.UpdateInodeSizeAndTouchForWriteParams{
				Ino:  int64(ino),
				Size: expectedNewSize,
				Mode: int32(meta.S_IFREG | 0o644),
			}).
			Return(updatedRow, nil).
			Once()

		resp, err := mgr.Write(ctx, &meta.WriteRequest{
			Ino:    ino,
			Fh:     fh,
			Offset: offset,
			Data:   payload,
		})

		require.NoError(t, err)
		require.NotNil(t, resp)
		assert.Equal(t, uint32(len(payload)), resp.Size)
		assert.Equal(t, uint64(expectedNewSize), resp.Inode.Attr.Size)
	})
}

// TestWrite_OAppend_IgnoreClientOffset 测试 O_APPEND 标志强制忽略客户端传入的偏移，锚定至 Inode.Size。
func TestWrite_OAppend_IgnoreClientOffset(t *testing.T) {
	ctx := context.Background()
	now := time.Now().Truncate(time.Microsecond)
	ino := uint64(301)
	fh := uint64(31)
	sliceID := int64(2001)
	payload := []byte("appended-bytes-payload")

	baseFileRow := db.Inode{
		Ino:        int64(ino),
		Generation: 1,
		Mode:       int32(meta.S_IFREG | 0o644),
		Size:       500, // 当前大小 500
		Nlink:      1,
		Uid:        1000,
		Gid:        1000,
		Rdev:       0,
		Atime:      pgtype.Timestamptz{Time: now.Add(-time.Hour), Valid: true},
		Mtime:      pgtype.Timestamptz{Time: now.Add(-time.Hour), Valid: true},
		Ctime:      pgtype.Timestamptz{Time: now.Add(-time.Hour), Valid: true},
	}

	testCases := []struct {
		name       string
		clientOff  uint64
		openFlags  int32
		expectedSz int64
	}{
		{
			name:       "O_APPEND 句柄下客户端传入 offset = 0，强制锚定至 Inode.Size = 500",
			clientOff:  0,
			openFlags:  int32(meta.O_WRONLY | meta.O_APPEND),
			expectedSz: 500 + int64(len(payload)),
		},
		{
			name:       "O_APPEND 句柄下客户端传入非零 offset = 999999，强制忽略并锚定至 Inode.Size = 500",
			clientOff:  999999,
			openFlags:  int32(meta.O_RDWR | meta.O_APPEND),
			expectedSz: 500 + int64(len(payload)),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			mockStore := dbmock.NewMockStore(t)
			mockQuerier := dbmock.NewMockQuerier(t)
			mockBS := metamock.NewMockBlockStore(t)
			mgr := meta.New(mockStore, meta.WithBlockStore(mockBS))

			mockStore.EXPECT().
				NextSliceID(mock.Anything).
				Return(sliceID, nil).
				Once()

			mockBS.EXPECT().
				PutBlock(mock.Anything, uint64(sliceID), uint32(0), payload).
				Return(nil).
				Once()

			setupMockWriteTx(mockStore, mockQuerier)

			mockQuerier.EXPECT().
				GetInodeForUpdate(mock.Anything, int64(ino)).
				Return(baseFileRow, nil).
				Once()

			mockQuerier.EXPECT().
				GetFileHandle(mock.Anything, db.GetFileHandleParams{
					Fh:  int64(fh),
					Ino: int64(ino),
				}).
				Return(db.FileHandle{
					Fh:    int64(fh),
					Ino:   int64(ino),
					Flags: tc.openFlags,
				}, nil).
				Once()

			// 写入起始位置必须为 actualOffset = 500，且在单 Chunk 内 (indx = 0)
			expectedSlice := meta.EncodeSlice(meta.Slice{
				Pos:  500,
				ID:   uint64(sliceID),
				Size: uint32(len(payload)),
				Off:  0,
				Len:  uint32(len(payload)),
			})
			mockQuerier.EXPECT().
				AppendChunkSlice(mock.Anything, db.AppendChunkSliceParams{
					Ino:    int64(ino),
					Indx:   0,
					Slices: expectedSlice,
				}).
				Return(nil).
				Once()

			mockQuerier.EXPECT().
				InsertSliceRef(mock.Anything, db.InsertSliceRefParams{
					SliceID: sliceID,
					Size:    int32(len(payload)),
					Refs:    1,
				}).
				Return(nil).
				Once()

			updatedRow := baseFileRow
			updatedRow.Size = tc.expectedSz
			updatedRow.Mtime = pgtype.Timestamptz{Time: now, Valid: true}
			updatedRow.Ctime = pgtype.Timestamptz{Time: now, Valid: true}

			mockQuerier.EXPECT().
				UpdateInodeSizeAndTouchForWrite(mock.Anything, db.UpdateInodeSizeAndTouchForWriteParams{
					Ino:  int64(ino),
					Size: tc.expectedSz,
					Mode: int32(meta.S_IFREG | 0o644),
				}).
				Return(updatedRow, nil).
				Once()

			resp, err := mgr.Write(ctx, &meta.WriteRequest{
				Ino:    ino,
				Fh:     fh,
				Offset: tc.clientOff,
				Data:   payload,
			})

			require.NoError(t, err)
			require.NotNil(t, resp)
			assert.Equal(t, uint32(len(payload)), resp.Size)
			assert.Equal(t, uint64(tc.expectedSz), resp.Inode.Attr.Size)
		})
	}
}

// TestWrite_OAppend_CrossChunkBoundary 测试 O_APPEND 追加写恰好跨越 64 MiB 边界时的双 Chunk 拆分与 refs = 2。
func TestWrite_OAppend_CrossChunkBoundary(t *testing.T) {
	ctx := context.Background()
	now := time.Now().Truncate(time.Microsecond)
	ino := uint64(401)
	fh := uint64(41)
	sliceID := int64(3001)

	// 初始文件大小距 64 MiB 边界仅剩 2048 字节
	initialSize := int64(meta.ChunkSize - 2048)
	payload := make([]byte, 4096)
	for i := range payload {
		payload[i] = byte(i % 127)
	}

	baseFileRow := db.Inode{
		Ino:        int64(ino),
		Generation: 1,
		Mode:       int32(meta.S_IFREG | 0o644),
		Size:       initialSize,
		Nlink:      1,
		Uid:        1000,
		Gid:        1000,
		Rdev:       0,
		Atime:      pgtype.Timestamptz{Time: now.Add(-time.Hour), Valid: true},
		Mtime:      pgtype.Timestamptz{Time: now.Add(-time.Hour), Valid: true},
		Ctime:      pgtype.Timestamptz{Time: now.Add(-time.Hour), Valid: true},
	}

	t.Run("O_APPEND 恰好跨越 64 MiB 边界时自动拆分至 indx 与 indx+1 且 refs = 2", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mockBS := metamock.NewMockBlockStore(t)
		mgr := meta.New(mockStore, meta.WithBlockStore(mockBS))

		mockStore.EXPECT().
			NextSliceID(mock.Anything).
			Return(sliceID, nil).
			Once()

		mockBS.EXPECT().
			PutBlock(mock.Anything, uint64(sliceID), uint32(0), payload).
			Return(nil).
			Once()

		setupMockWriteTx(mockStore, mockQuerier)

		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(ino)).
			Return(baseFileRow, nil).
			Once()

		mockQuerier.EXPECT().
			GetFileHandle(mock.Anything, db.GetFileHandleParams{
				Fh:  int64(fh),
				Ino: int64(ino),
			}).
			Return(db.FileHandle{
				Fh:    int64(fh),
				Ino:   int64(ino),
				Flags: int32(meta.O_WRONLY | meta.O_APPEND),
			}, nil).
			Once()

		// 第一个 Chunk (indx = 0)：[ChunkSize-2048, ChunkSize)，长度 2048
		firstSliceExpected := meta.EncodeSlice(meta.Slice{
			Pos:  uint32(meta.ChunkSize - 2048),
			ID:   uint64(sliceID),
			Size: uint32(len(payload)),
			Off:  0,
			Len:  2048,
		})
		mockQuerier.EXPECT().
			AppendChunkSlice(mock.Anything, db.AppendChunkSliceParams{
				Ino:    int64(ino),
				Indx:   0,
				Slices: firstSliceExpected,
			}).
			Return(nil).
			Once()

		// 第二个 Chunk (indx = 1)：[0, 2048)，长度 2048
		secondSliceExpected := meta.EncodeSlice(meta.Slice{
			Pos:  0,
			ID:   uint64(sliceID),
			Size: uint32(len(payload)),
			Off:  2048,
			Len:  2048,
		})
		mockQuerier.EXPECT().
			AppendChunkSlice(mock.Anything, db.AppendChunkSliceParams{
				Ino:    int64(ino),
				Indx:   1,
				Slices: secondSliceExpected,
			}).
			Return(nil).
			Once()

		// 跨界引用计数记为 2
		mockQuerier.EXPECT().
			InsertSliceRef(mock.Anything, db.InsertSliceRefParams{
				SliceID: sliceID,
				Size:    int32(len(payload)),
				Refs:    2,
			}).
			Return(nil).
			Once()

		expectedNewSize := initialSize + int64(len(payload))
		updatedRow := baseFileRow
		updatedRow.Size = expectedNewSize
		updatedRow.Mtime = pgtype.Timestamptz{Time: now, Valid: true}
		updatedRow.Ctime = pgtype.Timestamptz{Time: now, Valid: true}

		mockQuerier.EXPECT().
			UpdateInodeSizeAndTouchForWrite(mock.Anything, db.UpdateInodeSizeAndTouchForWriteParams{
				Ino:  int64(ino),
				Size: expectedNewSize,
				Mode: int32(meta.S_IFREG | 0o644),
			}).
			Return(updatedRow, nil).
			Once()

		resp, err := mgr.Write(ctx, &meta.WriteRequest{
			Ino:    ino,
			Fh:     fh,
			Offset: 0, // 客户端传入 0，被 O_APPEND 忽略
			Data:   payload,
		})

		require.NoError(t, err)
		require.NotNil(t, resp)
		assert.Equal(t, uint32(len(payload)), resp.Size)
		assert.Equal(t, uint64(expectedNewSize), resp.Inode.Attr.Size)
	})
}

// TestWrite_OAppend_OverflowMaxInt64 测试 O_APPEND 追加写导致 offset 溢出 MaxInt64 时被拦截且清理物理块。
func TestWrite_OAppend_OverflowMaxInt64(t *testing.T) {
	ctx := context.Background()
	ino := uint64(501)
	fh := uint64(51)
	sliceID := int64(4001)
	payload := []byte("overflow-test-data")

	// 初始文件大小极为接近 MaxInt64
	initialSize := int64(math.MaxInt64 - 5)

	baseFileRow := db.Inode{
		Ino:   int64(ino),
		Mode:  int32(meta.S_IFREG | 0o644),
		Size:  initialSize,
		Nlink: 1,
	}

	mockStore := dbmock.NewMockStore(t)
	mockQuerier := dbmock.NewMockQuerier(t)
	mockBS := metamock.NewMockBlockStore(t)
	mgr := meta.New(mockStore, meta.WithBlockStore(mockBS))

	mockStore.EXPECT().
		NextSliceID(mock.Anything).
		Return(sliceID, nil).
		Once()

	mockBS.EXPECT().
		PutBlock(mock.Anything, uint64(sliceID), uint32(0), payload).
		Return(nil).
		Once()

	setupMockWriteTx(mockStore, mockQuerier)

	mockQuerier.EXPECT().
		GetInodeForUpdate(mock.Anything, int64(ino)).
		Return(baseFileRow, nil).
		Once()

	mockQuerier.EXPECT().
		GetFileHandle(mock.Anything, db.GetFileHandleParams{
			Fh:  int64(fh),
			Ino: int64(ino),
		}).
		Return(db.FileHandle{
			Fh:    int64(fh),
			Ino:   int64(ino),
			Flags: int32(meta.O_WRONLY | meta.O_APPEND),
		}, nil).
		Once()

	// 溢出时事务失败回滚，外层立即调用 DeleteBlock 清理物理块
	mockBS.EXPECT().
		DeleteBlock(mock.Anything, uint64(sliceID), uint32(0), uint32(len(payload))).
		Return(nil).
		Once()

	resp, err := mgr.Write(ctx, &meta.WriteRequest{
		Ino:    ino,
		Fh:     fh,
		Offset: 0,
		Data:   payload,
	})

	require.Nil(t, resp)
	require.ErrorIs(t, err, meta.ErrInvalidArgument)
}

// TestWrite_OAppend_Concurrent_NoDataLoss 测试多个并发协程使用 O_APPEND 写入时，事务行锁确保线性追加且零数据覆盖丢失。
func TestWrite_OAppend_Concurrent_NoDataLoss(t *testing.T) {
	ctx := context.Background()
	ino := uint64(601)
	fh := uint64(61)

	// 使用状态机模拟真实数据库行为：互斥锁代表 Inode 行级排他锁 (FOR UPDATE)
	var mu sync.Mutex
	currentSize := int64(100)
	var appendedSlices []meta.Slice
	var sliceIDSeq int64 = 5000

	mockStore := dbmock.NewMockStore(t)
	mockBS := metamock.NewMockBlockStore(t)

	// 阶段 1：NextSliceID 原子自增
	mockStore.EXPECT().
		NextSliceID(mock.Anything).
		RunAndReturn(func(ctx context.Context) (int64, error) {
			return atomic.AddInt64(&sliceIDSeq, 1), nil
		})

	mockBS.EXPECT().
		PutBlock(mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(nil)

	// 阶段 2：事务内排他锁
	mockStore.EXPECT().
		ExecTxWithOptions(mock.Anything, mock.Anything).
		RunAndReturn(func(ctx context.Context, fn func(db.Querier) error, opts ...tx.Option) error {
			mu.Lock()
			defer mu.Unlock()

			mockQuerier := dbmock.NewMockQuerier(t)

			mockQuerier.EXPECT().
				GetInodeForUpdate(mock.Anything, int64(ino)).
				Return(db.Inode{
					Ino:   int64(ino),
					Mode:  int32(meta.S_IFREG | 0o644),
					Size:  currentSize,
					Nlink: 1,
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
					Flags: int32(meta.O_WRONLY | meta.O_APPEND),
				}, nil).
				Once()

			mockQuerier.EXPECT().
				AppendChunkSlice(mock.Anything, mock.Anything).
				RunAndReturn(func(ctx context.Context, params db.AppendChunkSliceParams) error {
					slices, err := meta.DecodeSlices(params.Slices)
					if err != nil {
						return err
					}
					appendedSlices = append(appendedSlices, slices...)
					return nil
				}).
				Maybe()

			mockQuerier.EXPECT().
				InsertSliceRef(mock.Anything, mock.Anything).
				Return(nil).
				Maybe()

			mockQuerier.EXPECT().
				UpdateInodeSizeAndTouchForWrite(mock.Anything, mock.Anything).
				RunAndReturn(func(ctx context.Context, params db.UpdateInodeSizeAndTouchForWriteParams) (db.Inode, error) {
					currentSize = params.Size
					return db.Inode{
						Ino:   params.Ino,
						Size:  params.Size,
						Mode:  params.Mode,
						Nlink: 1,
					}, nil
				}).
				Once()

			return fn(mockQuerier)
		})

	mgr := meta.New(mockStore, meta.WithBlockStore(mockBS))

	const concurrency = 10
	const writeLen = 64
	data := make([]byte, writeLen)
	for i := range data {
		data[i] = 'A' + byte(i%26)
	}

	var wg sync.WaitGroup
	errCh := make(chan error, concurrency)

	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			// 故意传入不同的 offset（如 0、123、999），验证 O_APPEND 会强制忽略 offset
			clientOff := uint64(id * 1000)
			res, err := mgr.Write(ctx, &meta.WriteRequest{
				Ino:    ino,
				Fh:     fh,
				Offset: clientOff,
				Data:   data,
			})
			if err != nil {
				errCh <- fmt.Errorf("write %d failed: %w", id, err)
				return
			}
			if res.Size != writeLen {
				errCh <- fmt.Errorf("write %d unexpected size: %d", id, res.Size)
				return
			}
		}(i)
	}

	wg.Wait()
	close(errCh)

	for err := range errCh {
		require.NoError(t, err)
	}

	// 验证最终状态：
	// 1. 最终文件大小必须严格等于 100 + concurrency * writeLen = 740
	expectedFinalSize := int64(100 + concurrency*writeLen)
	assert.Equal(t, expectedFinalSize, currentSize)

	// 2. 累积记录了 10 条 Slice 且 Pos 依次递增无重叠覆盖
	require.Len(t, appendedSlices, concurrency)
	// 对 appendedSlices 按 Pos 排序检验连续性
	for i := 0; i < concurrency; i++ {
		expectedPos := uint32(100 + i*writeLen)
		assert.Equal(t, expectedPos, appendedSlices[i].Pos, "slice %d 起始位置不连续或发生重叠", i)
		assert.Equal(t, uint32(writeLen), appendedSlices[i].Len)
	}
}

// TestWrite_CrossChunkBoundary_PostgresIntegration 真实 PostgreSQL 容器跨 64 MiB 边界与 O_APPEND 集成测试。
func TestWrite_CrossChunkBoundary_PostgresIntegration(t *testing.T) {
	dsn := os.Getenv("STORAGE_TEST_DSN")
	if dsn == "" {
		dsn = "postgres://postgres:postgres@localhost:5432/bowl?sslmode=disable"
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Skipf("跳过 PostgreSQL 测试 (连接初始化失败): %v", err)
	}
	defer pool.Close()

	if err := pool.Ping(ctx); err != nil {
		t.Skipf("跳过 PostgreSQL 测试 (ping 失败): %v", err)
	}

	store := db.NewStore(pool)
	dataDir := t.TempDir()
	bs := meta.NewLocalFSBlockStore(dataDir)
	mgr := meta.New(store, meta.WithBlockStore(bs))

	// 创建初始文件
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

	// 1. 测试普通跨 64 MiB 边界写入
	openRes, err := mgr.Open(ctx, meta.OpenParams{
		Ino:   ino,
		Flags: meta.O_RDWR,
	})
	require.NoError(t, err)
	fh := openRes.Fh
	defer func() {
		_ = mgr.Release(context.Background(), meta.ReleaseParams{Ino: ino, Fh: fh})
	}()

	// 在 64 MiB - 1024 处写入 4096 字节
	crossOffset := uint64(meta.ChunkSize - 1024)
	payload := make([]byte, 4096)
	for i := range payload {
		payload[i] = byte(i % 256)
	}

	writeRes, err := mgr.Write(ctx, &meta.WriteRequest{
		Ino:    ino,
		Fh:     fh,
		Offset: crossOffset,
		Data:   payload,
	})
	require.NoError(t, err)
	require.NotNil(t, writeRes)
	assert.Equal(t, uint32(4096), writeRes.Size)
	assert.Equal(t, uint64(crossOffset+4096), writeRes.Inode.Attr.Size)

	// 校验 Chunk 0
	chunk0, err := store.GetChunk(ctx, db.GetChunkParams{Ino: int64(ino), Indx: 0})
	require.NoError(t, err)
	slices0, err := meta.DecodeSlices(chunk0.Slices)
	require.NoError(t, err)
	require.Len(t, slices0, 1)
	assert.Equal(t, uint32(meta.ChunkSize-1024), slices0[0].Pos)
	assert.Equal(t, uint32(1024), slices0[0].Len)
	assert.Equal(t, uint32(0), slices0[0].Off)
	sliceID := slices0[0].ID

	// 校验 Chunk 1
	chunk1, err := store.GetChunk(ctx, db.GetChunkParams{Ino: int64(ino), Indx: 1})
	require.NoError(t, err)
	slices1, err := meta.DecodeSlices(chunk1.Slices)
	require.NoError(t, err)
	require.Len(t, slices1, 1)
	assert.Equal(t, uint32(0), slices1[0].Pos)
	assert.Equal(t, uint32(3072), slices1[0].Len)
	assert.Equal(t, uint32(1024), slices1[0].Off)
	assert.Equal(t, sliceID, slices1[0].ID)

	// 校验 slice_refs 引用计数为 2
	ref, err := store.GetSliceRef(ctx, int64(sliceID))
	require.NoError(t, err)
	assert.Equal(t, int32(2), ref.Refs)
	assert.Equal(t, int32(4096), ref.Size)

	// 2. 测试 O_APPEND 句柄追加
	appendOpenRes, err := mgr.Open(ctx, meta.OpenParams{
		Ino:   ino,
		Flags: meta.O_WRONLY | meta.O_APPEND,
	})
	require.NoError(t, err)
	appendFh := appendOpenRes.Fh
	defer func() {
		_ = mgr.Release(context.Background(), meta.ReleaseParams{Ino: ino, Fh: appendFh})
	}()

	appendData := []byte("append-to-tail")
	appendRes, err := mgr.Write(ctx, &meta.WriteRequest{
		Ino:    ino,
		Fh:     appendFh,
		Offset: 0, // 传入 0，应被忽略
		Data:   appendData,
	})
	require.NoError(t, err)
	require.NotNil(t, appendRes)
	assert.Equal(t, uint64(crossOffset+4096+uint64(len(appendData))), appendRes.Inode.Attr.Size)
}
