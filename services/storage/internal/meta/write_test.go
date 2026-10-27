package meta_test

import (
	"context"
	"errors"
	"math"
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

func TestWrite_Validation(t *testing.T) {
	ctx := context.Background()
	mockStore := dbmock.NewMockStore(t)
	mgr := meta.New(mockStore)

	tests := []struct {
		name string
		req  *meta.WriteRequest
	}{
		{
			name: "req 为 nil",
			req:  nil,
		},
		{
			name: "ino 为 0",
			req: &meta.WriteRequest{
				Ino:  0,
				Fh:   1,
				Data: []byte("a"),
			},
		},
		{
			name: "ino 超过 MaxInt64",
			req: &meta.WriteRequest{
				Ino:  uint64(math.MaxInt64) + 1,
				Fh:   1,
				Data: []byte("a"),
			},
		},
		{
			name: "fh 为 0",
			req: &meta.WriteRequest{
				Ino:  1,
				Fh:   0,
				Data: []byte("a"),
			},
		},
		{
			name: "fh 超过 MaxInt64",
			req: &meta.WriteRequest{
				Ino:  1,
				Fh:   uint64(math.MaxInt64) + 1,
				Data: []byte("a"),
			},
		},
		{
			name: "offset 超过 MaxInt64",
			req: &meta.WriteRequest{
				Ino:    1,
				Fh:     1,
				Offset: uint64(math.MaxInt64) + 1,
				Data:   []byte("a"),
			},
		},
		{
			name: "data 超过 4 MiB (BlockSize)",
			req: &meta.WriteRequest{
				Ino:  1,
				Fh:   1,
				Data: make([]byte, meta.BlockSize+1),
			},
		},
		{
			name: "offset + len(data) 溢出 MaxInt64",
			req: &meta.WriteRequest{
				Ino:    1,
				Fh:     1,
				Offset: uint64(math.MaxInt64),
				Data:   []byte("a"),
			},
		},
		{
			name: "session_id 为 0",
			req: &meta.WriteRequest{
				Ino:       1,
				Fh:        1,
				Data:      []byte("a"),
				SessionID: ptrOf(uint64(0)),
			},
		},
		{
			name: "session_id 超过 MaxInt64",
			req: &meta.WriteRequest{
				Ino:       1,
				Fh:        1,
				Data:      []byte("a"),
				SessionID: ptrOf(uint64(math.MaxInt64) + 1),
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			resp, err := mgr.Write(ctx, tc.req)
			require.Nil(t, resp)
			require.ErrorIs(t, err, meta.ErrInvalidArgument)
		})
	}
}

func TestWrite_ZeroLengthData(t *testing.T) {
	ctx := context.Background()
	now := time.Now().Truncate(time.Microsecond)
	ino := uint64(100)
	fh := uint64(10)

	baseFileRow := db.Inode{
		Ino:        int64(ino),
		Generation: 1,
		Mode:       int32(meta.S_IFREG | meta.S_ISUID | meta.S_ISGID | 0o644),
		Size:       1024,
		Nlink:      1,
		Uid:        1000,
		Gid:        1000,
		Rdev:       0,
		Atime:      pgtype.Timestamptz{Time: now.Add(-time.Hour), Valid: true},
		Mtime:      pgtype.Timestamptz{Time: now.Add(-time.Hour), Valid: true},
		Ctime:      pgtype.Timestamptz{Time: now.Add(-time.Hour), Valid: true},
	}

	t.Run("0 字节写常规文件且句柄具备写权限时直接返回且不修改元数据与特权位", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

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
				Flags: int32(meta.O_WRONLY),
			}, nil).
			Once()

		resp, err := mgr.Write(ctx, &meta.WriteRequest{
			Ino:    ino,
			Fh:     fh,
			Offset: 10,
			Data:   nil, // len == 0
		})

		require.NoError(t, err)
		require.NotNil(t, resp)
		assert.Equal(t, uint32(0), resp.Size)
		require.NotNil(t, resp.Inode)
		assert.Equal(t, ino, resp.Inode.Ino)
		assert.Equal(t, uint64(1024), resp.Inode.Attr.Size)
		// 特权位不应被修改
		assert.Equal(t, uint32(meta.S_IFREG|meta.S_ISUID|meta.S_ISGID|0o644), resp.Inode.Attr.Mode)
	})

	t.Run("0 字节写带 session_id 时续期会话", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)
		sessionID := uint64(999)

		setupMockWriteTx(mockStore, mockQuerier)

		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(ino)).
			Return(baseFileRow, nil).
			Once()

		mockQuerier.EXPECT().
			GetFileHandle(mock.Anything, db.GetFileHandleParams{
				Fh:        int64(fh),
				Ino:       int64(ino),
				SessionID: pgtype.Int8{Int64: int64(sessionID), Valid: true},
			}).
			Return(db.FileHandle{
				Fh:        int64(fh),
				Ino:       int64(ino),
				Flags:     int32(meta.O_RDWR),
				SessionID: pgtype.Int8{Int64: int64(sessionID), Valid: true},
			}, nil).
			Once()

		setupRenewClientSessionMocks(mockQuerier, sessionID)

		resp, err := mgr.Write(ctx, &meta.WriteRequest{
			Ino:       ino,
			Fh:        fh,
			Offset:    0,
			Data:      []byte{},
			SessionID: &sessionID,
		})

		require.NoError(t, err)
		require.NotNil(t, resp)
		assert.Equal(t, uint32(0), resp.Size)
	})

	t.Run("0 字节写目标为目录返回 ErrIsDir", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockWriteTx(mockStore, mockQuerier)

		dirRow := baseFileRow
		dirRow.Mode = int32(meta.S_IFDIR | 0o755)

		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(ino)).
			Return(dirRow, nil).
			Once()

		_, err := mgr.Write(ctx, &meta.WriteRequest{
			Ino:  ino,
			Fh:   fh,
			Data: nil,
		})
		require.ErrorIs(t, err, meta.ErrIsDir)
	})

	t.Run("0 字节写目标是非常规文件返回 ErrInvalidArgument", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockWriteTx(mockStore, mockQuerier)

		fifoRow := baseFileRow
		fifoRow.Mode = int32(meta.S_IFIFO | 0o644)

		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(ino)).
			Return(fifoRow, nil).
			Once()

		_, err := mgr.Write(ctx, &meta.WriteRequest{
			Ino:  ino,
			Fh:   fh,
			Data: nil,
		})
		require.ErrorIs(t, err, meta.ErrInvalidArgument)
	})

	t.Run("0 字节写目标 Inode 不存在返回 ErrNotFound", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockWriteTx(mockStore, mockQuerier)

		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(ino)).
			Return(db.Inode{}, pgx.ErrNoRows).
			Once()

		_, err := mgr.Write(ctx, &meta.WriteRequest{
			Ino:  ino,
			Fh:   fh,
			Data: nil,
		})
		require.ErrorIs(t, err, meta.ErrNotFound)
	})

	t.Run("0 字节写句柄为只读 O_RDONLY 返回 ErrBadFileHandle", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockWriteTx(mockStore, mockQuerier)

		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(ino)).
			Return(baseFileRow, nil).
			Once()

		mockQuerier.EXPECT().
			GetFileHandle(mock.Anything, mock.Anything).
			Return(db.FileHandle{
				Fh:    int64(fh),
				Ino:   int64(ino),
				Flags: int32(meta.O_RDONLY),
			}, nil).
			Once()

		_, err := mgr.Write(ctx, &meta.WriteRequest{
			Ino:  ino,
			Fh:   fh,
			Data: nil,
		})
		require.ErrorIs(t, err, meta.ErrBadFileHandle)
	})

	t.Run("0 字节写句柄不存在返回 ErrBadFileHandle", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockWriteTx(mockStore, mockQuerier)

		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(ino)).
			Return(baseFileRow, nil).
			Once()

		mockQuerier.EXPECT().
			GetFileHandle(mock.Anything, mock.Anything).
			Return(db.FileHandle{}, pgx.ErrNoRows).
			Once()

		_, err := mgr.Write(ctx, &meta.WriteRequest{
			Ino:  ino,
			Fh:   fh,
			Data: nil,
		})
		require.ErrorIs(t, err, meta.ErrBadFileHandle)
	})
}

func TestWrite_TwoPhaseCommit(t *testing.T) {
	ctx := context.Background()
	now := time.Now().Truncate(time.Microsecond)
	ino := uint64(100)
	fh := uint64(10)
	sliceID := int64(42)
	payload := []byte("hello-data-layer")

	baseFileRow := db.Inode{
		Ino:        int64(ino),
		Generation: 1,
		Mode:       int32(meta.S_IFREG | meta.S_ISUID | meta.S_ISGID | 0o755),
		Size:       50,
		Nlink:      1,
		Uid:        1000,
		Gid:        1000,
		Rdev:       0,
		Atime:      pgtype.Timestamptz{Time: now.Add(-time.Hour), Valid: true},
		Mtime:      pgtype.Timestamptz{Time: now.Add(-time.Hour), Valid: true},
		Ctime:      pgtype.Timestamptz{Time: now.Add(-time.Hour), Valid: true},
	}

	t.Run("两阶段正常写入成功：文件扩容、特权位剥离、mtime/ctime刷新", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mockBS := metamock.NewMockBlockStore(t)
		mgr := meta.New(mockStore, meta.WithBlockStore(mockBS))

		// 阶段 1：锁外申请 slice_id 与写入物理块
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
				Flags: int32(meta.O_WRONLY),
			}, nil).
			Once()

		expectedSliceBytes := meta.EncodeSlice(meta.Slice{
			Pos:  50,
			ID:   uint64(sliceID),
			Size: uint32(len(payload)),
			Off:  0,
			Len:  uint32(len(payload)),
		})

		mockQuerier.EXPECT().
			AppendChunkSlice(mock.Anything, db.AppendChunkSliceParams{
				Ino:    int64(ino),
				Indx:   0,
				Slices: expectedSliceBytes,
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

		// offset=50, len=16 -> newEnd=66 > 50 -> 扩容至 66
		expectedNewSize := int64(50 + len(payload))
		expectedNewMode := int32(meta.S_IFREG | 0o755) // S_ISUID 与 S_ISGID 被剥离

		updatedFileRow := baseFileRow
		updatedFileRow.Size = expectedNewSize
		updatedFileRow.Mode = expectedNewMode
		updatedFileRow.Mtime = pgtype.Timestamptz{Time: now, Valid: true}
		updatedFileRow.Ctime = pgtype.Timestamptz{Time: now, Valid: true}

		mockQuerier.EXPECT().
			UpdateInodeSizeAndTouchForWrite(mock.Anything, db.UpdateInodeSizeAndTouchForWriteParams{
				Ino:  int64(ino),
				Size: expectedNewSize,
				Mode: expectedNewMode,
			}).
			Return(updatedFileRow, nil).
			Once()

		resp, err := mgr.Write(ctx, &meta.WriteRequest{
			Ino:    ino,
			Fh:     fh,
			Offset: 50,
			Data:   payload,
		})

		require.NoError(t, err)
		require.NotNil(t, resp)
		assert.Equal(t, uint32(len(payload)), resp.Size)
		require.NotNil(t, resp.Inode)
		assert.Equal(t, uint64(expectedNewSize), resp.Inode.Attr.Size)
		assert.Equal(t, uint32(expectedNewMode), resp.Inode.Attr.Mode)
	})

	t.Run("内部覆写：offset+len <= oldSize 时文件尺寸保持不变，但特权位剥离且更新时间戳", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mockBS := metamock.NewMockBlockStore(t)
		mgr := meta.New(mockStore, meta.WithBlockStore(mockBS))

		largeFileRow := baseFileRow
		largeFileRow.Size = 1000 // 当前大小 1000

		// 阶段 1
		mockStore.EXPECT().
			NextSliceID(mock.Anything).
			Return(sliceID, nil).
			Once()

		mockBS.EXPECT().
			PutBlock(mock.Anything, uint64(sliceID), uint32(0), payload).
			Return(nil).
			Once()

		// 阶段 2
		setupMockWriteTx(mockStore, mockQuerier)

		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(ino)).
			Return(largeFileRow, nil).
			Once()

		mockQuerier.EXPECT().
			GetFileHandle(mock.Anything, mock.Anything).
			Return(db.FileHandle{
				Fh:    int64(fh),
				Ino:   int64(ino),
				Flags: int32(meta.O_RDWR),
			}, nil).
			Once()

		mockQuerier.EXPECT().
			AppendChunkSlice(mock.Anything, mock.Anything).
			Return(nil).
			Once()

		mockQuerier.EXPECT().
			InsertSliceRef(mock.Anything, mock.Anything).
			Return(nil).
			Once()

		// offset=100, len=16 -> newEnd=116 <= 1000，size 保持 1000
		expectedNewMode := int32(meta.S_IFREG | 0o755)
		updatedFileRow := largeFileRow
		updatedFileRow.Mode = expectedNewMode
		updatedFileRow.Mtime = pgtype.Timestamptz{Time: now, Valid: true}
		updatedFileRow.Ctime = pgtype.Timestamptz{Time: now, Valid: true}

		mockQuerier.EXPECT().
			UpdateInodeSizeAndTouchForWrite(mock.Anything, db.UpdateInodeSizeAndTouchForWriteParams{
				Ino:  int64(ino),
				Size: 1000,
				Mode: expectedNewMode,
			}).
			Return(updatedFileRow, nil).
			Once()

		resp, err := mgr.Write(ctx, &meta.WriteRequest{
			Ino:    ino,
			Fh:     fh,
			Offset: 100,
			Data:   payload,
		})

		require.NoError(t, err)
		require.NotNil(t, resp)
		assert.Equal(t, uint64(1000), resp.Inode.Attr.Size)
		assert.Equal(t, uint32(expectedNewMode), resp.Inode.Attr.Mode)
	})

	t.Run("阶段 1 PutBlock 失败：返回 ErrIO 且不污染数据库元数据", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockBS := metamock.NewMockBlockStore(t)
		mgr := meta.New(mockStore, meta.WithBlockStore(mockBS))

		mockStore.EXPECT().
			NextSliceID(mock.Anything).
			Return(sliceID, nil).
			Once()

		mockBS.EXPECT().
			PutBlock(mock.Anything, uint64(sliceID), uint32(0), payload).
			Return(errors.New("disk write failure")).
			Once()

		resp, err := mgr.Write(ctx, &meta.WriteRequest{
			Ino:    ino,
			Fh:     fh,
			Offset: 0,
			Data:   payload,
		})

		require.Nil(t, resp)
		require.ErrorIs(t, err, meta.ErrIO)
	})

	t.Run("阶段 2 事务回滚失败：在事务外自动调用 DeleteBlock 清理物理块确保零残留", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mockBS := metamock.NewMockBlockStore(t)
		mgr := meta.New(mockStore, meta.WithBlockStore(mockBS))

		// 阶段 1 成功
		mockStore.EXPECT().
			NextSliceID(mock.Anything).
			Return(sliceID, nil).
			Once()

		mockBS.EXPECT().
			PutBlock(mock.Anything, uint64(sliceID), uint32(0), payload).
			Return(nil).
			Once()

		// 阶段 2 事务中 Inode 查询报错导致回滚
		setupMockWriteTx(mockStore, mockQuerier)

		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(ino)).
			Return(db.Inode{}, pgx.ErrNoRows).
			Once()

		// 关键断言：事务外必须调用 DeleteBlock 回滚清理阶段 1 物理块！
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
		require.ErrorIs(t, err, meta.ErrNotFound)
	})

	t.Run("阶段 2 句柄只读返回 ErrBadFileHandle 并清理物理块", func(t *testing.T) {
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
			GetFileHandle(mock.Anything, mock.Anything).
			Return(db.FileHandle{
				Fh:    int64(fh),
				Ino:   int64(ino),
				Flags: int32(meta.O_RDONLY),
			}, nil).
			Once()

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
		require.ErrorIs(t, err, meta.ErrBadFileHandle)
	})
}

func TestFsync_ValidationAndPersistence(t *testing.T) {
	ctx := context.Background()
	ino := uint64(100)
	fh := uint64(10)

	t.Run("目标不存在返回 ErrNotFound", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockReadOnlyRepeatableReadTx(mockStore, mockQuerier, 1)

		mockQuerier.EXPECT().
			GetInode(mock.Anything, int64(ino)).
			Return(db.Inode{}, pgx.ErrNoRows).
			Once()

		err := mgr.Fsync(ctx, meta.FsyncParams{
			Ino: ino,
			Fh:  fh,
		})
		require.ErrorIs(t, err, meta.ErrNotFound)
	})

	t.Run("句柄不存在返回 ErrBadFileHandle", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockReadOnlyRepeatableReadTx(mockStore, mockQuerier, 1)

		mockQuerier.EXPECT().
			GetInode(mock.Anything, int64(ino)).
			Return(db.Inode{
				Ino:  int64(ino),
				Mode: int32(meta.S_IFREG | 0o644),
			}, nil).
			Once()

		mockQuerier.EXPECT().
			GetFileHandle(mock.Anything, mock.Anything).
			Return(db.FileHandle{}, pgx.ErrNoRows).
			Once()

		err := mgr.Fsync(ctx, meta.FsyncParams{
			Ino: ino,
			Fh:  fh,
		})
		require.ErrorIs(t, err, meta.ErrBadFileHandle)
	})

	t.Run("合法常规文件与句柄 Fsync 成功", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockReadOnlyRepeatableReadTx(mockStore, mockQuerier, 1)

		mockQuerier.EXPECT().
			GetInode(mock.Anything, int64(ino)).
			Return(db.Inode{
				Ino:  int64(ino),
				Mode: int32(meta.S_IFREG | 0o644),
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
				Flags: int32(meta.O_WRONLY),
			}, nil).
			Once()

		err := mgr.Fsync(ctx, meta.FsyncParams{
			Ino: ino,
			Fh:  fh,
		})
		require.NoError(t, err)
	})
}

// 辅助方法：生成指针
func ptrOf[T any](v T) *T {
	return &v
}

func TestWrite_PostgresIntegration(t *testing.T) {
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

	// 创建测试常规文件
	fileRow, err := store.CreateInode(ctx, db.CreateInodeParams{
		Mode:  int32(meta.S_IFREG | meta.S_ISUID | meta.S_ISGID | 0o755),
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

	// 打开句柄
	openRes, err := mgr.Open(ctx, meta.OpenParams{
		Ino:   ino,
		Flags: meta.O_RDWR,
	})
	require.NoError(t, err)
	fh := openRes.Fh
	defer func() {
		_ = mgr.Release(context.Background(), meta.ReleaseParams{
			Ino: ino,
			Fh:  fh,
		})
	}()

	// 1. 首次写入 10 字节
	data1 := []byte("0123456789")
	writeRes, err := mgr.Write(ctx, &meta.WriteRequest{
		Ino:    ino,
		Fh:     fh,
		Offset: 0,
		Data:   data1,
	})
	require.NoError(t, err)
	require.NotNil(t, writeRes)
	assert.Equal(t, uint32(10), writeRes.Size)
	assert.Equal(t, uint64(10), writeRes.Inode.Attr.Size)
	// 特权位被剥离
	assert.Equal(t, uint32(meta.S_IFREG|0o755), writeRes.Inode.Attr.Mode)

	// 验证 chunks 表记录
	chunkRow, err := store.GetChunk(ctx, db.GetChunkParams{
		Ino:  int64(ino),
		Indx: 0,
	})
	require.NoError(t, err)
	slices, err := meta.DecodeSlices(chunkRow.Slices)
	require.NoError(t, err)
	require.Len(t, slices, 1)
	assert.Equal(t, uint32(0), slices[0].Pos)
	assert.Equal(t, uint32(10), slices[0].Len)
	sliceID1 := slices[0].ID

	// 验证 slice_refs 表记录
	sliceRef, err := store.GetSliceRef(ctx, int64(sliceID1))
	require.NoError(t, err)
	assert.Equal(t, int32(10), sliceRef.Size)
	assert.Equal(t, int32(1), sliceRef.Refs)

	// 验证本地 NVMe 物理块文件精确可读
	readBuf := make([]byte, 10)
	err = bs.GetBlock(ctx, sliceID1, 0, 10, 0, readBuf)
	require.NoError(t, err)
	assert.Equal(t, data1, readBuf)

	// 2. 内部覆写：offset = 2, len = 4 (data = "xxxx")，写入区间 [2, 6) <= 10
	data2 := []byte("xxxx")
	writeRes2, err := mgr.Write(ctx, &meta.WriteRequest{
		Ino:    ino,
		Fh:     fh,
		Offset: 2,
		Data:   data2,
	})
	require.NoError(t, err)
	assert.Equal(t, uint32(4), writeRes2.Size)
	assert.Equal(t, uint64(10), writeRes2.Inode.Attr.Size, "内部覆写不应改变文件总尺寸")

	// 验证 chunks 追加了第 2 个 slice
	chunkRow2, err := store.GetChunk(ctx, db.GetChunkParams{
		Ino:  int64(ino),
		Indx: 0,
	})
	require.NoError(t, err)
	slices2, err := meta.DecodeSlices(chunkRow2.Slices)
	require.NoError(t, err)
	require.Len(t, slices2, 2)
	assert.Equal(t, uint32(2), slices2[1].Pos)
	assert.Equal(t, uint32(4), slices2[1].Len)

	// 3. Fsync 验证
	err = mgr.Fsync(ctx, meta.FsyncParams{
		Ino: ino,
		Fh:  fh,
	})
	require.NoError(t, err)
}
