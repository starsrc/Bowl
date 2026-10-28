package meta_test

import (
	"context"
	"errors"
	"math"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/starsrc/Aether/pkg/tx"
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
			Return(int64(24), nil).
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
			Return(int64(24), nil).
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

// cleanupWriteTestInoAndSlices 清理目标 Inode 关联的所有 chunks 及产生的测试专有 slice_refs，并断言零残留
func cleanupWriteTestInoAndSlices(t *testing.T, store db.Store, mgr meta.Manager, ino int64, fhs ...uint64) {
	t.Helper()
	cleanupCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if mgr != nil {
		for _, fh := range fhs {
			_ = mgr.Release(cleanupCtx, meta.ReleaseParams{Ino: uint64(ino), Fh: fh})
		}
	}

	var sliceIDs []int64
	for indx := int32(0); indx < 16; indx++ {
		chunkRow, err := store.GetChunk(cleanupCtx, db.GetChunkParams{Ino: ino, Indx: indx})
		if err == nil && len(chunkRow.Slices) > 0 {
			if slices, err := meta.DecodeSlices(chunkRow.Slices); err == nil {
				for _, s := range slices {
					if s.ID > 0 {
						sliceIDs = append(sliceIDs, int64(s.ID))
					}
				}
			}
		}
	}

	_ = meta.DeleteAllChunksByInos(cleanupCtx, store, []int64{ino})
	for _, sid := range sliceIDs {
		_ = store.DeleteSliceRef(cleanupCtx, sid)
	}
	_ = store.DeleteInode(cleanupCtx, ino)

	// 严格断言目标 ino 的 inodes、chunks 与本测试产生的 slice_refs 均已清除无残留！
	_, inoErr := store.GetInode(cleanupCtx, ino)
	assert.ErrorIs(t, inoErr, pgx.ErrNoRows, "目标 ino 必须已被清除")

	for indx := int32(0); indx < 16; indx++ {
		_, cErr := store.GetChunk(cleanupCtx, db.GetChunkParams{Ino: ino, Indx: indx})
		assert.ErrorIs(t, cErr, pgx.ErrNoRows, "目标 ino 的 chunk 行必须已被清除")
	}

	for _, sid := range sliceIDs {
		_, sErr := store.GetSliceRef(cleanupCtx, sid)
		assert.ErrorIs(t, sErr, pgx.ErrNoRows, "测试专有 slice_ref %d 必须已被清除", sid)
	}
}

func TestWrite_PostgresIntegration(t *testing.T) {
	pool, ctx := setupMetaTestDB(t)
	if pool == nil {
		return
	}
	acquireTestSliceGCAdvisoryLock(t, pool)

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

	// 打开句柄
	openRes, err := mgr.Open(ctx, meta.OpenParams{
		Ino:   ino,
		Flags: meta.O_RDWR,
	})
	require.NoError(t, err)
	fh := openRes.Fh
	t.Cleanup(func() {
		cleanupWriteTestInoAndSlices(t, store, mgr, fileRow.Ino, fh)
	})

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

// cancelOnPutBlockStore 包装真实 BlockStore，在 PutBlock 落盘成功后立即取消 ctx。
type cancelOnPutBlockStore struct {
	meta.BlockStore
	cancelFn context.CancelFunc
}

func (s *cancelOnPutBlockStore) PutBlock(ctx context.Context, sliceID uint64, blockIdx uint32, data []byte) error {
	if err := s.BlockStore.PutBlock(ctx, sliceID, blockIdx, data); err != nil {
		return err
	}
	if s.cancelFn != nil {
		s.cancelFn()
	}
	return nil
}

func TestWrite_ContextCanceledAfterPutBlock_CleansUpOrphanBlock(t *testing.T) {
	pool, ctx := setupMetaTestDB(t)
	if pool == nil {
		return
	}
	acquireTestSliceGCAdvisoryLock(t, pool)

	store := db.NewStore(pool)
	dataDir := t.TempDir()
	bs := meta.NewLocalFSBlockStore(dataDir)

	fileRow, err := store.CreateInode(ctx, db.CreateInodeParams{
		Mode:  int32(meta.S_IFREG | 0o755),
		Nlink: 1,
		Uid:   1000,
		Gid:   1000,
	})
	require.NoError(t, err)
	ino := uint64(fileRow.Ino)

	initMgr := meta.New(store, meta.WithBlockStore(bs))
	openRes, err := initMgr.Open(ctx, meta.OpenParams{
		Ino:   ino,
		Flags: meta.O_RDWR,
	})
	require.NoError(t, err)
	fh := openRes.Fh
	t.Cleanup(func() {
		cleanupWriteTestInoAndSlices(t, store, initMgr, fileRow.Ino, fh)
	})

	reqCtx, reqCancel := context.WithCancel(ctx)
	wrappedBS := &cancelOnPutBlockStore{
		BlockStore: bs,
		cancelFn:   reqCancel,
	}
	mgr := meta.New(store, meta.WithBlockStore(wrappedBS))

	writeData := []byte("hello context canceled orphan cleanup test")
	_, writeErr := mgr.Write(reqCtx, &meta.WriteRequest{
		Ino:    ino,
		Fh:     fh,
		Offset: 0,
		Data:   writeData,
	})
	require.Error(t, writeErr, "请求取消后阶段 2 事务应失败")

	var regularFiles int
	walkErr := filepath.WalkDir(dataDir, func(path string, d os.DirEntry, walkErr error) error {
		require.NoError(t, walkErr)
		if !d.IsDir() {
			regularFiles++
		}
		return nil
	})
	require.NoError(t, walkErr)
	assert.Equal(t, 0, regularFiles, "阶段 2 事务因 context 取消失败回滚时，阶段 1 已落盘物理块必须被立即回滚清理，不能残留孤儿块")
}

// commitAckErrorStore 代理真实 Store，允许在底层真实事务 COMMIT 成功后向调用方注入模拟网络确认断连错误。
type commitAckErrorStore struct {
	db.Store
	onExecTxWithOptions func(ctx context.Context, fn func(db.Querier) error, opts ...tx.Option) error
}

func (s *commitAckErrorStore) ExecTxWithOptions(ctx context.Context, fn func(db.Querier) error, opts ...tx.Option) error {
	if s.onExecTxWithOptions != nil {
		return s.onExecTxWithOptions(ctx, fn, opts...)
	}
	return s.Store.ExecTxWithOptions(ctx, fn, opts...)
}

func (s *commitAckErrorStore) ExecTx(ctx context.Context, fn func(db.Querier) error) error {
	return s.ExecTxWithOptions(ctx, fn)
}

func TestWrite_CommitAckErrorAfterServerCommit_DoesNotDeletePublishedBlock(t *testing.T) {
	pool, ctx := setupMetaTestDB(t)
	if pool == nil {
		return
	}
	acquireTestSliceGCAdvisoryLock(t, pool)

	store := db.NewStore(pool)
	dataDir := t.TempDir()
	bs := meta.NewLocalFSBlockStore(dataDir)

	t.Run("PublishedBlockPreservedOnCommitAckError", func(t *testing.T) {
		fileRow, err := store.CreateInode(ctx, db.CreateInodeParams{
			Mode:  int32(meta.S_IFREG | 0o755),
			Nlink: 1,
			Uid:   1000,
			Gid:   1000,
		})
		require.NoError(t, err)
		ino := uint64(fileRow.Ino)

		initMgr := meta.New(store, meta.WithBlockStore(bs))
		openRes, err := initMgr.Open(ctx, meta.OpenParams{
			Ino:   ino,
			Flags: meta.O_RDWR,
		})
		require.NoError(t, err)
		fh := openRes.Fh
		t.Cleanup(func() {
			cleanupWriteTestInoAndSlices(t, store, initMgr, fileRow.Ino, fh)
		})

		mockAckErr := errors.New("commit ack network lost")
		wrappedStore := &commitAckErrorStore{
			Store: store,
			onExecTxWithOptions: func(attemptCtx context.Context, fn func(db.Querier) error, opts ...tx.Option) error {
				if err := store.ExecTxWithOptions(attemptCtx, fn, opts...); err != nil {
					return err
				}
				return mockAckErr
			},
		}
		mgr := meta.New(wrappedStore, meta.WithBlockStore(bs))

		writeData := []byte("important committed data block")
		_, writeErr := mgr.Write(ctx, &meta.WriteRequest{
			Ino:    ino,
			Fh:     fh,
			Offset: 0,
			Data:   writeData,
		})
		require.Error(t, writeErr)
		assert.Contains(t, writeErr.Error(), "commit ack network lost")

		// 服务端已成功 COMMIT，物理块不得被误删，随后读操作必须能够成功读取
		readRes, readErr := initMgr.Read(ctx, &meta.ReadRequest{
			Ino:    ino,
			Fh:     fh,
			Offset: 0,
			Size:   uint32(len(writeData)),
		})
		require.NoError(t, readErr, "已成功 COMMIT 的数据块绝不能因确认阶段断连被误删，Read 不得返回 ErrIO")
		require.NotNil(t, readRes)
		assert.Equal(t, writeData, readRes.Data)
	})

	t.Run("ConcurrentSetAttrZeroAfterCommit_DelegatesToGC", func(t *testing.T) {
		subDataDir := t.TempDir()
		subBS := meta.NewLocalFSBlockStore(subDataDir)

		fileRow, err := store.CreateInode(ctx, db.CreateInodeParams{
			Mode:  int32(meta.S_IFREG | 0o755),
			Nlink: 1,
			Uid:   1000,
			Gid:   1000,
		})
		require.NoError(t, err)
		ino := uint64(fileRow.Ino)

		initMgr := meta.New(store, meta.WithBlockStore(subBS))
		// 排空历史可能残留的待回收垃圾，保证切片计数的纯净与隔离
		_, _ = initMgr.CollectGarbage(ctx, 1000)

		openRes, err := initMgr.Open(ctx, meta.OpenParams{
			Ino:   ino,
			Flags: meta.O_RDWR,
		})
		require.NoError(t, err)
		fh := openRes.Fh
		t.Cleanup(func() {
			cleanupWriteTestInoAndSlices(t, store, initMgr, fileRow.Ino, fh)
		})

		mockAckErr := errors.New("commit ack network lost")
		wrappedStore := &commitAckErrorStore{
			Store: store,
			onExecTxWithOptions: func(attemptCtx context.Context, fn func(db.Querier) error, opts ...tx.Option) error {
				if err := store.ExecTxWithOptions(attemptCtx, fn, opts...); err != nil {
					return err
				}
				// 真实事务 COMMIT 成功后、向 Write 返回 commit ack lost 之前，
				// 模拟并发请求将文件 truncate 为 0（使该 slice 的 refs 降为 0）
				zeroSize := uint64(0)
				_, setAttrErr := initMgr.SetAttr(context.Background(), ino, meta.SetAttrInput{
					Size: &zeroSize,
				})
				if setAttrErr != nil {
					return setAttrErr
				}
				return mockAckErr
			},
		}
		mgr := meta.New(wrappedStore, meta.WithBlockStore(subBS))

		writeData := []byte("data for concurrent truncate to zero test")
		_, writeErr := mgr.Write(ctx, &meta.WriteRequest{
			Ino:    ino,
			Fh:     fh,
			Offset: 0,
			Data:   writeData,
		})
		require.Error(t, writeErr)
		assert.Contains(t, writeErr.Error(), "commit ack network lost")

		// 检查物理块：因为 slice_refs 表中已存在该 slice（refs=0），Write 回滚逻辑不得越权提前删块，
		// 物理块应依然存在，交由 CollectGarbage 回收
		var regularFilesBeforeGC int
		walkErr := filepath.WalkDir(subDataDir, func(path string, d os.DirEntry, walkErr error) error {
			require.NoError(t, walkErr)
			if !d.IsDir() {
				regularFilesBeforeGC++
			}
			return nil
		})
		require.NoError(t, walkErr)
		assert.Equal(t, 1, regularFilesBeforeGC, "slice_refs 已存在（即便 refs=0）时 Write 不得越权删块，块文件在 GC 前必须依然存在")

		reclaimed, gcErr := initMgr.CollectGarbage(ctx, 10)
		require.NoError(t, gcErr)
		assert.Equal(t, 1, reclaimed, "CollectGarbage 应当安全回收此孤儿切片")
	})

	t.Run("InFlightServerCommitNotYetVisible_DoesNotDeleteBlock", func(t *testing.T) {
		subDataDir := t.TempDir()
		subBS := meta.NewLocalFSBlockStore(subDataDir)

		fileRow, err := store.CreateInode(ctx, db.CreateInodeParams{
			Mode:  int32(meta.S_IFREG | 0o755),
			Nlink: 1,
			Uid:   1000,
			Gid:   1000,
		})
		require.NoError(t, err)
		ino := uint64(fileRow.Ino)

		initMgr := meta.New(store, meta.WithBlockStore(subBS))
		openRes, err := initMgr.Open(ctx, meta.OpenParams{
			Ino:   ino,
			Flags: meta.O_RDWR,
		})
		require.NoError(t, err)
		fh := openRes.Fh
		t.Cleanup(func() {
			cleanupWriteTestInoAndSlices(t, store, initMgr, fileRow.Ino, fh)
		})

		allowServerCommitCh := make(chan struct{})
		var closeOnce sync.Once
		closeAllowCommit := func() {
			closeOnce.Do(func() {
				close(allowServerCommitCh)
			})
		}
		t.Cleanup(closeAllowCommit)

		reachedCommitCh := make(chan struct{})
		var onceReached sync.Once
		var bgErr error
		var bgWg sync.WaitGroup

		wrappedStore := &commitAckErrorStore{
			Store: store,
			onExecTxWithOptions: func(attemptCtx context.Context, fn func(db.Querier) error, opts ...tx.Option) error {
				bgWg.Add(1)
				go func() {
					defer bgWg.Done()
					bgCtx, bgCancel := context.WithTimeout(context.Background(), 15*time.Second)
					defer bgCancel()
					bgErr = store.ExecTxWithOptions(bgCtx, func(q db.Querier) error {
						if err := fn(q); err != nil {
							return err
						}
						onceReached.Do(func() {
							close(reachedCommitCh)
						})
						select {
						case <-allowServerCommitCh:
							return nil
						case <-bgCtx.Done():
							return bgCtx.Err()
						}
					}, opts...)
				}()

				select {
				case <-reachedCommitCh:
					return context.DeadlineExceeded
				case <-attemptCtx.Done():
					return attemptCtx.Err()
				}
			},
		}
		mgr := meta.New(wrappedStore, meta.WithBlockStore(subBS))

		writeData := []byte("important in-flight committed data block")
		_, writeErr := mgr.Write(ctx, &meta.WriteRequest{
			Ino:    ino,
			Fh:     fh,
			Offset: 0,
			Data:   writeData,
		})
		require.Error(t, writeErr)
		assert.True(t, errors.Is(writeErr, context.DeadlineExceeded) || strings.Contains(writeErr.Error(), context.DeadlineExceeded.Error()))

		// Write 的 txErr != nil 补偿分支已运行完毕。
		// 放行后台独立 goroutine 执行真正的 PostgreSQL COMMIT 并等待其完成。
		closeAllowCommit()
		bgWg.Wait()
		require.NoError(t, bgErr, "后台服务端事务 COMMIT 必须成功")

		// 服务端已成功 COMMIT，物理块绝不能被误删，随后读操作必须能够成功读取！
		readRes, readErr := initMgr.Read(ctx, &meta.ReadRequest{
			Ino:    ino,
			Fh:     fh,
			Offset: 0,
			Size:   uint32(len(writeData)),
		})
		require.NoError(t, readErr, "在途 COMMIT 的数据块绝不能因外部超时检查被误删，Read 不得返回 ErrIO")
		require.NotNil(t, readRes)
		assert.Equal(t, writeData, readRes.Data)
	})
}

// TestWrite_TxRetryResetsEverReachedCommit_CleansUpBlockOnSecondAttemptFailure 验证写事务重试首行重置 everReachedCommit 并精确清理物理块
func TestWrite_TxRetryResetsEverReachedCommit_CleansUpBlockOnSecondAttemptFailure(t *testing.T) {
	ctx := context.Background()
	now := time.Now().Truncate(time.Microsecond)
	ino := uint64(100)
	fh := uint64(10)

	baseFileRow := db.Inode{
		Ino:        int64(ino),
		Generation: 1,
		Mode:       int32(meta.S_IFREG | 0o644),
		Size:       1024,
		Nlink:      1,
		Uid:        1000,
		Gid:        1000,
		Rdev:       0,
		Atime:      pgtype.Timestamptz{Time: now, Valid: true},
		Mtime:      pgtype.Timestamptz{Time: now, Valid: true},
		Ctime:      pgtype.Timestamptz{Time: now, Valid: true},
	}

	mockStore := dbmock.NewMockStore(t)
	mockBS := metamock.NewMockBlockStore(t)

	mockBS.EXPECT().PutBlock(mock.Anything, uint64(101), uint32(0), []byte("test-data")).Return(nil).Once()
	mockStore.EXPECT().NextSliceID(mock.Anything).Return(int64(101), nil).Once()

	mockQuerier1 := dbmock.NewMockQuerier(t)
	mockQuerier1.EXPECT().GetInodeForUpdate(mock.Anything, int64(ino)).Return(baseFileRow, nil).Once()
	mockQuerier1.EXPECT().GetFileHandle(mock.Anything, db.GetFileHandleParams{
		Fh:  int64(fh),
		Ino: int64(ino),
	}).Return(db.FileHandle{
		Fh:    int64(fh),
		Ino:   int64(ino),
		Flags: int32(meta.O_RDWR),
	}, nil).Once()

	mockQuerier1.EXPECT().InsertSliceRef(mock.Anything, db.InsertSliceRefParams{
		SliceID: 101,
		Size:    int32(len("test-data")),
		Refs:    1,
	}).Return(nil).Once()

	mockQuerier1.EXPECT().AppendChunkSlice(mock.Anything, mock.Anything).Return(int64(1), nil).Once()

	updatedInode := baseFileRow
	mockQuerier1.EXPECT().UpdateInodeSizeAndTouchForWrite(mock.Anything, mock.Anything).Return(updatedInode, nil).Once()

	// 阶段 2 事务第 2 次尝试（在 GetInodeForUpdate 返回 pgx.ErrNoRows，未到达 Commit）
	mockQuerier2 := dbmock.NewMockQuerier(t)
	mockQuerier2.EXPECT().GetInodeForUpdate(mock.Anything, int64(ino)).Return(db.Inode{}, pgx.ErrNoRows).Once()

	var txCallCount int
	mockStore.EXPECT().ExecTxWithOptions(mock.Anything, mock.Anything).RunAndReturn(func(attemptCtx context.Context, fn func(db.Querier) error, opts ...tx.Option) error {
		txCallCount++
		if txCallCount == 1 {
			// 完整跑完闭包 fn(q1)，使 everReachedCommit 置为 true，但事务返回 40001
			if err := fn(mockQuerier1); err != nil {
				return err
			}
			return &pgconn.PgError{Code: "40001"}
		}
		// 重试轮次执行 fn(q2)
		return fn(mockQuerier2)
	}).Twice()

	// 精确调用 1 次 mockBS.DeleteBlock 清理阶段 1 物理块
	mockBS.EXPECT().DeleteBlock(mock.Anything, uint64(101), uint32(0), uint32(len("test-data"))).Return(nil).Once()

	mgr := meta.New(mockStore, meta.WithBlockStore(mockBS))
	resp, err := mgr.Write(ctx, &meta.WriteRequest{
		Ino:    ino,
		Fh:     fh,
		Offset: 0,
		Data:   []byte("test-data"),
	})
	require.Nil(t, resp)
	require.Error(t, err)
	assert.ErrorIs(t, err, meta.ErrNotFound, "第 2 次重试首条 GetInodeForUpdate 返回 ErrNoRows 应映射为 ErrNotFound")
}
