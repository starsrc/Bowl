package meta_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math"
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

// TestRead_Validation 验证 Read 的入参边界防护与合法性静态校验。
func TestRead_Validation(t *testing.T) {
	ctx := context.Background()
	mockStore := dbmock.NewMockStore(t)
	mgr := meta.New(mockStore)

	tests := []struct {
		name string
		req  *meta.ReadRequest
	}{
		{
			name: "req 为 nil",
			req:  nil,
		},
		{
			name: "ino 为 0",
			req: &meta.ReadRequest{
				Ino:  0,
				Fh:   1,
				Size: 1024,
			},
		},
		{
			name: "ino 超过 MaxInt64",
			req: &meta.ReadRequest{
				Ino:  uint64(math.MaxInt64) + 1,
				Fh:   1,
				Size: 1024,
			},
		},
		{
			name: "fh 为 0",
			req: &meta.ReadRequest{
				Ino:  1,
				Fh:   0,
				Size: 1024,
			},
		},
		{
			name: "fh 超过 MaxInt64",
			req: &meta.ReadRequest{
				Ino:  1,
				Fh:   uint64(math.MaxInt64) + 1,
				Size: 1024,
			},
		},
		{
			name: "offset 超过 MaxInt64",
			req: &meta.ReadRequest{
				Ino:    1,
				Fh:     1,
				Offset: uint64(math.MaxInt64) + 1,
				Size:   1024,
			},
		},
		{
			name: "size 超过 4 MiB (BlockSize)",
			req: &meta.ReadRequest{
				Ino:  1,
				Fh:   1,
				Size: meta.BlockSize + 1,
			},
		},
		{
			name: "session_id 为 0",
			req: &meta.ReadRequest{
				Ino:       1,
				Fh:        1,
				Size:      1024,
				SessionID: ptrOf(uint64(0)),
			},
		},
		{
			name: "session_id 超过 MaxInt64",
			req: &meta.ReadRequest{
				Ino:       1,
				Fh:        1,
				Size:      1024,
				SessionID: ptrOf(uint64(math.MaxInt64) + 1),
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			resp, err := mgr.Read(ctx, tc.req)
			require.Nil(t, resp)
			require.ErrorIs(t, err, meta.ErrInvalidArgument)
		})
	}
}

// TestRead_HandleAndFileTypeValidation 验证 Inode 类型校验与 FileHandle 权限判定。
func TestRead_HandleAndFileTypeValidation(t *testing.T) {
	ctx := context.Background()
	now := time.Now().Truncate(time.Microsecond)
	ino := uint64(100)
	fh := uint64(10)

	baseFileRow := db.Inode{
		Ino:   int64(ino),
		Mode:  int32(meta.S_IFREG | 0o644),
		Size:  1024,
		Atime: pgtype.Timestamptz{Time: now, Valid: true},
		Mtime: pgtype.Timestamptz{Time: now, Valid: true},
		Ctime: pgtype.Timestamptz{Time: now, Valid: true},
	}

	t.Run("Inode 不存在返回 ErrNotFound", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockWriteTx(mockStore, mockQuerier)

		mockQuerier.EXPECT().
			GetInode(mock.Anything, int64(ino)).
			Return(db.Inode{}, pgx.ErrNoRows).
			Once()

		resp, err := mgr.Read(ctx, &meta.ReadRequest{
			Ino:  ino,
			Fh:   fh,
			Size: 100,
		})
		require.Nil(t, resp)
		require.ErrorIs(t, err, meta.ErrNotFound)
	})

	t.Run("目标为目录返回 ErrIsDir", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockWriteTx(mockStore, mockQuerier)

		dirRow := baseFileRow
		dirRow.Mode = int32(meta.S_IFDIR | 0o755)

		mockQuerier.EXPECT().
			GetInode(mock.Anything, int64(ino)).
			Return(dirRow, nil).
			Once()

		resp, err := mgr.Read(ctx, &meta.ReadRequest{
			Ino:  ino,
			Fh:   fh,
			Size: 100,
		})
		require.Nil(t, resp)
		require.ErrorIs(t, err, meta.ErrIsDir)
	})

	t.Run("目标为非常规文件返回 ErrInvalidArgument", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockWriteTx(mockStore, mockQuerier)

		sockRow := baseFileRow
		sockRow.Mode = int32(meta.S_IFSOCK | 0o666)

		mockQuerier.EXPECT().
			GetInode(mock.Anything, int64(ino)).
			Return(sockRow, nil).
			Once()

		resp, err := mgr.Read(ctx, &meta.ReadRequest{
			Ino:  ino,
			Fh:   fh,
			Size: 100,
		})
		require.Nil(t, resp)
		require.ErrorIs(t, err, meta.ErrInvalidArgument)
	})

	t.Run("句柄不存在返回 ErrBadFileHandle", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockWriteTx(mockStore, mockQuerier)

		mockQuerier.EXPECT().
			GetInode(mock.Anything, int64(ino)).
			Return(baseFileRow, nil).
			Once()

		mockQuerier.EXPECT().
			GetFileHandle(mock.Anything, db.GetFileHandleParams{
				Fh:  int64(fh),
				Ino: int64(ino),
			}).
			Return(db.FileHandle{}, pgx.ErrNoRows).
			Once()

		resp, err := mgr.Read(ctx, &meta.ReadRequest{
			Ino:  ino,
			Fh:   fh,
			Size: 100,
		})
		require.Nil(t, resp)
		require.ErrorIs(t, err, meta.ErrBadFileHandle)
	})

	t.Run("句柄为只写模式 (O_WRONLY) 返回 ErrBadFileHandle", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockWriteTx(mockStore, mockQuerier)

		mockQuerier.EXPECT().
			GetInode(mock.Anything, int64(ino)).
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

		resp, err := mgr.Read(ctx, &meta.ReadRequest{
			Ino:  ino,
			Fh:   fh,
			Size: 100,
		})
		require.Nil(t, resp)
		require.ErrorIs(t, err, meta.ErrBadFileHandle)
	})

	t.Run("句柄具备 O_RDONLY 读权限允许正常执行", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockWriteTx(mockStore, mockQuerier)

		mockQuerier.EXPECT().
			GetInode(mock.Anything, int64(ino)).
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
				Flags: int32(meta.O_RDONLY),
			}, nil).
			Once()

		mockQuerier.EXPECT().
			GetChunk(mock.Anything, db.GetChunkParams{
				Ino:  int64(ino),
				Indx: 0,
			}).
			Return(db.Chunk{}, pgx.ErrNoRows).
			Once()

		resp, err := mgr.Read(ctx, &meta.ReadRequest{
			Ino:    ino,
			Fh:     fh,
			Offset: 0,
			Size:   10,
		})
		require.NoError(t, err)
		require.NotNil(t, resp)
		assert.Equal(t, make([]byte, 10), resp.Data)
	})

	t.Run("句柄具备 O_RDWR 读写权限允许正常读取", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockWriteTx(mockStore, mockQuerier)

		mockQuerier.EXPECT().
			GetInode(mock.Anything, int64(ino)).
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
				Flags: int32(meta.O_RDWR),
			}, nil).
			Once()

		mockQuerier.EXPECT().
			GetChunk(mock.Anything, db.GetChunkParams{
				Ino:  int64(ino),
				Indx: 0,
			}).
			Return(db.Chunk{}, pgx.ErrNoRows).
			Once()

		resp, err := mgr.Read(ctx, &meta.ReadRequest{
			Ino:    ino,
			Fh:     fh,
			Offset: 0,
			Size:   10,
		})
		require.NoError(t, err)
		require.NotNil(t, resp)
		assert.Equal(t, make([]byte, 10), resp.Data)
	})
}

// TestRead_EOFAndTruncation 验证 POSIX pread(2) 到达 EOF 的空流返回与越界自动截断语义。
func TestRead_EOFAndTruncation(t *testing.T) {
	ctx := context.Background()
	now := time.Now().Truncate(time.Microsecond)
	ino := uint64(200)
	fh := uint64(20)

	fileRow := db.Inode{
		Ino:   int64(ino),
		Mode:  int32(meta.S_IFREG | 0o644),
		Size:  100, // 文件大小 100 字节
		Atime: pgtype.Timestamptz{Time: now, Valid: true},
		Mtime: pgtype.Timestamptz{Time: now, Valid: true},
		Ctime: pgtype.Timestamptz{Time: now, Valid: true},
	}

	t.Run("Size == 0 时直接返回空字节流且不查 Chunk 表", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

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

		resp, err := mgr.Read(ctx, &meta.ReadRequest{
			Ino:    ino,
			Fh:     fh,
			Offset: 0,
			Size:   0,
		})
		require.NoError(t, err)
		require.NotNil(t, resp)
		assert.Empty(t, resp.Data)
	})

	t.Run("Offset == Size 时 (恰好 EOF) 直接返回空字节流", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

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

		resp, err := mgr.Read(ctx, &meta.ReadRequest{
			Ino:    ino,
			Fh:     fh,
			Offset: 100, // 等于 inode.Size
			Size:   50,
		})
		require.NoError(t, err)
		require.NotNil(t, resp)
		assert.Empty(t, resp.Data)
	})

	t.Run("Offset > Size 时 (超越 EOF) 直接返回空字节流", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

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

		resp, err := mgr.Read(ctx, &meta.ReadRequest{
			Ino:    ino,
			Fh:     fh,
			Offset: 200, // 远大于 inode.Size
			Size:   50,
		})
		require.NoError(t, err)
		require.NotNil(t, resp)
		assert.Empty(t, resp.Data)
	})

	t.Run("Offset + Size > Size 时自动截断实际读取长度为 inode.Size - Offset", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mockBS := metamock.NewMockBlockStore(t)
		mgr := meta.New(mockStore, meta.WithBlockStore(mockBS))

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

		// 构造一个包含完整 100 字节的 chunk
		sliceID := uint64(555)
		sliceDesc := meta.Slice{
			Pos:  0,
			ID:   sliceID,
			Size: 100,
			Off:  0,
			Len:  100,
		}
		rawSlices := meta.EncodeSlice(sliceDesc)

		mockQuerier.EXPECT().
			GetChunk(mock.Anything, db.GetChunkParams{
				Ino:  int64(ino),
				Indx: 0,
			}).
			Return(db.Chunk{
				Ino:    int64(ino),
				Indx:   0,
				Slices: rawSlices,
			}, nil).
			Once()

		// 请求 offset=80, size=50 -> 超出 100 边界，截断为 readLen = 100 - 80 = 20
		// overlapStart = 80, overlapEnd = 100, overlapLen = 20, sliceOff = 80
		expectedData := bytes.Repeat([]byte("T"), 20)
		mockBS.EXPECT().
			GetBlock(mock.Anything, sliceID, uint32(0), uint32(100), uint32(80), mock.Anything).
			RunAndReturn(func(ctx context.Context, sid uint64, bIdx, bSize, off uint32, dst []byte) error {
				require.Len(t, dst, 20)
				copy(dst, expectedData)
				return nil
			}).
			Once()

		resp, err := mgr.Read(ctx, &meta.ReadRequest{
			Ino:    ino,
			Fh:     fh,
			Offset: 80,
			Size:   50,
		})
		require.NoError(t, err)
		require.NotNil(t, resp)
		assert.Equal(t, 20, len(resp.Data), "实际返回长度必须被截断为 20 字节")
		assert.Equal(t, expectedData, resp.Data)
	})
}

// TestRead_SparseHolesAndUnwrittenChunks 验证稀疏文件未写入 Chunk 与跳写空洞切片直接读零且不产生磁盘 IO。
func TestRead_SparseHolesAndUnwrittenChunks(t *testing.T) {
	ctx := context.Background()
	now := time.Now().Truncate(time.Microsecond)
	ino := uint64(300)
	fh := uint64(30)

	fileRow := db.Inode{
		Ino:   int64(ino),
		Mode:  int32(meta.S_IFREG | 0o644),
		Size:  1000, // 逻辑大小 1000
		Atime: pgtype.Timestamptz{Time: now, Valid: true},
		Mtime: pgtype.Timestamptz{Time: now, Valid: true},
		Ctime: pgtype.Timestamptz{Time: now, Valid: true},
	}

	t.Run("目标 Chunk 在 chunks 表中不存在时直接返回全零缓冲区且不触发 BlockStore", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mockBS := metamock.NewMockBlockStore(t)
		mgr := meta.New(mockStore, meta.WithBlockStore(mockBS))

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

		mockQuerier.EXPECT().
			GetChunk(mock.Anything, db.GetChunkParams{
				Ino:  int64(ino),
				Indx: 0,
			}).
			Return(db.Chunk{}, pgx.ErrNoRows).
			Once()

		resp, err := mgr.Read(ctx, &meta.ReadRequest{
			Ino:    ino,
			Fh:     fh,
			Offset: 0,
			Size:   64,
		})
		require.NoError(t, err)
		require.NotNil(t, resp)
		assert.Equal(t, make([]byte, 64), resp.Data)
		// mockBS 绝未被调用
	})

	t.Run("chunks 表中的 slices 为空时直接返回全零缓冲区", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mockBS := metamock.NewMockBlockStore(t)
		mgr := meta.New(mockStore, meta.WithBlockStore(mockBS))

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

		mockQuerier.EXPECT().
			GetChunk(mock.Anything, db.GetChunkParams{
				Ino:  int64(ino),
				Indx: 0,
			}).
			Return(db.Chunk{
				Ino:    int64(ino),
				Indx:   0,
				Slices: []byte{},
			}, nil).
			Once()

		resp, err := mgr.Read(ctx, &meta.ReadRequest{
			Ino:    ino,
			Fh:     fh,
			Offset: 10,
			Size:   30,
		})
		require.NoError(t, err)
		require.NotNil(t, resp)
		assert.Equal(t, make([]byte, 30), resp.Data)
	})

	t.Run("稀疏跳写空洞切片 (ID == 0) 与视图末尾区域读零", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mockBS := metamock.NewMockBlockStore(t)
		mgr := meta.New(mockStore, meta.WithBlockStore(mockBS))

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

		// 仅在 pos=100 处写入了 50 字节数据，区间 [100, 150)
		sliceID := uint64(777)
		sliceDesc := meta.Slice{
			Pos:  100,
			ID:   sliceID,
			Size: 50,
			Off:  0,
			Len:  50,
		}
		rawSlices := meta.EncodeSlice(sliceDesc)

		mockQuerier.EXPECT().
			GetChunk(mock.Anything, db.GetChunkParams{
				Ino:  int64(ino),
				Indx: 0,
			}).
			Return(db.Chunk{
				Ino:    int64(ino),
				Indx:   0,
				Slices: rawSlices,
			}, nil).
			Once()

		// 请求读取 [50, 200) 共 150 字节：
		// [50, 100) 为 BuildSlice 合成的空洞区间 -> 保持 0x00
		// [100, 150) 为有效切片数据 -> 从 BlockStore 读取 50 字节
		// [150, 200) 为视图末尾之外的未写入区域 -> 保持 0x00
		realData := bytes.Repeat([]byte{0xEE}, 50)
		mockBS.EXPECT().
			GetBlock(mock.Anything, sliceID, uint32(0), uint32(50), uint32(0), mock.Anything).
			RunAndReturn(func(ctx context.Context, sid uint64, bIdx, bSize, off uint32, dst []byte) error {
				require.Len(t, dst, 50)
				copy(dst, realData)
				return nil
			}).
			Once()

		resp, err := mgr.Read(ctx, &meta.ReadRequest{
			Ino:    ino,
			Fh:     fh,
			Offset: 50,
			Size:   150,
		})
		require.NoError(t, err)
		require.NotNil(t, resp)
		assert.Equal(t, 150, len(resp.Data))

		// 验证结构: 50 字节 0x00 + 50 字节 0xEE + 50 字节 0x00
		expectedBuf := make([]byte, 150)
		copy(expectedBuf[50:100], realData)
		assert.Equal(t, expectedBuf, resp.Data)
	})
}

// TestRead_SequentialAndOverwrites 验证顺序写入与各种复杂重叠覆写场景下的 buildSlice 精确重组。
func TestRead_SequentialAndOverwrites(t *testing.T) {
	ctx := context.Background()
	now := time.Now().Truncate(time.Microsecond)
	ino := uint64(400)
	fh := uint64(40)

	fileRow := db.Inode{
		Ino:   int64(ino),
		Mode:  int32(meta.S_IFREG | 0o644),
		Size:  1000,
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

	t.Run("顺序单切片完整读取", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mockBS := metamock.NewMockBlockStore(t)
		mgr := meta.New(mockStore, meta.WithBlockStore(mockBS))

		setupBaseMocks(mockStore, mockQuerier)

		sliceID := uint64(101)
		slices := []meta.Slice{
			{Pos: 0, ID: sliceID, Size: 100, Off: 0, Len: 100},
		}

		mockQuerier.EXPECT().
			GetChunk(mock.Anything, db.GetChunkParams{Ino: int64(ino), Indx: 0}).
			Return(db.Chunk{Ino: int64(ino), Indx: 0, Slices: meta.EncodeSlices(slices)}, nil).
			Once()

		data := bytes.Repeat([]byte("A"), 100)
		mockBS.EXPECT().
			GetBlock(mock.Anything, sliceID, uint32(0), uint32(100), uint32(0), mock.Anything).
			RunAndReturn(func(ctx context.Context, sid uint64, bIdx, bSize, off uint32, dst []byte) error {
				copy(dst, data)
				return nil
			}).
			Once()

		resp, err := mgr.Read(ctx, &meta.ReadRequest{
			Ino:    ino,
			Fh:     fh,
			Offset: 0,
			Size:   100,
		})
		require.NoError(t, err)
		assert.Equal(t, data, resp.Data)
	})

	t.Run("完全覆盖：后写切片完整遮蔽先写切片", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mockBS := metamock.NewMockBlockStore(t)
		mgr := meta.New(mockStore, meta.WithBlockStore(mockBS))

		setupBaseMocks(mockStore, mockQuerier)

		sliceID1 := uint64(101)
		sliceID2 := uint64(102)
		slices := []meta.Slice{
			{Pos: 0, ID: sliceID1, Size: 100, Off: 0, Len: 100}, // 先写全 A
			{Pos: 0, ID: sliceID2, Size: 100, Off: 0, Len: 100}, // 后写全 B 完全覆盖
		}

		mockQuerier.EXPECT().
			GetChunk(mock.Anything, db.GetChunkParams{Ino: int64(ino), Indx: 0}).
			Return(db.Chunk{Ino: int64(ino), Indx: 0, Slices: meta.EncodeSlices(slices)}, nil).
			Once()

		dataB := bytes.Repeat([]byte("B"), 100)
		// 仅从 sliceID2 读取，绝不读取 sliceID1
		mockBS.EXPECT().
			GetBlock(mock.Anything, sliceID2, uint32(0), uint32(100), uint32(0), mock.Anything).
			RunAndReturn(func(ctx context.Context, sid uint64, bIdx, bSize, off uint32, dst []byte) error {
				copy(dst, dataB)
				return nil
			}).
			Once()

		resp, err := mgr.Read(ctx, &meta.ReadRequest{
			Ino:    ino,
			Fh:     fh,
			Offset: 0,
			Size:   100,
		})
		require.NoError(t, err)
		assert.Equal(t, dataB, resp.Data)
	})

	t.Run("头部覆盖：后写切片覆盖前半段", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mockBS := metamock.NewMockBlockStore(t)
		mgr := meta.New(mockStore, meta.WithBlockStore(mockBS))

		setupBaseMocks(mockStore, mockQuerier)

		sliceID1 := uint64(101)
		sliceID2 := uint64(102)
		slices := []meta.Slice{
			{Pos: 0, ID: sliceID1, Size: 100, Off: 0, Len: 100}, // [0, 100) A
			{Pos: 0, ID: sliceID2, Size: 30, Off: 0, Len: 30},   // [0, 30) B 头部覆盖
		}

		mockQuerier.EXPECT().
			GetChunk(mock.Anything, db.GetChunkParams{Ino: int64(ino), Indx: 0}).
			Return(db.Chunk{Ino: int64(ino), Indx: 0, Slices: meta.EncodeSlices(slices)}, nil).
			Once()

		dataB := bytes.Repeat([]byte("B"), 30)
		dataA := bytes.Repeat([]byte("A"), 70)

		// [0, 30) 读 sliceID2
		mockBS.EXPECT().
			GetBlock(mock.Anything, sliceID2, uint32(0), uint32(30), uint32(0), mock.Anything).
			RunAndReturn(func(ctx context.Context, sid uint64, bIdx, bSize, off uint32, dst []byte) error {
				copy(dst, dataB)
				return nil
			}).
			Once()

		// [30, 100) 读 sliceID1 偏移 30 处 70 字节
		mockBS.EXPECT().
			GetBlock(mock.Anything, sliceID1, uint32(0), uint32(100), uint32(30), mock.Anything).
			RunAndReturn(func(ctx context.Context, sid uint64, bIdx, bSize, off uint32, dst []byte) error {
				copy(dst, dataA)
				return nil
			}).
			Once()

		resp, err := mgr.Read(ctx, &meta.ReadRequest{
			Ino:    ino,
			Fh:     fh,
			Offset: 0,
			Size:   100,
		})
		require.NoError(t, err)
		expected := append(dataB, dataA...)
		assert.Equal(t, expected, resp.Data)
	})

	t.Run("尾部覆盖：后写切片覆盖后半段", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mockBS := metamock.NewMockBlockStore(t)
		mgr := meta.New(mockStore, meta.WithBlockStore(mockBS))

		setupBaseMocks(mockStore, mockQuerier)

		sliceID1 := uint64(101)
		sliceID2 := uint64(102)
		slices := []meta.Slice{
			{Pos: 0, ID: sliceID1, Size: 100, Off: 0, Len: 100}, // [0, 100) A
			{Pos: 70, ID: sliceID2, Size: 30, Off: 0, Len: 30},  // [70, 100) B 尾部覆盖
		}

		mockQuerier.EXPECT().
			GetChunk(mock.Anything, db.GetChunkParams{Ino: int64(ino), Indx: 0}).
			Return(db.Chunk{Ino: int64(ino), Indx: 0, Slices: meta.EncodeSlices(slices)}, nil).
			Once()

		dataA := bytes.Repeat([]byte("A"), 70)
		dataB := bytes.Repeat([]byte("B"), 30)

		// [0, 70) 读 sliceID1 偏移 0
		mockBS.EXPECT().
			GetBlock(mock.Anything, sliceID1, uint32(0), uint32(100), uint32(0), mock.Anything).
			RunAndReturn(func(ctx context.Context, sid uint64, bIdx, bSize, off uint32, dst []byte) error {
				copy(dst, dataA)
				return nil
			}).
			Once()

		// [70, 100) 读 sliceID2 偏移 0
		mockBS.EXPECT().
			GetBlock(mock.Anything, sliceID2, uint32(0), uint32(30), uint32(0), mock.Anything).
			RunAndReturn(func(ctx context.Context, sid uint64, bIdx, bSize, off uint32, dst []byte) error {
				copy(dst, dataB)
				return nil
			}).
			Once()

		resp, err := mgr.Read(ctx, &meta.ReadRequest{
			Ino:    ino,
			Fh:     fh,
			Offset: 0,
			Size:   100,
		})
		require.NoError(t, err)
		expected := append(dataA, dataB...)
		assert.Equal(t, expected, resp.Data)
	})

	t.Run("中间劈裂覆盖：后写切片将先写切片劈裂为左右两段", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mockBS := metamock.NewMockBlockStore(t)
		mgr := meta.New(mockStore, meta.WithBlockStore(mockBS))

		setupBaseMocks(mockStore, mockQuerier)

		sliceID1 := uint64(101)
		sliceID2 := uint64(102)
		slices := []meta.Slice{
			{Pos: 0, ID: sliceID1, Size: 100, Off: 0, Len: 100}, // [0, 100) A
			{Pos: 30, ID: sliceID2, Size: 40, Off: 0, Len: 40},  // [30, 70) B 中间覆写
		}

		mockQuerier.EXPECT().
			GetChunk(mock.Anything, db.GetChunkParams{Ino: int64(ino), Indx: 0}).
			Return(db.Chunk{Ino: int64(ino), Indx: 0, Slices: meta.EncodeSlices(slices)}, nil).
			Once()

		dataALeft := bytes.Repeat([]byte("A"), 30)
		dataB := bytes.Repeat([]byte("B"), 40)
		dataARight := bytes.Repeat([]byte("A"), 30)

		// 左段 [0, 30): sliceID1, off=0, len=30
		mockBS.EXPECT().
			GetBlock(mock.Anything, sliceID1, uint32(0), uint32(100), uint32(0), mock.Anything).
			RunAndReturn(func(ctx context.Context, sid uint64, bIdx, bSize, off uint32, dst []byte) error {
				copy(dst, dataALeft)
				return nil
			}).
			Once()

		// 中段 [30, 70): sliceID2, off=0, len=40
		mockBS.EXPECT().
			GetBlock(mock.Anything, sliceID2, uint32(0), uint32(40), uint32(0), mock.Anything).
			RunAndReturn(func(ctx context.Context, sid uint64, bIdx, bSize, off uint32, dst []byte) error {
				copy(dst, dataB)
				return nil
			}).
			Once()

		// 右段 [70, 100): sliceID1, off=70, len=30
		mockBS.EXPECT().
			GetBlock(mock.Anything, sliceID1, uint32(0), uint32(100), uint32(70), mock.Anything).
			RunAndReturn(func(ctx context.Context, sid uint64, bIdx, bSize, off uint32, dst []byte) error {
				copy(dst, dataARight)
				return nil
			}).
			Once()

		resp, err := mgr.Read(ctx, &meta.ReadRequest{
			Ino:    ino,
			Fh:     fh,
			Offset: 0,
			Size:   100,
		})
		require.NoError(t, err)
		expected := append(append(dataALeft, dataB...), dataARight...)
		assert.Equal(t, expected, resp.Data)
	})

	t.Run("中间打洞覆盖 (ID == 0)：中间空洞切片不触发 BlockStore 且填零", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mockBS := metamock.NewMockBlockStore(t)
		mgr := meta.New(mockStore, meta.WithBlockStore(mockBS))

		setupBaseMocks(mockStore, mockQuerier)

		sliceID1 := uint64(101)
		slices := []meta.Slice{
			{Pos: 0, ID: sliceID1, Size: 100, Off: 0, Len: 100}, // [0, 100) A
			{Pos: 30, ID: 0, Size: 0, Off: 0, Len: 40},          // [30, 70) 打洞空洞
		}

		mockQuerier.EXPECT().
			GetChunk(mock.Anything, db.GetChunkParams{Ino: int64(ino), Indx: 0}).
			Return(db.Chunk{Ino: int64(ino), Indx: 0, Slices: meta.EncodeSlices(slices)}, nil).
			Once()

		dataALeft := bytes.Repeat([]byte("A"), 30)
		dataARight := bytes.Repeat([]byte("A"), 30)

		// 左段 [0, 30)
		mockBS.EXPECT().
			GetBlock(mock.Anything, sliceID1, uint32(0), uint32(100), uint32(0), mock.Anything).
			RunAndReturn(func(ctx context.Context, sid uint64, bIdx, bSize, off uint32, dst []byte) error {
				copy(dst, dataALeft)
				return nil
			}).
			Once()

		// 中段 [30, 70) 为空洞，不调用 mockBS！

		// 右段 [70, 100)
		mockBS.EXPECT().
			GetBlock(mock.Anything, sliceID1, uint32(0), uint32(100), uint32(70), mock.Anything).
			RunAndReturn(func(ctx context.Context, sid uint64, bIdx, bSize, off uint32, dst []byte) error {
				copy(dst, dataARight)
				return nil
			}).
			Once()

		resp, err := mgr.Read(ctx, &meta.ReadRequest{
			Ino:    ino,
			Fh:     fh,
			Offset: 0,
			Size:   100,
		})
		require.NoError(t, err)
		expected := append(append(dataALeft, make([]byte, 40)...), dataARight...)
		assert.Equal(t, expected, resp.Data)
	})
}

// TestRead_AtimeModeAndSessionRenew 验证 AtimeMode (noatime/relatime/strictatime) 判定与 session_id 租约续期。
func TestRead_AtimeModeAndSessionRenew(t *testing.T) {
	ctx := context.Background()
	now := time.Now().Truncate(time.Microsecond)
	ino := uint64(500)
	fh := uint64(50)

	setupFileAndChunkMocks := func(mockQuerier *dbmock.MockQuerier, fileRow db.Inode) {
		mockQuerier.EXPECT().
			GetInode(mock.Anything, int64(ino)).
			Return(fileRow, nil).
			Once()
		mockQuerier.EXPECT().
			GetFileHandle(mock.Anything, mock.Anything).
			Return(db.FileHandle{
				Fh:    int64(fh),
				Ino:   int64(ino),
				Flags: int32(meta.O_RDONLY),
			}, nil).
			Once()
		mockQuerier.EXPECT().
			GetChunk(mock.Anything, db.GetChunkParams{Ino: int64(ino), Indx: 0}).
			Return(db.Chunk{}, pgx.ErrNoRows).
			Once()
	}

	t.Run("NoAtime 模式下绝不调用 TouchInodeAtime", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore, meta.WithAtimeMode(meta.AtimeModeNoAtime))

		setupMockWriteTx(mockStore, mockQuerier)

		fileRow := db.Inode{
			Ino:   int64(ino),
			Mode:  int32(meta.S_IFREG | 0o644),
			Size:  100,
			Atime: pgtype.Timestamptz{Time: now.Add(-48 * time.Hour), Valid: true}, // 很久以前
			Mtime: pgtype.Timestamptz{Time: now, Valid: true},
			Ctime: pgtype.Timestamptz{Time: now, Valid: true},
		}
		setupFileAndChunkMocks(mockQuerier, fileRow)

		resp, err := mgr.Read(ctx, &meta.ReadRequest{
			Ino:    ino,
			Fh:     fh,
			Offset: 0,
			Size:   10,
		})
		require.NoError(t, err)
		require.NotNil(t, resp)
		// mockQuerier.TouchInodeAtime 未被期望，若被调用则会 panic/fail
	})

	t.Run("StrictAtime 模式下超过 1 秒触发 TouchInodeAtime", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore, meta.WithAtimeMode(meta.AtimeModeStrictAtime))

		setupMockWriteTx(mockStore, mockQuerier)

		fileRow := db.Inode{
			Ino:   int64(ino),
			Mode:  int32(meta.S_IFREG | 0o644),
			Size:  100,
			Atime: pgtype.Timestamptz{Time: now.Add(-5 * time.Second), Valid: true},
			Mtime: pgtype.Timestamptz{Time: now, Valid: true},
			Ctime: pgtype.Timestamptz{Time: now, Valid: true},
		}
		setupFileAndChunkMocks(mockQuerier, fileRow)

		mockQuerier.EXPECT().
			TouchInodeAtime(mock.Anything, int64(ino)).
			Return(nil).
			Once()

		resp, err := mgr.Read(ctx, &meta.ReadRequest{
			Ino:    ino,
			Fh:     fh,
			Offset: 0,
			Size:   10,
		})
		require.NoError(t, err)
		require.NotNil(t, resp)
	})

	t.Run("RelAtime 模式下 atime < mtime 触发 TouchInodeAtime", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore, meta.WithAtimeMode(meta.AtimeModeRelAtime))

		setupMockWriteTx(mockStore, mockQuerier)

		fileRow := db.Inode{
			Ino:   int64(ino),
			Mode:  int32(meta.S_IFREG | 0o644),
			Size:  100,
			Atime: pgtype.Timestamptz{Time: now.Add(-10 * time.Minute), Valid: true},
			Mtime: pgtype.Timestamptz{Time: now.Add(-5 * time.Minute), Valid: true}, // mtime 更新
			Ctime: pgtype.Timestamptz{Time: now.Add(-5 * time.Minute), Valid: true},
		}
		setupFileAndChunkMocks(mockQuerier, fileRow)

		mockQuerier.EXPECT().
			TouchInodeAtime(mock.Anything, int64(ino)).
			Return(nil).
			Once()

		resp, err := mgr.Read(ctx, &meta.ReadRequest{
			Ino:    ino,
			Fh:     fh,
			Offset: 0,
			Size:   10,
		})
		require.NoError(t, err)
		require.NotNil(t, resp)
	})

	t.Run("RelAtime 模式下 atime >= mtime 且距离现在不足 24h 不触发 TouchInodeAtime", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore, meta.WithAtimeMode(meta.AtimeModeRelAtime))

		setupMockWriteTx(mockStore, mockQuerier)

		fileRow := db.Inode{
			Ino:   int64(ino),
			Mode:  int32(meta.S_IFREG | 0o644),
			Size:  100,
			Atime: pgtype.Timestamptz{Time: now.Add(-1 * time.Hour), Valid: true},
			Mtime: pgtype.Timestamptz{Time: now.Add(-2 * time.Hour), Valid: true}, // atime 比 mtime 新
			Ctime: pgtype.Timestamptz{Time: now.Add(-2 * time.Hour), Valid: true},
		}
		setupFileAndChunkMocks(mockQuerier, fileRow)

		resp, err := mgr.Read(ctx, &meta.ReadRequest{
			Ino:    ino,
			Fh:     fh,
			Offset: 0,
			Size:   10,
		})
		require.NoError(t, err)
		require.NotNil(t, resp)
	})

	t.Run("携带有效 session_id 时触发会话续期", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)
		sessionID := uint64(8888)

		setupMockWriteTx(mockStore, mockQuerier)

		fileRow := db.Inode{
			Ino:   int64(ino),
			Mode:  int32(meta.S_IFREG | 0o644),
			Size:  100,
			Atime: pgtype.Timestamptz{Time: now, Valid: true},
			Mtime: pgtype.Timestamptz{Time: now, Valid: true},
			Ctime: pgtype.Timestamptz{Time: now, Valid: true},
		}

		mockQuerier.EXPECT().
			GetInode(mock.Anything, int64(ino)).
			Return(fileRow, nil).
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
				Flags:     int32(meta.O_RDONLY),
				SessionID: pgtype.Int8{Int64: int64(sessionID), Valid: true},
			}, nil).
			Once()

		setupRenewClientSessionMocks(mockQuerier, sessionID)

		mockQuerier.EXPECT().
			GetChunk(mock.Anything, db.GetChunkParams{Ino: int64(ino), Indx: 0}).
			Return(db.Chunk{}, pgx.ErrNoRows).
			Once()

		resp, err := mgr.Read(ctx, &meta.ReadRequest{
			Ino:       ino,
			Fh:        fh,
			Offset:    0,
			Size:      10,
			SessionID: &sessionID,
		})
		require.NoError(t, err)
		require.NotNil(t, resp)
	})
}

// TestRead_BlockStoreIOError 验证 BlockStore 发生读取错误时向上透传包装包含 ErrIO 的错误。
func TestRead_BlockStoreIOError(t *testing.T) {
	ctx := context.Background()
	now := time.Now().Truncate(time.Microsecond)
	ino := uint64(600)
	fh := uint64(60)

	fileRow := db.Inode{
		Ino:   int64(ino),
		Mode:  int32(meta.S_IFREG | 0o644),
		Size:  100,
		Atime: pgtype.Timestamptz{Time: now, Valid: true},
		Mtime: pgtype.Timestamptz{Time: now, Valid: true},
		Ctime: pgtype.Timestamptz{Time: now, Valid: true},
	}

	mockStore := dbmock.NewMockStore(t)
	mockQuerier := dbmock.NewMockQuerier(t)
	mockBS := metamock.NewMockBlockStore(t)
	mgr := meta.New(mockStore, meta.WithBlockStore(mockBS))

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

	sliceID := uint64(999)
	sliceDesc := meta.Slice{
		Pos:  0,
		ID:   sliceID,
		Size: 100,
		Off:  0,
		Len:  100,
	}

	mockQuerier.EXPECT().
		GetChunk(mock.Anything, db.GetChunkParams{Ino: int64(ino), Indx: 0}).
		Return(db.Chunk{Ino: int64(ino), Indx: 0, Slices: meta.EncodeSlice(sliceDesc)}, nil).
		Once()

	mockBS.EXPECT().
		GetBlock(mock.Anything, sliceID, uint32(0), uint32(100), uint32(0), mock.Anything).
		Return(fmt.Errorf("%w: failed to read nvme file", meta.ErrIO)).
		Once()

	resp, err := mgr.Read(ctx, &meta.ReadRequest{
		Ino:    ino,
		Fh:     fh,
		Offset: 0,
		Size:   100,
	})
	require.Nil(t, resp)
	require.Error(t, err)
	assert.True(t, errors.Is(err, meta.ErrIO), "必须包含 ErrIO 哨兵错误以映射到 EIO")
}

// TestRead_LocalFSBlockStoreIntegration 验证使用真实 LocalFSBlockStore 从本地磁盘物理文件读取数据。
func TestRead_LocalFSBlockStoreIntegration(t *testing.T) {
	ctx := context.Background()
	now := time.Now().Truncate(time.Microsecond)
	ino := uint64(700)
	fh := uint64(70)

	tmpDir := t.TempDir()
	bs := meta.NewLocalFSBlockStore(tmpDir)

	mockStore := dbmock.NewMockStore(t)
	mockQuerier := dbmock.NewMockQuerier(t)
	mgr := meta.New(mockStore, meta.WithBlockStore(bs))

	setupMockWriteTx(mockStore, mockQuerier)

	payload := []byte("HelloWorld-LocalFSBlockStore-ReadVerification")
	sliceID := uint64(888)

	// 先在物理介质写入真实 block
	err := bs.PutBlock(ctx, sliceID, 0, payload)
	require.NoError(t, err)

	fileRow := db.Inode{
		Ino:   int64(ino),
		Mode:  int32(meta.S_IFREG | 0o644),
		Size:  int64(len(payload)),
		Atime: pgtype.Timestamptz{Time: now, Valid: true},
		Mtime: pgtype.Timestamptz{Time: now, Valid: true},
		Ctime: pgtype.Timestamptz{Time: now, Valid: true},
	}

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

	sliceDesc := meta.Slice{
		Pos:  0,
		ID:   sliceID,
		Size: uint32(len(payload)),
		Off:  0,
		Len:  uint32(len(payload)),
	}

	mockQuerier.EXPECT().
		GetChunk(mock.Anything, db.GetChunkParams{Ino: int64(ino), Indx: 0}).
		Return(db.Chunk{
			Ino:    int64(ino),
			Indx:   0,
			Slices: meta.EncodeSlice(sliceDesc),
		}, nil).
		Once()

	resp, err := mgr.Read(ctx, &meta.ReadRequest{
		Ino:    ino,
		Fh:     fh,
		Offset: 0,
		Size:   uint32(len(payload)),
	})
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.Equal(t, payload, resp.Data)
}
