package meta_test

import (
	"context"
	"errors"
	"math"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"

	"github.com/starsrc/Aether/pkg/tx"
	"github.com/starsrc/bowl/services/storage/internal/db"
	dbmock "github.com/starsrc/bowl/services/storage/internal/db/mock"
	"github.com/starsrc/bowl/services/storage/internal/meta"
)

// setupFallocateTest 初始化测试所需的 Mock 上下文。
func setupFallocateTest(t *testing.T) (context.Context, *dbmock.MockStore, *dbmock.MockQuerier, meta.Manager) {
	t.Helper()
	ctx := context.Background()
	mockStore := dbmock.NewMockStore(t)
	mockQuerier := dbmock.NewMockQuerier(t)
	mgr := meta.New(mockStore)
	return ctx, mockStore, mockQuerier, mgr
}

// mockExecTx 辅助配置默认事务穿透行为。
func mockExecTx(mockStore *dbmock.MockStore, mockQuerier *dbmock.MockQuerier) {
	mockStore.EXPECT().
		ExecTxWithOptions(mock.Anything, mock.Anything).
		RunAndReturn(func(ctx context.Context, fn func(db.Querier) error, opts ...tx.Option) error {
			return fn(mockQuerier)
		}).
		Once()
}

// TestFallocate_InputValidationAndOverflow 验证入参边界与整型溢出防御。
func TestFallocate_InputValidationAndOverflow(t *testing.T) {
	for _, tc := range []struct {
		name   string
		params meta.FallocateParams
	}{
		{
			name:   "ino 为 0",
			params: meta.FallocateParams{Ino: 0, Fh: 1, Offset: 0, Length: 10, Mode: 0},
		},
		{
			name:   "ino 超过 math.MaxInt64",
			params: meta.FallocateParams{Ino: uint64(math.MaxInt64) + 1, Fh: 1, Offset: 0, Length: 10, Mode: 0},
		},
		{
			name:   "fh 为 0",
			params: meta.FallocateParams{Ino: 1, Fh: 0, Offset: 0, Length: 10, Mode: 0},
		},
		{
			name:   "fh 超过 math.MaxInt64",
			params: meta.FallocateParams{Ino: 1, Fh: uint64(math.MaxInt64) + 1, Offset: 0, Length: 10, Mode: 0},
		},
		{
			name:   "length 为 0",
			params: meta.FallocateParams{Ino: 1, Fh: 1, Offset: 0, Length: 0, Mode: 0},
		},
		{
			name:   "length 超过 math.MaxInt64",
			params: meta.FallocateParams{Ino: 1, Fh: 1, Offset: 0, Length: uint64(math.MaxInt64) + 1, Mode: 0},
		},
		{
			name:   "offset 超过 math.MaxInt64",
			params: meta.FallocateParams{Ino: 1, Fh: 1, Offset: uint64(math.MaxInt64) + 1, Length: 10, Mode: 0},
		},
		{
			name:   "offset + length 溢出 (offset=MaxInt64, length=1)",
			params: meta.FallocateParams{Ino: 1, Fh: 1, Offset: math.MaxInt64, Length: 1, Mode: 0},
		},
		{
			name:   "offset + length 溢出 (相加超过 MaxInt64)",
			params: meta.FallocateParams{Ino: 1, Fh: 1, Offset: math.MaxInt64 - 5, Length: 10, Mode: 0},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, mockStore, _, mgr := setupFallocateTest(t)
			err := mgr.Fallocate(ctx, tc.params)
			require.ErrorIs(t, err, meta.ErrInvalidArgument)
			mockStore.AssertNotCalled(t, "ExecTxWithOptions")
		})
	}
}

// TestFallocate_ModeValidation 验证模式标志位校验（打洞未带保留大小、不支持的标志位）。
func TestFallocate_ModeValidation(t *testing.T) {
	t.Run("仅提供 FallocFlPunchHole 未携带 FallocFlKeepSize 返回 ErrInvalidArgument", func(t *testing.T) {
		ctx, mockStore, _, mgr := setupFallocateTest(t)
		err := mgr.Fallocate(ctx, meta.FallocateParams{
			Ino:    1,
			Fh:     1,
			Offset: 0,
			Length: 100,
			Mode:   meta.FallocFlPunchHole, // 仅 0x02
		})
		require.ErrorIs(t, err, meta.ErrInvalidArgument)
		mockStore.AssertNotCalled(t, "ExecTxWithOptions")
	})

	for _, invalidMode := range []uint32{
		0x04,
		0x08,
		0x10,
		0xFF,
		meta.FallocFlPunchHole | 0x04,
		meta.FallocFlKeepSize | 0x04,
		meta.FallocFlPunchHole | meta.FallocFlKeepSize | 0x04,
	} {
		t.Run("不支持的模式位返回 ErrNotSupported", func(t *testing.T) {
			ctx, mockStore, _, mgr := setupFallocateTest(t)
			err := mgr.Fallocate(ctx, meta.FallocateParams{
				Ino:    1,
				Fh:     1,
				Offset: 0,
				Length: 100,
				Mode:   invalidMode,
			})
			require.ErrorIs(t, err, meta.ErrNotSupported)
			mockStore.AssertNotCalled(t, "ExecTxWithOptions")
		})
	}
}

// TestFallocate_PreconditionsAndHandles 验证 Inode 存在性、类型限制与句柄合法性。
func TestFallocate_PreconditionsAndHandles(t *testing.T) {
	validParams := meta.FallocateParams{
		Ino:    10,
		Fh:     100,
		Offset: 0,
		Length: 1024,
		Mode:   0,
	}

	t.Run("Inode 不存在返回 ErrNotFound", func(t *testing.T) {
		ctx, mockStore, mockQuerier, mgr := setupFallocateTest(t)
		mockExecTx(mockStore, mockQuerier)

		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(10)).
			Return(db.Inode{}, pgx.ErrNoRows).
			Once()

		err := mgr.Fallocate(ctx, validParams)
		require.ErrorIs(t, err, meta.ErrNotFound)
	})

	t.Run("GetInodeForUpdate 查询失败向上传播包装错误", func(t *testing.T) {
		ctx, mockStore, mockQuerier, mgr := setupFallocateTest(t)
		mockExecTx(mockStore, mockQuerier)

		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(10)).
			Return(db.Inode{}, errors.New("db connection failure")).
			Once()

		err := mgr.Fallocate(ctx, validParams)
		require.Error(t, err)
		require.Contains(t, err.Error(), "get inode for update 10")
	})

	t.Run("目标为目录返回 ErrIsDir", func(t *testing.T) {
		ctx, mockStore, mockQuerier, mgr := setupFallocateTest(t)
		mockExecTx(mockStore, mockQuerier)

		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(10)).
			Return(db.Inode{
				Ino:  10,
				Mode: int32(meta.S_IFDIR | 0o755),
			}, nil).
			Once()

		err := mgr.Fallocate(ctx, validParams)
		require.ErrorIs(t, err, meta.ErrIsDir)
	})

	for _, tc := range []struct {
		name string
		mode uint32
	}{
		{name: "符号链接", mode: meta.S_IFLNK | 0o777},
		{name: "FIFO 命名管道", mode: meta.S_IFIFO | 0o644},
		{name: "字符设备", mode: meta.S_IFCHR | 0o660},
		{name: "块设备", mode: meta.S_IFBLK | 0o660},
		{name: "套接字", mode: meta.S_IFSOCK | 0o666},
	} {
		t.Run("非普通文件类型返回 ErrNotSupported: "+tc.name, func(t *testing.T) {
			ctx, mockStore, mockQuerier, mgr := setupFallocateTest(t)
			mockExecTx(mockStore, mockQuerier)

			mockQuerier.EXPECT().
				GetInodeForUpdate(mock.Anything, int64(10)).
				Return(db.Inode{
					Ino:  10,
					Mode: int32(tc.mode),
				}, nil).
				Once()

			err := mgr.Fallocate(ctx, validParams)
			require.ErrorIs(t, err, meta.ErrNotSupported)
		})
	}

	t.Run("文件句柄不存在返回 ErrBadFileHandle", func(t *testing.T) {
		ctx, mockStore, mockQuerier, mgr := setupFallocateTest(t)
		mockExecTx(mockStore, mockQuerier)

		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(10)).
			Return(db.Inode{
				Ino:  10,
				Mode: int32(meta.S_IFREG | 0o644),
			}, nil).
			Once()

		mockQuerier.EXPECT().
			GetFileHandle(mock.Anything, db.GetFileHandleParams{
				Fh:  100,
				Ino: 10,
			}).
			Return(db.FileHandle{}, pgx.ErrNoRows).
			Once()

		err := mgr.Fallocate(ctx, validParams)
		require.ErrorIs(t, err, meta.ErrBadFileHandle)
	})

	t.Run("GetFileHandle 查询失败向上传播包装错误", func(t *testing.T) {
		ctx, mockStore, mockQuerier, mgr := setupFallocateTest(t)
		mockExecTx(mockStore, mockQuerier)

		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(10)).
			Return(db.Inode{
				Ino:  10,
				Mode: int32(meta.S_IFREG | 0o644),
			}, nil).
			Once()

		mockQuerier.EXPECT().
			GetFileHandle(mock.Anything, db.GetFileHandleParams{
				Fh:  100,
				Ino: 10,
			}).
			Return(db.FileHandle{}, errors.New("db query timeout")).
			Once()

		err := mgr.Fallocate(ctx, validParams)
		require.Error(t, err)
		require.Contains(t, err.Error(), "get file handle (fh=100, ino=10)")
	})

	t.Run("文件句柄为只读打开返回 ErrBadFileHandle", func(t *testing.T) {
		ctx, mockStore, mockQuerier, mgr := setupFallocateTest(t)
		mockExecTx(mockStore, mockQuerier)

		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(10)).
			Return(db.Inode{
				Ino:  10,
				Mode: int32(meta.S_IFREG | 0o644),
			}, nil).
			Once()

		mockQuerier.EXPECT().
			GetFileHandle(mock.Anything, db.GetFileHandleParams{
				Fh:  100,
				Ino: 10,
			}).
			Return(db.FileHandle{
				Fh:    100,
				Ino:   10,
				Flags: int32(meta.O_RDONLY), // 只读
			}, nil).
			Once()

		err := mgr.Fallocate(ctx, validParams)
		require.ErrorIs(t, err, meta.ErrBadFileHandle)
	})

	t.Run("句柄以保留模式 (flags & O_ACCMODE == 3) 打开返回 ErrBadFileHandle", func(t *testing.T) {
		ctx, mockStore, mockQuerier, mgr := setupFallocateTest(t)
		mockExecTx(mockStore, mockQuerier)

		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(10)).
			Return(db.Inode{
				Ino:  10,
				Mode: int32(meta.S_IFREG | 0o644),
				Size: 100,
			}, nil).
			Once()

		mockQuerier.EXPECT().
			GetFileHandle(mock.Anything, db.GetFileHandleParams{
				Fh:  100,
				Ino: 10,
			}).
			Return(db.FileHandle{
				Fh:    100,
				Ino:   10,
				Flags: 3, // 保留/非法访问模式
			}, nil).
			Once()

		err := mgr.Fallocate(ctx, validParams)
		require.ErrorIs(t, err, meta.ErrBadFileHandle)
	})
}

// TestFallocate_PositivePaths 验证合法模式的行为（模式 0、保留大小模式、打洞模式）。
func TestFallocate_PositivePaths(t *testing.T) {
	now := time.Now()
	pgNow := pgtype.Timestamptz{Time: now, Valid: true}

	t.Run("模式 0 (默认预分配): 目标大小超过当前大小，按需扩展并更新 mtime/ctime，保留现有 Mode 特权位", func(t *testing.T) {
		ctx, mockStore, mockQuerier, mgr := setupFallocateTest(t)
		mockExecTx(mockStore, mockQuerier)

		// 包含 S_ISUID 特权位
		initialMode := int32(meta.S_IFREG | meta.S_ISUID | 0o755)

		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(10)).
			Return(db.Inode{
				Ino:   10,
				Mode:  initialMode,
				Size:  100,
				Uid:   1001,
				Gid:   1002,
				Atime: pgNow,
			}, nil).
			Once()

		mockQuerier.EXPECT().
			GetFileHandle(mock.Anything, db.GetFileHandleParams{
				Fh:  100,
				Ino: 10,
			}).
			Return(db.FileHandle{
				Fh:    100,
				Ino:   10,
				Flags: int32(meta.O_RDWR),
			}, nil).
			Once()

		// offset=50, length=100 -> targetSize=150 > currentSize(100)
		mockQuerier.EXPECT().
			UpdateInodeAttr(mock.Anything, db.UpdateInodeAttrParams{
				Ino:         10,
				Mode:        initialMode, // 必须保留特权位
				Size:        150,
				Uid:         1001,
				Gid:         1002,
				SetAtimeNow: false,
				Atime:       pgNow,
				SetMtimeNow: true,
				Mtime:       pgtype.Timestamptz{},
			}).
			Return(db.Inode{
				Ino:  10,
				Mode: initialMode,
				Size: 150,
			}, nil).
			Once()

		err := mgr.Fallocate(ctx, meta.FallocateParams{
			Ino:    10,
			Fh:     100,
			Offset: 50,
			Length: 100,
			Mode:   0,
		})
		require.NoError(t, err)
		mockQuerier.AssertNotCalled(t, "TouchInodeCtime")
	})

	t.Run("模式 0 (默认预分配): 目标大小小于当前大小，仅刷新 ctime", func(t *testing.T) {
		ctx, mockStore, mockQuerier, mgr := setupFallocateTest(t)
		mockExecTx(mockStore, mockQuerier)

		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(10)).
			Return(db.Inode{
				Ino:   10,
				Mode:  int32(meta.S_IFREG | 0o644),
				Size:  500,
				Uid:   1001,
				Gid:   1002,
				Atime: pgNow,
			}, nil).
			Once()

		mockQuerier.EXPECT().
			GetFileHandle(mock.Anything, db.GetFileHandleParams{
				Fh:  100,
				Ino: 10,
			}).
			Return(db.FileHandle{
				Fh:    100,
				Ino:   10,
				Flags: int32(meta.O_WRONLY),
			}, nil).
			Once()

		// offset=50, length=100 -> targetSize=150 <= currentSize(500)
		mockQuerier.EXPECT().
			TouchInodeCtime(mock.Anything, int64(10)).
			Return(nil).
			Once()

		err := mgr.Fallocate(ctx, meta.FallocateParams{
			Ino:    10,
			Fh:     100,
			Offset: 50,
			Length: 100,
			Mode:   0,
		})
		require.NoError(t, err)
		mockQuerier.AssertNotCalled(t, "UpdateInodeAttr")
	})

	t.Run("模式 0 (默认预分配): 目标大小等于当前大小，仅刷新 ctime", func(t *testing.T) {
		ctx, mockStore, mockQuerier, mgr := setupFallocateTest(t)
		mockExecTx(mockStore, mockQuerier)

		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(10)).
			Return(db.Inode{
				Ino:   10,
				Mode:  int32(meta.S_IFREG | 0o644),
				Size:  200,
				Uid:   1001,
				Gid:   1002,
				Atime: pgNow,
			}, nil).
			Once()

		mockQuerier.EXPECT().
			GetFileHandle(mock.Anything, db.GetFileHandleParams{
				Fh:  100,
				Ino: 10,
			}).
			Return(db.FileHandle{
				Fh:    100,
				Ino:   10,
				Flags: int32(meta.O_RDWR),
			}, nil).
			Once()

		// offset=100, length=100 -> targetSize=200 == currentSize(200)
		mockQuerier.EXPECT().
			TouchInodeCtime(mock.Anything, int64(10)).
			Return(nil).
			Once()

		err := mgr.Fallocate(ctx, meta.FallocateParams{
			Ino:    10,
			Fh:     100,
			Offset: 100,
			Length: 100,
			Mode:   0,
		})
		require.NoError(t, err)
		mockQuerier.AssertNotCalled(t, "UpdateInodeAttr")
	})

	t.Run("模式 FallocFlKeepSize: 即使 targetSize 超过原大小，仍仅刷新 ctime 且不改变大小", func(t *testing.T) {
		ctx, mockStore, mockQuerier, mgr := setupFallocateTest(t)
		mockExecTx(mockStore, mockQuerier)

		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(10)).
			Return(db.Inode{
				Ino:   10,
				Mode:  int32(meta.S_IFREG | 0o644),
				Size:  100,
				Uid:   1001,
				Gid:   1002,
				Atime: pgNow,
			}, nil).
			Once()

		mockQuerier.EXPECT().
			GetFileHandle(mock.Anything, db.GetFileHandleParams{
				Fh:  100,
				Ino: 10,
			}).
			Return(db.FileHandle{
				Fh:    100,
				Ino:   10,
				Flags: int32(meta.O_WRONLY),
			}, nil).
			Once()

		mockQuerier.EXPECT().
			TouchInodeCtime(mock.Anything, int64(10)).
			Return(nil).
			Once()

		err := mgr.Fallocate(ctx, meta.FallocateParams{
			Ino:    10,
			Fh:     100,
			Offset: 200,
			Length: 300,
			Mode:   meta.FallocFlKeepSize,
		})
		require.NoError(t, err)
		mockQuerier.AssertNotCalled(t, "UpdateInodeAttr")
	})

	t.Run("模式 FallocFlPunchHole | FallocFlKeepSize: 剥离 S_ISUID 和 S_ISGID，刷新 mtime/ctime，保持 size", func(t *testing.T) {
		ctx, mockStore, mockQuerier, mgr := setupFallocateTest(t)
		mockExecTx(mockStore, mockQuerier)

		// 带有 S_ISUID 与 S_ISGID 的模式
		initialMode := int32(meta.S_IFREG | meta.S_ISUID | meta.S_ISGID | 0o755)
		expectedCleanMode := int32(meta.S_IFREG | 0o755)

		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(10)).
			Return(db.Inode{
				Ino:   10,
				Mode:  initialMode,
				Size:  4096,
				Uid:   1001,
				Gid:   1002,
				Atime: pgNow,
			}, nil).
			Once()

		mockQuerier.EXPECT().
			GetFileHandle(mock.Anything, db.GetFileHandleParams{
				Fh:  100,
				Ino: 10,
			}).
			Return(db.FileHandle{
				Fh:    100,
				Ino:   10,
				Flags: int32(meta.O_RDWR),
			}, nil).
			Once()

		mockQuerier.EXPECT().
			UpdateInodeAttr(mock.Anything, db.UpdateInodeAttrParams{
				Ino:         10,
				Mode:        expectedCleanMode, // 特权位被剥离
				Size:        4096,              // 保持大小不变
				Uid:         1001,
				Gid:         1002,
				SetAtimeNow: false,
				Atime:       pgNow,
				SetMtimeNow: true,
				Mtime:       pgtype.Timestamptz{},
			}).
			Return(db.Inode{
				Ino:  10,
				Mode: expectedCleanMode,
				Size: 4096,
			}, nil).
			Once()

		err := mgr.Fallocate(ctx, meta.FallocateParams{
			Ino:    10,
			Fh:     100,
			Offset: 1024,
			Length: 2048,
			Mode:   meta.FallocFlPunchHole | meta.FallocFlKeepSize,
		})
		require.NoError(t, err)
		mockQuerier.AssertNotCalled(t, "TouchInodeCtime")
	})

	t.Run("模式 FallocFlPunchHole | FallocFlKeepSize: 无特权位时同样刷新 mtime/ctime 并保持 size", func(t *testing.T) {
		ctx, mockStore, mockQuerier, mgr := setupFallocateTest(t)
		mockExecTx(mockStore, mockQuerier)

		initialMode := int32(meta.S_IFREG | 0o644)

		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(10)).
			Return(db.Inode{
				Ino:   10,
				Mode:  initialMode,
				Size:  4096,
				Uid:   1001,
				Gid:   1002,
				Atime: pgNow,
			}, nil).
			Once()

		mockQuerier.EXPECT().
			GetFileHandle(mock.Anything, db.GetFileHandleParams{
				Fh:  100,
				Ino: 10,
			}).
			Return(db.FileHandle{
				Fh:    100,
				Ino:   10,
				Flags: int32(meta.O_WRONLY),
			}, nil).
			Once()

		mockQuerier.EXPECT().
			UpdateInodeAttr(mock.Anything, db.UpdateInodeAttrParams{
				Ino:         10,
				Mode:        initialMode,
				Size:        4096,
				Uid:         1001,
				Gid:         1002,
				SetAtimeNow: false,
				Atime:       pgNow,
				SetMtimeNow: true,
				Mtime:       pgtype.Timestamptz{},
			}).
			Return(db.Inode{
				Ino:  10,
				Mode: initialMode,
				Size: 4096,
			}, nil).
			Once()

		err := mgr.Fallocate(ctx, meta.FallocateParams{
			Ino:    10,
			Fh:     100,
			Offset: 512,
			Length: 1024,
			Mode:   meta.FallocFlPunchHole | meta.FallocFlKeepSize,
		})
		require.NoError(t, err)
		mockQuerier.AssertNotCalled(t, "TouchInodeCtime")
	})
}

// TestFallocate_DatabaseErrors 验证底层数据库错误向上传播与包装。
func TestFallocate_DatabaseErrors(t *testing.T) {
	pgNow := pgtype.Timestamptz{Time: time.Now(), Valid: true}

	t.Run("UpdateInodeAttr 失败向上传播包装错误", func(t *testing.T) {
		ctx, mockStore, mockQuerier, mgr := setupFallocateTest(t)
		mockExecTx(mockStore, mockQuerier)

		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(10)).
			Return(db.Inode{
				Ino:   10,
				Mode:  int32(meta.S_IFREG | 0o644),
				Size:  100,
				Uid:   1001,
				Gid:   1002,
				Atime: pgNow,
			}, nil).
			Once()

		mockQuerier.EXPECT().
			GetFileHandle(mock.Anything, db.GetFileHandleParams{
				Fh:  100,
				Ino: 10,
			}).
			Return(db.FileHandle{
				Fh:    100,
				Ino:   10,
				Flags: int32(meta.O_RDWR),
			}, nil).
			Once()

		mockQuerier.EXPECT().
			UpdateInodeAttr(mock.Anything, mock.Anything).
			Return(db.Inode{}, errors.New("disk full")).
			Once()

		err := mgr.Fallocate(ctx, meta.FallocateParams{
			Ino:    10,
			Fh:     100,
			Offset: 50,
			Length: 100,
			Mode:   0,
		})
		require.Error(t, err)
		require.Contains(t, err.Error(), "update inode attr 10")
	})

	t.Run("TouchInodeCtime 失败向上传播包装错误", func(t *testing.T) {
		ctx, mockStore, mockQuerier, mgr := setupFallocateTest(t)
		mockExecTx(mockStore, mockQuerier)

		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(10)).
			Return(db.Inode{
				Ino:   10,
				Mode:  int32(meta.S_IFREG | 0o644),
				Size:  500,
				Uid:   1001,
				Gid:   1002,
				Atime: pgNow,
			}, nil).
			Once()

		mockQuerier.EXPECT().
			GetFileHandle(mock.Anything, db.GetFileHandleParams{
				Fh:  100,
				Ino: 10,
			}).
			Return(db.FileHandle{
				Fh:    100,
				Ino:   10,
				Flags: int32(meta.O_RDWR),
			}, nil).
			Once()

		mockQuerier.EXPECT().
			TouchInodeCtime(mock.Anything, int64(10)).
			Return(errors.New("connection reset")).
			Once()

		err := mgr.Fallocate(ctx, meta.FallocateParams{
			Ino:    10,
			Fh:     100,
			Offset: 50,
			Length: 100,
			Mode:   meta.FallocFlKeepSize,
		})
		require.Error(t, err)
		require.Contains(t, err.Error(), "touch inode ctime 10")
	})

	t.Run("事务执行器底层错误向上传播", func(t *testing.T) {
		ctx, mockStore, _, mgr := setupFallocateTest(t)

		mockStore.EXPECT().
			ExecTxWithOptions(mock.Anything, mock.Anything).
			Return(errors.New("tx commit failed")).
			Once()

		err := mgr.Fallocate(ctx, meta.FallocateParams{
			Ino:    10,
			Fh:     100,
			Offset: 0,
			Length: 100,
			Mode:   0,
		})
		require.Error(t, err)
		require.Contains(t, err.Error(), "tx commit failed")
	})
}

// TestFallocate_DiskQuota 验证 Fallocate 预分配扩展文件大小时联动单事务容量配额校验与 used_bytes 更新。
func TestFallocate_DiskQuota(t *testing.T) {
	now := time.Now()
	pgNow := pgtype.Timestamptz{Time: now, Valid: true}
	const (
		testIno     = int64(10)
		testRootIno = int64(100)
		testFh      = int64(1001)
	)

	t.Run("模式 0 扩展文件大小时按 deltaBytes 锁定 Disk 校验配额并调用 AddDiskQuotaByRootIno 递增 used_bytes", func(t *testing.T) {
		ctx, mockStore, mockQuerier, mgr := setupFallocateTest(t)
		mockExecTx(mockStore, mockQuerier)

		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, testIno).
			Return(db.Inode{
				Ino:     testIno,
				RootIno: testRootIno,
				Mode:    int32(meta.S_IFREG | 0o644),
				Size:    100,
				Uid:     1001,
				Gid:     1002,
				Atime:   pgNow,
			}, nil).
			Once()

		mockQuerier.EXPECT().
			GetFileHandle(mock.Anything, db.GetFileHandleParams{
				Fh:  testFh,
				Ino: testIno,
			}).
			Return(db.FileHandle{
				Fh:    testFh,
				Ino:   testIno,
				Flags: int32(meta.O_RDWR),
			}, nil).
			Once()

		// offset=50, length=100 -> targetSize=150, deltaBytes=50
		mockQuerier.EXPECT().
			GetDiskByRootInoForUpdate(mock.Anything, testRootIno).
			Return(db.Disk{
				RootIno:   testRootIno,
				MaxBytes:  1000,
				UsedBytes: 200,
			}, nil).
			Once()

		mockQuerier.EXPECT().
			AddDiskQuotaByRootIno(mock.Anything, db.AddDiskQuotaByRootInoParams{
				RootIno:     testRootIno,
				DeltaBytes:  50,
				DeltaInodes: 0,
			}).
			Return(nil).
			Once()

		mockQuerier.EXPECT().
			UpdateInodeAttr(mock.Anything, db.UpdateInodeAttrParams{
				Ino:         testIno,
				Mode:        int32(meta.S_IFREG | 0o644),
				Size:        150,
				Uid:         1001,
				Gid:         1002,
				SetAtimeNow: false,
				Atime:       pgNow,
				SetMtimeNow: true,
				Mtime:       pgtype.Timestamptz{},
			}).
			Return(db.Inode{
				Ino:     testIno,
				RootIno: testRootIno,
				Mode:    int32(meta.S_IFREG | 0o644),
				Size:    150,
			}, nil).
			Once()

		err := mgr.Fallocate(ctx, meta.FallocateParams{
			Ino:    uint64(testIno),
			Fh:     uint64(testFh),
			Offset: 50,
			Length: 100,
			Mode:   0,
		})
		require.NoError(t, err)
		mockQuerier.AssertNotCalled(t, "TouchInodeCtime")
	})

	t.Run("模式 0 扩展超出 max_bytes 时中止事务并返回 ErrNoSpace (ResourceExhausted / ENOSPC)，且不调用 UpdateInodeAttr", func(t *testing.T) {
		ctx, mockStore, mockQuerier, mgr := setupFallocateTest(t)
		mockExecTx(mockStore, mockQuerier)

		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, testIno).
			Return(db.Inode{
				Ino:     testIno,
				RootIno: testRootIno,
				Mode:    int32(meta.S_IFREG | 0o644),
				Size:    100,
				Uid:     1001,
				Gid:     1002,
				Atime:   pgNow,
			}, nil).
			Once()

		mockQuerier.EXPECT().
			GetFileHandle(mock.Anything, db.GetFileHandleParams{
				Fh:  testFh,
				Ino: testIno,
			}).
			Return(db.FileHandle{
				Fh:    testFh,
				Ino:   testIno,
				Flags: int32(meta.O_RDWR),
			}, nil).
			Once()

		// offset=50, length=100 -> targetSize=150, deltaBytes=50.
		// max_bytes=1000, used_bytes=980 -> 980 + 50 = 1030 > 1000 -> 超额
		mockQuerier.EXPECT().
			GetDiskByRootInoForUpdate(mock.Anything, testRootIno).
			Return(db.Disk{
				RootIno:   testRootIno,
				MaxBytes:  1000,
				UsedBytes: 980,
			}, nil).
			Once()

		err := mgr.Fallocate(ctx, meta.FallocateParams{
			Ino:    uint64(testIno),
			Fh:     uint64(testFh),
			Offset: 50,
			Length: 100,
			Mode:   0,
		})
		require.ErrorIs(t, err, meta.ErrNoSpace)

		// 验证 meta.ToStatus 映射为 ResourceExhausted 与 ENOSPC
		st := meta.ToStatus(err)
		require.NotNil(t, st)
		assert.Equal(t, codes.ResourceExhausted, st.Code())
		require.NotEmpty(t, st.Details())
		info, ok := st.Details()[0].(*errdetails.ErrorInfo)
		require.True(t, ok)
		assert.Equal(t, "ENOSPC", info.Reason)
		assert.Equal(t, meta.ErrorDomain, info.Domain)

		mockQuerier.AssertNotCalled(t, "AddDiskQuotaByRootIno", mock.Anything, mock.Anything)
		mockQuerier.AssertNotCalled(t, "UpdateInodeAttr", mock.Anything, mock.Anything)
	})

	t.Run("模式 0 当前 used_bytes 已大于 max_bytes 时中止事务并返回 ErrNoSpace", func(t *testing.T) {
		ctx, mockStore, mockQuerier, mgr := setupFallocateTest(t)
		mockExecTx(mockStore, mockQuerier)

		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, testIno).
			Return(db.Inode{
				Ino:     testIno,
				RootIno: testRootIno,
				Mode:    int32(meta.S_IFREG | 0o644),
				Size:    100,
				Uid:     1001,
				Gid:     1002,
				Atime:   pgNow,
			}, nil).
			Once()

		mockQuerier.EXPECT().
			GetFileHandle(mock.Anything, db.GetFileHandleParams{
				Fh:  testFh,
				Ino: testIno,
			}).
			Return(db.FileHandle{
				Fh:    testFh,
				Ino:   testIno,
				Flags: int32(meta.O_RDWR),
			}, nil).
			Once()

		// used_bytes 超过 max_bytes
		mockQuerier.EXPECT().
			GetDiskByRootInoForUpdate(mock.Anything, testRootIno).
			Return(db.Disk{
				RootIno:   testRootIno,
				MaxBytes:  1000,
				UsedBytes: 1050,
			}, nil).
			Once()

		err := mgr.Fallocate(ctx, meta.FallocateParams{
			Ino:    uint64(testIno),
			Fh:     uint64(testFh),
			Offset: 50,
			Length: 100,
			Mode:   0,
		})
		require.ErrorIs(t, err, meta.ErrNoSpace)

		mockQuerier.AssertNotCalled(t, "AddDiskQuotaByRootIno", mock.Anything, mock.Anything)
		mockQuerier.AssertNotCalled(t, "UpdateInodeAttr", mock.Anything, mock.Anything)
	})

	t.Run("模式 0 目标大小小于或等于当前大小时不触发配额扣减", func(t *testing.T) {
		ctx, mockStore, mockQuerier, mgr := setupFallocateTest(t)
		mockExecTx(mockStore, mockQuerier)

		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, testIno).
			Return(db.Inode{
				Ino:     testIno,
				RootIno: testRootIno,
				Mode:    int32(meta.S_IFREG | 0o644),
				Size:    500,
				Uid:     1001,
				Gid:     1002,
				Atime:   pgNow,
			}, nil).
			Once()

		mockQuerier.EXPECT().
			GetFileHandle(mock.Anything, db.GetFileHandleParams{
				Fh:  testFh,
				Ino: testIno,
			}).
			Return(db.FileHandle{
				Fh:    testFh,
				Ino:   testIno,
				Flags: int32(meta.O_WRONLY),
			}, nil).
			Once()

		mockQuerier.EXPECT().
			TouchInodeCtime(mock.Anything, testIno).
			Return(nil).
			Once()

		// offset=50, length=100 -> targetSize=150 <= 500
		err := mgr.Fallocate(ctx, meta.FallocateParams{
			Ino:    uint64(testIno),
			Fh:     uint64(testFh),
			Offset: 50,
			Length: 100,
			Mode:   0,
		})
		require.NoError(t, err)

		mockQuerier.AssertNotCalled(t, "GetDiskByRootInoForUpdate", mock.Anything, mock.Anything)
		mockQuerier.AssertNotCalled(t, "AddDiskQuotaByRootIno", mock.Anything, mock.Anything)
		mockQuerier.AssertNotCalled(t, "UpdateInodeAttr", mock.Anything, mock.Anything)
	})

	t.Run("模式 FallocFlKeepSize 即使 targetSize 超过原大小也不触发配额扣减", func(t *testing.T) {
		ctx, mockStore, mockQuerier, mgr := setupFallocateTest(t)
		mockExecTx(mockStore, mockQuerier)

		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, testIno).
			Return(db.Inode{
				Ino:     testIno,
				RootIno: testRootIno,
				Mode:    int32(meta.S_IFREG | 0o644),
				Size:    100,
				Uid:     1001,
				Gid:     1002,
				Atime:   pgNow,
			}, nil).
			Once()

		mockQuerier.EXPECT().
			GetFileHandle(mock.Anything, db.GetFileHandleParams{
				Fh:  testFh,
				Ino: testIno,
			}).
			Return(db.FileHandle{
				Fh:    testFh,
				Ino:   testIno,
				Flags: int32(meta.O_WRONLY),
			}, nil).
			Once()

		mockQuerier.EXPECT().
			TouchInodeCtime(mock.Anything, testIno).
			Return(nil).
			Once()

		// targetSize=300 > 100
		err := mgr.Fallocate(ctx, meta.FallocateParams{
			Ino:    uint64(testIno),
			Fh:     uint64(testFh),
			Offset: 100,
			Length: 200,
			Mode:   meta.FallocFlKeepSize,
		})
		require.NoError(t, err)

		mockQuerier.AssertNotCalled(t, "GetDiskByRootInoForUpdate", mock.Anything, mock.Anything)
		mockQuerier.AssertNotCalled(t, "AddDiskQuotaByRootIno", mock.Anything, mock.Anything)
		mockQuerier.AssertNotCalled(t, "UpdateInodeAttr", mock.Anything, mock.Anything)
	})

	t.Run("模式 FallocFlPunchHole | FallocFlKeepSize 即使 offset+length 超过原大小也不触发配额扣减", func(t *testing.T) {
		ctx, mockStore, mockQuerier, mgr := setupFallocateTest(t)
		mockExecTx(mockStore, mockQuerier)

		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, testIno).
			Return(db.Inode{
				Ino:     testIno,
				RootIno: testRootIno,
				Mode:    int32(meta.S_IFREG | 0o644),
				Size:    100,
				Uid:     1001,
				Gid:     1002,
				Atime:   pgNow,
			}, nil).
			Once()

		mockQuerier.EXPECT().
			GetFileHandle(mock.Anything, db.GetFileHandleParams{
				Fh:  testFh,
				Ino: testIno,
			}).
			Return(db.FileHandle{
				Fh:    testFh,
				Ino:   testIno,
				Flags: int32(meta.O_RDWR),
			}, nil).
			Once()

		mockQuerier.EXPECT().
			UpdateInodeAttr(mock.Anything, db.UpdateInodeAttrParams{
				Ino:         testIno,
				Mode:        int32(meta.S_IFREG | 0o644),
				Size:        100,
				Uid:         1001,
				Gid:         1002,
				SetAtimeNow: false,
				Atime:       pgNow,
				SetMtimeNow: true,
				Mtime:       pgtype.Timestamptz{},
			}).
			Return(db.Inode{
				Ino:     testIno,
				RootIno: testRootIno,
				Mode:    int32(meta.S_IFREG | 0o644),
				Size:    100,
			}, nil).
			Once()

		err := mgr.Fallocate(ctx, meta.FallocateParams{
			Ino:    uint64(testIno),
			Fh:     uint64(testFh),
			Offset: 50,
			Length: 200,
			Mode:   meta.FallocFlPunchHole | meta.FallocFlKeepSize,
		})
		require.NoError(t, err)

		mockQuerier.AssertNotCalled(t, "GetDiskByRootInoForUpdate", mock.Anything, mock.Anything)
		mockQuerier.AssertNotCalled(t, "AddDiskQuotaByRootIno", mock.Anything, mock.Anything)
	})

	t.Run("current.RootIno == 0 扩展大小不触发配额检查与递增", func(t *testing.T) {
		ctx, mockStore, mockQuerier, mgr := setupFallocateTest(t)
		mockExecTx(mockStore, mockQuerier)

		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, testIno).
			Return(db.Inode{
				Ino:     testIno,
				RootIno: 0, // 未关联 Disk 根节点
				Mode:    int32(meta.S_IFREG | 0o644),
				Size:    100,
				Uid:     1001,
				Gid:     1002,
				Atime:   pgNow,
			}, nil).
			Once()

		mockQuerier.EXPECT().
			GetFileHandle(mock.Anything, db.GetFileHandleParams{
				Fh:  testFh,
				Ino: testIno,
			}).
			Return(db.FileHandle{
				Fh:    testFh,
				Ino:   testIno,
				Flags: int32(meta.O_RDWR),
			}, nil).
			Once()

		mockQuerier.EXPECT().
			UpdateInodeAttr(mock.Anything, db.UpdateInodeAttrParams{
				Ino:         testIno,
				Mode:        int32(meta.S_IFREG | 0o644),
				Size:        150,
				Uid:         1001,
				Gid:         1002,
				SetAtimeNow: false,
				Atime:       pgNow,
				SetMtimeNow: true,
				Mtime:       pgtype.Timestamptz{},
			}).
			Return(db.Inode{
				Ino:  testIno,
				Mode: int32(meta.S_IFREG | 0o644),
				Size: 150,
			}, nil).
			Once()

		err := mgr.Fallocate(ctx, meta.FallocateParams{
			Ino:    uint64(testIno),
			Fh:     uint64(testFh),
			Offset: 50,
			Length: 100,
			Mode:   0,
		})
		require.NoError(t, err)

		mockQuerier.AssertNotCalled(t, "GetDiskByRootInoForUpdate", mock.Anything, mock.Anything)
		mockQuerier.AssertNotCalled(t, "AddDiskQuotaByRootIno", mock.Anything, mock.Anything)
	})

	t.Run("GetDiskByRootInoForUpdate 返回 pgx.ErrNoRows 时兼容跳过配额检查并成功更新 Inode", func(t *testing.T) {
		ctx, mockStore, mockQuerier, mgr := setupFallocateTest(t)
		mockExecTx(mockStore, mockQuerier)

		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, testIno).
			Return(db.Inode{
				Ino:     testIno,
				RootIno: testRootIno,
				Mode:    int32(meta.S_IFREG | 0o644),
				Size:    100,
				Uid:     1001,
				Gid:     1002,
				Atime:   pgNow,
			}, nil).
			Once()

		mockQuerier.EXPECT().
			GetFileHandle(mock.Anything, db.GetFileHandleParams{
				Fh:  testFh,
				Ino: testIno,
			}).
			Return(db.FileHandle{
				Fh:    testFh,
				Ino:   testIno,
				Flags: int32(meta.O_RDWR),
			}, nil).
			Once()

		mockQuerier.EXPECT().
			GetDiskByRootInoForUpdate(mock.Anything, testRootIno).
			Return(db.Disk{}, pgx.ErrNoRows).
			Once()

		mockQuerier.EXPECT().
			UpdateInodeAttr(mock.Anything, db.UpdateInodeAttrParams{
				Ino:         testIno,
				Mode:        int32(meta.S_IFREG | 0o644),
				Size:        150,
				Uid:         1001,
				Gid:         1002,
				SetAtimeNow: false,
				Atime:       pgNow,
				SetMtimeNow: true,
				Mtime:       pgtype.Timestamptz{},
			}).
			Return(db.Inode{
				Ino:     testIno,
				RootIno: testRootIno,
				Mode:    int32(meta.S_IFREG | 0o644),
				Size:    150,
			}, nil).
			Once()

		err := mgr.Fallocate(ctx, meta.FallocateParams{
			Ino:    uint64(testIno),
			Fh:     uint64(testFh),
			Offset: 50,
			Length: 100,
			Mode:   0,
		})
		require.NoError(t, err)

		mockQuerier.AssertNotCalled(t, "AddDiskQuotaByRootIno", mock.Anything, mock.Anything)
	})

	t.Run("GetDiskByRootInoForUpdate 数据库错误向上传播并中止事务", func(t *testing.T) {
		ctx, mockStore, mockQuerier, mgr := setupFallocateTest(t)
		mockExecTx(mockStore, mockQuerier)

		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, testIno).
			Return(db.Inode{
				Ino:     testIno,
				RootIno: testRootIno,
				Mode:    int32(meta.S_IFREG | 0o644),
				Size:    100,
				Uid:     1001,
				Gid:     1002,
				Atime:   pgNow,
			}, nil).
			Once()

		mockQuerier.EXPECT().
			GetFileHandle(mock.Anything, db.GetFileHandleParams{
				Fh:  testFh,
				Ino: testIno,
			}).
			Return(db.FileHandle{
				Fh:    testFh,
				Ino:   testIno,
				Flags: int32(meta.O_RDWR),
			}, nil).
			Once()

		mockQuerier.EXPECT().
			GetDiskByRootInoForUpdate(mock.Anything, testRootIno).
			Return(db.Disk{}, errors.New("db query error")).
			Once()

		err := mgr.Fallocate(ctx, meta.FallocateParams{
			Ino:    uint64(testIno),
			Fh:     uint64(testFh),
			Offset: 50,
			Length: 100,
			Mode:   0,
		})
		require.Error(t, err)
		require.Contains(t, err.Error(), "get disk by root_ino 100 for update")

		mockQuerier.AssertNotCalled(t, "AddDiskQuotaByRootIno", mock.Anything, mock.Anything)
		mockQuerier.AssertNotCalled(t, "UpdateInodeAttr", mock.Anything, mock.Anything)
	})

	t.Run("AddDiskQuotaByRootIno 数据库错误向上传播并中止事务", func(t *testing.T) {
		ctx, mockStore, mockQuerier, mgr := setupFallocateTest(t)
		mockExecTx(mockStore, mockQuerier)

		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, testIno).
			Return(db.Inode{
				Ino:     testIno,
				RootIno: testRootIno,
				Mode:    int32(meta.S_IFREG | 0o644),
				Size:    100,
				Uid:     1001,
				Gid:     1002,
				Atime:   pgNow,
			}, nil).
			Once()

		mockQuerier.EXPECT().
			GetFileHandle(mock.Anything, db.GetFileHandleParams{
				Fh:  testFh,
				Ino: testIno,
			}).
			Return(db.FileHandle{
				Fh:    testFh,
				Ino:   testIno,
				Flags: int32(meta.O_RDWR),
			}, nil).
			Once()

		mockQuerier.EXPECT().
			GetDiskByRootInoForUpdate(mock.Anything, testRootIno).
			Return(db.Disk{
				RootIno:   testRootIno,
				MaxBytes:  1000,
				UsedBytes: 200,
			}, nil).
			Once()

		mockQuerier.EXPECT().
			AddDiskQuotaByRootIno(mock.Anything, db.AddDiskQuotaByRootInoParams{
				RootIno:     testRootIno,
				DeltaBytes:  50,
				DeltaInodes: 0,
			}).
			Return(errors.New("db update quota failure")).
			Once()

		err := mgr.Fallocate(ctx, meta.FallocateParams{
			Ino:    uint64(testIno),
			Fh:     uint64(testFh),
			Offset: 50,
			Length: 100,
			Mode:   0,
		})
		require.Error(t, err)
		require.Contains(t, err.Error(), "add disk quota by root_ino 100")

		mockQuerier.AssertNotCalled(t, "UpdateInodeAttr", mock.Anything, mock.Anything)
	})
}
