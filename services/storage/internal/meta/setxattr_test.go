package meta_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/starsrc/Aether/pkg/tx"
	"github.com/starsrc/bowl/services/storage/internal/db"
	dbmock "github.com/starsrc/bowl/services/storage/internal/db/mock"
	"github.com/starsrc/bowl/services/storage/internal/meta"
)

// setupSetXattrTest 初始化测试所需的 Mock 上下文。
func setupSetXattrTest(t *testing.T) (context.Context, *dbmock.MockStore, *dbmock.MockQuerier, meta.Manager) {
	t.Helper()
	ctx := context.Background()
	mockStore := dbmock.NewMockStore(t)
	mockQuerier := dbmock.NewMockQuerier(t)
	mgr := meta.New(mockStore)
	return ctx, mockStore, mockQuerier, mgr
}

// mockSetXattrExecTx 辅助配置默认事务穿透行为。
func mockSetXattrExecTx(mockStore *dbmock.MockStore, mockQuerier *dbmock.MockQuerier) {
	mockStore.EXPECT().
		ExecTxWithOptions(mock.Anything, mock.Anything).
		RunAndReturn(func(ctx context.Context, fn func(db.Querier) error, opts ...tx.Option) error {
			return fn(mockQuerier)
		}).
		Once()
}

// TestSetXattr_StaticValidation 验证第 1 步入参静态校验（不查库，不开启事务）。
func TestSetXattr_StaticValidation(t *testing.T) {
	ctx, mockStore, _, mgr := setupSetXattrTest(t)

	for _, tc := range []struct {
		name        string
		params      meta.SetXattrParams
		expectedErr error
	}{
		{
			name:        "ino 为 0",
			params:      meta.SetXattrParams{Ino: 0, Name: []byte("user.test"), Value: []byte("val"), Flags: 0},
			expectedErr: meta.ErrInvalidArgument,
		},
		{
			name:        "ino 超过 math.MaxInt64",
			params:      meta.SetXattrParams{Ino: uint64(math.MaxInt64) + 1, Name: []byte("user.test"), Value: []byte("val"), Flags: 0},
			expectedErr: meta.ErrInvalidArgument,
		},
		{
			name:        "flags 含有非法标志位 0x4",
			params:      meta.SetXattrParams{Ino: 1, Name: []byte("user.test"), Value: []byte("val"), Flags: 0x4},
			expectedErr: meta.ErrInvalidArgument,
		},
		{
			name:        "flags 为 uint32 最大值",
			params:      meta.SetXattrParams{Ino: 1, Name: []byte("user.test"), Value: []byte("val"), Flags: math.MaxUint32},
			expectedErr: meta.ErrInvalidArgument,
		},
		{
			name:        "name 为空切片 (0 字节)",
			params:      meta.SetXattrParams{Ino: 1, Name: []byte{}, Value: []byte("val"), Flags: 0},
			expectedErr: meta.ErrRange,
		},
		{
			name:        "name 为 nil (0 字节)",
			params:      meta.SetXattrParams{Ino: 1, Name: nil, Value: []byte("val"), Flags: 0},
			expectedErr: meta.ErrRange,
		},
		{
			name:        "name 超过 255 字节",
			params:      meta.SetXattrParams{Ino: 1, Name: bytes.Repeat([]byte("n"), 256), Value: []byte("val"), Flags: 0},
			expectedErr: meta.ErrRange,
		},
		{
			name:        "name 开头包含空字节 NUL",
			params:      meta.SetXattrParams{Ino: 1, Name: []byte("\x00user.test"), Value: []byte("val"), Flags: 0},
			expectedErr: meta.ErrInvalidArgument,
		},
		{
			name:        "name 中间包含空字节 NUL",
			params:      meta.SetXattrParams{Ino: 1, Name: []byte("user\x00test"), Value: []byte("val"), Flags: 0},
			expectedErr: meta.ErrInvalidArgument,
		},
		{
			name:        "name 结尾包含空字节 NUL",
			params:      meta.SetXattrParams{Ino: 1, Name: []byte("user.test\x00"), Value: []byte("val"), Flags: 0},
			expectedErr: meta.ErrInvalidArgument,
		},
		{
			name:        "value 超过 65536 字节",
			params:      meta.SetXattrParams{Ino: 1, Name: []byte("user.test"), Value: bytes.Repeat([]byte("v"), 65537), Flags: 0},
			expectedErr: meta.ErrTooBig,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := mgr.SetXattr(ctx, tc.params)
			require.ErrorIs(t, err, tc.expectedErr)
		})
	}

	// 确认未开启任何事务
	mockStore.AssertNotCalled(t, "ExecTxWithOptions", mock.Anything, mock.Anything)
}

// TestSetXattr_InodeLookupAndOrphan 验证第 2 步排他锁与存在性校验。
func TestSetXattr_InodeLookupAndOrphan(t *testing.T) {
	t.Run("Inode 不存在时返回 ErrNotFound", func(t *testing.T) {
		ctx, mockStore, mockQuerier, mgr := setupSetXattrTest(t)
		mockSetXattrExecTx(mockStore, mockQuerier)

		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(100)).
			Return(db.Inode{}, pgx.ErrNoRows).
			Once()

		err := mgr.SetXattr(ctx, meta.SetXattrParams{
			Ino:   100,
			Name:  []byte("user.test"),
			Value: []byte("val"),
			Flags: 0,
		})
		require.ErrorIs(t, err, meta.ErrNotFound)
	})

	t.Run("孤儿节点 (nlink == 0) 正常支持扩展属性写入", func(t *testing.T) {
		ctx, mockStore, mockQuerier, mgr := setupSetXattrTest(t)
		mockSetXattrExecTx(mockStore, mockQuerier)

		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(100)).
			Return(db.Inode{
				Ino:   100,
				Mode:  int32(meta.S_IFREG | 0o644),
				Nlink: 0, // 孤儿节点
			}, nil).
			Once()

		// 属性不存在
		mockQuerier.EXPECT().
			GetInodeXattr(mock.Anything, db.GetInodeXattrParams{Ino: 100, Name: []byte("user.test")}).
			Return(db.InodeXattr{}, pgx.ErrNoRows).
			Once()

		// 配额查询
		mockQuerier.EXPECT().
			GetInodeXattrNamesTotalBytes(mock.Anything, int64(100)).
			Return(int64(0), nil).
			Once()

		// Upsert 写入
		mockQuerier.EXPECT().
			UpsertInodeXattr(mock.Anything, db.UpsertInodeXattrParams{
				Ino:   100,
				Name:  []byte("user.test"),
				Value: []byte("val"),
			}).
			Return(nil).
			Once()

		// Touch ctime
		mockQuerier.EXPECT().
			TouchInodeCtime(mock.Anything, int64(100)).
			Return(nil).
			Once()

		err := mgr.SetXattr(ctx, meta.SetXattrParams{
			Ino:   100,
			Name:  []byte("user.test"),
			Value: []byte("val"),
			Flags: 0,
		})
		require.NoError(t, err)
	})
}

// TestSetXattr_UserNamespaceAdmission 验证第 3 步 user.* 命名空间文件类型准入控制。
func TestSetXattr_UserNamespaceAdmission(t *testing.T) {
	for _, tc := range []struct {
		name        string
		xName       []byte
		mode        uint32
		expectedErr error
	}{
		// user.* 命名空间准入：常规文件、目录、套接字放行
		{
			name:        "user.* 允许常规文件 S_IFREG",
			xName:       []byte("user.mime_type"),
			mode:        meta.S_IFREG | 0o644,
			expectedErr: nil,
		},
		{
			name:        "user.* 允许目录 S_IFDIR",
			xName:       []byte("user.folder_tag"),
			mode:        meta.S_IFDIR | 0o755,
			expectedErr: nil,
		},
		{
			name:        "user.* 允许套接字 S_IFSOCK",
			xName:       []byte("user.sock_info"),
			mode:        meta.S_IFSOCK | 0o777,
			expectedErr: nil,
		},

		// user.* 命名空间拦截：符号链接、字符设备、块设备、FIFO 返回 ErrPerm
		{
			name:        "user.* 拦截符号链接 S_IFLNK",
			xName:       []byte("user.test"),
			mode:        meta.S_IFLNK | 0o777,
			expectedErr: meta.ErrPerm,
		},
		{
			name:        "user.* 拦截字符设备 S_IFCHR",
			xName:       []byte("user.test"),
			mode:        meta.S_IFCHR | 0o666,
			expectedErr: meta.ErrPerm,
		},
		{
			name:        "user.* 拦截块设备 S_IFBLK",
			xName:       []byte("user.test"),
			mode:        meta.S_IFBLK | 0o660,
			expectedErr: meta.ErrPerm,
		},
		{
			name:        "user.* 拦截命名管道 S_IFIFO",
			xName:       []byte("user.test"),
			mode:        meta.S_IFIFO | 0o660,
			expectedErr: meta.ErrPerm,
		},

		// 非 user.* 命名空间：不限文件类型，直接放行
		{
			name:        "trusted.* 允许符号链接 S_IFLNK",
			xName:       []byte("trusted.overlay"),
			mode:        meta.S_IFLNK | 0o777,
			expectedErr: nil,
		},
		{
			name:        "security.* 允许字符设备 S_IFCHR",
			xName:       []byte("security.selinux"),
			mode:        meta.S_IFCHR | 0o666,
			expectedErr: nil,
		},
		{
			name:        "system.* 允许块设备 S_IFBLK",
			xName:       []byte("system.posix_acl_access"),
			mode:        meta.S_IFBLK | 0o660,
			expectedErr: nil,
		},
		{
			name:        "custom.* 允许命名管道 S_IFIFO",
			xName:       []byte("custom.metadata"),
			mode:        meta.S_IFIFO | 0o660,
			expectedErr: nil,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, mockStore, mockQuerier, mgr := setupSetXattrTest(t)
			mockSetXattrExecTx(mockStore, mockQuerier)

			mockQuerier.EXPECT().
				GetInodeForUpdate(mock.Anything, int64(10)).
				Return(db.Inode{
					Ino:   10,
					Mode:  int32(tc.mode),
					Nlink: 1,
				}, nil).
				Once()

			if tc.expectedErr == nil {
				// 放行通路，配置后续查询以保证成功返回
				mockQuerier.EXPECT().
					GetInodeXattr(mock.Anything, db.GetInodeXattrParams{Ino: 10, Name: tc.xName}).
					Return(db.InodeXattr{}, pgx.ErrNoRows).
					Once()

				mockQuerier.EXPECT().
					GetInodeXattrNamesTotalBytes(mock.Anything, int64(10)).
					Return(int64(0), nil).
					Once()

				mockQuerier.EXPECT().
					UpsertInodeXattr(mock.Anything, db.UpsertInodeXattrParams{
						Ino:   10,
						Name:  tc.xName,
						Value: []byte("val"),
					}).
					Return(nil).
					Once()

				mockQuerier.EXPECT().
					TouchInodeCtime(mock.Anything, int64(10)).
					Return(nil).
					Once()
			}

			err := mgr.SetXattr(ctx, meta.SetXattrParams{
				Ino:   10,
				Name:  tc.xName,
				Value: []byte("val"),
				Flags: 0,
			})

			if tc.expectedErr != nil {
				require.ErrorIs(t, err, tc.expectedErr)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

// TestSetXattr_FlagsStateMachine 验证第 4 步属性存在性与 Flags 状态机判定。
func TestSetXattr_FlagsStateMachine(t *testing.T) {
	xName := []byte("user.state")

	t.Run("属性已存在时的 Flags 判定", func(t *testing.T) {
		for _, tc := range []struct {
			name        string
			flags       uint32
			expectedErr error
		}{
			{
				name:        "flags=0 (默认覆盖或创建) -> 允许覆盖",
				flags:       0,
				expectedErr: nil,
			},
			{
				name:        "flags=XattrCreate (0x1) -> 返回 ErrAlreadyExists",
				flags:       meta.XattrCreate,
				expectedErr: meta.ErrAlreadyExists,
			},
			{
				name:        "flags=XattrReplace (0x2) -> 允许更新",
				flags:       meta.XattrReplace,
				expectedErr: nil,
			},
			{
				name:        "flags=0x3 (XattrCreate | XattrReplace) -> 属性存在返回 ErrAlreadyExists",
				flags:       meta.XattrCreate | meta.XattrReplace,
				expectedErr: meta.ErrAlreadyExists,
			},
		} {
			t.Run(tc.name, func(t *testing.T) {
				ctx, mockStore, mockQuerier, mgr := setupSetXattrTest(t)
				mockSetXattrExecTx(mockStore, mockQuerier)

				mockQuerier.EXPECT().
					GetInodeForUpdate(mock.Anything, int64(42)).
					Return(db.Inode{
						Ino:   42,
						Mode:  int32(meta.S_IFREG | 0o644),
						Nlink: 1,
					}, nil).
					Once()

				// 属性已存在
				mockQuerier.EXPECT().
					GetInodeXattr(mock.Anything, db.GetInodeXattrParams{Ino: 42, Name: xName}).
					Return(db.InodeXattr{Ino: 42, Name: xName, Value: []byte("old")}, nil).
					Once()

				if tc.expectedErr == nil {
					// 覆盖已有属性：不校验配额，直接 Upsert 与 Touch
					mockQuerier.EXPECT().
						UpsertInodeXattr(mock.Anything, db.UpsertInodeXattrParams{
							Ino:   42,
							Name:  xName,
							Value: []byte("new"),
						}).
						Return(nil).
						Once()

					mockQuerier.EXPECT().
						TouchInodeCtime(mock.Anything, int64(42)).
						Return(nil).
						Once()
				}

				err := mgr.SetXattr(ctx, meta.SetXattrParams{
					Ino:   42,
					Name:  xName,
					Value: []byte("new"),
					Flags: tc.flags,
				})

				if tc.expectedErr != nil {
					require.ErrorIs(t, err, tc.expectedErr)
				} else {
					require.NoError(t, err)
				}
			})
		}
	})

	t.Run("属性不存在时的 Flags 判定", func(t *testing.T) {
		for _, tc := range []struct {
			name        string
			flags       uint32
			expectedErr error
		}{
			{
				name:        "flags=0 (默认覆盖或创建) -> 允许新建",
				flags:       0,
				expectedErr: nil,
			},
			{
				name:        "flags=XattrCreate (0x1) -> 允许新建",
				flags:       meta.XattrCreate,
				expectedErr: nil,
			},
			{
				name:        "flags=XattrReplace (0x2) -> 属性不存在返回 ErrNoData",
				flags:       meta.XattrReplace,
				expectedErr: meta.ErrNoData,
			},
			{
				name:        "flags=0x3 (XattrCreate | XattrReplace) -> 属性不存在返回 ErrNoData",
				flags:       meta.XattrCreate | meta.XattrReplace,
				expectedErr: meta.ErrNoData,
			},
		} {
			t.Run(tc.name, func(t *testing.T) {
				ctx, mockStore, mockQuerier, mgr := setupSetXattrTest(t)
				mockSetXattrExecTx(mockStore, mockQuerier)

				mockQuerier.EXPECT().
					GetInodeForUpdate(mock.Anything, int64(42)).
					Return(db.Inode{
						Ino:   42,
						Mode:  int32(meta.S_IFREG | 0o644),
						Nlink: 1,
					}, nil).
					Once()

				// 属性不存在
				mockQuerier.EXPECT().
					GetInodeXattr(mock.Anything, db.GetInodeXattrParams{Ino: 42, Name: xName}).
					Return(db.InodeXattr{}, pgx.ErrNoRows).
					Once()

				if tc.expectedErr == nil {
					// 新增属性：校验配额通过后 Upsert 与 Touch
					mockQuerier.EXPECT().
						GetInodeXattrNamesTotalBytes(mock.Anything, int64(42)).
						Return(int64(0), nil).
						Once()

					mockQuerier.EXPECT().
						UpsertInodeXattr(mock.Anything, db.UpsertInodeXattrParams{
							Ino:   42,
							Name:  xName,
							Value: []byte("val"),
						}).
						Return(nil).
						Once()

					mockQuerier.EXPECT().
						TouchInodeCtime(mock.Anything, int64(42)).
						Return(nil).
						Once()
				}

				err := mgr.SetXattr(ctx, meta.SetXattrParams{
					Ino:   42,
					Name:  xName,
					Value: []byte("val"),
					Flags: tc.flags,
				})

				if tc.expectedErr != nil {
					require.ErrorIs(t, err, tc.expectedErr)
				} else {
					require.NoError(t, err)
				}
			})
		}
	})
}

// TestSetXattr_QuotaAndOverwrite 验证单节点 65536 字节配额控制与同名覆盖放行。
func TestSetXattr_QuotaAndOverwrite(t *testing.T) {
	ctx, mockStore, mockQuerier, mgr := setupSetXattrTest(t)

	t.Run("新增属性超出 65536 字节配额返回 ErrNoSpace", func(t *testing.T) {
		mockSetXattrExecTx(mockStore, mockQuerier)

		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(50)).
			Return(db.Inode{Ino: 50, Mode: int32(meta.S_IFREG | 0o644), Nlink: 1}, nil).
			Once()

		xName := []byte("user.newattr")
		mockQuerier.EXPECT().
			GetInodeXattr(mock.Anything, db.GetInodeXattrParams{Ino: 50, Name: xName}).
			Return(db.InodeXattr{}, pgx.ErrNoRows).
			Once()

		// 现有 65525 字节，加上 len(user.newattr) 即 12 字节 + 1 = 13 字节，总长 65538 > 65536
		mockQuerier.EXPECT().
			GetInodeXattrNamesTotalBytes(mock.Anything, int64(50)).
			Return(int64(65525), nil).
			Once()

		err := mgr.SetXattr(ctx, meta.SetXattrParams{
			Ino:   50,
			Name:  xName,
			Value: []byte("val"),
			Flags: 0,
		})
		require.ErrorIs(t, err, meta.ErrNoSpace)
	})

	t.Run("新增属性恰好达到 65536 字节配额边界允许写入", func(t *testing.T) {
		mockSetXattrExecTx(mockStore, mockQuerier)

		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(50)).
			Return(db.Inode{Ino: 50, Mode: int32(meta.S_IFREG | 0o644), Nlink: 1}, nil).
			Once()

		xName := []byte("user.fit") // len = 8, + 1 = 9
		mockQuerier.EXPECT().
			GetInodeXattr(mock.Anything, db.GetInodeXattrParams{Ino: 50, Name: xName}).
			Return(db.InodeXattr{}, pgx.ErrNoRows).
			Once()

		// 现有 65536 - 9 = 65527 字节，恰好等于 65536
		mockQuerier.EXPECT().
			GetInodeXattrNamesTotalBytes(mock.Anything, int64(50)).
			Return(int64(65527), nil).
			Once()

		mockQuerier.EXPECT().
			UpsertInodeXattr(mock.Anything, db.UpsertInodeXattrParams{
				Ino:   50,
				Name:  xName,
				Value: []byte("val"),
			}).
			Return(nil).
			Once()

		mockQuerier.EXPECT().
			TouchInodeCtime(mock.Anything, int64(50)).
			Return(nil).
			Once()

		err := mgr.SetXattr(ctx, meta.SetXattrParams{
			Ino:   50,
			Name:  xName,
			Value: []byte("val"),
			Flags: 0,
		})
		require.NoError(t, err)
	})

	t.Run("覆盖同名属性即使已满配额亦不拦截配额", func(t *testing.T) {
		mockSetXattrExecTx(mockStore, mockQuerier)

		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(50)).
			Return(db.Inode{Ino: 50, Mode: int32(meta.S_IFREG | 0o644), Nlink: 1}, nil).
			Once()

		xName := []byte("user.existing")
		// 属性已存在
		mockQuerier.EXPECT().
			GetInodeXattr(mock.Anything, db.GetInodeXattrParams{Ino: 50, Name: xName}).
			Return(db.InodeXattr{Ino: 50, Name: xName, Value: []byte("old")}, nil).
			Once()

		// 不应当调用 GetInodeXattrNamesTotalBytes
		mockQuerier.EXPECT().
			UpsertInodeXattr(mock.Anything, db.UpsertInodeXattrParams{
				Ino:   50,
				Name:  xName,
				Value: []byte("new_val"),
			}).
			Return(nil).
			Once()

		mockQuerier.EXPECT().
			TouchInodeCtime(mock.Anything, int64(50)).
			Return(nil).
			Once()

		err := mgr.SetXattr(ctx, meta.SetXattrParams{
			Ino:   50,
			Name:  xName,
			Value: []byte("new_val"),
			Flags: 0,
		})
		require.NoError(t, err)
	})
}

// TestSetXattr_NilValueNormalization 验证 Value 为 nil 时归一化为 []byte{}。
func TestSetXattr_NilValueNormalization(t *testing.T) {
	ctx, mockStore, mockQuerier, mgr := setupSetXattrTest(t)
	mockSetXattrExecTx(mockStore, mockQuerier)

	mockQuerier.EXPECT().
		GetInodeForUpdate(mock.Anything, int64(60)).
		Return(db.Inode{Ino: 60, Mode: int32(meta.S_IFREG | 0o644), Nlink: 1}, nil).
		Once()

	xName := []byte("user.empty")
	mockQuerier.EXPECT().
		GetInodeXattr(mock.Anything, db.GetInodeXattrParams{Ino: 60, Name: xName}).
		Return(db.InodeXattr{}, pgx.ErrNoRows).
		Once()

	mockQuerier.EXPECT().
		GetInodeXattrNamesTotalBytes(mock.Anything, int64(60)).
		Return(int64(0), nil).
		Once()

	// 核心断言：传入 UpsertInodeXattr 的 Value 必须为非 nil 切片 []byte{}
	mockQuerier.EXPECT().
		UpsertInodeXattr(mock.Anything, mock.MatchedBy(func(arg db.UpsertInodeXattrParams) bool {
			return arg.Ino == 60 &&
				bytes.Equal(arg.Name, xName) &&
				arg.Value != nil &&
				len(arg.Value) == 0
		})).
		Return(nil).
		Once()

	mockQuerier.EXPECT().
		TouchInodeCtime(mock.Anything, int64(60)).
		Return(nil).
		Once()

	err := mgr.SetXattr(ctx, meta.SetXattrParams{
		Ino:   60,
		Name:  xName,
		Value: nil, // 传入 nil
		Flags: 0,
	})
	require.NoError(t, err)
}

// TestSetXattr_DatabaseErrors 验证底层各 SQL 语句出错时的错误包装返回。
func TestSetXattr_DatabaseErrors(t *testing.T) {
	t.Run("GetInodeForUpdate 数据库错误", func(t *testing.T) {
		ctx, mockStore, mockQuerier, mgr := setupSetXattrTest(t)
		mockSetXattrExecTx(mockStore, mockQuerier)

		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(1)).
			Return(db.Inode{}, errors.New("db lock failed")).
			Once()

		err := mgr.SetXattr(ctx, meta.SetXattrParams{Ino: 1, Name: []byte("user.t"), Value: []byte("v"), Flags: 0})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "get inode for update")
	})

	t.Run("GetInodeXattr 数据库错误", func(t *testing.T) {
		ctx, mockStore, mockQuerier, mgr := setupSetXattrTest(t)
		mockSetXattrExecTx(mockStore, mockQuerier)

		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(1)).
			Return(db.Inode{Ino: 1, Mode: int32(meta.S_IFREG | 0o644), Nlink: 1}, nil).
			Once()

		mockQuerier.EXPECT().
			GetInodeXattr(mock.Anything, mock.Anything).
			Return(db.InodeXattr{}, errors.New("db xattr query failed")).
			Once()

		err := mgr.SetXattr(ctx, meta.SetXattrParams{Ino: 1, Name: []byte("user.t"), Value: []byte("v"), Flags: 0})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "get inode xattr")
	})

	t.Run("GetInodeXattrNamesTotalBytes 数据库错误", func(t *testing.T) {
		ctx, mockStore, mockQuerier, mgr := setupSetXattrTest(t)
		mockSetXattrExecTx(mockStore, mockQuerier)

		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(1)).
			Return(db.Inode{Ino: 1, Mode: int32(meta.S_IFREG | 0o644), Nlink: 1}, nil).
			Once()

		mockQuerier.EXPECT().
			GetInodeXattr(mock.Anything, mock.Anything).
			Return(db.InodeXattr{}, pgx.ErrNoRows).
			Once()

		mockQuerier.EXPECT().
			GetInodeXattrNamesTotalBytes(mock.Anything, int64(1)).
			Return(int64(0), errors.New("db sum failed")).
			Once()

		err := mgr.SetXattr(ctx, meta.SetXattrParams{Ino: 1, Name: []byte("user.t"), Value: []byte("v"), Flags: 0})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "get inode xattr names total bytes")
	})

	t.Run("UpsertInodeXattr 数据库错误", func(t *testing.T) {
		ctx, mockStore, mockQuerier, mgr := setupSetXattrTest(t)
		mockSetXattrExecTx(mockStore, mockQuerier)

		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(1)).
			Return(db.Inode{Ino: 1, Mode: int32(meta.S_IFREG | 0o644), Nlink: 1}, nil).
			Once()

		mockQuerier.EXPECT().
			GetInodeXattr(mock.Anything, mock.Anything).
			Return(db.InodeXattr{}, pgx.ErrNoRows).
			Once()

		mockQuerier.EXPECT().
			GetInodeXattrNamesTotalBytes(mock.Anything, int64(1)).
			Return(int64(0), nil).
			Once()

		mockQuerier.EXPECT().
			UpsertInodeXattr(mock.Anything, mock.Anything).
			Return(errors.New("db insert failed")).
			Once()

		err := mgr.SetXattr(ctx, meta.SetXattrParams{Ino: 1, Name: []byte("user.t"), Value: []byte("v"), Flags: 0})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "upsert inode xattr")
	})

	t.Run("TouchInodeCtime 数据库错误", func(t *testing.T) {
		ctx, mockStore, mockQuerier, mgr := setupSetXattrTest(t)
		mockSetXattrExecTx(mockStore, mockQuerier)

		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(1)).
			Return(db.Inode{Ino: 1, Mode: int32(meta.S_IFREG | 0o644), Nlink: 1}, nil).
			Once()

		mockQuerier.EXPECT().
			GetInodeXattr(mock.Anything, mock.Anything).
			Return(db.InodeXattr{}, pgx.ErrNoRows).
			Once()

		mockQuerier.EXPECT().
			GetInodeXattrNamesTotalBytes(mock.Anything, int64(1)).
			Return(int64(0), nil).
			Once()

		mockQuerier.EXPECT().
			UpsertInodeXattr(mock.Anything, mock.Anything).
			Return(nil).
			Once()

		mockQuerier.EXPECT().
			TouchInodeCtime(mock.Anything, int64(1)).
			Return(errors.New("db touch ctime failed")).
			Once()

		err := mgr.SetXattr(ctx, meta.SetXattrParams{Ino: 1, Name: []byte("user.t"), Value: []byte("v"), Flags: 0})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "touch inode ctime")
	})
}

// TestSetXattr_Integration_MaxXattrListBytesBoundary 基于真实 PostgreSQL 验证：
// SetXattr 触及 MaxXattrListBytes (65536) 含 + 1 NUL 计费临界值（恰好 65536 成功、超出返回 ErrNoSpace）。
func TestSetXattr_Integration_MaxXattrListBytesBoundary(t *testing.T) {
	pool := setupFormalPostgresPool(t)
	ctx := context.Background()
	store := db.NewStore(pool)
	mgr := meta.New(store)

	disk, err := mgr.CreateDisk(ctx, meta.CreateDiskParams{
		Name:     "test-xattr-boundary-disk",
		MaxBytes: 10 * 1024 * 1024,
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = mgr.DeleteDisk(context.Background(), disk.DiskID)
	})

	mknodRes, err := mgr.Mknod(ctx, meta.MknodParams{
		ParentIno: disk.RootIno,
		Name:      []byte("xattr_boundary_file.bin"),
		Mode:      meta.S_IFREG | 0o644,
	})
	require.NoError(t, err)
	ino := mknodRes.Ino

	// 1. 连续插入 255 个长度为 255 字节的属性（每个属性计费 255 + 1 = 256 字节）
	// 255 * 256 = 65280 字节，剩余配额为 65536 - 65280 = 256 字节
	for i := 0; i < 255; i++ {
		// 构造 255 字节的合法 user.* 属性名
		name := []byte(fmt.Sprintf("user.key_%03d_%s", i, bytes.Repeat([]byte("a"), 242)))
		require.Len(t, name, 255)
		err := mgr.SetXattr(ctx, meta.SetXattrParams{
			Ino:   ino,
			Name:  name,
			Value: []byte("v"),
			Flags: meta.XattrCreate,
		})
		require.NoError(t, err, "插入第 %d 个 xattr 必须成功", i)
	}

	// 2. 插入第 256 个属性，长度恰好为 255 字节（计费 255 + 1 = 256 字节）
	// 总字节数累计达到 65280 + 256 = 65536 字节，恰好等于 MaxXattrListBytes
	nameExact := []byte(fmt.Sprintf("user.exact_%s", bytes.Repeat([]byte("b"), 244)))
	require.Len(t, nameExact, 255)
	err = mgr.SetXattr(ctx, meta.SetXattrParams{
		Ino:   ino,
		Name:  nameExact,
		Value: []byte("exact-val"),
		Flags: meta.XattrCreate,
	})
	require.NoError(t, err, "恰好达到 65536 字节上限必须成功")

	// 验证底表统计正好为 65536 字节
	totalBytes, err := store.GetInodeXattrNamesTotalBytes(ctx, int64(ino))
	require.NoError(t, err)
	assert.Equal(t, int64(meta.MaxXattrListBytes), totalBytes, "底表扩展属性名称总计费必须精确为 65536 字节")

	// 3. 此时配额已满，再次尝试新增任意属性（超出 1 字节及以上）必返回 ErrNoSpace
	err = mgr.SetXattr(ctx, meta.SetXattrParams{
		Ino:   ino,
		Name:  []byte("user.overflow"),
		Value: []byte("val"),
		Flags: meta.XattrCreate,
	})
	require.Error(t, err)
	assert.True(t, errors.Is(err, meta.ErrNoSpace), "超出 65536 上限时必须返回 ErrNoSpace")

	// 4. 对已有属性执行覆盖更新（不增加名称长度），必须依然成功
	err = mgr.SetXattr(ctx, meta.SetXattrParams{
		Ino:   ino,
		Name:  nameExact,
		Value: []byte("updated-exact-val"),
		Flags: meta.XattrReplace,
	})
	require.NoError(t, err, "满额状态下对已有属性执行 Replace 覆盖写必须成功")
}
