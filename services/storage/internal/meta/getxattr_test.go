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

	"github.com/starsrc/Aether/pkg/tx"
	"github.com/starsrc/bowl/services/storage/internal/db"
	dbmock "github.com/starsrc/bowl/services/storage/internal/db/mock"
	"github.com/starsrc/bowl/services/storage/internal/meta"
)

// setupGetXattrTest 初始化测试所需的 Mock 上下文。
func setupGetXattrTest(t *testing.T) (context.Context, *dbmock.MockStore, *dbmock.MockQuerier, meta.Manager) {
	t.Helper()
	ctx := context.Background()
	mockStore := dbmock.NewMockStore(t)
	mockQuerier := dbmock.NewMockQuerier(t)
	mgr := meta.New(mockStore)
	return ctx, mockStore, mockQuerier, mgr
}

// mockGetXattrReadOnlyTx 辅助配置 GetXattr 的只读 RepeatableRead 事务穿透行为。
func mockGetXattrReadOnlyTx(mockStore *dbmock.MockStore, mockQuerier *dbmock.MockQuerier) {
	mockStore.EXPECT().
		ExecTxWithOptions(
			mock.Anything,
			mock.Anything,
			matchReadOnlyOpt(),
			matchRepeatableReadOpt(),
		).
		RunAndReturn(func(ctx context.Context, fn func(db.Querier) error, opts ...tx.Option) error {
			cfg := tx.ApplyOptions(opts...)
			if cfg.AccessMode != tx.ReadOnly || cfg.IsoLevel != tx.RepeatableRead {
				return fmt.Errorf("unexpected tx options: AccessMode=%v, IsoLevel=%v", cfg.AccessMode, cfg.IsoLevel)
			}
			return fn(mockQuerier)
		}).
		Once()
}

// TestGetXattr_StaticValidation 验证第 1 步入参静态校验（不查库，不开启事务）。
func TestGetXattr_StaticValidation(t *testing.T) {
	ctx, mockStore, _, mgr := setupGetXattrTest(t)

	for _, tc := range []struct {
		name        string
		ino         uint64
		xName       []byte
		expectedErr error
	}{
		{
			name:        "ino 为 0",
			ino:         0,
			xName:       []byte("user.test"),
			expectedErr: meta.ErrInvalidArgument,
		},
		{
			name:        "ino 超过 math.MaxInt64",
			ino:         uint64(math.MaxInt64) + 1,
			xName:       []byte("user.test"),
			expectedErr: meta.ErrInvalidArgument,
		},
		{
			name:        "ino 为 uint64 最大值",
			ino:         math.MaxUint64,
			xName:       []byte("user.test"),
			expectedErr: meta.ErrInvalidArgument,
		},
		{
			name:        "name 为空切片 (0 字节)",
			ino:         1,
			xName:       []byte{},
			expectedErr: meta.ErrInvalidArgument,
		},
		{
			name:        "name 为 nil (0 字节)",
			ino:         1,
			xName:       nil,
			expectedErr: meta.ErrInvalidArgument,
		},
		{
			name:        "name 超过 255 字节 (256 字节)",
			ino:         1,
			xName:       bytes.Repeat([]byte("n"), 256),
			expectedErr: meta.ErrRange,
		},
		{
			name:        "name 开头包含空字节 NUL",
			ino:         1,
			xName:       []byte("\x00user.test"),
			expectedErr: meta.ErrInvalidArgument,
		},
		{
			name:        "name 中间包含空字节 NUL",
			ino:         1,
			xName:       []byte("user\x00test"),
			expectedErr: meta.ErrInvalidArgument,
		},
		{
			name:        "name 结尾包含空字节 NUL",
			ino:         1,
			xName:       []byte("user.test\x00"),
			expectedErr: meta.ErrInvalidArgument,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			val, err := mgr.GetXattr(ctx, tc.ino, tc.xName)
			require.Nil(t, val)
			require.ErrorIs(t, err, tc.expectedErr)
		})
	}

	// 确认第 1 步拦截未开启任何数据库事务
	mockStore.AssertNotCalled(t, "ExecTxWithOptions", mock.Anything, mock.Anything)
}

// TestGetXattr_TxOptionsAndInodeLookup 验证第 2 步只读事务配置与 Inode 存在性校验。
func TestGetXattr_TxOptionsAndInodeLookup(t *testing.T) {
	t.Run("只读事务与 RepeatableRead 快照隔离级别保证", func(t *testing.T) {
		ctx, mockStore, mockQuerier, mgr := setupGetXattrTest(t)
		mockGetXattrReadOnlyTx(mockStore, mockQuerier)

		mockQuerier.EXPECT().
			GetInode(mock.Anything, int64(1)).
			Return(db.Inode{
				Ino:  1,
				Mode: int32(meta.S_IFREG | 0o644),
			}, nil).
			Once()

		mockQuerier.EXPECT().
			GetInodeXattr(mock.Anything, db.GetInodeXattrParams{
				Ino:  1,
				Name: []byte("user.test"),
			}).
			Return(db.InodeXattr{
				Ino:   1,
				Name:  []byte("user.test"),
				Value: []byte("success"),
			}, nil).
			Once()

		val, err := mgr.GetXattr(ctx, 1, []byte("user.test"))
		require.NoError(t, err)
		assert.Equal(t, []byte("success"), val)
	})

	t.Run("Inode 不存在时返回 ErrNotFound", func(t *testing.T) {
		ctx, mockStore, mockQuerier, mgr := setupGetXattrTest(t)
		mockGetXattrReadOnlyTx(mockStore, mockQuerier)

		mockQuerier.EXPECT().
			GetInode(mock.Anything, int64(100)).
			Return(db.Inode{}, pgx.ErrNoRows).
			Once()

		val, err := mgr.GetXattr(ctx, 100, []byte("user.test"))
		require.Nil(t, val)
		require.ErrorIs(t, err, meta.ErrNotFound)
	})

	t.Run("底层数据库 GetInode 查询错误时包装返回", func(t *testing.T) {
		ctx, mockStore, mockQuerier, mgr := setupGetXattrTest(t)
		mockGetXattrReadOnlyTx(mockStore, mockQuerier)

		mockQuerier.EXPECT().
			GetInode(mock.Anything, int64(100)).
			Return(db.Inode{}, errors.New("db connection lost")).
			Once()

		val, err := mgr.GetXattr(ctx, 100, []byte("user.test"))
		require.Nil(t, val)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "get inode 100")
	})

	t.Run("孤儿节点 (nlink == 0) 正常支持扩展属性读取", func(t *testing.T) {
		ctx, mockStore, mockQuerier, mgr := setupGetXattrTest(t)
		mockGetXattrReadOnlyTx(mockStore, mockQuerier)

		mockQuerier.EXPECT().
			GetInode(mock.Anything, int64(42)).
			Return(db.Inode{
				Ino:   42,
				Mode:  int32(meta.S_IFREG | 0o644),
				Nlink: 0, // 孤儿节点
			}, nil).
			Once()

		mockQuerier.EXPECT().
			GetInodeXattr(mock.Anything, db.GetInodeXattrParams{
				Ino:  42,
				Name: []byte("user.orphan"),
			}).
			Return(db.InodeXattr{
				Ino:   42,
				Name:  []byte("user.orphan"),
				Value: []byte("orphan_data"),
			}, nil).
			Once()

		val, err := mgr.GetXattr(ctx, 42, []byte("user.orphan"))
		require.NoError(t, err)
		assert.Equal(t, []byte("orphan_data"), val)
	})
}

// TestGetXattr_UserNamespaceAdmission 验证第 3 步 user.* 命名空间文件类型准入控制（7 种文件类型）。
func TestGetXattr_UserNamespaceAdmission(t *testing.T) {
	all7Types := []struct {
		typeName    string
		mode        uint32
		userAllowed bool
	}{
		{typeName: "常规文件 S_IFREG", mode: meta.S_IFREG | 0o644, userAllowed: true},
		{typeName: "目录 S_IFDIR", mode: meta.S_IFDIR | 0o755, userAllowed: true},
		{typeName: "套接字 S_IFSOCK", mode: meta.S_IFSOCK | 0o777, userAllowed: true},
		{typeName: "符号链接 S_IFLNK", mode: meta.S_IFLNK | 0o777, userAllowed: false},
		{typeName: "字符设备 S_IFCHR", mode: meta.S_IFCHR | 0o666, userAllowed: false},
		{typeName: "块设备 S_IFBLK", mode: meta.S_IFBLK | 0o660, userAllowed: false},
		{typeName: "命名管道 FIFO S_IFIFO", mode: meta.S_IFIFO | 0o660, userAllowed: false},
	}

	for _, tc := range all7Types {
		t.Run("user.* 命名空间: "+tc.typeName, func(t *testing.T) {
			ctx, mockStore, mockQuerier, mgr := setupGetXattrTest(t)
			mockGetXattrReadOnlyTx(mockStore, mockQuerier)

			mockQuerier.EXPECT().
				GetInode(mock.Anything, int64(10)).
				Return(db.Inode{
					Ino:  10,
					Mode: int32(tc.mode),
				}, nil).
				Once()

			if tc.userAllowed {
				mockQuerier.EXPECT().
					GetInodeXattr(mock.Anything, db.GetInodeXattrParams{
						Ino:  10,
						Name: []byte("user.test"),
					}).
					Return(db.InodeXattr{
						Ino:   10,
						Name:  []byte("user.test"),
						Value: []byte("user_val"),
					}, nil).
					Once()

				val, err := mgr.GetXattr(ctx, 10, []byte("user.test"))
				require.NoError(t, err)
				assert.Equal(t, []byte("user_val"), val)
			} else {
				// 非法文件类型在 user.* 下必须直接返回 ErrNoData，且严禁调用 GetInodeXattr
				val, err := mgr.GetXattr(ctx, 10, []byte("user.test"))
				require.Nil(t, val)
				require.ErrorIs(t, err, meta.ErrNoData)
				mockQuerier.AssertNotCalled(t, "GetInodeXattr", mock.Anything, mock.Anything)
			}
		})

		t.Run("非 user.* 命名空间 (trusted.*): "+tc.typeName, func(t *testing.T) {
			ctx, mockStore, mockQuerier, mgr := setupGetXattrTest(t)
			mockGetXattrReadOnlyTx(mockStore, mockQuerier)

			mockQuerier.EXPECT().
				GetInode(mock.Anything, int64(20)).
				Return(db.Inode{
					Ino:  20,
					Mode: int32(tc.mode),
				}, nil).
				Once()

			// 非 user.* 命名空间在所有文件类型上均放行，正常查询 GetInodeXattr
			mockQuerier.EXPECT().
				GetInodeXattr(mock.Anything, db.GetInodeXattrParams{
					Ino:  20,
					Name: []byte("trusted.overlay"),
				}).
				Return(db.InodeXattr{
					Ino:   20,
					Name:  []byte("trusted.overlay"),
					Value: []byte("trusted_val"),
				}, nil).
				Once()

			val, err := mgr.GetXattr(ctx, 20, []byte("trusted.overlay"))
			require.NoError(t, err)
			assert.Equal(t, []byte("trusted_val"), val)
		})
	}
}

// TestGetXattr_ReadValueAndNoAtime 验证第 4 步属性值读取、边界返回与 noatime 保证。
func TestGetXattr_ReadValueAndNoAtime(t *testing.T) {
	t.Run("属性不存在时返回 ErrNoData", func(t *testing.T) {
		ctx, mockStore, mockQuerier, mgr := setupGetXattrTest(t)
		mockGetXattrReadOnlyTx(mockStore, mockQuerier)

		mockQuerier.EXPECT().
			GetInode(mock.Anything, int64(1)).
			Return(db.Inode{
				Ino:  1,
				Mode: int32(meta.S_IFREG | 0o644),
			}, nil).
			Once()

		mockQuerier.EXPECT().
			GetInodeXattr(mock.Anything, db.GetInodeXattrParams{
				Ino:  1,
				Name: []byte("user.non_exist"),
			}).
			Return(db.InodeXattr{}, pgx.ErrNoRows).
			Once()

		val, err := mgr.GetXattr(ctx, 1, []byte("user.non_exist"))
		require.Nil(t, val)
		require.ErrorIs(t, err, meta.ErrNoData)
	})

	t.Run("底层数据库 GetInodeXattr 错误时包装返回", func(t *testing.T) {
		ctx, mockStore, mockQuerier, mgr := setupGetXattrTest(t)
		mockGetXattrReadOnlyTx(mockStore, mockQuerier)

		mockQuerier.EXPECT().
			GetInode(mock.Anything, int64(1)).
			Return(db.Inode{
				Ino:  1,
				Mode: int32(meta.S_IFREG | 0o644),
			}, nil).
			Once()

		mockQuerier.EXPECT().
			GetInodeXattr(mock.Anything, db.GetInodeXattrParams{
				Ino:  1,
				Name: []byte("user.test"),
			}).
			Return(db.InodeXattr{}, errors.New("read error")).
			Once()

		val, err := mgr.GetXattr(ctx, 1, []byte("user.test"))
		require.Nil(t, val)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "get inode xattr (ino=1)")
	})

	t.Run("精确返回 0 字节空属性值", func(t *testing.T) {
		ctx, mockStore, mockQuerier, mgr := setupGetXattrTest(t)
		mockGetXattrReadOnlyTx(mockStore, mockQuerier)

		mockQuerier.EXPECT().
			GetInode(mock.Anything, int64(1)).
			Return(db.Inode{
				Ino:  1,
				Mode: int32(meta.S_IFREG | 0o644),
			}, nil).
			Once()

		mockQuerier.EXPECT().
			GetInodeXattr(mock.Anything, db.GetInodeXattrParams{
				Ino:  1,
				Name: []byte("user.empty"),
			}).
			Return(db.InodeXattr{
				Ino:   1,
				Name:  []byte("user.empty"),
				Value: []byte{},
			}, nil).
			Once()

		val, err := mgr.GetXattr(ctx, 1, []byte("user.empty"))
		require.NoError(t, err)
		require.NotNil(t, val)
		assert.Empty(t, val)
		assert.Equal(t, []byte{}, val)
	})

	t.Run("精确返回非 UTF-8 原始字节属性值与特殊名称", func(t *testing.T) {
		ctx, mockStore, mockQuerier, mgr := setupGetXattrTest(t)
		mockGetXattrReadOnlyTx(mockStore, mockQuerier)

		rawName := []byte{0xff, 0xfe, 0x80, 'a'}
		rawValue := []byte{0x00, 0xff, 0xfe, 0x80, 0x01, 0x02}

		mockQuerier.EXPECT().
			GetInode(mock.Anything, int64(1)).
			Return(db.Inode{
				Ino:  1,
				Mode: int32(meta.S_IFREG | 0o644),
			}, nil).
			Once()

		mockQuerier.EXPECT().
			GetInodeXattr(mock.Anything, db.GetInodeXattrParams{
				Ino:  1,
				Name: rawName,
			}).
			Return(db.InodeXattr{
				Ino:   1,
				Name:  rawName,
				Value: rawValue,
			}, nil).
			Once()

		val, err := mgr.GetXattr(ctx, 1, rawName)
		require.NoError(t, err)
		assert.Equal(t, rawValue, val)
	})

	t.Run("遵循 noatime 策略不刷新任何时间戳", func(t *testing.T) {
		ctx, mockStore, mockQuerier, mgr := setupGetXattrTest(t)
		mockGetXattrReadOnlyTx(mockStore, mockQuerier)

		mockQuerier.EXPECT().
			GetInode(mock.Anything, int64(1)).
			Return(db.Inode{
				Ino:   1,
				Mode:  int32(meta.S_IFREG | 0o644),
				Atime: pgtype.Timestamptz{Time: time.Unix(1000, 0), Valid: true},
			}, nil).
			Once()

		mockQuerier.EXPECT().
			GetInodeXattr(mock.Anything, db.GetInodeXattrParams{
				Ino:  1,
				Name: []byte("user.noatime"),
			}).
			Return(db.InodeXattr{
				Ino:   1,
				Name:  []byte("user.noatime"),
				Value: []byte("test_value"),
			}, nil).
			Once()

		val, err := mgr.GetXattr(ctx, 1, []byte("user.noatime"))
		require.NoError(t, err)
		assert.Equal(t, []byte("test_value"), val)

		// 确保未调用任何时间戳更新方法
		mockQuerier.AssertNotCalled(t, "TouchInodeCtime", mock.Anything, mock.Anything)
		mockQuerier.AssertNotCalled(t, "TouchInodeTimestamps", mock.Anything, mock.Anything)
	})
}
