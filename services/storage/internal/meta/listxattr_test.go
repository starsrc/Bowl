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

// setupListXattrTest 初始化测试所需的 Mock 上下文。
func setupListXattrTest(t *testing.T) (context.Context, *dbmock.MockStore, *dbmock.MockQuerier, meta.Manager) {
	t.Helper()
	ctx := context.Background()
	mockStore := dbmock.NewMockStore(t)
	mockQuerier := dbmock.NewMockQuerier(t)
	mgr := meta.New(mockStore)
	return ctx, mockStore, mockQuerier, mgr
}

// mockListXattrReadOnlyTx 辅助配置 ListXattr 的只读 RepeatableRead 事务穿透行为。
func mockListXattrReadOnlyTx(mockStore *dbmock.MockStore, mockQuerier *dbmock.MockQuerier) {
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

// TestListXattr_StaticValidation 验证第 1 步入参静态校验（不查库，不开启事务）。
func TestListXattr_StaticValidation(t *testing.T) {
	ctx, mockStore, _, mgr := setupListXattrTest(t)

	for _, tc := range []struct {
		name        string
		ino         uint64
		expectedErr error
	}{
		{
			name:        "ino 为 0",
			ino:         0,
			expectedErr: meta.ErrInvalidArgument,
		},
		{
			name:        "ino 超过 math.MaxInt64",
			ino:         uint64(math.MaxInt64) + 1,
			expectedErr: meta.ErrInvalidArgument,
		},
		{
			name:        "ino 为 uint64 最大值",
			ino:         math.MaxUint64,
			expectedErr: meta.ErrInvalidArgument,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			names, err := mgr.ListXattr(ctx, tc.ino)
			require.Nil(t, names)
			require.ErrorIs(t, err, tc.expectedErr)
		})
	}

	// 确认静态校验未开启任何数据库事务
	mockStore.AssertNotCalled(t, "ExecTxWithOptions", mock.Anything, mock.Anything)
}

// TestListXattr_TxOptionsAndInodeLookup 验证第 2 步只读事务配置与 Inode 存在性校验。
func TestListXattr_TxOptionsAndInodeLookup(t *testing.T) {
	t.Run("只读事务与 RepeatableRead 快照隔离级别保证", func(t *testing.T) {
		ctx, mockStore, mockQuerier, mgr := setupListXattrTest(t)
		mockListXattrReadOnlyTx(mockStore, mockQuerier)

		mockQuerier.EXPECT().
			GetInode(mock.Anything, int64(1)).
			Return(db.Inode{
				Ino:  1,
				Mode: int32(meta.S_IFREG | 0o644),
			}, nil).
			Once()

		mockQuerier.EXPECT().
			ListInodeXattrNames(mock.Anything, int64(1)).
			Return([][]byte{[]byte("user.test")}, nil).
			Once()

		names, err := mgr.ListXattr(ctx, 1)
		require.NoError(t, err)
		assert.Equal(t, [][]byte{[]byte("user.test")}, names)
	})

	t.Run("Inode 不存在时返回 ErrNotFound", func(t *testing.T) {
		ctx, mockStore, mockQuerier, mgr := setupListXattrTest(t)
		mockListXattrReadOnlyTx(mockStore, mockQuerier)

		mockQuerier.EXPECT().
			GetInode(mock.Anything, int64(999)).
			Return(db.Inode{}, pgx.ErrNoRows).
			Once()

		names, err := mgr.ListXattr(ctx, 999)
		require.Nil(t, names)
		require.ErrorIs(t, err, meta.ErrNotFound)
	})

	t.Run("Inode 查询底层返回未知数据库错误时向上传递包装错误", func(t *testing.T) {
		ctx, mockStore, mockQuerier, mgr := setupListXattrTest(t)
		mockListXattrReadOnlyTx(mockStore, mockQuerier)

		dbErr := errors.New("connection reset by peer")
		mockQuerier.EXPECT().
			GetInode(mock.Anything, int64(1)).
			Return(db.Inode{}, dbErr).
			Once()

		names, err := mgr.ListXattr(ctx, 1)
		require.Nil(t, names)
		require.ErrorIs(t, err, dbErr)
	})
}

// TestListXattr_OrphanInodeSupport 验证对孤儿节点 (nlink == 0) 的扩展属性列表查询支持。
func TestListXattr_OrphanInodeSupport(t *testing.T) {
	for _, tc := range []struct {
		name string
		mode uint32
	}{
		{
			name: "孤儿常规文件 (S_IFREG, nlink=0)",
			mode: meta.S_IFREG | 0o644,
		},
		{
			name: "孤儿目录 (S_IFDIR, nlink=0)",
			mode: meta.S_IFDIR | 0o755,
		},
		{
			name: "孤儿符号链接 (S_IFLNK, nlink=0)",
			mode: meta.S_IFLNK | 0o777,
		},
		{
			name: "孤儿套接字 (S_IFSOCK, nlink=0)",
			mode: meta.S_IFSOCK | 0o666,
		},
		{
			name: "孤儿字符设备 (S_IFCHR, nlink=0)",
			mode: meta.S_IFCHR | 0o600,
		},
		{
			name: "孤儿块设备 (S_IFBLK, nlink=0)",
			mode: meta.S_IFBLK | 0o600,
		},
		{
			name: "孤儿 FIFO (S_IFIFO, nlink=0)",
			mode: meta.S_IFIFO | 0o644,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, mockStore, mockQuerier, mgr := setupListXattrTest(t)
			mockListXattrReadOnlyTx(mockStore, mockQuerier)

			mockQuerier.EXPECT().
				GetInode(mock.Anything, int64(100)).
				Return(db.Inode{
					Ino:   100,
					Mode:  int32(tc.mode),
					Nlink: 0, // 孤儿节点
				}, nil).
				Once()

			expectedNames := [][]byte{[]byte("user.orphan_attr")}
			mockQuerier.EXPECT().
				ListInodeXattrNames(mock.Anything, int64(100)).
				Return(expectedNames, nil).
				Once()

			names, err := mgr.ListXattr(ctx, 100)
			require.NoError(t, err)
			assert.Equal(t, expectedNames, names)
		})
	}
}

// TestListXattr_QueryResults 验证属性名称列表查询、原始字节序排序与空列表语义。
func TestListXattr_QueryResults(t *testing.T) {
	t.Run("无属性时返回空切片 [][]byte{} 而非 nil", func(t *testing.T) {
		ctx, mockStore, mockQuerier, mgr := setupListXattrTest(t)
		mockListXattrReadOnlyTx(mockStore, mockQuerier)

		mockQuerier.EXPECT().
			GetInode(mock.Anything, int64(1)).
			Return(db.Inode{
				Ino:   1,
				Mode:  int32(meta.S_IFREG | 0o644),
				Nlink: 1,
			}, nil).
			Once()

		// 模拟数据库返回 nil 或空切片
		mockQuerier.EXPECT().
			ListInodeXattrNames(mock.Anything, int64(1)).
			Return(nil, nil).
			Once()

		names, err := mgr.ListXattr(ctx, 1)
		require.NoError(t, err)
		assert.NotNil(t, names, "无属性时必须返回非 nil 的空切片")
		assert.Empty(t, names, "切片长度必须为 0")
	})

	t.Run("按 BYTEA ASC 原始字节序确定性升序返回（含非 UTF-8 字节序列）", func(t *testing.T) {
		ctx, mockStore, mockQuerier, mgr := setupListXattrTest(t)
		mockListXattrReadOnlyTx(mockStore, mockQuerier)

		mockQuerier.EXPECT().
			GetInode(mock.Anything, int64(1)).
			Return(db.Inode{
				Ino:   1,
				Mode:  int32(meta.S_IFREG | 0o644),
				Nlink: 1,
			}, nil).
			Once()

		// 构造严格按 BYTEA ASC 排序的属性名列表，包含特殊控制字符、非 UTF-8 字节
		sortedNames := [][]byte{
			{0x01},
			[]byte("security.selinux"),
			[]byte("system.posix_acl_access"),
			[]byte("trusted.overlay.opaque"),
			[]byte("user.a"),
			[]byte("user.b"),
			[]byte("user.mime/type"),
			{0x80, 'a'},
			{0xff, 0xfe},
		}

		// 验证测试数据本身确实满足严格升序
		for i := 1; i < len(sortedNames); i++ {
			assert.True(t, bytes.Compare(sortedNames[i-1], sortedNames[i]) < 0, "测试数据必须按字节序升序")
		}

		mockQuerier.EXPECT().
			ListInodeXattrNames(mock.Anything, int64(1)).
			Return(sortedNames, nil).
			Once()

		names, err := mgr.ListXattr(ctx, 1)
		require.NoError(t, err)
		assert.Equal(t, sortedNames, names)
	})

	t.Run("不按调用方过滤（对齐 Linux 内核 FUSE 行为，包含所有命名空间）", func(t *testing.T) {
		ctx, mockStore, mockQuerier, mgr := setupListXattrTest(t)
		mockListXattrReadOnlyTx(mockStore, mockQuerier)

		// 即使是在符号链接节点上，ListXattr 也不过滤 user.* 属性（对齐 Linux FUSE 语义）
		mockQuerier.EXPECT().
			GetInode(mock.Anything, int64(1)).
			Return(db.Inode{
				Ino:   1,
				Mode:  int32(meta.S_IFLNK | 0o777),
				Nlink: 1,
			}, nil).
			Once()

		allNames := [][]byte{
			[]byte("security.capability"),
			[]byte("system.posix_acl_access"),
			[]byte("trusted.overlay.opaque"),
			[]byte("user.my_attr"),
		}

		mockQuerier.EXPECT().
			ListInodeXattrNames(mock.Anything, int64(1)).
			Return(allNames, nil).
			Once()

		names, err := mgr.ListXattr(ctx, 1)
		require.NoError(t, err)
		assert.Equal(t, allNames, names)
	})

	t.Run("ListInodeXattrNames 查询失败向上传递包装错误", func(t *testing.T) {
		ctx, mockStore, mockQuerier, mgr := setupListXattrTest(t)
		mockListXattrReadOnlyTx(mockStore, mockQuerier)

		mockQuerier.EXPECT().
			GetInode(mock.Anything, int64(1)).
			Return(db.Inode{
				Ino:   1,
				Mode:  int32(meta.S_IFREG | 0o644),
				Nlink: 1,
			}, nil).
			Once()

		dbErr := errors.New("read table error")
		mockQuerier.EXPECT().
			ListInodeXattrNames(mock.Anything, int64(1)).
			Return(nil, dbErr).
			Once()

		names, err := mgr.ListXattr(ctx, 1)
		require.Nil(t, names)
		require.ErrorIs(t, err, dbErr)
	})

	t.Run("遵循 noatime 策略不刷新任何时间戳", func(t *testing.T) {
		ctx, mockStore, mockQuerier, mgr := setupListXattrTest(t)
		mockListXattrReadOnlyTx(mockStore, mockQuerier)

		mockQuerier.EXPECT().
			GetInode(mock.Anything, int64(1)).
			Return(db.Inode{
				Ino:   1,
				Mode:  int32(meta.S_IFREG | 0o644),
				Nlink: 1,
				Atime: pgtype.Timestamptz{Time: time.Unix(1000, 0), Valid: true},
			}, nil).
			Once()

		mockQuerier.EXPECT().
			ListInodeXattrNames(mock.Anything, int64(1)).
			Return([][]byte{[]byte("user.test")}, nil).
			Once()

		names, err := mgr.ListXattr(ctx, 1)
		require.NoError(t, err)
		assert.Equal(t, [][]byte{[]byte("user.test")}, names)

		// 确保未调用任何时间戳更新方法
		mockQuerier.AssertNotCalled(t, "TouchInodeCtime", mock.Anything, mock.Anything)
		mockQuerier.AssertNotCalled(t, "TouchInodeTimestamps", mock.Anything, mock.Anything)
	})
}
