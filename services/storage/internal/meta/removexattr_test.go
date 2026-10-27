package meta_test

import (
	"bytes"
	"context"
	"errors"
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

// setupRemoveXattrTest 初始化测试所需的 Mock 上下文。
func setupRemoveXattrTest(t *testing.T) (context.Context, *dbmock.MockStore, *dbmock.MockQuerier, meta.Manager) {
	t.Helper()
	ctx := context.Background()
	mockStore := dbmock.NewMockStore(t)
	mockQuerier := dbmock.NewMockQuerier(t)
	mgr := meta.New(mockStore)
	return ctx, mockStore, mockQuerier, mgr
}

// mockRemoveXattrExecTx 辅助配置默认事务穿透行为。
func mockRemoveXattrExecTx(mockStore *dbmock.MockStore, mockQuerier *dbmock.MockQuerier) {
	mockStore.EXPECT().
		ExecTxWithOptions(mock.Anything, mock.Anything).
		RunAndReturn(func(ctx context.Context, fn func(db.Querier) error, opts ...tx.Option) error {
			return fn(mockQuerier)
		}).
		Once()
}

// TestRemoveXattr_StaticValidation 验证第 1 步入参静态校验（不查库，不开启事务）。
func TestRemoveXattr_StaticValidation(t *testing.T) {
	ctx, mockStore, _, mgr := setupRemoveXattrTest(t)

	for _, tc := range []struct {
		name        string
		ino         uint64
		xName       []byte
		expectedErr error
	}{
		// ino 边界
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

		// name 长度边界
		{
			name:        "name 为空切片 (0 字节)",
			ino:         1,
			xName:       []byte{},
			expectedErr: meta.ErrRange,
		},
		{
			name:        "name 为 nil (0 字节)",
			ino:         1,
			xName:       nil,
			expectedErr: meta.ErrRange,
		},
		{
			name:        "name 超过 255 字节 (256 字节)",
			ino:         1,
			xName:       bytes.Repeat([]byte("n"), 256),
			expectedErr: meta.ErrRange,
		},

		// name 包含非法空字符 NUL
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
			err := mgr.RemoveXattr(ctx, tc.ino, tc.xName)
			assert.ErrorIs(t, err, tc.expectedErr)
		})
	}

	// 确认静态校验完全在内存中完成，未发生任何数据库事务调用
	mockStore.AssertNotCalled(t, "ExecTxWithOptions", mock.Anything, mock.Anything)
}

// TestRemoveXattr_InodeLookupAndOrphan 验证第 2 步排他锁与存在性校验。
func TestRemoveXattr_InodeLookupAndOrphan(t *testing.T) {
	t.Run("Inode 不存在时返回 ErrNotFound", func(t *testing.T) {
		ctx, mockStore, mockQuerier, mgr := setupRemoveXattrTest(t)
		mockRemoveXattrExecTx(mockStore, mockQuerier)

		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(100)).
			Return(db.Inode{}, pgx.ErrNoRows).
			Once()

		err := mgr.RemoveXattr(ctx, 100, []byte("user.test"))
		require.ErrorIs(t, err, meta.ErrNotFound)
	})

	t.Run("孤儿节点 (nlink == 0) 正常支持扩展属性移除", func(t *testing.T) {
		ctx, mockStore, mockQuerier, mgr := setupRemoveXattrTest(t)
		mockRemoveXattrExecTx(mockStore, mockQuerier)

		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(100)).
			Return(db.Inode{
				Ino:   100,
				Mode:  int32(meta.S_IFREG | 0o644),
				Nlink: 0, // 孤儿节点
			}, nil).
			Once()

		mockQuerier.EXPECT().
			DeleteInodeXattr(mock.Anything, db.DeleteInodeXattrParams{
				Ino:  100,
				Name: []byte("user.test"),
			}).
			Return(int64(1), nil).
			Once()

		mockQuerier.EXPECT().
			TouchInodeCtime(mock.Anything, int64(100)).
			Return(nil).
			Once()

		err := mgr.RemoveXattr(ctx, 100, []byte("user.test"))
		require.NoError(t, err)
	})
}

// TestRemoveXattr_UserNamespaceAdmission 验证第 3 步 user.* 命名空间文件类型准入控制。
func TestRemoveXattr_UserNamespaceAdmission(t *testing.T) {
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

		// user.* 命名空间拦截：符号链接、字符设备、块设备、FIFO 返回 ErrPerm（对齐 Linux xattr_permission 写掩码行为）
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
			xName:       []byte("custom.ipc_tag"),
			mode:        meta.S_IFIFO | 0o660,
			expectedErr: nil,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, mockStore, mockQuerier, mgr := setupRemoveXattrTest(t)
			mockRemoveXattrExecTx(mockStore, mockQuerier)

			mockQuerier.EXPECT().
				GetInodeForUpdate(mock.Anything, int64(10)).
				Return(db.Inode{
					Ino:   10,
					Mode:  int32(tc.mode),
					Nlink: 1,
				}, nil).
				Once()

			if tc.expectedErr == nil {
				mockQuerier.EXPECT().
					DeleteInodeXattr(mock.Anything, db.DeleteInodeXattrParams{
						Ino:  10,
						Name: tc.xName,
					}).
					Return(int64(1), nil).
					Once()

				mockQuerier.EXPECT().
					TouchInodeCtime(mock.Anything, int64(10)).
					Return(nil).
					Once()
			}

			err := mgr.RemoveXattr(ctx, 10, tc.xName)
			if tc.expectedErr == nil {
				require.NoError(t, err)
			} else {
				require.ErrorIs(t, err, tc.expectedErr)
				// 准入拦截先于删除操作，确保不调用 DeleteInodeXattr
				mockQuerier.AssertNotCalled(t, "DeleteInodeXattr", mock.Anything, mock.Anything)
			}
		})
	}
}

// TestRemoveXattr_ExecutionAndTouchCtime 验证第 4 步删除属性、受影响行数判定及 ctime 刷新。
func TestRemoveXattr_ExecutionAndTouchCtime(t *testing.T) {
	t.Run("成功删除扩展属性并刷新 ctime", func(t *testing.T) {
		ctx, mockStore, mockQuerier, mgr := setupRemoveXattrTest(t)
		mockRemoveXattrExecTx(mockStore, mockQuerier)

		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(42)).
			Return(db.Inode{
				Ino:   42,
				Mode:  int32(meta.S_IFREG | 0o644),
				Nlink: 1,
			}, nil).
			Once()

		mockQuerier.EXPECT().
			DeleteInodeXattr(mock.Anything, db.DeleteInodeXattrParams{
				Ino:  42,
				Name: []byte("user.test"),
			}).
			Return(int64(1), nil).
			Once()

		mockQuerier.EXPECT().
			TouchInodeCtime(mock.Anything, int64(42)).
			Return(nil).
			Once()

		err := mgr.RemoveXattr(ctx, 42, []byte("user.test"))
		require.NoError(t, err)
	})

	t.Run("属性不存在时返回 ErrNoData (ENODATA) 且不刷新 ctime", func(t *testing.T) {
		ctx, mockStore, mockQuerier, mgr := setupRemoveXattrTest(t)
		mockRemoveXattrExecTx(mockStore, mockQuerier)

		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(42)).
			Return(db.Inode{
				Ino:   42,
				Mode:  int32(meta.S_IFREG | 0o644),
				Nlink: 1,
			}, nil).
			Once()

		mockQuerier.EXPECT().
			DeleteInodeXattr(mock.Anything, db.DeleteInodeXattrParams{
				Ino:  42,
				Name: []byte("user.nonexistent"),
			}).
			Return(int64(0), nil).
			Once()

		err := mgr.RemoveXattr(ctx, 42, []byte("user.nonexistent"))
		require.ErrorIs(t, err, meta.ErrNoData)

		// 属性不存在时不得刷新 ctime
		mockQuerier.AssertNotCalled(t, "TouchInodeCtime", mock.Anything, mock.Anything)
	})

	t.Run("成功删除非 UTF-8 原始字节名称扩展属性", func(t *testing.T) {
		ctx, mockStore, mockQuerier, mgr := setupRemoveXattrTest(t)
		mockRemoveXattrExecTx(mockStore, mockQuerier)

		rawName := []byte{0xff, 0xfe, 0x80, 'b'}
		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(42)).
			Return(db.Inode{
				Ino:   42,
				Mode:  int32(meta.S_IFREG | 0o644),
				Nlink: 1,
			}, nil).
			Once()

		mockQuerier.EXPECT().
			DeleteInodeXattr(mock.Anything, db.DeleteInodeXattrParams{
				Ino:  42,
				Name: rawName,
			}).
			Return(int64(1), nil).
			Once()

		mockQuerier.EXPECT().
			TouchInodeCtime(mock.Anything, int64(42)).
			Return(nil).
			Once()

		err := mgr.RemoveXattr(ctx, 42, rawName)
		require.NoError(t, err)
	})

	t.Run("成功删除包含斜杠的扩展属性名称", func(t *testing.T) {
		ctx, mockStore, mockQuerier, mgr := setupRemoveXattrTest(t)
		mockRemoveXattrExecTx(mockStore, mockQuerier)

		nameWithSlash := []byte("user.mime/type")
		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(42)).
			Return(db.Inode{
				Ino:   42,
				Mode:  int32(meta.S_IFREG | 0o644),
				Nlink: 1,
			}, nil).
			Once()

		mockQuerier.EXPECT().
			DeleteInodeXattr(mock.Anything, db.DeleteInodeXattrParams{
				Ino:  42,
				Name: nameWithSlash,
			}).
			Return(int64(1), nil).
			Once()

		mockQuerier.EXPECT().
			TouchInodeCtime(mock.Anything, int64(42)).
			Return(nil).
			Once()

		err := mgr.RemoveXattr(ctx, 42, nameWithSlash)
		require.NoError(t, err)
	})
}

// TestRemoveXattr_DatabaseErrors 验证底层数据库错误透传。
func TestRemoveXattr_DatabaseErrors(t *testing.T) {
	t.Run("GetInodeForUpdate 数据库错误", func(t *testing.T) {
		ctx, mockStore, mockQuerier, mgr := setupRemoveXattrTest(t)
		mockRemoveXattrExecTx(mockStore, mockQuerier)

		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(1)).
			Return(db.Inode{}, errors.New("db query failed")).
			Once()

		err := mgr.RemoveXattr(ctx, 1, []byte("user.t"))
		require.Error(t, err)
		assert.Contains(t, err.Error(), "get inode for update")
	})

	t.Run("DeleteInodeXattr 数据库错误", func(t *testing.T) {
		ctx, mockStore, mockQuerier, mgr := setupRemoveXattrTest(t)
		mockRemoveXattrExecTx(mockStore, mockQuerier)

		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(1)).
			Return(db.Inode{Ino: 1, Mode: int32(meta.S_IFREG | 0o644), Nlink: 1}, nil).
			Once()

		mockQuerier.EXPECT().
			DeleteInodeXattr(mock.Anything, mock.Anything).
			Return(int64(0), errors.New("db delete failed")).
			Once()

		err := mgr.RemoveXattr(ctx, 1, []byte("user.t"))
		require.Error(t, err)
		assert.Contains(t, err.Error(), "delete inode xattr")
	})

	t.Run("TouchInodeCtime 数据库错误", func(t *testing.T) {
		ctx, mockStore, mockQuerier, mgr := setupRemoveXattrTest(t)
		mockRemoveXattrExecTx(mockStore, mockQuerier)

		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(1)).
			Return(db.Inode{Ino: 1, Mode: int32(meta.S_IFREG | 0o644), Nlink: 1}, nil).
			Once()

		mockQuerier.EXPECT().
			DeleteInodeXattr(mock.Anything, mock.Anything).
			Return(int64(1), nil).
			Once()

		mockQuerier.EXPECT().
			TouchInodeCtime(mock.Anything, int64(1)).
			Return(errors.New("db touch ctime failed")).
			Once()

		err := mgr.RemoveXattr(ctx, 1, []byte("user.t"))
		require.Error(t, err)
		assert.Contains(t, err.Error(), "touch inode ctime")
	})
}

// TestRemoveXattr_OrphanReclamationCascade 验证孤儿节点支持及最终被物理回收时的级联清理机制 (ON DELETE CASCADE)。
func TestRemoveXattr_OrphanReclamationCascade(t *testing.T) {
	ctx := context.Background()

	t.Run("孤儿节点在解链后仍可通过 RemoveXattr 独立移除其扩展属性", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)
		mockRemoveXattrExecTx(mockStore, mockQuerier)

		orphanIno := uint64(200)
		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(orphanIno)).
			Return(db.Inode{
				Ino:   int64(orphanIno),
				Mode:  int32(meta.S_IFREG | 0o644),
				Nlink: 0, // 已解链孤儿节点
			}, nil).
			Once()

		mockQuerier.EXPECT().
			DeleteInodeXattr(mock.Anything, db.DeleteInodeXattrParams{
				Ino:  int64(orphanIno),
				Name: []byte("user.custom_attr"),
			}).
			Return(int64(1), nil).
			Once()

		mockQuerier.EXPECT().
			TouchInodeCtime(mock.Anything, int64(orphanIno)).
			Return(nil).
			Once()

		err := mgr.RemoveXattr(ctx, orphanIno, []byte("user.custom_attr"))
		require.NoError(t, err)
	})

	t.Run("孤儿节点在最后一个打开句柄 Release 回收时由 DeleteInode 触发数据库外键级联删除 (ON DELETE CASCADE)", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		// 模拟写事务
		mockStore.EXPECT().
			ExecTxWithOptions(mock.Anything, mock.Anything).
			RunAndReturn(func(ctx context.Context, fn func(db.Querier) error, opts ...tx.Option) error {
				return fn(mockQuerier)
			}).
			Once()

		orphanIno := uint64(300)
		fh := uint64(10)

		// 1. GetInodeForUpdate 获取排他锁
		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(orphanIno)).
			Return(db.Inode{
				Ino:   int64(orphanIno),
				Mode:  int32(meta.S_IFREG | 0o644),
				Nlink: 0, // 孤儿节点
			}, nil).
			Once()

		// 2. 删除文件句柄
		mockQuerier.EXPECT().
			DeleteFileHandle(mock.Anything, db.DeleteFileHandleParams{
				Fh:  int64(fh),
				Ino: int64(orphanIno),
			}).
			Return(int64(fh), nil).
			Once()

		// 3. 检查是否还有剩余句柄
		mockQuerier.EXPECT().
			HasFileHandlesByIno(mock.Anything, int64(orphanIno)).
			Return(false, nil).
			Once()

		mockQuerier.EXPECT().
			DeleteChunksByIno(mock.Anything, int64(orphanIno)).
			Return(nil, nil).
			Once()

		// 4. 无剩余句柄，触发 DeleteInode 物理回收
		// 数据库级联约束：fk_inode_xattrs_ino FOREIGN KEY (ino) REFERENCES inodes(ino) ON DELETE CASCADE
		// 确保 DeleteInode 自动级联清理 inode_xattrs，无需领域层逐个调用 DeleteInodeXattr
		mockQuerier.EXPECT().
			DeleteInode(mock.Anything, int64(orphanIno)).
			Return(nil).
			Once()

		err := mgr.Release(ctx, meta.ReleaseParams{
			Ino: orphanIno,
			Fh:  fh,
		})
		require.NoError(t, err)
	})
}
