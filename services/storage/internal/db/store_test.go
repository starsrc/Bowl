package db_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/starsrc/bowl/services/storage/internal/db"
	dbmock "github.com/starsrc/bowl/services/storage/internal/db/mock"
)

// InodeAttrQuerier 提取 Inode 悲观锁读取与属性更新接口契约。
type InodeAttrQuerier interface {
	GetInodeForUpdate(ctx context.Context, ino int64) (db.Inode, error)
	UpdateInodeAttr(ctx context.Context, arg db.UpdateInodeAttrParams) (db.Inode, error)
	IncrementInodeNlinkAndTouch(ctx context.Context, ino int64) error
}

// SymlinkQuerier 提取符号链接创建与读取接口契约。
type SymlinkQuerier interface {
	CreateSymlinkInode(ctx context.Context, arg db.CreateSymlinkInodeParams) (db.Inode, error)
	GetSymlinkTarget(ctx context.Context, ino int64) (db.GetSymlinkTargetRow, error)
}

// LinkQuerier 提取硬链接原子递增与 ctime 刷新接口契约。
type LinkQuerier interface {
	IncrementInodeNlinkAndTouchCtime(ctx context.Context, ino int64) (db.Inode, error)
}

// UnlinkRmdirQuerier 提取 Unlink 与 Rmdir 目录项解链与计数更新接口契约。
type UnlinkRmdirQuerier interface {
	GetEntry(ctx context.Context, arg db.GetEntryParams) (db.Entry, error)
	DeleteEntry(ctx context.Context, arg db.DeleteEntryParams) error
	HasChildEntries(ctx context.Context, parentIno int64) (bool, error)
	DecrementInodeNlinkAndTouchCtime(ctx context.Context, ino int64) (db.Inode, error)
	ClearInodeNlinkAndTouchCtime(ctx context.Context, ino int64) (db.Inode, error)
	DecrementInodeNlinkAndTouch(ctx context.Context, ino int64) error
}

// RenameQuerier 提取 Rename 原子重命名、更新目录项与递归环检测接口契约。
type RenameQuerier interface {
	UpdateEntry(ctx context.Context, arg db.UpdateEntryParams) error
	UpdateEntryIno(ctx context.Context, arg db.UpdateEntryInoParams) error
	TouchInodeCtime(ctx context.Context, ino int64) error
	IsSubdirectory(ctx context.Context, arg db.IsSubdirectoryParams) (bool, error)
}

// FileHandleQuerier 提取文件句柄创建与持久化接口契约。
type FileHandleQuerier interface {
	CreateFileHandle(ctx context.Context, arg db.CreateFileHandleParams) (db.FileHandle, error)
	HasFileHandlesByIno(ctx context.Context, ino int64) (bool, error)
	GetFileHandle(ctx context.Context, arg db.GetFileHandleParams) (db.FileHandle, error)
	DeleteFileHandle(ctx context.Context, arg db.DeleteFileHandleParams) (int64, error)
}

// InodeLifecycleQuerier 提取 Inode 生命周期物理回收接口契约。
type InodeLifecycleQuerier interface {
	DeleteInode(ctx context.Context, ino int64) error
}

// ReadDirQuerier 提取目录遍历与父节点查询接口契约。
type ReadDirQuerier interface {
	GetParentInode(ctx context.Context, ino int64) (db.Inode, error)
	ListDirEntries(ctx context.Context, arg db.ListDirEntriesParams) ([]db.ListDirEntriesRow, error)
}

// XattrQuerier 提取扩展属性存取与配额查询接口契约。
type XattrQuerier interface {
	GetInodeXattr(ctx context.Context, arg db.GetInodeXattrParams) (db.InodeXattr, error)
	UpsertInodeXattr(ctx context.Context, arg db.UpsertInodeXattrParams) error
	GetInodeXattrNamesTotalBytes(ctx context.Context, ino int64) (int64, error)
	ListInodeXattrNames(ctx context.Context, ino int64) ([][]byte, error)
	DeleteInodeXattr(ctx context.Context, arg db.DeleteInodeXattrParams) (int64, error)
}

// FileLockQuerier 提取文件锁、阻塞等待者与客户端会话租约存取接口契约。
type FileLockQuerier interface {
	DeleteExpiredClientSessions(ctx context.Context) error
	UpsertClientSession(ctx context.Context, arg db.UpsertClientSessionParams) (db.ClientSession, error)
	GetFirstConflictingFileLock(ctx context.Context, arg db.GetFirstConflictingFileLockParams) (db.FileLock, error)
	ListActiveFileLocksByInoAndFlags(ctx context.Context, arg db.ListActiveFileLocksByInoAndFlagsParams) ([]db.FileLock, error)
	CreateFileLock(ctx context.Context, arg db.CreateFileLockParams) (db.FileLock, error)
	DeleteFileLocksByIDs(ctx context.Context, ids []int64) error
	DeletePosixFileLocksBySessionAndOwner(ctx context.Context, arg db.DeletePosixFileLocksBySessionAndOwnerParams) (int64, error)
	DeletePosixFileLocksByOwner(ctx context.Context, arg db.DeletePosixFileLocksByOwnerParams) (int64, error)
	DeleteFlockFileLocksBySessionAndFh(ctx context.Context, arg db.DeleteFlockFileLocksBySessionAndFhParams) (int64, error)
	DeleteFlockFileLocksByFh(ctx context.Context, arg db.DeleteFlockFileLocksByFhParams) (int64, error)
	GetFileLockWaiter(ctx context.Context, arg db.GetFileLockWaiterParams) (db.FileLockWaiter, error)
	ListPendingFileLockWaitersByInoAndFlags(ctx context.Context, arg db.ListPendingFileLockWaitersByInoAndFlagsParams) ([]db.FileLockWaiter, error)
	UpsertFileLockWaiter(ctx context.Context, arg db.UpsertFileLockWaiterParams) (db.FileLockWaiter, error)
	DeleteFileLockWaiter(ctx context.Context, arg db.DeleteFileLockWaiterParams) (int64, error)
}

// 静态验证 db.Querier、db.Store 与 *db.Queries 完备实现各接口契约。
var (
	_ InodeAttrQuerier = (db.Querier)(nil)
	_ InodeAttrQuerier = (db.Store)(nil)
	_ InodeAttrQuerier = (*db.Queries)(nil)

	_ SymlinkQuerier = (db.Querier)(nil)
	_ SymlinkQuerier = (db.Store)(nil)
	_ SymlinkQuerier = (*db.Queries)(nil)

	_ LinkQuerier = (db.Querier)(nil)
	_ LinkQuerier = (db.Store)(nil)
	_ LinkQuerier = (*db.Queries)(nil)

	_ UnlinkRmdirQuerier = (db.Querier)(nil)
	_ UnlinkRmdirQuerier = (db.Store)(nil)
	_ UnlinkRmdirQuerier = (*db.Queries)(nil)

	_ RenameQuerier = (db.Querier)(nil)
	_ RenameQuerier = (db.Store)(nil)
	_ RenameQuerier = (*db.Queries)(nil)

	_ FileHandleQuerier = (db.Querier)(nil)
	_ FileHandleQuerier = (db.Store)(nil)
	_ FileHandleQuerier = (*db.Queries)(nil)

	_ InodeLifecycleQuerier = (db.Querier)(nil)
	_ InodeLifecycleQuerier = (db.Store)(nil)
	_ InodeLifecycleQuerier = (*db.Queries)(nil)

	_ ReadDirQuerier = (db.Querier)(nil)
	_ ReadDirQuerier = (db.Store)(nil)
	_ ReadDirQuerier = (*db.Queries)(nil)

	_ XattrQuerier = (db.Querier)(nil)
	_ XattrQuerier = (db.Store)(nil)
	_ XattrQuerier = (*db.Queries)(nil)

	_ FileLockQuerier = (db.Querier)(nil)
	_ FileLockQuerier = (db.Store)(nil)
	_ FileLockQuerier = (*db.Queries)(nil)
)

func TestNewStore(t *testing.T) {
	store := db.NewStore(nil)
	assert.NotNil(t, store)

	var _ db.Store = store
}

func TestModule(t *testing.T) {
	assert.NotNil(t, db.Module)
}

func TestQuerierInterface(t *testing.T) {
	var _ db.Querier = (*db.Queries)(nil)
	var _ db.Querier = (db.Store)(nil)
}

func TestStore_GetInodeForUpdateBehavior(t *testing.T) {
	ctx := context.Background()

	t.Run("successfully retrieve inode for update", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		var store db.Store = mockStore

		expected := db.Inode{
			Ino:        42,
			Generation: 1,
			Mode:       0o100644,
			Size:       1024,
			Nlink:      1,
			Uid:        1000,
			Gid:        1000,
		}

		mockStore.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(42)).
			Return(expected, nil).
			Once()

		result, err := store.GetInodeForUpdate(ctx, 42)
		require.NoError(t, err)
		assert.Equal(t, expected, result)
	})

	t.Run("return pgx.ErrNoRows when inode not found", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		var store db.Store = mockStore

		mockStore.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(999)).
			Return(db.Inode{}, pgx.ErrNoRows).
			Once()

		_, err := store.GetInodeForUpdate(ctx, 999)
		require.Error(t, err)
		assert.True(t, errors.Is(err, pgx.ErrNoRows))
	})
}

func TestStore_UpdateInodeAttrBehavior(t *testing.T) {
	ctx := context.Background()
	now := time.Now().Truncate(time.Microsecond)

	t.Run("successfully update inode attributes", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		var store db.Store = mockStore

		params := db.UpdateInodeAttrParams{
			Ino:         42,
			Mode:        0o100755,
			Size:        2048,
			Uid:         1001,
			Gid:         1001,
			SetAtimeNow: false,
			Atime:       pgtype.Timestamptz{Time: now, Valid: true},
			SetMtimeNow: true,
			Mtime:       pgtype.Timestamptz{Valid: false},
		}

		expected := db.Inode{
			Ino:        42,
			Generation: 1,
			Mode:       0o100755,
			Size:       2048,
			Nlink:      1,
			Uid:        1001,
			Gid:        1001,
			Atime:      pgtype.Timestamptz{Time: now, Valid: true},
			Mtime:      pgtype.Timestamptz{Time: now, Valid: true},
			Ctime:      pgtype.Timestamptz{Time: now, Valid: true},
		}

		mockStore.EXPECT().
			UpdateInodeAttr(mock.Anything, params).
			Return(expected, nil).
			Once()

		result, err := store.UpdateInodeAttr(ctx, params)
		require.NoError(t, err)
		assert.Equal(t, expected, result)
	})

	t.Run("return error on database failure during update", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		var store db.Store = mockStore

		params := db.UpdateInodeAttrParams{
			Ino:  42,
			Mode: 0o100755,
		}

		expectedErr := errors.New("connection reset by peer")
		mockStore.EXPECT().
			UpdateInodeAttr(mock.Anything, params).
			Return(db.Inode{}, expectedErr).
			Once()

		_, err := store.UpdateInodeAttr(ctx, params)
		require.Error(t, err)
		assert.True(t, errors.Is(err, expectedErr))
	})
}

func TestQuerierInterface_MknodOperators(t *testing.T) {
	type mknodQuerier interface {
		CreateInode(ctx context.Context, arg db.CreateInodeParams) (db.Inode, error)
		CreateEntry(ctx context.Context, arg db.CreateEntryParams) error
		TouchInodeTimestamps(ctx context.Context, ino int64) error
	}

	var _ mknodQuerier = (db.Querier)(nil)
	var _ mknodQuerier = (db.Store)(nil)
}

func TestQuerier_MknodOperators(t *testing.T) {
	ctx := context.Background()

	t.Run("CreateInode 算子调用与参数透传", func(t *testing.T) {
		t.Run("成功创建 Inode 并返回数据实体", func(t *testing.T) {
			mockQuerier := dbmock.NewMockQuerier(t)
			params := db.CreateInodeParams{
				Mode:  0o100644,
				Nlink: 1,
				Uid:   1000,
				Gid:   1000,
				Rdev:  0,
			}
			expected := db.Inode{
				Ino:   100,
				Mode:  0o100644,
				Nlink: 1,
				Uid:   1000,
				Gid:   1000,
				Rdev:  0,
			}
			mockQuerier.EXPECT().CreateInode(ctx, params).Return(expected, nil).Once()

			var q db.Querier = mockQuerier
			actual, err := q.CreateInode(ctx, params)
			require.NoError(t, err)
			assert.Equal(t, expected, actual)
		})

		t.Run("底层执行失败透传错误", func(t *testing.T) {
			mockQuerier := dbmock.NewMockQuerier(t)
			params := db.CreateInodeParams{
				Mode:  0o100644,
				Nlink: 1,
				Uid:   1000,
				Gid:   1000,
				Rdev:  0,
			}
			expectedErr := errors.New("database insert error")
			mockQuerier.EXPECT().CreateInode(ctx, params).Return(db.Inode{}, expectedErr).Once()

			var q db.Querier = mockQuerier
			_, err := q.CreateInode(ctx, params)
			assert.ErrorIs(t, err, expectedErr)
		})
	})

	t.Run("CreateEntry 算子调用与参数透传", func(t *testing.T) {
		t.Run("成功插入目录项条目", func(t *testing.T) {
			mockQuerier := dbmock.NewMockQuerier(t)
			params := db.CreateEntryParams{
				ParentIno: 1,
				Name:      []byte("test.txt"),
				Ino:       100,
			}
			mockQuerier.EXPECT().CreateEntry(ctx, params).Return(nil).Once()

			var q db.Querier = mockQuerier
			err := q.CreateEntry(ctx, params)
			require.NoError(t, err)
		})

		t.Run("底层执行失败透传错误", func(t *testing.T) {
			mockQuerier := dbmock.NewMockQuerier(t)
			params := db.CreateEntryParams{
				ParentIno: 1,
				Name:      []byte("test.txt"),
				Ino:       100,
			}
			expectedErr := errors.New("unique violation")
			mockQuerier.EXPECT().CreateEntry(ctx, params).Return(expectedErr).Once()

			var q db.Querier = mockQuerier
			err := q.CreateEntry(ctx, params)
			assert.ErrorIs(t, err, expectedErr)
		})
	})

	t.Run("TouchInodeTimestamps 算子调用与参数透传", func(t *testing.T) {
		t.Run("成功刷新 Inode 时间戳", func(t *testing.T) {
			mockQuerier := dbmock.NewMockQuerier(t)
			ino := int64(42)
			mockQuerier.EXPECT().TouchInodeTimestamps(ctx, ino).Return(nil).Once()

			var q db.Querier = mockQuerier
			err := q.TouchInodeTimestamps(ctx, ino)
			require.NoError(t, err)
		})

		t.Run("底层执行失败透传错误", func(t *testing.T) {
			mockQuerier := dbmock.NewMockQuerier(t)
			ino := int64(42)
			expectedErr := errors.New("update timestamps error")
			mockQuerier.EXPECT().TouchInodeTimestamps(ctx, ino).Return(expectedErr).Once()

			var q db.Querier = mockQuerier
			err := q.TouchInodeTimestamps(ctx, ino)
			assert.ErrorIs(t, err, expectedErr)
		})
	})

	t.Run("IncrementInodeNlinkAndTouch 算子调用与参数透传", func(t *testing.T) {
		t.Run("成功递增硬链接计数并刷新时间戳", func(t *testing.T) {
			mockQuerier := dbmock.NewMockQuerier(t)
			ino := int64(42)
			mockQuerier.EXPECT().IncrementInodeNlinkAndTouch(ctx, ino).Return(nil).Once()

			var q db.Querier = mockQuerier
			err := q.IncrementInodeNlinkAndTouch(ctx, ino)
			require.NoError(t, err)
		})

		t.Run("底层执行失败透传错误", func(t *testing.T) {
			mockQuerier := dbmock.NewMockQuerier(t)
			ino := int64(42)
			expectedErr := errors.New("increment nlink error")
			mockQuerier.EXPECT().IncrementInodeNlinkAndTouch(ctx, ino).Return(expectedErr).Once()

			var q db.Querier = mockQuerier
			err := q.IncrementInodeNlinkAndTouch(ctx, ino)
			assert.ErrorIs(t, err, expectedErr)
		})
	})

	t.Run("CreateSymlinkInode 算子调用与参数透传", func(t *testing.T) {
		t.Run("成功插入符号链接 Inode", func(t *testing.T) {
			mockQuerier := dbmock.NewMockQuerier(t)
			params := db.CreateSymlinkInodeParams{
				Mode:          0o120777,
				Size:          11,
				Nlink:         1,
				Uid:           1000,
				Gid:           1000,
				Rdev:          0,
				SymlinkTarget: []byte("/var/target"),
			}
			expected := db.Inode{Ino: 50, Mode: params.Mode, Size: params.Size, SymlinkTarget: params.SymlinkTarget}
			mockQuerier.EXPECT().CreateSymlinkInode(ctx, params).Return(expected, nil).Once()

			var q db.Querier = mockQuerier
			res, err := q.CreateSymlinkInode(ctx, params)
			require.NoError(t, err)
			assert.Equal(t, expected, res)
		})

		t.Run("底层执行失败透传错误", func(t *testing.T) {
			mockQuerier := dbmock.NewMockQuerier(t)
			params := db.CreateSymlinkInodeParams{Mode: 0o120777}
			expectedErr := errors.New("check constraint violated")
			mockQuerier.EXPECT().CreateSymlinkInode(ctx, params).Return(db.Inode{}, expectedErr).Once()

			var q db.Querier = mockQuerier
			_, err := q.CreateSymlinkInode(ctx, params)
			assert.ErrorIs(t, err, expectedErr)
		})
	})

	t.Run("GetSymlinkTarget 算子调用与参数透传", func(t *testing.T) {
		t.Run("成功获取符号链接目标路径", func(t *testing.T) {
			mockQuerier := dbmock.NewMockQuerier(t)
			expected := db.GetSymlinkTargetRow{
				Mode:          0o120777,
				SymlinkTarget: []byte("/etc/hosts"),
			}
			mockQuerier.EXPECT().GetSymlinkTarget(ctx, int64(100)).Return(expected, nil).Once()

			var q db.Querier = mockQuerier
			res, err := q.GetSymlinkTarget(ctx, 100)
			require.NoError(t, err)
			assert.Equal(t, expected, res)
		})

		t.Run("底层执行失败透传错误", func(t *testing.T) {
			mockQuerier := dbmock.NewMockQuerier(t)
			expectedErr := errors.New("query failed")
			mockQuerier.EXPECT().GetSymlinkTarget(ctx, int64(100)).Return(db.GetSymlinkTargetRow{}, expectedErr).Once()

			var q db.Querier = mockQuerier
			_, err := q.GetSymlinkTarget(ctx, 100)
			assert.ErrorIs(t, err, expectedErr)
		})
	})

	t.Run("IncrementInodeNlinkAndTouchCtime 算子调用与参数透传", func(t *testing.T) {
		t.Run("成功递增硬链接计数并仅刷新 ctime", func(t *testing.T) {
			mockQuerier := dbmock.NewMockQuerier(t)
			ino := int64(42)
			expected := db.Inode{Ino: ino, Nlink: 2}
			mockQuerier.EXPECT().IncrementInodeNlinkAndTouchCtime(ctx, ino).Return(expected, nil).Once()

			var q db.Querier = mockQuerier
			res, err := q.IncrementInodeNlinkAndTouchCtime(ctx, ino)
			require.NoError(t, err)
			assert.Equal(t, expected, res)
		})

		t.Run("底层执行失败透传错误", func(t *testing.T) {
			mockQuerier := dbmock.NewMockQuerier(t)
			ino := int64(42)
			expectedErr := errors.New("update ctime error")
			mockQuerier.EXPECT().IncrementInodeNlinkAndTouchCtime(ctx, ino).Return(db.Inode{}, expectedErr).Once()

			var q db.Querier = mockQuerier
			_, err := q.IncrementInodeNlinkAndTouchCtime(ctx, ino)
			assert.ErrorIs(t, err, expectedErr)
		})
	})

	t.Run("GetEntry 算子调用与参数透传", func(t *testing.T) {
		t.Run("成功获取目录项", func(t *testing.T) {
			mockQuerier := dbmock.NewMockQuerier(t)
			params := db.GetEntryParams{ParentIno: 1, Name: []byte("file.txt")}
			expected := db.Entry{ParentIno: 1, Name: []byte("file.txt"), Ino: 100}
			mockQuerier.EXPECT().GetEntry(ctx, params).Return(expected, nil).Once()

			var q db.Querier = mockQuerier
			res, err := q.GetEntry(ctx, params)
			require.NoError(t, err)
			assert.Equal(t, expected, res)
		})

		t.Run("底层执行失败透传错误", func(t *testing.T) {
			mockQuerier := dbmock.NewMockQuerier(t)
			params := db.GetEntryParams{ParentIno: 1, Name: []byte("missing")}
			expectedErr := errors.New("not found")
			mockQuerier.EXPECT().GetEntry(ctx, params).Return(db.Entry{}, expectedErr).Once()

			var q db.Querier = mockQuerier
			_, err := q.GetEntry(ctx, params)
			assert.ErrorIs(t, err, expectedErr)
		})
	})

	t.Run("DeleteEntry 算子调用与参数透传", func(t *testing.T) {
		t.Run("成功删除目录项", func(t *testing.T) {
			mockQuerier := dbmock.NewMockQuerier(t)
			params := db.DeleteEntryParams{ParentIno: 1, Name: []byte("file.txt")}
			mockQuerier.EXPECT().DeleteEntry(ctx, params).Return(nil).Once()

			var q db.Querier = mockQuerier
			err := q.DeleteEntry(ctx, params)
			require.NoError(t, err)
		})

		t.Run("底层执行失败透传错误", func(t *testing.T) {
			mockQuerier := dbmock.NewMockQuerier(t)
			params := db.DeleteEntryParams{ParentIno: 1, Name: []byte("file.txt")}
			expectedErr := errors.New("delete error")
			mockQuerier.EXPECT().DeleteEntry(ctx, params).Return(expectedErr).Once()

			var q db.Querier = mockQuerier
			err := q.DeleteEntry(ctx, params)
			assert.ErrorIs(t, err, expectedErr)
		})
	})

	t.Run("HasChildEntries 算子调用与参数透传", func(t *testing.T) {
		t.Run("目录包含子项返回 true", func(t *testing.T) {
			mockQuerier := dbmock.NewMockQuerier(t)
			mockQuerier.EXPECT().HasChildEntries(ctx, int64(10)).Return(true, nil).Once()

			var q db.Querier = mockQuerier
			res, err := q.HasChildEntries(ctx, 10)
			require.NoError(t, err)
			assert.True(t, res)
		})

		t.Run("空目录返回 false", func(t *testing.T) {
			mockQuerier := dbmock.NewMockQuerier(t)
			mockQuerier.EXPECT().HasChildEntries(ctx, int64(10)).Return(false, nil).Once()

			var q db.Querier = mockQuerier
			res, err := q.HasChildEntries(ctx, 10)
			require.NoError(t, err)
			assert.False(t, res)
		})

		t.Run("底层执行失败透传错误", func(t *testing.T) {
			mockQuerier := dbmock.NewMockQuerier(t)
			expectedErr := errors.New("exists query error")
			mockQuerier.EXPECT().HasChildEntries(ctx, int64(10)).Return(false, expectedErr).Once()

			var q db.Querier = mockQuerier
			_, err := q.HasChildEntries(ctx, 10)
			assert.ErrorIs(t, err, expectedErr)
		})
	})

	t.Run("DecrementInodeNlinkAndTouchCtime 算子调用与参数透传", func(t *testing.T) {
		t.Run("成功递减子节点硬链接数并刷新 ctime", func(t *testing.T) {
			mockQuerier := dbmock.NewMockQuerier(t)
			ino := int64(100)
			expected := db.Inode{Ino: ino, Nlink: 1}
			mockQuerier.EXPECT().DecrementInodeNlinkAndTouchCtime(ctx, ino).Return(expected, nil).Once()

			var q db.Querier = mockQuerier
			res, err := q.DecrementInodeNlinkAndTouchCtime(ctx, ino)
			require.NoError(t, err)
			assert.Equal(t, expected, res)
		})

		t.Run("底层执行失败透传错误", func(t *testing.T) {
			mockQuerier := dbmock.NewMockQuerier(t)
			ino := int64(100)
			expectedErr := errors.New("decrement error")
			mockQuerier.EXPECT().DecrementInodeNlinkAndTouchCtime(ctx, ino).Return(db.Inode{}, expectedErr).Once()

			var q db.Querier = mockQuerier
			_, err := q.DecrementInodeNlinkAndTouchCtime(ctx, ino)
			assert.ErrorIs(t, err, expectedErr)
		})
	})

	t.Run("ClearInodeNlinkAndTouchCtime 算子调用与参数透传", func(t *testing.T) {
		t.Run("成功将目录硬链接数置 0 并刷新 ctime", func(t *testing.T) {
			mockQuerier := dbmock.NewMockQuerier(t)
			ino := int64(200)
			expected := db.Inode{Ino: ino, Nlink: 0}
			mockQuerier.EXPECT().ClearInodeNlinkAndTouchCtime(ctx, ino).Return(expected, nil).Once()

			var q db.Querier = mockQuerier
			res, err := q.ClearInodeNlinkAndTouchCtime(ctx, ino)
			require.NoError(t, err)
			assert.Equal(t, expected, res)
		})

		t.Run("底层执行失败透传错误", func(t *testing.T) {
			mockQuerier := dbmock.NewMockQuerier(t)
			ino := int64(200)
			expectedErr := errors.New("clear error")
			mockQuerier.EXPECT().ClearInodeNlinkAndTouchCtime(ctx, ino).Return(db.Inode{}, expectedErr).Once()

			var q db.Querier = mockQuerier
			_, err := q.ClearInodeNlinkAndTouchCtime(ctx, ino)
			assert.ErrorIs(t, err, expectedErr)
		})
	})

	t.Run("DecrementInodeNlinkAndTouch 算子调用与参数透传", func(t *testing.T) {
		t.Run("成功递减父目录硬链接数并刷新 mtime/ctime", func(t *testing.T) {
			mockQuerier := dbmock.NewMockQuerier(t)
			ino := int64(1)
			mockQuerier.EXPECT().DecrementInodeNlinkAndTouch(ctx, ino).Return(nil).Once()

			var q db.Querier = mockQuerier
			err := q.DecrementInodeNlinkAndTouch(ctx, ino)
			require.NoError(t, err)
		})

		t.Run("底层执行失败透传错误", func(t *testing.T) {
			mockQuerier := dbmock.NewMockQuerier(t)
			ino := int64(1)
			expectedErr := errors.New("parent decrement error")
			mockQuerier.EXPECT().DecrementInodeNlinkAndTouch(ctx, ino).Return(expectedErr).Once()

			var q db.Querier = mockQuerier
			err := q.DecrementInodeNlinkAndTouch(ctx, ino)
			assert.ErrorIs(t, err, expectedErr)
		})
	})

	t.Run("UpdateEntry 算子调用与参数透传", func(t *testing.T) {
		t.Run("成功更新目录项位置与名称", func(t *testing.T) {
			mockQuerier := dbmock.NewMockQuerier(t)
			params := db.UpdateEntryParams{
				OldParentIno: 1,
				OldName:      []byte("old.txt"),
				NewParentIno: 2,
				NewName:      []byte("new.txt"),
			}
			mockQuerier.EXPECT().UpdateEntry(ctx, params).Return(nil).Once()

			var q db.Querier = mockQuerier
			err := q.UpdateEntry(ctx, params)
			require.NoError(t, err)
		})

		t.Run("底层执行失败透传错误", func(t *testing.T) {
			mockQuerier := dbmock.NewMockQuerier(t)
			params := db.UpdateEntryParams{
				OldParentIno: 1,
				OldName:      []byte("old.txt"),
				NewParentIno: 2,
				NewName:      []byte("new.txt"),
			}
			expectedErr := errors.New("update entry error")
			mockQuerier.EXPECT().UpdateEntry(ctx, params).Return(expectedErr).Once()

			var q db.Querier = mockQuerier
			err := q.UpdateEntry(ctx, params)
			assert.ErrorIs(t, err, expectedErr)
		})
	})

	t.Run("UpdateEntryIno 算子调用与参数透传", func(t *testing.T) {
		t.Run("成功原地更新目录项的目标 Ino", func(t *testing.T) {
			mockQuerier := dbmock.NewMockQuerier(t)
			params := db.UpdateEntryInoParams{
				ParentIno: 1,
				Name:      []byte("swap.txt"),
				Ino:       200,
			}
			mockQuerier.EXPECT().UpdateEntryIno(ctx, params).Return(nil).Once()

			var q db.Querier = mockQuerier
			err := q.UpdateEntryIno(ctx, params)
			require.NoError(t, err)
		})

		t.Run("底层执行失败透传错误", func(t *testing.T) {
			mockQuerier := dbmock.NewMockQuerier(t)
			params := db.UpdateEntryInoParams{
				ParentIno: 1,
				Name:      []byte("swap.txt"),
				Ino:       200,
			}
			expectedErr := errors.New("update entry ino error")
			mockQuerier.EXPECT().UpdateEntryIno(ctx, params).Return(expectedErr).Once()

			var q db.Querier = mockQuerier
			err := q.UpdateEntryIno(ctx, params)
			assert.ErrorIs(t, err, expectedErr)
		})
	})

	t.Run("TouchInodeCtime 算子调用与参数透传", func(t *testing.T) {
		t.Run("成功刷新 Inode ctime", func(t *testing.T) {
			mockQuerier := dbmock.NewMockQuerier(t)
			ino := int64(42)
			mockQuerier.EXPECT().TouchInodeCtime(ctx, ino).Return(nil).Once()

			var q db.Querier = mockQuerier
			err := q.TouchInodeCtime(ctx, ino)
			require.NoError(t, err)
		})

		t.Run("底层执行失败透传错误", func(t *testing.T) {
			mockQuerier := dbmock.NewMockQuerier(t)
			ino := int64(42)
			expectedErr := errors.New("touch ctime error")
			mockQuerier.EXPECT().TouchInodeCtime(ctx, ino).Return(expectedErr).Once()

			var q db.Querier = mockQuerier
			err := q.TouchInodeCtime(ctx, ino)
			assert.ErrorIs(t, err, expectedErr)
		})
	})

	t.Run("IsSubdirectory 算子调用与参数透传", func(t *testing.T) {
		t.Run("祖先链中包含目标目录返回 true", func(t *testing.T) {
			mockQuerier := dbmock.NewMockQuerier(t)
			params := db.IsSubdirectoryParams{ChildIno: 30, AncestorIno: 10}
			mockQuerier.EXPECT().IsSubdirectory(ctx, params).Return(true, nil).Once()

			var q db.Querier = mockQuerier
			res, err := q.IsSubdirectory(ctx, params)
			require.NoError(t, err)
			assert.True(t, res)
		})

		t.Run("非子目录关系返回 false", func(t *testing.T) {
			mockQuerier := dbmock.NewMockQuerier(t)
			params := db.IsSubdirectoryParams{ChildIno: 10, AncestorIno: 30}
			mockQuerier.EXPECT().IsSubdirectory(ctx, params).Return(false, nil).Once()

			var q db.Querier = mockQuerier
			res, err := q.IsSubdirectory(ctx, params)
			require.NoError(t, err)
			assert.False(t, res)
		})

		t.Run("底层执行失败透传错误", func(t *testing.T) {
			mockQuerier := dbmock.NewMockQuerier(t)
			params := db.IsSubdirectoryParams{ChildIno: 30, AncestorIno: 10}
			expectedErr := errors.New("recursive query failed")
			mockQuerier.EXPECT().IsSubdirectory(ctx, params).Return(false, expectedErr).Once()

			var q db.Querier = mockQuerier
			_, err := q.IsSubdirectory(ctx, params)
			assert.ErrorIs(t, err, expectedErr)
		})
	})

	t.Run("CreateFileHandle 算子调用与参数透传", func(t *testing.T) {
		now := time.Now()
		params := db.CreateFileHandleParams{
			Ino:   100,
			Flags: 0o2, // O_RDWR
		}
		expected := db.FileHandle{
			Fh:        1,
			Ino:       100,
			Flags:     0o2,
			CreatedAt: pgtype.Timestamptz{Time: now, Valid: true},
		}

		t.Run("成功创建 FileHandle 并返回数据实体", func(t *testing.T) {
			mockStore := dbmock.NewMockStore(t)
			var store db.Store = mockStore

			mockStore.EXPECT().
				CreateFileHandle(mock.Anything, params).
				Return(expected, nil).
				Once()

			result, err := store.CreateFileHandle(ctx, params)
			require.NoError(t, err)
			assert.Equal(t, expected, result)
		})

		t.Run("底层执行失败透传错误", func(t *testing.T) {
			mockStore := dbmock.NewMockStore(t)
			var store db.Store = mockStore

			expectedErr := errors.New("database sequence error")
			mockStore.EXPECT().
				CreateFileHandle(mock.Anything, params).
				Return(db.FileHandle{}, expectedErr).
				Once()

			_, err := store.CreateFileHandle(ctx, params)
			require.Error(t, err)
			assert.True(t, errors.Is(err, expectedErr))
		})
	})

	t.Run("HasFileHandlesByIno 算子调用与参数透传", func(t *testing.T) {
		const targetIno int64 = 42

		t.Run("成功查询并返回存在活跃句柄", func(t *testing.T) {
			mockStore := dbmock.NewMockStore(t)
			var store db.Store = mockStore

			mockStore.EXPECT().
				HasFileHandlesByIno(mock.Anything, targetIno).
				Return(true, nil).
				Once()

			exists, err := store.HasFileHandlesByIno(ctx, targetIno)
			require.NoError(t, err)
			assert.True(t, exists)
		})

		t.Run("成功查询并返回无活跃句柄", func(t *testing.T) {
			mockStore := dbmock.NewMockStore(t)
			var store db.Store = mockStore

			mockStore.EXPECT().
				HasFileHandlesByIno(mock.Anything, targetIno).
				Return(false, nil).
				Once()

			exists, err := store.HasFileHandlesByIno(ctx, targetIno)
			require.NoError(t, err)
			assert.False(t, exists)
		})

		t.Run("底层执行失败透传错误", func(t *testing.T) {
			mockStore := dbmock.NewMockStore(t)
			var store db.Store = mockStore

			expectedErr := errors.New("database query error")
			mockStore.EXPECT().
				HasFileHandlesByIno(mock.Anything, targetIno).
				Return(false, expectedErr).
				Once()

			_, err := store.HasFileHandlesByIno(ctx, targetIno)
			require.Error(t, err)
			assert.True(t, errors.Is(err, expectedErr))
		})
	})

	t.Run("GetFileHandle 算子调用与参数透传", func(t *testing.T) {
		now := time.Now()
		params := db.GetFileHandleParams{
			Fh:  10,
			Ino: 42,
		}
		expected := db.FileHandle{
			Fh:        10,
			Ino:       42,
			Flags:     0o2,
			CreatedAt: pgtype.Timestamptz{Time: now, Valid: true},
		}

		t.Run("成功查询并返回目标 FileHandle", func(t *testing.T) {
			mockStore := dbmock.NewMockStore(t)
			var store db.Store = mockStore

			mockStore.EXPECT().
				GetFileHandle(mock.Anything, params).
				Return(expected, nil).
				Once()

			result, err := store.GetFileHandle(ctx, params)
			require.NoError(t, err)
			assert.Equal(t, expected, result)
		})

		t.Run("目标句柄不存在返回 pgx.ErrNoRows", func(t *testing.T) {
			mockStore := dbmock.NewMockStore(t)
			var store db.Store = mockStore

			mockStore.EXPECT().
				GetFileHandle(mock.Anything, params).
				Return(db.FileHandle{}, pgx.ErrNoRows).
				Once()

			_, err := store.GetFileHandle(ctx, params)
			require.Error(t, err)
			assert.True(t, errors.Is(err, pgx.ErrNoRows))
		})

		t.Run("底层执行失败透传错误", func(t *testing.T) {
			mockStore := dbmock.NewMockStore(t)
			var store db.Store = mockStore

			expectedErr := errors.New("database connection failed")
			mockStore.EXPECT().
				GetFileHandle(mock.Anything, params).
				Return(db.FileHandle{}, expectedErr).
				Once()

			_, err := store.GetFileHandle(ctx, params)
			require.Error(t, err)
			assert.True(t, errors.Is(err, expectedErr))
		})
	})

	t.Run("DeleteFileHandle 算子调用与参数透传", func(t *testing.T) {
		params := db.DeleteFileHandleParams{
			Fh:  10,
			Ino: 42,
		}

		t.Run("成功删除并返回 fh", func(t *testing.T) {
			mockStore := dbmock.NewMockStore(t)
			var store db.Store = mockStore

			mockStore.EXPECT().
				DeleteFileHandle(mock.Anything, params).
				Return(int64(10), nil).
				Once()

			deletedFh, err := store.DeleteFileHandle(ctx, params)
			require.NoError(t, err)
			assert.Equal(t, int64(10), deletedFh)
		})

		t.Run("目标句柄不存在返回 pgx.ErrNoRows", func(t *testing.T) {
			mockStore := dbmock.NewMockStore(t)
			var store db.Store = mockStore

			mockStore.EXPECT().
				DeleteFileHandle(mock.Anything, params).
				Return(int64(0), pgx.ErrNoRows).
				Once()

			_, err := store.DeleteFileHandle(ctx, params)
			require.Error(t, err)
			assert.True(t, errors.Is(err, pgx.ErrNoRows))
		})

		t.Run("底层执行失败透传错误", func(t *testing.T) {
			mockStore := dbmock.NewMockStore(t)
			var store db.Store = mockStore

			expectedErr := errors.New("database connection failed")
			mockStore.EXPECT().
				DeleteFileHandle(mock.Anything, params).
				Return(int64(0), expectedErr).
				Once()

			_, err := store.DeleteFileHandle(ctx, params)
			require.Error(t, err)
			assert.True(t, errors.Is(err, expectedErr))
		})
	})

	t.Run("DeleteInode 算子调用与参数透传", func(t *testing.T) {
		targetIno := int64(42)

		t.Run("成功删除孤儿 Inode", func(t *testing.T) {
			mockStore := dbmock.NewMockStore(t)
			var store db.Store = mockStore

			mockStore.EXPECT().
				DeleteInode(mock.Anything, targetIno).
				Return(nil).
				Once()

			err := store.DeleteInode(ctx, targetIno)
			require.NoError(t, err)
		})

		t.Run("底层执行失败透传错误", func(t *testing.T) {
			mockStore := dbmock.NewMockStore(t)
			var store db.Store = mockStore

			expectedErr := errors.New("database connection failed")
			mockStore.EXPECT().
				DeleteInode(mock.Anything, targetIno).
				Return(expectedErr).
				Once()

			err := store.DeleteInode(ctx, targetIno)
			require.Error(t, err)
			assert.True(t, errors.Is(err, expectedErr))
		})
	})

	t.Run("GetParentInode 算子调用与参数透传", func(t *testing.T) {
		const targetIno int64 = 42
		now := time.Now()
		expectedInode := db.Inode{
			Ino:        1,
			Generation: 1,
			Mode:       0o040755,
			Size:       4096,
			Nlink:      2,
			Uid:        1000,
			Gid:        1000,
			Rdev:       0,
			Atime:      pgtype.Timestamptz{Time: now, Valid: true},
			Mtime:      pgtype.Timestamptz{Time: now, Valid: true},
			Ctime:      pgtype.Timestamptz{Time: now, Valid: true},
		}

		t.Run("成功查询并返回父节点 Inode", func(t *testing.T) {
			mockStore := dbmock.NewMockStore(t)
			var store db.Store = mockStore

			mockStore.EXPECT().
				GetParentInode(mock.Anything, targetIno).
				Return(expectedInode, nil).
				Once()

			result, err := store.GetParentInode(ctx, targetIno)
			require.NoError(t, err)
			assert.Equal(t, expectedInode, result)
		})

		t.Run("父节点不存在返回 pgx.ErrNoRows", func(t *testing.T) {
			mockStore := dbmock.NewMockStore(t)
			var store db.Store = mockStore

			mockStore.EXPECT().
				GetParentInode(mock.Anything, targetIno).
				Return(db.Inode{}, pgx.ErrNoRows).
				Once()

			_, err := store.GetParentInode(ctx, targetIno)
			require.Error(t, err)
			assert.True(t, errors.Is(err, pgx.ErrNoRows))
		})

		t.Run("底层执行失败透传错误", func(t *testing.T) {
			mockStore := dbmock.NewMockStore(t)
			var store db.Store = mockStore

			expectedErr := errors.New("database connection failed")
			mockStore.EXPECT().
				GetParentInode(mock.Anything, targetIno).
				Return(db.Inode{}, expectedErr).
				Once()

			_, err := store.GetParentInode(ctx, targetIno)
			require.Error(t, err)
			assert.True(t, errors.Is(err, expectedErr))
		})
	})

	t.Run("ListDirEntries 算子调用与参数透传", func(t *testing.T) {
		params := db.ListDirEntriesParams{
			ParentIno: 1,
			Limit:     10,
			Offset:    0,
		}
		now := time.Now()
		expectedRows := []db.ListDirEntriesRow{
			{
				Name:       []byte("file.txt"),
				Ino:        42,
				Generation: 1,
				Mode:       0o100644,
				Size:       1024,
				Nlink:      1,
				Uid:        1000,
				Gid:        1000,
				Rdev:       0,
				Atime:      pgtype.Timestamptz{Time: now, Valid: true},
				Mtime:      pgtype.Timestamptz{Time: now, Valid: true},
				Ctime:      pgtype.Timestamptz{Time: now, Valid: true},
			},
		}

		t.Run("成功查询并返回子项列表", func(t *testing.T) {
			mockStore := dbmock.NewMockStore(t)
			var store db.Store = mockStore

			mockStore.EXPECT().
				ListDirEntries(mock.Anything, params).
				Return(expectedRows, nil).
				Once()

			result, err := store.ListDirEntries(ctx, params)
			require.NoError(t, err)
			assert.Equal(t, expectedRows, result)
		})

		t.Run("无子项返回空列表", func(t *testing.T) {
			mockStore := dbmock.NewMockStore(t)
			var store db.Store = mockStore

			mockStore.EXPECT().
				ListDirEntries(mock.Anything, params).
				Return([]db.ListDirEntriesRow{}, nil).
				Once()

			result, err := store.ListDirEntries(ctx, params)
			require.NoError(t, err)
			assert.Empty(t, result)
		})

		t.Run("底层执行失败透传错误", func(t *testing.T) {
			mockStore := dbmock.NewMockStore(t)
			var store db.Store = mockStore

			expectedErr := errors.New("database connection failed")
			mockStore.EXPECT().
				ListDirEntries(mock.Anything, params).
				Return(nil, expectedErr).
				Once()

			_, err := store.ListDirEntries(ctx, params)
			require.Error(t, err)
			assert.True(t, errors.Is(err, expectedErr))
		})
	})
}

func TestStore_XattrBehavior(t *testing.T) {
	ctx := context.Background()

	t.Run("GetInodeXattr", func(t *testing.T) {
		params := db.GetInodeXattrParams{
			Ino:  42,
			Name: []byte("user.test"),
		}
		expected := db.InodeXattr{
			Ino:   42,
			Name:  []byte("user.test"),
			Value: []byte("value-data"),
		}

		t.Run("成功查询扩展属性", func(t *testing.T) {
			mockStore := dbmock.NewMockStore(t)
			var store db.Store = mockStore

			mockStore.EXPECT().
				GetInodeXattr(mock.Anything, params).
				Return(expected, nil).
				Once()

			result, err := store.GetInodeXattr(ctx, params)
			require.NoError(t, err)
			assert.Equal(t, expected, result)
		})

		t.Run("属性不存在返回 pgx.ErrNoRows", func(t *testing.T) {
			mockStore := dbmock.NewMockStore(t)
			var store db.Store = mockStore

			mockStore.EXPECT().
				GetInodeXattr(mock.Anything, params).
				Return(db.InodeXattr{}, pgx.ErrNoRows).
				Once()

			_, err := store.GetInodeXattr(ctx, params)
			require.ErrorIs(t, err, pgx.ErrNoRows)
		})
	})

	t.Run("UpsertInodeXattr", func(t *testing.T) {
		params := db.UpsertInodeXattrParams{
			Ino:   42,
			Name:  []byte("user.test"),
			Value: []byte("value-data"),
		}

		t.Run("成功插入或更新扩展属性", func(t *testing.T) {
			mockStore := dbmock.NewMockStore(t)
			var store db.Store = mockStore

			mockStore.EXPECT().
				UpsertInodeXattr(mock.Anything, params).
				Return(nil).
				Once()

			err := store.UpsertInodeXattr(ctx, params)
			require.NoError(t, err)
		})

		t.Run("底层执行失败透传错误", func(t *testing.T) {
			mockStore := dbmock.NewMockStore(t)
			var store db.Store = mockStore

			expectedErr := errors.New("db write failed")
			mockStore.EXPECT().
				UpsertInodeXattr(mock.Anything, params).
				Return(expectedErr).
				Once()

			err := store.UpsertInodeXattr(ctx, params)
			require.ErrorIs(t, err, expectedErr)
		})
	})

	t.Run("GetInodeXattrNamesTotalBytes", func(t *testing.T) {
		t.Run("成功统计全部属性名总字节", func(t *testing.T) {
			mockStore := dbmock.NewMockStore(t)
			var store db.Store = mockStore

			mockStore.EXPECT().
				GetInodeXattrNamesTotalBytes(mock.Anything, int64(42)).
				Return(int64(128), nil).
				Once()

			total, err := store.GetInodeXattrNamesTotalBytes(ctx, 42)
			require.NoError(t, err)
			assert.Equal(t, int64(128), total)
		})
	})

	t.Run("ListInodeXattrNames", func(t *testing.T) {
		t.Run("成功返回属性名列表", func(t *testing.T) {
			mockStore := dbmock.NewMockStore(t)
			var store db.Store = mockStore

			expectedNames := [][]byte{[]byte("user.a"), []byte("user.b")}
			mockStore.EXPECT().
				ListInodeXattrNames(mock.Anything, int64(42)).
				Return(expectedNames, nil).
				Once()

			names, err := store.ListInodeXattrNames(ctx, 42)
			require.NoError(t, err)
			assert.Equal(t, expectedNames, names)
		})
	})

	t.Run("DeleteInodeXattr", func(t *testing.T) {
		params := db.DeleteInodeXattrParams{
			Ino:  42,
			Name: []byte("user.test"),
		}

		t.Run("成功删除扩展属性", func(t *testing.T) {
			mockStore := dbmock.NewMockStore(t)
			var store db.Store = mockStore

			mockStore.EXPECT().
				DeleteInodeXattr(mock.Anything, params).
				Return(int64(1), nil).
				Once()

			rows, err := store.DeleteInodeXattr(ctx, params)
			require.NoError(t, err)
			assert.Equal(t, int64(1), rows)
		})

		t.Run("扩展属性不存在返回 0 行", func(t *testing.T) {
			mockStore := dbmock.NewMockStore(t)
			var store db.Store = mockStore

			mockStore.EXPECT().
				DeleteInodeXattr(mock.Anything, params).
				Return(int64(0), nil).
				Once()

			rows, err := store.DeleteInodeXattr(ctx, params)
			require.NoError(t, err)
			assert.Equal(t, int64(0), rows)
		})

		t.Run("底层执行失败透传错误", func(t *testing.T) {
			mockStore := dbmock.NewMockStore(t)
			var store db.Store = mockStore
			expectedErr := errors.New("db error")

			mockStore.EXPECT().
				DeleteInodeXattr(mock.Anything, params).
				Return(int64(0), expectedErr).
				Once()

			rows, err := store.DeleteInodeXattr(ctx, params)
			assert.ErrorIs(t, err, expectedErr)
			assert.Equal(t, int64(0), rows)
		})
	})
}

func TestStore_FileLockBehavior(t *testing.T) {
	ctx := context.Background()
	now := time.Now().Truncate(time.Microsecond)

	t.Run("ClientSession 租约清理与续期", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		var store db.Store = mockStore

		mockStore.EXPECT().
			DeleteExpiredClientSessions(mock.Anything).
			Return(nil).
			Once()
		require.NoError(t, store.DeleteExpiredClientSessions(ctx))

		upsertParams := db.UpsertClientSessionParams{
			SessionID: 1001,
			ExpiresAt: pgtype.Timestamptz{Time: now.Add(60 * time.Second), Valid: true},
		}
		expectedSession := db.ClientSession{
			SessionID: 1001,
			ExpiresAt: upsertParams.ExpiresAt,
			UpdatedAt: pgtype.Timestamptz{Time: now, Valid: true},
		}
		mockStore.EXPECT().
			UpsertClientSession(mock.Anything, upsertParams).
			Return(expectedSession, nil).
			Once()

		session, err := store.UpsertClientSession(ctx, upsertParams)
		require.NoError(t, err)
		assert.Equal(t, expectedSession, session)
	})

	t.Run("GetFirstConflictingFileLock 与 ListActiveFileLocksByInoAndFlags", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		var store db.Store = mockStore

		conflictParams := db.GetFirstConflictingFileLockParams{
			Ino:         42,
			LkFlags:     0,
			StartOffset: 0,
			EndOffset:   100,
			ReqType:     1,
			SessionID:   1001,
			LockOwner:   9001,
			Fh:          10,
		}
		expectedLock := db.FileLock{
			ID:          1,
			SessionID:   1002,
			Ino:         42,
			Fh:          11,
			LockOwner:   9002,
			LkFlags:     0,
			StartOffset: 50,
			EndOffset:   150,
			LockType:    1,
			Pid:         1234,
			CreatedAt:   pgtype.Timestamptz{Time: now, Valid: true},
		}

		mockStore.EXPECT().
			GetFirstConflictingFileLock(mock.Anything, conflictParams).
			Return(expectedLock, nil).
			Once()

		lock, err := store.GetFirstConflictingFileLock(ctx, conflictParams)
		require.NoError(t, err)
		assert.Equal(t, expectedLock, lock)

		listParams := db.ListActiveFileLocksByInoAndFlagsParams{
			Ino:     42,
			LkFlags: 0,
		}
		mockStore.EXPECT().
			ListActiveFileLocksByInoAndFlags(mock.Anything, listParams).
			Return([]db.FileLock{expectedLock}, nil).
			Once()

		locks, err := store.ListActiveFileLocksByInoAndFlags(ctx, listParams)
		require.NoError(t, err)
		assert.Equal(t, []db.FileLock{expectedLock}, locks)
	})

	t.Run("CreateFileLock 与锁删除算子", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		var store db.Store = mockStore

		createParams := db.CreateFileLockParams{
			SessionID:   1001,
			Ino:         42,
			Fh:          10,
			LockOwner:   9001,
			LkFlags:     0,
			StartOffset: 0,
			EndOffset:   100,
			LockType:    0,
			Pid:         1234,
		}
		expectedLock := db.FileLock{
			ID:          5,
			SessionID:   1001,
			Ino:         42,
			Fh:          10,
			LockOwner:   9001,
			LkFlags:     0,
			StartOffset: 0,
			EndOffset:   100,
			LockType:    0,
			Pid:         1234,
			CreatedAt:   pgtype.Timestamptz{Time: now, Valid: true},
		}

		mockStore.EXPECT().
			CreateFileLock(mock.Anything, createParams).
			Return(expectedLock, nil).
			Once()
		created, err := store.CreateFileLock(ctx, createParams)
		require.NoError(t, err)
		assert.Equal(t, expectedLock, created)

		mockStore.EXPECT().
			DeleteFileLocksByIDs(mock.Anything, []int64{5, 6}).
			Return(nil).
			Once()
		require.NoError(t, store.DeleteFileLocksByIDs(ctx, []int64{5, 6}))

		delPosixSessionParams := db.DeletePosixFileLocksBySessionAndOwnerParams{
			SessionID: 1001,
			Ino:       42,
			LockOwner: 9001,
		}
		mockStore.EXPECT().
			DeletePosixFileLocksBySessionAndOwner(mock.Anything, delPosixSessionParams).
			Return(int64(2), nil).
			Once()
		rows, err := store.DeletePosixFileLocksBySessionAndOwner(ctx, delPosixSessionParams)
		require.NoError(t, err)
		assert.Equal(t, int64(2), rows)

		delPosixOwnerParams := db.DeletePosixFileLocksByOwnerParams{
			Ino:       42,
			LockOwner: 9001,
		}
		mockStore.EXPECT().
			DeletePosixFileLocksByOwner(mock.Anything, delPosixOwnerParams).
			Return(int64(1), nil).
			Once()
		rows, err = store.DeletePosixFileLocksByOwner(ctx, delPosixOwnerParams)
		require.NoError(t, err)
		assert.Equal(t, int64(1), rows)

		delFlockSessionParams := db.DeleteFlockFileLocksBySessionAndFhParams{
			SessionID: 1001,
			Ino:       42,
			Fh:        10,
		}
		mockStore.EXPECT().
			DeleteFlockFileLocksBySessionAndFh(mock.Anything, delFlockSessionParams).
			Return(int64(1), nil).
			Once()
		rows, err = store.DeleteFlockFileLocksBySessionAndFh(ctx, delFlockSessionParams)
		require.NoError(t, err)
		assert.Equal(t, int64(1), rows)

		delFlockFhParams := db.DeleteFlockFileLocksByFhParams{
			Ino: 42,
			Fh:  10,
		}
		mockStore.EXPECT().
			DeleteFlockFileLocksByFh(mock.Anything, delFlockFhParams).
			Return(int64(1), nil).
			Once()
		rows, err = store.DeleteFlockFileLocksByFh(ctx, delFlockFhParams)
		require.NoError(t, err)
		assert.Equal(t, int64(1), rows)
	})

	t.Run("FileLockWaiter 状态管理算子", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		var store db.Store = mockStore

		upsertParams := db.UpsertFileLockWaiterParams{
			SessionID:   1001,
			RequestID:   77,
			Ino:         42,
			Fh:          10,
			LockOwner:   9001,
			LkFlags:     0,
			StartOffset: 0,
			EndOffset:   100,
			LockType:    1,
			Pid:         1234,
			Status:      1,
		}
		expectedWaiter := db.FileLockWaiter{
			SessionID:   1001,
			RequestID:   77,
			Ino:         42,
			Fh:          10,
			LockOwner:   9001,
			LkFlags:     0,
			StartOffset: 0,
			EndOffset:   100,
			LockType:    1,
			Pid:         1234,
			Status:      1,
			CreatedAt:   pgtype.Timestamptz{Time: now, Valid: true},
		}

		mockStore.EXPECT().
			UpsertFileLockWaiter(mock.Anything, upsertParams).
			Return(expectedWaiter, nil).
			Once()
		waiter, err := store.UpsertFileLockWaiter(ctx, upsertParams)
		require.NoError(t, err)
		assert.Equal(t, expectedWaiter, waiter)

		getParams := db.GetFileLockWaiterParams{
			SessionID: 1001,
			RequestID: 77,
		}
		mockStore.EXPECT().
			GetFileLockWaiter(mock.Anything, getParams).
			Return(expectedWaiter, nil).
			Once()
		got, err := store.GetFileLockWaiter(ctx, getParams)
		require.NoError(t, err)
		assert.Equal(t, expectedWaiter, got)

		listParams := db.ListPendingFileLockWaitersByInoAndFlagsParams{
			Ino:     42,
			LkFlags: 0,
		}
		mockStore.EXPECT().
			ListPendingFileLockWaitersByInoAndFlags(mock.Anything, listParams).
			Return([]db.FileLockWaiter{expectedWaiter}, nil).
			Once()
		waiters, err := store.ListPendingFileLockWaitersByInoAndFlags(ctx, listParams)
		require.NoError(t, err)
		assert.Equal(t, []db.FileLockWaiter{expectedWaiter}, waiters)

		delParams := db.DeleteFileLockWaiterParams{
			SessionID: 1001,
			RequestID: 77,
		}
		mockStore.EXPECT().
			DeleteFileLockWaiter(mock.Anything, delParams).
			Return(int64(1), nil).
			Once()
		rows, err := store.DeleteFileLockWaiter(ctx, delParams)
		require.NoError(t, err)
		assert.Equal(t, int64(1), rows)
	})
}
