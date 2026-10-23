package db_test

import (
	"context"
	"crypto/rand"
	"errors"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/starsrc/Aether/pkg/tx"
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
	HasLinkableTmpFileHandleByIno(ctx context.Context, ino int64) (bool, error)
	ClearTmpFileFlagByIno(ctx context.Context, ino int64) error
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
	DeleteExpiredClientSessions(ctx context.Context) (int64, error)
	UpsertClientSession(ctx context.Context, arg db.UpsertClientSessionParams) (db.ClientSession, error)
	DeleteUnreferencedOrphanInodes(ctx context.Context) ([]db.DeleteUnreferencedOrphanInodesRow, error)
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

// DiskQuerier 提取磁盘元数据增删改查、配额更新与按根节点级联清理接口契约。
type DiskQuerier interface {
	CreateDisk(ctx context.Context, arg db.CreateDiskParams) (db.Disk, error)
	GetDisk(ctx context.Context, diskID pgtype.UUID) (db.Disk, error)
	GetDiskForUpdate(ctx context.Context, diskID pgtype.UUID) (db.Disk, error)
	GetDiskByRootIno(ctx context.Context, rootIno int64) (db.Disk, error)
	GetDiskByRootInoForUpdate(ctx context.Context, rootIno int64) (db.Disk, error)
	UpdateDisk(ctx context.Context, arg db.UpdateDiskParams) (db.Disk, error)
	UpdateDiskUsedBytes(ctx context.Context, arg db.UpdateDiskUsedBytesParams) (db.Disk, error)
	UpdateDiskQuotaByRootIno(ctx context.Context, arg db.UpdateDiskQuotaByRootInoParams) (db.Disk, error)
	AddDiskQuotaByRootIno(ctx context.Context, arg db.AddDiskQuotaByRootInoParams) error
	DeleteDisk(ctx context.Context, diskID pgtype.UUID) (int64, error)
	DeleteInodesByRootIno(ctx context.Context, rootIno int64) ([]int64, error)
	CreateRootInode(ctx context.Context, arg db.CreateRootInodeParams) (db.Inode, error)
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

	_ DiskQuerier = (db.Querier)(nil)
	_ DiskQuerier = (db.Store)(nil)
	_ DiskQuerier = (*db.Queries)(nil)

	_ tx.Transactor[db.Querier] = (db.Store)(nil)
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

		t.Run("成功创建绑定 session_id 的 FileHandle 并返回数据实体", func(t *testing.T) {
			mockStore := dbmock.NewMockStore(t)
			var store db.Store = mockStore

			withSessParams := db.CreateFileHandleParams{
				Ino:       100,
				Flags:     0o2,
				SessionID: pgtype.Int8{Int64: 1001, Valid: true},
			}
			withSessExpected := db.FileHandle{
				Fh:        2,
				Ino:       100,
				Flags:     0o2,
				CreatedAt: pgtype.Timestamptz{Time: now, Valid: true},
				SessionID: pgtype.Int8{Int64: 1001, Valid: true},
			}

			mockStore.EXPECT().
				CreateFileHandle(mock.Anything, withSessParams).
				Return(withSessExpected, nil).
				Once()

			result, err := store.CreateFileHandle(ctx, withSessParams)
			require.NoError(t, err)
			assert.Equal(t, withSessExpected, result)
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
			Fh:        10,
			Ino:       42,
			SessionID: pgtype.Int8{Int64: 100, Valid: true},
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

	t.Run("HasLinkableTmpFileHandleByIno 与 ClearTmpFileFlagByIno 算子调用与参数透传", func(t *testing.T) {
		const targetIno int64 = 42

		t.Run("HasLinkableTmpFileHandleByIno 成功查询并返回存在可具名化临时文件句柄", func(t *testing.T) {
			mockStore := dbmock.NewMockStore(t)
			var store db.Store = mockStore

			mockStore.EXPECT().
				HasLinkableTmpFileHandleByIno(mock.Anything, targetIno).
				Return(true, nil).
				Once()

			linkable, err := store.HasLinkableTmpFileHandleByIno(ctx, targetIno)
			require.NoError(t, err)
			assert.True(t, linkable)
		})

		t.Run("HasLinkableTmpFileHandleByIno 底层执行失败透传错误", func(t *testing.T) {
			mockStore := dbmock.NewMockStore(t)
			var store db.Store = mockStore

			expectedErr := errors.New("db query failed")
			mockStore.EXPECT().
				HasLinkableTmpFileHandleByIno(mock.Anything, targetIno).
				Return(false, expectedErr).
				Once()

			_, err := store.HasLinkableTmpFileHandleByIno(ctx, targetIno)
			require.Error(t, err)
			assert.True(t, errors.Is(err, expectedErr))
		})

		t.Run("ClearTmpFileFlagByIno 成功清除 O_TMPFILE 标志位", func(t *testing.T) {
			mockStore := dbmock.NewMockStore(t)
			var store db.Store = mockStore

			mockStore.EXPECT().
				ClearTmpFileFlagByIno(mock.Anything, targetIno).
				Return(nil).
				Once()

			err := store.ClearTmpFileFlagByIno(ctx, targetIno)
			require.NoError(t, err)
		})

		t.Run("ClearTmpFileFlagByIno 底层执行失败透传错误", func(t *testing.T) {
			mockStore := dbmock.NewMockStore(t)
			var store db.Store = mockStore

			expectedErr := errors.New("db update failed")
			mockStore.EXPECT().
				ClearTmpFileFlagByIno(mock.Anything, targetIno).
				Return(expectedErr).
				Once()

			err := store.ClearTmpFileFlagByIno(ctx, targetIno)
			require.Error(t, err)
			assert.True(t, errors.Is(err, expectedErr))
		})
	})

	t.Run("DeleteFileHandle 算子调用与参数透传", func(t *testing.T) {
		params := db.DeleteFileHandleParams{
			Fh:        10,
			Ino:       42,
			SessionID: pgtype.Int8{Int64: 100, Valid: true},
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
			Return(int64(0), nil).
			Once()
		count, err := store.DeleteExpiredClientSessions(ctx)
		require.NoError(t, err)
		assert.Equal(t, int64(0), count)

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

		expectedOrphans := []db.DeleteUnreferencedOrphanInodesRow{
			{Ino: 101, RootIno: 1, Mode: 0o100644, Size: 4096},
		}
		mockStore.EXPECT().
			DeleteUnreferencedOrphanInodes(mock.Anything).
			Return(expectedOrphans, nil).
			Once()
		deletedInos, err := store.DeleteUnreferencedOrphanInodes(ctx)
		require.NoError(t, err)
		assert.Equal(t, expectedOrphans, deletedInos)
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

func TestStore_DiskOperatorsBehavior(t *testing.T) {
	ctx := context.Background()

	t.Run("Disk 磁盘元数据与配额算子契约调用与参数透传", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		var store db.Store = mockStore

		diskID := newTestUUID()
		now := time.Now().Truncate(time.Microsecond)
		expectedDisk := db.Disk{
			DiskID:     diskID,
			Name:       "mock-disk",
			RootIno:    1001,
			MaxBytes:   1024 * 1024,
			UsedBytes:  0,
			UsedInodes: 1,
			CreatedAt:  pgtype.Timestamptz{Time: now, Valid: true},
			UpdatedAt:  pgtype.Timestamptz{Time: now, Valid: true},
		}

		// 1. CreateDisk
		createParams := db.CreateDiskParams{
			DiskID:   diskID,
			Name:     "mock-disk",
			RootIno:  1001,
			MaxBytes: 1024 * 1024,
		}
		mockStore.EXPECT().
			CreateDisk(mock.Anything, createParams).
			Return(expectedDisk, nil).
			Once()
		createdDisk, err := store.CreateDisk(ctx, createParams)
		require.NoError(t, err)
		assert.Equal(t, expectedDisk, createdDisk)

		// 2. GetDisk
		mockStore.EXPECT().
			GetDisk(mock.Anything, diskID).
			Return(expectedDisk, nil).
			Once()
		gotDisk, err := store.GetDisk(ctx, diskID)
		require.NoError(t, err)
		assert.Equal(t, expectedDisk, gotDisk)

		// 3. GetDiskForUpdate
		mockStore.EXPECT().
			GetDiskForUpdate(mock.Anything, diskID).
			Return(expectedDisk, nil).
			Once()
		gotForUpdate, err := store.GetDiskForUpdate(ctx, diskID)
		require.NoError(t, err)
		assert.Equal(t, expectedDisk, gotForUpdate)

		// 4. GetDiskByRootIno
		mockStore.EXPECT().
			GetDiskByRootIno(mock.Anything, int64(1001)).
			Return(expectedDisk, nil).
			Once()
		gotByRoot, err := store.GetDiskByRootIno(ctx, 1001)
		require.NoError(t, err)
		assert.Equal(t, expectedDisk, gotByRoot)

		// 5. GetDiskByRootInoForUpdate
		mockStore.EXPECT().
			GetDiskByRootInoForUpdate(mock.Anything, int64(1001)).
			Return(expectedDisk, nil).
			Once()
		gotByRootForUpdate, err := store.GetDiskByRootInoForUpdate(ctx, 1001)
		require.NoError(t, err)
		assert.Equal(t, expectedDisk, gotByRootForUpdate)

		// 6. UpdateDisk
		updateParams := db.UpdateDiskParams{
			DiskID:   diskID,
			Name:     "renamed-mock-disk",
			MaxBytes: 2048 * 1024,
		}
		updatedExpected := expectedDisk
		updatedExpected.Name = "renamed-mock-disk"
		updatedExpected.MaxBytes = 2048 * 1024
		mockStore.EXPECT().
			UpdateDisk(mock.Anything, updateParams).
			Return(updatedExpected, nil).
			Once()
		updatedDisk, err := store.UpdateDisk(ctx, updateParams)
		require.NoError(t, err)
		assert.Equal(t, updatedExpected, updatedDisk)

		// 7. UpdateDiskUsedBytes
		updateUsedParams := db.UpdateDiskUsedBytesParams{
			DiskID:    diskID,
			UsedBytes: 512 * 1024,
		}
		usedBytesExpected := updatedExpected
		usedBytesExpected.UsedBytes = 512 * 1024
		mockStore.EXPECT().
			UpdateDiskUsedBytes(mock.Anything, updateUsedParams).
			Return(usedBytesExpected, nil).
			Once()
		usedDisk, err := store.UpdateDiskUsedBytes(ctx, updateUsedParams)
		require.NoError(t, err)
		assert.Equal(t, usedBytesExpected, usedDisk)

		// 8. UpdateDiskQuotaByRootIno
		updateQuotaParams := db.UpdateDiskQuotaByRootInoParams{
			DeltaBytes:  4096,
			DeltaInodes: 1,
			RootIno:     1001,
		}
		quotaExpected := usedBytesExpected
		quotaExpected.UsedBytes += 4096
		quotaExpected.UsedInodes += 1
		mockStore.EXPECT().
			UpdateDiskQuotaByRootIno(mock.Anything, updateQuotaParams).
			Return(quotaExpected, nil).
			Once()
		quotaDisk, err := store.UpdateDiskQuotaByRootIno(ctx, updateQuotaParams)
		require.NoError(t, err)
		assert.Equal(t, quotaExpected, quotaDisk)

		// 9. AddDiskQuotaByRootIno
		addQuotaParams := db.AddDiskQuotaByRootInoParams{
			DeltaBytes:  -4096,
			DeltaInodes: -1,
			RootIno:     1001,
		}
		mockStore.EXPECT().
			AddDiskQuotaByRootIno(mock.Anything, addQuotaParams).
			Return(nil).
			Once()
		err = store.AddDiskQuotaByRootIno(ctx, addQuotaParams)
		require.NoError(t, err)

		// 10. DeleteDisk
		mockStore.EXPECT().
			DeleteDisk(mock.Anything, diskID).
			Return(int64(1), nil).
			Once()
		delRows, err := store.DeleteDisk(ctx, diskID)
		require.NoError(t, err)
		assert.Equal(t, int64(1), delRows)

		// 11. DeleteInodesByRootIno
		mockStore.EXPECT().
			DeleteInodesByRootIno(mock.Anything, int64(1001)).
			Return([]int64{1001, 1002}, nil).
			Once()
		deletedInos, err := store.DeleteInodesByRootIno(ctx, 1001)
		require.NoError(t, err)
		assert.Equal(t, []int64{1001, 1002}, deletedInos)

		// 12. CreateRootInode
		rootParams := db.CreateRootInodeParams{
			Mode:  0o040755,
			Nlink: 2,
			Uid:   0,
			Gid:   0,
			Rdev:  0,
			Size:  4096,
		}
		expectedRootInode := db.Inode{
			Ino:       1001,
			Mode:      0o040755,
			Nlink:     2,
			Uid:       0,
			Gid:       0,
			Rdev:      0,
			Size:      4096,
			ParentIno: 1001,
			RootIno:   1001,
			Atime:     pgtype.Timestamptz{Time: now, Valid: true},
			Mtime:     pgtype.Timestamptz{Time: now, Valid: true},
			Ctime:     pgtype.Timestamptz{Time: now, Valid: true},
		}
		mockStore.EXPECT().
			CreateRootInode(mock.Anything, rootParams).
			Return(expectedRootInode, nil).
			Once()
		createdRoot, err := store.CreateRootInode(ctx, rootParams)
		require.NoError(t, err)
		assert.Equal(t, expectedRootInode, createdRoot)
	})
}

// ==========================================
// 数据库持久化集成测试 (Database Integration Tests)
// ==========================================

var testInoSeq int64 = 9_500_000_000

func nextTestIno() int64 {
	return atomic.AddInt64(&testInoSeq, 1)
}

func newTestUUID() pgtype.UUID {
	var b [16]byte
	_, _ = rand.Read(b[:])
	b[6] = (b[6] & 0x0f) | 0x40 // RFC 4122 version 4
	b[8] = (b[8] & 0x3f) | 0x80 // RFC 4122 variant
	return pgtype.UUID{Bytes: b, Valid: true}
}

func setupTestDB(t *testing.T) (db.Store, *pgxpool.Pool) {
	t.Helper()
	dsn := os.Getenv("STORAGE_DB_DSN")
	if dsn == "" {
		dsn = os.Getenv("DATABASE_URL")
	}
	if dsn == "" {
		dsn = "postgres://postgres:postgres@localhost:5432/bowl?sslmode=disable"
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Skipf("跳过 PostgreSQL 测试 (连接初始化失败): %v", err)
	}

	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		t.Skipf("跳过 PostgreSQL 测试 (ping 失败): %v", err)
	}

	t.Cleanup(func() {
		pool.Close()
	})

	return db.NewStore(pool), pool
}

func TestStore_Integration_CreateAndGetDisk(t *testing.T) {
	store, _ := setupTestDB(t)
	ctx := context.Background()

	diskID := newTestUUID()
	rootIno := nextTestIno()

	t.Cleanup(func() {
		_, _ = store.DeleteDisk(context.Background(), diskID)
	})

	params := db.CreateDiskParams{
		DiskID:   diskID,
		Name:     "integration-storage-disk-1",
		RootIno:  rootIno,
		MaxBytes: 100 * 1024 * 1024,
	}

	// 1. 创建磁盘实体
	created, err := store.CreateDisk(ctx, params)
	require.NoError(t, err)
	assert.Equal(t, diskID, created.DiskID)
	assert.Equal(t, "integration-storage-disk-1", created.Name)
	assert.Equal(t, rootIno, created.RootIno)
	assert.Equal(t, int64(100*1024*1024), created.MaxBytes)
	assert.Equal(t, int64(0), created.UsedBytes)
	assert.Equal(t, int64(1), created.UsedInodes)
	assert.True(t, created.CreatedAt.Valid)
	assert.True(t, created.UpdatedAt.Valid)

	// 2. 主键查询磁盘实体
	got, err := store.GetDisk(ctx, diskID)
	require.NoError(t, err)
	assert.Equal(t, created.DiskID, got.DiskID)
	assert.Equal(t, created.Name, got.Name)
	assert.Equal(t, created.RootIno, got.RootIno)
	assert.Equal(t, created.MaxBytes, got.MaxBytes)
	assert.Equal(t, created.UsedBytes, got.UsedBytes)
	assert.Equal(t, created.UsedInodes, got.UsedInodes)

	// 3. 按 root_ino 查询磁盘实体
	gotByRoot, err := store.GetDiskByRootIno(ctx, rootIno)
	require.NoError(t, err)
	assert.Equal(t, created.DiskID, gotByRoot.DiskID)
	assert.Equal(t, created.Name, gotByRoot.Name)
	assert.Equal(t, created.RootIno, gotByRoot.RootIno)

	// 4. 查询不存在的磁盘实体返回 pgx.ErrNoRows
	nonExistentID := newTestUUID()
	_, err = store.GetDisk(ctx, nonExistentID)
	require.ErrorIs(t, err, pgx.ErrNoRows)

	_, err = store.GetDiskByRootIno(ctx, nextTestIno())
	require.ErrorIs(t, err, pgx.ErrNoRows)
}

func TestStore_Integration_GetDiskForUpdate(t *testing.T) {
	store, _ := setupTestDB(t)
	ctx := context.Background()

	diskID := newTestUUID()
	rootIno := nextTestIno()

	t.Cleanup(func() {
		_, _ = store.DeleteDisk(context.Background(), diskID)
	})

	params := db.CreateDiskParams{
		DiskID:   diskID,
		Name:     "for-update-storage-disk",
		RootIno:  rootIno,
		MaxBytes: 50 * 1024 * 1024,
	}

	_, err := store.CreateDisk(ctx, params)
	require.NoError(t, err)

	// 事务中悲观排他锁读取
	err = store.ExecTx(ctx, func(q db.Querier) error {
		locked, getErr := q.GetDiskForUpdate(ctx, diskID)
		require.NoError(t, getErr)
		assert.Equal(t, diskID, locked.DiskID)
		assert.Equal(t, "for-update-storage-disk", locked.Name)

		lockedByRoot, getByRootErr := q.GetDiskByRootInoForUpdate(ctx, rootIno)
		require.NoError(t, getByRootErr)
		assert.Equal(t, diskID, lockedByRoot.DiskID)
		assert.Equal(t, rootIno, lockedByRoot.RootIno)
		return nil
	})
	require.NoError(t, err)

	// 查询不存在的磁盘返回 pgx.ErrNoRows
	_, err = store.GetDiskForUpdate(ctx, newTestUUID())
	require.ErrorIs(t, err, pgx.ErrNoRows)

	_, err = store.GetDiskByRootInoForUpdate(ctx, nextTestIno())
	require.ErrorIs(t, err, pgx.ErrNoRows)
}

func TestStore_Integration_UpdateDisk(t *testing.T) {
	store, _ := setupTestDB(t)
	ctx := context.Background()

	diskID := newTestUUID()
	rootIno := nextTestIno()

	t.Cleanup(func() {
		_, _ = store.DeleteDisk(context.Background(), diskID)
	})

	params := db.CreateDiskParams{
		DiskID:   diskID,
		Name:     "original-disk-name",
		RootIno:  rootIno,
		MaxBytes: 50 * 1024 * 1024,
	}

	_, err := store.CreateDisk(ctx, params)
	require.NoError(t, err)

	// 更新名称与容量上限
	updateParams := db.UpdateDiskParams{
		DiskID:   diskID,
		Name:     "renamed-storage-disk",
		MaxBytes: 200 * 1024 * 1024,
	}
	updated, err := store.UpdateDisk(ctx, updateParams)
	require.NoError(t, err)
	assert.Equal(t, "renamed-storage-disk", updated.Name)
	assert.Equal(t, int64(200*1024*1024), updated.MaxBytes)

	// 更新已用字节数
	usedParams := db.UpdateDiskUsedBytesParams{
		DiskID:    diskID,
		UsedBytes: 10 * 1024 * 1024,
	}
	updatedUsed, err := store.UpdateDiskUsedBytes(ctx, usedParams)
	require.NoError(t, err)
	assert.Equal(t, int64(10*1024*1024), updatedUsed.UsedBytes)
}

func TestStore_Integration_DiskQuotaUpdates(t *testing.T) {
	store, _ := setupTestDB(t)
	ctx := context.Background()

	diskID := newTestUUID()
	rootIno := nextTestIno()

	t.Cleanup(func() {
		_, _ = store.DeleteDisk(context.Background(), diskID)
	})

	params := db.CreateDiskParams{
		DiskID:   diskID,
		Name:     "quota-disk",
		RootIno:  rootIno,
		MaxBytes: 100 * 1024 * 1024,
	}

	_, err := store.CreateDisk(ctx, params)
	require.NoError(t, err)

	// 1. UpdateDiskQuotaByRootIno 正向增加
	q1, err := store.UpdateDiskQuotaByRootIno(ctx, db.UpdateDiskQuotaByRootInoParams{
		DeltaBytes:  4096,
		DeltaInodes: 1,
		RootIno:     rootIno,
	})
	require.NoError(t, err)
	assert.Equal(t, int64(4096), q1.UsedBytes)
	assert.Equal(t, int64(2), q1.UsedInodes)

	// 2. UpdateDiskQuotaByRootIno 负向扣减
	q2, err := store.UpdateDiskQuotaByRootIno(ctx, db.UpdateDiskQuotaByRootInoParams{
		DeltaBytes:  -2048,
		DeltaInodes: -1,
		RootIno:     rootIno,
	})
	require.NoError(t, err)
	assert.Equal(t, int64(2048), q2.UsedBytes)
	assert.Equal(t, int64(1), q2.UsedInodes)

	// 3. UpdateDiskQuotaByRootIno 超额扣减（GREATEST 钳位归零）
	q3, err := store.UpdateDiskQuotaByRootIno(ctx, db.UpdateDiskQuotaByRootInoParams{
		DeltaBytes:  -100000,
		DeltaInodes: -10,
		RootIno:     rootIno,
	})
	require.NoError(t, err)
	assert.Equal(t, int64(0), q3.UsedBytes)
	assert.Equal(t, int64(0), q3.UsedInodes)

	// 4. AddDiskQuotaByRootIno 增量累加
	err = store.AddDiskQuotaByRootIno(ctx, db.AddDiskQuotaByRootInoParams{
		DeltaBytes:  8192,
		DeltaInodes: 5,
		RootIno:     rootIno,
	})
	require.NoError(t, err)

	got, err := store.GetDisk(ctx, diskID)
	require.NoError(t, err)
	assert.Equal(t, int64(8192), got.UsedBytes)
	assert.Equal(t, int64(5), got.UsedInodes)

	// 5. 对不存在的 root_ino 调用 AddDiskQuotaByRootIno 影响 0 行且不返回错误
	err = store.AddDiskQuotaByRootIno(ctx, db.AddDiskQuotaByRootInoParams{
		DeltaBytes:  1024,
		DeltaInodes: 1,
		RootIno:     nextTestIno(),
	})
	require.NoError(t, err)
}

func TestStore_Integration_DeleteDisk(t *testing.T) {
	store, _ := setupTestDB(t)
	ctx := context.Background()

	diskID := newTestUUID()
	rootIno := nextTestIno()

	params := db.CreateDiskParams{
		DiskID:   diskID,
		Name:     "delete-target-disk",
		RootIno:  rootIno,
		MaxBytes: 10 * 1024 * 1024,
	}

	_, err := store.CreateDisk(ctx, params)
	require.NoError(t, err)

	// 正常删除返回 1
	rows, err := store.DeleteDisk(ctx, diskID)
	require.NoError(t, err)
	assert.Equal(t, int64(1), rows)

	// 再次查询返回 pgx.ErrNoRows
	_, err = store.GetDisk(ctx, diskID)
	require.ErrorIs(t, err, pgx.ErrNoRows)

	// 再次删除影响 0 行
	rows, err = store.DeleteDisk(ctx, diskID)
	require.NoError(t, err)
	assert.Equal(t, int64(0), rows)
}

func TestStore_Integration_DiskConstraints(t *testing.T) {
	store, pool := setupTestDB(t)
	ctx := context.Background()

	t.Run("chk_disks_root_ino 约束: root_ino <= 0 被拒绝", func(t *testing.T) {
		diskID := newTestUUID()
		_, err := store.CreateDisk(ctx, db.CreateDiskParams{
			DiskID:   diskID,
			Name:     "invalid-root-ino",
			RootIno:  0,
			MaxBytes: 100 * 1024 * 1024,
		})
		require.Error(t, err)
		var pgErr *pgconn.PgError
		require.True(t, errors.As(err, &pgErr))
		assert.Equal(t, "chk_disks_root_ino", pgErr.ConstraintName)
	})

	t.Run("chk_disks_capacity 约束: max_bytes <= 0 被拒绝", func(t *testing.T) {
		diskID := newTestUUID()
		_, err := store.CreateDisk(ctx, db.CreateDiskParams{
			DiskID:   diskID,
			Name:     "invalid-max-bytes",
			RootIno:  nextTestIno(),
			MaxBytes: 0,
		})
		require.Error(t, err)
		var pgErr *pgconn.PgError
		require.True(t, errors.As(err, &pgErr))
		assert.Equal(t, "chk_disks_capacity", pgErr.ConstraintName)
	})

	t.Run("chk_disks_capacity 约束: used_bytes > max_bytes 被拒绝", func(t *testing.T) {
		diskID := newTestUUID()
		rootIno := nextTestIno()
		t.Cleanup(func() {
			_, _ = store.DeleteDisk(context.Background(), diskID)
		})

		_, err := store.CreateDisk(ctx, db.CreateDiskParams{
			DiskID:   diskID,
			Name:     "capacity-test-disk",
			RootIno:  rootIno,
			MaxBytes: 1000,
		})
		require.NoError(t, err)

		_, err = store.UpdateDiskUsedBytes(ctx, db.UpdateDiskUsedBytesParams{
			DiskID:    diskID,
			UsedBytes: 1001,
		})
		require.Error(t, err)
		var pgErr *pgconn.PgError
		require.True(t, errors.As(err, &pgErr))
		assert.Equal(t, "chk_disks_capacity", pgErr.ConstraintName)
	})

	t.Run("chk_disks_capacity 约束: used_inodes < 0 被拒绝", func(t *testing.T) {
		diskID := newTestUUID()
		rootIno := nextTestIno()
		t.Cleanup(func() {
			_, _ = store.DeleteDisk(context.Background(), diskID)
		})

		_, err := store.CreateDisk(ctx, db.CreateDiskParams{
			DiskID:   diskID,
			Name:     "inode-capacity-test",
			RootIno:  rootIno,
			MaxBytes: 1000,
		})
		require.NoError(t, err)

		_, err = pool.Exec(ctx, "UPDATE disks SET used_inodes = -1 WHERE disk_id = $1", diskID)
		require.Error(t, err)
		var pgErr *pgconn.PgError
		require.True(t, errors.As(err, &pgErr))
		assert.Equal(t, "chk_disks_capacity", pgErr.ConstraintName)
	})

	t.Run("root_ino UNIQUE 约束: 重复 root_ino 被拒绝", func(t *testing.T) {
		diskID1 := newTestUUID()
		diskID2 := newTestUUID()
		rootIno := nextTestIno()

		t.Cleanup(func() {
			_, _ = store.DeleteDisk(context.Background(), diskID1)
			_, _ = store.DeleteDisk(context.Background(), diskID2)
		})

		_, err := store.CreateDisk(ctx, db.CreateDiskParams{
			DiskID:   diskID1,
			Name:     "disk-1",
			RootIno:  rootIno,
			MaxBytes: 1000,
		})
		require.NoError(t, err)

		_, err = store.CreateDisk(ctx, db.CreateDiskParams{
			DiskID:   diskID2,
			Name:     "disk-2",
			RootIno:  rootIno,
			MaxBytes: 2000,
		})
		require.Error(t, err)
		var pgErr *pgconn.PgError
		require.True(t, errors.As(err, &pgErr))
		assert.Equal(t, "23505", pgErr.Code)
	})
}

func TestStore_Integration_InodesRootInoAndCascade(t *testing.T) {
	store, _ := setupTestDB(t)
	ctx := context.Background()

	// 1. CreateRootInode 自动设置 ino == root_ino == parent_ino
	rootInode, err := store.CreateRootInode(ctx, db.CreateRootInodeParams{
		Mode:  0o040755,
		Nlink: 2,
		Uid:   1000,
		Gid:   1000,
		Rdev:  0,
		Size:  4096,
	})
	require.NoError(t, err)
	assert.Greater(t, rootInode.Ino, int64(1))
	assert.Equal(t, rootInode.Ino, rootInode.RootIno)
	assert.Equal(t, rootInode.Ino, rootInode.ParentIno)

	t.Cleanup(func() {
		_, _ = store.DeleteInodesByRootIno(context.Background(), rootInode.Ino)
	})

	// 2. CreateInode 继承 root_ino
	childInode, err := store.CreateInode(ctx, db.CreateInodeParams{
		Mode:      0o100644,
		Nlink:     1,
		Uid:       1000,
		Gid:       1000,
		Rdev:      0,
		Size:      1024,
		ParentIno: rootInode.Ino,
		RootIno:   rootInode.Ino,
	})
	require.NoError(t, err)
	assert.Equal(t, rootInode.Ino, childInode.RootIno)
	assert.Equal(t, rootInode.Ino, childInode.ParentIno)

	// 3. CreateSymlinkInode 继承 root_ino
	symlinkInode, err := store.CreateSymlinkInode(ctx, db.CreateSymlinkInodeParams{
		Mode:          0o120777,
		Size:          10,
		Nlink:         1,
		Uid:           1000,
		Gid:           1000,
		Rdev:          0,
		SymlinkTarget: []byte("/target/path"),
		ParentIno:     rootInode.Ino,
		RootIno:       rootInode.Ino,
	})
	require.NoError(t, err)
	assert.Equal(t, rootInode.Ino, symlinkInode.RootIno)
	assert.Equal(t, rootInode.Ino, symlinkInode.ParentIno)

	// 4. 创建 entries 目录项
	err = store.CreateEntry(ctx, db.CreateEntryParams{
		ParentIno: rootInode.Ino,
		Name:      []byte("child-file.txt"),
		Ino:       childInode.Ino,
	})
	require.NoError(t, err)

	err = store.CreateEntry(ctx, db.CreateEntryParams{
		ParentIno: rootInode.Ino,
		Name:      []byte("child-symlink"),
		Ino:       symlinkInode.Ino,
	})
	require.NoError(t, err)

	// 5. 验证 GetInode 与 GetInodeByEntry 能够查到带有正确 root_ino 的数据
	gotChild, err := store.GetInode(ctx, childInode.Ino)
	require.NoError(t, err)
	assert.Equal(t, rootInode.Ino, gotChild.RootIno)

	gotByEntry, err := store.GetInodeByEntry(ctx, db.GetInodeByEntryParams{
		ParentIno: rootInode.Ino,
		Name:      []byte("child-file.txt"),
	})
	require.NoError(t, err)
	assert.Equal(t, childInode.Ino, gotByEntry.Ino)
	assert.Equal(t, rootInode.Ino, gotByEntry.RootIno)

	// 6. DeleteInodesByRootIno 级联清理所有 inodes 及 entries 表中的关联记录
	deletedInos, err := store.DeleteInodesByRootIno(ctx, rootInode.Ino)
	require.NoError(t, err)
	assert.ElementsMatch(t, []int64{rootInode.Ino, childInode.Ino, symlinkInode.Ino}, deletedInos)

	// 7. 验证 inodes 已全部被物理清理
	_, err = store.GetInode(ctx, rootInode.Ino)
	require.ErrorIs(t, err, pgx.ErrNoRows)

	_, err = store.GetInode(ctx, childInode.Ino)
	require.ErrorIs(t, err, pgx.ErrNoRows)

	_, err = store.GetInode(ctx, symlinkInode.Ino)
	require.ErrorIs(t, err, pgx.ErrNoRows)

	// 8. 验证 entries 记录因 fk_entries_ino ON DELETE CASCADE 也已被级联清理
	_, err = store.GetEntry(ctx, db.GetEntryParams{
		ParentIno: rootInode.Ino,
		Name:      []byte("child-file.txt"),
	})
	require.ErrorIs(t, err, pgx.ErrNoRows)

	_, err = store.GetEntry(ctx, db.GetEntryParams{
		ParentIno: rootInode.Ino,
		Name:      []byte("child-symlink"),
	})
	require.ErrorIs(t, err, pgx.ErrNoRows)
}

func TestStore_Integration_TransactionRollbackAndCommit(t *testing.T) {
	store, _ := setupTestDB(t)
	ctx := context.Background()

	t.Run("事务发生错误时原子回滚", func(t *testing.T) {
		diskID := newTestUUID()
		rootIno := nextTestIno()
		simulatedErr := errors.New("simulated business error inside tx")

		err := store.ExecTx(ctx, func(q db.Querier) error {
			_, createErr := q.CreateDisk(ctx, db.CreateDiskParams{
				DiskID:   diskID,
				Name:     "rollback-disk",
				RootIno:  rootIno,
				MaxBytes: 10 * 1024 * 1024,
			})
			require.NoError(t, createErr)

			return simulatedErr
		})
		require.ErrorIs(t, err, simulatedErr)

		// 验证 disk 未持久化
		_, err = store.GetDisk(ctx, diskID)
		require.ErrorIs(t, err, pgx.ErrNoRows)
	})

	t.Run("事务成功提交后持久化生效", func(t *testing.T) {
		diskID := newTestUUID()
		rootIno := nextTestIno()

		t.Cleanup(func() {
			_, _ = store.DeleteDisk(context.Background(), diskID)
		})

		err := store.ExecTx(ctx, func(q db.Querier) error {
			_, createErr := q.CreateDisk(ctx, db.CreateDiskParams{
				DiskID:   diskID,
				Name:     "commit-disk",
				RootIno:  rootIno,
				MaxBytes: 20 * 1024 * 1024,
			})
			require.NoError(t, createErr)

			return nil
		})
		require.NoError(t, err)

		// 验证 disk 已持久化
		disk, err := store.GetDisk(ctx, diskID)
		require.NoError(t, err)
		assert.Equal(t, diskID, disk.DiskID)
		assert.Equal(t, "commit-disk", disk.Name)
	})
}

// TestMigration_AddDisksAndInodeRootIno_BackfillAndDown 验证 20261023000000 迁移的存量目录树递归回填、子盘边界截断、孤儿节点归属及回滚数据保护。
func TestMigration_AddDisksAndInodeRootIno_BackfillAndDown(t *testing.T) {
	_, pool := setupTestDB(t)
	ctx := context.Background()

	migrationData, err := os.ReadFile("../../db/migrations/20261023000000_add_disks_and_inode_root_ino.sql")
	require.NoError(t, err)

	parts := strings.Split(string(migrationData), "-- +goose Down")
	require.Len(t, parts, 2)

	cleanSQL := func(s string) string {
		s = strings.ReplaceAll(s, "-- +goose Up", "")
		s = strings.ReplaceAll(s, "-- +goose StatementBegin", "")
		s = strings.ReplaceAll(s, "-- +goose StatementEnd", "")
		return strings.TrimSpace(s)
	}

	upSQL := cleanSQL(parts[0])
	downSQL := cleanSQL(parts[1])

	t.Run("单根环境下存量孤儿文件可安全回填至唯一根", func(t *testing.T) {
		tx, err := pool.Begin(ctx)
		require.NoError(t, err)
		defer func() {
			_ = tx.Rollback(context.Background())
		}()

		// 清理事务内的表数据，确保单根环境无任何干扰
		_, err = tx.Exec(ctx, "DELETE FROM entries; DELETE FROM inodes; DELETE FROM disks;")
		require.NoError(t, err)

		disk1ID := newTestUUID()
		diskRoot1 := int64(1)
		dirIno := int64(20)
		fileIno := int64(21)
		orphanIsolatedIno := int64(22)

		// 1. 创建单根盘 1 与根节点 (root_ino 初始为 0)
		_, err = tx.Exec(ctx, `
			INSERT INTO inodes (ino, parent_ino, mode, nlink, uid, gid, root_ino)
			VALUES ($1, $1, $2, 2, 1000, 1000, 0);
		`, diskRoot1, int32(0o040755))
		require.NoError(t, err)

		_, err = tx.Exec(ctx, `
			INSERT INTO disks (disk_id, name, root_ino, max_bytes, used_bytes, used_inodes)
			VALUES ($1, 'root-disk-1', $2, 104857600, 0, 1);
		`, disk1ID, diskRoot1)
		require.NoError(t, err)

		// 2. 创建普通子目录与文件 (root_ino 初始为 0)
		_, err = tx.Exec(ctx, `
			INSERT INTO inodes (ino, parent_ino, mode, nlink, uid, gid, root_ino)
			VALUES ($1, $2, $3, 2, 1000, 1000, 0),
			       ($4, $1, $5, 1, 1000, 1000, 0);
		`, dirIno, diskRoot1, int32(0o040755), fileIno, int32(0o100644))
		require.NoError(t, err)

		_, err = tx.Exec(ctx, `
			INSERT INTO entries (parent_ino, name, ino)
			VALUES ($1, 'subdir', $2),
			       ($2, 'child.txt', $3);
		`, diskRoot1, dirIno, fileIno)
		require.NoError(t, err)

		// 3. 创建存量孤立孤儿文件 (无 entries，parent_ino = 0，root_ino = 0)
		_, err = tx.Exec(ctx, `
			INSERT INTO inodes (ino, parent_ino, mode, nlink, uid, gid, root_ino)
			VALUES ($1, 0, $2, 0, 1000, 1000, 0);
		`, orphanIsolatedIno, int32(0o100644))
		require.NoError(t, err)

		// 4. 执行 Up 迁移
		_, err = tx.Exec(ctx, upSQL)
		require.NoError(t, err)

		// 5. 验证单根环境下孤儿节点及常规节点均成功回填为 root_ino = 1
		var r1, rDir, rFile, rIsolated int64
		err = tx.QueryRow(ctx, "SELECT root_ino FROM inodes WHERE ino = $1", diskRoot1).Scan(&r1)
		require.NoError(t, err)
		assert.Equal(t, diskRoot1, r1)

		err = tx.QueryRow(ctx, "SELECT root_ino FROM inodes WHERE ino = $1", dirIno).Scan(&rDir)
		require.NoError(t, err)
		assert.Equal(t, diskRoot1, rDir)

		err = tx.QueryRow(ctx, "SELECT root_ino FROM inodes WHERE ino = $1", fileIno).Scan(&rFile)
		require.NoError(t, err)
		assert.Equal(t, diskRoot1, rFile)

		err = tx.QueryRow(ctx, "SELECT root_ino FROM inodes WHERE ino = $1", orphanIsolatedIno).Scan(&rIsolated)
		require.NoError(t, err)
		assert.Equal(t, diskRoot1, rIsolated, "单根环境下孤立孤儿文件必须安全回填为唯一根 1")

		// 6. 验证 disks.used_inodes 统计
		var usedInodes1 int64
		err = tx.QueryRow(ctx, "SELECT used_inodes FROM disks WHERE disk_id = $1", disk1ID).Scan(&usedInodes1)
		require.NoError(t, err)
		assert.Equal(t, int64(4), usedInodes1, "根盘 1 的 used_inodes 应包含根、子目录、子文件与孤立孤儿共 4 个 inode")
	})

	t.Run("多根环境下子盘边界截断与明确父目录孤儿正常回填", func(t *testing.T) {
		tx, err := pool.Begin(ctx)
		require.NoError(t, err)
		defer func() {
			_ = tx.Rollback(context.Background())
		}()

		_, err = tx.Exec(ctx, "DELETE FROM entries; DELETE FROM inodes; DELETE FROM disks;")
		require.NoError(t, err)

		disk1ID := newTestUUID()
		disk10ID := newTestUUID()

		diskRoot1 := int64(1)
		subdiskRoot10 := int64(10)
		subdiskChild11 := int64(11)
		dirIno := int64(20)
		fileIno := int64(21)
		orphanIno1 := int64(22)
		orphanIno2 := int64(23)
		orphanIno10 := int64(12)

		// 1. 创建根盘 1 与根节点
		_, err = tx.Exec(ctx, `
			INSERT INTO inodes (ino, parent_ino, mode, nlink, uid, gid, root_ino)
			VALUES ($1, $1, $2, 2, 1000, 1000, 0);
		`, diskRoot1, int32(0o040755))
		require.NoError(t, err)

		_, err = tx.Exec(ctx, `
			INSERT INTO disks (disk_id, name, root_ino, max_bytes, used_bytes, used_inodes)
			VALUES ($1, 'root-disk-1', $2, 104857600, 0, 1);
		`, disk1ID, diskRoot1)
		require.NoError(t, err)

		// 2. 创建挂载在根 1 下的子盘根 10 及其磁盘记录，以及挂在 10 下的文件 11
		_, err = tx.Exec(ctx, `
			INSERT INTO inodes (ino, parent_ino, mode, nlink, uid, gid, root_ino)
			VALUES ($1, $2, $3, 2, 1000, 1000, 0),
			       ($4, $1, $5, 1, 1000, 1000, 0);
		`, subdiskRoot10, diskRoot1, int32(0o040755), subdiskChild11, int32(0o100644))
		require.NoError(t, err)

		_, err = tx.Exec(ctx, `
			INSERT INTO disks (disk_id, name, root_ino, max_bytes, used_bytes, used_inodes)
			VALUES ($1, 'sub-disk-10', $2, 104857600, 0, 1);
		`, disk10ID, subdiskRoot10)
		require.NoError(t, err)

		_, err = tx.Exec(ctx, `
			INSERT INTO entries (parent_ino, name, ino)
			VALUES ($1, 'subdisk', $2),
			       ($2, 'child11.txt', $3);
		`, diskRoot1, subdiskRoot10, subdiskChild11)
		require.NoError(t, err)

		// 3. 创建挂在根 1 下的普通目录与孙文件
		_, err = tx.Exec(ctx, `
			INSERT INTO inodes (ino, parent_ino, mode, nlink, uid, gid, root_ino)
			VALUES ($1, $2, $3, 2, 1000, 1000, 0),
			       ($4, $1, $5, 1, 1000, 1000, 0);
		`, dirIno, diskRoot1, int32(0o040755), fileIno, int32(0o100644))
		require.NoError(t, err)

		_, err = tx.Exec(ctx, `
			INSERT INTO entries (parent_ino, name, ino)
			VALUES ($1, 'subdir', $2),
			       ($2, 'child.txt', $3);
		`, diskRoot1, dirIno, fileIno)
		require.NoError(t, err)

		// 4. 创建具有明确 parent_ino 的孤儿目录（无 entries）：
		// orphanIno1: parent_ino = dirIno (归属于根 1)
		// orphanIno2: parent_ino = orphanIno1 (二级孤儿目录，沿链归属于根 1)
		// orphanIno10: parent_ino = subdiskRoot10 (归属于子盘 10)
		_, err = tx.Exec(ctx, `
			INSERT INTO inodes (ino, parent_ino, mode, nlink, uid, gid, root_ino)
			VALUES ($1, $2, $3, 0, 1000, 1000, 0),
			       ($4, $1, $3, 0, 1000, 1000, 0),
			       ($5, $6, $3, 0, 1000, 1000, 0);
		`, orphanIno1, dirIno, int32(0o040755), orphanIno2, orphanIno10, subdiskRoot10)
		require.NoError(t, err)

		// 5. 执行 Up 迁移
		_, err = tx.Exec(ctx, upSQL)
		require.NoError(t, err)

		// 6. 验证子盘根 10 及其子节点 11 不会被根 1 覆盖，其 root_ino 均为 10
		var r10, r11, rOrphan10 int64
		err = tx.QueryRow(ctx, "SELECT root_ino FROM inodes WHERE ino = $1", subdiskRoot10).Scan(&r10)
		require.NoError(t, err)
		assert.Equal(t, subdiskRoot10, r10, "挂载在根 1 下的子盘根 10 的 root_ino 必须为 10，不能被根 1 覆盖")

		err = tx.QueryRow(ctx, "SELECT root_ino FROM inodes WHERE ino = $1", subdiskChild11).Scan(&r11)
		require.NoError(t, err)
		assert.Equal(t, subdiskRoot10, r11, "子盘下的子节点 11 的 root_ino 必须为 10")

		err = tx.QueryRow(ctx, "SELECT root_ino FROM inodes WHERE ino = $1", orphanIno10).Scan(&rOrphan10)
		require.NoError(t, err)
		assert.Equal(t, subdiskRoot10, rOrphan10, "子盘下的孤儿目录应继承父节点 10 的 root_ino")

		// 7. 验证根 1 树节点归属
		var r1, rDir, rFile, rOrphan1, rOrphan2 int64
		err = tx.QueryRow(ctx, "SELECT root_ino FROM inodes WHERE ino = $1", diskRoot1).Scan(&r1)
		require.NoError(t, err)
		assert.Equal(t, diskRoot1, r1)

		err = tx.QueryRow(ctx, "SELECT root_ino FROM inodes WHERE ino = $1", dirIno).Scan(&rDir)
		require.NoError(t, err)
		assert.Equal(t, diskRoot1, rDir)

		err = tx.QueryRow(ctx, "SELECT root_ino FROM inodes WHERE ino = $1", fileIno).Scan(&rFile)
		require.NoError(t, err)
		assert.Equal(t, diskRoot1, rFile)

		err = tx.QueryRow(ctx, "SELECT root_ino FROM inodes WHERE ino = $1", orphanIno1).Scan(&rOrphan1)
		require.NoError(t, err)
		assert.Equal(t, diskRoot1, rOrphan1, "根 1 下的一级孤儿目录应继承父目录的 root_ino")

		err = tx.QueryRow(ctx, "SELECT root_ino FROM inodes WHERE ino = $1", orphanIno2).Scan(&rOrphan2)
		require.NoError(t, err)
		assert.Equal(t, diskRoot1, rOrphan2, "二级孤儿目录应沿 parent_ino 链递归继承父孤儿目录的 root_ino")

		// 8. 验证 disks.used_inodes 统计
		var usedInodes10 int64
		err = tx.QueryRow(ctx, "SELECT used_inodes FROM disks WHERE disk_id = $1", disk10ID).Scan(&usedInodes10)
		require.NoError(t, err)
		assert.Equal(t, int64(3), usedInodes10, "子盘 10 应包含节点 10、11 及孤儿目录 12，共 3 个 inode")

		var usedInodes1 int64
		err = tx.QueryRow(ctx, "SELECT used_inodes FROM disks WHERE disk_id = $1", disk1ID).Scan(&usedInodes1)
		require.NoError(t, err)
		assert.Equal(t, int64(5), usedInodes1, "根盘 1 的 used_inodes 应包含节点 1、20、21 以及孤儿目录 22、23，共 5 个 inode")
	})

	t.Run("多根环境下存在归属不明孤儿或跨盘硬链接时阻止迁移并报错", func(t *testing.T) {
		t.Run("多根环境下存在 parent_ino = 0 且无 entries 的孤立孤儿时阻止迁移", func(t *testing.T) {
			tx, err := pool.Begin(ctx)
			require.NoError(t, err)
			defer func() {
				_ = tx.Rollback(context.Background())
			}()

			_, err = tx.Exec(ctx, "DELETE FROM entries; DELETE FROM inodes; DELETE FROM disks;")
			require.NoError(t, err)

			// 插入根盘 1 与子盘 10
			_, err = tx.Exec(ctx, `
				INSERT INTO inodes (ino, parent_ino, mode, nlink, uid, gid, root_ino)
				VALUES (1, 1, $1, 2, 1000, 1000, 0),
				       (10, 1, $1, 2, 1000, 1000, 0);
			`, int32(0o040755))
			require.NoError(t, err)

			_, err = tx.Exec(ctx, `
				INSERT INTO disks (disk_id, name, root_ino, max_bytes, used_bytes, used_inodes)
				VALUES ($1, 'disk-1', 1, 104857600, 0, 1),
				       ($2, 'disk-10', 10, 104857600, 0, 1);
			`, newTestUUID(), newTestUUID())
			require.NoError(t, err)

			// 插入归属不明的存量孤儿 (parent_ino = 0，无 entries)
			_, err = tx.Exec(ctx, `
				INSERT INTO inodes (ino, parent_ino, mode, nlink, uid, gid, root_ino)
				VALUES (99, 0, $1, 0, 1000, 1000, 0);
			`, int32(0o100644))
			require.NoError(t, err)

			// 执行迁移应被拒绝
			_, err = tx.Exec(ctx, upSQL)
			require.Error(t, err)
			assert.Contains(t, err.Error(), "migration blocked: found inodes with ambiguous or unresolvable root_ino ownership")
		})

		t.Run("多根环境下存在跨磁盘硬链接导致多根可达歧义时阻止迁移", func(t *testing.T) {
			tx, err := pool.Begin(ctx)
			require.NoError(t, err)
			defer func() {
				_ = tx.Rollback(context.Background())
			}()

			_, err = tx.Exec(ctx, "DELETE FROM entries; DELETE FROM inodes; DELETE FROM disks;")
			require.NoError(t, err)

			// 插入根盘 1 与子盘 10
			_, err = tx.Exec(ctx, `
				INSERT INTO inodes (ino, parent_ino, mode, nlink, uid, gid, root_ino)
				VALUES (1, 1, $1, 2, 1000, 1000, 0),
				       (10, 10, $1, 2, 1000, 1000, 0);
			`, int32(0o040755))
			require.NoError(t, err)

			_, err = tx.Exec(ctx, `
				INSERT INTO disks (disk_id, name, root_ino, max_bytes, used_bytes, used_inodes)
				VALUES ($1, 'disk-1', 1, 104857600, 0, 1),
				       ($2, 'disk-10', 10, 104857600, 0, 1);
			`, newTestUUID(), newTestUUID())
			require.NoError(t, err)

			// 插入跨盘硬链接文件 50（无唯一定义父节点，parent_ino = 0），分别在盘 1 和盘 10 下拥有 entry
			_, err = tx.Exec(ctx, `
				INSERT INTO inodes (ino, parent_ino, mode, nlink, uid, gid, root_ino)
				VALUES (50, 0, $1, 2, 1000, 1000, 0);
			`, int32(0o100644))
			require.NoError(t, err)

			_, err = tx.Exec(ctx, `
				INSERT INTO entries (parent_ino, name, ino)
				VALUES (1, 'link_in_root1', 50),
				       (10, 'link_in_disk10', 50);
			`)
			require.NoError(t, err)

			// 执行迁移应因跨盘硬链接歧义被拒绝
			_, err = tx.Exec(ctx, upSQL)
			require.Error(t, err)
			assert.Contains(t, err.Error(), "migration blocked: found inodes with ambiguous or unresolvable root_ino ownership")
		})
	})

	t.Run("Down 回滚保护测试：保留 disks 存量记录与表结构，仅还原约束与新增列", func(t *testing.T) {
		diskID := newTestUUID()
		rootIno := nextTestIno()

		t.Cleanup(func() {
			_, _ = pool.Exec(context.Background(), "DELETE FROM disks WHERE disk_id = $1", diskID)
		})

		// 插入存量磁盘数据
		_, err := pool.Exec(ctx, `
			INSERT INTO disks (disk_id, name, root_ino, max_bytes, used_bytes, used_inodes)
			VALUES ($1, 'rollback-safe-disk', $2, 50000000, 1024, 2);
		`, diskID, rootIno)
		require.NoError(t, err)

		// 执行 Down 回滚逻辑（在隔离事务中验证，验证后回滚该事务以保持测试库处于最新 Up 状态）
		tx, err := pool.Begin(ctx)
		require.NoError(t, err)
		defer func() {
			_ = tx.Rollback(context.Background())
		}()

		_, err = tx.Exec(ctx, downSQL)
		require.NoError(t, err)

		// 验证 disks 表依然存在，且存量记录完整保留
		var name string
		var maxBytes, usedBytes int64
		err = tx.QueryRow(ctx, "SELECT name, max_bytes, used_bytes FROM disks WHERE disk_id = $1", diskID).Scan(&name, &maxBytes, &usedBytes)
		require.NoError(t, err)
		assert.Equal(t, "rollback-safe-disk", name)
		assert.Equal(t, int64(50000000), maxBytes)
		assert.Equal(t, int64(1024), usedBytes)

		// 验证 used_inodes 列已被删除
		var columnExists bool
		err = tx.QueryRow(ctx, `
			SELECT EXISTS (
				SELECT 1 FROM information_schema.columns
				WHERE table_name = 'disks' AND column_name = 'used_inodes'
			);
		`).Scan(&columnExists)
		require.NoError(t, err)
		assert.False(t, columnExists, "Down 回滚后 used_inodes 列应不存在")
	})
}
