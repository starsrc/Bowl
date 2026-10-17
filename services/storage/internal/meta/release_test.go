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

	"github.com/starsrc/bowl/services/storage/internal/db"
	dbmock "github.com/starsrc/bowl/services/storage/internal/db/mock"
	"github.com/starsrc/bowl/services/storage/internal/meta"
)

func TestRelease_InputValidation(t *testing.T) {
	ctx := context.Background()

	for _, tc := range []struct {
		name string
		ino  uint64
		fh   uint64
	}{
		{
			name: "ino 为 0",
			ino:  0,
			fh:   1,
		},
		{
			name: "ino 超过 math.MaxInt64",
			ino:  uint64(math.MaxInt64) + 1,
			fh:   1,
		},
		{
			name: "fh 为 0",
			ino:  1,
			fh:   0,
		},
		{
			name: "fh 超过 math.MaxInt64",
			ino:  1,
			fh:   uint64(math.MaxInt64) + 1,
		},
		{
			name: "ino 与 fh 均为 0",
			ino:  0,
			fh:   0,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mockStore := dbmock.NewMockStore(t)
			mgr := meta.New(mockStore)

			err := mgr.Release(ctx, meta.ReleaseParams{
				Ino: tc.ino,
				Fh:  tc.fh,
			})
			require.Error(t, err)
			assert.True(t, errors.Is(err, meta.ErrInvalidArgument))
		})
	}
}

func TestRelease_PositivePath(t *testing.T) {
	ctx := context.Background()
	now := time.Now().Truncate(time.Microsecond)
	ino := uint64(100)
	fh := uint64(1)

	baseInode := db.Inode{
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

	t.Run("正常节点释放 (nlink > 0): 销毁句柄且保留 Inode 行", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockWriteTx(mockStore, mockQuerier)

		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(ino)).
			Return(baseInode, nil).
			Once()

		mockQuerier.EXPECT().
			DeleteFileHandle(mock.Anything, db.DeleteFileHandleParams{
				Fh:  int64(fh),
				Ino: int64(ino),
			}).
			Return(int64(fh), nil).
			Once()

		err := mgr.Release(ctx, meta.ReleaseParams{
			Ino: ino,
			Fh:  fh,
		})
		require.NoError(t, err)
	})

	t.Run("多句柄并发打开的 Orphan Inode (nlink == 0): 释放其中一个句柄，仍有剩余句柄时不删除 Inode", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockWriteTx(mockStore, mockQuerier)

		orphanInode := baseInode
		orphanInode.Nlink = 0

		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(ino)).
			Return(orphanInode, nil).
			Once()

		mockQuerier.EXPECT().
			DeleteFileHandle(mock.Anything, db.DeleteFileHandleParams{
				Fh:  int64(fh),
				Ino: int64(ino),
			}).
			Return(int64(fh), nil).
			Once()

		mockQuerier.EXPECT().
			HasFileHandlesByIno(mock.Anything, int64(ino)).
			Return(true, nil).
			Once()

		err := mgr.Release(ctx, meta.ReleaseParams{
			Ino: ino,
			Fh:  fh,
		})
		require.NoError(t, err)
	})

	t.Run("最后一个句柄释放的 Orphan Inode (nlink == 0): 无剩余活跃句柄时物理回收 Inode", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockWriteTx(mockStore, mockQuerier)

		orphanInode := baseInode
		orphanInode.Nlink = 0

		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(ino)).
			Return(orphanInode, nil).
			Once()

		mockQuerier.EXPECT().
			DeleteFileHandle(mock.Anything, db.DeleteFileHandleParams{
				Fh:  int64(fh),
				Ino: int64(ino),
			}).
			Return(int64(fh), nil).
			Once()

		mockQuerier.EXPECT().
			HasFileHandlesByIno(mock.Anything, int64(ino)).
			Return(false, nil).
			Once()

		mockQuerier.EXPECT().
			DeleteInode(mock.Anything, int64(ino)).
			Return(nil).
			Once()

		err := mgr.Release(ctx, meta.ReleaseParams{
			Ino: ino,
			Fh:  fh,
		})
		require.NoError(t, err)
	})
}

func TestRelease_NegativeAndErrorPaths(t *testing.T) {
	ctx := context.Background()
	now := time.Now().Truncate(time.Microsecond)
	ino := uint64(100)
	fh := uint64(1)

	baseInode := db.Inode{
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

	t.Run("目标节点不存在返回 ErrNotFound", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockWriteTx(mockStore, mockQuerier)

		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(ino)).
			Return(db.Inode{}, pgx.ErrNoRows).
			Once()

		err := mgr.Release(ctx, meta.ReleaseParams{
			Ino: ino,
			Fh:  fh,
		})
		require.Error(t, err)
		assert.True(t, errors.Is(err, meta.ErrNotFound))
	})

	t.Run("目标节点为目录返回 ErrIsDir", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockWriteTx(mockStore, mockQuerier)

		dirInode := baseInode
		dirInode.Mode = int32(meta.S_IFDIR | 0o755)

		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(ino)).
			Return(dirInode, nil).
			Once()

		err := mgr.Release(ctx, meta.ReleaseParams{
			Ino: ino,
			Fh:  fh,
		})
		require.Error(t, err)
		assert.True(t, errors.Is(err, meta.ErrIsDir))
	})

	t.Run("句柄不存在或归属不匹配返回 ErrBadFileHandle", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockWriteTx(mockStore, mockQuerier)

		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(ino)).
			Return(baseInode, nil).
			Once()

		mockQuerier.EXPECT().
			DeleteFileHandle(mock.Anything, db.DeleteFileHandleParams{
				Fh:  int64(fh),
				Ino: int64(ino),
			}).
			Return(int64(0), pgx.ErrNoRows).
			Once()

		err := mgr.Release(ctx, meta.ReleaseParams{
			Ino: ino,
			Fh:  fh,
		})
		require.Error(t, err)
		assert.True(t, errors.Is(err, meta.ErrBadFileHandle))
	})

	t.Run("二次重复释放同一句柄返回 ErrBadFileHandle", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockWriteTx(mockStore, mockQuerier)

		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(ino)).
			Return(baseInode, nil).
			Once()

		mockQuerier.EXPECT().
			DeleteFileHandle(mock.Anything, db.DeleteFileHandleParams{
				Fh:  int64(fh),
				Ino: int64(ino),
			}).
			Return(int64(0), pgx.ErrNoRows).
			Once()

		err := mgr.Release(ctx, meta.ReleaseParams{
			Ino: ino,
			Fh:  fh,
		})
		require.Error(t, err)
		assert.True(t, errors.Is(err, meta.ErrBadFileHandle))
	})

	t.Run("GetInodeForUpdate 底层数据库错误", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockWriteTx(mockStore, mockQuerier)

		dbErr := errors.New("db query failed")
		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(ino)).
			Return(db.Inode{}, dbErr).
			Once()

		err := mgr.Release(ctx, meta.ReleaseParams{
			Ino: ino,
			Fh:  fh,
		})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "release tx")
		assert.True(t, errors.Is(err, dbErr))
	})

	t.Run("DeleteFileHandle 底层数据库错误", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockWriteTx(mockStore, mockQuerier)

		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(ino)).
			Return(baseInode, nil).
			Once()

		dbErr := errors.New("db delete handle failed")
		mockQuerier.EXPECT().
			DeleteFileHandle(mock.Anything, db.DeleteFileHandleParams{
				Fh:  int64(fh),
				Ino: int64(ino),
			}).
			Return(int64(0), dbErr).
			Once()

		err := mgr.Release(ctx, meta.ReleaseParams{
			Ino: ino,
			Fh:  fh,
		})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "release tx")
		assert.True(t, errors.Is(err, dbErr))
	})

	t.Run("HasFileHandlesByIno 底层数据库错误", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockWriteTx(mockStore, mockQuerier)

		orphanInode := baseInode
		orphanInode.Nlink = 0

		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(ino)).
			Return(orphanInode, nil).
			Once()

		mockQuerier.EXPECT().
			DeleteFileHandle(mock.Anything, db.DeleteFileHandleParams{
				Fh:  int64(fh),
				Ino: int64(ino),
			}).
			Return(int64(fh), nil).
			Once()

		dbErr := errors.New("db check handles failed")
		mockQuerier.EXPECT().
			HasFileHandlesByIno(mock.Anything, int64(ino)).
			Return(false, dbErr).
			Once()

		err := mgr.Release(ctx, meta.ReleaseParams{
			Ino: ino,
			Fh:  fh,
		})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "release tx")
		assert.True(t, errors.Is(err, dbErr))
	})

	t.Run("DeleteInode 底层数据库错误", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockWriteTx(mockStore, mockQuerier)

		orphanInode := baseInode
		orphanInode.Nlink = 0

		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(ino)).
			Return(orphanInode, nil).
			Once()

		mockQuerier.EXPECT().
			DeleteFileHandle(mock.Anything, db.DeleteFileHandleParams{
				Fh:  int64(fh),
				Ino: int64(ino),
			}).
			Return(int64(fh), nil).
			Once()

		mockQuerier.EXPECT().
			HasFileHandlesByIno(mock.Anything, int64(ino)).
			Return(false, nil).
			Once()

		dbErr := errors.New("db delete inode failed")
		mockQuerier.EXPECT().
			DeleteInode(mock.Anything, int64(ino)).
			Return(dbErr).
			Once()

		err := mgr.Release(ctx, meta.ReleaseParams{
			Ino: ino,
			Fh:  fh,
		})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "release tx")
		assert.True(t, errors.Is(err, dbErr))
	})
}
