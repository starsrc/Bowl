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

// 静态验证 db.Querier、db.Store 与 *db.Queries 完备实现 InodeAttrQuerier 与 SymlinkQuerier 接口契约。
var (
	_ InodeAttrQuerier = (db.Querier)(nil)
	_ InodeAttrQuerier = (db.Store)(nil)
	_ InodeAttrQuerier = (*db.Queries)(nil)

	_ SymlinkQuerier = (db.Querier)(nil)
	_ SymlinkQuerier = (db.Store)(nil)
	_ SymlinkQuerier = (*db.Queries)(nil)
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
				Name:      "test.txt",
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
				Name:      "test.txt",
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
				SymlinkTarget: pgtype.Text{String: "/var/target", Valid: true},
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
				SymlinkTarget: pgtype.Text{String: "/etc/hosts", Valid: true},
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
}
