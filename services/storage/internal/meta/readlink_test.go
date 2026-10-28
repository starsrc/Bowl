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

func TestReadlink_EmptySymlinkTargetReturnsErrIO(t *testing.T) {
	ctx := context.Background()

	t.Run("符号链接目标为空切片时返回 ErrIO (EIO)", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mgr := meta.New(mockStore)

		ino := uint64(100)
		mockStore.EXPECT().
			GetSymlinkTarget(mock.Anything, int64(ino)).
			Return(db.GetSymlinkTargetRow{
				Mode:          int32(meta.S_IFLNK | 0o777),
				SymlinkTarget: []byte{},
			}, nil).
			Once()

		target, err := mgr.Readlink(ctx, ino)
		assert.Nil(t, target)
		assert.ErrorIs(t, err, meta.ErrIO)
	})

	t.Run("符号链接目标为 nil 时返回 ErrIO (EIO)", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mgr := meta.New(mockStore)

		ino := uint64(101)
		mockStore.EXPECT().
			GetSymlinkTarget(mock.Anything, int64(ino)).
			Return(db.GetSymlinkTargetRow{
				Mode:          int32(meta.S_IFLNK | 0o777),
				SymlinkTarget: nil,
			}, nil).
			Once()

		target, err := mgr.Readlink(ctx, ino)
		assert.Nil(t, target)
		assert.ErrorIs(t, err, meta.ErrIO)
	})
}

func TestReadlink_AtimeUpdate(t *testing.T) {
	ctx := context.Background()
	now := time.Now()

	t.Run("StrictAtime 模式下超过 1 秒触发 TouchInodeAtime", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mgr := meta.New(mockStore, meta.WithAtimeMode(meta.AtimeModeStrictAtime))

		ino := uint64(102)
		targetBytes := []byte("/usr/local/bin")
		mockStore.EXPECT().
			GetSymlinkTarget(mock.Anything, int64(ino)).
			Return(db.GetSymlinkTargetRow{
				Mode:          int32(meta.S_IFLNK | 0o777),
				SymlinkTarget: targetBytes,
			}, nil).
			Once()

		mockStore.EXPECT().
			GetInode(mock.Anything, int64(ino)).
			Return(db.Inode{
				Ino:   int64(ino),
				Atime: pgtype.Timestamptz{Time: now.Add(-5 * time.Second), Valid: true},
				Mtime: pgtype.Timestamptz{Time: now.Add(-10 * time.Second), Valid: true},
				Ctime: pgtype.Timestamptz{Time: now.Add(-10 * time.Second), Valid: true},
			}, nil).
			Once()

		mockStore.EXPECT().
			TouchInodeAtime(mock.Anything, int64(ino)).
			Return(nil).
			Once()

		result, err := mgr.Readlink(ctx, ino)
		require.NoError(t, err)
		assert.Equal(t, targetBytes, result)
	})

	t.Run("NoAtime 模式下不触发 TouchInodeAtime 与 GetInode 查询", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mgr := meta.New(mockStore, meta.WithAtimeMode(meta.AtimeModeNoAtime))

		ino := uint64(103)
		targetBytes := []byte("/etc/resolv.conf")
		mockStore.EXPECT().
			GetSymlinkTarget(mock.Anything, int64(ino)).
			Return(db.GetSymlinkTargetRow{
				Mode:          int32(meta.S_IFLNK | 0o777),
				SymlinkTarget: targetBytes,
			}, nil).
			Once()

		result, err := mgr.Readlink(ctx, ino)
		require.NoError(t, err)
		assert.Equal(t, targetBytes, result)

		mockStore.AssertNotCalled(t, "GetInode", mock.Anything, mock.Anything)
		mockStore.AssertNotCalled(t, "TouchInodeAtime", mock.Anything, mock.Anything)
	})

	t.Run("RelAtime 模式下 atime < mtime 触发 TouchInodeAtime", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mgr := meta.New(mockStore, meta.WithAtimeMode(meta.AtimeModeRelAtime))

		ino := uint64(104)
		targetBytes := []byte("/data/link_mtime")
		mockStore.EXPECT().
			GetSymlinkTarget(mock.Anything, int64(ino)).
			Return(db.GetSymlinkTargetRow{
				Mode:          int32(meta.S_IFLNK | 0o777),
				SymlinkTarget: targetBytes,
			}, nil).
			Once()

		mockStore.EXPECT().
			GetInode(mock.Anything, int64(ino)).
			Return(db.Inode{
				Ino:   int64(ino),
				Atime: pgtype.Timestamptz{Time: now.Add(-2 * time.Hour), Valid: true},
				Mtime: pgtype.Timestamptz{Time: now.Add(-1 * time.Hour), Valid: true},
				Ctime: pgtype.Timestamptz{Time: now.Add(-3 * time.Hour), Valid: true},
			}, nil).
			Once()

		mockStore.EXPECT().
			TouchInodeAtime(mock.Anything, int64(ino)).
			Return(nil).
			Once()

		result, err := mgr.Readlink(ctx, ino)
		require.NoError(t, err)
		assert.Equal(t, targetBytes, result)
	})

	t.Run("RelAtime 模式下 atime < ctime 触发 TouchInodeAtime", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mgr := meta.New(mockStore, meta.WithAtimeMode(meta.AtimeModeRelAtime))

		ino := uint64(105)
		targetBytes := []byte("/data/link_ctime")
		mockStore.EXPECT().
			GetSymlinkTarget(mock.Anything, int64(ino)).
			Return(db.GetSymlinkTargetRow{
				Mode:          int32(meta.S_IFLNK | 0o777),
				SymlinkTarget: targetBytes,
			}, nil).
			Once()

		mockStore.EXPECT().
			GetInode(mock.Anything, int64(ino)).
			Return(db.Inode{
				Ino:   int64(ino),
				Atime: pgtype.Timestamptz{Time: now.Add(-2 * time.Hour), Valid: true},
				Mtime: pgtype.Timestamptz{Time: now.Add(-3 * time.Hour), Valid: true},
				Ctime: pgtype.Timestamptz{Time: now.Add(-1 * time.Hour), Valid: true},
			}, nil).
			Once()

		mockStore.EXPECT().
			TouchInodeAtime(mock.Anything, int64(ino)).
			Return(nil).
			Once()

		result, err := mgr.Readlink(ctx, ino)
		require.NoError(t, err)
		assert.Equal(t, targetBytes, result)
	})

	t.Run("RelAtime 模式下 now - atime >= 24h 触发 TouchInodeAtime", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mgr := meta.New(mockStore, meta.WithAtimeMode(meta.AtimeModeRelAtime))

		ino := uint64(106)
		targetBytes := []byte("/data/link_24h")
		mockStore.EXPECT().
			GetSymlinkTarget(mock.Anything, int64(ino)).
			Return(db.GetSymlinkTargetRow{
				Mode:          int32(meta.S_IFLNK | 0o777),
				SymlinkTarget: targetBytes,
			}, nil).
			Once()

		mockStore.EXPECT().
			GetInode(mock.Anything, int64(ino)).
			Return(db.Inode{
				Ino:   int64(ino),
				Atime: pgtype.Timestamptz{Time: now.Add(-25 * time.Hour), Valid: true},
				Mtime: pgtype.Timestamptz{Time: now.Add(-25 * time.Hour), Valid: true},
				Ctime: pgtype.Timestamptz{Time: now.Add(-25 * time.Hour), Valid: true},
			}, nil).
			Once()

		mockStore.EXPECT().
			TouchInodeAtime(mock.Anything, int64(ino)).
			Return(nil).
			Once()

		result, err := mgr.Readlink(ctx, ino)
		require.NoError(t, err)
		assert.Equal(t, targetBytes, result)
	})

	t.Run("RelAtime 模式下新鲜 atime（未早于 mtime/ctime 且未满 24 小时）不触发 TouchInodeAtime", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mgr := meta.New(mockStore, meta.WithAtimeMode(meta.AtimeModeRelAtime))

		ino := uint64(107)
		targetBytes := []byte("/data/link_fresh")
		mockStore.EXPECT().
			GetSymlinkTarget(mock.Anything, int64(ino)).
			Return(db.GetSymlinkTargetRow{
				Mode:          int32(meta.S_IFLNK | 0o777),
				SymlinkTarget: targetBytes,
			}, nil).
			Once()

		mockStore.EXPECT().
			GetInode(mock.Anything, int64(ino)).
			Return(db.Inode{
				Ino:   int64(ino),
				Atime: pgtype.Timestamptz{Time: now.Add(-1 * time.Hour), Valid: true},
				Mtime: pgtype.Timestamptz{Time: now.Add(-1 * time.Hour), Valid: true},
				Ctime: pgtype.Timestamptz{Time: now.Add(-1 * time.Hour), Valid: true},
			}, nil).
			Once()

		result, err := mgr.Readlink(ctx, ino)
		require.NoError(t, err)
		assert.Equal(t, targetBytes, result)

		mockStore.AssertNotCalled(t, "TouchInodeAtime", mock.Anything, mock.Anything)
	})

	t.Run("StrictAtime 模式下 now - atime < 1s 受节流保护不触发 TouchInodeAtime", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mgr := meta.New(mockStore, meta.WithAtimeMode(meta.AtimeModeStrictAtime))

		ino := uint64(108)
		targetBytes := []byte("/data/link_throttle")
		mockStore.EXPECT().
			GetSymlinkTarget(mock.Anything, int64(ino)).
			Return(db.GetSymlinkTargetRow{
				Mode:          int32(meta.S_IFLNK | 0o777),
				SymlinkTarget: targetBytes,
			}, nil).
			Once()

		mockStore.EXPECT().
			GetInode(mock.Anything, int64(ino)).
			Return(db.Inode{
				Ino:   int64(ino),
				Atime: pgtype.Timestamptz{Time: now.Add(-100 * time.Millisecond), Valid: true},
				Mtime: pgtype.Timestamptz{Time: now.Add(-10 * time.Second), Valid: true},
				Ctime: pgtype.Timestamptz{Time: now.Add(-10 * time.Second), Valid: true},
			}, nil).
			Once()

		result, err := mgr.Readlink(ctx, ino)
		require.NoError(t, err)
		assert.Equal(t, targetBytes, result)

		mockStore.AssertNotCalled(t, "TouchInodeAtime", mock.Anything, mock.Anything)
	})
}

func TestReadlink_AdditionalValidationAndErrors(t *testing.T) {
	ctx := context.Background()

	t.Run("ino 边界与非法校验", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mgr := meta.New(mockStore)

		_, err := mgr.Readlink(ctx, 0)
		assert.ErrorIs(t, err, meta.ErrInvalidArgument)

		_, err = mgr.Readlink(ctx, uint64(math.MaxInt64)+1)
		assert.ErrorIs(t, err, meta.ErrInvalidArgument)
	})

	t.Run("目标不存在返回 ErrNotFound", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mgr := meta.New(mockStore)

		ino := uint64(999)
		mockStore.EXPECT().
			GetSymlinkTarget(mock.Anything, int64(ino)).
			Return(db.GetSymlinkTargetRow{}, pgx.ErrNoRows).
			Once()

		_, err := mgr.Readlink(ctx, ino)
		assert.ErrorIs(t, err, meta.ErrNotFound)
	})

	t.Run("数据库错误返回包装错误", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mgr := meta.New(mockStore)

		ino := uint64(888)
		dbErr := errors.New("db disconnected")
		mockStore.EXPECT().
			GetSymlinkTarget(mock.Anything, int64(ino)).
			Return(db.GetSymlinkTargetRow{}, dbErr).
			Once()

		_, err := mgr.Readlink(ctx, ino)
		assert.ErrorIs(t, err, dbErr)
	})
}
