package meta_test

import (
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



func ptr[T any](v T) *T {
	return &v
}

func TestSetAttr_PositivePath(t *testing.T) {
	ctx := context.Background()
	now := time.Now().Truncate(time.Microsecond)
	ino := uint64(42)

	baseInodeRow := db.Inode{
		Ino:        int64(ino),
		Generation: 1,
		Mode:       int32(0o100644), // regular file, rw-r--r--
		Size:       1024,
		Nlink:      1,
		Uid:        1000,
		Gid:        1000,
		Rdev:       0,
		Atime:      pgtype.Timestamptz{Time: now.Add(-time.Hour), Valid: true},
		Mtime:      pgtype.Timestamptz{Time: now.Add(-30 * time.Minute), Valid: true},
		Ctime:      pgtype.Timestamptz{Time: now.Add(-10 * time.Minute), Valid: true},
	}

	t.Run("successfully update mode preserving file type bits", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockWriteTx(mockStore, mockQuerier)

		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(ino)).
			Return(baseInodeRow, nil).
			Once()

		expectedNewMode := int32(0o100755) // preserved 0o100000 | 0o755
		updatedRow := baseInodeRow
		updatedRow.Mode = expectedNewMode
		updatedRow.Ctime = pgtype.Timestamptz{Time: now, Valid: true}

		mockQuerier.EXPECT().
			UpdateInodeAttr(mock.Anything, db.UpdateInodeAttrParams{
				Ino:         int64(ino),
				Mode:        expectedNewMode,
				Size:        baseInodeRow.Size,
				Uid:         baseInodeRow.Uid,
				Gid:         baseInodeRow.Gid,
				SetAtimeNow: false,
				Atime:       baseInodeRow.Atime,
				SetMtimeNow: false,
				Mtime:       baseInodeRow.Mtime,
			}).
			Return(updatedRow, nil).
			Once()

		newMode := uint32(0o755)
		inode, err := mgr.SetAttr(ctx, ino, meta.SetAttrInput{
			Mode: &newMode,
		})
		require.NoError(t, err)
		require.NotNil(t, inode)

		assert.Equal(t, ino, inode.Ino)
		assert.Equal(t, uint32(0o100755), inode.Attr.Mode)
		assert.Equal(t, uint64(1024), inode.Attr.Size)
		assert.Equal(t, meta.DefaultBlockSize, inode.Attr.Blksize)
		assert.Equal(t, uint64(2), inode.Attr.Blocks)
		assert.Equal(t, updatedRow.Ctime.Time, inode.Attr.Ctime)
	})

	t.Run("successfully update uid and gid", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockWriteTx(mockStore, mockQuerier)

		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(ino)).
			Return(baseInodeRow, nil).
			Once()

		updatedRow := baseInodeRow
		updatedRow.Uid = 2000
		updatedRow.Gid = 3000
		updatedRow.Ctime = pgtype.Timestamptz{Time: now, Valid: true}

		mockQuerier.EXPECT().
			UpdateInodeAttr(mock.Anything, db.UpdateInodeAttrParams{
				Ino:         int64(ino),
				Mode:        baseInodeRow.Mode,
				Size:        baseInodeRow.Size,
				Uid:         2000,
				Gid:         3000,
				SetAtimeNow: false,
				Atime:       baseInodeRow.Atime,
				SetMtimeNow: false,
				Mtime:       baseInodeRow.Mtime,
			}).
			Return(updatedRow, nil).
			Once()

		inode, err := mgr.SetAttr(ctx, ino, meta.SetAttrInput{
			UID: ptr(uint32(2000)),
			GID: ptr(uint32(3000)),
		})
		require.NoError(t, err)
		require.NotNil(t, inode)

		assert.Equal(t, uint32(2000), inode.Attr.UID)
		assert.Equal(t, uint32(3000), inode.Attr.GID)
	})

	t.Run("updating size triggers mtime update when mtime is not specified", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockWriteTx(mockStore, mockQuerier)

		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(ino)).
			Return(baseInodeRow, nil).
			Once()

		updatedRow := baseInodeRow
		updatedRow.Size = 4097
		updatedRow.Mtime = pgtype.Timestamptz{Time: now, Valid: true}
		updatedRow.Ctime = pgtype.Timestamptz{Time: now, Valid: true}

		mockQuerier.EXPECT().
			UpdateInodeAttr(mock.Anything, db.UpdateInodeAttrParams{
				Ino:         int64(ino),
				Mode:        baseInodeRow.Mode,
				Size:        4097,
				Uid:         baseInodeRow.Uid,
				Gid:         baseInodeRow.Gid,
				SetAtimeNow: false,
				Atime:       baseInodeRow.Atime,
				SetMtimeNow: true,
				Mtime:       pgtype.Timestamptz{Valid: false},
			}).
			Return(updatedRow, nil).
			Once()

		inode, err := mgr.SetAttr(ctx, ino, meta.SetAttrInput{
			Size: ptr(uint64(4097)),
		})
		require.NoError(t, err)
		require.NotNil(t, inode)

		assert.Equal(t, uint64(4097), inode.Attr.Size)
		assert.Equal(t, uint64(9), inode.Attr.Blocks) // (4097 + 511) / 512 = 9
		assert.Equal(t, meta.DefaultBlockSize, inode.Attr.Blksize)
		assert.Equal(t, updatedRow.Mtime.Time, inode.Attr.Mtime)
	})

	t.Run("updating size with explicit mtime does not set mtime to now", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockWriteTx(mockStore, mockQuerier)

		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(ino)).
			Return(baseInodeRow, nil).
			Once()

		customMtime := now.Add(2 * time.Hour)
		updatedRow := baseInodeRow
		updatedRow.Size = 2048
		updatedRow.Mtime = pgtype.Timestamptz{Time: customMtime, Valid: true}
		updatedRow.Ctime = pgtype.Timestamptz{Time: now, Valid: true}

		mockQuerier.EXPECT().
			UpdateInodeAttr(mock.Anything, db.UpdateInodeAttrParams{
				Ino:         int64(ino),
				Mode:        baseInodeRow.Mode,
				Size:        2048,
				Uid:         baseInodeRow.Uid,
				Gid:         baseInodeRow.Gid,
				SetAtimeNow: false,
				Atime:       baseInodeRow.Atime,
				SetMtimeNow: false,
				Mtime:       pgtype.Timestamptz{Time: customMtime, Valid: true},
			}).
			Return(updatedRow, nil).
			Once()

		inode, err := mgr.SetAttr(ctx, ino, meta.SetAttrInput{
			Size:  ptr(uint64(2048)),
			Mtime: &customMtime,
		})
		require.NoError(t, err)
		require.NotNil(t, inode)

		assert.Equal(t, uint64(2048), inode.Attr.Size)
		assert.Equal(t, customMtime, inode.Attr.Mtime)
	})

	t.Run("updating atime and mtime with explicit timestamps", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockWriteTx(mockStore, mockQuerier)

		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(ino)).
			Return(baseInodeRow, nil).
			Once()

		customAtime := now.Add(-5 * time.Minute)
		customMtime := now.Add(-1 * time.Minute)
		updatedRow := baseInodeRow
		updatedRow.Atime = pgtype.Timestamptz{Time: customAtime, Valid: true}
		updatedRow.Mtime = pgtype.Timestamptz{Time: customMtime, Valid: true}
		updatedRow.Ctime = pgtype.Timestamptz{Time: now, Valid: true}

		mockQuerier.EXPECT().
			UpdateInodeAttr(mock.Anything, db.UpdateInodeAttrParams{
				Ino:         int64(ino),
				Mode:        baseInodeRow.Mode,
				Size:        baseInodeRow.Size,
				Uid:         baseInodeRow.Uid,
				Gid:         baseInodeRow.Gid,
				SetAtimeNow: false,
				Atime:       pgtype.Timestamptz{Time: customAtime, Valid: true},
				SetMtimeNow: false,
				Mtime:       pgtype.Timestamptz{Time: customMtime, Valid: true},
			}).
			Return(updatedRow, nil).
			Once()

		inode, err := mgr.SetAttr(ctx, ino, meta.SetAttrInput{
			Atime: &customAtime,
			Mtime: &customMtime,
		})
		require.NoError(t, err)
		require.NotNil(t, inode)

		assert.Equal(t, customAtime, inode.Attr.Atime)
		assert.Equal(t, customMtime, inode.Attr.Mtime)
	})

	t.Run("updating atime_now and mtime_now sets flags true", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockWriteTx(mockStore, mockQuerier)

		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(ino)).
			Return(baseInodeRow, nil).
			Once()

		updatedRow := baseInodeRow
		updatedRow.Atime = pgtype.Timestamptz{Time: now, Valid: true}
		updatedRow.Mtime = pgtype.Timestamptz{Time: now, Valid: true}
		updatedRow.Ctime = pgtype.Timestamptz{Time: now, Valid: true}

		mockQuerier.EXPECT().
			UpdateInodeAttr(mock.Anything, db.UpdateInodeAttrParams{
				Ino:         int64(ino),
				Mode:        baseInodeRow.Mode,
				Size:        baseInodeRow.Size,
				Uid:         baseInodeRow.Uid,
				Gid:         baseInodeRow.Gid,
				SetAtimeNow: true,
				Atime:       pgtype.Timestamptz{Valid: false},
				SetMtimeNow: true,
				Mtime:       pgtype.Timestamptz{Valid: false},
			}).
			Return(updatedRow, nil).
			Once()

		inode, err := mgr.SetAttr(ctx, ino, meta.SetAttrInput{
			AtimeNow: true,
			MtimeNow: true,
		})
		require.NoError(t, err)
		require.NotNil(t, inode)

		assert.Equal(t, updatedRow.Atime.Time, inode.Attr.Atime)
		assert.Equal(t, updatedRow.Mtime.Time, inode.Attr.Mtime)
	})

	t.Run("successfully update orphaned inode attributes with nlink 0 (fchmod and ftruncate scenarios)", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockWriteTx(mockStore, mockQuerier)

		orphanedRow := baseInodeRow
		orphanedRow.Nlink = 0 // 已解链孤儿节点（文件已被 unlink 但句柄依然打开）

		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(ino)).
			Return(orphanedRow, nil).
			Once()

		expectedNewMode := int32(0o100600)
		updatedRow := orphanedRow
		updatedRow.Mode = expectedNewMode
		updatedRow.Size = 8192
		updatedRow.Mtime = pgtype.Timestamptz{Time: now, Valid: true}
		updatedRow.Ctime = pgtype.Timestamptz{Time: now, Valid: true}

		mockQuerier.EXPECT().
			UpdateInodeAttr(mock.Anything, db.UpdateInodeAttrParams{
				Ino:         int64(ino),
				Mode:        expectedNewMode,
				Size:        8192,
				Uid:         orphanedRow.Uid,
				Gid:         orphanedRow.Gid,
				SetAtimeNow: false,
				Atime:       orphanedRow.Atime,
				SetMtimeNow: true,
				Mtime:       pgtype.Timestamptz{Valid: false},
			}).
			Return(updatedRow, nil).
			Once()

		inode, err := mgr.SetAttr(ctx, ino, meta.SetAttrInput{
			Mode: ptr(uint32(0o600)), // fchmod 场景
			Size: ptr(uint64(8192)),  // ftruncate 场景
		})
		require.NoError(t, err)
		require.NotNil(t, inode)

		assert.Equal(t, ino, inode.Ino)
		assert.Equal(t, uint32(0), inode.Attr.Nlink)
		assert.Equal(t, uint32(0o100600), inode.Attr.Mode)
		assert.Equal(t, uint64(8192), inode.Attr.Size)
		assert.Equal(t, uint64(16), inode.Attr.Blocks) // (8192 + 511) / 512 = 16
		assert.Equal(t, updatedRow.Ctime.Time, inode.Attr.Ctime)
	})

	t.Run("node not found returns ErrNotFound", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockWriteTx(mockStore, mockQuerier)

		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(ino)).
			Return(db.Inode{}, pgx.ErrNoRows).
			Once()

		inode, err := mgr.SetAttr(ctx, ino, meta.SetAttrInput{
			Mode: ptr(uint32(0o777)),
		})
		assert.Nil(t, inode)
		assert.ErrorIs(t, err, meta.ErrNotFound)
		mockQuerier.AssertNotCalled(t, "UpdateInodeAttr", mock.Anything, mock.Anything)
	})

	t.Run("db error in GetInodeForUpdate returns wrapped error", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockWriteTx(mockStore, mockQuerier)

		dbErr := errors.New("db lock acquisition failure")
		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(ino)).
			Return(db.Inode{}, dbErr).
			Once()

		inode, err := mgr.SetAttr(ctx, ino, meta.SetAttrInput{
			Mode: ptr(uint32(0o777)),
		})
		assert.Nil(t, inode)
		assert.ErrorIs(t, err, dbErr)
		mockQuerier.AssertNotCalled(t, "UpdateInodeAttr", mock.Anything, mock.Anything)
	})

	t.Run("db error in UpdateInodeAttr returns wrapped error", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockWriteTx(mockStore, mockQuerier)

		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(ino)).
			Return(baseInodeRow, nil).
			Once()

		dbErr := errors.New("db update disk failure")
		mockQuerier.EXPECT().
			UpdateInodeAttr(mock.Anything, mock.Anything).
			Return(db.Inode{}, dbErr).
			Once()

		inode, err := mgr.SetAttr(ctx, ino, meta.SetAttrInput{
			Mode: ptr(uint32(0o777)),
		})
		assert.Nil(t, inode)
		assert.ErrorIs(t, err, dbErr)
	})
}

func TestSetAttr_CombinedUpdates(t *testing.T) {
	ctx := context.Background()
	now := time.Now().Truncate(time.Microsecond)
	ino := uint64(100)

	baseInodeRow := db.Inode{
		Ino:        int64(ino),
		Generation: 2,
		Mode:       int32(0o100600),
		Size:       500,
		Nlink:      1,
		Uid:        500,
		Gid:        500,
		Rdev:       0,
		Atime:      pgtype.Timestamptz{Time: now.Add(-time.Hour), Valid: true},
		Mtime:      pgtype.Timestamptz{Time: now.Add(-time.Hour), Valid: true},
		Ctime:      pgtype.Timestamptz{Time: now.Add(-time.Hour), Valid: true},
	}

	t.Run("successfully update mode, uid, gid, size and atime together", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockWriteTx(mockStore, mockQuerier)

		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(ino)).
			Return(baseInodeRow, nil).
			Once()

		expectedNewMode := int32(0o100750)
		customAtime := now.Add(-10 * time.Minute)
		updatedRow := db.Inode{
			Ino:        int64(ino),
			Generation: 2,
			Mode:       expectedNewMode,
			Size:       1500,
			Nlink:      1,
			Uid:        1001,
			Gid:        1002,
			Rdev:       0,
			Atime:      pgtype.Timestamptz{Time: customAtime, Valid: true},
			Mtime:      pgtype.Timestamptz{Time: now, Valid: true},
			Ctime:      pgtype.Timestamptz{Time: now, Valid: true},
		}

		mockQuerier.EXPECT().
			UpdateInodeAttr(mock.Anything, db.UpdateInodeAttrParams{
				Ino:         int64(ino),
				Mode:        expectedNewMode,
				Size:        1500,
				Uid:         1001,
				Gid:         1002,
				SetAtimeNow: false,
				Atime:       pgtype.Timestamptz{Time: customAtime, Valid: true},
				SetMtimeNow: true, // size is updated and mtime is not specified, so set_mtime_now is true
				Mtime:       pgtype.Timestamptz{Valid: false},
			}).
			Return(updatedRow, nil).
			Once()

		inode, err := mgr.SetAttr(ctx, ino, meta.SetAttrInput{
			Mode:  ptr(uint32(0o750)),
			UID:   ptr(uint32(1001)),
			GID:   ptr(uint32(1002)),
			Size:  ptr(uint64(1500)),
			Atime: &customAtime,
		})
		require.NoError(t, err)
		require.NotNil(t, inode)

		assert.Equal(t, uint32(0o100750), inode.Attr.Mode)
		assert.Equal(t, uint32(1001), inode.Attr.UID)
		assert.Equal(t, uint32(1002), inode.Attr.GID)
		assert.Equal(t, uint64(1500), inode.Attr.Size)
		assert.Equal(t, uint64(3), inode.Attr.Blocks) // (1500 + 511) / 512 = 3
		assert.Equal(t, customAtime, inode.Attr.Atime)
		assert.Equal(t, updatedRow.Mtime.Time, inode.Attr.Mtime)
	})
}

func TestSetAttr_TransactionOptionsContract(t *testing.T) {
	ctx := context.Background()
	mockStore := dbmock.NewMockStore(t)
	mockQuerier := dbmock.NewMockQuerier(t)
	mgr := meta.New(mockStore)

	var capturedOpts []tx.Option
	mockStore.EXPECT().
		ExecTxWithOptions(mock.Anything, mock.Anything).
		RunAndReturn(func(ctx context.Context, fn func(db.Querier) error, opts ...tx.Option) error {
			capturedOpts = opts
			return fn(mockQuerier)
		}).
		Once()

	mockQuerier.EXPECT().
		GetInodeForUpdate(mock.Anything, int64(1)).
		Return(db.Inode{Ino: 1, Mode: int32(0o100644)}, nil).
		Once()

	mockQuerier.EXPECT().
		UpdateInodeAttr(mock.Anything, mock.Anything).
		Return(db.Inode{Ino: 1, Mode: int32(0o100644)}, nil).
		Once()

	inode, err := mgr.SetAttr(ctx, 1, meta.SetAttrInput{
		UID: ptr(uint32(1000)),
	})
	require.NoError(t, err)
	require.NotNil(t, inode)

	// Verify that ReadOnly is not set (it must be a read-write transaction)
	cfg := tx.ApplyOptions(capturedOpts...)
	assert.NotEqual(t, tx.ReadOnly, cfg.AccessMode)
}

func TestSetAttr_InputValidation(t *testing.T) {
	ctx := context.Background()
	now := time.Now()

	t.Run("ino 为 0 时直接返回 ErrInvalidArgument 且不开启事务", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mgr := meta.New(mockStore)

		inode, err := mgr.SetAttr(ctx, 0, meta.SetAttrInput{
			Mode: ptr(uint32(0o644)),
		})
		assert.Nil(t, inode)
		assert.ErrorIs(t, err, meta.ErrInvalidArgument)
		mockStore.AssertNotCalled(t, "ExecTxWithOptions")
	})

	t.Run("ino 超过 math.MaxInt64 时直接返回 ErrInvalidArgument 且不开启事务", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mgr := meta.New(mockStore)

		inode, err := mgr.SetAttr(ctx, uint64(math.MaxInt64)+1, meta.SetAttrInput{
			Mode: ptr(uint32(0o644)),
		})
		assert.Nil(t, inode)
		assert.ErrorIs(t, err, meta.ErrInvalidArgument)
		mockStore.AssertNotCalled(t, "ExecTxWithOptions")
	})

	t.Run("空更新请求（全部字段为 nil 且布尔为 false）直接返回 ErrInvalidArgument 且不开启事务", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mgr := meta.New(mockStore)

		inode, err := mgr.SetAttr(ctx, 100, meta.SetAttrInput{})
		assert.Nil(t, inode)
		assert.ErrorIs(t, err, meta.ErrInvalidArgument)
		mockStore.AssertNotCalled(t, "ExecTxWithOptions")
	})

	t.Run("atime 与 atime_now 冲突时直接返回 ErrInvalidArgument 且不开启事务", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mgr := meta.New(mockStore)

		inode, err := mgr.SetAttr(ctx, 100, meta.SetAttrInput{
			Atime:    &now,
			AtimeNow: true,
		})
		assert.Nil(t, inode)
		assert.ErrorIs(t, err, meta.ErrInvalidArgument)
		mockStore.AssertNotCalled(t, "ExecTxWithOptions")
	})

	t.Run("mtime 与 mtime_now 冲突时直接返回 ErrInvalidArgument 且不开启事务", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mgr := meta.New(mockStore)

		inode, err := mgr.SetAttr(ctx, 100, meta.SetAttrInput{
			Mtime:    &now,
			MtimeNow: true,
		})
		assert.Nil(t, inode)
		assert.ErrorIs(t, err, meta.ErrInvalidArgument)
		mockStore.AssertNotCalled(t, "ExecTxWithOptions")
	})

	t.Run("mode 超过 0o177777（65535）时直接返回 ErrInvalidArgument 且不开启事务", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mgr := meta.New(mockStore)

		inode, err := mgr.SetAttr(ctx, 100, meta.SetAttrInput{
			Mode: ptr(uint32(0o200000)),
		})
		assert.Nil(t, inode)
		assert.ErrorIs(t, err, meta.ErrInvalidArgument)
		mockStore.AssertNotCalled(t, "ExecTxWithOptions")
	})

	t.Run("uid 超过 math.MaxInt32 时直接返回 ErrInvalidArgument 且不开启事务", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mgr := meta.New(mockStore)

		inode, err := mgr.SetAttr(ctx, 100, meta.SetAttrInput{
			UID: ptr(uint32(math.MaxInt32) + 1),
		})
		assert.Nil(t, inode)
		assert.ErrorIs(t, err, meta.ErrInvalidArgument)
		mockStore.AssertNotCalled(t, "ExecTxWithOptions")
	})

	t.Run("gid 超过 math.MaxInt32 时直接返回 ErrInvalidArgument 且不开启事务", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mgr := meta.New(mockStore)

		inode, err := mgr.SetAttr(ctx, 100, meta.SetAttrInput{
			GID: ptr(uint32(math.MaxInt32) + 1),
		})
		assert.Nil(t, inode)
		assert.ErrorIs(t, err, meta.ErrInvalidArgument)
		mockStore.AssertNotCalled(t, "ExecTxWithOptions")
	})

	t.Run("size 超过 math.MaxInt64 时直接返回 ErrInvalidArgument 且不开启事务", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mgr := meta.New(mockStore)

		inode, err := mgr.SetAttr(ctx, 100, meta.SetAttrInput{
			Size: ptr(uint64(math.MaxInt64) + 1),
		})
		assert.Nil(t, inode)
		assert.ErrorIs(t, err, meta.ErrInvalidArgument)
		mockStore.AssertNotCalled(t, "ExecTxWithOptions")
	})
}

func TestSetAttr_POSIX_FileTypeAndSize(t *testing.T) {
	ctx := context.Background()
	now := time.Now().Truncate(time.Microsecond)
	ino := uint64(200)

	t.Run("目标节点为目录时修改 size 返回 ErrIsDir 且不调用 UpdateInodeAttr", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockWriteTx(mockStore, mockQuerier)

		dirRow := db.Inode{
			Ino:        int64(ino),
			Generation: 1,
			Mode:       int32(meta.S_IFDIR | 0o755),
			Size:       4096,
			Uid:        1000,
			Gid:        1000,
			Atime:      pgtype.Timestamptz{Time: now, Valid: true},
			Mtime:      pgtype.Timestamptz{Time: now, Valid: true},
			Ctime:      pgtype.Timestamptz{Time: now, Valid: true},
		}

		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(ino)).
			Return(dirRow, nil).
			Once()

		inode, err := mgr.SetAttr(ctx, ino, meta.SetAttrInput{
			Size: ptr(uint64(0)),
		})
		assert.Nil(t, inode)
		assert.ErrorIs(t, err, meta.ErrIsDir)
		mockQuerier.AssertNotCalled(t, "UpdateInodeAttr", mock.Anything, mock.Anything)
	})

	nonRegularTestCases := []struct {
		name     string
		mode     uint32
	}{
		{name: "符号链接 S_IFLNK", mode: 0o120777},
		{name: "FIFO 命名管道 S_IFIFO", mode: 0o010666},
		{name: "UNIX Domain Socket S_IFSOCK", mode: 0o140777},
		{name: "字符设备 S_IFCHR", mode: 0o020660},
		{name: "块设备 S_IFBLK", mode: 0o060660},
	}

	for _, tc := range nonRegularTestCases {
		t.Run(fmt.Sprintf("目标节点为非常规文件（%s）时修改 size 返回 ErrInvalidArgument", tc.name), func(t *testing.T) {
			mockStore := dbmock.NewMockStore(t)
			mockQuerier := dbmock.NewMockQuerier(t)
			mgr := meta.New(mockStore)

			setupMockWriteTx(mockStore, mockQuerier)

			row := db.Inode{
				Ino:        int64(ino),
				Generation: 1,
				Mode:       int32(tc.mode),
				Size:       0,
				Uid:        1000,
				Gid:        1000,
				Atime:      pgtype.Timestamptz{Time: now, Valid: true},
				Mtime:      pgtype.Timestamptz{Time: now, Valid: true},
				Ctime:      pgtype.Timestamptz{Time: now, Valid: true},
			}

			mockQuerier.EXPECT().
				GetInodeForUpdate(mock.Anything, int64(ino)).
				Return(row, nil).
				Once()

			inode, err := mgr.SetAttr(ctx, ino, meta.SetAttrInput{
				Size: ptr(uint64(1024)),
			})
			assert.Nil(t, inode)
			assert.ErrorIs(t, err, meta.ErrInvalidArgument)
			mockQuerier.AssertNotCalled(t, "UpdateInodeAttr", mock.Anything, mock.Anything)
		})
	}
}

func TestSetAttr_POSIX_ModeFileTypeTampering(t *testing.T) {
	ctx := context.Background()
	now := time.Now().Truncate(time.Microsecond)
	ino := uint64(300)

	t.Run("常规文件传入 mode 携带 S_IFDIR 文件类型位返回 ErrInvalidArgument", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockWriteTx(mockStore, mockQuerier)

		fileRow := db.Inode{
			Ino:        int64(ino),
			Generation: 1,
			Mode:       int32(meta.S_IFREG | 0o644),
			Size:       1024,
			Uid:        1000,
			Gid:        1000,
		}

		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(ino)).
			Return(fileRow, nil).
			Once()

		inode, err := mgr.SetAttr(ctx, ino, meta.SetAttrInput{
			Mode: ptr(uint32(meta.S_IFDIR | 0o755)),
		})
		assert.Nil(t, inode)
		assert.ErrorIs(t, err, meta.ErrInvalidArgument)
		mockQuerier.AssertNotCalled(t, "UpdateInodeAttr", mock.Anything, mock.Anything)
	})

	t.Run("目录传入 mode 携带 S_IFREG 文件类型位返回 ErrInvalidArgument", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockWriteTx(mockStore, mockQuerier)

		dirRow := db.Inode{
			Ino:        int64(ino),
			Generation: 1,
			Mode:       int32(meta.S_IFDIR | 0o755),
			Size:       4096,
			Uid:        1000,
			Gid:        1000,
		}

		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(ino)).
			Return(dirRow, nil).
			Once()

		inode, err := mgr.SetAttr(ctx, ino, meta.SetAttrInput{
			Mode: ptr(uint32(meta.S_IFREG | 0o644)),
		})
		assert.Nil(t, inode)
		assert.ErrorIs(t, err, meta.ErrInvalidArgument)
		mockQuerier.AssertNotCalled(t, "UpdateInodeAttr", mock.Anything, mock.Anything)
	})

	t.Run("符号链接传入 mode 返回 ErrInvalidArgument（符号链接权限固定为 S_IFLNK | 0o777）", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockWriteTx(mockStore, mockQuerier)

		symlinkRow := db.Inode{
			Ino:        int64(ino),
			Generation: 1,
			Mode:       int32(meta.S_IFLNK | 0o777),
			Size:       10,
			Uid:        1000,
			Gid:        1000,
		}

		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(ino)).
			Return(symlinkRow, nil).
			Once()

		inode, err := mgr.SetAttr(ctx, ino, meta.SetAttrInput{
			Mode: ptr(uint32(0o755)),
		})
		assert.Nil(t, inode)
		assert.ErrorIs(t, err, meta.ErrInvalidArgument)
		mockQuerier.AssertNotCalled(t, "UpdateInodeAttr", mock.Anything, mock.Anything)
	})

	t.Run("常规文件传入 mode 携带相同文件类型位（S_IFREG）成功更新并保留文件类型", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockWriteTx(mockStore, mockQuerier)

		fileRow := db.Inode{
			Ino:        int64(ino),
			Generation: 1,
			Mode:       int32(meta.S_IFREG | 0o644),
			Size:       1024,
			Uid:        1000,
			Gid:        1000,
			Atime:      pgtype.Timestamptz{Time: now, Valid: true},
			Mtime:      pgtype.Timestamptz{Time: now, Valid: true},
		}

		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(ino)).
			Return(fileRow, nil).
			Once()

		expectedNewMode := int32(meta.S_IFREG | 0o750)
		updatedRow := fileRow
		updatedRow.Mode = expectedNewMode
		updatedRow.Ctime = pgtype.Timestamptz{Time: now, Valid: true}

		mockQuerier.EXPECT().
			UpdateInodeAttr(mock.Anything, db.UpdateInodeAttrParams{
				Ino:         int64(ino),
				Mode:        expectedNewMode,
				Size:        fileRow.Size,
				Uid:         fileRow.Uid,
				Gid:         fileRow.Gid,
				SetAtimeNow: false,
				Atime:       fileRow.Atime,
				SetMtimeNow: false,
				Mtime:       fileRow.Mtime,
			}).
			Return(updatedRow, nil).
			Once()

		inode, err := mgr.SetAttr(ctx, ino, meta.SetAttrInput{
			Mode: ptr(uint32(meta.S_IFREG | 0o750)),
		})
		require.NoError(t, err)
		require.NotNil(t, inode)
		assert.Equal(t, uint32(meta.S_IFREG|0o750), inode.Attr.Mode)
	})

	t.Run("常规文件传入无文件类型位 mode（仅含权限位 0o755）成功更新并保留原文件类型位", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockWriteTx(mockStore, mockQuerier)

		fileRow := db.Inode{
			Ino:        int64(ino),
			Generation: 1,
			Mode:       int32(meta.S_IFREG | 0o644),
			Size:       1024,
			Uid:        1000,
			Gid:        1000,
			Atime:      pgtype.Timestamptz{Time: now, Valid: true},
			Mtime:      pgtype.Timestamptz{Time: now, Valid: true},
		}

		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(ino)).
			Return(fileRow, nil).
			Once()

		expectedNewMode := int32(meta.S_IFREG | 0o755)
		updatedRow := fileRow
		updatedRow.Mode = expectedNewMode
		updatedRow.Ctime = pgtype.Timestamptz{Time: now, Valid: true}

		mockQuerier.EXPECT().
			UpdateInodeAttr(mock.Anything, db.UpdateInodeAttrParams{
				Ino:         int64(ino),
				Mode:        expectedNewMode,
				Size:        fileRow.Size,
				Uid:         fileRow.Uid,
				Gid:         fileRow.Gid,
				SetAtimeNow: false,
				Atime:       fileRow.Atime,
				SetMtimeNow: false,
				Mtime:       fileRow.Mtime,
			}).
			Return(updatedRow, nil).
			Once()

		inode, err := mgr.SetAttr(ctx, ino, meta.SetAttrInput{
			Mode: ptr(uint32(0o755)),
		})
		require.NoError(t, err)
		require.NotNil(t, inode)
		assert.Equal(t, uint32(meta.S_IFREG|0o755), inode.Attr.Mode)
	})
}

func TestSetAttr_POSIX_PrivilegeBitsStripping(t *testing.T) {
	ctx := context.Background()
	now := time.Now().Truncate(time.Microsecond)
	ino := uint64(400)

	// 常规文件，带有 S_ISUID 和 S_ISGID (0o106755)
	fileWithPrivRow := db.Inode{
		Ino:        int64(ino),
		Generation: 1,
		Mode:       int32(meta.S_IFREG | meta.S_ISUID | meta.S_ISGID | 0o755),
		Size:       1024,
		Uid:        1000,
		Gid:        1000,
		Atime:      pgtype.Timestamptz{Time: now, Valid: true},
		Mtime:      pgtype.Timestamptz{Time: now, Valid: true},
		Ctime:      pgtype.Timestamptz{Time: now, Valid: true},
	}

	expectedStrippedMode := int32(meta.S_IFREG | 0o755) // SUID/SGID 剥离后

	t.Run("常规文件在 mode==nil 时修改 UID 自动剥离 S_ISUID 和 S_ISGID", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockWriteTx(mockStore, mockQuerier)

		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(ino)).
			Return(fileWithPrivRow, nil).
			Once()

		updatedRow := fileWithPrivRow
		updatedRow.Uid = 2000
		updatedRow.Mode = expectedStrippedMode

		mockQuerier.EXPECT().
			UpdateInodeAttr(mock.Anything, db.UpdateInodeAttrParams{
				Ino:         int64(ino),
				Mode:        expectedStrippedMode,
				Size:        fileWithPrivRow.Size,
				Uid:         2000,
				Gid:         fileWithPrivRow.Gid,
				SetAtimeNow: false,
				Atime:       fileWithPrivRow.Atime,
				SetMtimeNow: false,
				Mtime:       fileWithPrivRow.Mtime,
			}).
			Return(updatedRow, nil).
			Once()

		inode, err := mgr.SetAttr(ctx, ino, meta.SetAttrInput{
			UID: ptr(uint32(2000)),
		})
		require.NoError(t, err)
		require.NotNil(t, inode)
		assert.Equal(t, uint32(expectedStrippedMode), inode.Attr.Mode)
		assert.Equal(t, uint32(2000), inode.Attr.UID)
	})

	t.Run("常规文件在 mode==nil 时修改 GID 自动剥离 S_ISUID 和 S_ISGID", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockWriteTx(mockStore, mockQuerier)

		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(ino)).
			Return(fileWithPrivRow, nil).
			Once()

		updatedRow := fileWithPrivRow
		updatedRow.Gid = 3000
		updatedRow.Mode = expectedStrippedMode

		mockQuerier.EXPECT().
			UpdateInodeAttr(mock.Anything, db.UpdateInodeAttrParams{
				Ino:         int64(ino),
				Mode:        expectedStrippedMode,
				Size:        fileWithPrivRow.Size,
				Uid:         fileWithPrivRow.Uid,
				Gid:         3000,
				SetAtimeNow: false,
				Atime:       fileWithPrivRow.Atime,
				SetMtimeNow: false,
				Mtime:       fileWithPrivRow.Mtime,
			}).
			Return(updatedRow, nil).
			Once()

		inode, err := mgr.SetAttr(ctx, ino, meta.SetAttrInput{
			GID: ptr(uint32(3000)),
		})
		require.NoError(t, err)
		require.NotNil(t, inode)
		assert.Equal(t, uint32(expectedStrippedMode), inode.Attr.Mode)
		assert.Equal(t, uint32(3000), inode.Attr.GID)
	})

	t.Run("常规文件在 mode==nil 时修改 Size 自动剥离 S_ISUID 和 S_ISGID", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockWriteTx(mockStore, mockQuerier)

		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(ino)).
			Return(fileWithPrivRow, nil).
			Once()

		updatedRow := fileWithPrivRow
		updatedRow.Size = 2048
		updatedRow.Mode = expectedStrippedMode

		mockQuerier.EXPECT().
			UpdateInodeAttr(mock.Anything, db.UpdateInodeAttrParams{
				Ino:         int64(ino),
				Mode:        expectedStrippedMode,
				Size:        2048,
				Uid:         fileWithPrivRow.Uid,
				Gid:         fileWithPrivRow.Gid,
				SetAtimeNow: false,
				Atime:       fileWithPrivRow.Atime,
				SetMtimeNow: true, // size updated and mtime not given
				Mtime:       pgtype.Timestamptz{Valid: false},
			}).
			Return(updatedRow, nil).
			Once()

		inode, err := mgr.SetAttr(ctx, ino, meta.SetAttrInput{
			Size: ptr(uint64(2048)),
		})
		require.NoError(t, err)
		require.NotNil(t, inode)
		assert.Equal(t, uint32(expectedStrippedMode), inode.Attr.Mode)
		assert.Equal(t, uint64(2048), inode.Attr.Size)
	})

	t.Run("常规文件在显式指定 Mode 时修改 UID/Size 不剥离特权位（遵循指定 Mode）", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockWriteTx(mockStore, mockQuerier)

		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(ino)).
			Return(fileWithPrivRow, nil).
			Once()

		// 显式指定 Mode 包含 S_ISUID 与 S_ISGID (0o6755)
		explicitMode := uint32(meta.S_ISUID | meta.S_ISGID | 0o755)
		expectedMode := int32(meta.S_IFREG | explicitMode)

		updatedRow := fileWithPrivRow
		updatedRow.Uid = 2000
		updatedRow.Size = 4096
		updatedRow.Mode = expectedMode

		mockQuerier.EXPECT().
			UpdateInodeAttr(mock.Anything, db.UpdateInodeAttrParams{
				Ino:         int64(ino),
				Mode:        expectedMode,
				Size:        4096,
				Uid:         2000,
				Gid:         fileWithPrivRow.Gid,
				SetAtimeNow: false,
				Atime:       fileWithPrivRow.Atime,
				SetMtimeNow: true,
				Mtime:       pgtype.Timestamptz{Valid: false},
			}).
			Return(updatedRow, nil).
			Once()

		inode, err := mgr.SetAttr(ctx, ino, meta.SetAttrInput{
			Mode: &explicitMode,
			UID:  ptr(uint32(2000)),
			Size: ptr(uint64(4096)),
		})
		require.NoError(t, err)
		require.NotNil(t, inode)
		assert.Equal(t, uint32(expectedMode), inode.Attr.Mode)
	})

	t.Run("常规文件在 mode==nil 且仅修改时间戳时保留特权位不剥离", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockWriteTx(mockStore, mockQuerier)

		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(ino)).
			Return(fileWithPrivRow, nil).
			Once()

		updatedRow := fileWithPrivRow
		updatedRow.Atime = pgtype.Timestamptz{Time: now, Valid: true}

		mockQuerier.EXPECT().
			UpdateInodeAttr(mock.Anything, db.UpdateInodeAttrParams{
				Ino:         int64(ino),
				Mode:        fileWithPrivRow.Mode, // 保留 0o106755
				Size:        fileWithPrivRow.Size,
				Uid:         fileWithPrivRow.Uid,
				Gid:         fileWithPrivRow.Gid,
				SetAtimeNow: true,
				Atime:       pgtype.Timestamptz{Valid: false},
				SetMtimeNow: false,
				Mtime:       fileWithPrivRow.Mtime,
			}).
			Return(updatedRow, nil).
			Once()

		inode, err := mgr.SetAttr(ctx, ino, meta.SetAttrInput{
			AtimeNow: true,
		})
		require.NoError(t, err)
		require.NotNil(t, inode)
		assert.Equal(t, uint32(fileWithPrivRow.Mode), inode.Attr.Mode)
	})

	t.Run("目录包含 S_ISGID 时修改 UID/GID 保留 S_ISGID 特权位不剥离", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockWriteTx(mockStore, mockQuerier)

		dirWithSGIDRow := db.Inode{
			Ino:        int64(ino),
			Generation: 1,
			Mode:       int32(meta.S_IFDIR | meta.S_ISGID | 0o775),
			Size:       4096,
			Uid:        1000,
			Gid:        1000,
			Atime:      pgtype.Timestamptz{Time: now, Valid: true},
			Mtime:      pgtype.Timestamptz{Time: now, Valid: true},
			Ctime:      pgtype.Timestamptz{Time: now, Valid: true},
		}

		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(ino)).
			Return(dirWithSGIDRow, nil).
			Once()

		updatedRow := dirWithSGIDRow
		updatedRow.Uid = 2000
		updatedRow.Gid = 3000

		mockQuerier.EXPECT().
			UpdateInodeAttr(mock.Anything, db.UpdateInodeAttrParams{
				Ino:         int64(ino),
				Mode:        dirWithSGIDRow.Mode, // 保留 S_ISGID
				Size:        dirWithSGIDRow.Size,
				Uid:         2000,
				Gid:         3000,
				SetAtimeNow: false,
				Atime:       dirWithSGIDRow.Atime,
				SetMtimeNow: false,
				Mtime:       dirWithSGIDRow.Mtime,
			}).
			Return(updatedRow, nil).
			Once()

		inode, err := mgr.SetAttr(ctx, ino, meta.SetAttrInput{
			UID: ptr(uint32(2000)),
			GID: ptr(uint32(3000)),
		})
		require.NoError(t, err)
		require.NotNil(t, inode)
		assert.Equal(t, uint32(dirWithSGIDRow.Mode), inode.Attr.Mode)
	})
}

func TestSetAttr_NegativeCases(t *testing.T) {
	ctx := context.Background()
	ino := uint64(500)

	t.Run("事务执行中返回的 ErrNotFound 正确透传为领域错误", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockWriteTx(mockStore, mockQuerier)

		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(ino)).
			Return(db.Inode{}, pgx.ErrNoRows).
			Once()

		inode, err := mgr.SetAttr(ctx, ino, meta.SetAttrInput{
			Mode: ptr(uint32(0o644)),
		})
		assert.Nil(t, inode)
		assert.True(t, errors.Is(err, meta.ErrNotFound))
	})

	t.Run("事务执行中返回的 ErrIsDir 正确透传为领域错误", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockWriteTx(mockStore, mockQuerier)

		dirRow := db.Inode{
			Ino:  int64(ino),
			Mode: int32(meta.S_IFDIR | 0o755),
		}

		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(ino)).
			Return(dirRow, nil).
			Once()

		inode, err := mgr.SetAttr(ctx, ino, meta.SetAttrInput{
			Size: ptr(uint64(0)),
		})
		assert.Nil(t, inode)
		assert.True(t, errors.Is(err, meta.ErrIsDir))
	})

	t.Run("事务执行中返回的 ErrInvalidArgument 正确透传为领域错误", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockWriteTx(mockStore, mockQuerier)

		fileRow := db.Inode{
			Ino:  int64(ino),
			Mode: int32(meta.S_IFREG | 0o644),
		}

		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(ino)).
			Return(fileRow, nil).
			Once()

		inode, err := mgr.SetAttr(ctx, ino, meta.SetAttrInput{
			Mode: ptr(uint32(meta.S_IFDIR | 0o755)),
		})
		assert.Nil(t, inode)
		assert.True(t, errors.Is(err, meta.ErrInvalidArgument))
	})

	t.Run("事务执行失败（如连接池故障）返回包装错误且非哨兵错误", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mgr := meta.New(mockStore)

		txErr := errors.New("connection refused")
		mockStore.EXPECT().
			ExecTxWithOptions(mock.Anything, mock.Anything).
			Return(txErr).
			Once()

		inode, err := mgr.SetAttr(ctx, ino, meta.SetAttrInput{
			Mode: ptr(uint32(0o644)),
		})
		assert.Nil(t, inode)
		assert.ErrorIs(t, err, txErr)
		assert.False(t, errors.Is(err, meta.ErrNotFound))
		assert.False(t, errors.Is(err, meta.ErrIsDir))
		assert.False(t, errors.Is(err, meta.ErrInvalidArgument))
	})

	t.Run("仅设置 CtimeNow 或 Flags 的请求正常放行并保留已有 Flags", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockWriteTx(mockStore, mockQuerier)

		existingRow := db.Inode{
			Ino:   int64(ino),
			Mode:  int32(meta.S_IFREG | 0o644),
			Flags: int32(meta.FlagAppend),
		}
		updatedRow := existingRow

		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(ino)).
			Return(existingRow, nil).
			Once()

		mockQuerier.EXPECT().
			UpdateInodeAttr(mock.Anything, db.UpdateInodeAttrParams{
				Ino:   int64(ino),
				Mode:  existingRow.Mode,
				Flags: int32(meta.FlagAppend),
			}).
			Return(updatedRow, nil).
			Once()

		res, err := mgr.SetAttr(ctx, ino, meta.SetAttrInput{
			CtimeNow: true,
		})
		require.NoError(t, err)
		require.NotNil(t, res)
		assert.Equal(t, meta.FlagAppend, res.Attr.Flags)
	})

	t.Run("显式传入 Flags 时更新 Inode Flags", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockWriteTx(mockStore, mockQuerier)

		existingRow := db.Inode{
			Ino:  int64(ino),
			Mode: int32(meta.S_IFREG | 0o644),
		}
		updatedRow := existingRow
		updatedRow.Flags = int32(meta.FlagImmutable)

		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(ino)).
			Return(existingRow, nil).
			Once()

		mockQuerier.EXPECT().
			UpdateInodeAttr(mock.Anything, db.UpdateInodeAttrParams{
				Ino:   int64(ino),
				Mode:  existingRow.Mode,
				Flags: int32(meta.FlagImmutable),
			}).
			Return(updatedRow, nil).
			Once()

		res, err := mgr.SetAttr(ctx, ino, meta.SetAttrInput{
			Flags: ptr(meta.FlagImmutable),
		})
		require.NoError(t, err)
		require.NotNil(t, res)
		assert.Equal(t, meta.FlagImmutable, res.Attr.Flags)
	})

	t.Run("传入 Fh 与 Size (ftruncate) 时校验句柄写权限并续期客户端会话", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockWriteTx(mockStore, mockQuerier)

		existingRow := db.Inode{
			Ino:  int64(ino),
			Mode: int32(meta.S_IFREG | 0o644),
			Size: 1024,
		}
		updatedRow := existingRow
		updatedRow.Size = 512

		fh := uint64(88)
		sessionID := uint64(99)

		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(ino)).
			Return(existingRow, nil).
			Once()

		mockQuerier.EXPECT().
			GetFileHandle(mock.Anything, db.GetFileHandleParams{
				Fh:        int64(fh),
				Ino:       int64(ino),
				SessionID: pgtype.Int8{Int64: int64(sessionID), Valid: true},
			}).
			Return(db.FileHandle{
				Fh:    int64(fh),
				Ino:   int64(ino),
				Flags: int32(meta.O_RDWR),
			}, nil).
			Once()

		mockQuerier.EXPECT().
			DeleteExpiredClientSessions(mock.Anything).
			Return(int64(0), nil).
			Once()
		mockQuerier.EXPECT().
			UpsertClientSession(mock.Anything, mock.Anything).
			Return(db.ClientSession{SessionID: int64(sessionID)}, nil).
			Once()
		mockQuerier.EXPECT().
			DeleteUnreferencedOrphanInodes(mock.Anything).
			Return(nil, nil).
			Once()

		mockQuerier.EXPECT().
			UpdateInodeAttr(mock.Anything, db.UpdateInodeAttrParams{
				Ino:         int64(ino),
				Mode:        existingRow.Mode,
				Size:        512,
				SetMtimeNow: true,
			}).
			Return(updatedRow, nil).
			Once()

		res, err := mgr.SetAttr(ctx, ino, meta.SetAttrInput{
			Size:      ptr(uint64(512)),
			Fh:        &fh,
			SessionID: &sessionID,
		})
		require.NoError(t, err)
		require.NotNil(t, res)
		assert.Equal(t, uint64(512), res.Attr.Size)
	})

	t.Run("传入只读 Fh 执行 ftruncate (Size != nil) 返回 ErrBadFileHandle (EBADF)", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockWriteTx(mockStore, mockQuerier)

		existingRow := db.Inode{
			Ino:  int64(ino),
			Mode: int32(meta.S_IFREG | 0o644),
			Size: 1024,
		}
		fh := uint64(88)

		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(ino)).
			Return(existingRow, nil).
			Once()

		mockQuerier.EXPECT().
			GetFileHandle(mock.Anything, db.GetFileHandleParams{
				Fh:  int64(fh),
				Ino: int64(ino),
			}).
			Return(db.FileHandle{
				Fh:    int64(fh),
				Ino:   int64(ino),
				Flags: int32(meta.O_RDONLY),
			}, nil).
			Once()

		res, err := mgr.SetAttr(ctx, ino, meta.SetAttrInput{
			Size: ptr(uint64(0)),
			Fh:   &fh,
		})
		assert.Nil(t, res)
		assert.ErrorIs(t, err, meta.ErrBadFileHandle)
	})
}


