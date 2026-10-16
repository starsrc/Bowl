package meta_test

import (
	"context"
	"errors"
	"fmt"
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

func setupMockWriteTx(mockStore *dbmock.MockStore, mockQuerier *dbmock.MockQuerier) {
	mockStore.EXPECT().
		ExecTxWithOptions(
			mock.Anything,
			mock.Anything,
		).
		RunAndReturn(func(ctx context.Context, fn func(db.Querier) error, opts ...tx.Option) error {
			cfg := tx.ApplyOptions(opts...)
			if cfg.AccessMode == tx.ReadOnly {
				return fmt.Errorf("expected write transaction, got read-only")
			}
			return fn(mockQuerier)
		}).
		Once()
}

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
		assert.Equal(t, uint32(4096), inode.Attr.Blksize)
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
		assert.Equal(t, uint32(4096), inode.Attr.Blksize)
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

