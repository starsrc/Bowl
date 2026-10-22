package disk_test

import (
	"context"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/starsrc/Aether/pkg/tx"
	"github.com/starsrc/bowl/services/disk/internal/db"
	dbmock "github.com/starsrc/bowl/services/disk/internal/db/mock"
	"github.com/starsrc/bowl/services/disk/internal/disk"
)

func ptr[T any](v T) *T {
	return &v
}

func setupUpdateTest(t *testing.T) (context.Context, *dbmock.MockStore, *dbmock.MockQuerier, disk.Manager) {
	t.Helper()
	ctx := context.Background()
	mockStore := dbmock.NewMockStore(t)
	mockQuerier := dbmock.NewMockQuerier(t)
	mgr := disk.New(mockStore)
	return ctx, mockStore, mockQuerier, mgr
}

func mockUpdateExecTx(mockStore *dbmock.MockStore, mockQuerier *dbmock.MockQuerier) {
	mockStore.EXPECT().
		ExecTxWithOptions(mock.Anything, mock.Anything).
		RunAndReturn(func(ctx context.Context, fn func(db.Querier) error, opts ...tx.Option) error {
			return fn(mockQuerier)
		}).
		Once()
}

func parseUUID(t *testing.T, idStr string) pgtype.UUID {
	t.Helper()
	var id pgtype.UUID
	err := id.Scan(idStr)
	require.NoError(t, err)
	return id
}

func TestUpdateDisk_StaticValidation(t *testing.T) {
	ctx, mockStore, _, mgr := setupUpdateTest(t)
	validDiskID := "0192a3b4-c5d6-7e8f-9012-3456789abcde"

	testCases := []struct {
		name        string
		params      disk.UpdateDiskParams
		expectedErr error
	}{
		{
			name: "空 disk_id 返回 ErrInvalidArgument",
			params: disk.UpdateDiskParams{
				DiskID: "",
				Name:   ptr("new-disk"),
			},
			expectedErr: disk.ErrInvalidArgument,
		},
		{
			name: "非 UUID 格式 disk_id 返回 ErrInvalidArgument",
			params: disk.UpdateDiskParams{
				DiskID: "not-a-valid-uuid",
				Name:   ptr("new-disk"),
			},
			expectedErr: disk.ErrInvalidArgument,
		},
		{
			name: "全零 Nil UUID disk_id 返回 ErrInvalidArgument",
			params: disk.UpdateDiskParams{
				DiskID: "00000000-0000-0000-0000-000000000000",
				Name:   ptr("new-disk"),
			},
			expectedErr: disk.ErrInvalidArgument,
		},
		{
			name: "name 与 max_bytes 均为空指针时返回 ErrInvalidArgument",
			params: disk.UpdateDiskParams{
				DiskID:   validDiskID,
				Name:     nil,
				MaxBytes: nil,
			},
			expectedErr: disk.ErrInvalidArgument,
		},
		{
			name: "max_bytes 为 0 时返回 ErrInvalidArgument",
			params: disk.UpdateDiskParams{
				DiskID:   validDiskID,
				MaxBytes: ptr(uint64(0)),
			},
			expectedErr: disk.ErrInvalidArgument,
		},
		{
			name: "max_bytes 超过 math.MaxInt64 时返回 ErrInvalidArgument",
			params: disk.UpdateDiskParams{
				DiskID:   validDiskID,
				MaxBytes: ptr(uint64(math.MaxInt64) + 1),
			},
			expectedErr: disk.ErrInvalidArgument,
		},
		{
			name: "name 长度超过 255 字节返回 ErrNameTooLong",
			params: disk.UpdateDiskParams{
				DiskID: validDiskID,
				Name:   ptr(strings.Repeat("a", 256)),
			},
			expectedErr: disk.ErrNameTooLong,
		},
		{
			name: "name 包含空字符 '\\x00' 返回 ErrInvalidArgument",
			params: disk.UpdateDiskParams{
				DiskID: validDiskID,
				Name:   ptr("bad\x00name"),
			},
			expectedErr: disk.ErrInvalidArgument,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result, err := mgr.UpdateDisk(ctx, tc.params)
			assert.Nil(t, result)
			require.Error(t, err)
			assert.ErrorIs(t, err, tc.expectedErr)
		})
	}

	// 静态校验失败严禁触发任何数据库或事务操作
	mockStore.AssertNotCalled(t, "ExecTxWithOptions", mock.Anything, mock.Anything)
	mockStore.AssertNotCalled(t, "ExecTx", mock.Anything, mock.Anything)
}

func TestUpdateDisk_NotFound(t *testing.T) {
	ctx, mockStore, mockQuerier, mgr := setupUpdateTest(t)
	validDiskID := "0192a3b4-c5d6-7e8f-9012-3456789abcde"
	pgID := parseUUID(t, validDiskID)

	mockUpdateExecTx(mockStore, mockQuerier)
	mockQuerier.EXPECT().
		GetDiskForUpdate(ctx, pgID).
		Return(db.Disk{}, pgx.ErrNoRows).
		Once()

	result, err := mgr.UpdateDisk(ctx, disk.UpdateDiskParams{
		DiskID: validDiskID,
		Name:   ptr("new-name"),
	})
	assert.Nil(t, result)
	require.Error(t, err)
	assert.ErrorIs(t, err, disk.ErrNotFound)
}

func TestUpdateDisk_RenameOnly(t *testing.T) {
	ctx, mockStore, mockQuerier, mgr := setupUpdateTest(t)
	validDiskID := "0192a3b4-c5d6-7e8f-9012-3456789abcde"
	pgID := parseUUID(t, validDiskID)
	now := time.Now().Truncate(time.Microsecond)

	currentRow := db.Disk{
		DiskID:    pgID,
		Name:      "old-disk-name",
		RootIno:   1001,
		MaxBytes:  10737418240,
		UsedBytes: 1048576,
		CreatedAt: pgtype.Timestamptz{Time: now.Add(-time.Hour), Valid: true},
		UpdatedAt: pgtype.Timestamptz{Time: now.Add(-time.Hour), Valid: true},
	}

	updatedRow := db.Disk{
		DiskID:    pgID,
		Name:      "new-disk-name",
		RootIno:   1001,
		MaxBytes:  10737418240,
		UsedBytes: 1048576,
		CreatedAt: pgtype.Timestamptz{Time: now.Add(-time.Hour), Valid: true},
		UpdatedAt: pgtype.Timestamptz{Time: now, Valid: true},
	}

	mockUpdateExecTx(mockStore, mockQuerier)
	mockQuerier.EXPECT().
		GetDiskForUpdate(ctx, pgID).
		Return(currentRow, nil).
		Once()
	mockQuerier.EXPECT().
		UpdateDisk(ctx, db.UpdateDiskParams{
			DiskID:   pgID,
			Name:     "new-disk-name",
			MaxBytes: 10737418240,
		}).
		Return(updatedRow, nil).
		Once()

	result, err := mgr.UpdateDisk(ctx, disk.UpdateDiskParams{
		DiskID: validDiskID,
		Name:   ptr("new-disk-name"),
	})
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Equal(t, validDiskID, result.DiskID)
	assert.Equal(t, "new-disk-name", result.Name)
	assert.Equal(t, uint64(10737418240), result.MaxBytes)
	assert.Equal(t, uint64(1048576), result.UsedBytes)
	assert.Equal(t, uint64(1001), result.RootIno)
}

func TestUpdateDisk_ExpandMaxBytesOnly(t *testing.T) {
	ctx, mockStore, mockQuerier, mgr := setupUpdateTest(t)
	validDiskID := "0192a3b4-c5d6-7e8f-9012-3456789abcde"
	pgID := parseUUID(t, validDiskID)
	now := time.Now().Truncate(time.Microsecond)

	currentRow := db.Disk{
		DiskID:    pgID,
		Name:      "disk-1",
		RootIno:   2001,
		MaxBytes:  1000,
		UsedBytes: 200,
		CreatedAt: pgtype.Timestamptz{Time: now.Add(-time.Hour), Valid: true},
		UpdatedAt: pgtype.Timestamptz{Time: now.Add(-time.Hour), Valid: true},
	}

	updatedRow := db.Disk{
		DiskID:    pgID,
		Name:      "disk-1",
		RootIno:   2001,
		MaxBytes:  2000,
		UsedBytes: 200,
		CreatedAt: pgtype.Timestamptz{Time: now.Add(-time.Hour), Valid: true},
		UpdatedAt: pgtype.Timestamptz{Time: now, Valid: true},
	}

	mockUpdateExecTx(mockStore, mockQuerier)
	mockQuerier.EXPECT().
		GetDiskForUpdate(ctx, pgID).
		Return(currentRow, nil).
		Once()
	mockQuerier.EXPECT().
		UpdateDisk(ctx, db.UpdateDiskParams{
			DiskID:   pgID,
			Name:     "disk-1",
			MaxBytes: 2000,
		}).
		Return(updatedRow, nil).
		Once()

	result, err := mgr.UpdateDisk(ctx, disk.UpdateDiskParams{
		DiskID:   validDiskID,
		MaxBytes: ptr(uint64(2000)),
	})
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Equal(t, validDiskID, result.DiskID)
	assert.Equal(t, "disk-1", result.Name)
	assert.Equal(t, uint64(2000), result.MaxBytes)
	assert.Equal(t, uint64(200), result.UsedBytes)
	assert.Equal(t, uint64(2001), result.RootIno)
}

func TestUpdateDisk_ShrinkMaxBytes_EqualToUsedBytes(t *testing.T) {
	ctx, mockStore, mockQuerier, mgr := setupUpdateTest(t)
	validDiskID := "0192a3b4-c5d6-7e8f-9012-3456789abcde"
	pgID := parseUUID(t, validDiskID)
	now := time.Now().Truncate(time.Microsecond)

	currentRow := db.Disk{
		DiskID:    pgID,
		Name:      "disk-1",
		RootIno:   2001,
		MaxBytes:  1000,
		UsedBytes: 500,
		CreatedAt: pgtype.Timestamptz{Time: now.Add(-time.Hour), Valid: true},
		UpdatedAt: pgtype.Timestamptz{Time: now.Add(-time.Hour), Valid: true},
	}

	updatedRow := db.Disk{
		DiskID:    pgID,
		Name:      "disk-1",
		RootIno:   2001,
		MaxBytes:  500,
		UsedBytes: 500,
		CreatedAt: pgtype.Timestamptz{Time: now.Add(-time.Hour), Valid: true},
		UpdatedAt: pgtype.Timestamptz{Time: now, Valid: true},
	}

	mockUpdateExecTx(mockStore, mockQuerier)
	mockQuerier.EXPECT().
		GetDiskForUpdate(ctx, pgID).
		Return(currentRow, nil).
		Once()
	mockQuerier.EXPECT().
		UpdateDisk(ctx, db.UpdateDiskParams{
			DiskID:   pgID,
			Name:     "disk-1",
			MaxBytes: 500,
		}).
		Return(updatedRow, nil).
		Once()

	result, err := mgr.UpdateDisk(ctx, disk.UpdateDiskParams{
		DiskID:   validDiskID,
		MaxBytes: ptr(uint64(500)),
	})
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Equal(t, validDiskID, result.DiskID)
	assert.Equal(t, "disk-1", result.Name)
	assert.Equal(t, uint64(500), result.MaxBytes)
	assert.Equal(t, uint64(500), result.UsedBytes)
}

func TestUpdateDisk_ShrinkMaxBytes_BelowUsedBytes(t *testing.T) {
	ctx, mockStore, mockQuerier, mgr := setupUpdateTest(t)
	validDiskID := "0192a3b4-c5d6-7e8f-9012-3456789abcde"
	pgID := parseUUID(t, validDiskID)
	now := time.Now().Truncate(time.Microsecond)

	currentRow := db.Disk{
		DiskID:    pgID,
		Name:      "disk-1",
		RootIno:   2001,
		MaxBytes:  1000,
		UsedBytes: 500,
		CreatedAt: pgtype.Timestamptz{Time: now.Add(-time.Hour), Valid: true},
		UpdatedAt: pgtype.Timestamptz{Time: now.Add(-time.Hour), Valid: true},
	}

	mockUpdateExecTx(mockStore, mockQuerier)
	mockQuerier.EXPECT().
		GetDiskForUpdate(ctx, pgID).
		Return(currentRow, nil).
		Once()

	result, err := mgr.UpdateDisk(ctx, disk.UpdateDiskParams{
		DiskID:   validDiskID,
		MaxBytes: ptr(uint64(499)),
	})
	assert.Nil(t, result)
	require.Error(t, err)
	assert.ErrorIs(t, err, disk.ErrInvalidShrink)

	// 缩容防穿底校验拦截后，严禁调用 UpdateDisk
	mockQuerier.AssertNotCalled(t, "UpdateDisk", mock.Anything, mock.Anything)
}

func TestUpdateDisk_BothNameAndMaxBytes(t *testing.T) {
	ctx, mockStore, mockQuerier, mgr := setupUpdateTest(t)
	validDiskID := "0192a3b4-c5d6-7e8f-9012-3456789abcde"
	pgID := parseUUID(t, validDiskID)
	now := time.Now().Truncate(time.Microsecond)

	currentRow := db.Disk{
		DiskID:    pgID,
		Name:      "old-name",
		RootIno:   3001,
		MaxBytes:  1000,
		UsedBytes: 100,
		CreatedAt: pgtype.Timestamptz{Time: now.Add(-time.Hour), Valid: true},
		UpdatedAt: pgtype.Timestamptz{Time: now.Add(-time.Hour), Valid: true},
	}

	updatedRow := db.Disk{
		DiskID:    pgID,
		Name:      "new-name",
		RootIno:   3001,
		MaxBytes:  3000,
		UsedBytes: 100,
		CreatedAt: pgtype.Timestamptz{Time: now.Add(-time.Hour), Valid: true},
		UpdatedAt: pgtype.Timestamptz{Time: now, Valid: true},
	}

	mockUpdateExecTx(mockStore, mockQuerier)
	mockQuerier.EXPECT().
		GetDiskForUpdate(ctx, pgID).
		Return(currentRow, nil).
		Once()
	mockQuerier.EXPECT().
		UpdateDisk(ctx, db.UpdateDiskParams{
			DiskID:   pgID,
			Name:     "new-name",
			MaxBytes: 3000,
		}).
		Return(updatedRow, nil).
		Once()

	result, err := mgr.UpdateDisk(ctx, disk.UpdateDiskParams{
		DiskID:   validDiskID,
		Name:     ptr("new-name"),
		MaxBytes: ptr(uint64(3000)),
	})
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Equal(t, validDiskID, result.DiskID)
	assert.Equal(t, "new-name", result.Name)
	assert.Equal(t, uint64(3000), result.MaxBytes)
	assert.Equal(t, uint64(100), result.UsedBytes)
	assert.Equal(t, uint64(3001), result.RootIno)
}

func TestUpdateDisk_DBErrorPropagation(t *testing.T) {
	validDiskID := "0192a3b4-c5d6-7e8f-9012-3456789abcde"
	pgID := parseUUID(t, validDiskID)
	now := time.Now().Truncate(time.Microsecond)

	t.Run("GetDiskForUpdate 数据库错误透传", func(t *testing.T) {
		ctx, mockStore, mockQuerier, mgr := setupUpdateTest(t)
		mockUpdateExecTx(mockStore, mockQuerier)

		dbErr := assert.AnError
		mockQuerier.EXPECT().
			GetDiskForUpdate(ctx, pgID).
			Return(db.Disk{}, dbErr).
			Once()

		result, err := mgr.UpdateDisk(ctx, disk.UpdateDiskParams{
			DiskID: validDiskID,
			Name:   ptr("new-name"),
		})
		assert.Nil(t, result)
		require.Error(t, err)
		assert.ErrorIs(t, err, dbErr)
		assert.NotErrorIs(t, err, disk.ErrNotFound)
	})

	t.Run("UpdateDisk 数据库错误透传", func(t *testing.T) {
		ctx, mockStore, mockQuerier, mgr := setupUpdateTest(t)
		mockUpdateExecTx(mockStore, mockQuerier)

		currentRow := db.Disk{
			DiskID:    pgID,
			Name:      "disk-1",
			RootIno:   1001,
			MaxBytes:  1000,
			UsedBytes: 200,
			CreatedAt: pgtype.Timestamptz{Time: now, Valid: true},
			UpdatedAt: pgtype.Timestamptz{Time: now, Valid: true},
		}

		mockQuerier.EXPECT().
			GetDiskForUpdate(ctx, pgID).
			Return(currentRow, nil).
			Once()

		dbErr := assert.AnError
		mockQuerier.EXPECT().
			UpdateDisk(ctx, db.UpdateDiskParams{
				DiskID:   pgID,
				Name:     "new-name",
				MaxBytes: 1000,
			}).
			Return(db.Disk{}, dbErr).
			Once()

		result, err := mgr.UpdateDisk(ctx, disk.UpdateDiskParams{
			DiskID: validDiskID,
			Name:   ptr("new-name"),
		})
		assert.Nil(t, result)
		require.Error(t, err)
		assert.ErrorIs(t, err, dbErr)
	})

	t.Run("ExecTxWithOptions 事务开启或提交失败透传", func(t *testing.T) {
		ctx, mockStore, _, mgr := setupUpdateTest(t)

		txErr := assert.AnError
		mockStore.EXPECT().
			ExecTxWithOptions(mock.Anything, mock.Anything).
			Return(txErr).
			Once()

		result, err := mgr.UpdateDisk(ctx, disk.UpdateDiskParams{
			DiskID: validDiskID,
			Name:   ptr("new-name"),
		})
		assert.Nil(t, result)
		require.Error(t, err)
		assert.ErrorIs(t, err, txErr)
	})
}
