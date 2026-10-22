package disk_test

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

	"github.com/starsrc/Aether/pkg/tx"
	"github.com/starsrc/bowl/services/disk/internal/db"
	dbmock "github.com/starsrc/bowl/services/disk/internal/db/mock"
	"github.com/starsrc/bowl/services/disk/internal/disk"
)

func setupUsageTest(t *testing.T) (context.Context, *dbmock.MockStore, *dbmock.MockQuerier, disk.Manager) {
	ctx := context.Background()
	mockStore := dbmock.NewMockStore(t)
	mockQuerier := dbmock.NewMockQuerier(t)
	mgr := disk.New(mockStore)
	return ctx, mockStore, mockQuerier, mgr
}

func mockUsageExecTx(mockStore *dbmock.MockStore, mockQuerier *dbmock.MockQuerier) {
	mockStore.EXPECT().
		ExecTxWithOptions(mock.Anything, mock.Anything).
		RunAndReturn(func(ctx context.Context, fn func(db.Querier) error, opts ...tx.Option) error {
			return fn(mockQuerier)
		}).
		Once()
}

// TestUpdateDiskUsage_StaticValidation 验证静态参数校验（不查库，不开启事务）。
func TestUpdateDiskUsage_StaticValidation(t *testing.T) {
	ctx, mockStore, _, mgr := setupUsageTest(t)

	tests := []struct {
		name       string
		diskID     string
		deltaBytes int64
	}{
		{
			name:       "空字符串 DiskID",
			diskID:     "",
			deltaBytes: 100,
		},
		{
			name:       "非 UUID 格式 DiskID",
			diskID:     "not-a-valid-uuid",
			deltaBytes: 100,
		},
		{
			name:       "全零 Nil UUID",
			diskID:     "00000000-0000-0000-0000-000000000000",
			deltaBytes: 100,
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			result, err := mgr.UpdateDiskUsage(ctx, disk.UpdateDiskUsageParams{
				DiskID:     tc.diskID,
				DeltaBytes: tc.deltaBytes,
			})
			require.Error(t, err)
			assert.Nil(t, result)
			assert.ErrorIs(t, err, disk.ErrInvalidArgument)
		})
	}

	mockStore.AssertNotCalled(t, "ExecTxWithOptions", mock.Anything, mock.Anything)
	mockStore.AssertNotCalled(t, "ExecTx", mock.Anything, mock.Anything)
}

// TestUpdateDiskUsage_NotFound 验证磁盘不存在时返回 ErrNotFound。
func TestUpdateDiskUsage_NotFound(t *testing.T) {
	ctx, mockStore, mockQuerier, mgr := setupUsageTest(t)
	validDiskID := "0192a3b4-c5d6-7e8f-9012-3456789abcde"
	var pgID pgtype.UUID
	require.NoError(t, pgID.Scan(validDiskID))

	mockUsageExecTx(mockStore, mockQuerier)
	mockQuerier.EXPECT().
		GetDiskForUpdate(mock.Anything, pgID).
		Return(db.Disk{}, pgx.ErrNoRows).
		Once()

	result, err := mgr.UpdateDiskUsage(ctx, disk.UpdateDiskUsageParams{
		DiskID:     validDiskID,
		DeltaBytes: 100,
	})

	require.Error(t, err)
	assert.Nil(t, result)
	assert.ErrorIs(t, err, disk.ErrNotFound)
	mockQuerier.AssertNotCalled(t, "UpdateDiskUsedBytes", mock.Anything, mock.Anything)
}

// TestUpdateDiskUsage_GetDiskForUpdateError 验证查询磁盘 DB 异常透明传递。
func TestUpdateDiskUsage_GetDiskForUpdateError(t *testing.T) {
	ctx, mockStore, mockQuerier, mgr := setupUsageTest(t)
	validDiskID := "0192a3b4-c5d6-7e8f-9012-3456789abcde"
	var pgID pgtype.UUID
	require.NoError(t, pgID.Scan(validDiskID))

	dbErr := errors.New("db query connection reset")
	mockUsageExecTx(mockStore, mockQuerier)
	mockQuerier.EXPECT().
		GetDiskForUpdate(mock.Anything, pgID).
		Return(db.Disk{}, dbErr).
		Once()

	result, err := mgr.UpdateDiskUsage(ctx, disk.UpdateDiskUsageParams{
		DiskID:     validDiskID,
		DeltaBytes: 100,
	})

	require.Error(t, err)
	assert.Nil(t, result)
	assert.ErrorIs(t, err, dbErr)
	mockQuerier.AssertNotCalled(t, "UpdateDiskUsedBytes", mock.Anything, mock.Anything)
}

// TestUpdateDiskUsage_ZeroDelta 验证 DeltaBytes 为 0 时幂等短路直接返回当前实体，不执行写操作。
func TestUpdateDiskUsage_ZeroDelta(t *testing.T) {
	ctx, mockStore, mockQuerier, mgr := setupUsageTest(t)
	validDiskID := "0192a3b4-c5d6-7e8f-9012-3456789abcde"
	var pgID pgtype.UUID
	require.NoError(t, pgID.Scan(validDiskID))

	now := time.Now().Truncate(time.Microsecond)
	currentDisk := db.Disk{
		DiskID:    pgID,
		Name:      "test-disk",
		RootIno:   1001,
		MaxBytes:  1000,
		UsedBytes: 250,
		CreatedAt: pgtype.Timestamptz{Time: now, Valid: true},
		UpdatedAt: pgtype.Timestamptz{Time: now, Valid: true},
	}

	mockUsageExecTx(mockStore, mockQuerier)
	mockQuerier.EXPECT().
		GetDiskForUpdate(mock.Anything, pgID).
		Return(currentDisk, nil).
		Once()

	result, err := mgr.UpdateDiskUsage(ctx, disk.UpdateDiskUsageParams{
		DiskID:     validDiskID,
		DeltaBytes: 0,
	})

	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Equal(t, validDiskID, result.DiskID)
	assert.Equal(t, "test-disk", result.Name)
	assert.Equal(t, uint64(1001), result.RootIno)
	assert.Equal(t, uint64(1000), result.MaxBytes)
	assert.Equal(t, uint64(250), result.UsedBytes)
	assert.Equal(t, now, result.CreatedAt)
	assert.Equal(t, now, result.UpdatedAt)

	mockQuerier.AssertNotCalled(t, "UpdateDiskUsedBytes", mock.Anything, mock.Anything)
}

// TestUpdateDiskUsage_PositiveIncrement 验证正向扩容/用量累加与配额上限保护。
func TestUpdateDiskUsage_PositiveIncrement(t *testing.T) {
	validDiskID := "0192a3b4-c5d6-7e8f-9012-3456789abcde"
	var pgID pgtype.UUID
	require.NoError(t, pgID.Scan(validDiskID))
	now := time.Now().Truncate(time.Microsecond)

	t.Run("正常累加用量不超过上限", func(t *testing.T) {
		ctx, mockStore, mockQuerier, mgr := setupUsageTest(t)

		currentDisk := db.Disk{
			DiskID:    pgID,
			Name:      "test-disk",
			RootIno:   1001,
			MaxBytes:  1000,
			UsedBytes: 400,
			CreatedAt: pgtype.Timestamptz{Time: now, Valid: true},
			UpdatedAt: pgtype.Timestamptz{Time: now, Valid: true},
		}
		updatedDisk := db.Disk{
			DiskID:    pgID,
			Name:      "test-disk",
			RootIno:   1001,
			MaxBytes:  1000,
			UsedBytes: 650,
			CreatedAt: pgtype.Timestamptz{Time: now, Valid: true},
			UpdatedAt: pgtype.Timestamptz{Time: now.Add(time.Second), Valid: true},
		}

		mockUsageExecTx(mockStore, mockQuerier)
		mockQuerier.EXPECT().
			GetDiskForUpdate(mock.Anything, pgID).
			Return(currentDisk, nil).
			Once()
		mockQuerier.EXPECT().
			UpdateDiskUsedBytes(mock.Anything, db.UpdateDiskUsedBytesParams{
				DiskID:    pgID,
				UsedBytes: 650,
			}).
			Return(updatedDisk, nil).
			Once()

		result, err := mgr.UpdateDiskUsage(ctx, disk.UpdateDiskUsageParams{
			DiskID:     validDiskID,
			DeltaBytes: 250,
		})

		require.NoError(t, err)
		require.NotNil(t, result)
		assert.Equal(t, uint64(650), result.UsedBytes)
	})

	t.Run("恰好填满配额上限 (usedBytes + delta == maxBytes)", func(t *testing.T) {
		ctx, mockStore, mockQuerier, mgr := setupUsageTest(t)

		currentDisk := db.Disk{
			DiskID:    pgID,
			Name:      "test-disk",
			RootIno:   1001,
			MaxBytes:  1000,
			UsedBytes: 800,
			CreatedAt: pgtype.Timestamptz{Time: now, Valid: true},
			UpdatedAt: pgtype.Timestamptz{Time: now, Valid: true},
		}
		updatedDisk := db.Disk{
			DiskID:    pgID,
			Name:      "test-disk",
			RootIno:   1001,
			MaxBytes:  1000,
			UsedBytes: 1000,
			CreatedAt: pgtype.Timestamptz{Time: now, Valid: true},
			UpdatedAt: pgtype.Timestamptz{Time: now.Add(time.Second), Valid: true},
		}

		mockUsageExecTx(mockStore, mockQuerier)
		mockQuerier.EXPECT().
			GetDiskForUpdate(mock.Anything, pgID).
			Return(currentDisk, nil).
			Once()
		mockQuerier.EXPECT().
			UpdateDiskUsedBytes(mock.Anything, db.UpdateDiskUsedBytesParams{
				DiskID:    pgID,
				UsedBytes: 1000,
			}).
			Return(updatedDisk, nil).
			Once()

		result, err := mgr.UpdateDiskUsage(ctx, disk.UpdateDiskUsageParams{
			DiskID:     validDiskID,
			DeltaBytes: 200,
		})

		require.NoError(t, err)
		require.NotNil(t, result)
		assert.Equal(t, uint64(1000), result.UsedBytes)
	})

	t.Run("超出配额上限 1 字节拒绝并返回 ErrNoSpace", func(t *testing.T) {
		ctx, mockStore, mockQuerier, mgr := setupUsageTest(t)

		currentDisk := db.Disk{
			DiskID:    pgID,
			Name:      "test-disk",
			RootIno:   1001,
			MaxBytes:  1000,
			UsedBytes: 800,
			CreatedAt: pgtype.Timestamptz{Time: now, Valid: true},
			UpdatedAt: pgtype.Timestamptz{Time: now, Valid: true},
		}

		mockUsageExecTx(mockStore, mockQuerier)
		mockQuerier.EXPECT().
			GetDiskForUpdate(mock.Anything, pgID).
			Return(currentDisk, nil).
			Once()

		result, err := mgr.UpdateDiskUsage(ctx, disk.UpdateDiskUsageParams{
			DiskID:     validDiskID,
			DeltaBytes: 201, // 800 + 201 = 1001 > 1000
		})

		require.Error(t, err)
		assert.Nil(t, result)
		assert.ErrorIs(t, err, disk.ErrNoSpace)
		mockQuerier.AssertNotCalled(t, "UpdateDiskUsedBytes", mock.Anything, mock.Anything)
	})

	t.Run("当前用量已大于上限时正向增加直接返回 ErrNoSpace", func(t *testing.T) {
		ctx, mockStore, mockQuerier, mgr := setupUsageTest(t)

		currentDisk := db.Disk{
			DiskID:    pgID,
			Name:      "test-disk",
			RootIno:   1001,
			MaxBytes:  1000,
			UsedBytes: 1200,
			CreatedAt: pgtype.Timestamptz{Time: now, Valid: true},
			UpdatedAt: pgtype.Timestamptz{Time: now, Valid: true},
		}

		mockUsageExecTx(mockStore, mockQuerier)
		mockQuerier.EXPECT().
			GetDiskForUpdate(mock.Anything, pgID).
			Return(currentDisk, nil).
			Once()

		result, err := mgr.UpdateDiskUsage(ctx, disk.UpdateDiskUsageParams{
			DiskID:     validDiskID,
			DeltaBytes: 1,
		})

		require.Error(t, err)
		assert.Nil(t, result)
		assert.ErrorIs(t, err, disk.ErrNoSpace)
		mockQuerier.AssertNotCalled(t, "UpdateDiskUsedBytes", mock.Anything, mock.Anything)
	})
}

// TestUpdateDiskUsage_NegativeRelease 验证负向用量释放与下溢保护。
func TestUpdateDiskUsage_NegativeRelease(t *testing.T) {
	validDiskID := "0192a3b4-c5d6-7e8f-9012-3456789abcde"
	var pgID pgtype.UUID
	require.NoError(t, pgID.Scan(validDiskID))
	now := time.Now().Truncate(time.Microsecond)

	t.Run("正常释放未下溢", func(t *testing.T) {
		ctx, mockStore, mockQuerier, mgr := setupUsageTest(t)

		currentDisk := db.Disk{
			DiskID:    pgID,
			Name:      "test-disk",
			RootIno:   1001,
			MaxBytes:  1000,
			UsedBytes: 500,
			CreatedAt: pgtype.Timestamptz{Time: now, Valid: true},
			UpdatedAt: pgtype.Timestamptz{Time: now, Valid: true},
		}
		updatedDisk := db.Disk{
			DiskID:    pgID,
			Name:      "test-disk",
			RootIno:   1001,
			MaxBytes:  1000,
			UsedBytes: 300,
			CreatedAt: pgtype.Timestamptz{Time: now, Valid: true},
			UpdatedAt: pgtype.Timestamptz{Time: now.Add(time.Second), Valid: true},
		}

		mockUsageExecTx(mockStore, mockQuerier)
		mockQuerier.EXPECT().
			GetDiskForUpdate(mock.Anything, pgID).
			Return(currentDisk, nil).
			Once()
		mockQuerier.EXPECT().
			UpdateDiskUsedBytes(mock.Anything, db.UpdateDiskUsedBytesParams{
				DiskID:    pgID,
				UsedBytes: 300,
			}).
			Return(updatedDisk, nil).
			Once()

		result, err := mgr.UpdateDiskUsage(ctx, disk.UpdateDiskUsageParams{
			DiskID:     validDiskID,
			DeltaBytes: -200,
		})

		require.NoError(t, err)
		require.NotNil(t, result)
		assert.Equal(t, uint64(300), result.UsedBytes)
	})

	t.Run("释放量恰好等于当前已用量归零", func(t *testing.T) {
		ctx, mockStore, mockQuerier, mgr := setupUsageTest(t)

		currentDisk := db.Disk{
			DiskID:    pgID,
			Name:      "test-disk",
			RootIno:   1001,
			MaxBytes:  1000,
			UsedBytes: 500,
			CreatedAt: pgtype.Timestamptz{Time: now, Valid: true},
			UpdatedAt: pgtype.Timestamptz{Time: now, Valid: true},
		}
		updatedDisk := db.Disk{
			DiskID:    pgID,
			Name:      "test-disk",
			RootIno:   1001,
			MaxBytes:  1000,
			UsedBytes: 0,
			CreatedAt: pgtype.Timestamptz{Time: now, Valid: true},
			UpdatedAt: pgtype.Timestamptz{Time: now.Add(time.Second), Valid: true},
		}

		mockUsageExecTx(mockStore, mockQuerier)
		mockQuerier.EXPECT().
			GetDiskForUpdate(mock.Anything, pgID).
			Return(currentDisk, nil).
			Once()
		mockQuerier.EXPECT().
			UpdateDiskUsedBytes(mock.Anything, db.UpdateDiskUsedBytesParams{
				DiskID:    pgID,
				UsedBytes: 0,
			}).
			Return(updatedDisk, nil).
			Once()

		result, err := mgr.UpdateDiskUsage(ctx, disk.UpdateDiskUsageParams{
			DiskID:     validDiskID,
			DeltaBytes: -500,
		})

		require.NoError(t, err)
		require.NotNil(t, result)
		assert.Equal(t, uint64(0), result.UsedBytes)
	})

	t.Run("扣减量大于当前已用量下溢钳位归零", func(t *testing.T) {
		ctx, mockStore, mockQuerier, mgr := setupUsageTest(t)

		currentDisk := db.Disk{
			DiskID:    pgID,
			Name:      "test-disk",
			RootIno:   1001,
			MaxBytes:  1000,
			UsedBytes: 150,
			CreatedAt: pgtype.Timestamptz{Time: now, Valid: true},
			UpdatedAt: pgtype.Timestamptz{Time: now, Valid: true},
		}
		updatedDisk := db.Disk{
			DiskID:    pgID,
			Name:      "test-disk",
			RootIno:   1001,
			MaxBytes:  1000,
			UsedBytes: 0,
			CreatedAt: pgtype.Timestamptz{Time: now, Valid: true},
			UpdatedAt: pgtype.Timestamptz{Time: now.Add(time.Second), Valid: true},
		}

		mockUsageExecTx(mockStore, mockQuerier)
		mockQuerier.EXPECT().
			GetDiskForUpdate(mock.Anything, pgID).
			Return(currentDisk, nil).
			Once()
		mockQuerier.EXPECT().
			UpdateDiskUsedBytes(mock.Anything, db.UpdateDiskUsedBytesParams{
				DiskID:    pgID,
				UsedBytes: 0,
			}).
			Return(updatedDisk, nil).
			Once()

		result, err := mgr.UpdateDiskUsage(ctx, disk.UpdateDiskUsageParams{
			DiskID:     validDiskID,
			DeltaBytes: -300,
		})

		require.NoError(t, err)
		require.NotNil(t, result)
		assert.Equal(t, uint64(0), result.UsedBytes)
	})

	t.Run("math.MinInt64 极值扣减安全计算且钳位归零", func(t *testing.T) {
		ctx, mockStore, mockQuerier, mgr := setupUsageTest(t)

		currentDisk := db.Disk{
			DiskID:    pgID,
			Name:      "test-disk",
			RootIno:   1001,
			MaxBytes:  1000,
			UsedBytes: 500,
			CreatedAt: pgtype.Timestamptz{Time: now, Valid: true},
			UpdatedAt: pgtype.Timestamptz{Time: now, Valid: true},
		}
		updatedDisk := db.Disk{
			DiskID:    pgID,
			Name:      "test-disk",
			RootIno:   1001,
			MaxBytes:  1000,
			UsedBytes: 0,
			CreatedAt: pgtype.Timestamptz{Time: now, Valid: true},
			UpdatedAt: pgtype.Timestamptz{Time: now.Add(time.Second), Valid: true},
		}

		mockUsageExecTx(mockStore, mockQuerier)
		mockQuerier.EXPECT().
			GetDiskForUpdate(mock.Anything, pgID).
			Return(currentDisk, nil).
			Once()
		mockQuerier.EXPECT().
			UpdateDiskUsedBytes(mock.Anything, db.UpdateDiskUsedBytesParams{
				DiskID:    pgID,
				UsedBytes: 0,
			}).
			Return(updatedDisk, nil).
			Once()

		result, err := mgr.UpdateDiskUsage(ctx, disk.UpdateDiskUsageParams{
			DiskID:     validDiskID,
			DeltaBytes: math.MinInt64,
		})

		require.NoError(t, err)
		require.NotNil(t, result)
		assert.Equal(t, uint64(0), result.UsedBytes)
	})
}

// TestUpdateDiskUsage_UpdateDiskUsedBytesError 验证更新账本 DB 异常透明传递。
func TestUpdateDiskUsage_UpdateDiskUsedBytesError(t *testing.T) {
	ctx, mockStore, mockQuerier, mgr := setupUsageTest(t)
	validDiskID := "0192a3b4-c5d6-7e8f-9012-3456789abcde"
	var pgID pgtype.UUID
	require.NoError(t, pgID.Scan(validDiskID))
	now := time.Now().Truncate(time.Microsecond)

	currentDisk := db.Disk{
		DiskID:    pgID,
		Name:      "test-disk",
		RootIno:   1001,
		MaxBytes:  1000,
		UsedBytes: 200,
		CreatedAt: pgtype.Timestamptz{Time: now, Valid: true},
		UpdatedAt: pgtype.Timestamptz{Time: now, Valid: true},
	}
	dbErr := errors.New("db update failed: disk io error")

	mockUsageExecTx(mockStore, mockQuerier)
	mockQuerier.EXPECT().
		GetDiskForUpdate(mock.Anything, pgID).
		Return(currentDisk, nil).
		Once()
	mockQuerier.EXPECT().
		UpdateDiskUsedBytes(mock.Anything, db.UpdateDiskUsedBytesParams{
			DiskID:    pgID,
			UsedBytes: 300,
		}).
		Return(db.Disk{}, dbErr).
		Once()

	result, err := mgr.UpdateDiskUsage(ctx, disk.UpdateDiskUsageParams{
		DiskID:     validDiskID,
		DeltaBytes: 100,
	})

	require.Error(t, err)
	assert.Nil(t, result)
	assert.ErrorIs(t, err, dbErr)
}

// TestUpdateDiskUsage_TransactionError 验证事务管理器 ExecTxWithOptions 本身失败透传。
func TestUpdateDiskUsage_TransactionError(t *testing.T) {
	ctx, mockStore, _, mgr := setupUsageTest(t)
	validDiskID := "0192a3b4-c5d6-7e8f-9012-3456789abcde"

	txErr := errors.New("tx beginner connection refused")
	mockStore.EXPECT().
		ExecTxWithOptions(mock.Anything, mock.Anything).
		Return(txErr).
		Once()

	result, err := mgr.UpdateDiskUsage(ctx, disk.UpdateDiskUsageParams{
		DiskID:     validDiskID,
		DeltaBytes: 100,
	})

	require.Error(t, err)
	assert.Nil(t, result)
	assert.ErrorIs(t, err, txErr)
}
