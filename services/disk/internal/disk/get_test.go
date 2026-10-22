package disk_test

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

	"github.com/starsrc/bowl/services/disk/internal/db"
	dbmock "github.com/starsrc/bowl/services/disk/internal/db/mock"
	"github.com/starsrc/bowl/services/disk/internal/disk"
)

func TestGetDisk(t *testing.T) {
	const (
		validDiskIDStr = "0192a3b4-c5d6-7e8f-9012-3456789abcde"
		nilDiskIDStr   = "00000000-0000-0000-0000-000000000000"
	)

	var validDiskID pgtype.UUID
	err := validDiskID.Scan(validDiskIDStr)
	require.NoError(t, err)

	now := time.Now().Truncate(time.Microsecond)
	sampleDBDisk := db.Disk{
		DiskID:    validDiskID,
		Name:      "test-disk",
		RootIno:   2001,
		MaxBytes:  20 * 1024 * 1024 * 1024,
		UsedBytes: 5 * 1024 * 1024 * 1024,
		CreatedAt: pgtype.Timestamptz{Time: now, Valid: true},
		UpdatedAt: pgtype.Timestamptz{Time: now, Valid: true},
	}

	t.Run("成功查询存在的磁盘详情", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockStore.EXPECT().
			GetDisk(mock.Anything, validDiskID).
			Return(sampleDBDisk, nil).
			Once()

		mgr := disk.New(mockStore)
		result, err := mgr.GetDisk(context.Background(), validDiskIDStr)

		require.NoError(t, err)
		require.NotNil(t, result)
		assert.Equal(t, validDiskIDStr, result.DiskID)
		assert.Equal(t, sampleDBDisk.Name, result.Name)
		assert.Equal(t, uint64(sampleDBDisk.RootIno), result.RootIno)
		assert.Equal(t, uint64(sampleDBDisk.MaxBytes), result.MaxBytes)
		assert.Equal(t, uint64(sampleDBDisk.UsedBytes), result.UsedBytes)
		assert.Equal(t, now, result.CreatedAt)
		assert.Equal(t, now, result.UpdatedAt)
	})

	t.Run("静态校验拦截空 UUID 且不调用底层数据库", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		// mockStore 不注册任何预期，若触发调用则直接抛出 panic

		mgr := disk.New(mockStore)
		result, err := mgr.GetDisk(context.Background(), "")

		require.Error(t, err)
		assert.Nil(t, result)
		assert.ErrorIs(t, err, disk.ErrInvalidArgument)
	})

	t.Run("静态校验拦截非法格式 UUID 且不调用底层数据库", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)

		mgr := disk.New(mockStore)
		result, err := mgr.GetDisk(context.Background(), "invalid-uuid-format")

		require.Error(t, err)
		assert.Nil(t, result)
		assert.ErrorIs(t, err, disk.ErrInvalidArgument)
	})

	t.Run("静态校验拦截 Nil UUID 且不调用底层数据库", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)

		mgr := disk.New(mockStore)
		result, err := mgr.GetDisk(context.Background(), nilDiskIDStr)

		require.Error(t, err)
		assert.Nil(t, result)
		assert.ErrorIs(t, err, disk.ErrInvalidArgument)
	})

	t.Run("磁盘不存在时映射 pgx.ErrNoRows 为 ErrNotFound", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockStore.EXPECT().
			GetDisk(mock.Anything, validDiskID).
			Return(db.Disk{}, pgx.ErrNoRows).
			Once()

		mgr := disk.New(mockStore)
		result, err := mgr.GetDisk(context.Background(), validDiskIDStr)

		require.Error(t, err)
		assert.Nil(t, result)
		assert.ErrorIs(t, err, disk.ErrNotFound)
	})

	t.Run("底层数据库其他错误透明透传", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		dbErr := errors.New("database connection refused")
		mockStore.EXPECT().
			GetDisk(mock.Anything, validDiskID).
			Return(db.Disk{}, dbErr).
			Once()

		mgr := disk.New(mockStore)
		result, err := mgr.GetDisk(context.Background(), validDiskIDStr)

		require.Error(t, err)
		assert.Nil(t, result)
		assert.ErrorIs(t, err, dbErr)
		assert.False(t, errors.Is(err, disk.ErrNotFound))
	})
}
