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

func TestStatFS_ParameterValidation(t *testing.T) {
	ctx := context.Background()

	t.Run("无效 UUID 格式直接返回 ErrInvalidArgument 且不查询数据库", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mgr := disk.New(mockStore)

		res, err := mgr.StatFS(ctx, "invalid-uuid")
		require.Error(t, err)
		assert.Nil(t, res)
		assert.ErrorIs(t, err, disk.ErrInvalidArgument)
	})

	t.Run("空字符串 UUID 直接返回 ErrInvalidArgument 且不查询数据库", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mgr := disk.New(mockStore)

		res, err := mgr.StatFS(ctx, "")
		require.Error(t, err)
		assert.Nil(t, res)
		assert.ErrorIs(t, err, disk.ErrInvalidArgument)
	})

	t.Run("Nil 全零 UUID 直接返回 ErrInvalidArgument 且不查询数据库", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mgr := disk.New(mockStore)

		res, err := mgr.StatFS(ctx, "00000000-0000-0000-0000-000000000000")
		require.Error(t, err)
		assert.Nil(t, res)
		assert.ErrorIs(t, err, disk.ErrInvalidArgument)
	})
}

func TestStatFS_DatabaseErrors(t *testing.T) {
	ctx := context.Background()
	validUUIDStr := "0192a3b4-c5d6-7e8f-9012-3456789abcde"
	var validUUID pgtype.UUID
	require.NoError(t, validUUID.Scan(validUUIDStr))

	t.Run("磁盘不存在时返回 ErrNotFound", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockStore.EXPECT().
			GetDisk(mock.Anything, validUUID).
			Return(db.Disk{}, pgx.ErrNoRows).
			Once()

		mgr := disk.New(mockStore)
		res, err := mgr.StatFS(ctx, validUUIDStr)
		require.Error(t, err)
		assert.Nil(t, res)
		assert.ErrorIs(t, err, disk.ErrNotFound)
	})

	t.Run("数据库发生通用错误时透明透传", func(t *testing.T) {
		dbErr := errors.New("connection reset by peer")
		mockStore := dbmock.NewMockStore(t)
		mockStore.EXPECT().
			GetDisk(mock.Anything, validUUID).
			Return(db.Disk{}, dbErr).
			Once()

		mgr := disk.New(mockStore)
		res, err := mgr.StatFS(ctx, validUUIDStr)
		require.Error(t, err)
		assert.Nil(t, res)
		assert.ErrorIs(t, err, dbErr)
	})
}

func TestStatFS_Calculation_Normal(t *testing.T) {
	ctx := context.Background()
	validUUIDStr := "0192a3b4-c5d6-7e8f-9012-3456789abcde"
	var validUUID pgtype.UUID
	require.NoError(t, validUUID.Scan(validUUIDStr))

	now := time.Now().Truncate(time.Microsecond)
	// 场景：容量 10 GiB (10737418240 bytes), 已用 1 GiB (1073741824 bytes)
	// 4096 对齐:
	// totalBytes = 10737418240, blocks = 10737418240 / 4096 = 2621440
	// usedBytes = 1073741824
	// availBytes = 10737418240 - 1073741824 = 9663676416, bavail = 9663676416 / 4096 = 2359296
	// bfree = bavail = 2359296
	mockStore := dbmock.NewMockStore(t)
	mockStore.EXPECT().
		GetDisk(mock.Anything, validUUID).
		Return(db.Disk{
			DiskID:    validUUID,
			Name:      "prod-disk",
			RootIno:   1001,
			MaxBytes:  10737418240,
			UsedBytes: 1073741824,
			CreatedAt: pgtype.Timestamptz{Time: now, Valid: true},
			UpdatedAt: pgtype.Timestamptz{Time: now, Valid: true},
		}, nil).
		Once()

	mgr := disk.New(mockStore)
	res, err := mgr.StatFS(ctx, validUUIDStr)
	require.NoError(t, err)
	require.NotNil(t, res)

	assert.Equal(t, uint64(10737418240), res.TotalBytes)
	assert.Equal(t, uint64(1073741824), res.UsedBytes)
	assert.Equal(t, uint64(9663676416), res.AvailBytes)
	assert.Equal(t, uint32(disk.DefaultStatFSBlockSize), res.Bsize)
	assert.Equal(t, uint32(disk.DefaultStatFSBlockSize), res.Frsize)
	assert.Equal(t, uint64(2621440), res.Blocks)
	assert.Equal(t, uint64(2359296), res.Bavail)
	assert.Equal(t, uint64(2359296), res.Bfree)
	assert.Equal(t, uint64(0), res.Files)
	assert.Equal(t, uint64(0), res.Ffree)
	assert.Equal(t, uint32(disk.MaxDiskNameBytes), res.Namelen)
}

func TestStatFS_Calculation_EdgeCases(t *testing.T) {
	ctx := context.Background()
	validUUIDStr := "0192a3b4-c5d6-7e8f-9012-3456789abcde"
	var validUUID pgtype.UUID
	require.NoError(t, validUUID.Scan(validUUIDStr))

	now := time.Now().Truncate(time.Microsecond)

	t.Run("极小容量 0 < max_bytes < 4096 时 blocks 保底为 1", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockStore.EXPECT().
			GetDisk(mock.Anything, validUUID).
			Return(db.Disk{
				DiskID:    validUUID,
				Name:      "tiny-disk",
				RootIno:   2001,
				MaxBytes:  1024,
				UsedBytes: 256,
				CreatedAt: pgtype.Timestamptz{Time: now, Valid: true},
				UpdatedAt: pgtype.Timestamptz{Time: now, Valid: true},
			}, nil).
			Once()

		mgr := disk.New(mockStore)
		res, err := mgr.StatFS(ctx, validUUIDStr)
		require.NoError(t, err)
		require.NotNil(t, res)

		assert.Equal(t, uint64(1024), res.TotalBytes)
		assert.Equal(t, uint64(256), res.UsedBytes)
		assert.Equal(t, uint64(768), res.AvailBytes)
		assert.Equal(t, uint64(1), res.Blocks, "0 < total_bytes < 4096 必须保底为 1 块")
		assert.Equal(t, uint64(0), res.Bavail)
		assert.Equal(t, uint64(0), res.Bfree)
		assert.Equal(t, uint32(disk.DefaultStatFSBlockSize), res.Bsize)
		assert.Equal(t, uint32(disk.DefaultStatFSBlockSize), res.Frsize)
		assert.Equal(t, uint32(disk.MaxDiskNameBytes), res.Namelen)
	})

	t.Run("配额已满 used_bytes == max_bytes 时 avail_bytes 与 bavail 为 0", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockStore.EXPECT().
			GetDisk(mock.Anything, validUUID).
			Return(db.Disk{
				DiskID:    validUUID,
				Name:      "full-disk",
				RootIno:   2002,
				MaxBytes:  8192,
				UsedBytes: 8192,
				CreatedAt: pgtype.Timestamptz{Time: now, Valid: true},
				UpdatedAt: pgtype.Timestamptz{Time: now, Valid: true},
			}, nil).
			Once()

		mgr := disk.New(mockStore)
		res, err := mgr.StatFS(ctx, validUUIDStr)
		require.NoError(t, err)
		require.NotNil(t, res)

		assert.Equal(t, uint64(8192), res.TotalBytes)
		assert.Equal(t, uint64(8192), res.UsedBytes)
		assert.Equal(t, uint64(0), res.AvailBytes)
		assert.Equal(t, uint64(2), res.Blocks)
		assert.Equal(t, uint64(0), res.Bavail)
		assert.Equal(t, uint64(0), res.Bfree)
	})

	t.Run("防御性超配额 used_bytes > max_bytes 时 avail_bytes 钳位为 0 且 bavail 为 0", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockStore.EXPECT().
			GetDisk(mock.Anything, validUUID).
			Return(db.Disk{
				DiskID:    validUUID,
				Name:      "overflow-disk",
				RootIno:   2003,
				MaxBytes:  8192,
				UsedBytes: 12288,
				CreatedAt: pgtype.Timestamptz{Time: now, Valid: true},
				UpdatedAt: pgtype.Timestamptz{Time: now, Valid: true},
			}, nil).
			Once()

		mgr := disk.New(mockStore)
		res, err := mgr.StatFS(ctx, validUUIDStr)
		require.NoError(t, err)
		require.NotNil(t, res)

		assert.Equal(t, uint64(8192), res.TotalBytes)
		assert.Equal(t, uint64(12288), res.UsedBytes)
		assert.Equal(t, uint64(0), res.AvailBytes, "used > max 时 availBytes 必须钳位为 0，防止无符号整数反转下溢")
		assert.Equal(t, uint64(2), res.Blocks)
		assert.Equal(t, uint64(0), res.Bavail)
		assert.Equal(t, uint64(0), res.Bfree)
	})

	t.Run("容量为 0 时 blocks 与 avail_bytes 均为 0", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockStore.EXPECT().
			GetDisk(mock.Anything, validUUID).
			Return(db.Disk{
				DiskID:    validUUID,
				Name:      "zero-disk",
				RootIno:   2004,
				MaxBytes:  0,
				UsedBytes: 0,
				CreatedAt: pgtype.Timestamptz{Time: now, Valid: true},
				UpdatedAt: pgtype.Timestamptz{Time: now, Valid: true},
			}, nil).
			Once()

		mgr := disk.New(mockStore)
		res, err := mgr.StatFS(ctx, validUUIDStr)
		require.NoError(t, err)
		require.NotNil(t, res)

		assert.Equal(t, uint64(0), res.TotalBytes)
		assert.Equal(t, uint64(0), res.UsedBytes)
		assert.Equal(t, uint64(0), res.AvailBytes)
		assert.Equal(t, uint64(0), res.Blocks)
		assert.Equal(t, uint64(0), res.Bavail)
		assert.Equal(t, uint64(0), res.Bfree)
	})
}
