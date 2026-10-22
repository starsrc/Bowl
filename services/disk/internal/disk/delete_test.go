package disk_test

import (
	"context"
	"errors"
	"testing"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	dbmock "github.com/starsrc/bowl/services/disk/internal/db/mock"
	"github.com/starsrc/bowl/services/disk/internal/disk"
)

// TestDeleteDisk_ParamValidation 测试 DeleteDisk 静态参数校验逻辑。
// 空字符串、非法格式 UUID 以及全零 Nil UUID 均必须直接返回 ErrInvalidArgument，且不触发数据库调用。
func TestDeleteDisk_ParamValidation(t *testing.T) {
	ctx := context.Background()

	testCases := []struct {
		name   string
		diskID string
	}{
		{
			name:   "空字符串 diskID",
			diskID: "",
		},
		{
			name:   "非法格式 diskID",
			diskID: "not-a-valid-uuid",
		},
		{
			name:   "全零 Nil UUID",
			diskID: "00000000-0000-0000-0000-000000000000",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			mockStore := dbmock.NewMockStore(t)
			mgr := disk.New(mockStore)

			err := mgr.DeleteDisk(ctx, tc.diskID)
			require.Error(t, err)
			assert.ErrorIs(t, err, disk.ErrInvalidArgument)
		})
	}
}

// TestDeleteDisk_Success 测试 DeleteDisk 成功删除磁盘（受影响行数为 1）。
func TestDeleteDisk_Success(t *testing.T) {
	ctx := context.Background()
	diskIDStr := "0192a3b4-c5d6-7e8f-9012-3456789abcde"

	var expectedUUID pgtype.UUID
	err := expectedUUID.Scan(diskIDStr)
	require.NoError(t, err)

	mockStore := dbmock.NewMockStore(t)
	mockStore.EXPECT().
		DeleteDisk(ctx, expectedUUID).
		Return(int64(1), nil).
		Once()

	mgr := disk.New(mockStore)
	err = mgr.DeleteDisk(ctx, diskIDStr)
	require.NoError(t, err)
}

// TestDeleteDisk_NotFound 测试删除不存在的磁盘（受影响行数为 0）时返回 ErrNotFound。
func TestDeleteDisk_NotFound(t *testing.T) {
	ctx := context.Background()
	diskIDStr := "0192a3b4-c5d6-7e8f-9012-3456789abcde"

	var expectedUUID pgtype.UUID
	err := expectedUUID.Scan(diskIDStr)
	require.NoError(t, err)

	mockStore := dbmock.NewMockStore(t)
	mockStore.EXPECT().
		DeleteDisk(ctx, expectedUUID).
		Return(int64(0), nil).
		Once()

	mgr := disk.New(mockStore)
	err = mgr.DeleteDisk(ctx, diskIDStr)
	require.Error(t, err)
	assert.ErrorIs(t, err, disk.ErrNotFound)
}

// TestDeleteDisk_StoreError 测试持久层 DeleteDisk 报错时错误透明透传。
func TestDeleteDisk_StoreError(t *testing.T) {
	ctx := context.Background()
	diskIDStr := "0192a3b4-c5d6-7e8f-9012-3456789abcde"

	var expectedUUID pgtype.UUID
	err := expectedUUID.Scan(diskIDStr)
	require.NoError(t, err)

	expectedErr := errors.New("database connection failed")

	mockStore := dbmock.NewMockStore(t)
	mockStore.EXPECT().
		DeleteDisk(ctx, expectedUUID).
		Return(int64(0), expectedErr).
		Once()

	mgr := disk.New(mockStore)
	err = mgr.DeleteDisk(ctx, diskIDStr)
	require.Error(t, err)
	assert.ErrorIs(t, err, expectedErr)
}
