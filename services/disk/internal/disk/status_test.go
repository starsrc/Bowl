package disk_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/starsrc/bowl/services/disk/internal/disk"
	"github.com/starsrc/bowl/services/disk/internal/disk/mock"
)

func TestConstants(t *testing.T) {
	assert.Equal(t, uint32(4096), uint32(disk.DefaultStatFSBlockSize))
	assert.Equal(t, 255, disk.MaxDiskNameBytes)
	assert.Equal(t, "bowl.disk", disk.ErrorDomain)
}

func TestEntitiesAndParamStructs(t *testing.T) {
	now := time.Now()
	d := disk.Disk{
		DiskID:    "0192a3b4-c5d6-7e8f-9012-3456789abcde",
		Name:      "test-disk",
		RootIno:   1001,
		MaxBytes:  10737418240,
		UsedBytes: 1048576,
		CreatedAt: now,
		UpdatedAt: now,
	}
	assert.Equal(t, "0192a3b4-c5d6-7e8f-9012-3456789abcde", d.DiskID)
	assert.Equal(t, "test-disk", d.Name)
	assert.Equal(t, uint64(1001), d.RootIno)
	assert.Equal(t, uint64(10737418240), d.MaxBytes)
	assert.Equal(t, uint64(1048576), d.UsedBytes)
	assert.Equal(t, now, d.CreatedAt)
	assert.Equal(t, now, d.UpdatedAt)

	stat := disk.StatFSResult{
		TotalBytes: 10737418240,
		UsedBytes:  1048576,
		AvailBytes: 10736369664,
		Bsize:      4096,
		Frsize:     4096,
		Blocks:     2621440,
		Bfree:      2621184,
		Bavail:     2621184,
		Files:      0,
		Ffree:      0,
		Namelen:    255,
	}
	assert.Equal(t, uint32(4096), stat.Bsize)
	assert.Equal(t, uint32(4096), stat.Frsize)
	assert.Equal(t, uint32(255), stat.Namelen)

	createParams := disk.CreateDiskParams{
		Name:     "create-disk",
		RootIno:  2001,
		MaxBytes: 5368709120,
	}
	assert.Equal(t, "create-disk", createParams.Name)
	assert.Equal(t, uint64(2001), createParams.RootIno)
	assert.Equal(t, uint64(5368709120), createParams.MaxBytes)

	name := "updated-disk"
	maxBytes := uint64(21474836480)
	updateParams := disk.UpdateDiskParams{
		DiskID:   d.DiskID,
		Name:     &name,
		MaxBytes: &maxBytes,
	}
	assert.Equal(t, d.DiskID, updateParams.DiskID)
	assert.Equal(t, &name, updateParams.Name)
	assert.Equal(t, &maxBytes, updateParams.MaxBytes)

	usageParams := disk.UpdateDiskUsageParams{
		DiskID:     d.DiskID,
		DeltaBytes: 4096,
	}
	assert.Equal(t, d.DiskID, usageParams.DiskID)
	assert.Equal(t, int64(4096), usageParams.DeltaBytes)
}

func TestToStatus_All6SentinelErrors(t *testing.T) {
	testCases := []struct {
		name           string
		err            error
		expectedCode   codes.Code
		expectedReason string
	}{
		{
			name:           "ErrInvalidArgument 映射为 InvalidArgument 与 EINVAL",
			err:            disk.ErrInvalidArgument,
			expectedCode:   codes.InvalidArgument,
			expectedReason: "EINVAL",
		},
		{
			name:           "ErrNameTooLong 映射为 InvalidArgument 与 ENAMETOOLONG",
			err:            disk.ErrNameTooLong,
			expectedCode:   codes.InvalidArgument,
			expectedReason: "ENAMETOOLONG",
		},
		{
			name:           "ErrNotFound 映射为 NotFound 与 ENOENT",
			err:            disk.ErrNotFound,
			expectedCode:   codes.NotFound,
			expectedReason: "ENOENT",
		},
		{
			name:           "ErrAlreadyExists 映射为 AlreadyExists 与 EEXIST",
			err:            disk.ErrAlreadyExists,
			expectedCode:   codes.AlreadyExists,
			expectedReason: "EEXIST",
		},
		{
			name:           "ErrInvalidShrink 映射为 FailedPrecondition 与 EINVAL",
			err:            disk.ErrInvalidShrink,
			expectedCode:   codes.FailedPrecondition,
			expectedReason: "EINVAL",
		},
		{
			name:           "ErrNoSpace 映射为 ResourceExhausted 与 ENOSPC",
			err:            disk.ErrNoSpace,
			expectedCode:   codes.ResourceExhausted,
			expectedReason: "ENOSPC",
		},
	}

	require.Equal(t, 6, len(testCases), "必须完整覆盖全部 6 个领域哨兵错误")

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			st := disk.ToStatus(tc.err)
			require.NotNil(t, st)
			assert.Equal(t, tc.expectedCode, st.Code())
			assert.Equal(t, tc.err.Error(), st.Message())

			details := st.Details()
			require.Len(t, details, 1, "必须恰好包含一个 Detail (ErrorInfo)")

			info, ok := details[0].(*errdetails.ErrorInfo)
			require.True(t, ok, "Detail 必须为 *errdetails.ErrorInfo 类型")
			assert.Equal(t, disk.ErrorDomain, info.GetDomain())
			assert.Equal(t, "bowl.disk", info.GetDomain())
			assert.Equal(t, tc.expectedReason, info.GetReason())
		})
	}
}

func TestToStatus_WrappedError(t *testing.T) {
	wrapped := fmt.Errorf("上层操作包装: %w", disk.ErrNotFound)
	st := disk.ToStatus(wrapped)

	require.NotNil(t, st)
	assert.Equal(t, codes.NotFound, st.Code())
	assert.Equal(t, wrapped.Error(), st.Message())

	details := st.Details()
	require.Len(t, details, 1)

	info, ok := details[0].(*errdetails.ErrorInfo)
	require.True(t, ok)
	assert.Equal(t, disk.ErrorDomain, info.GetDomain())
	assert.Equal(t, "ENOENT", info.GetReason())
}

func TestToStatus_NilError(t *testing.T) {
	assert.Nil(t, disk.ToStatus(nil))
	assert.NoError(t, disk.ToGRPCError(nil))
}

func TestToStatus_UnknownError(t *testing.T) {
	err := errors.New("底层数据库未预期的网络断开")
	st := disk.ToStatus(err)

	require.NotNil(t, st)
	assert.Equal(t, codes.Unknown, st.Code())
	assert.Equal(t, err.Error(), st.Message())
	assert.Empty(t, st.Details(), "未知错误不携带 ErrorInfo 领域信息")
}

func TestToGRPCError_Conversion(t *testing.T) {
	err := disk.ToGRPCError(disk.ErrNoSpace)
	require.Error(t, err)

	st, ok := status.FromError(err)
	require.True(t, ok)
	assert.Equal(t, codes.ResourceExhausted, st.Code())

	details := st.Details()
	require.Len(t, details, 1)
	info, ok := details[0].(*errdetails.ErrorInfo)
	require.True(t, ok)
	assert.Equal(t, disk.ErrorDomain, info.GetDomain())
	assert.Equal(t, "ENOSPC", info.GetReason())
}

type mockGRPCStatusError struct {
	msg    string
	reason string
}

func (e *mockGRPCStatusError) Error() string { return e.msg }

func (e *mockGRPCStatusError) GRPCStatus() *status.Status {
	st := status.New(codes.InvalidArgument, e.msg)
	withDetails, err := st.WithDetails(&errdetails.ErrorInfo{
		Reason: e.reason,
		Domain: disk.ErrorDomain,
	})
	if err == nil {
		return withDetails
	}
	return st
}

func TestToStatus_PreservesValidatorGRPCStatus(t *testing.T) {
	for _, reason := range []string{"EINVAL", "ENAMETOOLONG"} {
		t.Run(reason, func(t *testing.T) {
			valErr := &mockGRPCStatusError{
				msg:    "validation failure: " + reason,
				reason: reason,
			}
			st := disk.ToStatus(valErr)
			require.NotNil(t, st)
			assert.Equal(t, codes.InvalidArgument, st.Code())

			details := st.Details()
			require.Len(t, details, 1)
			info, ok := details[0].(*errdetails.ErrorInfo)
			require.True(t, ok)
			assert.Equal(t, disk.ErrorDomain, info.GetDomain())
			assert.Equal(t, reason, info.GetReason())
		})
	}
}

func TestMockManager_ImplementsInterface(t *testing.T) {
	mockMgr := mock.NewMockManager(t)
	var mgr disk.Manager = mockMgr
	require.NotNil(t, mgr)

	ctx := context.Background()
	expectedDisk := &disk.Disk{
		DiskID:    "0192a3b4-c5d6-7e8f-9012-3456789abcde",
		Name:      "mock-disk",
		RootIno:   1001,
		MaxBytes:  1024,
		UsedBytes: 0,
	}

	mockMgr.EXPECT().GetDisk(ctx, "0192a3b4-c5d6-7e8f-9012-3456789abcde").Return(expectedDisk, nil).Once()

	res, err := mgr.GetDisk(ctx, "0192a3b4-c5d6-7e8f-9012-3456789abcde")
	require.NoError(t, err)
	assert.Equal(t, expectedDisk, res)
}
