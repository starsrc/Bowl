package meta_test

import (
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/starsrc/bowl/services/storage/internal/meta"
)

func TestToStatus_All22SentinelErrors(t *testing.T) {
	testCases := []struct {
		name           string
		err            error
		expectedCode   codes.Code
		expectedReason string
	}{
		{
			name:           "ErrInvalidArgument 映射为 InvalidArgument 与 EINVAL",
			err:            meta.ErrInvalidArgument,
			expectedCode:   codes.InvalidArgument,
			expectedReason: "EINVAL",
		},
		{
			name:           "ErrNameTooLong 映射为 InvalidArgument 与 ENAMETOOLONG",
			err:            meta.ErrNameTooLong,
			expectedCode:   codes.InvalidArgument,
			expectedReason: "ENAMETOOLONG",
		},
		{
			name:           "ErrRange 映射为 InvalidArgument 与 ERANGE",
			err:            meta.ErrRange,
			expectedCode:   codes.InvalidArgument,
			expectedReason: "ERANGE",
		},
		{
			name:           "ErrTooBig 映射为 InvalidArgument 与 E2BIG",
			err:            meta.ErrTooBig,
			expectedCode:   codes.InvalidArgument,
			expectedReason: "E2BIG",
		},
		{
			name:           "ErrNotFound 映射为 NotFound 与 ENOENT",
			err:            meta.ErrNotFound,
			expectedCode:   codes.NotFound,
			expectedReason: "ENOENT",
		},
		{
			name:           "ErrNoData 映射为 NotFound 与 ENODATA",
			err:            meta.ErrNoData,
			expectedCode:   codes.NotFound,
			expectedReason: "ENODATA",
		},
		{
			name:           "ErrAlreadyExists 映射为 AlreadyExists 与 EEXIST",
			err:            meta.ErrAlreadyExists,
			expectedCode:   codes.AlreadyExists,
			expectedReason: "EEXIST",
		},
		{
			name:           "ErrPerm 映射为 PermissionDenied 与 EPERM",
			err:            meta.ErrPerm,
			expectedCode:   codes.PermissionDenied,
			expectedReason: "EPERM",
		},
		{
			name:           "ErrNotDir 映射为 FailedPrecondition 与 ENOTDIR",
			err:            meta.ErrNotDir,
			expectedCode:   codes.FailedPrecondition,
			expectedReason: "ENOTDIR",
		},
		{
			name:           "ErrIsDir 映射为 FailedPrecondition 与 EISDIR",
			err:            meta.ErrIsDir,
			expectedCode:   codes.FailedPrecondition,
			expectedReason: "EISDIR",
		},
		{
			name:           "ErrNotEmpty 映射为 FailedPrecondition 与 ENOTEMPTY",
			err:            meta.ErrNotEmpty,
			expectedCode:   codes.FailedPrecondition,
			expectedReason: "ENOTEMPTY",
		},
		{
			name:           "ErrBadFileHandle 映射为 FailedPrecondition 与 EBADF",
			err:            meta.ErrBadFileHandle,
			expectedCode:   codes.FailedPrecondition,
			expectedReason: "EBADF",
		},
		{
			name:           "ErrNoSpace 映射为 ResourceExhausted 与 ENOSPC",
			err:            meta.ErrNoSpace,
			expectedCode:   codes.ResourceExhausted,
			expectedReason: "ENOSPC",
		},
		{
			name:           "ErrAgain 映射为 ResourceExhausted 与 EAGAIN",
			err:            meta.ErrAgain,
			expectedCode:   codes.ResourceExhausted,
			expectedReason: "EAGAIN",
		},
		{
			name:           "ErrDeadlock 映射为 Aborted 与 EDEADLK",
			err:            meta.ErrDeadlock,
			expectedCode:   codes.Aborted,
			expectedReason: "EDEADLK",
		},
		{
			name:           "ErrInterrupted 映射为 Canceled 与 EINTR",
			err:            meta.ErrInterrupted,
			expectedCode:   codes.Canceled,
			expectedReason: "EINTR",
		},
		{
			name:           "ErrNotSupported 映射为 Unimplemented 与 EOPNOTSUPP",
			err:            meta.ErrNotSupported,
			expectedCode:   codes.Unimplemented,
			expectedReason: "EOPNOTSUPP",
		},
		{
			name:           "ErrTooManyLinks 映射为 ResourceExhausted 与 EMLINK",
			err:            meta.ErrTooManyLinks,
			expectedCode:   codes.ResourceExhausted,
			expectedReason: "EMLINK",
		},
		{
			name:           "ErrFileTooLarge 映射为 InvalidArgument 与 EFBIG",
			err:            meta.ErrFileTooLarge,
			expectedCode:   codes.InvalidArgument,
			expectedReason: "EFBIG",
		},
		{
			name:           "ErrIllegalSeek 映射为 InvalidArgument 与 ESPIPE",
			err:            meta.ErrIllegalSeek,
			expectedCode:   codes.InvalidArgument,
			expectedReason: "ESPIPE",
		},
		{
			name:           "ErrIO 映射为 Internal 与 EIO",
			err:            meta.ErrIO,
			expectedCode:   codes.Internal,
			expectedReason: "EIO",
		},
		{
			name:           "ErrAccess 映射为 PermissionDenied 与 EACCES",
			err:            meta.ErrAccess,
			expectedCode:   codes.PermissionDenied,
			expectedReason: "EACCES",
		},
	}

	require.Equal(t, 22, len(testCases), "必须完整覆盖全部 22 个领域哨兵错误")

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			st := meta.ToStatus(tc.err)
			require.NotNil(t, st)
			assert.Equal(t, tc.expectedCode, st.Code())
			assert.Equal(t, tc.err.Error(), st.Message())

			details := st.Details()
			require.Len(t, details, 1, "必须恰好包含一个 Detail (ErrorInfo)")

			info, ok := details[0].(*errdetails.ErrorInfo)
			require.True(t, ok, "Detail 必须为 *errdetails.ErrorInfo 类型")
			assert.Equal(t, meta.ErrorDomain, info.GetDomain())
			assert.Equal(t, "bowl.storage", info.GetDomain())
			assert.Equal(t, tc.expectedReason, info.GetReason())
		})
	}
}

func TestFileLockConstants(t *testing.T) {
	assert.Equal(t, uint32(0), meta.F_RDLCK)
	assert.Equal(t, uint32(1), meta.F_WRLCK)
	assert.Equal(t, uint32(2), meta.F_UNLCK)
	assert.Equal(t, uint32(0x1), meta.FuseLkFlock)
	assert.Equal(t, uint32(0x2), meta.FuseReleaseFlockUnlock)
	assert.Equal(t, uint64(0x7fffffffffffffff), meta.OffsetMax)
}

func TestToStatus_WrappedError(t *testing.T) {
	wrapped := fmt.Errorf("上层操作包装: %w", meta.ErrNotFound)
	st := meta.ToStatus(wrapped)

	require.NotNil(t, st)
	assert.Equal(t, codes.NotFound, st.Code())
	assert.Equal(t, wrapped.Error(), st.Message())

	details := st.Details()
	require.Len(t, details, 1)

	info, ok := details[0].(*errdetails.ErrorInfo)
	require.True(t, ok)
	assert.Equal(t, meta.ErrorDomain, info.GetDomain())
	assert.Equal(t, "ENOENT", info.GetReason())
}

func TestToStatus_NilError(t *testing.T) {
	assert.Nil(t, meta.ToStatus(nil))
	assert.NoError(t, meta.ToGRPCError(nil))
}

func TestToStatus_UnknownError(t *testing.T) {
	err := errors.New("底层数据库未预期的网络断开")
	st := meta.ToStatus(err)

	require.NotNil(t, st)
	assert.Equal(t, codes.Unknown, st.Code())
	assert.Equal(t, err.Error(), st.Message())
	assert.Empty(t, st.Details(), "未知错误不携带 ErrorInfo 领域信息")
}

func TestToGRPCError_Conversion(t *testing.T) {
	err := meta.ToGRPCError(meta.ErrNoSpace)
	require.Error(t, err)

	st, ok := status.FromError(err)
	require.True(t, ok)
	assert.Equal(t, codes.ResourceExhausted, st.Code())

	details := st.Details()
	require.Len(t, details, 1)
	info, ok := details[0].(*errdetails.ErrorInfo)
	require.True(t, ok)
	assert.Equal(t, meta.ErrorDomain, info.GetDomain())
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
		Domain: meta.ErrorDomain,
	})
	if err == nil {
		return withDetails
	}
	return st
}

func TestToStatus_PreservesValidatorGRPCStatus(t *testing.T) {
	for _, reason := range []string{"ERANGE", "E2BIG", "ENAMETOOLONG", "EINVAL"} {
		t.Run(reason, func(t *testing.T) {
			valErr := &mockGRPCStatusError{
				msg:    "validation failure: " + reason,
				reason: reason,
			}
			st := meta.ToStatus(valErr)
			require.NotNil(t, st)
			assert.Equal(t, codes.InvalidArgument, st.Code())

			details := st.Details()
			require.Len(t, details, 1)
			info, ok := details[0].(*errdetails.ErrorInfo)
			require.True(t, ok)
			assert.Equal(t, meta.ErrorDomain, info.GetDomain())
			assert.Equal(t, reason, info.GetReason())
		})
	}
}
