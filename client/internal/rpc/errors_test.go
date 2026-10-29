package rpc

import (
	"context"
	"errors"
	"io"
	"syscall"
	"testing"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/require"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
)

func TestMapError_All24ErrorInfos(t *testing.T) {
	testCases := []struct {
		reason        string
		expectedErrno syscall.Errno
	}{
		{"EINVAL", syscall.EINVAL},
		{"ENAMETOOLONG", syscall.ENAMETOOLONG},
		{"ERANGE", syscall.ERANGE},
		{"E2BIG", syscall.E2BIG},
		{"ENOENT", syscall.ENOENT},
		{"ENODATA", syscall.ENODATA},
		{"EEXIST", syscall.EEXIST},
		{"EPERM", syscall.EPERM},
		{"EACCES", syscall.EACCES},
		{"ENOTDIR", syscall.ENOTDIR},
		{"EISDIR", syscall.EISDIR},
		{"ENOTEMPTY", syscall.ENOTEMPTY},
		{"EBADF", syscall.EBADF},
		{"ENOSPC", syscall.ENOSPC},
		{"EAGAIN", syscall.EAGAIN},
		{"EDEADLK", syscall.EDEADLK},
		{"EINTR", syscall.EINTR},
		{"EOPNOTSUPP", syscall.EOPNOTSUPP},
		{"EMLINK", syscall.EMLINK},
		{"EFBIG", syscall.EFBIG},
		{"ESPIPE", syscall.ESPIPE},
		{"EXDEV", syscall.EXDEV},
		{"EIO", syscall.EIO},
		{"EROFS", syscall.EROFS},
	}

	require.Len(t, testCases, 24, "must cover exactly 24 POSIX ErrorInfo reasons")

	for _, tc := range testCases {
		t.Run("reason_"+tc.reason, func(t *testing.T) {
			// 构造带有 ErrorInfo 的 connect.Error
			connErr := connect.NewError(connect.CodeInternal, nil)
			info := &errdetails.ErrorInfo{
				Reason: tc.reason,
				Domain: "bowl.storage",
			}
			detail, err := connect.NewErrorDetail(info)
			require.NoError(t, err)
			connErr.AddDetail(detail)

			errno := MapError(connErr)
			require.Equal(t, tc.expectedErrno, errno, "Reason %s must map to %v", tc.reason, tc.expectedErrno)
		})
	}
}

func TestMapError_ErrorInfoDomainAndReason(t *testing.T) {
	t.Run("empty reason in ErrorInfo falls through", func(t *testing.T) {
		connErr := connect.NewError(connect.CodeNotFound, nil)
		info := &errdetails.ErrorInfo{
			Reason: "",
			Domain: "bowl.storage",
		}
		detail, err := connect.NewErrorDetail(info)
		require.NoError(t, err)
		connErr.AddDetail(detail)

		// 无非空 reason，应该兜底到 connect.Code 映射 (CodeNotFound -> ENOENT)
		errno := MapError(connErr)
		require.Equal(t, syscall.ENOENT, errno)
	})

	t.Run("non-bowl domain with valid reason", func(t *testing.T) {
		connErr := connect.NewError(connect.CodeInvalidArgument, nil)
		info := &errdetails.ErrorInfo{
			Reason: "EEXIST",
			Domain: "custom.domain",
		}
		detail, err := connect.NewErrorDetail(info)
		require.NoError(t, err)
		connErr.AddDetail(detail)

		// 规范要求: Domain == "bowl.storage" 或非空 Reason
		errno := MapError(connErr)
		require.Equal(t, syscall.EEXIST, errno)
	})
}

func TestMapError_ConnectCodeFallback(t *testing.T) {
	testCases := []struct {
		code          connect.Code
		expectedErrno syscall.Errno
	}{
		{connect.CodeNotFound, syscall.ENOENT},
		{connect.CodeAlreadyExists, syscall.EEXIST},
		{connect.CodePermissionDenied, syscall.EACCES},
		{connect.CodeInvalidArgument, syscall.EINVAL},
		{connect.CodeUnimplemented, syscall.EOPNOTSUPP},
		{connect.CodeResourceExhausted, syscall.ENOSPC},
		{connect.CodeCanceled, syscall.EINTR},
		// 其余所有 Connect Code 一律兜底为 EIO
		{connect.CodeUnavailable, syscall.EIO},
		{connect.CodeInternal, syscall.EIO},
		{connect.CodeDeadlineExceeded, syscall.EIO},
		{connect.CodeUnknown, syscall.EIO},
		{connect.CodeAborted, syscall.EIO},
		{connect.CodeFailedPrecondition, syscall.EIO},
		{connect.CodeOutOfRange, syscall.EIO},
		{connect.CodeDataLoss, syscall.EIO},
		{connect.CodeUnauthenticated, syscall.EIO},
	}

	for _, tc := range testCases {
		t.Run("code_"+tc.code.String(), func(t *testing.T) {
			connErr := connect.NewError(tc.code, nil)
			errno := MapError(connErr)
			require.Equal(t, tc.expectedErrno, errno, "code %s must map to %v", tc.code, tc.expectedErrno)
		})
	}
}

func TestMapError_TransportAndNil(t *testing.T) {
	t.Run("nil error returns 0", func(t *testing.T) {
		require.Equal(t, syscall.Errno(0), MapError(nil))
	})

	t.Run("io.EOF maps to EIO", func(t *testing.T) {
		require.Equal(t, syscall.EIO, MapError(io.EOF))
	})

	t.Run("arbitrary network error maps to EIO", func(t *testing.T) {
		require.Equal(t, syscall.EIO, MapError(errors.New("connection reset by peer")))
	})

	t.Run("CodeCanceled wrapping ECONNRESET maps to EIO", func(t *testing.T) {
		connErr := connect.NewError(connect.CodeCanceled, syscall.ECONNRESET)
		require.Equal(t, syscall.EIO, MapError(connErr))
	})

	t.Run("CodeCanceled wrapping EPIPE maps to EIO", func(t *testing.T) {
		connErr := connect.NewError(connect.CodeCanceled, syscall.EPIPE)
		require.Equal(t, syscall.EIO, MapError(connErr))
	})
}

func TestContextWithCancelChan_CancelTriggered(t *testing.T) {
	cancel := make(chan struct{})
	ctx, cancelFn := ContextWithCancelChan(context.Background(), cancel)
	defer cancelFn()

	// 此时 context 尚未取消
	require.NoError(t, ctx.Err())

	// 模拟内核下发 FUSE_INTERRUPT 关闭 cancel 通道
	close(cancel)

	// 等待 context 取消
	<-ctx.Done()
	require.ErrorIs(t, ctx.Err(), context.Canceled)
	require.ErrorIs(t, context.Cause(ctx), ErrKernelInterrupted)

	// 验证 MapError 识别该 context 错误返回 EINTR
	require.Equal(t, syscall.EINTR, MapError(context.Cause(ctx)))
}

func TestContextWithCancelChan_NormalCancel(t *testing.T) {
	cancel := make(chan struct{})
	ctx, cancelFn := ContextWithCancelChan(context.Background(), cancel)

	cancelFn()
	<-ctx.Done()
	require.ErrorIs(t, ctx.Err(), context.Canceled)
	require.False(t, errors.Is(context.Cause(ctx), ErrKernelInterrupted))
}

func TestContextWithCancelChan_NilChan(t *testing.T) {
	ctx, cancelFn := ContextWithCancelChan(context.Background(), nil)
	require.NoError(t, ctx.Err())
	cancelFn()
	<-ctx.Done()
	require.ErrorIs(t, ctx.Err(), context.Canceled)
}

func TestMapErrorWithCancel(t *testing.T) {
	t.Run("prioritize EINTR when cancel chan is closed", func(t *testing.T) {
		cancel := make(chan struct{})
		close(cancel)

		// 即使底层返回的是 CodeInternal，由于 cancel 已关闭，必须优先返回 EINTR
		connErr := connect.NewError(connect.CodeInternal, errors.New("internal server failure"))
		errno := MapErrorWithCancel(connErr, cancel)
		require.Equal(t, syscall.EINTR, errno)
	})

	t.Run("fallback to regular mapping when cancel chan is open", func(t *testing.T) {
		cancel := make(chan struct{})
		connErr := connect.NewError(connect.CodeNotFound, nil)
		errno := MapErrorWithCancel(connErr, cancel)
		require.Equal(t, syscall.ENOENT, errno)
	})

	t.Run("handle nil cancel chan normally", func(t *testing.T) {
		connErr := connect.NewError(connect.CodePermissionDenied, nil)
		errno := MapErrorWithCancel(connErr, nil)
		require.Equal(t, syscall.EACCES, errno)
	})
}
