package rpc

import (
	"context"
	"errors"
	"io"
	"syscall"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/require"
	"google.golang.org/genproto/googleapis/rpc/errdetails"

	"github.com/starsrc/bowl/pkg/rpcerr"
)

func requireReasonMapping(t *testing.T, reason string, expected syscall.Errno) {
	t.Helper()
	connErr := connect.NewError(connect.CodeInternal, nil)
	detail, err := connect.NewErrorDetail(&errdetails.ErrorInfo{Reason: reason, Domain: rpcerr.Domain})
	require.NoError(t, err)
	connErr.AddDetail(detail)
	require.Equal(t, expected, MapError(connErr), "Reason %s 必须映射为 %v", reason, expected)
}

func requireCodeMapping(t *testing.T, code connect.Code, expected syscall.Errno) {
	t.Helper()
	require.Equal(t, expected, MapError(connect.NewError(code, nil)), "状态码 %s 必须映射为 %v", code, expected)
}

func TestMapError_GivenAll18POSIXErrorInfos_MapsToExpectedErrno(t *testing.T) {
	requireReasonMapping(t, rpcerr.ReasonInvalidArgument, syscall.EINVAL)
	requireReasonMapping(t, rpcerr.ReasonNameTooLong, syscall.ENAMETOOLONG)
	requireReasonMapping(t, rpcerr.ReasonRange, syscall.ERANGE)
	requireReasonMapping(t, rpcerr.ReasonNotFound, syscall.ENOENT)
	requireReasonMapping(t, rpcerr.ReasonNoData, syscall.ENODATA)
	requireReasonMapping(t, rpcerr.ReasonAlreadyExists, syscall.EEXIST)
	requireReasonMapping(t, rpcerr.ReasonPermissionDenied, syscall.EPERM)
	requireReasonMapping(t, rpcerr.ReasonReadOnly, syscall.EROFS)
	requireReasonMapping(t, rpcerr.ReasonNotDir, syscall.ENOTDIR)
	requireReasonMapping(t, rpcerr.ReasonIsDir, syscall.EISDIR)
	requireReasonMapping(t, rpcerr.ReasonNotEmpty, syscall.ENOTEMPTY)
	requireReasonMapping(t, rpcerr.ReasonNoSpace, syscall.ENOSPC)
	requireReasonMapping(t, rpcerr.ReasonNotSupported, syscall.EOPNOTSUPP)
	requireReasonMapping(t, rpcerr.ReasonTooManyLinks, syscall.EMLINK)
	requireReasonMapping(t, rpcerr.ReasonFileTooLarge, syscall.EFBIG)
	requireReasonMapping(t, rpcerr.ReasonCrossDevice, syscall.EXDEV)
	requireReasonMapping(t, rpcerr.ReasonIO, syscall.EIO)
	requireReasonMapping(t, rpcerr.ReasonBusy, syscall.EBUSY)

	conflictErr := connect.NewError(connect.CodeAborted, nil)
	conflictDetail, err := connect.NewErrorDetail(&errdetails.ErrorInfo{
		Domain: rpcerr.Domain,
		Reason: rpcerr.ReasonConflict,
	})
	require.NoError(t, err)
	conflictErr.AddDetail(conflictDetail)
	require.Equal(t, syscall.EIO, MapError(conflictErr))
}

func TestMapError_GivenEmptyReason_FallsBackToConnectCode(t *testing.T) {
	// Given
	connErr := connect.NewError(connect.CodeNotFound, nil)
	detail, err := connect.NewErrorDetail(&errdetails.ErrorInfo{Reason: "", Domain: rpcerr.Domain})
	require.NoError(t, err)
	connErr.AddDetail(detail)

	// When / Then
	require.Equal(t, syscall.ENOENT, MapError(connErr))
}

func TestMapError_GivenForeignOrEmptyDomain_IgnoresReasonAndFallsBackToConnectCode(t *testing.T) {
	for _, domain := range []string{"custom.domain", ""} {
		// Given
		connErr := connect.NewError(connect.CodeInvalidArgument, nil)
		detail, err := connect.NewErrorDetail(&errdetails.ErrorInfo{
			Domain: domain,
			Reason: rpcerr.ReasonAlreadyExists,
		})
		require.NoError(t, err)
		connErr.AddDetail(detail)

		// When / Then
		require.Equal(t, syscall.EINVAL, MapError(connErr), "domain %q 必须忽略 Reason 并回退为 EINVAL", domain)
	}
}

func TestMapError_GivenConnectCodesWithoutErrorInfo_MapsDeterministically(t *testing.T) {
	requireCodeMapping(t, connect.CodeNotFound, syscall.ENOENT)
	requireCodeMapping(t, connect.CodeAlreadyExists, syscall.EEXIST)
	requireCodeMapping(t, connect.CodePermissionDenied, syscall.EACCES)
	requireCodeMapping(t, connect.CodeInvalidArgument, syscall.EINVAL)
	requireCodeMapping(t, connect.CodeUnimplemented, syscall.EOPNOTSUPP)
	requireCodeMapping(t, connect.CodeResourceExhausted, syscall.ENOSPC)
	requireCodeMapping(t, connect.CodeCanceled, syscall.EINTR)
	requireCodeMapping(t, connect.CodeUnavailable, syscall.EIO)
	requireCodeMapping(t, connect.CodeInternal, syscall.EIO)
	requireCodeMapping(t, connect.CodeDeadlineExceeded, syscall.EIO)
	requireCodeMapping(t, connect.CodeUnknown, syscall.EIO)
	requireCodeMapping(t, connect.CodeAborted, syscall.EIO)
	requireCodeMapping(t, connect.CodeFailedPrecondition, syscall.EBUSY)
	requireCodeMapping(t, connect.CodeOutOfRange, syscall.EIO)
	requireCodeMapping(t, connect.CodeDataLoss, syscall.EIO)
	requireCodeMapping(t, connect.CodeUnauthenticated, syscall.EIO)
}

func TestMapError_GivenNilAndTransportErrors_MapsExpectedErrno(t *testing.T) {
	require.Equal(t, syscall.Errno(0), MapError(nil))
	require.Equal(t, syscall.EIO, MapError(io.EOF))
	require.Equal(t, syscall.EIO, MapError(errors.New("connection reset by peer")))
	require.Equal(t, syscall.EIO, MapError(connect.NewError(connect.CodeCanceled, syscall.ECONNRESET)))
	require.Equal(t, syscall.EIO, MapError(connect.NewError(connect.CodeCanceled, syscall.EPIPE)))
}

func TestContextWithCancelChan_GivenPreClosedAndNilAndManualCancel_PropagatesExpectedCause(t *testing.T) {
	// Given / When / Then: 已关闭 cancel 通道立即触发 ErrKernelInterrupted -> EINTR
	closedCancel := make(chan struct{})
	close(closedCancel)
	interruptedCtx, cancelInterrupted := ContextWithCancelChan(context.Background(), closedCancel)
	defer cancelInterrupted()
	require.ErrorIs(t, interruptedCtx.Err(), context.Canceled)
	require.ErrorIs(t, context.Cause(interruptedCtx), ErrKernelInterrupted)
	require.Equal(t, syscall.EINTR, MapError(context.Cause(interruptedCtx)))

	// 未关闭 cancel 通道在显式取消后不携带 ErrKernelInterrupted
	openCancel := make(chan struct{})
	normalCtx, cancelNormal := ContextWithCancelChan(context.Background(), openCancel)
	cancelNormal()
	<-normalCtx.Done()
	require.ErrorIs(t, normalCtx.Err(), context.Canceled)
	require.NotErrorIs(t, context.Cause(normalCtx), ErrKernelInterrupted)

	// nil cancel 通道安全工作
	nilChanCtx, cancelNil := ContextWithCancelChan(context.Background(), nil)
	require.NoError(t, nilChanCtx.Err())
	cancelNil()
	<-nilChanCtx.Done()
	require.ErrorIs(t, nilChanCtx.Err(), context.Canceled)

	// MapErrorWithCancel 优先检查已关闭内核 cancel 通道
	require.Equal(t, syscall.EINTR, MapErrorWithCancel(connect.NewError(connect.CodeInternal, errors.New("boom")), closedCancel))
	require.Equal(t, syscall.ENOENT, MapErrorWithCancel(connect.NewError(connect.CodeNotFound, nil), openCancel))
	require.Equal(t, syscall.EACCES, MapErrorWithCancel(connect.NewError(connect.CodePermissionDenied, nil), nil))
}

func TestContextWithCancelChan_GivenGracePeriod_CompletedWithinGraceIgnoresInterruptExpiredTriggersEINTR(t *testing.T) {
	// Given: 场景 1 —— 宽限期内正常结束不触发 ErrKernelInterrupted
	clk1 := newManualClock(time.Date(2026, 11, 13, 10, 0, 0, 0, time.UTC))
	cancel1 := make(chan struct{})
	ctx1, finish1 := contextWithCancelChanClock(context.Background(), cancel1, clk1)

	// When
	close(cancel1)
	require.Equal(t, inFlightCancelGracePeriod, <-clk1.TimerArmed())
	finish1()
	<-ctx1.Done()

	// Then
	require.NotErrorIs(t, context.Cause(ctx1), ErrKernelInterrupted)

	// Given: 场景 2 —— 超过 15ms 宽限期触发 ErrKernelInterrupted -> EINTR
	clk2 := newManualClock(time.Date(2026, 11, 13, 10, 0, 0, 0, time.UTC))
	cancel2 := make(chan struct{})
	ctx2, finish2 := contextWithCancelChanClock(context.Background(), cancel2, clk2)
	defer finish2()

	// When
	close(cancel2)
	require.Equal(t, inFlightCancelGracePeriod, <-clk2.TimerArmed())
	clk2.Advance(inFlightCancelGracePeriod)
	<-ctx2.Done()

	// Then
	require.ErrorIs(t, context.Cause(ctx2), ErrKernelInterrupted)
	require.Equal(t, syscall.EINTR, MapError(context.Cause(ctx2)))
}
