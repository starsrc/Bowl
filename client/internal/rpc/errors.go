package rpc

import (
	"context"
	"errors"
	"syscall"
	"time"

	"connectrpc.com/connect"
	"github.com/starsrc/pkg/serverx"
)

const inFlightCancelGracePeriod = 15 * time.Millisecond

var ErrKernelInterrupted = errors.New("operation interrupted by kernel signal")

func ContextWithCancelChan(parent context.Context, cancel <-chan struct{}) (context.Context, context.CancelFunc) {
	return contextWithCancelChanClock(parent, cancel, realClock{})
}

func contextWithCancelChanClock(parent context.Context, cancel <-chan struct{}, clk rpcClock) (context.Context, context.CancelFunc) {
	ctx, cancelCause := context.WithCancelCause(parent)
	if cancel == nil {
		return ctx, func() { cancelCause(nil) }
	}

	select {
	case <-cancel:
		cancelCause(ErrKernelInterrupted)
		return ctx, func() { cancelCause(nil) }
	default:
	}

	go func() {
		select {
		case <-cancel:
			timer := clk.NewTimer(inFlightCancelGracePeriod)
			defer timer.Stop()
			select {
			case <-timer.Chan():
				cancelCause(ErrKernelInterrupted)
			case <-ctx.Done():
			}
		case <-ctx.Done():
		}
	}()

	return ctx, func() { cancelCause(nil) }
}

func MapError(err error) syscall.Errno {
	if err == nil {
		return 0
	}

	if errors.Is(err, ErrKernelInterrupted) || errors.Is(err, context.Canceled) {
		return syscall.EINTR
	}

	if connectErr, ok := errors.AsType[*connect.Error](err); ok {
		if info := serverx.ExtractErrorInfo(connectErr); info != nil && info.Reason != "" {
			switch info.Reason {
			case "EINVAL":
				return syscall.EINVAL
			case "ENAMETOOLONG":
				return syscall.ENAMETOOLONG
			case "ERANGE":
				return syscall.ERANGE
			case "E2BIG":
				return syscall.E2BIG
			case "ENOENT":
				return syscall.ENOENT
			case "ENODATA":
				return syscall.ENODATA
			case "EEXIST":
				return syscall.EEXIST
			case "EPERM":
				return syscall.EPERM
			case "EACCES":
				return syscall.EACCES
			case "ENOTDIR":
				return syscall.ENOTDIR
			case "EISDIR":
				return syscall.EISDIR
			case "ENOTEMPTY":
				return syscall.ENOTEMPTY
			case "EBADF":
				return syscall.EBADF
			case "ENOSPC":
				return syscall.ENOSPC
			case "EAGAIN":
				return syscall.EAGAIN
			case "EDEADLK":
				return syscall.EDEADLK
			case "EINTR":
				return syscall.EINTR
			case "EOPNOTSUPP":
				return syscall.EOPNOTSUPP
			case "EMLINK":
				return syscall.EMLINK
			case "EFBIG":
				return syscall.EFBIG
			case "ESPIPE":
				return syscall.ESPIPE
			case "EXDEV":
				return syscall.EXDEV
			case "EIO":
				return syscall.EIO
			case "EROFS":
				return syscall.EROFS
			case "EBUSY":
				return syscall.EBUSY
			}
		}

		switch connectErr.Code() {
		case connect.CodeNotFound:
			return syscall.ENOENT
		case connect.CodeAlreadyExists:
			return syscall.EEXIST
		case connect.CodePermissionDenied:
			return syscall.EACCES
		case connect.CodeInvalidArgument:
			return syscall.EINVAL
		case connect.CodeFailedPrecondition:
			return syscall.EBUSY
		case connect.CodeUnimplemented:
			return syscall.EOPNOTSUPP
		case connect.CodeResourceExhausted:
			return syscall.ENOSPC
		case connect.CodeCanceled:
			if isTransportError(connectErr.Unwrap()) {
				return syscall.EIO
			}
			return syscall.EINTR
		default:
			return syscall.EIO
		}
	}

	return syscall.EIO
}
