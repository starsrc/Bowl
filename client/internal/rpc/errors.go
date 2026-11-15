package rpc

import (
	"context"
	"errors"
	"syscall"
	"time"

	"connectrpc.com/connect"

	"github.com/starsrc/bowl/pkg/rpcerr"
)

// inFlightCancelGracePeriod 为在途 RPC 收到内核 cancel (FUSE_INTERRUPT) 信号后的宽限期。
// Go 运行时每 ~10ms 发送 SIGURG 抢占信号会触发 Linux FUSE 内核下发 FUSE_INTERRUPT；
// 对已在途且可在数毫秒内完成的快速 RPC 给予短暂宽限，避免被 SIGURG 中途打断返回伪 EINTR。
const inFlightCancelGracePeriod = 15 * time.Millisecond

// ErrKernelInterrupted 表示因内核中断信号（例如 FUSE_INTERRUPT 导致 cancel 通道关闭）而引发的取消。
var ErrKernelInterrupted = errors.New("operation interrupted by kernel signal")

// ContextWithCancelChan 创建一个与内核 cancel <-chan struct{} 联动的派生 context。
// 当内核关闭 cancel 通道时，自动以 ErrKernelInterrupted 为原因取消派生上下文；
// 当外部显式调用返回的 CancelFunc 时，安全停止监听并取消上下文。
//
// 快速路径不分配 stop 通道与 sync.Once：cancelCause(nil) 本身幂等，且会关闭 ctx.Done()，
// 监听协程的两个 select 都以 ctx.Done() 作为退出信号，因此无需额外的停止通道。
func ContextWithCancelChan(parent context.Context, cancel <-chan struct{}) (context.Context, context.CancelFunc) {
	return contextWithCancelChanClock(parent, cancel, realClock{})
}

func contextWithCancelChanClock(parent context.Context, cancel <-chan struct{}, clk rpcClock) (context.Context, context.CancelFunc) {
	ctx, cancelCause := context.WithCancelCause(parent)
	if cancel == nil {
		return ctx, func() { cancelCause(nil) }
	}

	// 快速探测 cancel 通道是否已处于关闭状态
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

// MapErrorWithCancel 在调用 MapError 前优先检查内核 cancel 通道是否已触发关闭。
// 若内核 cancel 通道已关闭，直接优先返回 syscall.EINTR。
func MapErrorWithCancel(err error, cancel <-chan struct{}) syscall.Errno {
	if cancel != nil {
		select {
		case <-cancel:
			return syscall.EINTR
		default:
		}
	}
	return MapError(err)
}

// MapError 将 ConnectRPC 错误或底层传输错误还原为 Linux syscall.Errno。
// 若 err 为 nil，返回 0。
// 若 err 因 ErrKernelInterrupted 或 context.Canceled 取消，返回 syscall.EINTR。
// 若 err 携带 Domain == rpcerr.Domain 且非空 Reason 的 google.rpc.ErrorInfo，
// 按 18 类 POSIX Reason 契约映射为对应 syscall.Errno。
// 若未携带匹配域的 ErrorInfo 或 Reason 为非 POSIX 契约值，按 Connect 状态码兜底映射，其余未知或传输错误统一返回 syscall.EIO。
func MapError(err error) syscall.Errno {
	if err == nil {
		return 0
	}

	if errors.Is(err, ErrKernelInterrupted) || errors.Is(err, context.Canceled) {
		return syscall.EINTR
	}

	if connectErr, isConnect := errors.AsType[*connect.Error](err); isConnect {
		if reason, ok := rpcerr.ExtractReason(connectErr); ok {
			switch reason {
			case rpcerr.ReasonInvalidArgument:
				return syscall.EINVAL
			case rpcerr.ReasonNameTooLong:
				return syscall.ENAMETOOLONG
			case rpcerr.ReasonRange:
				return syscall.ERANGE
			case rpcerr.ReasonNotFound:
				return syscall.ENOENT
			case rpcerr.ReasonNoData:
				return syscall.ENODATA
			case rpcerr.ReasonAlreadyExists:
				return syscall.EEXIST
			case rpcerr.ReasonPermissionDenied:
				return syscall.EPERM
			case rpcerr.ReasonReadOnly:
				return syscall.EROFS
			case rpcerr.ReasonNotDir:
				return syscall.ENOTDIR
			case rpcerr.ReasonIsDir:
				return syscall.EISDIR
			case rpcerr.ReasonNotEmpty:
				return syscall.ENOTEMPTY
			case rpcerr.ReasonNoSpace:
				return syscall.ENOSPC
			case rpcerr.ReasonNotSupported:
				return syscall.EOPNOTSUPP
			case rpcerr.ReasonTooManyLinks:
				return syscall.EMLINK
			case rpcerr.ReasonFileTooLarge:
				return syscall.EFBIG
			case rpcerr.ReasonCrossDevice:
				return syscall.EXDEV
			case rpcerr.ReasonIO:
				return syscall.EIO
			case rpcerr.ReasonBusy:
				return syscall.EBUSY
			}
		}

		// 无 ErrorInfo 时的 Connect 状态码兜底映射
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
