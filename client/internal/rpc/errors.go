package rpc

import (
	"context"
	"errors"
	"syscall"
	"time"

	"connectrpc.com/connect"
	"github.com/starsrc/pkg/serverx"
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

// MapError 将 ConnectRPC 错误或底层传输错误无损还原为 Linux syscall.Errno。
// 若 err 为 nil，返回 0（成功）。
// 若 err 因内核中断信号取消（ErrKernelInterrupted 或 context.Canceled），返回 syscall.EINTR。
// 若 err 携带 google.rpc.ErrorInfo（Domain == "bowl.storage" 或非空 Reason），
// 严格按 25 类 Reason 契约映射为对应 syscall.Errno。
// 若未携带匹配 ErrorInfo，按 Connect 状态码确定性兜底映射（其余未知或普通传输错误一律兜底为 syscall.EIO）。
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

		// 无 ErrorInfo 时的 Connect 状态码兜底映射
		switch connectErr.Code() {
		case connect.CodeNotFound:
			return syscall.ENOENT
		case connect.CodeAlreadyExists:
			return syscall.EEXIST
		case connect.CodePermissionDenied, connect.CodeUnauthenticated:
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
