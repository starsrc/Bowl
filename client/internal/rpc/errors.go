package rpc

import (
	"context"
	"errors"
	"sync"
	"syscall"

	"connectrpc.com/connect"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
)

// ErrKernelInterrupted 表示因内核中断信号（例如 FUSE_INTERRUPT 导致 cancel 通道关闭）而引发的取消。
var ErrKernelInterrupted = errors.New("operation interrupted by kernel signal")

// ContextWithCancelChan 创建一个与内核 cancel <-chan struct{} 联动的派生 context。
// 当内核关闭 cancel 通道时，自动以 ErrKernelInterrupted 为原因取消派生上下文；
// 当外部显式调用返回的 CancelFunc 时，安全停止监听并取消上下文。
func ContextWithCancelChan(parent context.Context, cancel <-chan struct{}) (context.Context, context.CancelFunc) {
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

	stop := make(chan struct{})
	var once sync.Once

	go func() {
		select {
		case <-cancel:
			cancelCause(ErrKernelInterrupted)
		case <-ctx.Done():
		case <-stop:
		}
	}()

	cancelFunc := func() {
		once.Do(func() {
			close(stop)
			cancelCause(nil)
		})
	}

	return ctx, cancelFunc
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

// MapError 将 ConnectRPC 错误或底层传输错误无损还原为 Linux syscall.Errno。
// 若 err 为 nil，返回 0（成功）。
// 若 err 因内核中断信号取消（ErrKernelInterrupted 或 context.Canceled），返回 syscall.EINTR。
// 若 err 携带 google.rpc.ErrorInfo（Domain == "bowl.storage" 或非空 Reason），
// 严格按 24 类 Reason 契约映射为对应 syscall.Errno。
// 若未携带匹配 ErrorInfo，按 Connect 状态码确定性兜底映射（其余未知或普通传输错误一律兜底为 syscall.EIO）。
func MapError(err error) syscall.Errno {
	if err == nil {
		return 0
	}

	if errors.Is(err, ErrKernelInterrupted) || errors.Is(err, context.Canceled) {
		return syscall.EINTR
	}

	var connectErr *connect.Error
	if errors.As(err, &connectErr) {
		for _, detail := range connectErr.Details() {
			msg, detailErr := detail.Value()
			if detailErr != nil {
				continue
			}
			if info, ok := msg.(*errdetails.ErrorInfo); ok {
				if (info.Domain == "bowl.storage" || info.Reason != "") && info.Reason != "" {
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
					}
				}
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
