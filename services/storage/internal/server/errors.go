package server

import (
	"context"
	"errors"

	"connectrpc.com/connect"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/starsrc/bowl/services/storage/internal/meta"
)

// grpcCodeToConnectCode 将 gRPC codes.Code 精确映射为对应的 connect.Code。
func grpcCodeToConnectCode(c codes.Code) connect.Code {
	switch c {
	case codes.OK:
		return 0
	case codes.Canceled:
		return connect.CodeCanceled
	case codes.Unknown:
		return connect.CodeUnknown
	case codes.InvalidArgument:
		return connect.CodeInvalidArgument
	case codes.DeadlineExceeded:
		return connect.CodeDeadlineExceeded
	case codes.NotFound:
		return connect.CodeNotFound
	case codes.AlreadyExists:
		return connect.CodeAlreadyExists
	case codes.PermissionDenied:
		return connect.CodePermissionDenied
	case codes.ResourceExhausted:
		return connect.CodeResourceExhausted
	case codes.FailedPrecondition:
		return connect.CodeFailedPrecondition
	case codes.Aborted:
		return connect.CodeAborted
	case codes.OutOfRange:
		return connect.CodeOutOfRange
	case codes.Unimplemented:
		return connect.CodeUnimplemented
	case codes.Internal:
		return connect.CodeInternal
	case codes.Unavailable:
		return connect.CodeUnavailable
	case codes.DataLoss:
		return connect.CodeDataLoss
	case codes.Unauthenticated:
		return connect.CodeUnauthenticated
	default:
		return connect.CodeUnknown
	}
}

// statusToConnectError 将 gRPC *status.Status（含 google.rpc.ErrorInfo 等 Details）无损转换为 *connect.Error。
// 若 st == nil 或 st.Code() == codes.OK，返回 nil。
func statusToConnectError(st *status.Status) error {
	if st == nil || st.Code() == codes.OK {
		return nil
	}

	code := grpcCodeToConnectCode(st.Code())
	connErr := connect.NewError(code, errors.New(st.Message()))

	for _, detail := range st.Details() {
		if msg, ok := detail.(proto.Message); ok {
			if d, err := connect.NewErrorDetail(msg); err == nil {
				connErr.AddDetail(d)
			}
		}
	}

	return connErr
}

// StatusToConnectError 是 statusToConnectError 的导出版本，供外部包或测试直接调用。
func StatusToConnectError(st *status.Status) error {
	return statusToConnectError(st)
}

// domainSentinels 汇总 meta 领域层全部 24 个哨兵错误。
var domainSentinels = [...]error{
	meta.ErrInvalidShrink,
	meta.ErrInvalidArgument,
	meta.ErrNameTooLong,
	meta.ErrRange,
	meta.ErrTooBig,
	meta.ErrNotFound,
	meta.ErrNoData,
	meta.ErrAlreadyExists,
	meta.ErrPerm,
	meta.ErrNotDir,
	meta.ErrIsDir,
	meta.ErrNotEmpty,
	meta.ErrBadFileHandle,
	meta.ErrNoSpace,
	meta.ErrAgain,
	meta.ErrDeadlock,
	meta.ErrInterrupted,
	meta.ErrNotSupported,
	meta.ErrTooManyLinks,
	meta.ErrFileTooLarge,
	meta.ErrIllegalSeek,
	meta.ErrIO,
	meta.ErrAccess,
	meta.ErrCrossDevice,
}

// isDomainSentinel 检查 err 是否命中领域层哨兵错误。
func isDomainSentinel(err error) bool {
	for _, s := range domainSentinels {
		if errors.Is(err, s) {
			return true
		}
	}
	return false
}

// toConnectError 将任意错误（领域哨兵错误、上下文取消/超时或未知内部错误）转换为对应的 *connect.Error。
// 契约：
//  1. 若 err == nil 返回 nil；
//  2. 若 err 是未命中领域哨兵（即非 meta.ErrInterrupted 等领域错误）的纯 context.Canceled 或 context.DeadlineExceeded，
//     返回对应的 connect.NewError(connect.CodeCanceled, err) / connect.NewError(connect.CodeDeadlineExceeded, err)；
//  3. 其余统一通过 meta.ToStatus(err) 获取 *status.Status 并调用 statusToConnectError 转换。
func toConnectError(err error) error {
	if err == nil {
		return nil
	}

	if !isDomainSentinel(err) {
		if errors.Is(err, context.Canceled) {
			return connect.NewError(connect.CodeCanceled, err)
		}
		if errors.Is(err, context.DeadlineExceeded) {
			return connect.NewError(connect.CodeDeadlineExceeded, err)
		}
	}

	st := meta.ToStatus(err)
	return statusToConnectError(st)
}

// ToConnectError 是 toConnectError 的导出版本，供外部包或后续 RPC 实现调用。
func ToConnectError(err error) error {
	return toConnectError(err)
}

// ExtractErrorInfo 从错误中提取首个附带的 google.rpc.ErrorInfo 详情。
func ExtractErrorInfo(err error) *errdetails.ErrorInfo {
	var connErr *connect.Error
	if !errors.As(err, &connErr) {
		return nil
	}
	for _, detail := range connErr.Details() {
		msg, err := detail.Value()
		if err != nil {
			continue
		}
		if info, ok := msg.(*errdetails.ErrorInfo); ok {
			return info
		}
	}
	return nil
}
