package meta

import (
	"errors"

	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// ErrorDomain 定义全项目统一的 ErrorInfo 错误领域标识符。
const ErrorDomain = "bowl.storage"

// errorMapping 记录领域哨兵错误与 gRPC 状态码及 POSIX errno reason 的契约映射。
type errorMapping struct {
	sentinel error
	code     codes.Code
	reason   string
}

var mappings = []errorMapping{
	{sentinel: ErrInvalidArgument, code: codes.InvalidArgument, reason: "EINVAL"},
	{sentinel: ErrNameTooLong, code: codes.InvalidArgument, reason: "ENAMETOOLONG"},
	{sentinel: ErrRange, code: codes.InvalidArgument, reason: "ERANGE"},
	{sentinel: ErrTooBig, code: codes.InvalidArgument, reason: "E2BIG"},
	{sentinel: ErrNotFound, code: codes.NotFound, reason: "ENOENT"},
	{sentinel: ErrNoData, code: codes.NotFound, reason: "ENODATA"},
	{sentinel: ErrAlreadyExists, code: codes.AlreadyExists, reason: "EEXIST"},
	{sentinel: ErrPerm, code: codes.PermissionDenied, reason: "EPERM"},
	{sentinel: ErrNotDir, code: codes.FailedPrecondition, reason: "ENOTDIR"},
	{sentinel: ErrIsDir, code: codes.FailedPrecondition, reason: "EISDIR"},
	{sentinel: ErrNotEmpty, code: codes.FailedPrecondition, reason: "ENOTEMPTY"},
	{sentinel: ErrBadFileHandle, code: codes.FailedPrecondition, reason: "EBADF"},
	{sentinel: ErrNoSpace, code: codes.ResourceExhausted, reason: "ENOSPC"},
	{sentinel: ErrAgain, code: codes.ResourceExhausted, reason: "EAGAIN"},
	{sentinel: ErrDeadlock, code: codes.Aborted, reason: "EDEADLK"},
	{sentinel: ErrInterrupted, code: codes.Canceled, reason: "EINTR"},
	{sentinel: ErrNotSupported, code: codes.Unimplemented, reason: "EOPNOTSUPP"},
	{sentinel: ErrTooManyLinks, code: codes.ResourceExhausted, reason: "EMLINK"},
	{sentinel: ErrFileTooLarge, code: codes.InvalidArgument, reason: "EFBIG"},
	{sentinel: ErrIllegalSeek, code: codes.InvalidArgument, reason: "ESPIPE"},
	{sentinel: ErrIO, code: codes.Internal, reason: "EIO"},
	{sentinel: ErrAccess, code: codes.PermissionDenied, reason: "EACCES"},
}

// ToStatus 将领域层返回的 error 映射为携带 google.rpc.ErrorInfo 详情的 gRPC status.Status。
// 若 err == nil，直接返回 nil。
// 若 err 命中 17 个领域哨兵错误之一，映射为对应的 gRPC 状态码并在 ErrorInfo 中注入 POSIX errno reason 与 bowl.storage domain。
// 若 err 为未知错误，映射为 codes.Unknown。
func ToStatus(err error) *status.Status {
	if err == nil {
		return nil
	}

	for _, m := range mappings {
		if errors.Is(err, m.sentinel) {
			st := status.New(m.code, err.Error())
			stWithDetails, detailErr := st.WithDetails(&errdetails.ErrorInfo{
				Reason: m.reason,
				Domain: ErrorDomain,
			})
			if detailErr == nil {
				return stWithDetails
			}
			return st
		}
	}

	if s, ok := status.FromError(err); ok {
		return s
	}

	return status.New(codes.Unknown, err.Error())
}

// ToGRPCError 将领域层返回的 error 转换为携带 google.rpc.ErrorInfo 的 gRPC error。
// 若 err == nil，直接返回 nil。
func ToGRPCError(err error) error {
	if err == nil {
		return nil
	}
	return ToStatus(err).Err()
}
