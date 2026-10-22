package disk

import (
	"errors"

	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// 领域哨兵错误定义。
var (
	ErrInvalidArgument = errors.New("invalid argument")
	ErrNameTooLong     = errors.New("disk name too long")
	ErrNotFound        = errors.New("disk not found")
	ErrAlreadyExists   = errors.New("disk already exists")
	ErrInvalidShrink   = errors.New("cannot shrink disk capacity below current usage")
	ErrNoSpace         = errors.New("disk quota exceeded")
)

// ErrorDomain 定义 Disk 模块统一的 ErrorInfo 错误领域标识符。
const ErrorDomain = "bowl.disk"

// errorMapping 记录领域哨兵错误与 gRPC 状态码及 POSIX errno reason 的契约映射。
type errorMapping struct {
	sentinel error
	code     codes.Code
	reason   string
}

var mappings = []errorMapping{
	{sentinel: ErrInvalidArgument, code: codes.InvalidArgument, reason: "EINVAL"},
	{sentinel: ErrNameTooLong, code: codes.InvalidArgument, reason: "ENAMETOOLONG"},
	{sentinel: ErrNotFound, code: codes.NotFound, reason: "ENOENT"},
	{sentinel: ErrAlreadyExists, code: codes.AlreadyExists, reason: "EEXIST"},
	{sentinel: ErrInvalidShrink, code: codes.FailedPrecondition, reason: "EINVAL"},
	{sentinel: ErrNoSpace, code: codes.ResourceExhausted, reason: "ENOSPC"},
}

// ToStatus 将领域层返回的 error 映射为携带 google.rpc.ErrorInfo 详情的 gRPC status.Status。
// 若 err == nil，直接返回 nil。
// 若 err 命中 6 个领域哨兵错误之一，映射为对应的 gRPC 状态码并在 ErrorInfo 中注入 POSIX errno reason 与 bowl.disk domain。
// 若 err 自身已实现 gRPCStatus()，直接返回对应状态（如参数校验器返回的 gRPC 状态）。
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
