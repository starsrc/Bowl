package control

import (
	"context"
	"errors"

	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/starsrc/bowl/pkg/rpcerr"
	"github.com/starsrc/bowl/services/storage/internal/db"
	"github.com/starsrc/bowl/services/storage/internal/store"
)

// errorMapping 记录领域哨兵错误与 gRPC 状态码及 POSIX errno reason 的契约映射。
type errorMapping struct {
	sentinel error
	code     codes.Code
	reason   string
}

var mappings = []errorMapping{
	{sentinel: store.ErrQuotaShrinkBelowUsed, code: codes.FailedPrecondition, reason: rpcerr.ReasonInvalidArgument},
	{sentinel: store.ErrStaleEpoch, code: codes.FailedPrecondition, reason: rpcerr.ReasonBusy},
	{sentinel: store.ErrInvalid, code: codes.InvalidArgument, reason: rpcerr.ReasonInvalidArgument},
	{sentinel: store.ErrInvalidKey, code: codes.InvalidArgument, reason: rpcerr.ReasonInvalidArgument},
	{sentinel: store.ErrInvalidValue, code: codes.InvalidArgument, reason: rpcerr.ReasonInvalidArgument},
	{sentinel: store.ErrNameTooLong, code: codes.InvalidArgument, reason: rpcerr.ReasonNameTooLong},
	{sentinel: store.ErrRange, code: codes.InvalidArgument, reason: rpcerr.ReasonRange},
	{sentinel: store.ErrNotFound, code: codes.NotFound, reason: rpcerr.ReasonNotFound},
	{sentinel: store.ErrNoData, code: codes.NotFound, reason: rpcerr.ReasonNoData},
	{sentinel: store.ErrExist, code: codes.AlreadyExists, reason: rpcerr.ReasonAlreadyExists},
	{sentinel: store.ErrPerm, code: codes.PermissionDenied, reason: rpcerr.ReasonPermissionDenied},
	{sentinel: store.ErrReadOnly, code: codes.FailedPrecondition, reason: rpcerr.ReasonReadOnly},
	{sentinel: store.ErrNotDir, code: codes.FailedPrecondition, reason: rpcerr.ReasonNotDir},
	{sentinel: store.ErrIsDir, code: codes.FailedPrecondition, reason: rpcerr.ReasonIsDir},
	{sentinel: store.ErrNotEmpty, code: codes.FailedPrecondition, reason: rpcerr.ReasonNotEmpty},
	{sentinel: store.ErrNoSpace, code: codes.ResourceExhausted, reason: rpcerr.ReasonNoSpace},
	{sentinel: store.ErrInodeExhausted, code: codes.ResourceExhausted, reason: rpcerr.ReasonNoSpace},
	{sentinel: store.ErrNotSupported, code: codes.Unimplemented, reason: rpcerr.ReasonNotSupported},
	{sentinel: store.ErrTooManyLinks, code: codes.ResourceExhausted, reason: rpcerr.ReasonTooManyLinks},
	{sentinel: store.ErrFileTooLarge, code: codes.InvalidArgument, reason: rpcerr.ReasonFileTooLarge},
	{sentinel: store.ErrIO, code: codes.Internal, reason: rpcerr.ReasonIO},
	{sentinel: store.ErrClosed, code: codes.Internal, reason: rpcerr.ReasonIO},
	{sentinel: ErrCrossLineage, code: codes.FailedPrecondition, reason: rpcerr.ReasonCrossDevice},
	{sentinel: ErrAmbiguousLCA, code: codes.FailedPrecondition, reason: rpcerr.ReasonInvalidArgument},
	{sentinel: ErrNoBaseCommit, code: codes.FailedPrecondition, reason: rpcerr.ReasonInvalidArgument},
	{sentinel: ErrConcurrentMerge, code: codes.Aborted, reason: rpcerr.ReasonBusy},
	{sentinel: ErrDiskMounted, code: codes.Aborted, reason: rpcerr.ReasonBusy},
	{sentinel: db.ErrStaleBaseCommit, code: codes.Aborted, reason: rpcerr.ReasonBusy},
}

// ToStatus 将领域层与存储层返回的 error 映射为携带 google.rpc.ErrorInfo 详情的 gRPC status.Status。
// 若 err == nil，直接返回 nil。
// 若 err 命中领域哨兵错误，映射为对应的 gRPC 状态码并在 ErrorInfo 中注入 POSIX errno reason 与 bowl.storage domain。
// 若 err 是已有 gRPC status，直接透传。
// 若 err 是 context.Canceled 或 context.DeadlineExceeded，映射为对应状态码且不携带 ErrorInfo。
// 若 err 为未知错误，映射为 codes.Unknown。
func ToStatus(err error) *status.Status {
	if err == nil {
		return nil
	}

	if conflictErr, ok := errors.AsType[*MergeConflictError](err); ok {
		st := status.New(codes.Aborted, conflictErr.Error())
		stWithDetails, detailErr := st.WithDetails(&errdetails.ErrorInfo{
			Reason: rpcerr.ReasonConflict,
			Domain: rpcerr.Domain,
		})
		if detailErr == nil {
			return stWithDetails
		}
		return st
	}

	for _, m := range mappings {
		if errors.Is(err, m.sentinel) {
			st := status.New(m.code, err.Error())
			stWithDetails, detailErr := st.WithDetails(&errdetails.ErrorInfo{
				Reason: m.reason,
				Domain: rpcerr.Domain,
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

	if errors.Is(err, context.Canceled) {
		return status.New(codes.Canceled, err.Error())
	}

	if errors.Is(err, context.DeadlineExceeded) {
		return status.New(codes.DeadlineExceeded, err.Error())
	}

	return status.New(codes.Unknown, err.Error())
}

// ToGRPCError 将领域层与存储层返回的 error 转换为携带 google.rpc.ErrorInfo 的 gRPC error。
// 若 err == nil，直接返回 nil。
func ToGRPCError(err error) error {
	if err == nil {
		return nil
	}
	return ToStatus(err).Err()
}
