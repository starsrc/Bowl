package server

import (
	"context"
	"errors"

	"buf.build/go/protovalidate"
	"connectrpc.com/connect"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/starsrc/bowl/services/storage/internal/engine"
	"github.com/starsrc/bowl/services/storage/internal/projection"
)

const ErrorDomain = "bowl.storage"

type errorMapping struct {
	sentinel error
	code     codes.Code
	reason   string
}

var errorMappings = []errorMapping{
	{sentinel: engine.ErrQuotaShrinkBelowUsed, code: codes.FailedPrecondition, reason: "EINVAL"},
	{sentinel: engine.ErrStaleEpoch, code: codes.FailedPrecondition, reason: "EBUSY"},
	{sentinel: engine.ErrDirtyWorkLayer, code: codes.FailedPrecondition, reason: "EDIRTY"},
	{sentinel: engine.ErrDefaultBranch, code: codes.FailedPrecondition, reason: "EPERM"},
	{sentinel: engine.ErrCommitInUse, code: codes.FailedPrecondition, reason: "EBUSY"},
	{sentinel: engine.ErrAmbiguousLCA, code: codes.FailedPrecondition, reason: "EINVAL"},
	{sentinel: engine.ErrDiskMounted, code: codes.Aborted, reason: "EBUSY"},
	{sentinel: engine.ErrNotFound, code: codes.NotFound, reason: "ENOENT"},
	{sentinel: projection.ErrNotFound, code: codes.NotFound, reason: "ENOENT"},
	{sentinel: engine.ErrNoData, code: codes.NotFound, reason: "ENODATA"},
	{sentinel: engine.ErrExist, code: codes.AlreadyExists, reason: "EEXIST"},
	{sentinel: engine.ErrPerm, code: codes.PermissionDenied, reason: "EPERM"},
	{sentinel: engine.ErrNotDir, code: codes.FailedPrecondition, reason: "ENOTDIR"},
	{sentinel: engine.ErrIsDir, code: codes.FailedPrecondition, reason: "EISDIR"},
	{sentinel: engine.ErrNotEmpty, code: codes.FailedPrecondition, reason: "ENOTEMPTY"},
	{sentinel: engine.ErrNoSpace, code: codes.ResourceExhausted, reason: "ENOSPC"},
	{sentinel: engine.ErrInodeExhausted, code: codes.ResourceExhausted, reason: "ENOSPC"},
	{sentinel: engine.ErrTooManyLinks, code: codes.ResourceExhausted, reason: "EMLINK"},
	{sentinel: engine.ErrFileTooLarge, code: codes.InvalidArgument, reason: "EFBIG"},
	{sentinel: engine.ErrNameTooLong, code: codes.InvalidArgument, reason: "ENAMETOOLONG"},
	{sentinel: engine.ErrNotSupported, code: codes.Unimplemented, reason: "EOPNOTSUPP"},
	{sentinel: engine.ErrInvalid, code: codes.InvalidArgument, reason: "EINVAL"},
	{sentinel: engine.ErrInvalidKey, code: codes.InvalidArgument, reason: "EINVAL"},
	{sentinel: engine.ErrInvalidValue, code: codes.InvalidArgument, reason: "EINVAL"},
	{sentinel: projection.ErrInvalidArgument, code: codes.InvalidArgument, reason: "EINVAL"},
	{sentinel: engine.ErrClosed, code: codes.Internal, reason: "EIO"},
	{sentinel: projection.ErrUnavailable, code: codes.Unavailable, reason: "EIO"},
}

func grpcCodeToConnectCode(c codes.Code) connect.Code {
	switch c {
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

func ToStatus(err error) *status.Status {
	if err == nil {
		return nil
	}

	if conflictErr, ok := errors.AsType[*engine.ErrMergeConflict](err); ok {
		st := status.New(codes.Aborted, conflictErr.Error())
		stWithDetails, detailErr := st.WithDetails(&errdetails.ErrorInfo{
			Reason: "ECONFLICT",
			Domain: ErrorDomain,
		})
		if detailErr == nil {
			return stWithDetails
		}
		return st
	}

	for _, m := range errorMappings {
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
	if storageValErr, ok := errors.AsType[*ValidationError](err); ok {
		return buildInvalidArgumentStatus(err.Error(), storageValErr.Reason(), storageValErr.Domain())
	}
	if protoValErr, ok := errors.AsType[*protovalidate.ValidationError](err); ok {
		reason, domain := translateValidation(protoValErr, ErrorDomain)
		return buildInvalidArgumentStatus(err.Error(), reason, domain)
	}
	if errors.Is(err, context.Canceled) {
		return status.New(codes.Canceled, err.Error())
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return status.New(codes.DeadlineExceeded, err.Error())
	}
	return status.New(codes.Unknown, err.Error())
}

func ToGRPCError(err error) error {
	if err == nil {
		return nil
	}
	return ToStatus(err).Err()
}

func statusToConnectError(st *status.Status) error {
	return StatusToConnectError(st)
}

func StatusToConnectError(st *status.Status) error {
	if st == nil || st.Code() == codes.OK {
		return nil
	}

	code := grpcCodeToConnectCode(st.Code())
	connectErr := connect.NewError(code, errors.New(st.Message()))

	if stProto := st.Proto(); stProto != nil {
		for _, anyDetail := range stProto.GetDetails() {
			if anyDetail == nil {
				continue
			}
			detail, err := connect.NewErrorDetail(anyDetail)
			if err != nil {
				continue
			}
			connectErr.AddDetail(detail)
		}
	}

	return connectErr
}

func toConnectError(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, context.Canceled) {
		return connect.NewError(connect.CodeCanceled, err)
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return connect.NewError(connect.CodeDeadlineExceeded, err)
	}
	if conflictErr, ok := errors.AsType[*engine.ErrMergeConflict](err); ok {
		connectErr := connect.NewError(connect.CodeAborted, conflictErr)
		if infoDetail, detailErr := connect.NewErrorDetail(&errdetails.ErrorInfo{
			Reason: "ECONFLICT",
			Domain: ErrorDomain,
		}); detailErr == nil {
			connectErr.AddDetail(infoDetail)
		}
		if reportDetail, detailErr := connect.NewErrorDetail(toProtoMergeConflictReport(&conflictErr.Report)); detailErr == nil {
			connectErr.AddDetail(reportDetail)
		}
		return connectErr
	}
	return StatusToConnectError(ToStatus(err))
}

func ToConnectError(err error) error {
	return toConnectError(err)
}

func extractErrorInfoFromStatus(st *status.Status) *errdetails.ErrorInfo {
	if st == nil || st.Code() == codes.OK {
		return nil
	}
	for _, detail := range st.Details() {
		if info, ok := detail.(*errdetails.ErrorInfo); ok {
			return info
		}
	}
	if stProto := st.Proto(); stProto != nil {
		for _, anyDetail := range stProto.GetDetails() {
			if anyDetail == nil {
				continue
			}
			msg, unmarshalErr := anyDetail.UnmarshalNew()
			if unmarshalErr != nil {
				continue
			}
			if info, ok := msg.(*errdetails.ErrorInfo); ok {
				return info
			}
		}
	}
	return nil
}

func ExtractErrorInfo(err error) *errdetails.ErrorInfo {
	if err == nil {
		return nil
	}

	if connectErr, ok := errors.AsType[*connect.Error](err); ok {
		for _, detail := range connectErr.Details() {
			msg, valErr := detail.Value()
			if valErr != nil {
				continue
			}
			if info, infoOK := msg.(*errdetails.ErrorInfo); infoOK {
				return info
			}
		}
	}

	for curr := err; curr != nil; curr = errors.Unwrap(curr) {
		if st, ok := status.FromError(curr); ok {
			if info := extractErrorInfoFromStatus(st); info != nil {
				return info
			}
		}
	}

	if se, ok := errors.AsType[interface {
		error
		GRPCStatus() *status.Status
	}](err); ok {
		if info := extractErrorInfoFromStatus(se.GRPCStatus()); info != nil {
			return info
		}
	}

	return nil
}
