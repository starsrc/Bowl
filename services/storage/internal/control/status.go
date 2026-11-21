package control

import (
	"context"
	"errors"

	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/starsrc/bowl/services/storage/internal/db"
	"github.com/starsrc/bowl/services/storage/internal/store"
)

const ErrorDomain = "bowl.storage"

type errorMapping struct {
	sentinel error
	code     codes.Code
	reason   string
}

var mappings = []errorMapping{
	{sentinel: store.ErrQuotaShrinkBelowUsed, code: codes.FailedPrecondition, reason: "EINVAL"},
	{sentinel: store.ErrStaleEpoch, code: codes.FailedPrecondition, reason: "EBUSY"},
	{sentinel: store.ErrInvalid, code: codes.InvalidArgument, reason: "EINVAL"},
	{sentinel: store.ErrInvalidKey, code: codes.InvalidArgument, reason: "EINVAL"},
	{sentinel: store.ErrInvalidValue, code: codes.InvalidArgument, reason: "EINVAL"},
	{sentinel: store.ErrNameTooLong, code: codes.InvalidArgument, reason: "ENAMETOOLONG"},
	{sentinel: store.ErrRange, code: codes.InvalidArgument, reason: "ERANGE"},
	{sentinel: store.ErrNotFound, code: codes.NotFound, reason: "ENOENT"},
	{sentinel: store.ErrNoData, code: codes.NotFound, reason: "ENODATA"},
	{sentinel: store.ErrExist, code: codes.AlreadyExists, reason: "EEXIST"},
	{sentinel: store.ErrPerm, code: codes.PermissionDenied, reason: "EPERM"},
	{sentinel: store.ErrReadOnly, code: codes.PermissionDenied, reason: "EPERM"},
	{sentinel: store.ErrNotDir, code: codes.FailedPrecondition, reason: "ENOTDIR"},
	{sentinel: store.ErrIsDir, code: codes.FailedPrecondition, reason: "EISDIR"},
	{sentinel: store.ErrNotEmpty, code: codes.FailedPrecondition, reason: "ENOTEMPTY"},
	{sentinel: store.ErrNoSpace, code: codes.ResourceExhausted, reason: "ENOSPC"},
	{sentinel: store.ErrInodeExhausted, code: codes.ResourceExhausted, reason: "ENOSPC"},
	{sentinel: store.ErrNotSupported, code: codes.Unimplemented, reason: "EOPNOTSUPP"},
	{sentinel: store.ErrTooManyLinks, code: codes.ResourceExhausted, reason: "EMLINK"},
	{sentinel: store.ErrFileTooLarge, code: codes.InvalidArgument, reason: "EFBIG"},
	{sentinel: store.ErrIO, code: codes.Internal, reason: "EIO"},
	{sentinel: store.ErrClosed, code: codes.Internal, reason: "EIO"},
	{sentinel: ErrCrossLineage, code: codes.FailedPrecondition, reason: "EXDEV"},
	{sentinel: ErrAmbiguousLCA, code: codes.FailedPrecondition, reason: "EINVAL"},
	{sentinel: ErrNoBaseSnapshot, code: codes.FailedPrecondition, reason: "EINVAL"},
	{sentinel: ErrConcurrentMerge, code: codes.Aborted, reason: "EBUSY"},
	{sentinel: ErrDiskMounted, code: codes.Aborted, reason: "EBUSY"},
	{sentinel: ErrSnapshotInUse, code: codes.FailedPrecondition, reason: "EBUSY"},
	{sentinel: db.ErrStaleBaseSnapshot, code: codes.Aborted, reason: "EBUSY"},
}

func ToStatus(err error) *status.Status {
	if err == nil {
		return nil
	}

	if conflictErr, ok := errors.AsType[*MergeConflictError](err); ok {
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
