package server

import (
	"context"
	"errors"

	"connectrpc.com/connect"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/status"

	"github.com/starsrc/pkg/serverx"

	"github.com/starsrc/bowl/services/storage/internal/control"
)

func statusToConnectError(st *status.Status) error {
	return serverx.StatusToConnectError(st)
}

func StatusToConnectError(st *status.Status) error {
	return serverx.StatusToConnectError(st)
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
	if conflictErr, ok := errors.AsType[*control.MergeConflictError](err); ok {
		connectErr := connect.NewError(connect.CodeAborted, conflictErr)
		if infoDetail, detailErr := connect.NewErrorDetail(&errdetails.ErrorInfo{
			Reason: "ECONFLICT",
			Domain: control.ErrorDomain,
		}); detailErr == nil {
			connectErr.AddDetail(infoDetail)
		}
		if reportDetail, detailErr := connect.NewErrorDetail(toProtoMergeConflictReport(conflictErr.LCACommitID, &conflictErr.Report)); detailErr == nil {
			connectErr.AddDetail(reportDetail)
		}
		return connectErr
	}
	return StatusToConnectError(control.ToStatus(err))
}

func ToConnectError(err error) error {
	return toConnectError(err)
}

func ExtractErrorInfo(err error) *errdetails.ErrorInfo {
	return serverx.ExtractErrorInfo(err)
}
