// Package server 实现 StorageService 的 ConnectRPC 处理器、拦截器与错误转换。
package server

import (
	"context"
	"errors"

	"connectrpc.com/connect"

	"github.com/starsrc/pkg/serverx"

	"github.com/starsrc/bowl/services/storage/internal/control"
)

// toConnectError 将任意错误转换为对应的 *connect.Error。
// 若 err 为 nil 则返回 nil，上下文取消或超时保留原始错误链，
// 其余错误由 control.ToStatus 构造 ErrorInfo 并按需附加合并冲突报告。
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
	outErr := serverx.StatusToConnectError(control.ToStatus(err))
	if conflictErr, ok := errors.AsType[*control.MergeConflictError](err); ok {
		if connectErr, isConnectErr := errors.AsType[*connect.Error](outErr); isConnectErr {
			if reportDetail, detailErr := connect.NewErrorDetail(toProtoMergeConflictReport(conflictErr.LCACommitID, &conflictErr.Report)); detailErr == nil {
				connectErr.AddDetail(reportDetail)
			}
		}
	}
	return outErr
}
