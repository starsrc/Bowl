package server

import (
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/status"

	"github.com/starsrc/Aether/pkg/serverx"
	"github.com/starsrc/bowl/services/storage/internal/meta"
)

// statusToConnectError 将 gRPC *status.Status（含 google.rpc.ErrorInfo 等 Details）无损转换为 *connect.Error。
// 委托给 serverx.StatusToConnectError。
func statusToConnectError(st *status.Status) error {
	return serverx.StatusToConnectError(st)
}

// StatusToConnectError 是 statusToConnectError 的导出版本，供外部包或测试直接调用。
func StatusToConnectError(st *status.Status) error {
	return serverx.StatusToConnectError(st)
}

// toConnectError 将任意错误（领域哨兵错误、上下文取消/超时或未知内部错误）转换为对应的 *connect.Error。
// 若 err == nil 返回 nil；其余通过 meta.ToStatus(err) 获得 *status.Status 并委托给 StatusToConnectError。
func toConnectError(err error) error {
	if err == nil {
		return nil
	}
	return StatusToConnectError(meta.ToStatus(err))
}

// ToConnectError 是 toConnectError 的导出版本，供外部包或后续 RPC 实现调用。
func ToConnectError(err error) error {
	return toConnectError(err)
}

// ExtractErrorInfo 从错误中提取首个附带的 google.rpc.ErrorInfo 详情。
// 委托给 serverx.ExtractErrorInfo。
func ExtractErrorInfo(err error) *errdetails.ErrorInfo {
	return serverx.ExtractErrorInfo(err)
}
