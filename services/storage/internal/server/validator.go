package server

import (
	"context"

	"buf.build/go/protovalidate"
	"connectrpc.com/connect"
	"google.golang.org/protobuf/proto"

	"github.com/starsrc/bowl/pkg/validator"
)

// NewValidationInterceptor 基于 protovalidate.Validator 创建专属一元请求校验拦截器。
// 在调用下游 Handler 前，若 req.Any() 为 proto.Message，调用 v.Validate(msg)；
// 若校验失败，通过 validator.ToStatus(err) 提取 gRPC InvalidArgument 状态与
// google.rpc.ErrorInfo{Reason, Domain: "bowl.storage"}，
// 并通过 statusToConnectError 转换为携带该 ErrorInfo Detail 的 *connect.Error 直接返回，
// 从而阻止非法请求进入下游 Handler 或底层 meta.Manager。
func NewValidationInterceptor(v protovalidate.Validator) connect.UnaryInterceptorFunc {
	return func(next connect.UnaryFunc) connect.UnaryFunc {
		return func(ctx context.Context, req connect.AnyRequest) (connect.AnyResponse, error) {
			if req != nil {
				if msg, ok := req.Any().(proto.Message); ok {
					if err := v.Validate(msg); err != nil {
						st := validator.ToStatus(err)
						return nil, statusToConnectError(st)
					}
				}
			}
			return next(ctx, req)
		}
	}
}
