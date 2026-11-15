package server

import (
	"context"
	"math"

	"buf.build/go/protovalidate"
	"connectrpc.com/connect"
	"google.golang.org/protobuf/proto"

	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
	"github.com/starsrc/bowl/pkg/validator"
)

func isValidNonNilUUID(s string) bool {
	if len(s) != 36 {
		return false
	}
	if s[8] != '-' || s[13] != '-' || s[18] != '-' || s[23] != '-' {
		return false
	}
	nonZero := false
	for i := range 36 {
		if i == 8 || i == 13 || i == 18 || i == 23 {
			continue
		}
		c := s[i]
		switch {
		case c == '0':
		case (c >= '1' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F'):
			nonZero = true
		default:
			return false
		}
	}
	return nonZero
}

func isValidFastPathDataReq(anyMsg any) bool {
	switch m := anyMsg.(type) {
	case *storagev1.WriteRequest:
		if m == nil {
			return false
		}
		dataLen := uint64(len(m.Data))
		return m.Ino > 0 && m.Ino <= math.MaxInt64 &&
			m.Fh <= math.MaxInt64 &&
			m.Offset <= math.MaxInt64 &&
			dataLen <= maxReadWriteSize &&
			m.Offset <= uint64(math.MaxInt64)-dataLen &&
			m.Epoch > 0 && m.Epoch <= math.MaxInt64 &&
			isValidNonNilUUID(m.DiskId)
	case *storagev1.ReadRequest:
		if m == nil {
			return false
		}
		return m.Ino > 0 && m.Ino <= math.MaxInt64 &&
			m.Fh <= math.MaxInt64 &&
			m.Offset <= math.MaxInt64 &&
			m.Size <= maxReadWriteSize &&
			isValidNonNilUUID(m.DiskId)
	default:
		return false
	}
}

// NewValidationInterceptor 基于 protovalidate.Validator 创建专属一元请求校验拦截器。
// 在调用下游 Handler 前，若 req.Any() 为 proto.Message，调用 v.Validate(msg)；
// 若校验失败，通过 validator.ToStatus(err) 提取 gRPC InvalidArgument 状态与
// google.rpc.ErrorInfo{Reason, Domain: "bowl.storage"}，
// 并通过 statusToConnectError 转换为携带该 ErrorInfo Detail 的 *connect.Error 直接返回，
// 从而阻止非法请求进入下游 Handler 或底层存储引擎。
func NewValidationInterceptor(v protovalidate.Validator) connect.UnaryInterceptorFunc {
	return func(next connect.UnaryFunc) connect.UnaryFunc {
		return func(ctx context.Context, req connect.AnyRequest) (connect.AnyResponse, error) {
			if req != nil {
				anyMsg := req.Any()
				if !isValidFastPathDataReq(anyMsg) {
					if msg, ok := anyMsg.(proto.Message); ok {
						if err := v.Validate(msg); err != nil {
							st := validator.ToStatus(err)
							return nil, statusToConnectError(st)
						}
					}
				}
			}
			return next(ctx, req)
		}
	}
}
