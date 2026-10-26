package testutil

import (
	"errors"
	"net/http"
	"os"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/status"

	"github.com/starsrc/bowl/gen/go/storage/v1/storagev1connect"
)

const (
	defaultStorageServiceAddr = "http://localhost:9000"
	expectedErrorDomain       = "bowl.storage"
)

// GetStorageServiceAddr 返回 storage-service 服务地址，默认值为 http://localhost:9000。
func GetStorageServiceAddr() string {
	if addr := os.Getenv("STORAGE_SERVICE_ADDR"); addr != "" {
		return addr
	}
	return defaultStorageServiceAddr
}

// NewStorageClient 创建指向 storage-service 地址的 ConnectRPC StorageService 客户端。
func NewStorageClient(opts ...connect.ClientOption) storagev1connect.StorageServiceClient {
	client := &http.Client{
		Timeout: 10 * time.Second,
	}
	return storagev1connect.NewStorageServiceClient(
		client,
		GetStorageServiceAddr(),
		opts...,
	)
}

// ExtractErrorInfo 从 error 中提取 *errdetails.ErrorInfo 详情。
// 支持 *connect.Error 与 gRPC *status.Status。若未包含 ErrorInfo 则返回 nil。
func ExtractErrorInfo(err error) *errdetails.ErrorInfo {
	if err == nil {
		return nil
	}

	var connErr *connect.Error
	if errors.As(err, &connErr) {
		for _, detail := range connErr.Details() {
			val, err := detail.Value()
			if err != nil {
				continue
			}
			if info, ok := val.(*errdetails.ErrorInfo); ok {
				return info
			}
		}
	}

	if st, ok := status.FromError(err); ok {
		for _, detail := range st.Details() {
			if info, ok := detail.(*errdetails.ErrorInfo); ok {
				return info
			}
		}
	}

	return nil
}

// RequireConnectError 断言返回的错误符合 ConnectRPC 错误码及 POSIX 细粒度 ErrorInfo 契约。
// 校验 connect.CodeOf(err) == wantCode，且 ErrorInfo.GetDomain() == "bowl.storage"、ErrorInfo.GetReason() == wantReason。
func RequireConnectError(t testing.TB, err error, wantCode connect.Code, wantReason string) {
	t.Helper()
	require.Error(t, err, "预期产生 RPC 错误，实际为 nil")

	code := connect.CodeOf(err)
	assert.Equal(t, wantCode, code, "RPC 错误码不匹配，want %v, got %v (err: %v)", wantCode, code, err)

	info := ExtractErrorInfo(err)
	require.NotNil(t, info, "RPC 错误详情中必须携带 *errdetails.ErrorInfo (err: %v)", err)
	assert.Equal(t, expectedErrorDomain, info.GetDomain(), "ErrorInfo Domain 必须为 %s", expectedErrorDomain)
	assert.Equal(t, wantReason, info.GetReason(), "ErrorInfo Reason 不匹配，want %s, got %s", wantReason, info.GetReason())
}
