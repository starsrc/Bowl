package testutil

import (
	"net/http"
	"os"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/genproto/googleapis/rpc/errdetails"

	"github.com/starsrc/Aether/pkg/serverx"
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

// ExtractErrorInfo 从 error 中提取 *errdetails.ErrorInfo 详情，
// 委托给 serverx.ExtractErrorInfo。
func ExtractErrorInfo(err error) *errdetails.ErrorInfo {
	return serverx.ExtractErrorInfo(err)
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
