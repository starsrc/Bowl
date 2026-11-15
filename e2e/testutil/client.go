// Package testutil 提供存储模块端到端测试的客户端构造与错误断言辅助实现。
package testutil

import (
	"cmp"
	"net/http"
	"os"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/starsrc/pkg/serverx"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/starsrc/bowl/gen/go/storage/v1/storagev1connect"
	"github.com/starsrc/bowl/pkg/rpcerr"
)

const defaultStorageServiceAddr = "http://localhost:9000"

// GetStorageServiceAddr 返回 `storage-service` 接入地址，默认值为 http://localhost:9000。
func GetStorageServiceAddr() string {
	return cmp.Or(os.Getenv("STORAGE_SERVICE_ADDR"), defaultStorageServiceAddr)
}

// GetStorageAdminAddr 返回 `storage-service` 内网管理接入地址，默认值为 http://127.0.0.1:9090。
func GetStorageAdminAddr() string {
	return cmp.Or(os.Getenv("STORAGE_ADMIN_ADDR"), "http://127.0.0.1:9090")
}

// NewStorageClient 创建指向默认 storage-service 地址的 ConnectRPC StorageService 客户端。
func NewStorageClient(opts ...connect.ClientOption) storagev1connect.StorageServiceClient {
	return NewStorageClientAt(GetStorageServiceAddr(), opts...)
}

// NewStorageClientAt 创建指向指定地址的 ConnectRPC StorageService 客户端。
func NewStorageClientAt(addr string, opts ...connect.ClientOption) storagev1connect.StorageServiceClient {
	client := &http.Client{
		Timeout: 15 * time.Second,
	}
	return storagev1connect.NewStorageServiceClient(client, addr, opts...)
}

// NewH2CStorageClient 创建启用明文 HTTP/2 (h2c) 与大流控窗口的 ConnectRPC 客户端。
func NewH2CStorageClient(addr string, opts ...connect.ClientOption) storagev1connect.StorageServiceClient {
	var protocols http.Protocols
	protocols.SetUnencryptedHTTP2(true)
	client := &http.Client{
		Timeout: 30 * time.Second,
		Transport: &http.Transport{
			Protocols: &protocols,
			HTTP2: &http.HTTP2Config{
				MaxReadFrameSize:              1 << 20,
				MaxReceiveBufferPerStream:     8 << 20,
				MaxReceiveBufferPerConnection: 32 << 20,
			},
		},
	}
	return storagev1connect.NewStorageServiceClient(client, addr, opts...)
}

// RequireConnectError 断言返回的错误符合 ConnectRPC 错误码及 POSIX 细粒度 ErrorInfo 契约。
// 校验 connect.CodeOf(err) == wantCode，且 ErrorInfo.GetDomain() == rpcerr.Domain、ErrorInfo.GetReason() == wantReason。
func RequireConnectError(t testing.TB, err error, wantCode connect.Code, wantReason string) {
	t.Helper()
	require.Error(t, err, "预期产生 RPC 错误，实际为 nil")

	code := connect.CodeOf(err)
	assert.Equal(t, wantCode, code, "RPC 错误码不匹配，want %v, got %v (err: %v)", wantCode, code, err)

	info := serverx.ExtractErrorInfo(err)
	require.NotNil(t, info, "RPC 错误详情中必须携带 *errdetails.ErrorInfo (err: %v)", err)
	assert.Equal(t, rpcerr.Domain, info.GetDomain(), "ErrorInfo Domain 必须为 %s", rpcerr.Domain)
	assert.Equal(t, wantReason, info.GetReason(), "ErrorInfo Reason 不匹配，want %s, got %s", wantReason, info.GetReason())
}
