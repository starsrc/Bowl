package testutil

import (
	"cmp"
	"net/http"
	"os"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/genproto/googleapis/rpc/errdetails"

	"github.com/starsrc/pkg/serverx"

	"github.com/starsrc/bowl/gen/go/storage/v1/storagev1connect"
)

const (
	defaultStorageServiceAddr = "http://localhost:9000"
	expectedErrorDomain       = "bowl.storage"
)

func GetStorageServiceAddr() string {
	return cmp.Or(os.Getenv("STORAGE_SERVICE_ADDR"), defaultStorageServiceAddr)
}

func GetStorageAdminAddr() string {
	return cmp.Or(os.Getenv("STORAGE_ADMIN_ADDR"), "http://127.0.0.1:9090")
}

func NewStorageClient(opts ...connect.ClientOption) storagev1connect.StorageServiceClient {
	return NewStorageClientAt(GetStorageServiceAddr(), opts...)
}

func NewStorageClientAt(addr string, opts ...connect.ClientOption) storagev1connect.StorageServiceClient {
	client := &http.Client{
		Timeout: 15 * time.Second,
	}
	return storagev1connect.NewStorageServiceClient(client, addr, opts...)
}

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

func ExtractErrorInfo(err error) *errdetails.ErrorInfo {
	return serverx.ExtractErrorInfo(err)
}

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
