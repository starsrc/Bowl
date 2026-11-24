package testutil

import (
	"cmp"
	"errors"
	"net/http"
	"os"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/genproto/googleapis/rpc/errdetails"

	"github.com/starsrc/pkg/serverx"

	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
	"github.com/starsrc/bowl/gen/go/storage/v1/storagev1connect"
)

const (
	defaultStorageServiceAddr = "http://localhost:9000"
	expectedErrorDomain       = "bowl.storage"
	DefaultBranch             = "main"
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
	require.Error(t, err, "expected RPC error, got nil")

	code := connect.CodeOf(err)
	assert.Equal(t, wantCode, code, "unexpected RPC code: want %v, got %v (err: %v)", wantCode, code, err)

	info := ExtractErrorInfo(err)
	require.NotNil(t, info, "RPC error must carry *errdetails.ErrorInfo (err: %v)", err)
	assert.Equal(t, expectedErrorDomain, info.GetDomain(), "ErrorInfo domain must be %s", expectedErrorDomain)
	assert.Equal(t, wantReason, info.GetReason(), "ErrorInfo reason mismatch: want %s, got %s", wantReason, info.GetReason())
}

func RequireMergeConflictReport(t testing.TB, err error) *storagev1.MergeConflictReport {
	t.Helper()
	RequireConnectError(t, err, connect.CodeAborted, "ECONFLICT")

	connErr, ok := errors.AsType[*connect.Error](err)
	require.True(t, ok, "expected *connect.Error, got %T", err)

	for _, detail := range connErr.Details() {
		val, valErr := detail.Value()
		if valErr != nil {
			continue
		}
		if report, isReport := val.(*storagev1.MergeConflictReport); isReport {
			return report
		}
	}
	require.Fail(t, "MergeConflictReport detail not found on ECONFLICT error")
	return nil
}
