package rpc

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"sync"
	"testing"

	"connectrpc.com/connect"
	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
	"github.com/starsrc/bowl/gen/go/storage/v1/storagev1connect"
	"github.com/stretchr/testify/require"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"
)

func TestResolveEndpoint_Fallback(t *testing.T) {
	t.Run("explicit endpoint takes precedence", func(t *testing.T) {
		t.Setenv(EnvStorageEndpoint, "http://env-endpoint:9000")
		ep, err := ResolveEndpoint("http://explicit-endpoint:9000")
		require.NoError(t, err)
		require.Equal(t, "http://explicit-endpoint:9000", ep)
	})

	t.Run("fallback to environment variable", func(t *testing.T) {
		t.Setenv(EnvStorageEndpoint, "http://env-endpoint:9000")
		ep, err := ResolveEndpoint("")
		require.NoError(t, err)
		require.Equal(t, "http://env-endpoint:9000", ep)
	})

	t.Run("fallback to default endpoint", func(t *testing.T) {
		_ = os.Unsetenv(EnvStorageEndpoint)
		ep, err := ResolveEndpoint("")
		require.NoError(t, err)
		require.Equal(t, DefaultEndpoint, ep)
		require.Equal(t, "http://127.0.0.1:9000", ep)
	})
}

func TestResolveEndpoint_Validation(t *testing.T) {
	testCases := []struct {
		name     string
		endpoint string
		wantErr  bool
	}{
		{
			name:     "valid http",
			endpoint: "http://127.0.0.1:9000",
			wantErr:  false,
		},
		{
			name:     "valid https",
			endpoint: "https://storage.bowl.internal:443",
			wantErr:  false,
		},
		{
			name:     "invalid scheme ftp",
			endpoint: "ftp://127.0.0.1:9000",
			wantErr:  true,
		},
		{
			name:     "invalid scheme grpc",
			endpoint: "grpc://127.0.0.1:9000",
			wantErr:  true,
		},
		{
			name:     "missing scheme",
			endpoint: "127.0.0.1:9000",
			wantErr:  true,
		},
		{
			name:     "missing host",
			endpoint: "http://",
			wantErr:  true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ep, err := ResolveEndpoint(tc.endpoint)
			if tc.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.NotEmpty(t, ep)
			}
		})
	}
}

func TestResolveToken_Fallback(t *testing.T) {
	t.Run("explicit token takes precedence", func(t *testing.T) {
		t.Setenv(EnvToken, "env-token-123")
		tok := ResolveToken("explicit-token-456")
		require.Equal(t, "explicit-token-456", tok)
	})

	t.Run("fallback to environment variable", func(t *testing.T) {
		t.Setenv(EnvToken, "env-token-123")
		tok := ResolveToken("")
		require.Equal(t, "env-token-123", tok)
	})

	t.Run("empty when neither set", func(t *testing.T) {
		_ = os.Unsetenv(EnvToken)
		tok := ResolveToken("")
		require.Empty(t, tok)
	})
}

// testStorageHandler 记录测试请求的元信息并返回虚构数据。
type testStorageHandler struct {
	storagev1connect.UnimplementedStorageServiceHandler

	mu            sync.Mutex
	lastProto     string
	lastProtoMaj  int
	lastAuthToken string
	callCount     int
}

func (h *testStorageHandler) GetDisk(ctx context.Context, req *connect.Request[storagev1.GetDiskRequest]) (*connect.Response[storagev1.GetDiskResponse], error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.callCount++
	h.lastAuthToken = req.Header().Get("Authorization")

	return connect.NewResponse(&storagev1.GetDiskResponse{
		Disk: &storagev1.Disk{
			DiskId:   req.Msg.GetDiskId(),
			Name:     "test-disk",
			RootIno:  1001,
			MaxBytes: 1024 * 1024 * 1024,
		},
	}), nil
}

func TestNewClient_H2C_Transport(t *testing.T) {
	handler := &testStorageHandler{}
	mux := http.NewServeMux()
	path, srvHandler := storagev1connect.NewStorageServiceHandler(handler)

	var interceptedProto string
	var interceptedProtoMaj int
	wrappedHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		interceptedProto = r.Proto
		interceptedProtoMaj = r.ProtoMajor
		srvHandler.ServeHTTP(w, r)
	})
	mux.Handle(path, wrappedHandler)

	// 启用明文 HTTP/2 (h2c)
	h2cServer := &http2.Server{}
	server := httptest.NewServer(h2c.NewHandler(mux, h2cServer))
	defer server.Close()

	client, err := NewClient(server.URL, "")
	require.NoError(t, err)
	require.NotNil(t, client)

	resp, err := client.GetDisk(context.Background(), connect.NewRequest(&storagev1.GetDiskRequest{
		DiskId: "00000000-0000-0000-0000-000000000001",
	}))
	require.NoError(t, err)
	require.Equal(t, uint64(1001), resp.Msg.GetDisk().GetRootIno())

	// 断言确实使用了 HTTP/2 明文传输协议
	require.Equal(t, 2, interceptedProtoMaj, "client request must use HTTP/2")
	require.Equal(t, "HTTP/2.0", interceptedProto)
}

func TestNewClient_BearerToken(t *testing.T) {
	t.Run("with token injected", func(t *testing.T) {
		handler := &testStorageHandler{}
		mux := http.NewServeMux()
		path, srvHandler := storagev1connect.NewStorageServiceHandler(handler)
		mux.Handle(path, srvHandler)

		server := httptest.NewServer(h2c.NewHandler(mux, &http2.Server{}))
		defer server.Close()

		client, err := NewClient(server.URL, "secret-token-xyz")
		require.NoError(t, err)

		_, err = client.GetDisk(context.Background(), connect.NewRequest(&storagev1.GetDiskRequest{
			DiskId: "00000000-0000-0000-0000-000000000001",
		}))
		require.NoError(t, err)

		handler.mu.Lock()
		defer handler.mu.Unlock()
		require.Equal(t, "Bearer secret-token-xyz", handler.lastAuthToken)
	})

	t.Run("without token - no authorization header", func(t *testing.T) {
		handler := &testStorageHandler{}
		mux := http.NewServeMux()
		path, srvHandler := storagev1connect.NewStorageServiceHandler(handler)
		mux.Handle(path, srvHandler)

		server := httptest.NewServer(h2c.NewHandler(mux, &http2.Server{}))
		defer server.Close()

		client, err := NewClient(server.URL, "")
		require.NoError(t, err)

		_, err = client.GetDisk(context.Background(), connect.NewRequest(&storagev1.GetDiskRequest{
			DiskId: "00000000-0000-0000-0000-000000000001",
		}))
		require.NoError(t, err)

		handler.mu.Lock()
		defer handler.mu.Unlock()
		require.Empty(t, handler.lastAuthToken, "Authorization header must not be set when token is empty")
	})
}

func TestNewClient_TLS(t *testing.T) {
	handler := &testStorageHandler{}
	mux := http.NewServeMux()
	path, srvHandler := storagev1connect.NewStorageServiceHandler(handler)
	mux.Handle(path, srvHandler)

	server := httptest.NewTLSServer(mux)
	defer server.Close()

	// 传入测试服务器的 TLSClientConfig 信任本地测试自签名证书
	tlsConfig := server.Client().Transport.(*http.Transport).TLSClientConfig
	client, err := NewClientFromConfig(ClientConfig{
		Endpoint:  server.URL,
		Token:     "tls-token",
		TLSConfig: tlsConfig,
	})
	require.NoError(t, err)

	resp, err := client.GetDisk(context.Background(), connect.NewRequest(&storagev1.GetDiskRequest{
		DiskId: "00000000-0000-0000-0000-000000000002",
	}))
	require.NoError(t, err)
	require.Equal(t, "00000000-0000-0000-0000-000000000002", resp.Msg.GetDisk().GetDiskId())

	handler.mu.Lock()
	defer handler.mu.Unlock()
	require.Equal(t, "Bearer tls-token", handler.lastAuthToken)
}

func TestNewClient_ConnectOptions(t *testing.T) {
	handler := &testStorageHandler{}
	mux := http.NewServeMux()
	path, srvHandler := storagev1connect.NewStorageServiceHandler(handler)
	mux.Handle(path, srvHandler)

	server := httptest.NewServer(h2c.NewHandler(mux, &http2.Server{}))
	defer server.Close()

	var customHeaderObserved bool
	customInterceptor := connect.UnaryInterceptorFunc(func(next connect.UnaryFunc) connect.UnaryFunc {
		return func(ctx context.Context, req connect.AnyRequest) (connect.AnyResponse, error) {
			req.Header().Set("X-Custom-Client", "bowl-test")
			customHeaderObserved = true
			return next(ctx, req)
		}
	})

	client, err := NewClient(server.URL, "", connect.WithInterceptors(customInterceptor))
	require.NoError(t, err)

	_, err = client.GetDisk(context.Background(), connect.NewRequest(&storagev1.GetDiskRequest{
		DiskId: "00000000-0000-0000-0000-000000000003",
	}))
	require.NoError(t, err)
	require.True(t, customHeaderObserved, "custom connect interceptor should be executed")
}
