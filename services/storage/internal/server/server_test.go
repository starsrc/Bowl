package server_test

import (
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.uber.org/fx"
	"go.uber.org/fx/fxtest"
	"golang.org/x/net/http2"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
	"github.com/starsrc/bowl/gen/go/storage/v1/storagev1connect"
	"github.com/starsrc/bowl/pkg/validator"
	"github.com/starsrc/bowl/services/storage/internal/config"
	"github.com/starsrc/bowl/services/storage/internal/meta"
	metamock "github.com/starsrc/bowl/services/storage/internal/meta/mock"
	"github.com/starsrc/bowl/services/storage/internal/server"
)

type fakeShutdowner struct {
	mu             sync.Mutex
	shutdownCalled atomic.Bool
	shutdownOpts   []fx.ShutdownOption
	shutdownErr    error
}

func (f *fakeShutdowner) Shutdown(opts ...fx.ShutdownOption) error {
	f.shutdownCalled.Store(true)
	f.mu.Lock()
	f.shutdownOpts = append(f.shutdownOpts, opts...)
	f.mu.Unlock()
	return f.shutdownErr
}

func (f *fakeShutdowner) Options() []fx.ShutdownOption {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]fx.ShutdownOption(nil), f.shutdownOpts...)
}

var _ fx.Shutdowner = (*fakeShutdowner)(nil)

func allocateFreePort(t *testing.T) int {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	port := ln.Addr().(*net.TCPAddr).Port
	require.NoError(t, ln.Close())
	return port
}

func assertPortListening(t *testing.T, addr string) {
	t.Helper()
	require.Eventually(t, func() bool {
		conn, err := net.DialTimeout("tcp", addr, 50*time.Millisecond)
		if err == nil {
			_ = conn.Close()
			return true
		}
		return false
	}, 2*time.Second, 10*time.Millisecond, "端口未就绪: %s", addr)
}

func assertPortClosed(t *testing.T, addr string) {
	t.Helper()
	require.Eventually(t, func() bool {
		conn, err := net.DialTimeout("tcp", addr, 50*time.Millisecond)
		if err != nil {
			return true
		}
		_ = conn.Close()
		return false
	}, 2*time.Second, 10*time.Millisecond, "端口未关闭: %s", addr)
}

type logCapture struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (c *logCapture) Write(p []byte) (n int, err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.buf.Write(p)
}

func (c *logCapture) String() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.buf.String()
}

func (c *logCapture) Reset() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.buf.Reset()
}

func testLogger(capture *logCapture) *slog.Logger {
	handler := slog.NewJSONHandler(capture, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	})
	return slog.New(handler)
}

func defaultTestConfig() config.ServerConfig {
	return config.ServerConfig{
		ServerHost:              "127.0.0.1",
		ServerPort:              0,
		ServerReadHeaderTimeout: 5 * time.Second,
		ServerStopTimeout:       5 * time.Second,
		ServerReadMaxBytes:      config.DefaultServerReadMaxBytes,
	}
}

type protoCapturingRoundTripper struct {
	base        http.RoundTripper
	protoHolder *atomic.Pointer[string]
}

func (p *protoCapturingRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	resp, err := p.base.RoundTrip(req)
	if err == nil && resp != nil && p.protoHolder != nil {
		protoStr := resp.Proto
		p.protoHolder.Store(&protoStr)
	}
	return resp, err
}

func newH2CTestClient() (*http.Client, *atomic.Pointer[string]) {
	protoHolder := &atomic.Pointer[string]{}
	h2cTransport := &http2.Transport{
		AllowHTTP: true,
		DialTLSContext: func(ctx context.Context, network, addr string, _ *tls.Config) (net.Conn, error) {
			var d net.Dialer
			return d.DialContext(ctx, network, addr)
		},
	}
	client := &http.Client{
		Timeout: 5 * time.Second,
		Transport: &protoCapturingRoundTripper{
			base:        h2cTransport,
			protoHolder: protoHolder,
		},
	}
	return client, protoHolder
}

// 1. 验证 toConnectError 对 24 个领域哨兵、包装错误以及纯 context.Canceled / DeadlineExceeded 的转换准确性。
func TestToConnectError_AllDomainSentinelsAndContext(t *testing.T) {
	tests := []struct {
		name           string
		err            error
		expectedCode   connect.Code
		expectedReason string
	}{
		{"ErrInvalidShrink", meta.ErrInvalidShrink, connect.CodeFailedPrecondition, "EINVAL"},
		{"ErrInvalidArgument", meta.ErrInvalidArgument, connect.CodeInvalidArgument, "EINVAL"},
		{"ErrNameTooLong", meta.ErrNameTooLong, connect.CodeInvalidArgument, "ENAMETOOLONG"},
		{"ErrRange", meta.ErrRange, connect.CodeInvalidArgument, "ERANGE"},
		{"ErrTooBig", meta.ErrTooBig, connect.CodeInvalidArgument, "E2BIG"},
		{"ErrNotFound", meta.ErrNotFound, connect.CodeNotFound, "ENOENT"},
		{"ErrNoData", meta.ErrNoData, connect.CodeNotFound, "ENODATA"},
		{"ErrAlreadyExists", meta.ErrAlreadyExists, connect.CodeAlreadyExists, "EEXIST"},
		{"ErrPerm", meta.ErrPerm, connect.CodePermissionDenied, "EPERM"},
		{"ErrNotDir", meta.ErrNotDir, connect.CodeFailedPrecondition, "ENOTDIR"},
		{"ErrIsDir", meta.ErrIsDir, connect.CodeFailedPrecondition, "EISDIR"},
		{"ErrNotEmpty", meta.ErrNotEmpty, connect.CodeFailedPrecondition, "ENOTEMPTY"},
		{"ErrBadFileHandle", meta.ErrBadFileHandle, connect.CodeFailedPrecondition, "EBADF"},
		{"ErrNoSpace", meta.ErrNoSpace, connect.CodeResourceExhausted, "ENOSPC"},
		{"ErrAgain", meta.ErrAgain, connect.CodeResourceExhausted, "EAGAIN"},
		{"ErrDeadlock", meta.ErrDeadlock, connect.CodeAborted, "EDEADLK"},
		{"ErrInterrupted", meta.ErrInterrupted, connect.CodeCanceled, "EINTR"},
		{"ErrNotSupported", meta.ErrNotSupported, connect.CodeUnimplemented, "EOPNOTSUPP"},
		{"ErrTooManyLinks", meta.ErrTooManyLinks, connect.CodeResourceExhausted, "EMLINK"},
		{"ErrFileTooLarge", meta.ErrFileTooLarge, connect.CodeInvalidArgument, "EFBIG"},
		{"ErrIllegalSeek", meta.ErrIllegalSeek, connect.CodeInvalidArgument, "ESPIPE"},
		{"ErrIO", meta.ErrIO, connect.CodeInternal, "EIO"},
		{"ErrAccess", meta.ErrAccess, connect.CodePermissionDenied, "EACCES"},
		{"ErrCrossDevice", meta.ErrCrossDevice, connect.CodeFailedPrecondition, "EXDEV"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			res := server.ToConnectError(tt.err)
			require.Error(t, res)
			assert.Equal(t, tt.expectedCode, connect.CodeOf(res))

			info := server.ExtractErrorInfo(res)
			require.NotNil(t, info, "必须无损挂载 google.rpc.ErrorInfo")
			assert.Equal(t, tt.expectedReason, info.GetReason())
			assert.Equal(t, "bowl.storage", info.GetDomain())

			// 验证包含上下文包装的错误同样能正确提取
			wrappedErr := fmt.Errorf("context trace: %w", tt.err)
			resWrapped := server.ToConnectError(wrappedErr)
			require.Error(t, resWrapped)
			assert.Equal(t, tt.expectedCode, connect.CodeOf(resWrapped))
			infoWrapped := server.ExtractErrorInfo(resWrapped)
			require.NotNil(t, infoWrapped)
			assert.Equal(t, tt.expectedReason, infoWrapped.GetReason())
			assert.Equal(t, "bowl.storage", infoWrapped.GetDomain())
		})
	}

	t.Run("nil 返回 nil", func(t *testing.T) {
		assert.NoError(t, server.ToConnectError(nil))
	})

	t.Run("纯 context.Canceled", func(t *testing.T) {
		err := server.ToConnectError(context.Canceled)
		require.Error(t, err)
		assert.Equal(t, connect.CodeCanceled, connect.CodeOf(err))
		assert.Nil(t, server.ExtractErrorInfo(err), "纯 context.Canceled 不附带领域 ErrorInfo")
	})

	t.Run("纯 context.DeadlineExceeded", func(t *testing.T) {
		err := server.ToConnectError(context.DeadlineExceeded)
		require.Error(t, err)
		assert.Equal(t, connect.CodeDeadlineExceeded, connect.CodeOf(err))
		assert.Nil(t, server.ExtractErrorInfo(err), "纯 context.DeadlineExceeded 不附带领域 ErrorInfo")
	})

	t.Run("StatusToConnectError 所有 gRPC 状态码映射", func(t *testing.T) {
		allCodes := []struct {
			grpcCode    codes.Code
			connectCode connect.Code
		}{
			{codes.Canceled, connect.CodeCanceled},
			{codes.Unknown, connect.CodeUnknown},
			{codes.InvalidArgument, connect.CodeInvalidArgument},
			{codes.DeadlineExceeded, connect.CodeDeadlineExceeded},
			{codes.NotFound, connect.CodeNotFound},
			{codes.AlreadyExists, connect.CodeAlreadyExists},
			{codes.PermissionDenied, connect.CodePermissionDenied},
			{codes.ResourceExhausted, connect.CodeResourceExhausted},
			{codes.FailedPrecondition, connect.CodeFailedPrecondition},
			{codes.Aborted, connect.CodeAborted},
			{codes.OutOfRange, connect.CodeOutOfRange},
			{codes.Unimplemented, connect.CodeUnimplemented},
			{codes.Internal, connect.CodeInternal},
			{codes.Unavailable, connect.CodeUnavailable},
			{codes.DataLoss, connect.CodeDataLoss},
			{codes.Unauthenticated, connect.CodeUnauthenticated},
		}

		for _, item := range allCodes {
			st := status.New(item.grpcCode, "test error")
			connErr := server.StatusToConnectError(st)
			require.Error(t, connErr)
			assert.Equal(t, item.connectCode, connect.CodeOf(connErr))
		}

		assert.NoError(t, server.StatusToConnectError(nil))
		assert.NoError(t, server.StatusToConnectError(status.New(codes.OK, "ok")))
	})
}

// 2. 验证校验拦截器对非法请求直接拦截并返回 connect.CodeInvalidArgument 及 ErrorInfo{Reason, Domain}，不调用底层 meta.Manager。
func TestValidationInterceptor_RejectsInvalidRequests(t *testing.T) {
	mockMgr := metamock.NewMockManager(t) // 零 Expectation，任何被测调用将导致直接失败

	logCap := &logCapture{}
	logger := testLogger(logCap)

	srv, err := server.New(server.Params{
		Config:      defaultTestConfig(),
		MetaManager: mockMgr,
		Logger:      logger,
	})
	require.NoError(t, err)

	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	client := storagev1connect.NewStorageServiceClient(http.DefaultClient, ts.URL)

	ctx := context.Background()

	t.Run("parent_ino = 0 违规拦截 (EINVAL)", func(t *testing.T) {
		req := connect.NewRequest(&storagev1.LookupRequest{
			ParentIno: 0, // 违反 >= 1
			Name:      []byte("valid-name"),
		})
		resp, callErr := client.Lookup(ctx, req)
		require.Error(t, callErr)
		assert.Nil(t, resp)

		assert.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(callErr))
		info := server.ExtractErrorInfo(callErr)
		require.NotNil(t, info, "必须返回 ErrorInfo")
		assert.Equal(t, validator.ReasonInvalidArgument, info.GetReason())
		assert.Equal(t, validator.ErrorDomain, info.GetDomain())
	})

	t.Run("超长文件名违规拦截 (ENAMETOOLONG)", func(t *testing.T) {
		req := connect.NewRequest(&storagev1.LookupRequest{
			ParentIno: 1,
			Name:      bytes.Repeat([]byte("a"), 256), // > 255
		})
		resp, callErr := client.Lookup(ctx, req)
		require.Error(t, callErr)
		assert.Nil(t, resp)

		assert.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(callErr))
		info := server.ExtractErrorInfo(callErr)
		require.NotNil(t, info)
		assert.Equal(t, validator.ReasonNameTooLong, info.GetReason())
		assert.Equal(t, validator.ErrorDomain, info.GetDomain())
	})

	t.Run("超长 Xattr 值违规拦截 (ERANGE)", func(t *testing.T) {
		req := connect.NewRequest(&storagev1.SetXattrRequest{
			Ino:   1,
			Name:  []byte("user.test"),
			Value: bytes.Repeat([]byte("x"), 65537), // > 65536
		})
		resp, callErr := client.SetXattr(ctx, req)
		require.Error(t, callErr)
		assert.Nil(t, resp)

		assert.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(callErr))
		info := server.ExtractErrorInfo(callErr)
		require.NotNil(t, info)
		assert.Equal(t, validator.ReasonRange, info.GetReason())
		assert.Equal(t, validator.ErrorDomain, info.GetDomain())
	})

	// 确认底层 mock 确实未发生任何调用
	mockMgr.AssertExpectations(t)
}

// 3. 验证未知内部错误与包装错误经 MaskInterceptor 脱敏为 "internal server error"，同时完整保留 ErrorInfo Detail。
func TestMaskInterceptor_MasksInternalAndRetainsErrorInfo(t *testing.T) {
	mockMgr := metamock.NewMockManager(t)
	logCap := &logCapture{}
	logger := testLogger(logCap)

	srv, err := server.New(server.Params{
		Config:      defaultTestConfig(),
		MetaManager: mockMgr,
		Logger:      logger,
	})
	require.NoError(t, err)

	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	client := storagev1connect.NewStorageServiceClient(http.DefaultClient, ts.URL)
	ctx := context.Background()

	t.Run("原生数据库异常脱敏为 internal server error", func(t *testing.T) {
		logCap.Reset()
		rawDBErr := errors.New("pq: syntax error at or near \"SELECT\" (SQLSTATE 42601)")
		mockMgr.EXPECT().Lookup(mock.Anything, uint64(1), []byte("db_err.txt")).
			Return(nil, rawDBErr).Once()

		resp, callErr := client.Lookup(ctx, connect.NewRequest(&storagev1.LookupRequest{
			ParentIno: 1,
			Name:      []byte("db_err.txt"),
		}))
		require.Error(t, callErr)
		assert.Nil(t, resp)

		assert.Equal(t, connect.CodeInternal, connect.CodeOf(callErr))
		var connErr *connect.Error
		require.True(t, errors.As(callErr, &connErr))
		assert.Equal(t, "internal server error", connErr.Message())
		assert.NotContains(t, connErr.Message(), "pq:")
		assert.NotContains(t, connErr.Message(), "SQLSTATE")

		// 日志中精确记录了包含原始错误的日志与脱敏警告
		assert.Contains(t, logCap.String(), "internal server error masked")
		assert.Contains(t, logCap.String(), "pq: syntax error at or near")
		assert.Contains(t, logCap.String(), "SQLSTATE 42601")
	})

	t.Run("meta.ErrIO 内部错误脱敏但保留 EIO Detail", func(t *testing.T) {
		logCap.Reset()
		mockMgr.EXPECT().Lookup(mock.Anything, uint64(1), []byte("sector_test.txt")).
			Return(nil, fmt.Errorf("%w: underlying disk sector failed", meta.ErrIO)).Once()

		resp, callErr := client.Lookup(ctx, connect.NewRequest(&storagev1.LookupRequest{
			ParentIno: 1,
			Name:      []byte("sector_test.txt"),
		}))
		require.Error(t, callErr)
		assert.Nil(t, resp)

		assert.Equal(t, connect.CodeInternal, connect.CodeOf(callErr))
		var connErr *connect.Error
		require.True(t, errors.As(callErr, &connErr))
		assert.Equal(t, "internal server error", connErr.Message())

		// Detail 中必须完好无损地挂载 Reason: "EIO"
		info := server.ExtractErrorInfo(callErr)
		require.NotNil(t, info, "脱敏后必须完整保留 Details 中的 ErrorInfo")
		assert.Equal(t, "EIO", info.GetReason())
		assert.Equal(t, "bowl.storage", info.GetDomain())

		assert.Contains(t, logCap.String(), "internal server error masked")
	})

	t.Run("业务正常错误（如 ENOENT）不被脱敏且保留详情", func(t *testing.T) {
		logCap.Reset()
		mockMgr.EXPECT().Lookup(mock.Anything, uint64(1), []byte("missing.txt")).
			Return(nil, fmt.Errorf("wrapped: %w", meta.ErrNotFound)).Once()

		resp, callErr := client.Lookup(ctx, connect.NewRequest(&storagev1.LookupRequest{
			ParentIno: 1,
			Name:      []byte("missing.txt"),
		}))
		require.Error(t, callErr)
		assert.Nil(t, resp)

		assert.Equal(t, connect.CodeNotFound, connect.CodeOf(callErr))
		var connErr *connect.Error
		require.True(t, errors.As(callErr, &connErr))
		assert.NotEqual(t, "internal server error", connErr.Message())
		assert.Contains(t, connErr.Message(), "wrapped:")

		info := server.ExtractErrorInfo(callErr)
		require.NotNil(t, info)
		assert.Equal(t, "ENOENT", info.GetReason())
		assert.Equal(t, "bowl.storage", info.GetDomain())
	})

	t.Run("业务参数错误（如 EINVAL）不被脱敏且保留详情", func(t *testing.T) {
		logCap.Reset()
		mockMgr.EXPECT().Lookup(mock.Anything, uint64(1), []byte("invalid_arg.txt")).
			Return(nil, fmt.Errorf("domain check: %w", meta.ErrInvalidArgument)).Once()

		resp, callErr := client.Lookup(ctx, connect.NewRequest(&storagev1.LookupRequest{
			ParentIno: 1,
			Name:      []byte("invalid_arg.txt"),
		}))
		require.Error(t, callErr)
		assert.Nil(t, resp)

		assert.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(callErr))
		var connErr *connect.Error
		require.True(t, errors.As(callErr, &connErr))
		assert.NotEqual(t, "internal server error", connErr.Message())
		assert.Contains(t, connErr.Message(), "domain check:")

		info := server.ExtractErrorInfo(callErr)
		require.NotNil(t, info)
		assert.Equal(t, "EINVAL", info.GetReason())
		assert.Equal(t, "bowl.storage", info.GetDomain())
	})

	mockMgr.AssertExpectations(t)
}

// 4. 验证 server.Module 的 fx.Lifecycle 启动监听、网络通信与优雅停机
func TestServer_FxLifecycle_StartAndStop(t *testing.T) {
	t.Run("配置正常停机超时时OnStart与OnStop平滑完成", func(t *testing.T) {
		mockMgr := metamock.NewMockManager(t)
		logCap := &logCapture{}
		logger := testLogger(logCap)

		var srv *server.Server

		port := allocateFreePort(t)
		cfg := defaultTestConfig()
		cfg.ServerPort = port

		app := fxtest.New(t,
			fx.Provide(
				func() config.ServerConfig { return cfg },
				func() meta.Manager { return mockMgr },
				func() *slog.Logger { return logger },
			),
			server.Module,
			fx.Populate(&srv),
		)

		app.RequireStart()
		require.NotNil(t, srv)

		addr := srv.Addr()
		require.NotEmpty(t, addr)
		assert.Equal(t, cfg.Addr(), addr)

		// 等待后台 goroutine ListenAndServe 就绪
		assertPortListening(t, addr)

		// 验证 HTTP Server 真实就绪，且通过真实 h2c 通信时协议版本为 HTTP/2
		h2cClient, _ := newH2CTestClient()
		resp, err := h2cClient.Get("http://" + addr + "/")
		require.NoError(t, err)
		defer resp.Body.Close()
		assert.Equal(t, "HTTP/2.0", resp.Proto)
		_, _ = io.Copy(io.Discard, resp.Body)

		// 优雅停机
		app.RequireStop()

		// 停机后验证端口已释放关闭
		assertPortClosed(t, addr)
	})

	t.Run("停机超时配置为0时亦可平滑关停", func(t *testing.T) {
		mockMgr := metamock.NewMockManager(t)
		shutdowner := &fakeShutdowner{}
		lc := fxtest.NewLifecycle(t)

		port := allocateFreePort(t)
		cfg := defaultTestConfig()
		cfg.ServerPort = port
		cfg.ServerStopTimeout = 0

		srv, err := server.New(server.Params{
			Config:      cfg,
			MetaManager: mockMgr,
			Lifecycle:   lc,
			Shutdowner:  shutdowner,
		})
		require.NoError(t, err)
		require.NotNil(t, srv)

		lc.RequireStart()
		assertPortListening(t, cfg.Addr())

		require.NoError(t, lc.Stop(context.Background()))
		assert.False(t, shutdowner.shutdownCalled.Load())
		assertPortClosed(t, cfg.Addr())
	})
}

// 5. 验证存在在途 RPC 请求时触发 OnStop 优雅停机排空：OnStop 会等待在途 RPC 处理完毕并成功返回响应后才完成停机。
func TestServer_FxLifecycle_GracefulShutdownDrainsInflightRequests(t *testing.T) {
	mockMgr := metamock.NewMockManager(t)
	var srv *server.Server

	port := allocateFreePort(t)
	cfg := defaultTestConfig()
	cfg.ServerPort = port
	cfg.ServerStopTimeout = 5 * time.Second

	app := fxtest.New(t,
		fx.Provide(
			func() config.ServerConfig { return cfg },
			func() meta.Manager { return mockMgr },
		),
		server.Module,
		fx.Populate(&srv),
	)

	app.RequireStart()
	require.NotNil(t, srv)

	addr := srv.Addr()
	assertPortListening(t, addr)

	client := storagev1connect.NewStorageServiceClient(http.DefaultClient, "http://"+addr)

	reqStarted := make(chan struct{})
	allowFinish := make(chan struct{})

	mockMgr.EXPECT().Lookup(mock.Anything, uint64(1), []byte("slow.txt")).Run(func(ctx context.Context, parentIno uint64, name []byte) {
		close(reqStarted)
		<-allowFinish
	}).Return(sampleInode(42, meta.S_IFREG|0o644), nil).Once()

	var (
		callResp *connect.Response[storagev1.LookupResponse]
		callErr  error
		rpcDone  = make(chan struct{})
	)

	go func() {
		defer close(rpcDone)
		callResp, callErr = client.Lookup(context.Background(), connect.NewRequest(&storagev1.LookupRequest{
			ParentIno: 1,
			Name:      []byte("slow.txt"),
		}))
	}()

	select {
	case <-reqStarted:
	case <-time.After(3 * time.Second):
		t.Fatal("timeout waiting for RPC to start")
	}

	stopDone := make(chan struct{})
	go func() {
		defer close(stopDone)
		app.RequireStop()
	}()

	// 此时停机流程应因在途 RPC 正在执行而等待
	select {
	case <-stopDone:
		t.Fatal("server stopped prematurely while RPC was still inflight")
	case <-time.After(200 * time.Millisecond):
	}

	// 允许在途 RPC 恢复并完成响应
	close(allowFinish)

	select {
	case <-rpcDone:
		require.NoError(t, callErr)
		require.NotNil(t, callResp)
		assert.Equal(t, uint64(42), callResp.Msg.GetInode().GetIno())
	case <-time.After(3 * time.Second):
		t.Fatal("timeout waiting for inflight RPC to complete")
	}

	// 验证停机流程在在途 RPC 响应完成后顺利退出
	select {
	case <-stopDone:
	case <-time.After(3 * time.Second):
		t.Fatal("timeout waiting for OnStop to complete after inflight RPC finished")
	}

	assertPortClosed(t, addr)
}

// 6. 验证当 Logger 为 nil 时，通过真实 srv.Handler() 发起的未知内部错误依然被脱敏为 "internal server error"，且 meta.ErrIO 的 ErrorInfo 依然完整保留。
func TestMaskInterceptor_NilLogger_MasksInternalAndRetainsErrorInfo(t *testing.T) {
	mockMgr := metamock.NewMockManager(t)

	srv, err := server.New(server.Params{
		Config:      defaultTestConfig(),
		MetaManager: mockMgr,
		Logger:      nil,
	})
	require.NoError(t, err)

	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	client := storagev1connect.NewStorageServiceClient(http.DefaultClient, ts.URL)
	ctx := context.Background()

	t.Run("Logger 为 nil 时未知内部错误依然被脱敏为 internal server error", func(t *testing.T) {
		mockMgr.EXPECT().Lookup(mock.Anything, uint64(1), []byte("secret.txt")).
			Return(nil, errors.New("db connection reset: pq fatal socket read")).Once()

		resp, callErr := client.Lookup(ctx, connect.NewRequest(&storagev1.LookupRequest{
			ParentIno: 1,
			Name:      []byte("secret.txt"),
		}))
		require.Error(t, callErr)
		assert.Nil(t, resp)

		assert.Equal(t, connect.CodeInternal, connect.CodeOf(callErr))
		var connErr *connect.Error
		require.True(t, errors.As(callErr, &connErr))
		assert.Equal(t, "internal server error", connErr.Message())
		assert.NotContains(t, connErr.Message(), "pq fatal socket read")
	})

	t.Run("Logger 为 nil 时 meta.ErrIO 脱敏为 internal server error 且保留 EIO ErrorInfo", func(t *testing.T) {
		mockMgr.EXPECT().Lookup(mock.Anything, uint64(1), []byte("io_err.txt")).
			Return(nil, fmt.Errorf("%w: underlying disk crash", meta.ErrIO)).Once()

		resp, callErr := client.Lookup(ctx, connect.NewRequest(&storagev1.LookupRequest{
			ParentIno: 1,
			Name:      []byte("io_err.txt"),
		}))
		require.Error(t, callErr)
		assert.Nil(t, resp)

		assert.Equal(t, connect.CodeInternal, connect.CodeOf(callErr))
		var connErr *connect.Error
		require.True(t, errors.As(callErr, &connErr))
		assert.Equal(t, "internal server error", connErr.Message())

		info := server.ExtractErrorInfo(callErr)
		require.NotNil(t, info, "脱敏后必须完整保留 Details 中的 ErrorInfo")
		assert.Equal(t, "EIO", info.GetReason())
		assert.Equal(t, "bowl.storage", info.GetDomain())
	})

	mockMgr.AssertExpectations(t)
}

func TestServer_ValidateConfigError(t *testing.T) {
	mockMgr := metamock.NewMockManager(t)
	cfg := defaultTestConfig()
	cfg.ServerPort = 99999 // 非法端口

	_, err := server.New(server.Params{
		Config:      cfg,
		MetaManager: mockMgr,
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid server port")

	cfg.ServerPort = -1
	_, err = server.New(server.Params{
		Config:      cfg,
		MetaManager: mockMgr,
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid server port")
}

func TestServer_PortConflict_TriggersShutdown(t *testing.T) {
	t.Run("端口冲突监听失败自动触发应用退出且传递 ExitCode 选项", func(t *testing.T) {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)
		defer ln.Close()

		tcpAddr := ln.Addr().(*net.TCPAddr)

		shutdowner := &fakeShutdowner{}
		lc := fxtest.NewLifecycle(t)

		cfg := defaultTestConfig()
		cfg.ServerPort = tcpAddr.Port

		mockMgr := metamock.NewMockManager(t)
		srv, err := server.New(server.Params{
			Config:      cfg,
			MetaManager: mockMgr,
			Lifecycle:   lc,
			Shutdowner:  shutdowner,
		})
		require.NoError(t, err)
		require.NotNil(t, srv)

		lc.RequireStart()

		// 等待后台 goroutine 触发 ListenAndServe 端口冲突并级联调用 shutdowner.Shutdown
		require.Eventually(t, func() bool {
			return shutdowner.shutdownCalled.Load()
		}, 2*time.Second, 10*time.Millisecond)

		assert.NotEmpty(t, shutdowner.Options(), "端口冲突异常退出时应传递 ExitCode 选项")

		// 调用 OnStop 收集错误
		stopErr := lc.Stop(context.Background())
		require.Error(t, stopErr)
		assert.Contains(t, stopErr.Error(), "server error")
		assert.Contains(t, stopErr.Error(), "address already in use")
	})

	t.Run("应用停机通知返回错误时完整合并错误信息", func(t *testing.T) {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)
		defer ln.Close()

		tcpAddr := ln.Addr().(*net.TCPAddr)

		shutdowner := &fakeShutdowner{
			shutdownErr: errors.New("app shutdown failed"),
		}
		lc := fxtest.NewLifecycle(t)

		cfg := defaultTestConfig()
		cfg.ServerPort = tcpAddr.Port

		mockMgr := metamock.NewMockManager(t)
		srv, err := server.New(server.Params{
			Config:      cfg,
			MetaManager: mockMgr,
			Lifecycle:   lc,
			Shutdowner:  shutdowner,
		})
		require.NoError(t, err)
		require.NotNil(t, srv)

		lc.RequireStart()

		require.Eventually(t, func() bool {
			return shutdowner.shutdownCalled.Load()
		}, 2*time.Second, 10*time.Millisecond)

		stopErr := lc.Stop(context.Background())
		require.Error(t, stopErr)
		assert.Contains(t, stopErr.Error(), "server error")
		assert.Contains(t, stopErr.Error(), "request application shutdown: app shutdown failed")
	})
}

func TestToConnectError_UnknownAndUnmappedErrors(t *testing.T) {
	unknownErr := errors.New("something totally random")
	res := server.ToConnectError(unknownErr)
	require.Error(t, res)
	assert.Equal(t, connect.CodeUnknown, connect.CodeOf(res))
	assert.Nil(t, server.ExtractErrorInfo(res))
}

func TestServer_Getters(t *testing.T) {
	mockMgr := metamock.NewMockManager(t)
	cfg := defaultTestConfig()
	srv, err := server.New(server.Params{
		Config:      cfg,
		MetaManager: mockMgr,
	})
	require.NoError(t, err)

	assert.Equal(t, mockMgr, srv.MetaManager())
	assert.NotNil(t, srv.Handler())
	assert.Contains(t, srv.Addr(), "127.0.0.1")
}

// 6. 验证 ADR-0017 规定的 h2c（HTTP/2 Cleartext）与原生 gRPC / gRPC-Web 客户端协议契约：
// 既验证正向 RPC 成功响应且传输协议为 HTTP/2，又验证负向领域错误与 ErrorInfo（含 Reason、Domain、Metadata["errno"]）完整透传。
func TestServer_H2C_GRPC_ProtocolContract(t *testing.T) {
	mockMgr := metamock.NewMockManager(t)
	logCap := &logCapture{}
	logger := testLogger(logCap)

	var srv *server.Server

	port := allocateFreePort(t)
	cfg := defaultTestConfig()
	cfg.ServerPort = port

	app := fxtest.New(t,
		fx.Provide(
			func() config.ServerConfig { return cfg },
			func() meta.Manager { return mockMgr },
			func() *slog.Logger { return logger },
		),
		server.Module,
		fx.Populate(&srv),
	)

	app.RequireStart()
	t.Cleanup(func() {
		app.RequireStop()
		assertPortClosed(t, srv.Addr())
	})

	addr := srv.Addr()
	assertPortListening(t, addr)

	h2cClient, lastProto := newH2CTestClient()
	baseURL := "http://" + addr

	grpcClient := storagev1connect.NewStorageServiceClient(
		h2cClient,
		baseURL,
		connect.WithGRPC(),
	)

	grpcWebClient := storagev1connect.NewStorageServiceClient(
		h2cClient,
		baseURL,
		connect.WithGRPCWeb(),
	)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	t.Run("原生 gRPC 客户端正向调用成功且底层通过 HTTP/2 通信", func(t *testing.T) {
		mockMgr.EXPECT().Lookup(mock.Anything, uint64(1), []byte("h2c_success.txt")).
			Return(&meta.Inode{
				Ino:                 2001,
				Generation:          1,
				EntryTimeoutSeconds: 1.0,
				AttrTimeoutSeconds:  1.0,
			}, nil).Once()

		resp, err := grpcClient.Lookup(ctx, connect.NewRequest(&storagev1.LookupRequest{
			ParentIno: 1,
			Name:      []byte("h2c_success.txt"),
		}))
		require.NoError(t, err)
		require.NotNil(t, resp)
		assert.Equal(t, uint64(2001), resp.Msg.GetInode().GetIno())

		captured := lastProto.Load()
		require.NotNil(t, captured)
		assert.Equal(t, "HTTP/2.0", *captured, "原生 gRPC 经由 h2c 传输时必须为 HTTP/2.0")
	})

	t.Run("原生 gRPC 客户端负向错误 (ErrNotFound) 完整透传 CodeNotFound 与 ErrorInfo", func(t *testing.T) {
		mockMgr.EXPECT().Lookup(mock.Anything, uint64(1), []byte("missing.txt")).
			Return(nil, meta.ErrNotFound).Once()

		resp, err := grpcClient.Lookup(ctx, connect.NewRequest(&storagev1.LookupRequest{
			ParentIno: 1,
			Name:      []byte("missing.txt"),
		}))
		require.Error(t, err)
		assert.Nil(t, resp)

		assert.Equal(t, connect.CodeNotFound, connect.CodeOf(err))
		info := server.ExtractErrorInfo(err)
		require.NotNil(t, info, "必须无损透传 google.rpc.ErrorInfo")
		assert.Equal(t, "ENOENT", info.GetReason())
		assert.Equal(t, "bowl.storage", info.GetDomain())
		if info.GetMetadata() != nil {
			if errnoVal, ok := info.GetMetadata()["errno"]; ok {
				assert.Equal(t, "ENOENT", errnoVal)
			}
		}
	})

	t.Run("原生 gRPC 客户端负向错误 (ErrAlreadyExists) 完整透传 CodeAlreadyExists 与 ErrorInfo", func(t *testing.T) {
		mockMgr.EXPECT().Lookup(mock.Anything, uint64(1), []byte("exist.txt")).
			Return(nil, meta.ErrAlreadyExists).Once()

		resp, err := grpcClient.Lookup(ctx, connect.NewRequest(&storagev1.LookupRequest{
			ParentIno: 1,
			Name:      []byte("exist.txt"),
		}))
		require.Error(t, err)
		assert.Nil(t, resp)

		assert.Equal(t, connect.CodeAlreadyExists, connect.CodeOf(err))
		info := server.ExtractErrorInfo(err)
		require.NotNil(t, info, "必须无损透传 google.rpc.ErrorInfo")
		assert.Equal(t, "EEXIST", info.GetReason())
		assert.Equal(t, "bowl.storage", info.GetDomain())
		if info.GetMetadata() != nil {
			if errnoVal, ok := info.GetMetadata()["errno"]; ok {
				assert.Equal(t, "EEXIST", errnoVal)
			}
		}
	})

	t.Run("原生 gRPC 客户端携带 Metadata[errno] 的 ErrorInfo 完整透传", func(t *testing.T) {
		customStatus, stErr := status.New(codes.AlreadyExists, "meta: entry already exists").WithDetails(&errdetails.ErrorInfo{
			Reason: "EEXIST",
			Domain: "bowl.storage",
			Metadata: map[string]string{
				"errno": "EEXIST",
			},
		})
		require.NoError(t, stErr)

		mockMgr.EXPECT().Lookup(mock.Anything, uint64(1), []byte("exist_with_metadata.txt")).
			Return(nil, customStatus.Err()).Once()

		resp, err := grpcClient.Lookup(ctx, connect.NewRequest(&storagev1.LookupRequest{
			ParentIno: 1,
			Name:      []byte("exist_with_metadata.txt"),
		}))
		require.Error(t, err)
		assert.Nil(t, resp)

		assert.Equal(t, connect.CodeAlreadyExists, connect.CodeOf(err))
		info := server.ExtractErrorInfo(err)
		require.NotNil(t, info, "必须完整透传 google.rpc.ErrorInfo")
		assert.Equal(t, "EEXIST", info.GetReason())
		assert.Equal(t, "bowl.storage", info.GetDomain())
		require.NotNil(t, info.GetMetadata())
		assert.Equal(t, "EEXIST", info.GetMetadata()["errno"])
	})

	t.Run("gRPC-Web 客户端正向调用与负向错误 (ErrNotFound) 协议契约验证", func(t *testing.T) {
		// 正向调用
		mockMgr.EXPECT().Lookup(mock.Anything, uint64(1), []byte("grpcweb_success.txt")).
			Return(&meta.Inode{
				Ino:                 3001,
				Generation:          1,
				EntryTimeoutSeconds: 1.0,
				AttrTimeoutSeconds:  1.0,
			}, nil).Once()

		resp, err := grpcWebClient.Lookup(ctx, connect.NewRequest(&storagev1.LookupRequest{
			ParentIno: 1,
			Name:      []byte("grpcweb_success.txt"),
		}))
		require.NoError(t, err)
		require.NotNil(t, resp)
		assert.Equal(t, uint64(3001), resp.Msg.GetInode().GetIno())

		// 负向调用 (ErrNotFound)
		mockMgr.EXPECT().Lookup(mock.Anything, uint64(1), []byte("grpcweb_missing.txt")).
			Return(nil, meta.ErrNotFound).Once()

		respErr, callErr := grpcWebClient.Lookup(ctx, connect.NewRequest(&storagev1.LookupRequest{
			ParentIno: 1,
			Name:      []byte("grpcweb_missing.txt"),
		}))
		require.Error(t, callErr)
		assert.Nil(t, respErr)

		assert.Equal(t, connect.CodeNotFound, connect.CodeOf(callErr))
		info := server.ExtractErrorInfo(callErr)
		require.NotNil(t, info)
		assert.Equal(t, "ENOENT", info.GetReason())
		assert.Equal(t, "bowl.storage", info.GetDomain())
	})
}

// 验证使用 config.Load() 默认配置时，Server 能够正常处理 4 MiB Write RPC（支持 Protobuf 与 JSON）
func TestServer_DefaultReadMaxBytesAllows4MiBWrite(t *testing.T) {
	tempDir := t.TempDir()
	t.Chdir(tempDir)

	t.Setenv("DB_DSN", "postgres://user:pass@localhost:5432/bowl")
	t.Setenv("SERVER_READ_MAX_BYTES", "")

	cfg, err := config.Load()
	require.NoError(t, err)

	const fourMiB = 4 * 1024 * 1024

	mockMgr := metamock.NewMockManager(t)
	mockMgr.EXPECT().Write(mock.Anything, mock.MatchedBy(func(req *meta.WriteRequest) bool {
		return req != nil && len(req.Data) == fourMiB && req.Ino == 1
	})).Return(&meta.WriteResponse{
		Size: fourMiB,
		Inode: &meta.Inode{
			Ino:        1,
			Generation: 1,
			Attr: meta.Attribute{
				Size: fourMiB,
			},
		},
	}, nil).Maybe()

	srv, err := server.New(server.Params{
		Config:      cfg.ServerConfig,
		MetaManager: mockMgr,
	})
	require.NoError(t, err)

	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	ctx := context.Background()
	payload := make([]byte, fourMiB)

	// 1. Protobuf 编码客户端
	protoClient := storagev1connect.NewStorageServiceClient(http.DefaultClient, ts.URL)
	reqProto := connect.NewRequest(&storagev1.WriteRequest{
		Ino:    1,
		Fh:     1,
		Offset: 0,
		Data:   payload,
	})
	respProto, errProto := protoClient.Write(ctx, reqProto)
	assert.NoError(t, errProto, "Protobuf 客户端 4 MiB 写入应成功")
	if assert.NotNil(t, respProto) {
		assert.Equal(t, uint32(fourMiB), respProto.Msg.Size)
	}

	// 2. JSON 编码客户端
	jsonClient := storagev1connect.NewStorageServiceClient(http.DefaultClient, ts.URL, connect.WithProtoJSON())
	reqJSON := connect.NewRequest(&storagev1.WriteRequest{
		Ino:    1,
		Fh:     1,
		Offset: 0,
		Data:   payload,
	})
	respJSON, errJSON := jsonClient.Write(ctx, reqJSON)
	assert.NoError(t, errJSON, "JSON 客户端 4 MiB 写入应成功")
	if assert.NotNil(t, respJSON) {
		assert.Equal(t, uint32(fourMiB), respJSON.Msg.Size)
	}
}
