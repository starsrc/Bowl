package server_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/fx"
	"go.uber.org/fx/fxtest"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
	"github.com/starsrc/bowl/gen/go/storage/v1/storagev1connect"
	"github.com/starsrc/bowl/pkg/validator"
	"github.com/starsrc/bowl/services/storage/internal/config"
	"github.com/starsrc/bowl/services/storage/internal/meta"
	"github.com/starsrc/bowl/services/storage/internal/meta/mock"
	"github.com/starsrc/bowl/services/storage/internal/server"
)

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

func testLogger(capture *logCapture) *slog.Logger {
	handler := slog.NewJSONHandler(capture, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	})
	return slog.New(handler)
}

func defaultTestConfig() config.ServerConfig {
	return config.ServerConfig{
		Host:                 "127.0.0.1",
		Port:                 0,
		ReadHeaderTimeoutVal: 5 * time.Second,
		StopTimeoutVal:       5 * time.Second,
		ReadMaxBytesVal:      131072,
	}
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
	mockMgr := mock.NewMockManager(t) // 零 Expectation，任何被测调用将导致直接失败

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

// overrideLookupHandler 用于测试注入特定 Handler 返回错误
type overrideLookupHandler struct {
	*server.Server
	lookupFn func(context.Context, *connect.Request[storagev1.LookupRequest]) (*connect.Response[storagev1.LookupResponse], error)
}

func (h *overrideLookupHandler) Lookup(ctx context.Context, req *connect.Request[storagev1.LookupRequest]) (*connect.Response[storagev1.LookupResponse], error) {
	if h.lookupFn != nil {
		return h.lookupFn(ctx, req)
	}
	return h.Server.Lookup(ctx, req)
}

// 3. 验证未知内部错误与包装错误经 MaskInterceptor 脱敏为 "internal server error"，同时完整保留 ErrorInfo Detail。
func TestMaskInterceptor_MasksInternalAndRetainsErrorInfo(t *testing.T) {
	mockMgr := mock.NewMockManager(t)
	logCap := &logCapture{}
	logger := testLogger(logCap)

	srv, err := server.New(server.Params{
		Config:      defaultTestConfig(),
		MetaManager: mockMgr,
		Logger:      logger,
	})
	require.NoError(t, err)

	handler := &overrideLookupHandler{Server: srv}

	// 组装真实的生产洋葱拦截器链（包含真实的 MaskInterceptor、otelconnect、slogx 日志与 validator）
	interceptors, err := server.BuildInterceptors(nil, nil, logger)
	require.NoError(t, err)

	mux := http.NewServeMux()
	path, h2 := storagev1connect.NewStorageServiceHandler(
		handler,
		connect.WithInterceptors(interceptors...),
	)
	mux.Handle(path, h2)

	ts := httptest.NewServer(mux)
	defer ts.Close()

	client := storagev1connect.NewStorageServiceClient(http.DefaultClient, ts.URL)
	ctx := context.Background()

	t.Run("原生数据库异常脱敏为 internal server error", func(t *testing.T) {
		handler.lookupFn = func(ctx context.Context, req *connect.Request[storagev1.LookupRequest]) (*connect.Response[storagev1.LookupResponse], error) {
			return nil, server.ToConnectError(errors.New("db connection reset: pq fatal socket read"))
		}

		resp, callErr := client.Lookup(ctx, connect.NewRequest(&storagev1.LookupRequest{
			ParentIno: 1,
			Name:      []byte("normal.txt"),
		}))
		require.Error(t, callErr)
		assert.Nil(t, resp)

		assert.Equal(t, connect.CodeInternal, connect.CodeOf(callErr))
		var connErr *connect.Error
		require.True(t, errors.As(callErr, &connErr))
		assert.Equal(t, "internal server error", connErr.Message())
		assert.NotContains(t, connErr.Message(), "pq fatal socket read", "原始敏感数据库错误严禁泄露")

		// 日志中需记录脱敏警告
		assert.Contains(t, logCap.String(), "internal server error masked")
	})

	t.Run("meta.ErrIO 内部错误脱敏但保留 EIO Detail", func(t *testing.T) {
		handler.lookupFn = func(ctx context.Context, req *connect.Request[storagev1.LookupRequest]) (*connect.Response[storagev1.LookupResponse], error) {
			return nil, server.ToConnectError(fmt.Errorf("%w: underlying disk sector failed", meta.ErrIO))
		}

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
	})

	t.Run("业务正常错误（如 ENOENT）不被脱敏且保留详情", func(t *testing.T) {
		handler.lookupFn = func(ctx context.Context, req *connect.Request[storagev1.LookupRequest]) (*connect.Response[storagev1.LookupResponse], error) {
			return nil, server.ToConnectError(meta.ErrNotFound)
		}

		resp, callErr := client.Lookup(ctx, connect.NewRequest(&storagev1.LookupRequest{
			ParentIno: 1,
			Name:      []byte("missing.txt"),
		}))
		require.Error(t, callErr)
		assert.Nil(t, resp)

		assert.Equal(t, connect.CodeNotFound, connect.CodeOf(callErr))
		info := server.ExtractErrorInfo(callErr)
		require.NotNil(t, info)
		assert.Equal(t, "ENOENT", info.GetReason())
	})
}

// 4. 验证 server.Module 的 fx.Lifecycle 启动监听、网络通信与优雅停机
func TestServer_FxLifecycle_StartAndStop(t *testing.T) {
	mockMgr := mock.NewMockManager(t)
	logCap := &logCapture{}
	logger := testLogger(logCap)

	var srv *server.Server

	cfg := defaultTestConfig()
	cfg.Port = 0 // 动态随机端口

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
	assert.NotEqual(t, "127.0.0.1:0", addr, "必须绑定到真实的系统随机端口")

	// 验证 HTTP Server 真实就绪可通信
	resp, err := http.Get("http://" + addr + "/")
	require.NoError(t, err)
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)

	// 优雅停机
	app.RequireStop()

	// 停机后再次连接应拒绝
	_, dialErr := net.DialTimeout("tcp", addr, 100*time.Millisecond)
	assert.Error(t, dialErr, "停机后监听端口应已释放关闭")
}

func TestServer_ValidateConfigError(t *testing.T) {
	mockMgr := mock.NewMockManager(t)
	cfg := defaultTestConfig()
	cfg.Port = 99999 // 非法端口

	_, err := server.New(server.Params{
		Config:      cfg,
		MetaManager: mockMgr,
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid server port")

	cfg.Port = -1
	_, err = server.New(server.Params{
		Config:      cfg,
		MetaManager: mockMgr,
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid server port")
}

func TestServer_PortConflict_StartFails(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer ln.Close()

	tcpAddr := ln.Addr().(*net.TCPAddr)

	cfg := defaultTestConfig()
	cfg.Port = tcpAddr.Port

	app := fxtest.New(t,
		fx.Provide(
			func() config.ServerConfig { return cfg },
			func() meta.Manager { return mock.NewMockManager(t) },
		),
		server.Module,
	)

	startErr := app.Start(context.Background())
	require.Error(t, startErr)
	assert.Contains(t, startErr.Error(), "address already in use")
}

func TestToConnectError_UnknownAndUnmappedErrors(t *testing.T) {
	unknownErr := errors.New("something totally random")
	res := server.ToConnectError(unknownErr)
	require.Error(t, res)
	assert.Equal(t, connect.CodeUnknown, connect.CodeOf(res))
	assert.Nil(t, server.ExtractErrorInfo(res))
}

func TestServer_Getters(t *testing.T) {
	mockMgr := mock.NewMockManager(t)
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
