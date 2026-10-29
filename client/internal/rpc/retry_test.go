package rpc

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"connectrpc.com/connect"
	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
	"github.com/starsrc/bowl/gen/go/storage/v1/storagev1connect"
	"github.com/stretchr/testify/require"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
)

// mockStorageHandler 用于故障注入与请求计数。
type mockStorageHandler struct {
	storagev1connect.UnimplementedStorageServiceHandler

	mu               sync.Mutex
	lookupCallCount  int32
	getDiskCallCount int32
	createCallCount  int32
	onLookup         func(ctx context.Context, req *connect.Request[storagev1.LookupRequest]) (*connect.Response[storagev1.LookupResponse], error)
	onGetDisk        func(ctx context.Context, req *connect.Request[storagev1.GetDiskRequest]) (*connect.Response[storagev1.GetDiskResponse], error)
	onCreate         func(ctx context.Context, req *connect.Request[storagev1.CreateRequest]) (*connect.Response[storagev1.CreateResponse], error)
}

func (m *mockStorageHandler) Lookup(ctx context.Context, req *connect.Request[storagev1.LookupRequest]) (*connect.Response[storagev1.LookupResponse], error) {
	atomic.AddInt32(&m.lookupCallCount, 1)
	m.mu.Lock()
	fn := m.onLookup
	m.mu.Unlock()
	if fn != nil {
		return fn(ctx, req)
	}
	return connect.NewResponse(&storagev1.LookupResponse{
		Inode: &storagev1.Inode{
			Ino: 2002,
		},
	}), nil
}

func (m *mockStorageHandler) GetDisk(ctx context.Context, req *connect.Request[storagev1.GetDiskRequest]) (*connect.Response[storagev1.GetDiskResponse], error) {
	atomic.AddInt32(&m.getDiskCallCount, 1)
	m.mu.Lock()
	fn := m.onGetDisk
	m.mu.Unlock()
	if fn != nil {
		return fn(ctx, req)
	}
	return connect.NewResponse(&storagev1.GetDiskResponse{
		Disk: &storagev1.Disk{
			DiskId: req.Msg.GetDiskId(),
		},
	}), nil
}

func (m *mockStorageHandler) Create(ctx context.Context, req *connect.Request[storagev1.CreateRequest]) (*connect.Response[storagev1.CreateResponse], error) {
	atomic.AddInt32(&m.createCallCount, 1)
	m.mu.Lock()
	fn := m.onCreate
	m.mu.Unlock()
	if fn != nil {
		return fn(ctx, req)
	}
	return connect.NewResponse(&storagev1.CreateResponse{
		Inode: &storagev1.Inode{
			Ino: 3003,
		},
		Fh: 12345,
	}), nil
}

func startMockServer(t *testing.T, handler storagev1connect.StorageServiceHandler) (*httptest.Server, string) {
	t.Helper()
	mux := http.NewServeMux()
	path, srvHandler := storagev1connect.NewStorageServiceHandler(handler)
	mux.Handle(path, srvHandler)
	server := httptest.NewServer(h2c.NewHandler(mux, &http2.Server{}))
	t.Cleanup(server.Close)
	return server, server.URL
}

func TestIdempotentProceduresWhitelist(t *testing.T) {
	expectedProcedures := []string{
		storagev1connect.StorageServiceGetDiskProcedure,
		storagev1connect.StorageServiceListDisksProcedure,
		storagev1connect.StorageServiceLookupProcedure,
		storagev1connect.StorageServiceGetAttrProcedure,
		storagev1connect.StorageServiceReadlinkProcedure,
		storagev1connect.StorageServiceReadProcedure,
		storagev1connect.StorageServiceFsyncProcedure,
		storagev1connect.StorageServiceFsyncDirProcedure,
		storagev1connect.StorageServiceReadDirProcedure,
		storagev1connect.StorageServiceReadDirPlusProcedure,
		storagev1connect.StorageServiceGetXattrProcedure,
		storagev1connect.StorageServiceListXattrProcedure,
		storagev1connect.StorageServiceGetLkProcedure,
		storagev1connect.StorageServiceStatFSProcedure,
		storagev1connect.StorageServiceRenewSessionProcedure,
		storagev1connect.StorageServiceCloseSessionProcedure,
	}

	require.Len(t, expectedProcedures, 16, "白名单必须恰好包含 16 个只读与幂等 RPC")
	for _, proc := range expectedProcedures {
		require.True(t, IsIdempotentProcedure(proc), "Procedure %s 必须被判定为幂等", proc)
	}

	// 非幂等方法必须返回 false
	nonIdempotent := []string{
		storagev1connect.StorageServiceCreateProcedure,
		storagev1connect.StorageServiceMkdirProcedure,
		storagev1connect.StorageServiceUnlinkProcedure,
		storagev1connect.StorageServiceRenameProcedure,
		storagev1connect.StorageServiceWriteProcedure,
		storagev1connect.StorageServiceSetAttrProcedure,
		storagev1connect.StorageServiceSetLkProcedure,
		storagev1connect.StorageServiceSetLkwProcedure,
	}
	for _, proc := range nonIdempotent {
		require.False(t, IsIdempotentProcedure(proc), "Procedure %s 不应被判定为幂等", proc)
	}
}

// Slice 1: 幂等 RPC 在遇到 CodeUnavailable 时自动指数退避重试并在服务端恢复后成功
func TestRetry_IdempotentRPC_TransientUnavailable_Success(t *testing.T) {
	handler := &mockStorageHandler{}
	var attempts int32
	handler.onLookup = func(ctx context.Context, req *connect.Request[storagev1.LookupRequest]) (*connect.Response[storagev1.LookupResponse], error) {
		att := atomic.AddInt32(&attempts, 1)
		if att < 3 {
			// 前 2 次调用返回 CodeUnavailable
			return nil, connect.NewError(connect.CodeUnavailable, errors.New("service temporarily overloaded"))
		}
		// 第 3 次成功返回
		return connect.NewResponse(&storagev1.LookupResponse{
			Inode: &storagev1.Inode{Ino: 2024},
		}), nil
	}

	_, serverURL := startMockServer(t, handler)

	retryCfg := &RetryConfig{
		MaxElapsedTime:  2 * time.Second,
		InitialInterval: 5 * time.Millisecond,
		Multiplier:      2.0,
		MaxInterval:     20 * time.Millisecond,
		RandFloat64:     func() float64 { return 0.5 },
	}

	client, err := NewClientFromConfig(ClientConfig{
		Endpoint:    serverURL,
		RetryConfig: retryCfg,
	})
	require.NoError(t, err)

	resp, err := client.Lookup(context.Background(), connect.NewRequest(&storagev1.LookupRequest{
		ParentIno: 1,
		Name:      []byte("test.txt"),
	}))

	require.NoError(t, err)
	require.NotNil(t, resp)
	require.Equal(t, uint64(2024), resp.Msg.GetInode().GetIno())
	require.Equal(t, int32(3), atomic.LoadInt32(&attempts), "应当在重试 2 次后在第 3 次调用成功")
}

// Slice 1: 幂等 RPC 在遇到底层传输错误（连接重置/意外 EOF）时重试成功
func TestRetry_IdempotentRPC_TransportError_Success(t *testing.T) {
	handler := &mockStorageHandler{}
	var attempts int32
	handler.onGetDisk = func(ctx context.Context, req *connect.Request[storagev1.GetDiskRequest]) (*connect.Response[storagev1.GetDiskResponse], error) {
		att := atomic.AddInt32(&attempts, 1)
		if att < 2 {
			// 模拟底层网络传输中断
			return nil, connect.NewError(connect.CodeUnavailable, syscall.ECONNRESET)
		}
		return connect.NewResponse(&storagev1.GetDiskResponse{
			Disk: &storagev1.Disk{DiskId: req.Msg.GetDiskId()},
		}), nil
	}

	_, serverURL := startMockServer(t, handler)

	retryCfg := &RetryConfig{
		MaxElapsedTime:  1 * time.Second,
		InitialInterval: 5 * time.Millisecond,
		Multiplier:      2.0,
		MaxInterval:     20 * time.Millisecond,
	}

	client, err := NewClientFromConfig(ClientConfig{
		Endpoint:    serverURL,
		RetryConfig: retryCfg,
	})
	require.NoError(t, err)

	resp, err := client.GetDisk(context.Background(), connect.NewRequest(&storagev1.GetDiskRequest{
		DiskId: "disk-id-123",
	}))
	require.NoError(t, err)
	require.Equal(t, "disk-id-123", resp.Msg.GetDisk().GetDiskId())
	require.Equal(t, int32(2), atomic.LoadInt32(&attempts))
}

// Slice 2: 绝不重试（零重试）——携带业务 ErrorInfo 的错误立即返回对应 errno
func TestRetry_NeverRetry_WithErrorInfo(t *testing.T) {
	handler := &mockStorageHandler{}
	var attempts int32
	handler.onLookup = func(ctx context.Context, req *connect.Request[storagev1.LookupRequest]) (*connect.Response[storagev1.LookupResponse], error) {
		atomic.AddInt32(&attempts, 1)
		// 即使是 CodeUnavailable，但只要携带了业务 ErrorInfo，就说明业务逻辑已确定拒绝，绝不重试
		connErr := connect.NewError(connect.CodeUnavailable, errors.New("file not found"))
		detail, _ := connect.NewErrorDetail(&errdetails.ErrorInfo{
			Reason: "ENOENT",
			Domain: "bowl.storage",
		})
		connErr.AddDetail(detail)
		return nil, connErr
	}

	_, serverURL := startMockServer(t, handler)

	retryCfg := &RetryConfig{
		MaxElapsedTime:  2 * time.Second,
		InitialInterval: 5 * time.Millisecond,
	}
	client, err := NewClientFromConfig(ClientConfig{
		Endpoint:    serverURL,
		RetryConfig: retryCfg,
	})
	require.NoError(t, err)

	_, err = client.Lookup(context.Background(), connect.NewRequest(&storagev1.LookupRequest{
		ParentIno: 1,
		Name:      []byte("missing.txt"),
	}))
	require.Error(t, err)
	require.Equal(t, int32(1), atomic.LoadInt32(&attempts), "携带 ErrorInfo 的错误必须零重试立即返回")
	require.Equal(t, syscall.ENOENT, MapError(err), "必须精确映射为 syscall.ENOENT")
}

// Slice 2: 绝不重试（零重试）——特定业务与客户端 Connect 状态码立即返回
func TestRetry_NeverRetry_SpecificCodes(t *testing.T) {
	neverRetryCodes := []struct {
		code          connect.Code
		expectedErrno syscall.Errno
	}{
		{connect.CodeInvalidArgument, syscall.EINVAL},
		{connect.CodeNotFound, syscall.ENOENT},
		{connect.CodeAlreadyExists, syscall.EEXIST},
		{connect.CodePermissionDenied, syscall.EACCES},
		{connect.CodeFailedPrecondition, syscall.EIO},
		{connect.CodeResourceExhausted, syscall.ENOSPC},
		{connect.CodeAborted, syscall.EIO},
		{connect.CodeUnimplemented, syscall.EOPNOTSUPP},
	}

	for _, tc := range neverRetryCodes {
		t.Run(tc.code.String(), func(t *testing.T) {
			handler := &mockStorageHandler{}
			var attempts int32
			handler.onLookup = func(ctx context.Context, req *connect.Request[storagev1.LookupRequest]) (*connect.Response[storagev1.LookupResponse], error) {
				atomic.AddInt32(&attempts, 1)
				return nil, connect.NewError(tc.code, errors.New("terminal error"))
			}

			_, serverURL := startMockServer(t, handler)
			client, err := NewClientFromConfig(ClientConfig{
				Endpoint: serverURL,
				RetryConfig: &RetryConfig{
					MaxElapsedTime:  2 * time.Second,
					InitialInterval: 5 * time.Millisecond,
				},
			})
			require.NoError(t, err)

			_, err = client.Lookup(context.Background(), connect.NewRequest(&storagev1.LookupRequest{
				ParentIno: 1,
				Name:      []byte("file"),
			}))
			require.Error(t, err)
			require.Equal(t, int32(1), atomic.LoadInt32(&attempts), "Code %s 必须零重试立即返回", tc.code)
			require.Equal(t, tc.expectedErrno, MapError(err))
		})
	}
}

// Slice 3: 非幂等 RPC 仅在建连阶段失败（ECONNREFUSED / dial error）时允许重试
func TestRetry_NonIdempotentRPC_DialError_RetrySuccess(t *testing.T) {
	// 创建一个未启动的 listener 获取随机未被占用的端口
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := l.Addr().String()
	require.NoError(t, l.Close()) // 立即关闭，确保 ECONNREFUSED

	retryCfg := &RetryConfig{
		MaxElapsedTime:  2 * time.Second,
		InitialInterval: 10 * time.Millisecond,
		Multiplier:      1.5,
		MaxInterval:     50 * time.Millisecond,
	}

	client, err := NewClientFromConfig(ClientConfig{
		Endpoint:    "http://" + addr,
		RetryConfig: retryCfg,
	})
	require.NoError(t, err)

	handler := &mockStorageHandler{}
	var created bool
	handler.onCreate = func(ctx context.Context, req *connect.Request[storagev1.CreateRequest]) (*connect.Response[storagev1.CreateResponse], error) {
		created = true
		return connect.NewResponse(&storagev1.CreateResponse{
			Inode: &storagev1.Inode{Ino: 999},
			Fh:    888,
		}), nil
	}

	// 异步延迟 60ms 启动监听服务（模拟服务滚动启动）
	go func() {
		time.Sleep(60 * time.Millisecond)
		mux := http.NewServeMux()
		path, srvHandler := storagev1connect.NewStorageServiceHandler(handler)
		mux.Handle(path, srvHandler)
		l, err := net.Listen("tcp", addr)
		if err != nil {
			return
		}
		server := &http.Server{Handler: h2c.NewHandler(mux, &http2.Server{})}
		t.Cleanup(func() { server.Close() })
		_ = server.Serve(l)
	}()

	resp, err := client.Create(context.Background(), connect.NewRequest(&storagev1.CreateRequest{
		ParentIno: 1,
		Name:      []byte("new-file"),
		Mode:      0644,
	}))
	require.NoError(t, err)
	require.True(t, created)
	require.Equal(t, uint64(999), resp.Msg.GetInode().GetIno())
}

// Slice 3: 非幂等 RPC 请求已发出后，服务端返回 CodeUnavailable 或断开连接，严禁重试并映射为 EIO
func TestRetry_NonIdempotentRPC_SentFailure_NeverRetry(t *testing.T) {
	t.Run("server returns CodeUnavailable after receiving Create", func(t *testing.T) {
		handler := &mockStorageHandler{}
		var attempts int32
		handler.onCreate = func(ctx context.Context, req *connect.Request[storagev1.CreateRequest]) (*connect.Response[storagev1.CreateResponse], error) {
			atomic.AddInt32(&attempts, 1)
			return nil, connect.NewError(connect.CodeUnavailable, errors.New("server crashed mid-way"))
		}

		_, serverURL := startMockServer(t, handler)
		client, err := NewClientFromConfig(ClientConfig{
			Endpoint: serverURL,
			RetryConfig: &RetryConfig{
				MaxElapsedTime:  2 * time.Second,
				InitialInterval: 5 * time.Millisecond,
			},
		})
		require.NoError(t, err)

		_, err = client.Create(context.Background(), connect.NewRequest(&storagev1.CreateRequest{
			ParentIno: 1,
			Name:      []byte("new-file"),
			Mode:      0644,
		}))
		require.Error(t, err)
		require.Equal(t, int32(1), atomic.LoadInt32(&attempts), "非幂等 RPC 在请求送达服务端后发生失败严禁重放")
		require.Equal(t, syscall.EIO, MapError(err), "必须准确映射为 syscall.EIO")
	})

	t.Run("connection drops with EOF mid-way for Create", func(t *testing.T) {
		handler := &mockStorageHandler{}
		var attempts int32
		handler.onCreate = func(ctx context.Context, req *connect.Request[storagev1.CreateRequest]) (*connect.Response[storagev1.CreateResponse], error) {
			atomic.AddInt32(&attempts, 1)
			// 模拟连接已建立但传输中断返回 EOF
			return nil, connect.NewError(connect.CodeUnavailable, io.EOF)
		}

		_, serverURL := startMockServer(t, handler)
		client, err := NewClientFromConfig(ClientConfig{
			Endpoint: serverURL,
			RetryConfig: &RetryConfig{
				MaxElapsedTime:  2 * time.Second,
				InitialInterval: 5 * time.Millisecond,
			},
		})
		require.NoError(t, err)

		_, err = client.Create(context.Background(), connect.NewRequest(&storagev1.CreateRequest{
			ParentIno: 1,
			Name:      []byte("new-file"),
			Mode:      0644,
		}))
		require.Error(t, err)
		require.Equal(t, int32(1), atomic.LoadInt32(&attempts), "非幂等 RPC 在非建连阶段中途断连严禁重放")
		require.Equal(t, syscall.EIO, MapError(err))
	})
}

// Slice 4: 取消中断——在退避等待期间外部 context / cancel 通道取消，立即中止并返回 EINTR
func TestRetry_CancelDuringBackoff_ReturnsEINTR(t *testing.T) {
	handler := &mockStorageHandler{}
	var attempts int32
	handler.onLookup = func(ctx context.Context, req *connect.Request[storagev1.LookupRequest]) (*connect.Response[storagev1.LookupResponse], error) {
		atomic.AddInt32(&attempts, 1)
		return nil, connect.NewError(connect.CodeUnavailable, errors.New("unavailable"))
	}

	_, serverURL := startMockServer(t, handler)

	cancelChan := make(chan struct{})
	ctx, cancelFunc := ContextWithCancelChan(context.Background(), cancelChan)
	defer cancelFunc()

	client, err := NewClientFromConfig(ClientConfig{
		Endpoint: serverURL,
		RetryConfig: &RetryConfig{
			MaxElapsedTime:  10 * time.Second,
			InitialInterval: 500 * time.Millisecond, // 较长等待，便于在等待期间触发取消
			Multiplier:      2.0,
			MaxInterval:     1 * time.Second,
		},
	})
	require.NoError(t, err)

	// 异步在 50ms 后关闭 cancel 通道（模拟 Linux 内核 FUSE_INTERRUPT 信号）
	go func() {
		time.Sleep(50 * time.Millisecond)
		close(cancelChan)
	}()

	start := time.Now()
	_, err = client.Lookup(ctx, connect.NewRequest(&storagev1.LookupRequest{
		ParentIno: 1,
		Name:      []byte("interrupt.txt"),
	}))
	elapsed := time.Since(start)

	require.Error(t, err)
	require.Less(t, elapsed, 400*time.Millisecond, "收到 cancel 中断后应立即返回，不应等待完整退避周期")
	require.Equal(t, syscall.EINTR, MapErrorWithCancel(err, cancelChan), "收到中断取消必须准确映射为 EINTR")
}

// Slice 4: 预算耗尽——超出总预算仍未恢复时停止重试，准确返回 EIO 且绝不误报 EINTR
func TestRetry_BudgetExceeded_ReturnsEIO(t *testing.T) {
	handler := &mockStorageHandler{}
	var attempts int32
	handler.onLookup = func(ctx context.Context, req *connect.Request[storagev1.LookupRequest]) (*connect.Response[storagev1.LookupResponse], error) {
		atomic.AddInt32(&attempts, 1)
		return nil, connect.NewError(connect.CodeUnavailable, errors.New("always down"))
	}

	_, serverURL := startMockServer(t, handler)

	client, err := NewClientFromConfig(ClientConfig{
		Endpoint: serverURL,
		RetryConfig: &RetryConfig{
			MaxElapsedTime:  60 * time.Millisecond, // 快速耗尽预算
			InitialInterval: 10 * time.Millisecond,
			Multiplier:      2.0,
			MaxInterval:     20 * time.Millisecond,
		},
	})
	require.NoError(t, err)

	start := time.Now()
	_, err = client.Lookup(context.Background(), connect.NewRequest(&storagev1.LookupRequest{
		ParentIno: 1,
		Name:      []byte("timeout.txt"),
	}))
	elapsed := time.Since(start)

	require.Error(t, err)
	require.GreaterOrEqual(t, elapsed, 60*time.Millisecond)
	require.Greater(t, atomic.LoadInt32(&attempts), int32(1), "在预算耗尽前应至少重试过")

	// 关键契约校验：内部超时必须准确映射为 EIO，严禁伪装成 EINTR
	errno := MapError(err)
	require.Equal(t, syscall.EIO, errno, "预算耗尽必须映射为 EIO 而非 EINTR")
}

// Slice 5: 默认配置与禁用配置
func TestRetry_ConfigDefaultsAndDisable(t *testing.T) {
	t.Run("default RetryConfig values", func(t *testing.T) {
		cfg := RetryConfig{}
		normalized := cfg.withDefaults()
		require.Equal(t, 5*time.Second, normalized.MaxElapsedTime)
		require.Equal(t, 100*time.Millisecond, normalized.InitialInterval)
		require.Equal(t, 2.0, normalized.Multiplier)
		require.Equal(t, 1*time.Second, normalized.MaxInterval)
	})

	t.Run("nil RetryConfig enables retry by default", func(t *testing.T) {
		handler := &mockStorageHandler{}
		var attempts int32
		handler.onLookup = func(ctx context.Context, req *connect.Request[storagev1.LookupRequest]) (*connect.Response[storagev1.LookupResponse], error) {
			att := atomic.AddInt32(&attempts, 1)
			if att < 2 {
				return nil, connect.NewError(connect.CodeUnavailable, errors.New("brief blip"))
			}
			return connect.NewResponse(&storagev1.LookupResponse{
				Inode: &storagev1.Inode{Ino: 8888},
			}), nil
		}

		_, serverURL := startMockServer(t, handler)
		// 未指定 RetryConfig（为 nil）
		client, err := NewClientFromConfig(ClientConfig{
			Endpoint: serverURL,
		})
		require.NoError(t, err)

		resp, err := client.Lookup(context.Background(), connect.NewRequest(&storagev1.LookupRequest{
			ParentIno: 1,
			Name:      []byte("default-retry.txt"),
		}))
		require.NoError(t, err)
		require.Equal(t, uint64(8888), resp.Msg.GetInode().GetIno())
		require.Equal(t, int32(2), atomic.LoadInt32(&attempts), "默认配置下必须自动重试并成功恢复")
	})

	t.Run("disable retry", func(t *testing.T) {
		handler := &mockStorageHandler{}
		var attempts int32
		handler.onLookup = func(ctx context.Context, req *connect.Request[storagev1.LookupRequest]) (*connect.Response[storagev1.LookupResponse], error) {
			atomic.AddInt32(&attempts, 1)
			return nil, connect.NewError(connect.CodeUnavailable, errors.New("unavailable"))
		}

		_, serverURL := startMockServer(t, handler)
		client, err := NewClientFromConfig(ClientConfig{
			Endpoint: serverURL,
			RetryConfig: &RetryConfig{
				Disable: true,
			},
		})
		require.NoError(t, err)

		_, err = client.Lookup(context.Background(), connect.NewRequest(&storagev1.LookupRequest{
			ParentIno: 1,
			Name:      []byte("no-retry.txt"),
		}))
		require.Error(t, err)
		require.Equal(t, int32(1), atomic.LoadInt32(&attempts), "禁用重试时必须仅调用 1 次")
	})
}
