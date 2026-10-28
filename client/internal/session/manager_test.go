package session_test

import (
	"context"
	"errors"
	"log/slog"
	"math"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"connectrpc.com/connect"
	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
	"github.com/starsrc/bowl/gen/go/storage/v1/storagev1connect"
	"github.com/stretchr/testify/require"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/starsrc/bowl/client/internal/session"
)

type mockStorageHandler struct {
	storagev1connect.UnimplementedStorageServiceHandler

	mu                sync.Mutex
	renewSessionCalls []uint64
	renewSessionFunc  func(ctx context.Context, req *connect.Request[storagev1.RenewSessionRequest]) (*connect.Response[storagev1.RenewSessionResponse], error)
}

func (m *mockStorageHandler) RenewSession(ctx context.Context, req *connect.Request[storagev1.RenewSessionRequest]) (*connect.Response[storagev1.RenewSessionResponse], error) {
	m.mu.Lock()
	m.renewSessionCalls = append(m.renewSessionCalls, req.Msg.GetSessionId())
	fn := m.renewSessionFunc
	m.mu.Unlock()

	if fn != nil {
		return fn(ctx, req)
	}

	return connect.NewResponse(&storagev1.RenewSessionResponse{
		ExpiresAt:        timestamppb.New(time.Now().Add(60 * time.Second)),
		SessionRecreated: false,
	}), nil
}

func (m *mockStorageHandler) getCalls() []uint64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	res := make([]uint64, len(m.renewSessionCalls))
	copy(res, m.renewSessionCalls)
	return res
}

func setupTestServer(t *testing.T, handler storagev1connect.StorageServiceHandler) (storagev1connect.StorageServiceClient, func()) {
	t.Helper()
	mux := http.NewServeMux()
	path, srvHandler := storagev1connect.NewStorageServiceHandler(handler)
	mux.Handle(path, srvHandler)

	server := httptest.NewServer(h2c.NewHandler(mux, &http2.Server{}))
	client := storagev1connect.NewStorageServiceClient(server.Client(), server.URL)

	return client, func() {
		server.Close()
	}
}

func TestManager_InitialActivation(t *testing.T) {
	t.Run("fails when initial RenewSession RPC returns error", func(t *testing.T) {
		handler := &mockStorageHandler{
			renewSessionFunc: func(ctx context.Context, req *connect.Request[storagev1.RenewSessionRequest]) (*connect.Response[storagev1.RenewSessionResponse], error) {
				return nil, connect.NewError(connect.CodeUnavailable, errors.New("storage service unavailable"))
			},
		}
		client, cleanup := setupTestServer(t, handler)
		defer cleanup()

		mgr, err := session.New(context.Background(), client, session.Config{})
		require.Error(t, err)
		require.Nil(t, mgr)
		require.Contains(t, err.Error(), "activate initial session")
	})

	t.Run("fails when client is nil", func(t *testing.T) {
		mgr, err := session.New(context.Background(), nil, session.Config{})
		require.Error(t, err)
		require.Nil(t, mgr)
		require.Contains(t, err.Error(), "storage client is nil")
	})

	t.Run("succeeds with 63-bit session ID and registers session on server", func(t *testing.T) {
		handler := &mockStorageHandler{
			renewSessionFunc: func(ctx context.Context, req *connect.Request[storagev1.RenewSessionRequest]) (*connect.Response[storagev1.RenewSessionResponse], error) {
				sid := req.Msg.GetSessionId()
				require.GreaterOrEqual(t, sid, uint64(1))
				require.LessOrEqual(t, sid, uint64(math.MaxInt64))
				return connect.NewResponse(&storagev1.RenewSessionResponse{
					ExpiresAt:        timestamppb.New(time.Now().Add(30 * time.Second)),
					SessionRecreated: true, // 初始首次创建，服务端返回 true 属正常现象
				}), nil
			},
		}
		client, cleanup := setupTestServer(t, handler)
		defer cleanup()

		mgr, err := session.New(context.Background(), client, session.Config{})
		require.NoError(t, err)
		require.NotNil(t, mgr)
		defer func() { _ = mgr.Close() }()

		curID := mgr.CurrentSessionID()
		require.GreaterOrEqual(t, curID, uint64(1))
		require.LessOrEqual(t, curID, uint64(math.MaxInt64))

		calls := handler.getCalls()
		require.Len(t, calls, 1)
		require.Equal(t, curID, calls[0])
	})
}

func TestManager_HandleSessionBinding(t *testing.T) {
	handler := &mockStorageHandler{}
	client, cleanup := setupTestServer(t, handler)
	defer cleanup()

	mgr, err := session.New(context.Background(), client, session.Config{})
	require.NoError(t, err)
	defer func() { _ = mgr.Close() }()

	curSID := mgr.CurrentSessionID()

	t.Run("lookup and resolve unbound handle returns EBADF", func(t *testing.T) {
		sid, ok := mgr.LookupHandleSession(9999)
		require.False(t, ok)
		require.Equal(t, uint64(0), sid)

		resSID, err := mgr.ResolveHandleSession(9999)
		require.ErrorIs(t, err, session.ErrBadFileHandle)
		require.Equal(t, uint64(0), resSID)
	})

	t.Run("bind, lookup, resolve, and unbind lifecycle", func(t *testing.T) {
		fh := uint64(1001)
		mgr.BindHandle(fh, curSID)

		sid, ok := mgr.LookupHandleSession(fh)
		require.True(t, ok)
		require.Equal(t, curSID, sid)

		resSID, err := mgr.ResolveHandleSession(fh)
		require.NoError(t, err)
		require.Equal(t, curSID, resSID)

		// 释放句柄
		mgr.UnbindHandle(fh)

		sidAfter, okAfter := mgr.LookupHandleSession(fh)
		require.False(t, okAfter)
		require.Equal(t, uint64(0), sidAfter)

		resAfter, errAfter := mgr.ResolveHandleSession(fh)
		require.ErrorIs(t, errAfter, session.ErrBadFileHandle)
		require.Equal(t, uint64(0), resAfter)
	})

	t.Run("concurrent handle operations are thread-safe", func(t *testing.T) {
		const goroutines = 20
		const opsPerGoroutine = 50

		var wg sync.WaitGroup
		wg.Add(goroutines)

		for i := 0; i < goroutines; i++ {
			go func(baseFH uint64) {
				defer wg.Done()
				for j := uint64(0); j < opsPerGoroutine; j++ {
					fh := baseFH*1000 + j
					mgr.BindHandle(fh, curSID)
					sid, ok := mgr.LookupHandleSession(fh)
					if ok && sid != curSID {
						t.Errorf("expected sid %d, got %d", curSID, sid)
					}
					_, _ = mgr.ResolveHandleSession(fh)
					mgr.UnbindHandle(fh)
				}
			}(uint64(i + 1))
		}

		wg.Wait()
	})
}

func TestManager_AdaptiveRenewLoop(t *testing.T) {
	t.Run("periodically renews session at 1/3 lease interval and terminates cleanly on Close", func(t *testing.T) {
		renewCount := 0
		var mu sync.Mutex

		handler := &mockStorageHandler{
			renewSessionFunc: func(ctx context.Context, req *connect.Request[storagev1.RenewSessionRequest]) (*connect.Response[storagev1.RenewSessionResponse], error) {
				mu.Lock()
				renewCount++
				mu.Unlock()
				// 每次返回 120ms 后过期，1/3 即 40ms 续期一次
				return connect.NewResponse(&storagev1.RenewSessionResponse{
					ExpiresAt:        timestamppb.New(time.Now().Add(120 * time.Millisecond)),
					SessionRecreated: false,
				}), nil
			},
		}
		client, cleanup := setupTestServer(t, handler)
		defer cleanup()

		cfg := session.Config{
			MinInterval:         20 * time.Millisecond,
			MaxInterval:         100 * time.Millisecond,
			InitialRetryBackoff: 20 * time.Millisecond,
			MaxRetryBackoff:     50 * time.Millisecond,
		}

		mgr, err := session.New(context.Background(), client, cfg)
		require.NoError(t, err)
		defer func() { _ = mgr.Close() }()

		err = mgr.Start(context.Background())
		require.NoError(t, err)

		// 重复 Start 返回错误
		err = mgr.Start(context.Background())
		require.Error(t, err)
		require.Contains(t, err.Error(), "already started")

		// 等待 150ms，应该触发至少 2 次以上续期（初始 1 次 + 后台续期多次）
		require.Eventually(t, func() bool {
			mu.Lock()
			defer mu.Unlock()
			return renewCount >= 3
		}, 5*time.Second, 10*time.Millisecond)

		// 验证 Close 优雅退出
		err = mgr.Close()
		require.NoError(t, err)

		mu.Lock()
		countAtClose := renewCount
		mu.Unlock()

		// 等待 50ms 确认后台协程已停止，续期次数不再增加
		time.Sleep(50 * time.Millisecond)
		mu.Lock()
		countAfterWait := renewCount
		mu.Unlock()
		require.Equal(t, countAtClose, countAfterWait, "renewLoop must stop after Close()")

		// 幂等 Close
		require.NoError(t, mgr.Close())
	})

	t.Run("retries with backoff on transient network error without crashing", func(t *testing.T) {
		var mu sync.Mutex
		attempts := 0

		handler := &mockStorageHandler{
			renewSessionFunc: func(ctx context.Context, req *connect.Request[storagev1.RenewSessionRequest]) (*connect.Response[storagev1.RenewSessionResponse], error) {
				mu.Lock()
				attempts++
				curAttempt := attempts
				mu.Unlock()

				// 第 1 次是初始激活（成功）
				if curAttempt == 1 {
					return connect.NewResponse(&storagev1.RenewSessionResponse{
						ExpiresAt:        timestamppb.New(time.Now().Add(90 * time.Millisecond)),
						SessionRecreated: false,
					}), nil
				}
				// 第 2 次模拟网络瞬断返回 Unavailable
				if curAttempt == 2 {
					return nil, connect.NewError(connect.CodeUnavailable, errors.New("network glitch"))
				}
				// 第 3 次重试成功
				return connect.NewResponse(&storagev1.RenewSessionResponse{
					ExpiresAt:        timestamppb.New(time.Now().Add(300 * time.Millisecond)),
					SessionRecreated: false,
				}), nil
			},
		}
		client, cleanup := setupTestServer(t, handler)
		defer cleanup()

		cfg := session.Config{
			MinInterval:         20 * time.Millisecond,
			MaxInterval:         100 * time.Millisecond,
			InitialRetryBackoff: 20 * time.Millisecond,
			MaxRetryBackoff:     50 * time.Millisecond,
		}

		mgr, err := session.New(context.Background(), client, cfg)
		require.NoError(t, err)

		err = mgr.Start(context.Background())
		require.NoError(t, err)

		// 重复 Start 返回错误
		err = mgr.Start(context.Background())
		require.Error(t, err)
		require.Contains(t, err.Error(), "already started")
		defer func() { _ = mgr.Close() }()

		require.Eventually(t, func() bool {
			mu.Lock()
			defer mu.Unlock()
			return attempts >= 3
		}, 5*time.Second, 10*time.Millisecond)
	})
}

// logCaptureHandler 记录所有 slog 输出以便单测断言日志告警
type logCaptureHandler struct {
	mu      sync.Mutex
	records []slog.Record
}

func (h *logCaptureHandler) Enabled(_ context.Context, _ slog.Level) bool {
	return true
}

func (h *logCaptureHandler) Handle(_ context.Context, r slog.Record) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.records = append(h.records, r.Clone())
	return nil
}

func (h *logCaptureHandler) WithAttrs(_ []slog.Attr) slog.Handler {
	return h
}

func (h *logCaptureHandler) WithGroup(_ string) slog.Handler {
	return h
}

func (h *logCaptureHandler) hasWarnLog(msgSubstr string) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, r := range h.records {
		if r.Level >= slog.LevelWarn && strings.Contains(r.Message, msgSubstr) {
			return true
		}
	}
	return false
}

func TestManager_SessionRecreated_RotationAndHandleIsolation(t *testing.T) {
	var mu sync.Mutex
	calls := make([]uint64, 0)
	recreatedReturned := false

	handler := &mockStorageHandler{
		renewSessionFunc: func(ctx context.Context, req *connect.Request[storagev1.RenewSessionRequest]) (*connect.Response[storagev1.RenewSessionResponse], error) {
			mu.Lock()
			defer mu.Unlock()
			sid := req.Msg.GetSessionId()
			calls = append(calls, sid)

			// 初始创建
			if len(calls) == 1 {
				return connect.NewResponse(&storagev1.RenewSessionResponse{
					ExpiresAt:        timestamppb.New(time.Now().Add(100 * time.Millisecond)),
					SessionRecreated: false,
				}), nil
			}

			// 第 2 次续期模拟服务端会话超期被 sweep 清理后重新 upsert（SessionRecreated == true）
			if !recreatedReturned {
				recreatedReturned = true
				return connect.NewResponse(&storagev1.RenewSessionResponse{
					ExpiresAt:        timestamppb.New(time.Now().Add(100 * time.Millisecond)),
					SessionRecreated: true,
				}), nil
			}

			// 轮换新 ID 后的正常续期
			return connect.NewResponse(&storagev1.RenewSessionResponse{
				ExpiresAt:        timestamppb.New(time.Now().Add(200 * time.Millisecond)),
				SessionRecreated: false,
			}), nil
		},
	}
	client, cleanup := setupTestServer(t, handler)
	defer cleanup()

	logCapture := &logCaptureHandler{}
	logger := slog.New(logCapture)

	cfg := session.Config{
		MinInterval:         20 * time.Millisecond,
		MaxInterval:         100 * time.Millisecond,
		InitialRetryBackoff: 20 * time.Millisecond,
		MaxRetryBackoff:     50 * time.Millisecond,
		Logger:              logger,
	}

	mgr, err := session.New(context.Background(), client, cfg)
	require.NoError(t, err)

	oldSID := mgr.CurrentSessionID()

	// 断网前打开一个旧句柄
	oldFH := uint64(501)
	mgr.BindHandle(oldFH, oldSID)

	// 此时旧句柄有效
	resSID, err := mgr.ResolveHandleSession(oldFH)
	require.NoError(t, err)
	require.Equal(t, oldSID, resSID)

	// 启动后台续期
	err = mgr.Start(context.Background())
	require.NoError(t, err)
	defer func() { _ = mgr.Close() }()

	// 等待发生会话轮换（活跃 sessionID 发生变化且不等于 oldSID）
	require.Eventually(t, func() bool {
		cur := mgr.CurrentSessionID()
		return cur != oldSID
	}, 5*time.Second, 10*time.Millisecond)

	newSID := mgr.CurrentSessionID()
	require.GreaterOrEqual(t, newSID, uint64(1))
	require.LessOrEqual(t, newSID, uint64(math.MaxInt64))
	require.NotEqual(t, oldSID, newSID)

	// 1. 验证输出明确的 WARN 告警日志
	require.True(t, logCapture.hasWarnLog("client session expired on server; rotating session_id"), "必须输出会话超期轮换警告日志")

	// 2. 验证断网前打开的老句柄确定性返回 EBADF，绝不借用新会话续命
	oldResSID, err := mgr.ResolveHandleSession(oldFH)
	require.ErrorIs(t, err, session.ErrBadFileHandle, "老句柄解析必须返回 EBADF")
	require.Equal(t, oldSID, oldResSID, "老句柄解析应返回其创建时的原始 sessionID")

	// 验证 LookupHandleSession 仍然保留打开时的原始 ID
	lookupSID, ok := mgr.LookupHandleSession(oldFH)
	require.True(t, ok)
	require.Equal(t, oldSID, lookupSID)

	// 3. 验证新句柄在轮换后绑定新 session_id 并自愈可用
	newFH := uint64(502)
	mgr.BindHandle(newFH, newSID)
	activeResSID, err := mgr.ResolveHandleSession(newFH)
	require.NoError(t, err)
	require.Equal(t, newSID, activeResSID)

	// 4. 验证 UnbindHandle 清理老句柄后不再查到
	mgr.UnbindHandle(oldFH)
	_, okAfterUnbind := mgr.LookupHandleSession(oldFH)
	require.False(t, okAfterUnbind)
}

func TestManager_SessionRecreated_RetryOnRotationFailure(t *testing.T) {
	var mu sync.Mutex
	calls := make([]uint64, 0)
	rotationAttempts := 0

	handler := &mockStorageHandler{
		renewSessionFunc: func(ctx context.Context, req *connect.Request[storagev1.RenewSessionRequest]) (*connect.Response[storagev1.RenewSessionResponse], error) {
			mu.Lock()
			defer mu.Unlock()
			sid := req.Msg.GetSessionId()
			calls = append(calls, sid)

			// 初始激活
			if len(calls) == 1 {
				return connect.NewResponse(&storagev1.RenewSessionResponse{
					ExpiresAt:        timestamppb.New(time.Now().Add(100 * time.Millisecond)),
					SessionRecreated: false,
				}), nil
			}

			// 第 2 次续期返回 session_recreated = true 触发轮换
			if len(calls) == 2 {
				return connect.NewResponse(&storagev1.RenewSessionResponse{
					ExpiresAt:        timestamppb.New(time.Now().Add(100 * time.Millisecond)),
					SessionRecreated: true,
				}), nil
			}

			// 轮换激活新 ID 阶段：第一次失败（模拟瞬态网络故障），第二次成功
			rotationAttempts++
			if rotationAttempts == 1 {
				return nil, connect.NewError(connect.CodeUnavailable, errors.New("network glitch during rotation"))
			}

			return connect.NewResponse(&storagev1.RenewSessionResponse{
				ExpiresAt:        timestamppb.New(time.Now().Add(200 * time.Millisecond)),
				SessionRecreated: false,
			}), nil
		},
	}
	client, cleanup := setupTestServer(t, handler)
	defer cleanup()

	cfg := session.Config{
		MinInterval:         20 * time.Millisecond,
		MaxInterval:         100 * time.Millisecond,
		InitialRetryBackoff: 20 * time.Millisecond,
		MaxRetryBackoff:     50 * time.Millisecond,
	}

	mgr, err := session.New(context.Background(), client, cfg)
	require.NoError(t, err)

	initialSID := mgr.CurrentSessionID()

	err = mgr.Start(context.Background())
	require.NoError(t, err)
	defer func() { _ = mgr.Close() }()

	// 等待最终成功轮换
	require.Eventually(t, func() bool {
		return mgr.CurrentSessionID() != initialSID
	}, 5*time.Second, 10*time.Millisecond)

	mu.Lock()
	defer mu.Unlock()
	require.GreaterOrEqual(t, rotationAttempts, 2, "轮换激活应发生至少一次重试后成功")
}
