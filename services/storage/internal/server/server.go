package server

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"connectrpc.com/connect"
	"go.uber.org/fx"

	"github.com/starsrc/Aether/pkg/observability/telemetry"
	"github.com/starsrc/bowl/gen/go/storage/v1/storagev1connect"
	"github.com/starsrc/bowl/services/storage/internal/config"
	"github.com/starsrc/bowl/services/storage/internal/meta"
)

var _ storagev1connect.StorageServiceHandler = (*Server)(nil)

// Server 封装并管理底层 HTTP 服务与 ConnectRPC 路由处理器。
type Server struct {
	metaManager     meta.Manager
	config          config.ServerConfig
	logger          *slog.Logger
	handler         http.Handler
	httpServer      *http.Server
	shutdownTimeout time.Duration

	listener      net.Listener
	mu            sync.Mutex
	lifecycleOnce sync.Once
}

// Handler 返回 HTTP 处理器实例，便于测试与中间件挂载。
func (s *Server) Handler() http.Handler {
	if s.handler != nil {
		return s.handler
	}
	if s.httpServer != nil {
		return s.httpServer.Handler
	}
	return nil
}

// MetaManager 返回持有的元数据领域层管理器。
func (s *Server) MetaManager() meta.Manager {
	return s.metaManager
}

// Addr 返回服务器监听的网络地址。若已完成 Listen，优先返回实际绑定的地址（如动态分配端口）。
func (s *Server) Addr() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.listener != nil {
		return s.listener.Addr().String()
	}
	if s.httpServer != nil {
		return s.httpServer.Addr
	}
	return s.config.Addr()
}

// Params 定义 Server 初始化所需的依赖注入参数。
type Params struct {
	fx.In

	Config         config.ServerConfig
	MetaManager    meta.Manager
	Telemetry      telemetry.Config             `optional:"true"`
	TelemetryReady telemetry.Ready              `optional:"true"`
	LogInterceptor connect.UnaryInterceptorFunc `optional:"true"`
	Logger         *slog.Logger                 `optional:"true"`
	Lifecycle      fx.Lifecycle                 `optional:"true"`
	Shutdowner     fx.Shutdowner                `optional:"true"`
}

// New 构造 Server 实例，组装洋葱拦截器链与 ConnectRPC 处理器，并按需在 Fx 生命周期中注册启停钩子。
func New(p Params) (*Server, error) {
	if p.Config.Port < 0 || p.Config.Port > 65535 {
		return nil, fmt.Errorf("invalid server port %d: must be between 0 and 65535", p.Config.Port)
	}

	interceptors, err := buildInterceptors(p.Telemetry, p.LogInterceptor, p.Logger)
	if err != nil {
		return nil, fmt.Errorf("build interceptors: %w", err)
	}

	srv := &Server{
		metaManager:     p.MetaManager,
		config:          p.Config,
		logger:          p.Logger,
		shutdownTimeout: p.Config.ShutdownTimeout(),
	}

	mux := http.NewServeMux()
	path, handler := storagev1connect.NewStorageServiceHandler(
		srv,
		connect.WithInterceptors(interceptors...),
		connect.WithReadMaxBytes(int(p.Config.ReadMaxBytes())),
	)
	mux.Handle(path, handler)

	// 底层启用 HTTP/1.1 与非加密 HTTP/2 (h2c)
	var protocols http.Protocols
	protocols.SetHTTP1(true)
	protocols.SetUnencryptedHTTP2(true)

	rootHandler := http.MaxBytesHandler(mux, p.Config.ReadMaxBytes())

	httpServer := &http.Server{
		Addr:              p.Config.Addr(),
		Handler:           rootHandler,
		Protocols:         &protocols,
		ReadHeaderTimeout: p.Config.ReadHeaderTimeout(),
	}

	srv.handler = rootHandler
	srv.httpServer = httpServer

	if p.Lifecycle != nil && p.Shutdowner != nil {
		srv.registerLifecycle(p.Lifecycle, p.Shutdowner)
	}

	return srv, nil
}

// registerLifecycle 向 Fx 生命周期注册异步监听与优雅停机钩子。
func (s *Server) registerLifecycle(lc fx.Lifecycle, shutdowner fx.Shutdowner) {
	if lc == nil {
		return
	}

	s.lifecycleOnce.Do(func() {
		var (
			stopping  atomic.Bool
			serveErr  error
			serveDone = make(chan struct{})
		)

		lc.Append(fx.Hook{
			OnStart: func(ctx context.Context) error {
				s.mu.Lock()
				ln, err := net.Listen("tcp", s.httpServer.Addr)
				if err != nil {
					s.mu.Unlock()
					return fmt.Errorf("listen %s: %w", s.httpServer.Addr, err)
				}
				s.listener = ln
				s.mu.Unlock()

				go func() {
					defer close(serveDone)
					err := s.httpServer.Serve(ln)
					if err != nil && !errors.Is(err, http.ErrServerClosed) {
						serveErr = fmt.Errorf("server error: %w", err)
					}
					if !stopping.Load() && shutdowner != nil {
						var opts []fx.ShutdownOption
						if serveErr != nil {
							opts = append(opts, fx.ExitCode(1))
						}
						if shutErr := shutdowner.Shutdown(opts...); shutErr != nil {
							serveErr = errors.Join(serveErr, fmt.Errorf("request application shutdown: %w", shutErr))
						}
					}
				}()

				return nil
			},
			OnStop: func(ctx context.Context) error {
				stopping.Store(true)

				shutdownCtx := ctx
				if s.shutdownTimeout > 0 {
					var cancel context.CancelFunc
					shutdownCtx, cancel = context.WithTimeout(ctx, s.shutdownTimeout)
					defer cancel()
				}

				var shutdownErr error
				if err := s.httpServer.Shutdown(shutdownCtx); err != nil {
					shutdownErr = fmt.Errorf("server shutdown: %w", err)
					if closeErr := s.httpServer.Close(); closeErr != nil {
						shutdownErr = errors.Join(shutdownErr, fmt.Errorf("server close: %w", closeErr))
					}
				}
				<-serveDone

				return errors.Join(serveErr, shutdownErr)
			},
		})
	})
}
