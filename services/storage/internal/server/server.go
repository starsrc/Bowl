package server

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"

	"connectrpc.com/connect"
	"go.uber.org/fx"

	"github.com/starsrc/Aether/pkg/observability/telemetry"
	"github.com/starsrc/Aether/pkg/serverx"
	"github.com/starsrc/bowl/gen/go/storage/v1/storagev1connect"
	"github.com/starsrc/bowl/pkg/validator"
	"github.com/starsrc/bowl/services/storage/internal/config"
	"github.com/starsrc/bowl/services/storage/internal/meta"
)

type responseControllerKey struct{}

func withResponseController(ctx context.Context, w http.ResponseWriter) context.Context {
	return context.WithValue(ctx, responseControllerKey{}, http.NewResponseController(w))
}

func responseControllerFromContext(ctx context.Context) *http.ResponseController {
	if ctx == nil {
		return nil
	}
	rc, _ := ctx.Value(responseControllerKey{}).(*http.ResponseController)
	return rc
}

var _ storagev1connect.StorageServiceHandler = (*Server)(nil)

// Server 封装并管理底层 HTTP 服务与 ConnectRPC 路由处理器。
type Server struct {
	metaManager meta.Manager
	config      config.ServerConfig
	logger      *slog.Logger
	httpServer  *http.Server
}

// Handler 返回 HTTP 处理器实例，便于测试与中间件挂载。
func (s *Server) Handler() http.Handler {
	if s.httpServer != nil {
		return s.httpServer.Handler
	}
	return nil
}

// MetaManager 返回持有的元数据领域层管理器。
func (s *Server) MetaManager() meta.Manager {
	return s.metaManager
}

// Addr 返回服务器监听的网络地址。
func (s *Server) Addr() string {
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
	Observability  serverx.ObservabilityConfig  `optional:"true"`
	Telemetry      telemetry.Config             `optional:"true"`
	TelemetryReady telemetry.Ready              `optional:"true"`
	LogInterceptor connect.UnaryInterceptorFunc `optional:"true"`
	Logger         *slog.Logger                 `optional:"true"`
	Lifecycle      fx.Lifecycle                 `optional:"true"`
	Shutdowner     fx.Shutdowner                `optional:"true"`
}

// New 构造 Server 实例，组装洋葱拦截器链与 ConnectRPC 处理器，并按需在 Fx 生命周期中注册启停钩子。
func New(p Params) (*Server, error) {
	if p.Config.Port() < 0 || p.Config.Port() > 65535 {
		return nil, fmt.Errorf("invalid server port %d: must be between 0 and 65535", p.Config.Port())
	}

	obsCfg := p.Observability
	if obsCfg == nil && p.Telemetry != nil {
		obsCfg = toObservabilityConfig(p.Telemetry)
	}

	val, err := validator.NewStorageValidator()
	if err != nil {
		return nil, fmt.Errorf("create storage validator: %w", err)
	}
	valInterceptor := NewValidationInterceptor(val)

	var extra []connect.Interceptor
	if p.LogInterceptor != nil {
		extra = append(extra, p.LogInterceptor)
	}

	interceptors, err := serverx.NewInterceptorsWithValidator(
		p.Logger,
		obsCfg,
		valInterceptor,
		extra...,
	)
	if err != nil {
		return nil, fmt.Errorf("build interceptors: %w", err)
	}

	srv := &Server{
		metaManager: p.MetaManager,
		config:      p.Config,
		logger:      p.Logger,
	}

	mux := http.NewServeMux()
	var handlerOpts []connect.HandlerOption
	handlerOpts = append(handlerOpts, connect.WithInterceptors(interceptors...))
	if p.Config.ReadMaxBytes() > 0 {
		handlerOpts = append(handlerOpts, connect.WithReadMaxBytes(int(p.Config.ReadMaxBytes())))
	}

	path, handler := storagev1connect.NewStorageServiceHandler(
		srv,
		handlerOpts...,
	)
	mux.Handle(path, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		handler.ServeHTTP(w, r.WithContext(withResponseController(r.Context(), w)))
	}))

	httpServer := serverx.NewHTTPServer(p.Config.Addr(), mux, p.Config)
	srv.httpServer = httpServer

	if p.Lifecycle != nil && p.Shutdowner != nil {
		serverx.RegisterSingleLifecycle(p.Lifecycle, p.Shutdowner, p.Config, "server", httpServer)
	}

	return srv, nil
}
