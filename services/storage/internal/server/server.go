package server

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"

	"connectrpc.com/connect"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/fx"

	"github.com/starsrc/pkg/observability"
	"github.com/starsrc/pkg/observability/telemetry"
	"github.com/starsrc/pkg/serverx"

	"github.com/starsrc/bowl/gen/go/storage/v1/storagev1connect"
	"github.com/starsrc/bowl/services/storage/internal/config"
	"github.com/starsrc/bowl/services/storage/internal/control"
)

var _ storagev1connect.StorageServiceHandler = (*Server)(nil)

const (
	// HTTP2MaxReadFrameSize 为服务端 HTTP/2 接收帧大小上限 (1 MiB)。
	HTTP2MaxReadFrameSize = 1 << 20

	// HTTP2MaxReceiveBufferPerStream 为服务端单流 HTTP/2 接收流控窗口上限 (8 MiB)，
	// 确保单次 4 MiB + Protobuf 信封载荷无需流级 WINDOW_UPDATE 停顿即可完整接收。
	HTTP2MaxReceiveBufferPerStream = 8 << 20

	// HTTP2MaxReceiveBufferPerConnection 为服务端单连接 HTTP/2 接收流控窗口上限 (128 MiB)，
	// 支撑客户端高并发多路 4 MiB Write 请求在单条连接上无连接级流控阻塞地流水线接收。
	HTTP2MaxReceiveBufferPerConnection = 128 << 20
)

// Server 封装并管理底层公网 RPC HTTP 服务与内网管理面 HTTP 服务。
type Server struct {
	storagev1connect.UnimplementedStorageServiceHandler
	control     control.Manager
	config      config.ServerConfig
	logger      *slog.Logger
	httpMetrics *HTTPMetrics
	httpServer  *http.Server
	adminServer *http.Server
}

// HTTPMetrics 返回公网 HTTP 全包指标收集器实例。
func (s *Server) HTTPMetrics() *HTTPMetrics {
	if s == nil {
		return nil
	}
	return s.httpMetrics
}

// Handler 返回公网 RPC HTTP 处理器实例，便于测试与中间件挂载。
func (s *Server) Handler() http.Handler {
	if s.httpServer != nil {
		return s.httpServer.Handler
	}
	return nil
}

// HTTPServer 返回底层公网 HTTP 服务器实例。
func (s *Server) HTTPServer() *http.Server {
	return s.httpServer
}

// AdminHandler 返回内网管理面 HTTP 处理器实例（未启用内网管理端口时返回 nil）。
func (s *Server) AdminHandler() http.Handler {
	if s.adminServer != nil {
		return s.adminServer.Handler
	}
	return nil
}

// AdminHTTPServer 返回底层内网管理面 HTTP 服务器实例（未启用内网管理端口时返回 nil）。
func (s *Server) AdminHTTPServer() *http.Server {
	return s.adminServer
}

// Control 返回持有的单盘控制面与 StoreActor 实例池管理器。
func (s *Server) Control() control.Manager {
	return s.control
}

// Addr 返回公网服务器监听的网络地址。
func (s *Server) Addr() string {
	if s.httpServer != nil {
		return s.httpServer.Addr
	}
	return s.config.Addr()
}

// AdminAddr 返回内网管理服务器监听的网络地址（未启用内网管理端口时返回空字符串）。
func (s *Server) AdminAddr() string {
	if s.adminServer != nil {
		return s.adminServer.Addr
	}
	if s.config.InternalServerPort > 0 {
		return s.config.InternalAddr()
	}
	return ""
}

// Params 定义 Server 初始化所需的依赖注入参数。
type Params struct {
	fx.In

	Config        config.ServerConfig
	AuthToken     config.AuthToken `optional:"true"`
	Control       control.Manager
	Observability serverx.ObservabilityConfig    `optional:"true"`
	Telemetry     telemetry.Config               `optional:"true"`
	AdminCfg      observability.AdminRouteConfig `optional:"true"`
	Gatherer      prometheus.Gatherer            `optional:"true"`
	Pool          *pgxpool.Pool                  `optional:"true"`
	ObsReady      observability.Ready            `optional:"true"`
	FineBuckets   FineBucketsReady               `optional:"true"`
	Tracing       TracingReady                   `optional:"true"`
	HTTPMetrics   *HTTPMetrics                   `optional:"true"`
	Logger        *slog.Logger                   `optional:"true"`
	Lifecycle     fx.Lifecycle                   `optional:"true"`
	Shutdowner    fx.Shutdowner                  `optional:"true"`
}

// New 构造 Server 实例，组装洋葱拦截器链、公网 ConnectRPC 服务与可选双端口内网管理服务，
// 并按需在 Fx 生命周期中注册启停钩子。
//
//nolint:gocritic // hugeParam: Uber Fx fx.In 参数结构要求按值传入构造器。
func New(p Params) (*Server, error) {
	if p.Config.Port() < 0 || p.Config.Port() > 65535 {
		return nil, fmt.Errorf("invalid server port %d: must be between 0 and 65535", p.Config.Port())
	}
	if p.Config.InternalServerPort < 0 || p.Config.InternalServerPort > 65535 {
		return nil, fmt.Errorf("invalid internal server port %d: must be between 0 and 65535", p.Config.InternalServerPort)
	}
	if p.Config.Port() > 0 && p.Config.InternalServerPort > 0 {
		if err := p.Config.ValidateDualPorts(); err != nil {
			return nil, err
		}
	}

	obsCfg := p.Observability
	if obsCfg == nil && p.Telemetry != nil {
		obsCfg = toObservabilityConfig(p.Telemetry)
	}

	interceptors, err := BuildInterceptors(obsCfg, p.Logger)
	if err != nil {
		return nil, fmt.Errorf("build interceptors: %w", err)
	}

	logger := p.Logger
	if logger == nil {
		logger = slog.Default()
	}

	httpMetrics := p.HTTPMetrics
	if httpMetrics == nil {
		httpMetrics = NewHTTPMetrics()
	}

	srv := &Server{
		control:     p.Control,
		config:      p.Config,
		logger:      logger,
		httpMetrics: httpMetrics,
	}

	mux := http.NewServeMux()
	allInterceptors := make([]connect.Interceptor, 0, len(interceptors)+1)
	allInterceptors = append(allInterceptors, newWritePayloadReleaseInterceptor())
	allInterceptors = append(allInterceptors, interceptors...)

	handlerOpts := []connect.HandlerOption{
		connect.WithCodec(newProtoBinaryCodec()),
		connect.WithInterceptors(allInterceptors...),
	}
	if p.Config.ReadMaxBytes() > 0 {
		handlerOpts = append(handlerOpts, connect.WithReadMaxBytes(int(p.Config.ReadMaxBytes())))
	}

	path, handler := storagev1connect.NewStorageServiceHandler(
		srv,
		handlerOpts...,
	)
	mux.Handle(path, handler)
	registerHandoffRoutes(mux, p.Control)

	httpServer := serverx.NewHTTPServer(p.Config.Addr(), requireBearerToken(mux, string(p.AuthToken)), p.Config)
	httpServer.Handler = WrapHTTPHandler(httpServer.Handler, httpMetrics)
	httpServer.HTTP2 = &http.HTTP2Config{
		MaxReadFrameSize:              HTTP2MaxReadFrameSize,
		MaxReceiveBufferPerStream:     HTTP2MaxReceiveBufferPerStream,
		MaxReceiveBufferPerConnection: HTTP2MaxReceiveBufferPerConnection,
	}
	srv.httpServer = httpServer

	if p.Config.InternalServerPort > 0 {
		var readyOpts []observability.AdminOption
		if p.Pool != nil {
			readyOpts = append(readyOpts, observability.WithReadyCheck("postgres", func(ctx context.Context) error {
				return p.Pool.Ping(ctx)
			}))
		}
		srv.adminServer = serverx.NewAdminServer(p.Config, p.Gatherer, p.AdminCfg, readyOpts...)
	}

	if p.Lifecycle != nil && p.Shutdowner != nil {
		if srv.adminServer != nil {
			serverx.RegisterLifecycle(
				p.Lifecycle,
				p.Shutdowner,
				p.Config,
				serverx.Listener{Name: "public", Server: httpServer},
				serverx.Listener{Name: "admin", Server: srv.adminServer},
			)
		} else {
			serverx.RegisterSingleLifecycle(p.Lifecycle, p.Shutdowner, p.Config, "server", httpServer)
		}
	}

	return srv, nil
}
