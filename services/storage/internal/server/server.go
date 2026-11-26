package server

import (
	"fmt"
	"log/slog"
	"net/http"

	"connectrpc.com/connect"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/starsrc/pkg/observability"
	"github.com/starsrc/pkg/observability/telemetry"

	"github.com/starsrc/bowl/gen/go/storage/v1/storagev1connect"
	"github.com/starsrc/bowl/services/storage/internal/config"
	"github.com/starsrc/bowl/services/storage/internal/engine"
)

var _ storagev1connect.StorageServiceHandler = (*Server)(nil)

const (
	HTTP2MaxReadFrameSize = 1 << 20

	HTTP2MaxReceiveBufferPerStream = 8 << 20

	HTTP2MaxReceiveBufferPerConnection = 128 << 20
)

type Server struct {
	node        *engine.NodeStore
	metaCfg     config.MetaConfig
	config      config.ServerConfig
	logger      *slog.Logger
	httpMetrics *HTTPMetrics
	httpServer  *http.Server
	adminServer *http.Server
}

func (s *Server) HTTPMetrics() *HTTPMetrics {
	if s == nil {
		return nil
	}
	return s.httpMetrics
}

func (s *Server) Handler() http.Handler {
	if s.httpServer != nil {
		return s.httpServer.Handler
	}
	return nil
}

func (s *Server) HTTPServer() *http.Server {
	return s.httpServer
}

func (s *Server) AdminHandler() http.Handler {
	if s.adminServer != nil {
		return s.adminServer.Handler
	}
	return nil
}

func (s *Server) AdminHTTPServer() *http.Server {
	return s.adminServer
}

func (s *Server) Node() *engine.NodeStore {
	return s.node
}

func (s *Server) Addr() string {
	if s.httpServer != nil {
		return s.httpServer.Addr
	}
	return s.config.Addr()
}

func (s *Server) AdminAddr() string {
	if s.adminServer != nil {
		return s.adminServer.Addr
	}
	if s.config.InternalServerPort > 0 {
		return s.config.InternalAddr()
	}
	return ""
}

func (s *Server) entryTimeoutForMode(mode uint32) float64 {
	if mode&engine.ModeTypeMask == engine.ModeDir {
		return s.metaCfg.DirEntryTimeoutSeconds()
	}
	return s.metaCfg.EntryTimeoutSeconds()
}

func (s *Server) attrTimeoutSeconds() float64 {
	return s.metaCfg.AttrTimeoutSeconds()
}

func (s *Server) leaseTTLSeconds() uint32 {
	if s.node == nil {
		return uint32(engine.DefaultMountLeaseTTL.Seconds())
	}
	secs := int64(s.node.MountLeaseTTL().Seconds())
	if secs <= 0 {
		return uint32(engine.DefaultMountLeaseTTL.Seconds())
	}
	return uint32(secs)
}

type Params struct {
	Config        config.ServerConfig
	MetaCfg       *config.MetaConfig
	MetaVal       config.MetaConfig
	Node          *engine.NodeStore
	Observability ObservabilityConfig
	Telemetry     telemetry.Config
	AdminCfg      observability.AdminRouteConfig
	Gatherer      prometheus.Gatherer
	HTTPMetrics   *HTTPMetrics
	Logger        *slog.Logger
}

func defaultMetaConfig() config.MetaConfig {
	return config.MetaConfig{
		AtimeModeVal:              config.DefaultAtimeMode,
		EntryTimeoutSecondsVal:    config.DefaultEntryTimeoutSeconds,
		AttrTimeoutSecondsVal:     config.DefaultAttrTimeoutSeconds,
		DirEntryTimeoutSecondsVal: config.DefaultDirEntryTimeoutSeconds,
		OpenKeepCacheVal:          config.DefaultOpenKeepCache,
		OpenDirCacheVal:           config.DefaultOpenDirCache,
		DiskIdleTTLVal:            config.DefaultDiskIdleTTL,
	}
}

func newHTTPServer(addr string, handler http.Handler, cfg config.ServerConfig) *http.Server {
	var protocols http.Protocols
	protocols.SetHTTP1(true)
	protocols.SetUnencryptedHTTP2(true)

	return &http.Server{
		Addr:              addr,
		Handler:           http.MaxBytesHandler(handler, cfg.ReadMaxBytes()),
		Protocols:         &protocols,
		ReadHeaderTimeout: cfg.ReadHeaderTimeout(),
	}
}

func newAdminServer(
	cfg config.ServerConfig,
	gatherer prometheus.Gatherer,
	adminCfg observability.AdminRouteConfig,
	opts ...observability.AdminOption,
) *http.Server {
	addr := cfg.InternalServerAddress()
	if addr == "" {
		addr = "0.0.0.0:8090"
	}
	mux := http.NewServeMux()
	observability.RegisterAdminRoutes(mux, gatherer, adminCfg, opts...)
	return newHTTPServer(addr, mux, cfg)
}

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

	metaCfg := defaultMetaConfig()
	switch {
	case p.MetaCfg != nil:
		metaCfg = *p.MetaCfg
	case p.MetaVal != (config.MetaConfig{}):
		metaCfg = p.MetaVal
	}

	srv := &Server{
		node:        p.Node,
		metaCfg:     metaCfg,
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

	httpServer := newHTTPServer(p.Config.Addr(), mux, p.Config)
	httpServer.Handler = WrapHTTPHandler(httpServer.Handler, httpMetrics)
	httpServer.HTTP2 = &http.HTTP2Config{
		MaxReadFrameSize:              HTTP2MaxReadFrameSize,
		MaxReceiveBufferPerStream:     HTTP2MaxReceiveBufferPerStream,
		MaxReceiveBufferPerConnection: HTTP2MaxReceiveBufferPerConnection,
	}
	srv.httpServer = httpServer

	if p.Config.InternalServerPort > 0 {
		var readyOpts []observability.AdminOption
		if p.Node != nil {
			readyOpts = append(readyOpts, observability.WithReadyCheck("storage", p.Node.Ready))
		}
		srv.adminServer = newAdminServer(p.Config, p.Gatherer, p.AdminCfg, readyOpts...)
	}

	return srv, nil
}
