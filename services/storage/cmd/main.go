package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"slices"
	"sync"
	"syscall"
	"time"

	"github.com/starsrc/pkg/observability"

	"github.com/starsrc/bowl/services/storage/internal/config"
	"github.com/starsrc/bowl/services/storage/internal/projection"
	"github.com/starsrc/bowl/services/storage/internal/server"
)

type namedServer struct {
	name   string
	server *http.Server
}

type boundServer struct {
	name     string
	server   *http.Server
	listener net.Listener
}

type cleanupHook struct {
	name string
	fn   func(context.Context) error
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)

	if err := run(ctx); err != nil {
		stop()
		slog.Error("service terminated with error", "error", err)
		os.Exit(1)
	}
	stop()
}

func run(ctx context.Context) error {
	cfg, err := config.Load()
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}
	return runWithConfig(ctx, cfg)
}

func runWithConfig(ctx context.Context, cfg *config.Config) error {
	var cleanups []cleanupHook
	addCleanup := func(name string, fn func(context.Context) error) {
		cleanups = append(cleanups, cleanupHook{name: name, fn: fn})
	}
	abort := func(cause error) error {
		stopCtx, cancel := context.WithTimeout(context.Background(), cfg.StopTimeout())
		defer cancel()
		return errors.Join(cause, runCleanups(stopCtx, cleanups))
	}

	obsRT, err := observability.Setup(ctx, config.NewObservabilityConfig(&cfg.EnvConfig))
	if err != nil {
		return fmt.Errorf("setup observability: %w", err)
	}
	addCleanup("observability", obsRT.Shutdown)

	mp, err := server.ConfigureFineBucketMeterProvider(obsRT, &cfg.TelemetryConfig, cfg)
	if err != nil {
		return abort(fmt.Errorf("configure fine-bucket meter provider: %w", err))
	}
	if mp != nil {
		addCleanup("meter provider", mp.Shutdown)
	}

	tp, err := server.ConfigureStorageTracerProvider(ctx, obsRT, &cfg.TelemetryConfig)
	if err != nil {
		return abort(fmt.Errorf("configure storage tracer provider: %w", err))
	}
	if tp != nil {
		addCleanup("tracer provider", tp.Shutdown)
	}

	pool, err := projection.OpenPool(cfg.PoolConfig)
	if err != nil {
		return abort(fmt.Errorf("open db pool: %w", err))
	}
	addCleanup("db pool", func(context.Context) error {
		pool.Close()
		return nil
	})

	if pingErr := pool.Ping(ctx); pingErr != nil {
		return abort(fmt.Errorf("ping database: %w", pingErr))
	}

	mgr, err := projection.ProvideControlManager(projection.ControlManagerParams{
		Store:   projection.ProvideStore(pool),
		MetaCfg: &cfg.Meta,
		Logger:  obsRT.Logger(),
	})
	if err != nil {
		return abort(fmt.Errorf("initialize control manager: %w", err))
	}
	addCleanup("control manager", mgr.Close)

	httpMetrics := server.NewHTTPMetrics()
	storageCollector := projection.NewStorageMetricsCollector(mgr)
	if regErr := obsRT.RegisterCollector(httpMetrics, storageCollector); regErr != nil {
		return abort(fmt.Errorf("register metrics collectors: %w", regErr))
	}

	srv, err := server.New(server.Params{
		Config:        cfg.ServerConfig,
		MetaCfg:       &cfg.Meta,
		Manager:       mgr,
		Observability: cfg,
		Telemetry:     &cfg.TelemetryConfig,
		AdminCfg:      cfg,
		Gatherer:      obsRT.Gatherer(),
		Pool:          pool,
		HTTPMetrics:   httpMetrics,
		Logger:        obsRT.Logger(),
	})
	if err != nil {
		return abort(fmt.Errorf("create storage server: %w", err))
	}

	servers := []namedServer{
		{name: "public", server: srv.HTTPServer()},
	}
	if srv.AdminHTTPServer() != nil {
		servers = append(servers, namedServer{name: "admin", server: srv.AdminHTTPServer()})
	}

	return runManagedServers(ctx, cfg.ShutdownTimeout(), cfg.StopTimeout(), servers, cleanups, obsRT.Logger())
}

func bindListeners(servers []namedServer) ([]boundServer, error) {
	bound := make([]boundServer, 0, len(servers))
	for _, item := range servers {
		if item.server == nil {
			continue
		}
		ln, err := net.Listen("tcp", item.server.Addr)
		if err != nil {
			for _, b := range slices.Backward(bound) {
				_ = b.listener.Close()
			}
			return nil, fmt.Errorf("bind %s listener %q: %w", item.name, item.server.Addr, err)
		}
		bound = append(bound, boundServer{
			name:     item.name,
			server:   item.server,
			listener: ln,
		})
	}
	return bound, nil
}

func serveBoundServers(
	ctx context.Context,
	stopCtxFn func() (context.Context, context.CancelFunc),
	shutdownTimeout time.Duration,
	bound []boundServer,
	logger *slog.Logger,
) (serveErr, shutdownErr error) {
	if logger == nil {
		logger = slog.Default()
	}

	var (
		serveMu sync.Mutex
		serveWg sync.WaitGroup
	)
	exitCh := make(chan struct{}, len(bound))

	recordServeErr := func(err error) {
		if err == nil || errors.Is(err, http.ErrServerClosed) {
			return
		}
		serveMu.Lock()
		serveErr = errors.Join(serveErr, err)
		serveMu.Unlock()
	}

	serveWg.Add(len(bound))
	for _, item := range bound {
		go func(bs boundServer) {
			defer serveWg.Done()
			if err := bs.server.Serve(bs.listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
				recordServeErr(fmt.Errorf("%s serve: %w", bs.name, err))
			}
			exitCh <- struct{}{}
		}(item)
	}

	select {
	case <-ctx.Done():
	case <-exitCh:
	}

	logger.Info("shutting down storage service gracefully...")
	parentStopCtx, parentCancel := stopCtxFn()
	defer parentCancel()

	shutdownCtx, cancel := context.WithTimeout(parentStopCtx, shutdownTimeout)
	defer cancel()

	var (
		shutdownMu sync.Mutex
		shutdownWg sync.WaitGroup
	)
	recordShutdownErr := func(err error) {
		if err == nil {
			return
		}
		shutdownMu.Lock()
		shutdownErr = errors.Join(shutdownErr, err)
		shutdownMu.Unlock()
	}

	shutdownWg.Add(len(bound))
	for _, item := range bound {
		go func(bs boundServer) {
			defer shutdownWg.Done()
			if err := bs.server.Shutdown(shutdownCtx); err != nil {
				err1 := fmt.Errorf("%s shutdown: %w", bs.name, err)
				if closeErr := bs.server.Close(); closeErr != nil {
					err1 = errors.Join(err1, fmt.Errorf("%s close: %w", bs.name, closeErr))
				}
				recordShutdownErr(err1)
			}
		}(item)
	}

	shutdownWg.Wait()
	serveWg.Wait()

	serveMu.Lock()
	finalServeErr := serveErr
	serveMu.Unlock()

	return finalServeErr, shutdownErr
}

func runCleanups(ctx context.Context, cleanups []cleanupHook) error {
	var errs error
	for _, hook := range slices.Backward(cleanups) {
		if hook.fn == nil {
			continue
		}
		if err := hook.fn(ctx); err != nil {
			errs = errors.Join(errs, fmt.Errorf("%s cleanup: %w", hook.name, err))
		}
	}
	return errs
}

func runManagedServers(
	ctx context.Context,
	shutdownTimeout time.Duration,
	stopTimeout time.Duration,
	servers []namedServer,
	cleanups []cleanupHook,
	logger *slog.Logger,
) error {
	if logger == nil {
		logger = slog.Default()
	}

	bound, err := bindListeners(servers)
	if err != nil {
		stopCtx, cancel := context.WithTimeout(context.Background(), stopTimeout)
		defer cancel()
		return errors.Join(err, runCleanups(stopCtx, cleanups))
	}

	var (
		stopCtx    context.Context
		stopCancel context.CancelFunc
	)
	serveErr, shutdownErr := serveBoundServers(
		ctx,
		func() (context.Context, context.CancelFunc) {
			stopCtx, stopCancel = context.WithTimeout(context.Background(), stopTimeout)
			return stopCtx, func() {}
		},
		shutdownTimeout,
		bound,
		logger,
	)
	if stopCancel != nil {
		defer stopCancel()
	}
	if stopCtx == nil {
		var fallbackCancel context.CancelFunc
		stopCtx, fallbackCancel = context.WithTimeout(context.Background(), stopTimeout)
		defer fallbackCancel()
	}

	cleanupErr := runCleanups(stopCtx, cleanups)
	if finalErr := errors.Join(serveErr, shutdownErr, cleanupErr); finalErr != nil {
		return finalErr
	}

	logger.Info("storage service stopped")
	return nil
}
