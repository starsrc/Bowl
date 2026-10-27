package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"go.uber.org/fx"

	"github.com/starsrc/Aether/pkg/observability/slogx"
	"github.com/starsrc/Aether/pkg/observability/telemetry"
	"github.com/starsrc/bowl/services/storage/internal/config"
	"github.com/starsrc/bowl/services/storage/internal/db"
	"github.com/starsrc/bowl/services/storage/internal/meta"
	"github.com/starsrc/bowl/services/storage/internal/server"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := run(ctx); err != nil {
		slog.Error("service terminated with error", "error", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, opts ...fx.Option) error {
	cfg, err := config.Load()
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}

	options := []fx.Option{
		fx.Supply(cfg),
		config.Module,
		telemetry.Module,
		slogx.Module,
		db.Module,
		meta.Module,
		server.Module,
		fx.NopLogger,
		fx.Provide(func() context.Context { return ctx }),
		fx.StopTimeout(cfg.StopTimeout()),
	}

	application := fx.New(append(options, opts...)...)
	if err := application.Start(ctx); err != nil {
		return fmt.Errorf("failed to start application: %w", err)
	}

	var sig fx.ShutdownSignal
	select {
	case <-ctx.Done():
	case sig = <-application.Wait():
	}

	slog.Info("shutting down storage service gracefully...")
	stopCtx, cancel := context.WithTimeout(context.Background(), application.StopTimeout())
	defer cancel()

	if err := application.Stop(stopCtx); err != nil {
		return fmt.Errorf("server graceful shutdown error: %w", err)
	}
	if sig.ExitCode != 0 {
		return fmt.Errorf("service stopped with exit code %d", sig.ExitCode)
	}

	slog.Info("storage service stopped")
	return nil
}
