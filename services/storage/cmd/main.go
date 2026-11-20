package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"go.uber.org/fx"

	"github.com/starsrc/pkg/observability/obsfx"

	"github.com/starsrc/bowl/services/storage/internal/config"
	"github.com/starsrc/bowl/services/storage/internal/control"
	"github.com/starsrc/bowl/services/storage/internal/db"
	"github.com/starsrc/bowl/services/storage/internal/server"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)

	if err := run(ctx); err != nil {
		stop()
		slog.Error("service terminated with error", "error", err)
		os.Exit(1)
	}
	stop()
}

func run(ctx context.Context, opts ...fx.Option) error {
	cfg, err := config.Load()
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}

	options := []fx.Option{
		fx.Supply(cfg),
		config.Module,
		obsfx.Module,
		db.Module,
		control.Module,
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
