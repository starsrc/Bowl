package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/starsrc/bowl/services/disk/internal/config"
	"github.com/starsrc/bowl/services/disk/internal/db"
	"github.com/starsrc/bowl/services/disk/internal/server"
	"github.com/starsrc/bowl/services/disk/internal/service"
	"github.com/starsrc/bowl/services/disk/internal/storage"
	"github.com/starsrc/bowl/services/disk/internal/worker"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	slog.SetDefault(logger)

	// ADR 0013: Fail-Fast Environment Configuration
	cfg, err := config.Load()
	if err != nil {
		slog.Error("failed to load configuration", "error", err)
		os.Exit(1)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	// Initialize database connection pool with fail-fast ping
	poolCfg, err := pgxpool.ParseConfig(cfg.DB.DSN)
	if err != nil {
		slog.Error("invalid database DSN", "error", err)
		os.Exit(1)
	}
	if cfg.DB.MaxConns > 0 {
		poolCfg.MaxConns = cfg.DB.MaxConns
	}
	if cfg.DB.MinConns > 0 {
		poolCfg.MinConns = cfg.DB.MinConns
	}
	if cfg.DB.MaxConnLifetime > 0 {
		poolCfg.MaxConnLifetime = cfg.DB.MaxConnLifetime
	}
	if cfg.DB.MaxConnIdleTime > 0 {
		poolCfg.MaxConnIdleTime = cfg.DB.MaxConnIdleTime
	}
	if cfg.DB.HealthCheckPeriod > 0 {
		poolCfg.HealthCheckPeriod = cfg.DB.HealthCheckPeriod
	}

	pool, err := pgxpool.NewWithConfig(ctx, poolCfg)
	if err != nil {
		slog.Error("failed to create database connection pool", "error", err)
		os.Exit(1)
	}
	defer pool.Close()

	if err := pool.Ping(ctx); err != nil {
		slog.Error("database ping failed, terminating process", "error", err)
		os.Exit(1)
	}
	slog.Info("connected to database successfully")

	// Run goose migrations on startup
	if err := db.MigrateUp(cfg.DB.DSN); err != nil {
		slog.Error("database migration failed", "error", err)
		os.Exit(1)
	}
	slog.Info("database migrations applied successfully")

	store := db.NewStore(pool)

	// 初始化底层存储物理清理驱动与常驻清理工作器 (ADR 0015)
	purger, err := storage.NewMinIOObjectStoragePurger(cfg.S3, cfg.Storage)
	if err != nil {
		slog.Error("failed to create object storage purger", "error", err)
		os.Exit(1)
	}

	cleanupWorker := worker.NewDiskCleanupWorker(store, purger)
	cleanupWorker.Start(ctx)
	slog.Info("disk cleanup worker started successfully")

	diskService := service.NewDiskService(
		store,
		service.WithDataDir(cfg.Storage.DataDir),
		service.WithCleanupWorker(cleanupWorker),
	)
	srv := server.New(cfg.Server, diskService)

	go func() {
		slog.Info("starting disk service", "addr", cfg.Server.Addr())
		if err := srv.Start(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			slog.Error("disk service stopped with error", "error", err)
			os.Exit(1)
		}
	}()

	<-ctx.Done()
	slog.Info("shutting down disk service gracefully...")

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), cfg.Server.ShutdownTimeout())
	defer shutdownCancel()

	if err := srv.Stop(shutdownCtx); err != nil {
		slog.Error("disk service graceful shutdown error", "error", err)
		os.Exit(1)
	}

	// 优雅停止后台异步清理工作器
	cleanupWorker.Stop()
	slog.Info("disk cleanup worker stopped cleanly")

	slog.Info("disk service stopped cleanly")
}
