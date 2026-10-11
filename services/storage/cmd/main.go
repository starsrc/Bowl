package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	connect "connectrpc.com/connect/v2"
	"connectrpc.com/connect/v2/connecthttp"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/starsrc/bowl/gen/go/bowl/v1/bowlv1connect"
	"github.com/starsrc/bowl/services/storage/internal/chunk"
	"github.com/starsrc/bowl/services/storage/internal/config"
	"github.com/starsrc/bowl/services/storage/internal/server"
	"github.com/starsrc/bowl/services/storage/internal/service"
	"github.com/starsrc/bowl/services/storage/internal/session"
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

	// Ensure base chunk directory exists
	if err := os.MkdirAll(cfg.Storage.DataDir, 0o755); err != nil {
		slog.Error("failed to create data directory", "error", err)
		os.Exit(1)
	}

	// Connect to upstream DiskService
	transport := connecthttp.NewTransport(http.DefaultClient, cfg.DiskService.Addr)
	connectClient := connect.NewClient(transport)
	diskClient := bowlv1connect.NewDiskServiceClient(connectClient)

	// Initialize storage driver based on configuration
	var driver service.FileStorageDriver
	var s3Uploader *chunk.S3Uploader
	if cfg.Storage.Driver == "chunk" {
		if err := os.MkdirAll(cfg.Storage.StagingDir, 0o755); err != nil {
			slog.Error("failed to create staging directory", "error", err)
			os.Exit(1)
		}
		stagingMgr := chunk.NewStagingManager(cfg.Storage.StagingDir)
		if cfg.S3.Enabled {
			var err error
			s3Uploader, err = chunk.NewS3Uploader(cfg.S3, stagingMgr)
			if err != nil {
				slog.Error("failed to initialize S3 uploader", "error", err)
				os.Exit(1)
			}
			s3Uploader.Start(ctx)
			slog.Info("S3 asynchronous upload pipeline started", "bucket", cfg.S3.Bucket, "endpoint", cfg.S3.Endpoint)
		}
		chunkDriver := service.NewChunkDriver(stagingMgr, pool, s3Uploader)
		driver = chunkDriver
		slog.Info("using chunk storage driver", "staging_dir", cfg.Storage.StagingDir)

		// Recover unflushed NVMe staging slices and replay to S3
		recoveryReport, err := chunkDriver.RecoverStaging(ctx)
		if err != nil {
			slog.Error("failed to recover NVMe staging slices", "error", err)
		} else if recoveryReport != nil && recoveryReport.ScannedSlices > 0 {
			slog.Info("NVMe staging recovery completed",
				"scanned_disks", recoveryReport.ScannedDisks,
				"scanned_slices", recoveryReport.ScannedSlices,
				"requeued_slices", recoveryReport.RequeuedSlices,
				"skipped_slices", recoveryReport.SkippedSlices,
				"orphaned_slices", recoveryReport.OrphanedSlices,
				"max_slice_id", recoveryReport.MaxSliceID,
			)
		}
	} else {
		driver = service.NewLegacyFileDriver(cfg.Storage.DataDir)
		slog.Info("using legacy file storage driver", "data_dir", cfg.Storage.DataDir)
	}


	// Initialize in-memory session manager and StorageService
	sessionMgr := session.NewMemoryManager()
	storageService := service.NewStorageService(
		diskClient,
		pool,
		sessionMgr,
		cfg.Storage.DataDir,
		service.WithFileStorageDriver(driver),
	)
	srv := server.New(cfg.Server, storageService)

	go func() {
		slog.Info("starting storage service", "addr", cfg.Server.Addr())
		if err := srv.Start(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			slog.Error("storage service stopped with error", "error", err)
			os.Exit(1)
		}
	}()

	<-ctx.Done()
	slog.Info("shutting down storage service gracefully...")

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), cfg.Server.ShutdownTimeout())
	defer shutdownCancel()

	if err := srv.Stop(shutdownCtx); err != nil {
		slog.Error("storage service graceful shutdown error", "error", err)
		os.Exit(1)
	}

	if s3Uploader != nil {
		s3Uploader.Stop()
		slog.Info("S3 uploader stopped cleanly")
	}

	slog.Info("storage service stopped cleanly")
}

