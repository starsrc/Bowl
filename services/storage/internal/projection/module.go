package projection

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/fx"

	"github.com/starsrc/pkg/observability/obsfx"
	"github.com/starsrc/pkg/tx"

	"github.com/starsrc/bowl/services/storage/internal/config"
)

func OpenPool(cfg config.DBPoolConfig) (*pgxpool.Pool, error) {
	pool, err := tx.OpenPool(cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to parse dbpool config: %w", err)
	}
	return pool, nil
}

type PoolParams struct {
	fx.In
	Lifecycle fx.Lifecycle
	Config    config.DBPoolConfig
}

func ProvidePool(params PoolParams) (*pgxpool.Pool, error) {
	pool, err := OpenPool(params.Config)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize dbpool: %w", err)
	}
	tx.RegisterPoolLifecycle(params.Lifecycle, pool)
	return pool, nil
}

func ProvideStore(pool *pgxpool.Pool) Store {
	return NewPGStore(pool)
}

type ControlManagerParams struct {
	fx.In
	Lifecycle fx.Lifecycle       `optional:"true"`
	Store     Store              `optional:"true"`
	MetaCfg   *config.MetaConfig `optional:"true"`
	MetaVal   config.MetaConfig  `optional:"true"`
}

func ProvideControlManager(params ControlManagerParams) (*ControlManager, error) {
	cfg := params.MetaCfg
	if cfg == nil && params.MetaVal != (config.MetaConfig{}) {
		cfg = &params.MetaVal
	}
	storageRoot := config.DefaultDataDir
	actorIdleTTL := config.DefaultActorIdleTTL
	gcInterval := config.DefaultGCInterval
	disablePeriodicGC := false
	if cfg != nil {
		storageRoot = cfg.DataDir()
		actorIdleTTL = cfg.ActorIdleTTL()
		gcInterval = cfg.GCInterval()
		disablePeriodicGC = gcInterval == 0
	}

	mgr, err := NewControlManager(ManagerOptions{
		StorageRoot:       storageRoot,
		Store:             params.Store,
		ActorIdleTTL:      actorIdleTTL,
		GCInterval:        gcInterval,
		DisablePeriodicGC: disablePeriodicGC,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to initialize control manager: %w", err)
	}

	if params.Lifecycle != nil {
		params.Lifecycle.Append(fx.Hook{
			OnStop: func(ctx context.Context) error {
				return mgr.Close(ctx)
			},
		})
	}
	return mgr, nil
}

func provideStorageMetricsCollector(c *StorageMetricsCollector) prometheus.Collector {
	return c
}

var Module = fx.Module("projection",
	fx.Provide(
		ProvidePool,
		ProvideStore,
		ProvideControlManager,
		NewStorageMetricsCollector,
		obsfx.AsCollector(provideStorageMetricsCollector),
	),
)
