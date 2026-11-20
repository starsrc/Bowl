package control

import (
	"context"
	"log/slog"

	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/fx"

	"github.com/starsrc/pkg/observability/obsfx"

	"github.com/starsrc/bowl/services/storage/internal/config"
	"github.com/starsrc/bowl/services/storage/internal/db"
	"github.com/starsrc/bowl/services/storage/internal/store"
)

type ManagerParams struct {
	fx.In
	Lifecycle    fx.Lifecycle `optional:"true"`
	Store        db.Store
	MetaCfg      *config.MetaConfig      `optional:"true"`
	MetaVal      config.MetaConfig       `optional:"true"`
	Logger       *slog.Logger            `optional:"true"`
	StoreMetrics *store.Metrics          `optional:"true"`
	Collector    *storageEngineCollector `optional:"true"`
}

func newManagerFromParams(params ManagerParams) Manager {
	cfg := params.MetaCfg
	if cfg == nil && params.MetaVal != (config.MetaConfig{}) {
		cfg = &params.MetaVal
	}
	opts := make([]Option, 0, 10)
	if cfg != nil {
		mode := AtimeModeNoAtime
		switch cfg.AtimeMode() {
		case "relatime":
			mode = AtimeModeRelAtime
		case "strictatime":
			mode = AtimeModeStrictAtime
		}
		opts = append(opts,
			WithStorageRoot(cfg.DataDir()),
			WithAtimeMode(mode),
			WithEntryTimeout(cfg.EntryTimeoutSeconds()),
			WithAttrTimeout(cfg.AttrTimeoutSeconds()),
			WithDirEntryTimeout(cfg.DirEntryTimeoutSeconds()),
			WithOpenKeepCache(cfg.OpenKeepCache()),
			WithOpenDirCache(cfg.OpenDirCache()),
			WithActorIdleTTL(cfg.ActorIdleTTL()),
		)
	}
	if params.Logger != nil {
		opts = append(opts, WithLogger(params.Logger))
	}
	if params.StoreMetrics != nil {
		opts = append(opts, WithStoreMetrics(params.StoreMetrics))
	}
	if params.Collector != nil {
		opts = append(opts, withEngineCollector(params.Collector))
	}

	m := newManager(params.Store, opts...)
	if params.Lifecycle != nil {
		params.Lifecycle.Append(fx.Hook{
			OnStop: func(context.Context) error {
				return m.Close()
			},
		})
	}
	return m
}

func provideStoreMetricsCollector(m *store.Metrics) prometheus.Collector {
	return m
}

func provideEngineCollector(c *storageEngineCollector) prometheus.Collector {
	return c
}

var Module = fx.Module("control",
	fx.Provide(
		store.NewMetrics,
		newStorageEngineCollector,
		obsfx.AsCollector(provideStoreMetricsCollector),
		obsfx.AsCollector(provideEngineCollector),
		newManagerFromParams,
	),
)
