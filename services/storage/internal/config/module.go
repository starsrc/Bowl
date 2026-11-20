package config

import (
	"go.uber.org/fx"

	"github.com/starsrc/pkg/observability"
	"github.com/starsrc/pkg/observability/slogx"
	"github.com/starsrc/pkg/observability/telemetry"
	"github.com/starsrc/pkg/serverx"
	"github.com/starsrc/pkg/tx"
)

type noDefaultMetricsObsConfig struct {
	*ObservabilityConfig
}

func (noDefaultMetricsObsConfig) IsMetricsEnabled() bool {
	return false
}

func (noDefaultMetricsObsConfig) Enabled() bool {
	return false
}

func NewObservabilityConfig(cfg *ObservabilityConfig) observability.Config {
	if cfg == nil {
		return nil
	}
	return noDefaultMetricsObsConfig{ObservabilityConfig: cfg}
}

var Module = fx.Module("config",
	fx.Provide(
		func(cfg *Config) observability.Config { return NewObservabilityConfig(&cfg.EnvConfig) },
		func(cfg *Config) observability.AdminRouteConfig { return cfg },
		func(cfg *Config) slogx.Config { return cfg.LogConfig },
		func(cfg *Config) telemetry.Config { return &cfg.TelemetryConfig },
		func(cfg *Config) serverx.Config { return cfg.ServerConfig },
		func(cfg *Config) serverx.AdminServerConfig { return cfg },
		func(cfg *Config) serverx.ObservabilityConfig { return cfg },
		func(cfg *Config) serverx.ServerConfig { return cfg.ServerConfig },
		func(cfg *Config) tx.PoolConfig { return cfg.PoolConfig },
		func(cfg *Config) MetaConfig { return cfg.Meta },
	),
)
