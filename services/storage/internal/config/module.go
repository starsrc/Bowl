package config

import (
	"go.uber.org/fx"

	"github.com/starsrc/Aether/pkg/observability/slogx"
	"github.com/starsrc/Aether/pkg/observability/telemetry"
	"github.com/starsrc/Aether/pkg/serverx"
	"github.com/starsrc/Aether/pkg/tx"
)

// Module 按组件需要的契约提供配置依赖。
var Module = fx.Module("config",
	fx.Provide(
		func(cfg *Config) slogx.Config { return cfg.LogConfig },
		func(cfg *Config) telemetry.Config { return &cfg.TelemetryConfig },
		func(cfg *Config) serverx.Config { return cfg.ServerConfig },
		func(cfg *Config) serverx.ObservabilityConfig { return cfg },
		func(cfg *Config) serverx.ServerConfig { return cfg.ServerConfig },
		func(cfg *Config) tx.PoolConfig { return cfg.PoolConfig },
		func(cfg *Config) MetaConfig { return cfg.Meta },
	),
)
