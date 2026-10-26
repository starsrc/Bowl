package config

import (
	"go.uber.org/fx"

	"github.com/starsrc/Aether/pkg/observability/slogx"
	"github.com/starsrc/Aether/pkg/observability/telemetry"
)

// Module 按组件需要的契约提供配置依赖。
var Module = fx.Module("config",
	fx.Provide(
		func(cfg *Config) slogx.Config { return cfg.Log },
		func(cfg *Config) ServerConfig { return cfg.Server },
		func(cfg *Config) DBPoolConfig { return cfg.DB },
		func(cfg *Config) telemetry.Config { return &cfg.Telemetry },
		func(cfg *Config) *TelemetryConfig { return &cfg.Telemetry },
		func(cfg *Config) MetaConfig { return cfg.Meta },
		func(cfg *Config) *MetaConfig { return &cfg.Meta },
	),
)
