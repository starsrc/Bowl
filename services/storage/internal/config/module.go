package config

import (
	"go.uber.org/fx"

	"github.com/starsrc/pkg/observability"
	"github.com/starsrc/pkg/observability/slogx"
	"github.com/starsrc/pkg/observability/telemetry"
	"github.com/starsrc/pkg/serverx"
	"github.com/starsrc/pkg/tx"
)

// noDefaultMetricsObsConfig 包装 *EnvConfig，在 obsfx.Module 调用 observability.Setup 时
// 同时抑制默认 5ms 首桶 MeterProvider 与默认 TracerProvider 的创建（避免与 server 模块
// 18 级亚毫秒桶 MeterProvider 及控制面提权 TracerProvider 产生二次创建与二次 Shutdown 冲突），
// 其余日志与参数校验契约原样透传。
// 真实的 METRICS_ENABLED 开关由 AdminRouteConfig (*Config) 提供，OTEL_ENABLED 由 telemetry.Config 提供。
type noDefaultMetricsObsConfig struct {
	*ObservabilityConfig
}

func (noDefaultMetricsObsConfig) IsMetricsEnabled() bool {
	return false
}

func (noDefaultMetricsObsConfig) Enabled() bool {
	return false
}

// NewObservabilityConfig 构造供 observability.Setup 使用的配置视图（屏蔽默认 MeterProvider 创建）。
func NewObservabilityConfig(cfg *ObservabilityConfig) observability.Config {
	if cfg == nil {
		return nil
	}
	return noDefaultMetricsObsConfig{ObservabilityConfig: cfg}
}

// Module 按组件需要的契约提供配置依赖。
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
		func(cfg *Config) AuthToken { return cfg.AuthToken },
	),
)
