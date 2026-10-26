package config

import (
	"time"
)

// DBPoolConfig 承载数据库连接池配置。
type DBPoolConfig struct {
	DSNVal               string        `env:"DB_DSN,required,notEmpty"`
	MaxConnsVal          int32         `env:"DB_MAX_CONNS"`
	MinConnsVal          int32         `env:"DB_MIN_CONNS"`
	MaxConnLifetimeVal   time.Duration `env:"DB_MAX_CONN_LIFETIME" envDefault:"1h"`
	MaxConnIdleTimeVal   time.Duration `env:"DB_MAX_CONN_IDLE_TIME" envDefault:"30m"`
	HealthCheckPeriodVal time.Duration `env:"DB_HEALTH_CHECK_PERIOD" envDefault:"1m"`
	TracingEnabledVal    bool          `env:"DB_TRACING_ENABLED" envDefault:"true"`
}

// DSN 返回数据库连接串。
func (c DBPoolConfig) DSN() string {
	return c.DSNVal
}

// MaxConns 返回连接池最大连接数。
func (c DBPoolConfig) MaxConns() int32 {
	return c.MaxConnsVal
}

// MinConns 返回连接池最小连接数。
func (c DBPoolConfig) MinConns() int32 {
	return c.MinConnsVal
}

// MaxConnLifetime 返回单连接最大存活时间。
func (c DBPoolConfig) MaxConnLifetime() time.Duration {
	return c.MaxConnLifetimeVal
}

// MaxConnIdleTime 返回空闲连接最大存活时间。
func (c DBPoolConfig) MaxConnIdleTime() time.Duration {
	return c.MaxConnIdleTimeVal
}

// HealthCheckPeriod 返回健康检查周期。
func (c DBPoolConfig) HealthCheckPeriod() time.Duration {
	return c.HealthCheckPeriodVal
}

// TracingEnabled 返回是否启用连接池链路追踪。
func (c DBPoolConfig) TracingEnabled() bool {
	return c.TracingEnabledVal
}
