// Package config 提供 Storage 服务的环境变量与配置加载。
package config

import (
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/caarlos0/env/v11"
	"github.com/joho/godotenv"

	"github.com/starsrc/pkg/observability"
	"github.com/starsrc/pkg/serverx"
	"github.com/starsrc/pkg/tx"
)

// DefaultServerReadMaxBytes 定义 Storage 服务默认的最大请求体读取字节数（8 MiB）。
// 确保可承载协议承诺的 4 MiB 二进制写入载荷以及 Connect JSON Base64 编码后的完整请求包，同时保留明确防 OOM 上界。
const DefaultServerReadMaxBytes = 8 * 1024 * 1024

var otlpEndpointEnvVars = [...]string{
	"OTEL_EXPORTER_OTLP_ENDPOINT",
	"OTEL_EXPORTER_OTLP_TRACES_ENDPOINT",
	"OTEL_EXPORTER_OTLP_METRICS_ENDPOINT",
	"OTEL_EXPORTER_OTLP_LOGS_ENDPOINT",
}

// NormalizeOTLPEndpoint 对缺少协议前缀（http:// 或 https://）的非空 OTLP 地址自动补齐 http://，
// 避免裸 host:port（如 jaeger:4317）被 OTel SDK url.Parse 当作 scheme 解析失败。
func NormalizeOTLPEndpoint(raw string) string {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" || strings.Contains(trimmed, "://") {
		return trimmed
	}
	return "http://" + trimmed
}

func normalizeOTLPEnvVars() {
	for _, key := range otlpEndpointEnvVars {
		val, ok := os.LookupEnv(key)
		if !ok {
			continue
		}
		normalized := NormalizeOTLPEndpoint(val)
		if normalized != "" && normalized != val {
			_ = os.Setenv(key, normalized)
		}
	}
}

// ServerConfig 作为 serverx.ServerConfig 的类型别名。
type ServerConfig = serverx.ServerConfig

// ObservabilityConfig 作为 observability.EnvConfig 的类型别名。
type ObservabilityConfig = observability.EnvConfig

// DBPoolConfig 作为 tx.PoolConfig 的类型别名。
type DBPoolConfig = tx.PoolConfig

// Config 聚合 Storage 服务全局配置。
type Config struct {
	serverx.ServerConfig
	observability.EnvConfig
	tx.PoolConfig
	Meta MetaConfig
	// AuthToken 非空时，公网端口上的全部请求（RPC 与交接路由）都必须携带
	// Authorization: Bearer <token>。留空表示不鉴权，仅适用于可信网络内的开发环境。
	AuthToken AuthToken `env:"STORAGE_AUTH_TOKEN"`
}

// AuthToken 是公网端口的访问令牌，独立成类型以便按类型注入给 server 模块。
type AuthToken string

var (
	_ serverx.AdminServerConfig      = (*Config)(nil)
	_ observability.AdminRouteConfig = (*Config)(nil)
)

// InternalPort 返回管理/内网监听端口。
func (c *Config) InternalPort() int {
	if c == nil {
		return 0
	}
	return c.InternalServerPort
}

// ApplyDefaultInternalPort 在未显式指定 INTERNAL_SERVER_PORT 环境变量且端口处于零值或 serverx 默认 8090 时，
// 将管理/内网监听端口设置为指定的 defaultPort。
func (c *Config) ApplyDefaultInternalPort(defaultPort int) {
	if c == nil {
		return
	}
	envVal, envSet := os.LookupEnv("INTERNAL_SERVER_PORT")
	if (!envSet || strings.TrimSpace(envVal) == "") && (c.InternalServerPort == 0 || c.InternalServerPort == 8090) {
		c.InternalServerPort = defaultPort
	}
}

// Validate 校验全局配置合法性。
func (c *Config) Validate() error {
	if err := c.ValidateDualPorts(); err != nil {
		return fmt.Errorf("validate server config: %w", err)
	}

	if err := c.EnvConfig.Validate(); err != nil {
		return fmt.Errorf("validate observability config: %w", err)
	}

	if err := c.PoolConfig.Validate(); err != nil {
		return fmt.Errorf("validate db pool config: %w", err)
	}

	if err := c.Meta.Validate(); err != nil {
		return fmt.Errorf("validate meta config: %w", err)
	}

	return nil
}

// Load 读取环境变量及配置文件。
// 若未指定 filenames 则读取当前工作目录下的 .env（若文件不存在则忽略）。
// 若指定了 filenames 则依次加载指定文件。
func Load(filenames ...string) (*Config, error) {
	if len(filenames) == 0 {
		if err := godotenv.Load(); err != nil && !errors.Is(err, os.ErrNotExist) && !os.IsNotExist(err) {
			return nil, fmt.Errorf("load .env: %w", err)
		}
	} else {
		if err := godotenv.Load(filenames...); err != nil {
			return nil, fmt.Errorf("load env file: %w", err)
		}
	}

	normalizeOTLPEnvVars()

	var cfg Config
	if err := env.Parse(&cfg); err != nil {
		return nil, fmt.Errorf("parse environment variables: %w", err)
	}

	cfg.OtelEndpoint = NormalizeOTLPEndpoint(cfg.OtelEndpoint)
	cfg.OtelTracesEndpoint = NormalizeOTLPEndpoint(cfg.OtelTracesEndpoint)
	cfg.ApplyDefaultPort(9000)
	cfg.ApplyDefaultInternalPort(9090)
	cfg.ApplyDefaultServiceName("bowl-storage")
	cfg.ApplyDefaultTrustRemote(true)

	if strings.TrimSpace(os.Getenv("SERVER_READ_MAX_BYTES")) == "" {
		cfg.ServerReadMaxBytes = DefaultServerReadMaxBytes
	}

	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	return &cfg, nil
}
