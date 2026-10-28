package config

import (
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/caarlos0/env/v11"
	"github.com/joho/godotenv"

	"github.com/starsrc/Aether/pkg/observability/slogx"
	"github.com/starsrc/Aether/pkg/observability/telemetry"
	"github.com/starsrc/Aether/pkg/serverx"
	"github.com/starsrc/Aether/pkg/tx"
)

// DefaultServerReadMaxBytes 定义 Storage 服务默认的最大请求体读取字节数（8 MiB）。
// 确保可承载协议承诺的 4 MiB 二进制写入载荷以及 Connect JSON Base64 编码后的完整请求包，同时保留明确防 OOM 上界。
const DefaultServerReadMaxBytes = 8 * 1024 * 1024

// ServerConfig 作为 serverx.ServerConfig 的类型别名。
type ServerConfig = serverx.ServerConfig

// LogConfig 作为 slogx.LogConfig 的类型别名。
type LogConfig = slogx.LogConfig

// TelemetryConfig 作为 telemetry.TelemetryConfig 的类型别名。
type TelemetryConfig = telemetry.TelemetryConfig

// DBPoolConfig 作为 tx.PoolConfig 的类型别名。
type DBPoolConfig = tx.PoolConfig

// Config 聚合 Storage 服务全局配置。
type Config struct {
	serverx.ServerConfig
	slogx.LogConfig
	telemetry.TelemetryConfig
	tx.PoolConfig
	Meta MetaConfig
}

// Validate 校验全局配置合法性。
func (c *Config) Validate() error {
	if err := c.ValidateSinglePort(); err != nil {
		return fmt.Errorf("validate server config: %w", err)
	}

	if err := c.TelemetryConfig.Validate(); err != nil {
		return fmt.Errorf("validate telemetry config: %w", err)
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

	var cfg Config
	if err := env.Parse(&cfg); err != nil {
		return nil, fmt.Errorf("parse environment variables: %w", err)
	}

	cfg.ApplyDefaultPort(9000)
	cfg.ApplyDefaultServiceName("bowl-storage")

	if strings.TrimSpace(os.Getenv("SERVER_READ_MAX_BYTES")) == "" {
		cfg.ServerReadMaxBytes = DefaultServerReadMaxBytes
	}

	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	return &cfg, nil
}
