package config

import (
	"errors"
	"fmt"
	"os"

	"github.com/caarlos0/env/v11"
	"github.com/joho/godotenv"
)

// Config 聚合 Storage 服务全局配置。
type Config struct {
	Server    ServerConfig
	DB        DBPoolConfig
	Telemetry TelemetryConfig
	Log       LogConfig
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

	if err := cfg.Server.Validate(); err != nil {
		return nil, fmt.Errorf("validate server config: %w", err)
	}

	return &cfg, nil
}
