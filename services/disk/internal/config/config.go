package config

import (
	"errors"
	"fmt"
	"net"
	"os"
	"strconv"
	"time"

	"github.com/caarlos0/env/v11"
	"github.com/joho/godotenv"
)

// Config 聚合磁盘服务配置。
type Config struct {
	Server  ServerConfig
	DB      DBConfig
	Storage StorageConfig
	S3      S3Config
}

// StorageConfig 定义本地块存储与暂存目录配置。
type StorageConfig struct {
	DataDir    string `env:"DATA_DIR" envDefault:"/tmp/bowl/data"`
	StagingDir string `env:"STAGING_DIR" envDefault:"/tmp/bowl/staging"`
}

// S3Config 定义用于异步块持久化与对象清理的对象存储配置。
type S3Config struct {
	Enabled        bool   `env:"S3_ENABLED" envDefault:"false"`
	Endpoint       string `env:"S3_ENDPOINT" envDefault:"http://127.0.0.1:9000"`
	Bucket         string `env:"S3_BUCKET" envDefault:"bowl-data"`
	AccessKey      string `env:"S3_ACCESS_KEY"`
	SecretKey      string `env:"S3_SECRET_KEY"`
	Region         string `env:"S3_REGION" envDefault:"us-east-1"`
	UseSSL         bool   `env:"S3_USE_SSL" envDefault:"false"`
	ForcePathStyle bool   `env:"S3_FORCE_PATH_STYLE" envDefault:"true"`
}


// ServerConfig 定义 HTTP 服务配置。
type ServerConfig struct {
	Host                 string        `env:"SERVER_HOST" envDefault:"0.0.0.0"`
	Port                 int           `env:"SERVER_PORT" envDefault:"8080"`
	ReadHeaderTimeoutVal time.Duration `env:"SERVER_READ_HEADER_TIMEOUT" envDefault:"10s"`
	ShutdownTimeoutVal   time.Duration `env:"SERVER_SHUTDOWN_TIMEOUT" envDefault:"10s"`
}

// Addr 返回服务监听地址（如 0.0.0.0:8080）。
func (s ServerConfig) Addr() string {
	return net.JoinHostPort(s.Host, strconv.Itoa(s.Port))
}

// ReadHeaderTimeout 返回读取请求头超时时间。
func (s ServerConfig) ReadHeaderTimeout() time.Duration {
	return s.ReadHeaderTimeoutVal
}

// ShutdownTimeout 返回平滑停机超时时间。
func (s ServerConfig) ShutdownTimeout() time.Duration {
	return s.ShutdownTimeoutVal
}

// DBConfig 定义数据库连接池配置。
type DBConfig struct {
	DSN               string        `env:"DB_DSN,required,notEmpty"`
	MaxConns          int32         `env:"DB_MAX_CONNS" envDefault:"20"`
	MinConns          int32         `env:"DB_MIN_CONNS" envDefault:"2"`
	MaxConnLifetime   time.Duration `env:"DB_MAX_CONN_LIFETIME" envDefault:"1h"`
	MaxConnIdleTime   time.Duration `env:"DB_MAX_CONN_IDLE_TIME" envDefault:"30m"`
	HealthCheckPeriod time.Duration `env:"DB_HEALTH_CHECK_PERIOD" envDefault:"1m"`
}

// Load 从环境变量及可选的 .env 文件中加载配置。
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

	if cfg.S3.Enabled {
		if cfg.S3.Endpoint == "" {
			return nil, errors.New("S3_ENDPOINT is required when S3_ENABLED is true")
		}
		if cfg.S3.Bucket == "" {
			return nil, errors.New("S3_BUCKET is required when S3_ENABLED is true")
		}
		if cfg.S3.AccessKey == "" || cfg.S3.SecretKey == "" {
			return nil, errors.New("S3_ACCESS_KEY and S3_SECRET_KEY are required when S3_ENABLED is true")
		}
	}

	return &cfg, nil
}
