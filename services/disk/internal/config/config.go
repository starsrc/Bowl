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

// Config aggregates service configuration.
type Config struct {
	Server  ServerConfig
	DB      DBConfig
	Storage StorageConfig
}

// StorageConfig defines local block storage configuration.
type StorageConfig struct {
	DataDir string `env:"DATA_DIR" envDefault:"/tmp/bowl/data"`
}


// ServerConfig defines HTTP server configuration.
type ServerConfig struct {
	Host                 string        `env:"SERVER_HOST" envDefault:"0.0.0.0"`
	Port                 int           `env:"SERVER_PORT" envDefault:"8080"`
	ReadHeaderTimeoutVal time.Duration `env:"SERVER_READ_HEADER_TIMEOUT" envDefault:"10s"`
	ShutdownTimeoutVal   time.Duration `env:"SERVER_SHUTDOWN_TIMEOUT" envDefault:"10s"`
}

// Addr returns the server listen address (e.g. 0.0.0.0:8080).
func (s ServerConfig) Addr() string {
	return net.JoinHostPort(s.Host, strconv.Itoa(s.Port))
}

// ReadHeaderTimeout returns read header timeout.
func (s ServerConfig) ReadHeaderTimeout() time.Duration {
	return s.ReadHeaderTimeoutVal
}

// ShutdownTimeout returns shutdown timeout.
func (s ServerConfig) ShutdownTimeout() time.Duration {
	return s.ShutdownTimeoutVal
}

// DBConfig defines database connection pool configuration.
type DBConfig struct {
	DSN               string        `env:"DB_DSN,required,notEmpty"`
	MaxConns          int32         `env:"DB_MAX_CONNS" envDefault:"20"`
	MinConns          int32         `env:"DB_MIN_CONNS" envDefault:"2"`
	MaxConnLifetime   time.Duration `env:"DB_MAX_CONN_LIFETIME" envDefault:"1h"`
	MaxConnIdleTime   time.Duration `env:"DB_MAX_CONN_IDLE_TIME" envDefault:"30m"`
	HealthCheckPeriod time.Duration `env:"DB_HEALTH_CHECK_PERIOD" envDefault:"1m"`
}

// Load loads configuration from environment variables and optional .env files.
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

	return &cfg, nil
}
