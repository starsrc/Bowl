package config

import (
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/caarlos0/env/v11"
	"github.com/joho/godotenv"

	"github.com/starsrc/pkg/observability"
	"github.com/starsrc/pkg/tx"
)

const DefaultServerReadMaxBytes = 8 * 1024 * 1024

var otlpEndpointEnvVars = [...]string{
	"OTEL_EXPORTER_OTLP_ENDPOINT",
	"OTEL_EXPORTER_OTLP_TRACES_ENDPOINT",
	"OTEL_EXPORTER_OTLP_METRICS_ENDPOINT",
	"OTEL_EXPORTER_OTLP_LOGS_ENDPOINT",
}

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

type ObservabilityConfig = observability.EnvConfig

type DBPoolConfig = tx.PoolConfig

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

type Config struct {
	ServerConfig
	observability.EnvConfig
	tx.PoolConfig
	Meta MetaConfig
}

var _ observability.AdminRouteConfig = (*Config)(nil)

func (c *Config) InternalPort() int {
	if c == nil {
		return 0
	}
	return c.InternalServerPort
}

func (c *Config) ApplyDefaultInternalPort(defaultPort int) {
	if c == nil {
		return
	}
	envVal, envSet := os.LookupEnv("INTERNAL_SERVER_PORT")
	if (!envSet || strings.TrimSpace(envVal) == "") && (c.InternalServerPort == 0 || c.InternalServerPort == 8090) {
		c.InternalServerPort = defaultPort
	}
}

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
