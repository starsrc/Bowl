package config_test

import (
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/starsrc/bowl/services/disk/internal/config"
)

func TestLoad_FailFastMissingDBDSN(t *testing.T) {
	// Ensure DB_DSN is unset
	_ = os.Unsetenv("DB_DSN")

	cfg, err := config.Load()
	require.Error(t, err, "config.Load must fail fast when DB_DSN is missing")
	assert.Nil(t, cfg)
	assert.Contains(t, err.Error(), "DB_DSN")
}

func TestLoad_SuccessWithDefaults(t *testing.T) {
	t.Setenv("DB_DSN", "postgres://postgres:postgres@localhost:5432/bowl?sslmode=disable")
	t.Setenv("SERVER_PORT", "9090")

	cfg, err := config.Load()
	require.NoError(t, err)
	require.NotNil(t, cfg)

	assert.Equal(t, "postgres://postgres:postgres@localhost:5432/bowl?sslmode=disable", cfg.DB.DSN)
	assert.Equal(t, int32(20), cfg.DB.MaxConns)
	assert.Equal(t, int32(2), cfg.DB.MinConns)
	assert.Equal(t, time.Hour, cfg.DB.MaxConnLifetime)
	assert.Equal(t, 9090, cfg.Server.Port)
	assert.Equal(t, "0.0.0.0:9090", cfg.Server.Addr())
	assert.Equal(t, 10*time.Second, cfg.Server.ReadHeaderTimeout())
	assert.Equal(t, 10*time.Second, cfg.Server.ShutdownTimeout())
	assert.Equal(t, "/tmp/bowl/data", cfg.Storage.DataDir)
}

