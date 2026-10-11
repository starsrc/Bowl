package config

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLoad_FailFastMissingDBDSN(t *testing.T) {
	_ = os.Unsetenv("DB_DSN")

	cfg, err := Load()
	assert.Error(t, err)
	assert.Nil(t, cfg)
}

func TestLoad_SuccessWithDefaults(t *testing.T) {
	t.Setenv("DB_DSN", "postgres://postgres:postgres@localhost:5432/bowl?sslmode=disable")
	_ = os.Unsetenv("PORT")
	_ = os.Unsetenv("DATA_DIR")
	_ = os.Unsetenv("DISK_SERVICE_ADDR")

	cfg, err := Load()
	require.NoError(t, err)
	require.NotNil(t, cfg)

	assert.Equal(t, "0.0.0.0", cfg.Server.Host)
	assert.Equal(t, 8081, cfg.Server.Port)
	assert.Equal(t, "0.0.0.0:8081", cfg.Server.Addr())
	assert.Equal(t, "postgres://postgres:postgres@localhost:5432/bowl?sslmode=disable", cfg.DB.DSN)
	assert.Equal(t, "/tmp/bowl/data", cfg.Storage.DataDir)
	assert.Equal(t, "http://127.0.0.1:8080", cfg.DiskService.Addr)
}

func TestLoad_CustomOverrides(t *testing.T) {
	t.Setenv("DB_DSN", "postgres://postgres:postgres@localhost:5432/testdb?sslmode=disable")
	t.Setenv("PORT", "9090")
	t.Setenv("HOST", "127.0.0.1")
	t.Setenv("DATA_DIR", "/var/lib/bowl/data")
	t.Setenv("DISK_SERVICE_ADDR", "http://disk.internal:8080")

	cfg, err := Load()
	require.NoError(t, err)
	require.NotNil(t, cfg)

	assert.Equal(t, "127.0.0.1", cfg.Server.Host)
	assert.Equal(t, 9090, cfg.Server.Port)
	assert.Equal(t, "127.0.0.1:9090", cfg.Server.Addr())
	assert.Equal(t, "postgres://postgres:postgres@localhost:5432/testdb?sslmode=disable", cfg.DB.DSN)
	assert.Equal(t, "/var/lib/bowl/data", cfg.Storage.DataDir)
	assert.Equal(t, "http://disk.internal:8080", cfg.DiskService.Addr)
}
