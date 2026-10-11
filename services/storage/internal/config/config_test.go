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

func TestLoad_S3Defaults(t *testing.T) {
	t.Setenv("DB_DSN", "postgres://postgres:postgres@localhost:5432/bowl?sslmode=disable")
	_ = os.Unsetenv("S3_ENABLED")
	_ = os.Unsetenv("S3_ENDPOINT")
	_ = os.Unsetenv("S3_BUCKET")
	_ = os.Unsetenv("S3_ACCESS_KEY")
	_ = os.Unsetenv("S3_SECRET_KEY")

	cfg, err := Load()
	require.NoError(t, err)
	require.NotNil(t, cfg)

	assert.False(t, cfg.S3.Enabled)
	assert.Equal(t, "http://127.0.0.1:9000", cfg.S3.Endpoint)
	assert.Equal(t, "bowl-data", cfg.S3.Bucket)
	assert.Equal(t, "", cfg.S3.AccessKey)
	assert.Equal(t, "", cfg.S3.SecretKey)
	assert.Equal(t, "us-east-1", cfg.S3.Region)
	assert.False(t, cfg.S3.UseSSL)
	assert.True(t, cfg.S3.ForcePathStyle)
}

func TestLoad_S3Enabled_Validation(t *testing.T) {
	t.Setenv("DB_DSN", "postgres://postgres:postgres@localhost:5432/bowl?sslmode=disable")
	t.Setenv("S3_ENABLED", "true")

	// Missing access key and secret key
	_ = os.Unsetenv("S3_ACCESS_KEY")
	_ = os.Unsetenv("S3_SECRET_KEY")
	cfg, err := Load()
	assert.Error(t, err)
	assert.Nil(t, cfg)

	// Missing secret key
	t.Setenv("S3_ACCESS_KEY", "custom_key")
	_ = os.Unsetenv("S3_SECRET_KEY")
	cfg, err = Load()
	assert.Error(t, err)
	assert.Nil(t, cfg)

	// Valid S3 configuration
	t.Setenv("S3_ACCESS_KEY", "custom_key")
	t.Setenv("S3_SECRET_KEY", "custom_secret")
	t.Setenv("S3_BUCKET", "my-bucket")
	t.Setenv("S3_REGION", "ap-southeast-1")
	t.Setenv("S3_USE_SSL", "true")
	t.Setenv("S3_FORCE_PATH_STYLE", "false")

	cfg, err = Load()
	require.NoError(t, err)
	require.NotNil(t, cfg)
	assert.True(t, cfg.S3.Enabled)
	assert.Equal(t, "http://127.0.0.1:9000", cfg.S3.Endpoint)
	assert.Equal(t, "my-bucket", cfg.S3.Bucket)
	assert.Equal(t, "custom_key", cfg.S3.AccessKey)
	assert.Equal(t, "custom_secret", cfg.S3.SecretKey)
	assert.Equal(t, "ap-southeast-1", cfg.S3.Region)
	assert.True(t, cfg.S3.UseSSL)
	assert.False(t, cfg.S3.ForcePathStyle)
}

