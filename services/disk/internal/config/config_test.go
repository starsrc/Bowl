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
	assert.Equal(t, "/tmp/bowl/staging", cfg.Storage.StagingDir)
	assert.False(t, cfg.S3.Enabled)
	assert.Equal(t, "http://127.0.0.1:9000", cfg.S3.Endpoint)
	assert.Equal(t, "bowl-data", cfg.S3.Bucket)
	assert.Equal(t, "", cfg.S3.AccessKey)
	assert.Equal(t, "", cfg.S3.SecretKey)
	assert.Equal(t, "us-east-1", cfg.S3.Region)
	assert.False(t, cfg.S3.UseSSL)
	assert.True(t, cfg.S3.ForcePathStyle)
}

func TestLoad_CustomStorageAndS3Overrides(t *testing.T) {
	// 测试自定义存储与 S3 覆盖参数
	t.Setenv("DB_DSN", "postgres://postgres:postgres@localhost:5432/bowl?sslmode=disable")
	t.Setenv("DATA_DIR", "/var/bowl/data")
	t.Setenv("STAGING_DIR", "/var/bowl/staging")
	t.Setenv("S3_ENABLED", "true")
	t.Setenv("S3_ENDPOINT", "http://s3.local:9000")
	t.Setenv("S3_BUCKET", "custom-bucket")
	t.Setenv("S3_ACCESS_KEY", "ak_123")
	t.Setenv("S3_SECRET_KEY", "sk_456")
	t.Setenv("S3_REGION", "ap-southeast-1")
	t.Setenv("S3_USE_SSL", "true")
	t.Setenv("S3_FORCE_PATH_STYLE", "false")

	cfg, err := config.Load()
	require.NoError(t, err)
	require.NotNil(t, cfg)

	assert.Equal(t, "/var/bowl/data", cfg.Storage.DataDir)
	assert.Equal(t, "/var/bowl/staging", cfg.Storage.StagingDir)
	assert.True(t, cfg.S3.Enabled)
	assert.Equal(t, "http://s3.local:9000", cfg.S3.Endpoint)
	assert.Equal(t, "custom-bucket", cfg.S3.Bucket)
	assert.Equal(t, "ak_123", cfg.S3.AccessKey)
	assert.Equal(t, "sk_456", cfg.S3.SecretKey)
	assert.Equal(t, "ap-southeast-1", cfg.S3.Region)
	assert.True(t, cfg.S3.UseSSL)
	assert.False(t, cfg.S3.ForcePathStyle)
}

func TestLoad_S3Enabled_Validation(t *testing.T) {
	// 当 S3_ENABLED 为 true 时校验必填参数
	t.Setenv("DB_DSN", "postgres://postgres:postgres@localhost:5432/bowl?sslmode=disable")
	t.Setenv("S3_ENABLED", "true")

	// 缺失 AccessKey 和 SecretKey
	_ = os.Unsetenv("S3_ACCESS_KEY")
	_ = os.Unsetenv("S3_SECRET_KEY")
	cfg, err := config.Load()
	assert.Error(t, err)
	assert.Nil(t, cfg)
	assert.Contains(t, err.Error(), "S3_ACCESS_KEY and S3_SECRET_KEY are required")

	// 缺失 SecretKey
	t.Setenv("S3_ACCESS_KEY", "custom_key")
	_ = os.Unsetenv("S3_SECRET_KEY")
	cfg, err = config.Load()
	assert.Error(t, err)
	assert.Nil(t, cfg)
	assert.Contains(t, err.Error(), "S3_ACCESS_KEY and S3_SECRET_KEY are required")

	// 完整有效的 S3 配置
	t.Setenv("S3_ACCESS_KEY", "custom_key")
	t.Setenv("S3_SECRET_KEY", "custom_secret")
	t.Setenv("S3_BUCKET", "my-bucket")
	t.Setenv("S3_REGION", "ap-southeast-1")
	t.Setenv("S3_USE_SSL", "true")
	t.Setenv("S3_FORCE_PATH_STYLE", "false")

	cfg, err = config.Load()
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
