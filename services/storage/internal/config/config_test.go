package config_test

import (
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/fx"
	"go.uber.org/fx/fxtest"

	"github.com/starsrc/Aether/pkg/observability/slogx"
	"github.com/starsrc/Aether/pkg/observability/telemetry"
	"github.com/starsrc/Aether/pkg/serverx"
	"github.com/starsrc/Aether/pkg/tx"
	"github.com/starsrc/bowl/services/storage/internal/config"
)

// cleanConfigEnv 清理测试涉及的环境变量，并在测试结束时自动还原。
func cleanConfigEnv(t *testing.T) {
	t.Helper()
	keys := []string{
		"DB_DSN", "DB_MAX_CONNS", "DB_MIN_CONNS", "DB_MAX_CONN_LIFETIME", "DB_MAX_CONN_IDLE_TIME", "DB_HEALTH_CHECK_PERIOD", "DB_TRACING_ENABLED",
		"SERVER_HOST", "SERVER_PORT", "SERVER_READ_HEADER_TIMEOUT", "SERVER_STOP_TIMEOUT", "SERVER_READ_MAX_BYTES",
		"LOG_LEVEL", "LOG_FORMAT", "LOG_CONNECT_MESSAGE", "LOG_OTEL_RECORD_TRACE_FLAGS", "LOG_OTEL_SAMPLED_ONLY",
		"OTEL_ENABLED", "OTEL_SERVICE_NAME", "OTEL_EXPORTER_OTLP_ENDPOINT", "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT",
		"OTEL_EXPORTER_OTLP_INSECURE", "OTEL_EXPORTER_OTLP_TRACES_INSECURE", "OTEL_SAMPLE_RATIO", "OTEL_SHUTDOWN_TIMEOUT", "OTEL_TRUST_REMOTE",
		"STORAGE_SESSION_LEASE_TTL", "STORAGE_ATIME_MODE",
		"STORAGE_ENTRY_TIMEOUT_SECONDS", "STORAGE_ATTR_TIMEOUT_SECONDS", "STORAGE_DIR_ENTRY_TIMEOUT_SECONDS",
		"STORAGE_OPEN_KEEP_CACHE", "STORAGE_OPEN_DIR_CACHE",
	}
	for _, key := range keys {
		oldVal, exists := os.LookupEnv(key)
		os.Unsetenv(key)
		k := key
		if exists {
			v := oldVal
			t.Cleanup(func() {
				_ = os.Setenv(k, v)
			})
		} else {
			t.Cleanup(func() {
				_ = os.Unsetenv(k)
			})
		}
	}
}

// 验证类型别名定义与 Aether 共享包的兼容性
func TestTypeAliases(t *testing.T) {
	var (
		_ config.ServerConfig    = serverx.ServerConfig{}
		_ config.LogConfig       = slogx.LogConfig{}
		_ config.TelemetryConfig = telemetry.TelemetryConfig{}
		_ config.DBPoolConfig    = tx.PoolConfig{}
	)
}

func TestConfig_Load_Defaults(t *testing.T) {
	cleanConfigEnv(t)
	tempDir := t.TempDir()
	t.Chdir(tempDir)

	t.Setenv("DB_DSN", "postgres://user:pass@localhost:5432/bowl")

	cfg, err := config.Load()
	require.NoError(t, err)
	require.NotNil(t, cfg)

	// 验证未显式配置时的默认端口为 9000
	assert.Equal(t, 9000, cfg.Port())
	assert.Equal(t, "0.0.0.0:9000", cfg.Addr())
	assert.Equal(t, 10*time.Second, cfg.ReadHeaderTimeout())
	assert.Equal(t, 10*time.Second, cfg.StopTimeout())
	assert.Equal(t, 7*time.Second, cfg.ShutdownTimeout())
	assert.Equal(t, int64(131072), cfg.ReadMaxBytes())

	// 验证未显式配置时的默认服务名为 "bowl-storage"
	assert.Equal(t, "bowl-storage", cfg.ServiceName())
	assert.Equal(t, "localhost:4317", cfg.Endpoint())
	assert.False(t, cfg.Enabled())
	assert.True(t, cfg.Insecure())
	assert.InDelta(t, 1.0, cfg.SampleRatio(), 0.0001)

	// 验证数据库连接池默认配置
	assert.Equal(t, "postgres://user:pass@localhost:5432/bowl", cfg.DSN)
	assert.Equal(t, int32(0), cfg.MaxConns)
	assert.Equal(t, int32(0), cfg.MinConns)
	assert.Equal(t, time.Hour, cfg.MaxConnLifetime)
	assert.Equal(t, 30*time.Minute, cfg.MaxConnIdleTime)
	assert.Equal(t, time.Minute, cfg.HealthCheckPeriod)
	assert.True(t, cfg.TracingEnabled)

	// 验证日志默认配置
	assert.Equal(t, "info", cfg.Level())
	assert.Equal(t, "json", cfg.Format())
	assert.Equal(t, slog.LevelInfo, cfg.SlogLevel())
	assert.Equal(t, "handled rpc", cfg.Message())

	// 验证元数据领域默认配置
	assert.Equal(t, 60*time.Second, cfg.Meta.SessionLeaseTTL())
	assert.Equal(t, "noatime", cfg.Meta.AtimeMode())
	assert.Equal(t, 1.0, cfg.Meta.EntryTimeoutSeconds())
	assert.Equal(t, 1.0, cfg.Meta.AttrTimeoutSeconds())
	assert.Equal(t, 1.0, cfg.Meta.DirEntryTimeoutSeconds())
	assert.False(t, cfg.Meta.OpenKeepCache())
	assert.False(t, cfg.Meta.OpenDirCache())
}

func TestConfig_Load_EnvOverrides(t *testing.T) {
	cleanConfigEnv(t)
	tempDir := t.TempDir()
	t.Chdir(tempDir)

	// 设置各类环境变量
	t.Setenv("DB_DSN", "postgres://custom:pass@pg.internal:5432/custom_db")
	t.Setenv("DB_MAX_CONNS", "25")
	t.Setenv("DB_MIN_CONNS", "5")
	t.Setenv("DB_TRACING_ENABLED", "false")

	t.Setenv("SERVER_HOST", "127.0.0.1")
	t.Setenv("SERVER_PORT", "9090")
	t.Setenv("SERVER_READ_HEADER_TIMEOUT", "15s")
	t.Setenv("SERVER_STOP_TIMEOUT", "20s")
	t.Setenv("SERVER_READ_MAX_BYTES", "65536")

	t.Setenv("OTEL_ENABLED", "true")
	t.Setenv("OTEL_SERVICE_NAME", "custom-storage-svc")
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "otel.internal:4317")
	t.Setenv("OTEL_SAMPLE_RATIO", "0.5")

	t.Setenv("LOG_LEVEL", "debug")
	t.Setenv("LOG_FORMAT", "text")

	t.Setenv("STORAGE_SESSION_LEASE_TTL", "90s")
	t.Setenv("STORAGE_ATIME_MODE", "strictatime")

	cfg, err := config.Load()
	require.NoError(t, err)
	require.NotNil(t, cfg)

	// 验证环境变量覆盖有效
	assert.Equal(t, "postgres://custom:pass@pg.internal:5432/custom_db", cfg.DSN)
	assert.Equal(t, int32(25), cfg.MaxConns)
	assert.Equal(t, int32(5), cfg.MinConns)
	assert.False(t, cfg.TracingEnabled)

	assert.Equal(t, 9090, cfg.Port())
	assert.Equal(t, "127.0.0.1:9090", cfg.Addr())
	assert.Equal(t, 15*time.Second, cfg.ReadHeaderTimeout())
	assert.Equal(t, 20*time.Second, cfg.StopTimeout())
	assert.Equal(t, 14*time.Second, cfg.ShutdownTimeout())
	assert.Equal(t, int64(65536), cfg.ReadMaxBytes())

	assert.True(t, cfg.Enabled())
	assert.Equal(t, "custom-storage-svc", cfg.ServiceName())
	assert.Equal(t, "otel.internal:4317", cfg.Endpoint())
	assert.InDelta(t, 0.5, cfg.SampleRatio(), 0.0001)

	assert.Equal(t, "debug", cfg.Level())
	assert.Equal(t, "text", cfg.Format())
	assert.Equal(t, slog.LevelDebug, cfg.SlogLevel())

	assert.Equal(t, 90*time.Second, cfg.Meta.SessionLeaseTTL())
	assert.Equal(t, "strictatime", cfg.Meta.AtimeMode())
}

func TestConfig_Load_DotenvFile(t *testing.T) {
	cleanConfigEnv(t)
	tempDir := t.TempDir()
	t.Chdir(tempDir)

	envContent := "DB_DSN=postgres://dotenv:pass@localhost:5432/dotenv_db\n" +
		"SERVER_PORT=9005\n" +
		"OTEL_SERVICE_NAME=dotenv-storage\n"
	require.NoError(t, os.WriteFile(filepath.Join(tempDir, ".env"), []byte(envContent), 0o600))

	cfg, err := config.Load()
	require.NoError(t, err)
	require.NotNil(t, cfg)

	assert.Equal(t, "postgres://dotenv:pass@localhost:5432/dotenv_db", cfg.DSN)
	assert.Equal(t, "0.0.0.0:9005", cfg.Addr())
	assert.Equal(t, "dotenv-storage", cfg.ServiceName())
}

func TestConfig_Load_CustomFile(t *testing.T) {
	cleanConfigEnv(t)
	tempDir := t.TempDir()
	t.Chdir(tempDir)

	customEnvPath := filepath.Join(tempDir, "custom.env")
	envContent := "DB_DSN=postgres://custom:pass@localhost:5432/custom_db\n" +
		"SERVER_PORT=9010\n"
	require.NoError(t, os.WriteFile(customEnvPath, []byte(envContent), 0o600))

	cfg, err := config.Load(customEnvPath)
	require.NoError(t, err)
	require.NotNil(t, cfg)

	assert.Equal(t, "postgres://custom:pass@localhost:5432/custom_db", cfg.DSN)
	assert.Equal(t, "0.0.0.0:9010", cfg.Addr())
}

func TestCleanConfigEnv_RestoresUnsetAfterDotenvLoad(t *testing.T) {
	targetKeys := []string{"DB_DSN", "SERVER_PORT", "OTEL_SERVICE_NAME"}

	// 1. 父测试中保存并在父测试 t.Cleanup 兜底恢复原始环境，同时临时 Unsetenv
	for _, key := range targetKeys {
		k := key
		origVal, exists := os.LookupEnv(k)
		if exists {
			t.Cleanup(func() {
				_ = os.Setenv(k, origVal)
			})
		} else {
			t.Cleanup(func() {
				_ = os.Unsetenv(k)
			})
		}
		require.NoError(t, os.Unsetenv(k))
		_, stillExists := os.LookupEnv(k)
		require.False(t, stillExists, "key %s should be unset initially in parent test", k)
	}

	// 2. 在 t.Run("dotenv_load_subtest", ...) 子测试内调用 cleanConfigEnv(t) 并通过临时 .env 文件调用 config.Load(...)
	t.Run("dotenv_load_subtest", func(t *testing.T) {
		cleanConfigEnv(t)
		tempDir := t.TempDir()
		t.Chdir(tempDir)

		envContent := "DB_DSN=postgres://clean_test:pass@localhost:5432/clean_db\n" +
			"SERVER_PORT=9007\n" +
			"OTEL_SERVICE_NAME=clean-test-storage\n"
		require.NoError(t, os.WriteFile(filepath.Join(tempDir, ".env"), []byte(envContent), 0o600))

		cfg, err := config.Load()
		require.NoError(t, err)
		require.NotNil(t, cfg)

		// 在子测试内断言 os.LookupEnv 已变为 exists == true（证明 godotenv 确实写入了进程环境）
		for _, key := range targetKeys {
			_, exists := os.LookupEnv(key)
			assert.True(t, exists, "key %s should exist in process env during subtest", key)
		}
	})

	// 3. 待 t.Run 子测试退出（触发子测试的 t.Cleanup）后，在父测试中断言上述各键经 os.LookupEnv 检查严格恢复为 exists == false！
	for _, key := range targetKeys {
		val, exists := os.LookupEnv(key)
		assert.False(t, exists, "key %s should be restored to unset after subtest, but got %q", key, val)
	}
}

func TestConfig_Load_Errors(t *testing.T) {
	t.Run("不存在的env文件报错", func(t *testing.T) {
		cleanConfigEnv(t)
		_, err := config.Load("non_existent.env")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "load env file:")
	})

	t.Run("环境变量类型错误报错", func(t *testing.T) {
		cleanConfigEnv(t)
		tempDir := t.TempDir()
		t.Chdir(tempDir)

		t.Setenv("DB_DSN", "postgres://localhost/bowl")
		t.Setenv("SERVER_PORT", "not_an_int")

		_, err := config.Load()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "parse environment variables:")
	})

	t.Run("缺失DB_DSN必填项报错", func(t *testing.T) {
		cleanConfigEnv(t)
		tempDir := t.TempDir()
		t.Chdir(tempDir)

		_, err := config.Load()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "DB_DSN")
	})

	t.Run("单端口服务端校验失败报错", func(t *testing.T) {
		cleanConfigEnv(t)
		tempDir := t.TempDir()
		t.Chdir(tempDir)

		t.Setenv("DB_DSN", "postgres://localhost/bowl")
		t.Setenv("SERVER_PORT", "70000")

		_, err := config.Load()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "validate server config:")
	})

	t.Run("元数据配置校验失败报错", func(t *testing.T) {
		cleanConfigEnv(t)
		tempDir := t.TempDir()
		t.Chdir(tempDir)

		t.Setenv("DB_DSN", "postgres://localhost/bowl")
		t.Setenv("STORAGE_ATIME_MODE", "invalid_atime_mode")

		_, err := config.Load()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "validate meta config:")
	})

	t.Run("遥测配置校验失败报错", func(t *testing.T) {
		cleanConfigEnv(t)
		tempDir := t.TempDir()
		t.Chdir(tempDir)

		t.Setenv("DB_DSN", "postgres://localhost/bowl")
		t.Setenv("OTEL_SAMPLE_RATIO", "1.5") // 采样率必须在 0.0 到 1.0 之间

		_, err := config.Load()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "validate telemetry config:")
	})
}

func TestConfig_Validate(t *testing.T) {
	validConfig := func() *config.Config {
		return &config.Config{
			ServerConfig: serverx.ServerConfig{
				ServerPort: 9000,
			},
			PoolConfig: tx.PoolConfig{
				DSN: "postgres://localhost/bowl",
			},
			TelemetryConfig: telemetry.TelemetryConfig{
				OtelSampleRatio: 1.0,
			},
			Meta: config.MetaConfig{
				SessionLeaseTTLVal:        time.Minute,
				AtimeModeVal:              "noatime",
				EntryTimeoutSecondsVal:    1.0,
				AttrTimeoutSecondsVal:     1.0,
				DirEntryTimeoutSecondsVal: 1.0,
			},
		}
	}

	t.Run("合法配置校验通过", func(t *testing.T) {
		cfg := validConfig()
		assert.NoError(t, cfg.Validate())
	})

	t.Run("ServerConfig端口非法校验失败", func(t *testing.T) {
		cfg := validConfig()
		cfg.ServerPort = 0
		err := cfg.Validate()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "validate server config:")
	})

	t.Run("PoolConfig DSN为空校验失败", func(t *testing.T) {
		cfg := validConfig()
		cfg.DSN = ""
		err := cfg.Validate()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "validate db pool config:")
	})

	t.Run("PoolConfig 连接数为负数校验失败", func(t *testing.T) {
		cfg := validConfig()
		cfg.MaxConns = -1
		err := cfg.Validate()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "validate db pool config:")
	})

	t.Run("TelemetryConfig 采样率超出范围校验失败", func(t *testing.T) {
		cfg := validConfig()
		cfg.OtelSampleRatio = 2.0
		err := cfg.Validate()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "validate telemetry config:")
	})

	t.Run("MetaConfig 非法atime策略校验失败", func(t *testing.T) {
		cfg := validConfig()
		cfg.Meta.AtimeModeVal = "invalid"
		err := cfg.Validate()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "validate meta config:")
	})
}

func TestConfigModule_ProvidesComponents(t *testing.T) {
	rootCfg := &config.Config{
		ServerConfig: serverx.ServerConfig{
			ServerHost: "127.0.0.1",
			ServerPort: 9000,
		},
		PoolConfig: tx.PoolConfig{
			DSN: "postgres://localhost/bowl",
		},
		TelemetryConfig: telemetry.TelemetryConfig{
			OtelServiceName: "bowl-storage",
		},
		LogConfig: slogx.LogConfig{
			LogLevel:  "debug",
			LogFormat: "json",
		},
		Meta: config.MetaConfig{
			SessionLeaseTTLVal:        45 * time.Second,
			AtimeModeVal:              "strictatime",
			EntryTimeoutSecondsVal:    2.5,
			AttrTimeoutSecondsVal:     3.5,
			DirEntryTimeoutSecondsVal: 4.5,
			OpenKeepCacheVal:          true,
			OpenDirCacheVal:           true,
		},
	}

	var (
		extractedLog         slogx.Config
		extractedTelemetry   telemetry.Config
		extractedServerXCfg  serverx.Config
		extractedObsCfg      serverx.ObservabilityConfig
		extractedServerX     serverx.ServerConfig
		extractedServerAlias config.ServerConfig
		extractedPoolCfg     tx.PoolConfig
		extractedPoolAlias   config.DBPoolConfig
		extractedMeta        config.MetaConfig
	)

	app := fxtest.New(
		t,
		fx.Supply(rootCfg),
		config.Module,
		fx.Populate(
			&extractedLog,
			&extractedTelemetry,
			&extractedServerXCfg,
			&extractedObsCfg,
			&extractedServerX,
			&extractedServerAlias,
			&extractedPoolCfg,
			&extractedPoolAlias,
			&extractedMeta,
		),
	)
	require.NoError(t, app.Err())

	assert.Equal(t, slog.LevelDebug, extractedLog.SlogLevel())
	assert.Equal(t, "bowl-storage", extractedTelemetry.ServiceName())
	assert.Equal(t, 10*time.Second, extractedServerXCfg.ReadHeaderTimeout())
	assert.NotNil(t, extractedObsCfg)
	assert.Equal(t, slog.LevelDebug, extractedObsCfg.SlogLevel())
	assert.Equal(t, "127.0.0.1:9000", extractedServerX.Addr())
	assert.Equal(t, "127.0.0.1:9000", extractedServerAlias.Addr())
	assert.Equal(t, "postgres://localhost/bowl", extractedPoolCfg.DSN)
	assert.Equal(t, "postgres://localhost/bowl", extractedPoolAlias.DSN)
	assert.Equal(t, 45*time.Second, extractedMeta.SessionLeaseTTL())
	assert.Equal(t, "strictatime", extractedMeta.AtimeMode())
	assert.Equal(t, 2.5, extractedMeta.EntryTimeoutSeconds())
	assert.Equal(t, 3.5, extractedMeta.AttrTimeoutSeconds())
	assert.Equal(t, 4.5, extractedMeta.DirEntryTimeoutSeconds())
	assert.True(t, extractedMeta.OpenKeepCache())
	assert.True(t, extractedMeta.OpenDirCache())
}
