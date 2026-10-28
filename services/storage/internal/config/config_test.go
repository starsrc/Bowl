package config_test

import (
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/joho/godotenv"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/fx"
	"go.uber.org/fx/fxtest"
	"go.yaml.in/yaml/v3"

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
		"DATA_DIR", "STORAGE_DATA_DIR",
		"STORAGE_GC_INTERVAL",
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
	assert.Equal(t, int64(config.DefaultServerReadMaxBytes), cfg.ReadMaxBytes())

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
	assert.Equal(t, "/tmp/bowl-storage-data", cfg.Meta.DataDir())
	assert.Equal(t, time.Minute, cfg.Meta.GCInterval())
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
	t.Setenv("STORAGE_GC_INTERVAL", "30s")
	t.Setenv("DATA_DIR", "/mnt/nvme0n1/data")

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
	assert.Equal(t, 30*time.Second, cfg.Meta.GCInterval())
	assert.Equal(t, "/mnt/nvme0n1/data", cfg.Meta.DataDir())

	// 测试 STORAGE_DATA_DIR 备用环境变量
	t.Setenv("DATA_DIR", "")
	t.Setenv("STORAGE_DATA_DIR", "/mnt/nvme0n2/storage_data")
	cfg2, err := config.Load()
	require.NoError(t, err)
	assert.Equal(t, "/mnt/nvme0n2/storage_data", cfg2.Meta.DataDir())
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
				DataDirVal:                "/tmp/bowl-storage-data",
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

	t.Run("MetaConfig DataDir为空校验失败", func(t *testing.T) {
		cfg := validConfig()
		cfg.Meta.DataDirVal = "   "
		err := cfg.Validate()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "validate meta config:")
		assert.Contains(t, err.Error(), "invalid data dir")
	})

	t.Run("MetaConfig 负数GCInterval校验失败", func(t *testing.T) {
		cfg := validConfig()
		cfg.Meta.GCIntervalVal = -1 * time.Second
		err := cfg.Validate()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "validate meta config:")
		assert.Contains(t, err.Error(), "invalid gc interval")
	})
}

func TestConfig_Load_GCInterval(t *testing.T) {
	cleanConfigEnv(t)
	tempDir := t.TempDir()
	t.Chdir(tempDir)
	t.Setenv("DB_DSN", "postgres://user:pass@localhost:5432/bowl")

	t.Run("未设置环境变量时默认1m", func(t *testing.T) {
		cfg, err := config.Load()
		require.NoError(t, err)
		assert.Equal(t, time.Minute, cfg.Meta.GCInterval())
	})

	t.Run("自定义合法值40ms", func(t *testing.T) {
		t.Setenv("STORAGE_GC_INTERVAL", "40ms")
		cfg, err := config.Load()
		require.NoError(t, err)
		assert.Equal(t, 40*time.Millisecond, cfg.Meta.GCInterval())
	})

	t.Run("自定义合法值30s", func(t *testing.T) {
		t.Setenv("STORAGE_GC_INTERVAL", "30s")
		cfg, err := config.Load()
		require.NoError(t, err)
		assert.Equal(t, 30*time.Second, cfg.Meta.GCInterval())
	})

	t.Run("设置为0s时采用默认值1m", func(t *testing.T) {
		t.Setenv("STORAGE_GC_INTERVAL", "0s")
		cfg, err := config.Load()
		require.NoError(t, err)
		assert.Equal(t, time.Minute, cfg.Meta.GCInterval())
	})

	t.Run("设置负值时拒绝", func(t *testing.T) {
		t.Setenv("STORAGE_GC_INTERVAL", "-10s")
		_, err := config.Load()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "invalid gc interval")
	})

	t.Run("设置非法字符串时拒绝", func(t *testing.T) {
		t.Setenv("STORAGE_GC_INTERVAL", "invalid-duration")
		_, err := config.Load()
		require.Error(t, err)
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
			DataDirVal:                "/custom/storage/data",
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
	assert.Equal(t, "/custom/storage/data", extractedMeta.DataDir())
}

// 验证默认 SERVER_READ_MAX_BYTES 配置足够承载 4 MiB Write 请求（至少 8 MiB）
func TestLoad_DefaultServerReadMaxBytesSupports4MiBWrite(t *testing.T) {
	cleanConfigEnv(t)
	tempDir := t.TempDir()
	t.Chdir(tempDir)

	t.Setenv("DB_DSN", "postgres://user:pass@localhost:5432/bowl")
	t.Setenv("SERVER_READ_MAX_BYTES", "")

	cfg, err := config.Load()
	require.NoError(t, err)
	require.NotNil(t, cfg)

	// 断言 cfg.Server.ReadMaxBytes() >= 8*1024*1024（8 MiB）以容纳 4 MiB 载荷及 JSON 编码膨胀
	assert.GreaterOrEqual(t, cfg.ReadMaxBytes(), int64(8*1024*1024), "默认 ServerReadMaxBytes 必须至少为 8 MiB 以容纳 4 MiB 数据载荷")
}

// validateComposeContent 纯字节校验辅助函数，校验 Compose 配置中 storage-service 的持久化与数据目录配置。
func validateComposeContent(data []byte) error {
	var compose map[string]any
	if err := yaml.Unmarshal(data, &compose); err != nil {
		return fmt.Errorf("yaml unmarshal error: %w", err)
	}

	// 1. 顶层 volumes 必须声明 storage_data
	volumes, ok := compose["volumes"].(map[string]any)
	if !ok || volumes == nil {
		return fmt.Errorf("top-level volumes must be defined")
	}
	if _, hasStorageData := volumes["storage_data"]; !hasStorageData {
		return fmt.Errorf("top-level volumes must declare storage_data")
	}

	// 2. services.storage-service 必须存在且为 map
	services, ok := compose["services"].(map[string]any)
	if !ok || services == nil {
		return fmt.Errorf("services must be defined")
	}
	rawSvc, ok := services["storage-service"]
	if !ok || rawSvc == nil {
		return fmt.Errorf("services.storage-service must be defined")
	}
	svc, ok := rawSvc.(map[string]any)
	if !ok || svc == nil {
		return fmt.Errorf("services.storage-service must be a map structure")
	}

	// 3. 解析 services.storage-service.environment 中的有效数据目录 effectiveDataDir
	// 遵循 config.MetaConfig.DataDir() 优先级：优先 DATA_DIR，其次 STORAGE_DATA_DIR
	var dataDirVal, storageDataDirVal string
	var hasDataDir, hasStorageDataDir bool

	if envMap, ok := svc["environment"].(map[string]any); ok {
		if v, exists := envMap["DATA_DIR"]; exists && v != nil {
			dataDirVal = fmt.Sprint(v)
			hasDataDir = true
		}
		if v, exists := envMap["STORAGE_DATA_DIR"]; exists && v != nil {
			storageDataDirVal = fmt.Sprint(v)
			hasStorageDataDir = true
		}
	} else if envList, ok := svc["environment"].([]any); ok {
		for _, item := range envList {
			s := fmt.Sprint(item)
			if strings.HasPrefix(s, "DATA_DIR=") {
				dataDirVal = strings.TrimPrefix(s, "DATA_DIR=")
				hasDataDir = true
			} else if strings.HasPrefix(s, "STORAGE_DATA_DIR=") {
				storageDataDirVal = strings.TrimPrefix(s, "STORAGE_DATA_DIR=")
				hasStorageDataDir = true
			}
		}
	}

	var effectiveDataDir string
	if hasDataDir && strings.TrimSpace(dataDirVal) != "" {
		effectiveDataDir = strings.TrimSpace(dataDirVal)
	} else if hasStorageDataDir && strings.TrimSpace(storageDataDirVal) != "" {
		effectiveDataDir = strings.TrimSpace(storageDataDirVal)
	} else if hasDataDir {
		effectiveDataDir = dataDirVal
	} else if hasStorageDataDir {
		effectiveDataDir = storageDataDirVal
	} else {
		return fmt.Errorf("services.storage-service environment must configure DATA_DIR or STORAGE_DATA_DIR")
	}

	cleanDataDir := path.Clean(strings.TrimSpace(effectiveDataDir))
	if !strings.HasPrefix(cleanDataDir, "/") {
		return fmt.Errorf("effective data dir %q must be an absolute path starting with /", effectiveDataDir)
	}
	if cleanDataDir == "/tmp" || strings.HasPrefix(cleanDataDir, "/tmp/") {
		return fmt.Errorf("effective data dir %q must not reside in /tmp", cleanDataDir)
	}

	// 4. 解析 services.storage-service.volumes 中绑定 storage_data 的卷条目
	volList, ok := svc["volumes"].([]any)
	if !ok || len(volList) == 0 {
		return fmt.Errorf("services.storage-service.volumes must be defined and non-empty")
	}

	var foundStorageData bool
	var mountTarget string
	var isReadOnly bool

	for _, v := range volList {
		switch item := v.(type) {
		case string:
			parts := strings.Split(item, ":")
			if len(parts) >= 2 && parts[0] == "storage_data" {
				foundStorageData = true
				mountTarget = parts[1]
				if len(parts) >= 3 {
					for _, mode := range strings.Split(parts[2], ",") {
						if strings.TrimSpace(mode) == "ro" {
							isReadOnly = true
						}
					}
				}
				break
			}
		case map[string]any:
			source := fmt.Sprint(item["source"])
			if source == "storage_data" {
				foundStorageData = true
				if tgt, exists := item["target"]; exists && tgt != nil {
					mountTarget = fmt.Sprint(tgt)
				}
				if ro, ok := item["read_only"].(bool); ok && ro {
					isReadOnly = true
				} else if roStr, ok := item["read_only"].(string); ok && strings.TrimSpace(roStr) == "true" {
					isReadOnly = true
				}
				break
			}
		}
		if foundStorageData {
			break
		}
	}

	if !foundStorageData {
		return fmt.Errorf("services.storage-service.volumes must mount storage_data named volume")
	}

	if isReadOnly {
		return fmt.Errorf("storage_data volume mount must not be read-only")
	}

	cleanMountTarget := path.Clean(strings.TrimSpace(mountTarget))
	if cleanMountTarget == "." || !strings.HasPrefix(cleanMountTarget, "/") {
		return fmt.Errorf("storage_data mount target %q must be an absolute path starting with /", mountTarget)
	}

	if cleanDataDir != cleanMountTarget && !strings.HasPrefix(cleanDataDir, cleanMountTarget+"/") {
		return fmt.Errorf("effective data dir %q is not within storage_data mount target %q", cleanDataDir, cleanMountTarget)
	}

	return nil
}

// 验证 docker-compose.yaml / docker-compose.yaml.example / services/storage/.env.example 的持久卷及环境变量对齐
func TestDockerCompose_StorageDataVolumeAndEnvExampleAligned(t *testing.T) {
	_, currentFile, _, ok := runtime.Caller(0)
	require.True(t, ok)
	configDir := filepath.Dir(currentFile)
	// configDir 为 services/storage/internal/config
	// 往上 4 级目录到达仓库根目录，往上 2 级目录到达 services/storage
	repoRoot := filepath.Clean(filepath.Join(configDir, "..", "..", "..", ".."))

	composePath := filepath.Join(repoRoot, "docker-compose.yaml")
	composeExamplePath := filepath.Join(repoRoot, "docker-compose.yaml.example")
	envExamplePath := filepath.Join(configDir, "..", "..", ".env.example")

	// 1. 对受版本控制的权威文件 docker-compose.yaml.example 保持强制硬断言
	t.Run("docker-compose.yaml.example", func(t *testing.T) {
		exampleData, err := os.ReadFile(composeExamplePath)
		require.NoError(t, err, "权威文件 docker-compose.yaml.example 必须存在且可读: %s", composeExamplePath)
		require.NoError(t, validateComposeContent(exampleData), "docker-compose.yaml.example 校验失败")
	})

	// 2. 在 t.TempDir() 中从 docker-compose.yaml.example 复制生成临时 docker-compose.yaml 夹具（验证 init 任务生成产物）并校验
	t.Run("temp_docker-compose.yaml_fixture", func(t *testing.T) {
		exampleData, err := os.ReadFile(composeExamplePath)
		require.NoError(t, err, "权威文件 docker-compose.yaml.example 必须存在且可读")
		tempComposePath := filepath.Join(t.TempDir(), "docker-compose.yaml")
		require.NoError(t, os.WriteFile(tempComposePath, exampleData, 0o600))

		tempData, err := os.ReadFile(tempComposePath)
		require.NoError(t, err, "临时 docker-compose.yaml 必须可读: %s", tempComposePath)
		require.NoError(t, validateComposeContent(tempData), "临时生成的 docker-compose.yaml 校验失败")
	})

	// 3. 工作树根目录下的本地 docker-compose.yaml：若存在则校验，若不存在（.gitignore忽略）则安全跳过
	t.Run("local_docker-compose.yaml", func(t *testing.T) {
		if _, err := os.Stat(composePath); err == nil {
			localData, err := os.ReadFile(composePath)
			require.NoError(t, err, "本地 docker-compose.yaml 存在但读取失败: %s", composePath)
			require.NoError(t, validateComposeContent(localData), "本地 docker-compose.yaml 校验失败: %s", composePath)
		} else if errors.Is(err, os.ErrNotExist) {
			t.Logf("本地 docker-compose.yaml 不存在，安全跳过本地文件校验: %s", composePath)
		} else {
			require.NoError(t, err, "访问本地 docker-compose.yaml 发生意外错误: %s", composePath)
		}
	})

	// 4. 对受版本控制的权威文件 services/storage/.env.example 保持强制硬断言
	t.Run("services/storage/.env.example", func(t *testing.T) {
		envData, err := os.ReadFile(envExamplePath)
		require.NoError(t, err, "文件必须存在且可读: %s", envExamplePath)
		envMap, err := godotenv.Unmarshal(string(envData))
		require.NoError(t, err, "解析 .env.example 失败")
		valStr, ok := envMap["SERVER_READ_MAX_BYTES"]
		require.True(t, ok, "services/storage/.env.example 必须包含 SERVER_READ_MAX_BYTES")
		val, err := strconv.ParseInt(valStr, 10, 64)
		require.NoError(t, err, "SERVER_READ_MAX_BYTES 必须为有效整数")
		assert.GreaterOrEqual(t, val, int64(8*1024*1024), "services/storage/.env.example 中 SERVER_READ_MAX_BYTES 必须 >= 8388608")
	})

	// 5. validateComposeContent 负例表驱动子测试
	t.Run("validateComposeContent_NegativeCases", func(t *testing.T) {
		tests := []struct {
			name        string
			yamlContent string
			errContains string
		}{
			{
				name: "挂载目标不匹配",
				yamlContent: `
services:
  storage-service:
    environment:
      STORAGE_DATA_DIR: "/var/lib/bowl/data"
    volumes:
      - storage_data:/unrelated/path
volumes:
  storage_data:
`,
				errContains: "is not within storage_data mount target",
			},
			{
				name: "只读挂载",
				yamlContent: `
services:
  storage-service:
    environment:
      STORAGE_DATA_DIR: "/var/lib/bowl/data"
    volumes:
      - storage_data:/var/lib/bowl/data:ro
volumes:
  storage_data:
`,
				errContains: "must not be read-only",
			},
			{
				name: "长语法只读挂载",
				yamlContent: `
services:
  storage-service:
    environment:
      STORAGE_DATA_DIR: "/var/lib/bowl/data"
    volumes:
      - type: volume
        source: storage_data
        target: /var/lib/bowl/data
        read_only: true
volumes:
  storage_data:
`,
				errContains: "must not be read-only",
			},
			{
				name: "DATA_DIR高优先级覆盖指向临时层",
				yamlContent: `
services:
  storage-service:
    environment:
      DATA_DIR: "/tmp/ephemeral"
      STORAGE_DATA_DIR: "/var/lib/bowl/data"
    volumes:
      - storage_data:/var/lib/bowl/data
volumes:
  storage_data:
`,
				errContains: "must not reside in /tmp",
			},
			{
				name: "顶层volumes未声明storage_data",
				yamlContent: `
services:
  storage-service:
    environment:
      STORAGE_DATA_DIR: "/var/lib/bowl/data"
    volumes:
      - storage_data:/var/lib/bowl/data
volumes:
  postgres_data:
`,
				errContains: "top-level volumes must declare storage_data",
			},
			{
				name: "缺失顶层volumes",
				yamlContent: `
services:
  storage-service:
    environment:
      STORAGE_DATA_DIR: "/var/lib/bowl/data"
    volumes:
      - storage_data:/var/lib/bowl/data
`,
				errContains: "top-level volumes must be defined",
			},
			{
				name: "storage-service缺少storage_data挂载",
				yamlContent: `
services:
  storage-service:
    environment:
      STORAGE_DATA_DIR: "/var/lib/bowl/data"
    volumes:
      - postgres_data:/var/lib/postgresql
volumes:
  storage_data:
`,
				errContains: "must mount storage_data named volume",
			},
			{
				name: "缺少数据目录配置",
				yamlContent: `
services:
  storage-service:
    environment:
      SERVER_PORT: 9000
    volumes:
      - storage_data:/var/lib/bowl/data
volumes:
  storage_data:
`,
				errContains: "must configure DATA_DIR or STORAGE_DATA_DIR",
			},
		}

		for _, tc := range tests {
			t.Run(tc.name, func(t *testing.T) {
				err := validateComposeContent([]byte(tc.yamlContent))
				require.Error(t, err, "测试用例 [%s] 必须返回非 nil 错误", tc.name)
				if tc.errContains != "" {
					assert.Contains(t, err.Error(), tc.errContains, "错误信息应包含预期内容")
				}
			})
		}
	})

	// 6. validateComposeContent 正例子测试
	t.Run("validateComposeContent_PositiveCases", func(t *testing.T) {
		t.Run("长语法卷挂载有效", func(t *testing.T) {
			yamlContent := `
services:
  storage-service:
    environment:
      STORAGE_DATA_DIR: "/var/lib/bowl/data"
    volumes:
      - type: volume
        source: storage_data
        target: /var/lib/bowl/data
        read_only: false
volumes:
  storage_data:
`
			require.NoError(t, validateComposeContent([]byte(yamlContent)))
		})

		t.Run("数据目录位于挂载目标的子目录下有效", func(t *testing.T) {
			yamlContent := `
services:
  storage-service:
    environment:
      - STORAGE_DATA_DIR=/var/lib/bowl/data/subpath
    volumes:
      - storage_data:/var/lib/bowl/data:rw
volumes:
  storage_data:
`
			require.NoError(t, validateComposeContent([]byte(yamlContent)))
		})
	})
}
