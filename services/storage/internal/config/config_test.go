package config_test

import (
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/caarlos0/env/v11"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/fx"
	"go.uber.org/fx/fxtest"

	"github.com/starsrc/Aether/pkg/observability/slogx"
	"github.com/starsrc/Aether/pkg/observability/telemetry"
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
		if exists {
			k := key
			v := oldVal
			t.Cleanup(func() {
				_ = os.Setenv(k, v)
			})
		}
	}
}

func TestServerConfig_Addr(t *testing.T) {
	cases := []struct {
		name     string
		host     string
		port     int
		expected string
	}{
		{name: "标准IPv4与默认端口", host: "0.0.0.0", port: 9000, expected: "0.0.0.0:9000"},
		{name: "本地回路与自定义端口", host: "127.0.0.1", port: 8080, expected: "127.0.0.1:8080"},
		{name: "IPv6地址与端口", host: "::1", port: 9000, expected: "[::1]:9000"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := config.ServerConfig{Host: tc.host, Port: tc.port}
			assert.Equal(t, tc.expected, cfg.Addr())
		})
	}
}

func TestServerConfig_TimeoutsAndReadMaxBytes(t *testing.T) {
	t.Run("默认停机预算70%折算与自定义读取限制", func(t *testing.T) {
		cfg := config.ServerConfig{
			ReadHeaderTimeoutVal: 5 * time.Second,
			StopTimeoutVal:       10 * time.Second,
			ReadMaxBytesVal:      65536,
		}

		assert.Equal(t, 5*time.Second, cfg.ReadHeaderTimeout())
		assert.Equal(t, 10*time.Second, cfg.StopTimeout())
		assert.Equal(t, int64(65536), cfg.ReadMaxBytes())
		// StopTimeout 为 10s 时，70% 折算结果为 7s
		assert.Equal(t, 7*time.Second, cfg.ShutdownTimeout())
	})

	t.Run("不同停机超时时间的70%动态折算", func(t *testing.T) {
		cfg := config.ServerConfig{
			StopTimeoutVal: 20 * time.Second,
		}
		// 20s * 7 / 10 = 14s
		assert.Equal(t, 14*time.Second, cfg.ShutdownTimeout())
	})

	t.Run("测试显式覆盖停机预算优先于动态折算", func(t *testing.T) {
		var cfg config.ServerConfig
		cfg.StopTimeoutVal = 10 * time.Second
		cfg.SetShutdownTimeoutForTest(4 * time.Second)

		assert.Equal(t, 4*time.Second, cfg.ShutdownTimeout())
	})

	t.Run("ReadMaxBytes非正数时回退至默认131072", func(t *testing.T) {
		cfgZero := config.ServerConfig{ReadMaxBytesVal: 0}
		assert.Equal(t, config.DefaultReadMaxBytes, cfgZero.ReadMaxBytes())

		cfgNegative := config.ServerConfig{ReadMaxBytesVal: -1}
		assert.Equal(t, config.DefaultReadMaxBytes, cfgNegative.ReadMaxBytes())
	})
}

func TestServerConfig_Validate(t *testing.T) {
	testCases := []struct {
		name        string
		port        int
		expectError bool
		errContains string
	}{
		{name: "合法端口9000", port: 9000, expectError: false},
		{name: "合法端口边界1", port: 1, expectError: false},
		{name: "合法端口边界65535", port: 65535, expectError: false},
		{name: "非法端口0", port: 0, expectError: true, errContains: "invalid server port 0"},
		{name: "非法端口负数", port: -1, expectError: true, errContains: "invalid server port -1"},
		{name: "非法端口溢出65536", port: 65536, expectError: true, errContains: "invalid server port 65536"},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := config.ServerConfig{Port: tc.port}
			err := cfg.Validate()
			if tc.expectError {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tc.errContains)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestServerConfig_Parsing(t *testing.T) {
	t.Run("解析默认环境变量值", func(t *testing.T) {
		cleanConfigEnv(t)
		var cfg config.ServerConfig
		err := env.Parse(&cfg)
		require.NoError(t, err)

		assert.Equal(t, config.DefaultServerHost, cfg.Host)
		assert.Equal(t, config.DefaultServerPort, cfg.Port)
		assert.Equal(t, "0.0.0.0:9000", cfg.Addr())
		assert.Equal(t, 10*time.Second, cfg.ReadHeaderTimeout())
		assert.Equal(t, 10*time.Second, cfg.StopTimeout())
		assert.Equal(t, int64(131072), cfg.ReadMaxBytes())
		assert.Equal(t, 7*time.Second, cfg.ShutdownTimeout())
		assert.NoError(t, cfg.Validate())
	})

	t.Run("解析自定义环境变量值", func(t *testing.T) {
		cleanConfigEnv(t)
		t.Setenv("SERVER_HOST", "127.0.0.1")
		t.Setenv("SERVER_PORT", "9999")
		t.Setenv("SERVER_READ_HEADER_TIMEOUT", "15s")
		t.Setenv("SERVER_STOP_TIMEOUT", "20s")
		t.Setenv("SERVER_READ_MAX_BYTES", "262144")

		var cfg config.ServerConfig
		err := env.Parse(&cfg)
		require.NoError(t, err)

		assert.Equal(t, "127.0.0.1", cfg.Host)
		assert.Equal(t, 9999, cfg.Port)
		assert.Equal(t, "127.0.0.1:9999", cfg.Addr())
		assert.Equal(t, 15*time.Second, cfg.ReadHeaderTimeout())
		assert.Equal(t, 20*time.Second, cfg.StopTimeout())
		assert.Equal(t, int64(262144), cfg.ReadMaxBytes())
		assert.Equal(t, 14*time.Second, cfg.ShutdownTimeout())
		assert.NoError(t, cfg.Validate())
	})
}

func TestDBPoolConfig_DefaultsAndParsing(t *testing.T) {
	t.Run("当仅设置DB_DSN时解析默认值", func(t *testing.T) {
		cleanConfigEnv(t)
		t.Setenv("DB_DSN", "postgres://user:pass@localhost:5432/bowl")

		var cfg config.DBPoolConfig
		err := env.Parse(&cfg)
		require.NoError(t, err)

		assert.Equal(t, "postgres://user:pass@localhost:5432/bowl", cfg.DSN())
		assert.Equal(t, int32(0), cfg.MaxConns())
		assert.Equal(t, int32(0), cfg.MinConns())
		assert.Equal(t, time.Hour, cfg.MaxConnLifetime())
		assert.Equal(t, 30*time.Minute, cfg.MaxConnIdleTime())
		assert.Equal(t, time.Minute, cfg.HealthCheckPeriod())
		assert.True(t, cfg.TracingEnabled())
	})

	t.Run("解析自定义环境变量值", func(t *testing.T) {
		cleanConfigEnv(t)
		t.Setenv("DB_DSN", "postgres://custom:pass@remote:5432/storage_db")
		t.Setenv("DB_MAX_CONNS", "50")
		t.Setenv("DB_MIN_CONNS", "10")
		t.Setenv("DB_MAX_CONN_LIFETIME", "2h")
		t.Setenv("DB_MAX_CONN_IDLE_TIME", "15m")
		t.Setenv("DB_HEALTH_CHECK_PERIOD", "30s")
		t.Setenv("DB_TRACING_ENABLED", "false")

		var cfg config.DBPoolConfig
		err := env.Parse(&cfg)
		require.NoError(t, err)

		assert.Equal(t, "postgres://custom:pass@remote:5432/storage_db", cfg.DSN())
		assert.Equal(t, int32(50), cfg.MaxConns())
		assert.Equal(t, int32(10), cfg.MinConns())
		assert.Equal(t, 2*time.Hour, cfg.MaxConnLifetime())
		assert.Equal(t, 15*time.Minute, cfg.MaxConnIdleTime())
		assert.Equal(t, 30*time.Second, cfg.HealthCheckPeriod())
		assert.False(t, cfg.TracingEnabled())
	})

	t.Run("缺失必填DB_DSN时报错", func(t *testing.T) {
		cleanConfigEnv(t)
		var cfg config.DBPoolConfig
		err := env.Parse(&cfg)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "DB_DSN")
	})

	t.Run("DB_DSN为空字符串时报错", func(t *testing.T) {
		cleanConfigEnv(t)
		t.Setenv("DB_DSN", "")
		var cfg config.DBPoolConfig
		err := env.Parse(&cfg)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "DB_DSN")
	})
}

func TestTelemetryConfig_Endpoint_Priority(t *testing.T) {
	t.Run("链路追踪专属端点非空时优先使用专属端点", func(t *testing.T) {
		cfg := config.TelemetryConfig{
			EndpointVal:       "localhost:4317",
			TracesEndpointVal: "custom-jaeger:4317",
		}
		assert.Equal(t, "custom-jaeger:4317", cfg.Endpoint())
	})

	t.Run("链路追踪专属端点为空时回退至基础端点", func(t *testing.T) {
		cfg := config.TelemetryConfig{
			EndpointVal:       "localhost:4317",
			TracesEndpointVal: "",
		}
		assert.Equal(t, "localhost:4317", cfg.Endpoint())
	})
}

func TestTelemetryConfig_Insecure_Priority(t *testing.T) {
	t.Run("专属非安全传输标志显式设置为true时优先使用", func(t *testing.T) {
		insecure := true
		cfg := config.TelemetryConfig{
			InsecureVal:       false,
			TracesInsecureVal: &insecure,
		}
		assert.True(t, cfg.Insecure())
	})

	t.Run("专属非安全传输标志显式设置为false时优先使用", func(t *testing.T) {
		insecure := false
		cfg := config.TelemetryConfig{
			InsecureVal:       true,
			TracesInsecureVal: &insecure,
		}
		assert.False(t, cfg.Insecure())
	})

	t.Run("专属非安全传输标志为nil时回退至基础非安全传输标志", func(t *testing.T) {
		cfg := config.TelemetryConfig{
			InsecureVal:       true,
			TracesInsecureVal: nil,
		}
		assert.True(t, cfg.Insecure())

		cfg.InsecureVal = false
		assert.False(t, cfg.Insecure())
	})
}

func TestTelemetryConfig_AccessorsAndParsing(t *testing.T) {
	t.Run("访问器方法返回值", func(t *testing.T) {
		cfg := config.TelemetryConfig{
			EnabledVal:         true,
			ServiceNameVal:     "test-storage",
			SampleRatioVal:     0.5,
			ShutdownTimeoutVal: 3 * time.Second,
			TrustRemoteVal:     true,
		}

		assert.True(t, cfg.Enabled())
		assert.Equal(t, "test-storage", cfg.ServiceName())
		assert.InDelta(t, 0.5, cfg.SampleRatio(), 0.0001)
		assert.Equal(t, 3*time.Second, cfg.ShutdownTimeout())
		assert.True(t, cfg.TrustRemote())
	})

	t.Run("解析默认环境变量值", func(t *testing.T) {
		cleanConfigEnv(t)
		var cfg config.TelemetryConfig
		err := env.Parse(&cfg)
		require.NoError(t, err)

		assert.False(t, cfg.Enabled())
		assert.Equal(t, "bowl-storage", cfg.ServiceName())
		assert.Equal(t, "localhost:4317", cfg.Endpoint())
		assert.True(t, cfg.Insecure())
		assert.InDelta(t, 1.0, cfg.SampleRatio(), 0.0001)
		assert.Equal(t, 5*time.Second, cfg.ShutdownTimeout())
		assert.False(t, cfg.TrustRemote())
	})

	t.Run("解析自定义环境变量值", func(t *testing.T) {
		cleanConfigEnv(t)
		t.Setenv("OTEL_ENABLED", "true")
		t.Setenv("OTEL_SERVICE_NAME", "custom-bowl-storage")
		t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "otel-collector:4317")
		t.Setenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", "traces-collector:4317")
		t.Setenv("OTEL_EXPORTER_OTLP_INSECURE", "false")
		t.Setenv("OTEL_EXPORTER_OTLP_TRACES_INSECURE", "true")
		t.Setenv("OTEL_SAMPLE_RATIO", "0.25")
		t.Setenv("OTEL_SHUTDOWN_TIMEOUT", "8s")
		t.Setenv("OTEL_TRUST_REMOTE", "true")

		var cfg config.TelemetryConfig
		err := env.Parse(&cfg)
		require.NoError(t, err)

		assert.True(t, cfg.Enabled())
		assert.Equal(t, "custom-bowl-storage", cfg.ServiceName())
		assert.Equal(t, "traces-collector:4317", cfg.Endpoint())
		assert.True(t, cfg.Insecure())
		assert.InDelta(t, 0.25, cfg.SampleRatio(), 0.0001)
		assert.Equal(t, 8*time.Second, cfg.ShutdownTimeout())
		assert.True(t, cfg.TrustRemote())
	})
}

func TestLogConfig_SlogLevel_Conversion(t *testing.T) {
	cases := []struct {
		name     string
		levelVal string
		expected slog.Level
	}{
		{name: "全小写debug", levelVal: "debug", expected: slog.LevelDebug},
		{name: "全大写DEBUG", levelVal: "DEBUG", expected: slog.LevelDebug},
		{name: "混合大小写Debug", levelVal: "Debug", expected: slog.LevelDebug},
		{name: "全小写info", levelVal: "info", expected: slog.LevelInfo},
		{name: "全大写INFO", levelVal: "INFO", expected: slog.LevelInfo},
		{name: "全小写warn", levelVal: "warn", expected: slog.LevelWarn},
		{name: "全大写WARN", levelVal: "WARN", expected: slog.LevelWarn},
		{name: "全小写warning", levelVal: "warning", expected: slog.LevelWarn},
		{name: "全大写WARNING", levelVal: "WARNING", expected: slog.LevelWarn},
		{name: "混合大小写Warning", levelVal: "Warning", expected: slog.LevelWarn},
		{name: "全小写error", levelVal: "error", expected: slog.LevelError},
		{name: "全大写ERROR", levelVal: "ERROR", expected: slog.LevelError},
		{name: "混合大小写Error", levelVal: "Error", expected: slog.LevelError},
		{name: "前后空格清洗debug", levelVal: "  debug  ", expected: slog.LevelDebug},
		{name: "未知字符串默认info", levelVal: "unknown_level", expected: slog.LevelInfo},
		{name: "空字符串默认info", levelVal: "", expected: slog.LevelInfo},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := config.LogConfig{LevelVal: tc.levelVal}
			assert.Equal(t, tc.expected, cfg.SlogLevel())
		})
	}
}

func TestLogConfig_AccessorsAndParsing(t *testing.T) {
	t.Run("访问器方法返回值", func(t *testing.T) {
		cfg := config.LogConfig{
			LevelVal:  "debug",
			FormatVal: "text",
			LogOtelConfig: config.LogOtelConfig{
				RecordTraceFlagsVal: true,
				SampledOnlyVal:      true,
			},
			LogConnectConfig: config.LogConnectConfig{
				MessageVal: "custom rpc msg",
			},
		}

		assert.Equal(t, "debug", cfg.Level())
		assert.Equal(t, "text", cfg.Format())
		assert.True(t, cfg.RecordTraceFlags())
		assert.True(t, cfg.SampledOnly())
		assert.Equal(t, "custom rpc msg", cfg.Message())
	})

	t.Run("解析默认环境变量值", func(t *testing.T) {
		cleanConfigEnv(t)
		var cfg config.LogConfig
		err := env.Parse(&cfg)
		require.NoError(t, err)

		assert.Equal(t, "info", cfg.Level())
		assert.Equal(t, "json", cfg.Format())
		assert.Equal(t, slog.LevelInfo, cfg.SlogLevel())
		assert.Equal(t, "handled rpc", cfg.Message())
		assert.False(t, cfg.RecordTraceFlags())
		assert.False(t, cfg.SampledOnly())
	})

	t.Run("解析自定义环境变量值", func(t *testing.T) {
		cleanConfigEnv(t)
		t.Setenv("LOG_LEVEL", "warn")
		t.Setenv("LOG_FORMAT", "text")
		t.Setenv("LOG_CONNECT_MESSAGE", "bowl rpc finished")
		t.Setenv("LOG_OTEL_RECORD_TRACE_FLAGS", "true")
		t.Setenv("LOG_OTEL_SAMPLED_ONLY", "true")

		var cfg config.LogConfig
		err := env.Parse(&cfg)
		require.NoError(t, err)

		assert.Equal(t, "warn", cfg.Level())
		assert.Equal(t, "text", cfg.Format())
		assert.Equal(t, slog.LevelWarn, cfg.SlogLevel())
		assert.Equal(t, "bowl rpc finished", cfg.Message())
		assert.True(t, cfg.RecordTraceFlags())
		assert.True(t, cfg.SampledOnly())
	})
}

func TestConfig_Load(t *testing.T) {
	t.Run("默认加载无文件且环境合法", func(t *testing.T) {
		cleanConfigEnv(t)
		tempDir := t.TempDir()
		t.Chdir(tempDir)

		t.Setenv("DB_DSN", "postgres://user:pass@localhost:5432/bowl")

		cfg, err := config.Load()
		require.NoError(t, err)
		require.NotNil(t, cfg)

		assert.Equal(t, "postgres://user:pass@localhost:5432/bowl", cfg.DB.DSN())
		assert.Equal(t, "0.0.0.0:9000", cfg.Server.Addr())
		assert.Equal(t, "bowl-storage", cfg.Telemetry.ServiceName())
		assert.Equal(t, slog.LevelInfo, cfg.Log.SlogLevel())
	})

	t.Run("默认加载当前工作目录下存在的.env文件", func(t *testing.T) {
		cleanConfigEnv(t)
		tempDir := t.TempDir()
		t.Chdir(tempDir)

		envContent := "DB_DSN=postgres://dotenv:pass@localhost:5432/dotenv_db\n" +
			"SERVER_PORT=9005\n"
		require.NoError(t, os.WriteFile(filepath.Join(tempDir, ".env"), []byte(envContent), 0o600))

		cfg, err := config.Load()
		require.NoError(t, err)
		require.NotNil(t, cfg)

		assert.Equal(t, "postgres://dotenv:pass@localhost:5432/dotenv_db", cfg.DB.DSN())
		assert.Equal(t, "0.0.0.0:9005", cfg.Server.Addr())
	})

	t.Run("指定自定义环境文件路径加载成功", func(t *testing.T) {
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

		assert.Equal(t, "postgres://custom:pass@localhost:5432/custom_db", cfg.DB.DSN())
		assert.Equal(t, "0.0.0.0:9010", cfg.Server.Addr())
	})

	t.Run("指定不存在的环境文件返回错误", func(t *testing.T) {
		cleanConfigEnv(t)
		_, err := config.Load("non_existent_path.env")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "load env file:")
	})

	t.Run("环境变量类型解析失败返回错误", func(t *testing.T) {
		cleanConfigEnv(t)
		tempDir := t.TempDir()
		t.Chdir(tempDir)

		t.Setenv("DB_DSN", "postgres://localhost/bowl")
		t.Setenv("SERVER_PORT", "not_an_int")

		_, err := config.Load()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "parse environment variables:")
	})

	t.Run("ServerConfig校验失败返回错误", func(t *testing.T) {
		cleanConfigEnv(t)
		tempDir := t.TempDir()
		t.Chdir(tempDir)

		t.Setenv("DB_DSN", "postgres://localhost/bowl")
		t.Setenv("SERVER_PORT", "70000")

		_, err := config.Load()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "validate server config:")
	})
}

func TestConfigModule_ProvidesComponents(t *testing.T) {
	rootCfg := &config.Config{
		Server: config.ServerConfig{
			Host: "127.0.0.1",
			Port: 9000,
		},
		DB: config.DBPoolConfig{
			DSNVal: "postgres://localhost/bowl",
		},
		Telemetry: config.TelemetryConfig{
			ServiceNameVal: "bowl-storage",
		},
		Log: config.LogConfig{
			LevelVal:  "debug",
			FormatVal: "json",
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
		extractedLog       slogx.Config
		extractedServer    config.ServerConfig
		extractedDB        config.DBPoolConfig
		extractedTelemetry telemetry.Config
		extractedOtelPtr   *config.TelemetryConfig
		extractedMeta      config.MetaConfig
		extractedMetaPtr   *config.MetaConfig
	)

	app := fxtest.New(
		t,
		fx.Supply(rootCfg),
		config.Module,
		fx.Populate(
			&extractedLog,
			&extractedServer,
			&extractedDB,
			&extractedTelemetry,
			&extractedOtelPtr,
			&extractedMeta,
			&extractedMetaPtr,
		),
	)
	require.NoError(t, app.Err())

	assert.Equal(t, slog.LevelDebug, extractedLog.SlogLevel())
	assert.Equal(t, "127.0.0.1:9000", extractedServer.Addr())
	assert.Equal(t, "postgres://localhost/bowl", extractedDB.DSN())
	assert.Equal(t, "bowl-storage", extractedTelemetry.ServiceName())
	assert.Equal(t, "bowl-storage", extractedOtelPtr.ServiceName())
	assert.Equal(t, 45*time.Second, extractedMeta.SessionLeaseTTL())
	assert.Equal(t, "strictatime", extractedMeta.AtimeMode())
	assert.Equal(t, 2.5, extractedMeta.EntryTimeoutSeconds())
	assert.Equal(t, 3.5, extractedMeta.AttrTimeoutSeconds())
	assert.Equal(t, 4.5, extractedMeta.DirEntryTimeoutSeconds())
	assert.True(t, extractedMeta.OpenKeepCache())
	assert.True(t, extractedMeta.OpenDirCache())
	require.NotNil(t, extractedMetaPtr)
	assert.Equal(t, 45*time.Second, extractedMetaPtr.SessionLeaseTTL())
}

func TestMetaConfig_Defaults(t *testing.T) {
	cleanConfigEnv(t)

	var cfg config.MetaConfig
	err := env.Parse(&cfg)
	require.NoError(t, err)

	assert.Equal(t, 60*time.Second, cfg.SessionLeaseTTL())
	assert.Equal(t, "noatime", cfg.AtimeMode())
	assert.Equal(t, 1.0, cfg.EntryTimeoutSeconds())
	assert.Equal(t, 1.0, cfg.AttrTimeoutSeconds())
	assert.Equal(t, 1.0, cfg.DirEntryTimeoutSeconds())
	assert.False(t, cfg.OpenKeepCache())
	assert.False(t, cfg.OpenDirCache())
	assert.NoError(t, cfg.Validate())
}

func TestMetaConfig_EnvOverrides(t *testing.T) {
	cleanConfigEnv(t)

	t.Setenv("STORAGE_SESSION_LEASE_TTL", "120s")
	t.Setenv("STORAGE_ATIME_MODE", "RelAtime")
	t.Setenv("STORAGE_ENTRY_TIMEOUT_SECONDS", "2.5")
	t.Setenv("STORAGE_ATTR_TIMEOUT_SECONDS", "3.5")
	t.Setenv("STORAGE_DIR_ENTRY_TIMEOUT_SECONDS", "4.5")
	t.Setenv("STORAGE_OPEN_KEEP_CACHE", "true")
	t.Setenv("STORAGE_OPEN_DIR_CACHE", "true")

	var cfg config.MetaConfig
	err := env.Parse(&cfg)
	require.NoError(t, err)

	assert.Equal(t, 120*time.Second, cfg.SessionLeaseTTL())
	assert.Equal(t, "relatime", cfg.AtimeMode())
	assert.Equal(t, 2.5, cfg.EntryTimeoutSeconds())
	assert.Equal(t, 3.5, cfg.AttrTimeoutSeconds())
	assert.Equal(t, 4.5, cfg.DirEntryTimeoutSeconds())
	assert.True(t, cfg.OpenKeepCache())
	assert.True(t, cfg.OpenDirCache())
	assert.NoError(t, cfg.Validate())
}

func TestMetaConfig_CaseInsensitiveAtimeMode(t *testing.T) {
	modes := []struct {
		input    string
		expected string
	}{
		{input: "noatime", expected: "noatime"},
		{input: "NOATIME", expected: "noatime"},
		{input: "NoAtime", expected: "noatime"},
		{input: "relatime", expected: "relatime"},
		{input: "RELATIME", expected: "relatime"},
		{input: "RelAtime", expected: "relatime"},
		{input: "strictatime", expected: "strictatime"},
		{input: "STRICTATIME", expected: "strictatime"},
		{input: "StrictAtime", expected: "strictatime"},
	}

	for _, tc := range modes {
		t.Run(tc.input, func(t *testing.T) {
			cfg := config.MetaConfig{
				SessionLeaseTTLVal:        time.Minute,
				AtimeModeVal:              tc.input,
				EntryTimeoutSecondsVal:    1.0,
				AttrTimeoutSecondsVal:     1.0,
				DirEntryTimeoutSecondsVal: 1.0,
			}
			assert.NoError(t, cfg.Validate())
			assert.Equal(t, tc.expected, cfg.AtimeMode())
		})
	}
}

func TestMetaConfig_ValidateErrors(t *testing.T) {
	validCfg := func() config.MetaConfig {
		return config.MetaConfig{
			SessionLeaseTTLVal:        60 * time.Second,
			AtimeModeVal:              "noatime",
			EntryTimeoutSecondsVal:    1.0,
			AttrTimeoutSecondsVal:     1.0,
			DirEntryTimeoutSecondsVal: 1.0,
		}
	}

	t.Run("非法atime策略报错", func(t *testing.T) {
		cfg := validCfg()
		cfg.AtimeModeVal = "fastatime"
		err := cfg.Validate()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "invalid atime mode")
	})

	t.Run("负数entry超时报错", func(t *testing.T) {
		cfg := validCfg()
		cfg.EntryTimeoutSecondsVal = -0.1
		err := cfg.Validate()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "invalid entry timeout")
	})

	t.Run("负数attr超时报错", func(t *testing.T) {
		cfg := validCfg()
		cfg.AttrTimeoutSecondsVal = -1.0
		err := cfg.Validate()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "invalid attr timeout")
	})

	t.Run("负数dir entry超时报错", func(t *testing.T) {
		cfg := validCfg()
		cfg.DirEntryTimeoutSecondsVal = -0.5
		err := cfg.Validate()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "invalid dir entry timeout")
	})

	t.Run("零或负数session租约TTL报错", func(t *testing.T) {
		cfgZero := validCfg()
		cfgZero.SessionLeaseTTLVal = 0
		err := cfgZero.Validate()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "invalid session lease TTL")

		cfgNegative := validCfg()
		cfgNegative.SessionLeaseTTLVal = -5 * time.Second
		err = cfgNegative.Validate()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "invalid session lease TTL")
	})
}

func TestLoad_MetaConfigValidation(t *testing.T) {
	cleanConfigEnv(t)
	tempDir := t.TempDir()
	t.Chdir(tempDir)

	t.Setenv("DB_DSN", "postgres://localhost/bowl")
	t.Setenv("STORAGE_ATIME_MODE", "invalid_mode")

	_, err := config.Load()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "validate meta config:")
}
