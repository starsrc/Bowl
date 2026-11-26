package config_test

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"

	"github.com/starsrc/pkg/observability/telemetry"

	"github.com/starsrc/bowl/services/storage/internal/config"
)

const (
	validTestDSN       = "postgres://bowl:bowl@127.0.0.1:5432/bowl?sslmode=disable"
	configLoadDeadline = 2 * time.Second
)

var isolatedConfigEnvKeys = []string{
	"SERVER_HOST",
	"SERVER_PORT",
	"INTERNAL_SERVER_HOST",
	"INTERNAL_SERVER_PORT",
	"SERVER_READ_HEADER_TIMEOUT",
	"SERVER_STOP_TIMEOUT",
	"SERVER_READ_MAX_BYTES",
	"DB_DSN",
	"DB_MAX_CONNS",
	"DB_MIN_CONNS",
	"DB_MAX_CONN_LIFETIME",
	"DB_MAX_CONN_IDLE_TIME",
	"DB_HEALTH_CHECK_PERIOD",
	"DB_TRACING_ENABLED",
	"DB_METRICS_ENABLED",
	"LOG_LEVEL",
	"LOG_FORMAT",
	"OTEL_ENABLED",
	"OTEL_SERVICE_NAME",
	"OTEL_SERVICE_VERSION",
	"DEPLOY_ENV",
	"OTEL_TRACES_EXPORTER",
	"OTEL_EXPORTER_OTLP_ENDPOINT",
	"OTEL_EXPORTER_OTLP_TRACES_ENDPOINT",
	"OTEL_EXPORTER_OTLP_METRICS_ENDPOINT",
	"OTEL_EXPORTER_OTLP_LOGS_ENDPOINT",
	"OTEL_EXPORTER_OTLP_INSECURE",
	"OTEL_EXPORTER_OTLP_TRACES_INSECURE",
	"OTEL_TRACES_SAMPLER_ARG",
	"OTEL_TRUST_REMOTE",
	"METRICS_ENABLED",
	"PPROF_ENABLED",
	"OBS_ADMIN_ADDR",
	"STORAGE_ATIME_MODE",
	"STORAGE_ENTRY_TIMEOUT_SECONDS",
	"STORAGE_ATTR_TIMEOUT_SECONDS",
	"STORAGE_DIR_ENTRY_TIMEOUT_SECONDS",
	"STORAGE_OPEN_KEEP_CACHE",
	"STORAGE_OPEN_DIR_CACHE",
	"STORAGE_ACTOR_IDLE_TTL",
	"STORAGE_GC_INTERVAL",
	"DATA_DIR",
	"STORAGE_DATA_DIR",
}

func runSubtests[T any](t *testing.T, cases []T, nameFn func(T) string, runFn func(*testing.T, T)) {
	t.Helper()
	for _, tc := range cases {
		t.Run(nameFn(tc), func(t *testing.T) {
			runFn(t, tc)
		})
	}
}

func isolateConfigEnv(t *testing.T) {
	t.Helper()
	t.Chdir(t.TempDir())
	for _, key := range isolatedConfigEnvKeys {
		t.Setenv(key, "")
		require.NoError(t, os.Unsetenv(key), "清理测试环境变量 %s 失败", key)
	}
}

func applyEnvOverrides(t *testing.T, overrides map[string]string) {
	t.Helper()
	for k, v := range overrides {
		t.Setenv(k, v)
	}
}

func writeEnvFile(t *testing.T, name, body string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), name)
	require.NoError(t, os.WriteFile(path, []byte(body), 0o600), "写入测试环境文件 %s 失败", path)
	return path
}

func loadConfigWithinDeadline(t *testing.T, filenames ...string) (*config.Config, error) {
	t.Helper()
	type loadResult struct {
		cfg *config.Config
		err error
	}
	ctx, cancel := context.WithTimeout(context.Background(), configLoadDeadline)
	defer cancel()

	ch := make(chan loadResult, 1)
	go func() {
		cfg, err := config.Load(filenames...)
		ch <- loadResult{cfg: cfg, err: err}
	}()

	select {
	case res := <-ch:
		return res.cfg, res.err
	case <-ctx.Done():
		t.Fatalf("config.Load 超出截止时间 %s 未返回: %v", configLoadDeadline, ctx.Err())
		return nil, ctx.Err()
	}
}

func TestConfigLoad_GivenMinimalRequiredEnv_WhenLoaded_ThenAppliesDefaults(t *testing.T) {
	isolateConfigEnv(t)
	t.Setenv("DB_DSN", validTestDSN)

	cfg, err := loadConfigWithinDeadline(t)
	require.NoError(t, err, "仅设置 DB_DSN 加载默认配置不应失败")
	require.Equal(t, 9000, cfg.ServerPort, "未指定 SERVER_PORT 时 want=9000, got=%d", cfg.ServerPort)
	require.Equal(t, 9090, cfg.InternalPort(), "未指定 INTERNAL_SERVER_PORT 时 want=9090, got=%d", cfg.InternalPort())
	require.Equal(t, int64(config.DefaultServerReadMaxBytes), cfg.ServerReadMaxBytes, "默认最大读取字节 want=%d, got=%d", config.DefaultServerReadMaxBytes, cfg.ServerReadMaxBytes)
	require.Equal(t, "bowl-storage", cfg.OtelServiceName, "默认遥测名称 want=bowl-storage, got=%s", cfg.OtelServiceName)
	require.True(t, cfg.OtelTrustRemote, "默认应信任远端追踪上下文")
	require.Equal(t, config.DefaultAtimeMode, cfg.Meta.AtimeMode(), "默认访问时间策略 want=%s, got=%s", config.DefaultAtimeMode, cfg.Meta.AtimeMode())
	require.InDelta(t, config.DefaultEntryTimeoutSeconds, cfg.Meta.EntryTimeoutSeconds(), 0, "默认条目缓存时长 mismatch")
	require.InDelta(t, config.DefaultAttrTimeoutSeconds, cfg.Meta.AttrTimeoutSeconds(), 0, "默认属性缓存时长 mismatch")
	require.InDelta(t, config.DefaultDirEntryTimeoutSeconds, cfg.Meta.DirEntryTimeoutSeconds(), 0, "默认目录缓存时长 mismatch")
	require.False(t, cfg.Meta.OpenKeepCache(), "默认应关闭文件页缓存保留")
	require.False(t, cfg.Meta.OpenDirCache(), "默认应关闭目录缓存保留")
	require.Equal(t, config.DefaultActorIdleTTL, cfg.Meta.ActorIdleTTL(), "默认空闲淘汰时长 want=%s, got=%s", config.DefaultActorIdleTTL, cfg.Meta.ActorIdleTTL())
	require.Equal(t, config.DefaultGCInterval, cfg.Meta.GCInterval(), "默认 GC 间隔 want=%s, got=%s", config.DefaultGCInterval, cfg.Meta.GCInterval())
	require.Equal(t, config.DefaultDataDir, cfg.Meta.DataDir(), "默认数据目录 want=%s, got=%s", config.DefaultDataDir, cfg.Meta.DataDir())
}

func TestConfigLoad_GivenExtremeValidEnv_WhenLoaded_ThenPreservesExplicitValues(t *testing.T) {
	type extremeCase struct {
		name               string
		env                map[string]string
		wantPort           int
		wantInternalPort   int
		wantReadMaxBytes   int64
		wantAtimeMode      string
		wantEntryTimeout   float64
		wantAttrTimeout    float64
		wantDirTimeout     float64
		wantOpenKeepCache  bool
		wantOpenDirCache   bool
		wantActorIdleTTL   time.Duration
		wantGCInterval     time.Duration
		wantDataDir        string
		wantOtelSampleRate float64
	}
	cases := []extremeCase{
		{
			name: "min_ports_zero_timeouts_and_storage_data_dir_fallback",
			env: map[string]string{
				"SERVER_HOST":                       "127.0.0.1",
				"SERVER_PORT":                       "1",
				"INTERNAL_SERVER_HOST":              "127.0.0.2",
				"INTERNAL_SERVER_PORT":              "1",
				"SERVER_READ_MAX_BYTES":             "4096",
				"STORAGE_ATIME_MODE":                "  ReLaTiMe ",
				"STORAGE_ENTRY_TIMEOUT_SECONDS":     "0",
				"STORAGE_ATTR_TIMEOUT_SECONDS":      "0",
				"STORAGE_DIR_ENTRY_TIMEOUT_SECONDS": "0",
				"STORAGE_OPEN_KEEP_CACHE":           "true",
				"STORAGE_OPEN_DIR_CACHE":            "true",
				"STORAGE_ACTOR_IDLE_TTL":            "0",
				"STORAGE_GC_INTERVAL":               "0",
				"STORAGE_DATA_DIR":                  "/var/lib/bowl-fallback",
				"OTEL_TRACES_SAMPLER_ARG":           "0",
				"OTEL_TRACES_EXPORTER":              "none",
				"OBS_ADMIN_ADDR":                    "off",
			},
			wantPort:           1,
			wantInternalPort:   1,
			wantReadMaxBytes:   4096,
			wantAtimeMode:      "relatime",
			wantEntryTimeout:   0,
			wantAttrTimeout:    0,
			wantDirTimeout:     0,
			wantOpenKeepCache:  true,
			wantOpenDirCache:   true,
			wantActorIdleTTL:   0,
			wantGCInterval:     0,
			wantDataDir:        "/var/lib/bowl-fallback",
			wantOtelSampleRate: 0,
		},
		{
			name: "max_ports_explicit_8080_8090_and_data_dir_precedence",
			env: map[string]string{
				"SERVER_PORT":                       "8080",
				"INTERNAL_SERVER_PORT":              "8090",
				"STORAGE_ATIME_MODE":                "strictatime",
				"STORAGE_ENTRY_TIMEOUT_SECONDS":     "86400",
				"STORAGE_ATTR_TIMEOUT_SECONDS":      "3600",
				"STORAGE_DIR_ENTRY_TIMEOUT_SECONDS": "60",
				"STORAGE_ACTOR_IDLE_TTL":            "15m",
				"STORAGE_GC_INTERVAL":               "2m",
				"DATA_DIR":                          "/var/lib/bowl-primary",
				"STORAGE_DATA_DIR":                  "/var/lib/bowl-ignored",
				"OTEL_TRACES_SAMPLER_ARG":           "1.0",
				"OBS_ADMIN_ADDR":                    "127.0.0.1:0",
			},
			wantPort:           8080,
			wantInternalPort:   8090,
			wantReadMaxBytes:   config.DefaultServerReadMaxBytes,
			wantAtimeMode:      "strictatime",
			wantEntryTimeout:   86400,
			wantAttrTimeout:    3600,
			wantDirTimeout:     60,
			wantOpenKeepCache:  false,
			wantOpenDirCache:   false,
			wantActorIdleTTL:   15 * time.Minute,
			wantGCInterval:     2 * time.Minute,
			wantDataDir:        "/var/lib/bowl-primary",
			wantOtelSampleRate: 1.0,
		},
		{
			name: "port_upper_limit_65535",
			env: map[string]string{
				"SERVER_PORT":          "65535",
				"INTERNAL_SERVER_PORT": "65534",
				"DB_MAX_CONNS":         "0",
				"DB_MIN_CONNS":         "0",
			},
			wantPort:           65535,
			wantInternalPort:   65534,
			wantReadMaxBytes:   config.DefaultServerReadMaxBytes,
			wantAtimeMode:      "noatime",
			wantEntryTimeout:   1.0,
			wantAttrTimeout:    1.0,
			wantDirTimeout:     1.0,
			wantOpenKeepCache:  false,
			wantOpenDirCache:   false,
			wantActorIdleTTL:   5 * time.Minute,
			wantGCInterval:     config.DefaultGCInterval,
			wantDataDir:        config.DefaultDataDir,
			wantOtelSampleRate: 1.0,
		},
	}

	runSubtests(t, cases, func(tc extremeCase) string { return tc.name }, func(t *testing.T, tc extremeCase) {
		isolateConfigEnv(t)
		t.Setenv("DB_DSN", validTestDSN)
		applyEnvOverrides(t, tc.env)

		cfg, err := loadConfigWithinDeadline(t)
		require.NoError(t, err, "合法极值配置加载失败: %v", err)
		require.Equal(t, tc.wantPort, cfg.ServerPort, "ServerPort want=%d, got=%d", tc.wantPort, cfg.ServerPort)
		require.Equal(t, tc.wantInternalPort, cfg.InternalPort(), "InternalPort want=%d, got=%d", tc.wantInternalPort, cfg.InternalPort())
		require.Equal(t, tc.wantReadMaxBytes, cfg.ServerReadMaxBytes, "ServerReadMaxBytes want=%d, got=%d", tc.wantReadMaxBytes, cfg.ServerReadMaxBytes)
		require.Equal(t, tc.wantAtimeMode, cfg.Meta.AtimeMode(), "AtimeMode want=%s, got=%s", tc.wantAtimeMode, cfg.Meta.AtimeMode())
		require.InDelta(t, tc.wantEntryTimeout, cfg.Meta.EntryTimeoutSeconds(), 0, "EntryTimeoutSeconds want=%v, got=%v", tc.wantEntryTimeout, cfg.Meta.EntryTimeoutSeconds())
		require.InDelta(t, tc.wantAttrTimeout, cfg.Meta.AttrTimeoutSeconds(), 0, "AttrTimeoutSeconds want=%v, got=%v", tc.wantAttrTimeout, cfg.Meta.AttrTimeoutSeconds())
		require.InDelta(t, tc.wantDirTimeout, cfg.Meta.DirEntryTimeoutSeconds(), 0, "DirEntryTimeoutSeconds want=%v, got=%v", tc.wantDirTimeout, cfg.Meta.DirEntryTimeoutSeconds())
		require.Equal(t, tc.wantOpenKeepCache, cfg.Meta.OpenKeepCache(), "OpenKeepCache want=%v, got=%v", tc.wantOpenKeepCache, cfg.Meta.OpenKeepCache())
		require.Equal(t, tc.wantOpenDirCache, cfg.Meta.OpenDirCache(), "OpenDirCache want=%v, got=%v", tc.wantOpenDirCache, cfg.Meta.OpenDirCache())
		require.Equal(t, tc.wantActorIdleTTL, cfg.Meta.ActorIdleTTL(), "ActorIdleTTL want=%s, got=%s", tc.wantActorIdleTTL, cfg.Meta.ActorIdleTTL())
		require.Equal(t, tc.wantGCInterval, cfg.Meta.GCInterval(), "GCInterval want=%s, got=%s", tc.wantGCInterval, cfg.Meta.GCInterval())
		require.Equal(t, tc.wantDataDir, cfg.Meta.DataDir(), "DataDir want=%s, got=%s", tc.wantDataDir, cfg.Meta.DataDir())
		require.InDelta(t, tc.wantOtelSampleRate, cfg.SampleRatio(), 0, "SampleRatio want=%v, got=%v", tc.wantOtelSampleRate, cfg.SampleRatio())
	})
}

func TestConfigLoad_GivenEmptyOrMalformedEnvFiles_WhenLoaded_ThenFailsWithinDeadline(t *testing.T) {
	t.Run("empty_env_file_missing_required_dsn", func(t *testing.T) {
		isolateConfigEnv(t)
		emptyFile := writeEnvFile(t, "empty.env", "")

		cfg, err := loadConfigWithinDeadline(t, emptyFile)
		require.Error(t, err, "空配置且未提供 DB_DSN 必须返回错误: want error, got cfg=%+v", cfg)
	})

	t.Run("missing_explicit_env_file", func(t *testing.T) {
		isolateConfigEnv(t)
		t.Setenv("DB_DSN", validTestDSN)
		missingPath := filepath.Join(t.TempDir(), "does-not-exist.env")

		cfg, err := loadConfigWithinDeadline(t, missingPath)
		require.Error(t, err, "显式指定的环境文件不存在时必须返回错误: want error, got cfg=%+v", cfg)
	})

	t.Run("malformed_unterminated_quote_env_file", func(t *testing.T) {
		isolateConfigEnv(t)
		t.Setenv("DB_DSN", validTestDSN)
		badFile := writeEnvFile(t, "malformed.env", "DATA_DIR=\"unterminated\n")

		cfg, err := loadConfigWithinDeadline(t, badFile)
		require.Error(t, err, "语法畸形的环境文件必须返回错误: want error, got cfg=%+v", cfg)
	})

	t.Run("oversized_env_value_completes_within_deadline", func(t *testing.T) {
		isolateConfigEnv(t)
		hugeVal := strings.Repeat("x", 64*1024)
		hugeFile := writeEnvFile(t, "huge.env", "DB_DSN="+validTestDSN+"\nDATA_DIR=/tmp/"+hugeVal+"\n")

		cfg, err := loadConfigWithinDeadline(t, hugeFile)
		require.NoError(t, err, "超长合法环境值应在截止时间内完成解析: %v", err)
		require.Equal(t, "/tmp/"+hugeVal, cfg.Meta.DataDir())
	})
}

func TestConfigLoad_GivenInvalidEnvValues_WhenLoaded_ThenReturnsError(t *testing.T) {
	type invalidCase struct {
		name string
		env  map[string]string
	}
	cases := []invalidCase{
		{
			name: "missing_dsn",
			env:  map[string]string{},
		},
		{
			name: "empty_dsn",
			env:  map[string]string{"DB_DSN": ""},
		},
		{
			name: "server_port_zero",
			env:  map[string]string{"DB_DSN": validTestDSN, "SERVER_PORT": "0"},
		},
		{
			name: "server_port_over_65535",
			env:  map[string]string{"DB_DSN": validTestDSN, "SERVER_PORT": "65536"},
		},
		{
			name: "server_port_non_numeric",
			env:  map[string]string{"DB_DSN": validTestDSN, "SERVER_PORT": "not-a-port"},
		},
		{
			name: "internal_port_zero",
			env:  map[string]string{"DB_DSN": validTestDSN, "INTERNAL_SERVER_PORT": "0"},
		},
		{
			name: "internal_port_negative",
			env:  map[string]string{"DB_DSN": validTestDSN, "INTERNAL_SERVER_PORT": "-1"},
		},
		{
			name: "internal_port_over_65535",
			env:  map[string]string{"DB_DSN": validTestDSN, "INTERNAL_SERVER_PORT": "65536"},
		},
		{
			name: "dual_ports_conflict_on_unspecified_host",
			env:  map[string]string{"DB_DSN": validTestDSN, "SERVER_PORT": "9000", "INTERNAL_SERVER_PORT": "9000"},
		},
		{
			name: "dual_ports_conflict_unspecified_vs_loopback",
			env:  map[string]string{"DB_DSN": validTestDSN, "SERVER_HOST": "0.0.0.0", "SERVER_PORT": "9000", "INTERNAL_SERVER_HOST": "127.0.0.1", "INTERNAL_SERVER_PORT": "9000"},
		},
		{
			name: "db_max_conns_negative",
			env:  map[string]string{"DB_DSN": validTestDSN, "DB_MAX_CONNS": "-1"},
		},
		{
			name: "db_min_conns_negative",
			env:  map[string]string{"DB_DSN": validTestDSN, "DB_MIN_CONNS": "-1"},
		},
		{
			name: "data_dir_whitespace_only",
			env:  map[string]string{"DB_DSN": validTestDSN, "DATA_DIR": "   "},
		},
		{
			name: "storage_data_dir_whitespace_only",
			env:  map[string]string{"DB_DSN": validTestDSN, "STORAGE_DATA_DIR": "\t\n"},
		},
		{
			name: "atime_mode_invalid",
			env:  map[string]string{"DB_DSN": validTestDSN, "STORAGE_ATIME_MODE": "invalid-mode"},
		},
		{
			name: "atime_mode_whitespace_only",
			env:  map[string]string{"DB_DSN": validTestDSN, "STORAGE_ATIME_MODE": "   "},
		},
		{
			name: "entry_timeout_negative",
			env:  map[string]string{"DB_DSN": validTestDSN, "STORAGE_ENTRY_TIMEOUT_SECONDS": "-0.01"},
		},
		{
			name: "entry_timeout_nan",
			env:  map[string]string{"DB_DSN": validTestDSN, "STORAGE_ENTRY_TIMEOUT_SECONDS": "NaN"},
		},
		{
			name: "entry_timeout_pos_inf",
			env:  map[string]string{"DB_DSN": validTestDSN, "STORAGE_ENTRY_TIMEOUT_SECONDS": "+Inf"},
		},
		{
			name: "entry_timeout_neg_inf",
			env:  map[string]string{"DB_DSN": validTestDSN, "STORAGE_ENTRY_TIMEOUT_SECONDS": "-Inf"},
		},
		{
			name: "attr_timeout_negative",
			env:  map[string]string{"DB_DSN": validTestDSN, "STORAGE_ATTR_TIMEOUT_SECONDS": "-1"},
		},
		{
			name: "attr_timeout_nan",
			env:  map[string]string{"DB_DSN": validTestDSN, "STORAGE_ATTR_TIMEOUT_SECONDS": "NaN"},
		},
		{
			name: "attr_timeout_pos_inf",
			env:  map[string]string{"DB_DSN": validTestDSN, "STORAGE_ATTR_TIMEOUT_SECONDS": "+Inf"},
		},
		{
			name: "dir_entry_timeout_negative",
			env:  map[string]string{"DB_DSN": validTestDSN, "STORAGE_DIR_ENTRY_TIMEOUT_SECONDS": "-1"},
		},
		{
			name: "dir_entry_timeout_nan",
			env:  map[string]string{"DB_DSN": validTestDSN, "STORAGE_DIR_ENTRY_TIMEOUT_SECONDS": "NaN"},
		},
		{
			name: "dir_entry_timeout_pos_inf",
			env:  map[string]string{"DB_DSN": validTestDSN, "STORAGE_DIR_ENTRY_TIMEOUT_SECONDS": "+Inf"},
		},
		{
			name: "actor_idle_ttl_negative",
			env:  map[string]string{"DB_DSN": validTestDSN, "STORAGE_ACTOR_IDLE_TTL": "-1s"},
		},
		{
			name: "actor_idle_ttl_malformed",
			env:  map[string]string{"DB_DSN": validTestDSN, "STORAGE_ACTOR_IDLE_TTL": "not-a-duration"},
		},
		{
			name: "gc_interval_negative",
			env:  map[string]string{"DB_DSN": validTestDSN, "STORAGE_GC_INTERVAL": "-1s"},
		},
		{
			name: "gc_interval_malformed",
			env:  map[string]string{"DB_DSN": validTestDSN, "STORAGE_GC_INTERVAL": "not-a-duration"},
		},
		{
			name: "otel_sample_ratio_negative",
			env:  map[string]string{"DB_DSN": validTestDSN, "OTEL_TRACES_SAMPLER_ARG": "-0.1"},
		},
		{
			name: "otel_sample_ratio_above_one",
			env:  map[string]string{"DB_DSN": validTestDSN, "OTEL_TRACES_SAMPLER_ARG": "1.1"},
		},
		{
			name: "otel_sample_ratio_nan",
			env:  map[string]string{"DB_DSN": validTestDSN, "OTEL_TRACES_SAMPLER_ARG": "NaN"},
		},
		{
			name: "otel_traces_exporter_invalid",
			env:  map[string]string{"DB_DSN": validTestDSN, "OTEL_TRACES_EXPORTER": "jaeger"},
		},
		{
			name: "obs_admin_addr_malformed",
			env:  map[string]string{"DB_DSN": validTestDSN, "OBS_ADMIN_ADDR": "not-host-port"},
		},
	}

	runSubtests(t, cases, func(tc invalidCase) string { return tc.name }, func(t *testing.T, tc invalidCase) {
		isolateConfigEnv(t)
		applyEnvOverrides(t, tc.env)

		cfg, err := loadConfigWithinDeadline(t)
		require.Error(t, err, "非法配置 %s 必须返回错误: want error, got cfg=%+v", tc.name, cfg)
	})
}

func TestConfigLoad_GivenBareOTLPEndpoints_WhenLoaded_ThenNormalizesSchemeForTracerProvider(t *testing.T) {
	isolateConfigEnv(t)
	require.Empty(t, config.NormalizeOTLPEndpoint("   "))
	require.Equal(t, "http://jaeger:4317", config.NormalizeOTLPEndpoint("jaeger:4317"))
	require.Equal(t, "http://172.16.1.205:4317", config.NormalizeOTLPEndpoint(" 172.16.1.205:4317 "))
	require.Equal(t, "http://alloy:4317", config.NormalizeOTLPEndpoint("http://alloy:4317"))
	require.Equal(t, "https://collector.example.com:4317", config.NormalizeOTLPEndpoint("https://collector.example.com:4317"))

	envFile := writeEnvFile(t, "storage.env", "DB_DSN="+validTestDSN+"\nOTEL_EXPORTER_OTLP_METRICS_ENDPOINT=alloy:4317\n")
	t.Setenv("OTEL_ENABLED", "true")
	t.Setenv("OTEL_TRACES_EXPORTER", "otlp")
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "jaeger:4317")
	t.Setenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", "127.0.0.1:4317")

	cfg, err := loadConfigWithinDeadline(t, envFile)
	require.NoError(t, err)
	require.Equal(t, "http://jaeger:4317", cfg.OtelEndpoint)
	require.Equal(t, "http://127.0.0.1:4317", cfg.OtelTracesEndpoint)
	require.Equal(t, "http://127.0.0.1:4317", cfg.Endpoint())
	require.Equal(t, "http://jaeger:4317", os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT"))
	require.Equal(t, "http://127.0.0.1:4317", os.Getenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT"))
	require.Equal(t, "http://alloy:4317", os.Getenv("OTEL_EXPORTER_OTLP_METRICS_ENDPOINT"))

	var (
		errMu    sync.Mutex
		otelErrs []error
	)
	prevHandler := otel.GetErrorHandler()
	otel.SetErrorHandler(otel.ErrorHandlerFunc(func(hErr error) {
		errMu.Lock()
		otelErrs = append(otelErrs, hErr)
		errMu.Unlock()
	}))
	defer otel.SetErrorHandler(prevHandler)

	tp, err := telemetry.NewTracerProvider(context.Background(), cfg)
	require.NoError(t, err)
	require.NoError(t, tp.Shutdown(context.Background()))

	errMu.Lock()
	defer errMu.Unlock()
	require.Empty(t, otelErrs, "OTel 初始化裸 host:port 环境变量时不得产生 URL 解析错误")
}

func TestConfig_GivenNilPointers_WhenAccessorsCalled_ThenHandlesSafely(t *testing.T) {
	var nilCfg *config.Config
	require.Equal(t, 0, nilCfg.InternalPort(), "nil Config 的 InternalPort want=0, got=%d", nilCfg.InternalPort())
	nilCfg.ApplyDefaultInternalPort(9090)
	require.Nil(t, config.NewObservabilityConfig(nil), "nil ObservabilityConfig 应返回 nil")

	obsCfg := config.NewObservabilityConfig(&config.ObservabilityConfig{MetricsEnabled: true})
	require.False(t, obsCfg.IsMetricsEnabled(), "NewObservabilityConfig 应抑制默认指标提供器")
	require.False(t, obsCfg.Enabled(), "NewObservabilityConfig 应抑制默认链路提供器")

	var zeroMeta config.MetaConfig
	require.Equal(t, config.DefaultAtimeMode, zeroMeta.AtimeMode(), "零值 MetaConfig.AtimeMode 应回退默认值")
	require.Error(t, zeroMeta.Validate(), "未经过默认填充的零值 MetaConfig.Validate 应拒绝空 AtimeMode")
}

func TestServerConfig_GivenDualPortsAndTimeouts_WhenValidated_ThenMatchesContract(t *testing.T) {
	zero := config.ServerConfig{}
	require.Equal(t, 10*time.Second, zero.ReadHeaderTimeout())
	require.Equal(t, 10*time.Second, zero.StopTimeout())
	require.Equal(t, 7*time.Second, zero.ShutdownTimeout())
	require.Equal(t, int64(128<<10), zero.ReadMaxBytes())

	custom := config.ServerConfig{
		ServerHost:              "127.0.0.1",
		ServerPort:              9000,
		InternalServerHost:      "127.0.0.1",
		InternalServerPort:      9090,
		ServerReadHeaderTimeout: 3 * time.Second,
		ServerStopTimeout:       20 * time.Second,
		ServerReadMaxBytes:      4096,
	}
	require.Equal(t, "127.0.0.1:9000", custom.Addr())
	require.Equal(t, "127.0.0.1:9090", custom.InternalAddr())
	require.Equal(t, 3*time.Second, custom.ReadHeaderTimeout())
	require.Equal(t, 20*time.Second, custom.StopTimeout())
	require.Equal(t, 14*time.Second, custom.ShutdownTimeout())
	require.Equal(t, int64(4096), custom.ReadMaxBytes())
	require.NoError(t, custom.ValidateDualPorts())

	conflictCases := []config.ServerConfig{
		{ServerHost: "::", ServerPort: 9000, InternalServerHost: "127.0.0.1", InternalServerPort: 9000},
		{ServerHost: "::ffff:127.0.0.1", ServerPort: 9000, InternalServerHost: "127.0.0.1", InternalServerPort: 9000},
		{ServerHost: "LOCALHOST", ServerPort: 9000, InternalServerHost: "localhost", InternalServerPort: 9000},
		{ServerHost: "", ServerPort: 9000, InternalServerHost: "127.0.0.1", InternalServerPort: 9000},
	}
	for _, tc := range conflictCases {
		require.Error(t, tc.ValidateDualPorts())
	}

	distinctHost := config.ServerConfig{
		ServerHost:         "127.0.0.1",
		ServerPort:         9000,
		InternalServerHost: "127.0.0.2",
		InternalServerPort: 9000,
	}
	require.NoError(t, distinctHost.ValidateDualPorts())
}
