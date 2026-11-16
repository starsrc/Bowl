package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"

	"github.com/starsrc/bowl/client/internal/fusefs"
)

func newTestSamplerProvider(exporter sdktrace.SpanExporter, ratio float64) *sdktrace.TracerProvider {
	return sdktrace.NewTracerProvider(
		sdktrace.WithSyncer(exporter),
		sdktrace.WithSampler(newClientTraceSampler(ratio)),
	)
}

func withStatsMuxTransport(t *testing.T, handler http.Handler) *string {
	t.Helper()
	prev := statsHTTPClient
	var lastURL string
	statsHTTPClient = &http.Client{
		Transport: roundTripperFunc(func(req *http.Request) (*http.Response, error) {
			lastURL = req.URL.String()
			rec := httptest.NewRecorder()
			handler.ServeHTTP(rec, req)
			resp := rec.Result()
			resp.Request = req
			return resp, nil
		}),
	}
	t.Cleanup(func() { statsHTTPClient = prev })
	return &lastURL
}

func withStatsErrorTransport(t *testing.T, err error) {
	t.Helper()
	prev := statsHTTPClient
	statsHTTPClient = &http.Client{
		Transport: roundTripperFunc(func(*http.Request) (*http.Response, error) {
			return nil, err
		}),
	}
	t.Cleanup(func() { statsHTTPClient = prev })
}

func setupTestObsAndMux(t *testing.T) (*clientRuntime, *http.ServeMux) {
	t.Helper()
	withCleanOTelAndLogger(t)
	var logBuf bytes.Buffer
	opts := &mountOptions{
		logLevel:         "info",
		logFormat:        "json",
		traceSampleRatio: 1.0,
	}
	obs, err := setupObservability(context.Background(), opts, &logBuf)
	require.NoError(t, err)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = obs.Shutdown(ctx)
	})

	rawFS := fusefs.NewRawFS(nil, fusefs.Config{
		DiskID:  testDiskID1,
		RootIno: 1,
	})
	t.Cleanup(rawFS.OnUnmount)
	require.NoError(t, obs.RegisterCollector(rawFS.Collector()))
	return obs, newAdminMux(obs, rawFS)
}

func TestStats_GivenHelpAndInvalidFlags_ReturnsExpectedExitCodes(t *testing.T) {
	// Given / When / Then
	requireHelpExit0(t, "bowl stats [mountpoint]", "stats", "-h")
	requireHelpExit0(t, "bowl stats [mountpoint]", "stats", "--help")

	requireCLIError2(t, "最多接受 1 个位置参数", "stats", "/mnt/a", "/mnt/b")
	requireCLIError2(t, "--json 与 --raw 不能同时指定", "stats", "--json", "--raw")
	requireCLIError2(t, `未知标志 "--bogus"`, "stats", "--bogus")
	requireCLIError2(t, "--admin-addr 需要指定参数值", "stats", "--admin-addr")
	requireCLIError2(t, "无效的 --admin-addr", "stats", "--admin-addr", "off")
	requireCLIError2(t, "无效的 --admin-addr", "stats", "--admin-addr", "invalid-host")
}

func TestStats_GivenNoOrAmbiguousMounts_ReturnsExit1WithActionableMessage(t *testing.T) {
	// Given: 空元数据目录
	metaDir := t.TempDir()
	withTempMountMetaDirs(t, metaDir)

	// When / Then: 无活跃挂载
	code, _, errOut := runCLI("stats")
	require.Equal(t, 1, code)
	require.Contains(t, errOut, "未发现活跃的 bowl 挂载")

	// Given: 写入两个存活挂载，且另写入一个已死亡 PID 元数据应被自动忽略
	_, err := writeMountMeta(&mountMeta{
		Mountpoint: "/mnt/a",
		PID:        os.Getpid(),
		DiskID:     testDiskID1,
		AdminAddr:  "127.0.0.1:19001",
		StartedAt:  time.Now().UTC(),
	})
	require.NoError(t, err)
	_, err = writeMountMeta(&mountMeta{
		Mountpoint: "/mnt/b",
		PID:        os.Getpid(),
		DiskID:     testDiskID2,
		AdminAddr:  "127.0.0.1:19002",
		StartedAt:  time.Now().UTC(),
	})
	require.NoError(t, err)
	_, err = writeMountMeta(&mountMeta{
		Mountpoint: "/mnt/dead",
		PID:        99999999,
		DiskID:     testDiskID1,
		AdminAddr:  "127.0.0.1:19003",
		StartedAt:  time.Now().UTC(),
	})
	require.NoError(t, err)

	// When / Then: 多挂载未指定路径报错，指定不存在挂载点也报错
	code, _, errOut = runCLI("stats")
	require.Equal(t, 1, code)
	require.Contains(t, errOut, "发现多个活跃挂载")
	require.Contains(t, errOut, "/mnt/a")
	require.Contains(t, errOut, "/mnt/b")
	require.NotContains(t, errOut, "/mnt/dead")

	code, _, errOut = runCLI("stats", "/mnt/missing")
	require.Equal(t, 1, code)
	require.Contains(t, errOut, "未找到挂载点")
}

func TestStats_GivenSingleActiveMountAndSymlink_DiscoversAdminAndRendersFormats(t *testing.T) {
	// Given
	_, mux := setupTestObsAndMux(t)
	lastURL := withStatsMuxTransport(t, mux)
	metaDir := t.TempDir()
	withTempMountMetaDirs(t, metaDir)

	realMP := filepath.Join(t.TempDir(), "real-mp")
	require.NoError(t, os.MkdirAll(realMP, 0o755))
	symMP := filepath.Join(t.TempDir(), "sym-mp")
	require.NoError(t, os.Symlink(realMP, symMP))

	_, err := writeMountMeta(&mountMeta{
		Mountpoint: realMP,
		PID:        os.Getpid(),
		DiskID:     testDiskID1,
		AdminAddr:  "127.0.0.1:19100",
		StartedAt:  time.Now().UTC(),
	})
	require.NoError(t, err)

	// When: 自动发现单一挂载默认 text 输出
	code, textOut, errOut := runCLI("stats")
	require.Equal(t, 0, code)
	require.Empty(t, errOut)
	require.Equal(t, "http://127.0.0.1:19100/debug/stats?format=text", *lastURL)
	require.NotEmpty(t, textOut)

	// When: 通过符号链接指定挂载点输出 --json
	code, jsonOut, errOut := runCLI("stats", symMP, "--json")
	require.Equal(t, 0, code)
	require.Empty(t, errOut)
	require.Equal(t, "http://127.0.0.1:19100/debug/stats?format=json", *lastURL)
	var snap fusefs.Snapshot
	require.NoError(t, json.Unmarshal([]byte(jsonOut), &snap))
	require.Len(t, snap.FUSE.Ops, fusefs.OpCount)

	// When: 显式 --admin-addr 归一化并输出 --raw (.stats 格式)
	code, rawOut, errOut := runCLI("stats", "--admin-addr", "http://127.0.0.1:19200/", "--raw")
	require.Equal(t, 0, code)
	require.Empty(t, errOut)
	require.Equal(t, "http://127.0.0.1:19200/debug/stats?format=stats", *lastURL)
	require.NotEmpty(t, rawOut)
}

func TestStats_GivenDisabledAdminOrHTTPError_ReturnsExit1(t *testing.T) {
	// Given: 管理面被显式关闭
	metaDir := t.TempDir()
	withTempMountMetaDirs(t, metaDir)
	_, err := writeMountMeta(&mountMeta{
		Mountpoint: "/mnt/no-admin",
		PID:        os.Getpid(),
		DiskID:     testDiskID1,
		AdminAddr:  "off",
		StartedAt:  time.Now().UTC(),
	})
	require.NoError(t, err)

	code, _, errOut := runCLI("stats")
	require.Equal(t, 1, code)
	require.Contains(t, errOut, "未开启管理面")

	// Given: 传输失败
	withStatsErrorTransport(t, errors.New("connection refused"))
	code, _, errOut = runCLI("stats", "--admin-addr", "127.0.0.1:19300")
	require.Equal(t, 1, code)
	require.Contains(t, errOut, "请求管理面 127.0.0.1:19300 失败")

	// Given: 非 200 响应
	withStatsMuxTransport(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "internal error", http.StatusInternalServerError)
	}))
	code, _, errOut = runCLI("stats", "--admin-addr", "127.0.0.1:19300")
	require.Equal(t, 1, code)
	require.Contains(t, errOut, "管理面返回 500")
}

func TestAdminObservability_GivenBareOTLPEndpoint_NormalizesSchemeWithoutExternalDial(t *testing.T) {
	// Given
	require.Empty(t, normalizeOTLPEndpoint("   "))
	require.Equal(t, "http://172.16.1.205:4317", normalizeOTLPEndpoint("172.16.1.205:4317"))
	require.Equal(t, "http://172.16.1.205:4317", normalizeOTLPEndpoint("  172.16.1.205:4317  "))
	require.Equal(t, "http://127.0.0.1:4317", normalizeOTLPEndpoint("http://127.0.0.1:4317"))
	require.Equal(t, "https://collector.example.com:4317", normalizeOTLPEndpoint("https://collector.example.com:4317"))

	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "172.16.1.205:4317")
	t.Setenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", "")

	// When
	cfg := buildObsConfig(&mountOptions{
		logLevel:         "info",
		logFormat:        "json",
		traceSampleRatio: 0.001,
	})

	// Then
	require.Equal(t, "http://172.16.1.205:4317", os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT"))
	require.Equal(t, "http://172.16.1.205:4317", cfg.Endpoint())
	require.True(t, cfg.Insecure())
}

func TestAdminObservability_GivenPriorityOpSampler_BoostsBarriersAndLinkedSpans(t *testing.T) {
	// Given
	exporter := tracetest.NewInMemoryExporter()
	tp := newTestSamplerProvider(exporter, 1e-12)
	defer func() { _ = tp.Shutdown(context.Background()) }()
	tracer := tp.Tracer("bowl/client/test")

	// When / Then: Fsync / FsyncDir / OnUnmount 在极低采样率下确定性采样
	_, spFsync := tracer.Start(context.Background(), "fuse.Fsync")
	require.True(t, spFsync.SpanContext().IsSampled())
	spFsync.End()

	_, spFsyncDir := tracer.Start(context.Background(), "fuse.FsyncDir")
	require.True(t, spFsyncDir.SpanContext().IsSampled())
	spFsyncDir.End()

	_, spUnmount := tracer.Start(context.Background(), "fuse.OnUnmount")
	require.True(t, spUnmount.SpanContext().IsSampled())
	spUnmount.End()

	// 携带已采样 SpanLink 的后台异步根 Span 跟随采样
	sampledCtx := trace.ContextWithSpanContext(context.Background(), trace.NewSpanContext(trace.SpanContextConfig{
		TraceID:    [16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16},
		SpanID:     [8]byte{1, 2, 3, 4, 5, 6, 7, 8},
		TraceFlags: trace.FlagsSampled,
	}))
	_, linkedRoot := tracer.Start(context.Background(), "stage.UploadChunk",
		trace.WithNewRoot(),
		trace.WithLinks(trace.LinkFromContext(sampledCtx)),
	)
	require.True(t, linkedRoot.SpanContext().IsSampled())
	linkedRoot.End()

	// 采样率为 0 时完全关闭采样
	zeroTP := newTestSamplerProvider(exporter, 0)
	defer func() { _ = zeroTP.Shutdown(context.Background()) }()
	_, zeroSpan := zeroTP.Tracer("bowl/client/test").Start(context.Background(), "fuse.Fsync")
	require.False(t, zeroSpan.SpanContext().IsSampled())
	zeroSpan.End()
}

func TestAdminMux_GivenSubMsMetricsAndInvalidStatsFormat_ServesExpectedResponsesWithoutNetwork(t *testing.T) {
	// Given
	obs, mux := setupTestObsAndMux(t)
	hist, err := obs.meterProvider.Meter("connectrpc").Float64Histogram("rpc.client.duration")
	require.NoError(t, err)
	hist.Record(context.Background(), 0.00003)

	// When: 请求 /metrics
	metricsReq := httptest.NewRequest(http.MethodGet, "/metrics", http.NoBody)
	metricsRec := httptest.NewRecorder()
	mux.ServeHTTP(metricsRec, metricsReq)

	// Then
	require.Equal(t, http.StatusOK, metricsRec.Code)
	metricsText := metricsRec.Body.String()
	require.Contains(t, metricsText, `le="2.5e-05"} 0`)
	require.Contains(t, metricsText, `le="5e-05"} 1`)
	require.Contains(t, metricsText, "bowl_client_stage_writes_total")
	require.Contains(t, metricsText, "bowl_client_fuse_connection_waiting")

	// When: 非法 format 返回 400
	badReq := httptest.NewRequest(http.MethodGet, "/debug/stats?format=xml", http.NoBody)
	badRec := httptest.NewRecorder()
	mux.ServeHTTP(badRec, badReq)
	require.Equal(t, http.StatusBadRequest, badRec.Code)

	// When: startAdminServer("off") 返回 nil 且方法对 nil 安全
	disabled, startErr := startAdminServer("off", obs, nil)
	require.NoError(t, startErr)
	require.Nil(t, disabled)
	require.Equal(t, "off", disabled.addr())
	disabled.shutdown(context.Background())
}
