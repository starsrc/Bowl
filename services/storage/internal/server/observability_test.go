package server

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"connectrpc.com/connect"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"

	"github.com/starsrc/pkg/observability"
	"github.com/starsrc/pkg/observability/slogx"
	"github.com/starsrc/pkg/observability/telemetry"

	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
	"github.com/starsrc/bowl/gen/go/storage/v1/storagev1connect"
	"github.com/starsrc/bowl/services/storage/internal/config"
	"github.com/starsrc/bowl/services/storage/internal/engine"
)

type syncLogBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncLogBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncLogBuffer) Records(t *testing.T) []map[string]any {
	t.Helper()
	b.mu.Lock()
	raw := b.buf.String()
	b.mu.Unlock()

	lines := strings.Split(strings.TrimSpace(raw), "\n")
	out := make([]map[string]any, 0, len(lines))
	for _, line := range lines {
		if strings.TrimSpace(line) == "" {
			continue
		}
		var rec map[string]any
		require.NoError(t, json.Unmarshal([]byte(line), &rec))
		out = append(out, rec)
	}
	return out
}

func installOTelCleanup(t *testing.T) {
	t.Helper()
	prevMP := otel.GetMeterProvider()
	prevTP := otel.GetTracerProvider()
	prevProp := otel.GetTextMapPropagator()
	t.Cleanup(func() {
		if otel.GetMeterProvider() != prevMP {
			otel.SetMeterProvider(prevMP)
		}
		if otel.GetTracerProvider() != prevTP {
			otel.SetTracerProvider(prevTP)
		}
		otel.SetTextMapPropagator(prevProp)
	})
}

func assertMetricFamiliesFineBuckets(t *testing.T, fams []*dto.MetricFamily, minReqBytes, minRespBytes float64) {
	t.Helper()
	byName := make(map[string]*dto.MetricFamily, len(fams))
	for _, fam := range fams {
		byName[fam.GetName()] = fam
	}

	if targetFam := byName["target_info"]; targetFam != nil {
		require.Len(t, targetFam.GetMetric(), 1)
	}

	rpcDurFam := byName["rpc_server_call_duration_seconds"]
	require.NotNil(t, rpcDurFam)
	require.NotEmpty(t, rpcDurFam.GetMetric())
	rpcBuckets := rpcDurFam.GetMetric()[0].GetHistogram().GetBucket()
	require.GreaterOrEqual(t, len(rpcBuckets), len(SubMillisecondBuckets))
	require.InDelta(t, 0.000025, rpcBuckets[0].GetUpperBound(), 1e-12)

	httpDurFam := byName["bowl_storage_http_request_duration_seconds"]
	require.NotNil(t, httpDurFam)
	var sawWrite2xx, sawRead2xx, sawFork2xx, sawOther4xx bool
	for _, m := range httpDurFam.GetMetric() {
		var proc, sc string
		for _, lp := range m.GetLabel() {
			switch lp.GetName() {
			case "procedure":
				proc = lp.GetValue()
			case "status_class":
				sc = lp.GetValue()
			}
		}
		if proc == storagev1connect.StorageServiceWriteProcedure && sc == "2xx" {
			sawWrite2xx = true
		}
		if proc == storagev1connect.StorageServiceReadProcedure && sc == "2xx" {
			sawRead2xx = true
		}
		if proc == storagev1connect.StorageServiceForkBranchProcedure && sc == "2xx" {
			sawFork2xx = true
		}
		if proc == "other" && sc == "4xx" {
			sawOther4xx = true
		}
	}
	require.True(t, sawWrite2xx)
	require.True(t, sawRead2xx)
	require.True(t, sawFork2xx)
	require.True(t, sawOther4xx)

	reqBytesFam := byName["bowl_storage_http_request_bytes_total"]
	require.NotNil(t, reqBytesFam)
	var writeReqBytes float64
	for _, m := range reqBytesFam.GetMetric() {
		for _, lp := range m.GetLabel() {
			if lp.GetName() == "procedure" && lp.GetValue() == storagev1connect.StorageServiceWriteProcedure {
				writeReqBytes = m.GetCounter().GetValue()
			}
		}
	}
	require.GreaterOrEqual(t, writeReqBytes, minReqBytes)

	respBytesFam := byName["bowl_storage_http_response_bytes_total"]
	require.NotNil(t, respBytesFam)
	var readRespBytes float64
	for _, m := range respBytesFam.GetMetric() {
		for _, lp := range m.GetLabel() {
			if lp.GetName() == "procedure" && lp.GetValue() == storagev1connect.StorageServiceReadProcedure {
				readRespBytes = m.GetCounter().GetValue()
			}
		}
	}
	require.GreaterOrEqual(t, readRespBytes, minRespBytes)
}

func TestObservability_EndToEndRPC_ExportsFineBucketMetricsSpansAndJSONLogs(t *testing.T) {
	ctx := context.Background()
	installOTelCleanup(t)

	obsCfg := observability.DefaultConfig()
	obsCfg.AdminAddr = "off"
	obsCfg.LogLevel = "debug"
	obsCfg.ApplyDefaultTrustRemote(true)

	logBuf := &syncLogBuffer{}
	logger := slog.New(slog.NewJSONHandler(logBuf, &slog.HandlerOptions{Level: slog.LevelDebug}))

	spanRec := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithSampler(NewStorageSampler(0.0001)),
		sdktrace.WithSpanProcessor(spanRec),
	)
	t.Cleanup(func() {
		_ = tp.Shutdown(context.Background())
	})
	telemetry.RegisterGlobalTracer(tp)

	node, err := engine.OpenNode(engine.NodeOptions{
		DataDir: t.TempDir(),
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, node.Close(context.Background()))
	})

	srvCfg := config.ServerConfig{
		ServerHost:         "127.0.0.1",
		ServerPort:         0,
		InternalServerHost: "127.0.0.1",
		InternalServerPort: 19090,
		ServerReadMaxBytes: config.DefaultServerReadMaxBytes,
	}
	fullCfg := &config.Config{
		ServerConfig: srvCfg,
		EnvConfig:    obsCfg,
	}

	httpMetrics := NewHTTPMetrics()
	storageCollector := NewStorageMetricsCollector(node)
	obsRT, err := observability.Setup(
		ctx,
		config.NewObservabilityConfig(&fullCfg.EnvConfig),
		observability.WithSetDefaultLogger(false),
		observability.WithCollectors(httpMetrics, storageCollector),
	)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, obsRT.Shutdown(context.Background()))
	})

	mp, err := ConfigureFineBucketMeterProvider(obsRT, &fullCfg.TelemetryConfig, fullCfg)
	require.NoError(t, err)
	require.NotNil(t, mp)
	t.Cleanup(func() {
		require.NoError(t, mp.Shutdown(context.Background()))
	})

	srv, err := New(Params{
		Config:        fullCfg.ServerConfig,
		MetaVal:       fullCfg.Meta,
		Node:          node,
		Observability: fullCfg,
		Telemetry:     &fullCfg.TelemetryConfig,
		AdminCfg:      fullCfg,
		Gatherer:      obsRT.Gatherer(),
		HTTPMetrics:   httpMetrics,
		Logger:        logger,
	})
	require.NoError(t, err)

	ts := httptest.NewServer(srv.Handler())
	t.Cleanup(ts.Close)
	client := storagev1connect.NewStorageServiceClient(ts.Client(), ts.URL)

	createReq := connect.NewRequest(&storagev1.CreateDiskRequest{
		Name:     "obs-disk",
		MaxBytes: 4 * 1024 * 1024,
	})
	createReq.Header().Set("X-Request-Id", "req-obs-1")
	created, err := client.CreateDisk(ctx, createReq)
	require.NoError(t, err)
	diskID := created.Msg.GetDisk().GetDiskId()

	mountResp, err := client.MountBranch(ctx, connect.NewRequest(&storagev1.MountBranchRequest{
		DiskId: diskID,
		Branch: defaultBranch(),
	}))
	require.NoError(t, err)
	epoch := mountResp.Msg.GetEpoch()

	fileResp, err := client.Create(ctx, connect.NewRequest(&storagev1.CreateRequest{
		DiskId:    diskID,
		Branch:    defaultBranch(),
		ParentIno: engine.RootIno,
		Name:      []byte("data.bin"),
		Mode:      0o644,
		Epoch:     epoch,
	}))
	require.NoError(t, err)
	fileIno := fileResp.Msg.GetInode().GetIno()

	payload := make([]byte, 4096)
	state := uint64(0x9e3779b97f4a7c15)
	for i := range payload {
		state ^= state << 13
		state ^= state >> 7
		state ^= state << 17
		payload[i] = byte(state)
	}
	_, err = client.Write(ctx, connect.NewRequest(&storagev1.WriteRequest{
		DiskId: diskID,
		Branch: defaultBranch(),
		Ino:    fileIno,
		Data:   payload,
		Epoch:  epoch,
	}))
	require.NoError(t, err)

	readResp, err := client.Read(ctx, connect.NewRequest(&storagev1.ReadRequest{
		DiskId: diskID,
		Branch: defaultBranch(),
		Ino:    fileIno,
		Size:   4096,
	}))
	require.NoError(t, err)
	require.Equal(t, payload, readResp.Msg.GetData())

	unsampledTraceID, err := trace.TraceIDFromHex("4bf92f3577b34da6a3ce929d0e0e4736")
	require.NoError(t, err)
	unsampledSpanID, err := trace.SpanIDFromHex("00f067aa0ba902b7")
	require.NoError(t, err)
	remoteUnsampledCtx := trace.ContextWithRemoteSpanContext(ctx, trace.NewSpanContext(trace.SpanContextConfig{
		TraceID:    unsampledTraceID,
		SpanID:     unsampledSpanID,
		TraceFlags: 0,
		Remote:     true,
	}))
	forkReq := connect.NewRequest(&storagev1.ForkBranchRequest{
		DiskId:       diskID,
		SourceBranch: defaultBranch(),
		NewBranch:    "feat-obs",
	})
	otel.GetTextMapPropagator().Inject(remoteUnsampledCtx, propagation.HeaderCarrier(forkReq.Header()))
	_, err = client.ForkBranch(ctx, forkReq)
	require.NoError(t, err)

	unknownResp, err := ts.Client().Get(ts.URL + "/unknown-path")
	require.NoError(t, err)
	_ = unknownResp.Body.Close()
	require.Equal(t, http.StatusNotFound, unknownResp.StatusCode)

	fams, err := obsRT.Registry().Gather()
	require.NoError(t, err)
	assertMetricFamiliesFineBuckets(t, fams, float64(len(payload)), float64(len(payload)))

	adminReq := httptest.NewRequest(http.MethodGet, "/metrics", http.NoBody)
	adminRec := httptest.NewRecorder()
	srv.AdminHandler().ServeHTTP(adminRec, adminReq)
	require.Equal(t, http.StatusOK, adminRec.Code)
	metricsText := adminRec.Body.String()
	for _, want := range []string{
		"go_goroutines",
		"bowl_storage_disks_loaded",
		"bowl_storage_logical_bytes",
		"bowl_storage_pack_physical_bytes",
		"bowl_storage_inodes",
		"bowl_storage_pebble_wal_bytes_in_total",
		"bowl_storage_pebble_wal_bytes_written_total",
		"bowl_storage_pebble_l0_sublevels",
		"bowl_storage_pebble_l0_tables",
		"bowl_storage_pebble_read_amp",
		"bowl_storage_pebble_compaction_debt_bytes",
		"bowl_storage_pebble_compactions_in_progress",
		"bowl_storage_pebble_flushes_in_progress",
		"bowl_storage_pebble_filter_hits_total",
		"bowl_storage_pebble_filter_misses_total",
		"bowl_storage_pebble_open_table_iters",
		"bowl_storage_pebble_block_cache_bytes",
		"bowl_storage_pebble_block_cache_count",
		"bowl_storage_pebble_block_cache_hits_total",
		"bowl_storage_pebble_block_cache_misses_total",
		"bowl_storage_http_request_duration_seconds_bucket",
		"rpc_server_call_duration_seconds_bucket",
	} {
		require.Contains(t, metricsText, want)
	}
	require.NotContains(t, metricsText, "bowl_storage_branch_actors_loaded")
	require.NotContains(t, metricsText, "bowl_storage_projection_")
	require.NotContains(t, metricsText, diskID)
	require.NotContains(t, metricsText, "disk_id=")
	require.NotContains(t, metricsText, "bowl_disk_id=")
	if runtime.GOOS == "linux" {
		require.Contains(t, metricsText, "process_cpu_seconds_total")
	}

	var foundPromotedFork bool
	for _, sp := range spanRec.Ended() {
		if sp.Name() == "storage.v1.StorageService/ForkBranch" || sp.Name() == storagev1connect.StorageServiceForkBranchProcedure {
			require.Equal(t, unsampledTraceID, sp.SpanContext().TraceID())
			foundPromotedFork = true
		}
	}
	require.True(t, foundPromotedFork)

	recs := logBuf.Records(t)
	var sawReqID bool
	for _, rec := range recs {
		if rec[logKeyRequestID] == "req-obs-1" && rec[logKeyDiskID] == diskID {
			sawReqID = true
		}
	}
	require.True(t, sawReqID)
}

func TestObservability_ReadyzChecksNodeStore(t *testing.T) {
	ctx := context.Background()
	node, err := engine.OpenNode(engine.NodeOptions{
		DataDir: t.TempDir(),
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, node.Close(context.Background()))
	})

	srv, err := New(Params{
		Config: config.ServerConfig{
			ServerHost:         "127.0.0.1",
			ServerPort:         0,
			InternalServerHost: "127.0.0.1",
			InternalServerPort: 19091,
		},
		Node: node,
	})
	require.NoError(t, err)

	reqCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	req := httptest.NewRequest(http.MethodGet, "/readyz", http.NoBody).WithContext(reqCtx)
	rec := httptest.NewRecorder()
	srv.AdminHandler().ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)
	body, err := io.ReadAll(rec.Body)
	require.NoError(t, err)
	require.Contains(t, string(body), `"storage":"ok"`)
	require.NotContains(t, string(body), "postgres")

	require.NoError(t, node.Close(ctx))

	closedReq := httptest.NewRequest(http.MethodGet, "/readyz", http.NoBody).WithContext(reqCtx)
	closedRec := httptest.NewRecorder()
	srv.AdminHandler().ServeHTTP(closedRec, closedReq)
	require.Equal(t, http.StatusServiceUnavailable, closedRec.Code)
	closedBody, err := io.ReadAll(closedRec.Body)
	require.NoError(t, err)
	require.Contains(t, string(closedBody), "storage")
}

func TestObservability_SlowRPCWarningThresholds(t *testing.T) {
	logBuf := &syncLogBuffer{}
	logger := slog.New(slog.NewJSONHandler(logBuf, &slog.HandlerOptions{Level: slog.LevelWarn}))

	t0 := time.Unix(1700000000, 0)
	makeClock := func(delta time.Duration) func() time.Time {
		calls := 0
		return func() time.Time {
			calls++
			if calls == 1 {
				return t0
			}
			return t0.Add(delta)
		}
	}

	ctrlReq := connect.NewRequest(&storagev1.CreateDiskRequest{Name: "d", MaxBytes: 4096})
	dataReq := connect.NewRequest(&storagev1.ReadRequest{DiskId: validTestDiskUUID, Branch: defaultBranch(), Ino: 2, Size: 64})

	nextOK := func(_ context.Context, _ connect.AnyRequest) (connect.AnyResponse, error) {
		return connect.NewResponse(&storagev1.ReadResponse{}), nil
	}

	dataSlow := newStorageObservabilityInterceptorWithClock(logger, makeClock(SlowStorageRPCThreshold+time.Millisecond))
	_, err := dataSlow(nextOK)(context.Background(), withSpecProcedure(dataReq, storagev1connect.StorageServiceReadProcedure))
	require.NoError(t, err)

	ctrlFast := newStorageObservabilityInterceptorWithClock(logger, makeClock(SlowStorageRPCThreshold+time.Millisecond))
	_, err = ctrlFast(nextOK)(context.Background(), withSpecProcedure(ctrlReq, storagev1connect.StorageServiceCreateDiskProcedure))
	require.NoError(t, err)

	recs := logBuf.Records(t)
	require.Len(t, recs, 1)
	require.Equal(t, storagev1connect.StorageServiceReadProcedure, recs[0][slogx.DefaultProcedureKey])
	require.Equal(t, "slow storage rpc", recs[0]["msg"])
}

type specOverrideReq struct {
	connect.AnyRequest
	proc string
}

func (r specOverrideReq) Spec() connect.Spec {
	sp := r.AnyRequest.Spec()
	sp.Procedure = r.proc
	return sp
}

func withSpecProcedure(req connect.AnyRequest, proc string) connect.AnyRequest {
	return specOverrideReq{AnyRequest: req, proc: proc}
}

var _ ObservabilityConfig = (*telemetryObsAdapter)(nil)
