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
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/fx"

	"github.com/starsrc/pkg/observability"
	"github.com/starsrc/pkg/observability/obsfx"
	"github.com/starsrc/pkg/observability/slogx"
	"github.com/starsrc/pkg/observability/telemetry"
	"github.com/starsrc/pkg/serverx"

	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
	"github.com/starsrc/bowl/gen/go/storage/v1/storagev1connect"
	"github.com/starsrc/bowl/services/storage/internal/config"
	"github.com/starsrc/bowl/services/storage/internal/control"
	"github.com/starsrc/bowl/services/storage/internal/db"
	"github.com/starsrc/bowl/services/storage/internal/store"
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
		require.NoError(t, json.Unmarshal([]byte(line), &rec), "日志行必须为合法 JSON: %s", line)
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

func generateXorshiftPayload(size int) []byte {
	payload := make([]byte, size)
	state := uint64(0x9e3779b97f4a7c15)
	for i := range payload {
		state ^= state << 13
		state ^= state >> 7
		state ^= state << 17
		payload[i] = byte(state)
	}
	return payload
}

func assertMetricFamiliesFineBuckets(
	t *testing.T,
	fams []*dto.MetricFamily,
	minReqBytes float64,
	minRespBytes float64,
) {
	t.Helper()
	byName := make(map[string]*dto.MetricFamily, len(fams))
	for _, fam := range fams {
		byName[fam.GetName()] = fam
	}

	if targetFam := byName["target_info"]; targetFam != nil {
		require.Len(t, targetFam.GetMetric(), 1, "升级 MeterProvider 后 target_info 不得重复")
	}

	rpcDurFam := byName["rpc_server_call_duration_seconds"]
	require.NotNil(t, rpcDurFam, "必须导出 otelconnect rpc_server_call_duration_seconds 直方图")
	require.NotEmpty(t, rpcDurFam.GetMetric())
	rpcBuckets := rpcDurFam.GetMetric()[0].GetHistogram().GetBucket()
	require.GreaterOrEqual(t, len(rpcBuckets), len(SubMillisecondBuckets))
	require.InDelta(t, 0.000025, rpcBuckets[0].GetUpperBound(), 1e-12, "首桶上界必须下探至 25µs")
	require.InDelta(t, 10.0, rpcBuckets[len(SubMillisecondBuckets)-1].GetUpperBound(), 1e-9)

	httpDurFam := byName["bowl_storage_http_request_duration_seconds"]
	require.NotNil(t, httpDurFam, "必须导出 HTTP 入口直方图")
	var sawWrite2xx, sawRead2xx, sawFork2xx, sawFork4xx, sawOther4xx bool
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
		require.Len(t, m.GetHistogram().GetBucket(), len(SubMillisecondBuckets))
		require.InDelta(t, 0.000025, m.GetHistogram().GetBucket()[0].GetUpperBound(), 1e-12)
		if proc == storagev1connect.StorageServiceWriteProcedure && sc == "2xx" && m.GetHistogram().GetSampleCount() == 1 {
			sawWrite2xx = true
		}
		if proc == storagev1connect.StorageServiceReadProcedure && sc == "2xx" && m.GetHistogram().GetSampleCount() == 1 {
			sawRead2xx = true
		}
		if proc == storagev1connect.StorageServiceForkDiskProcedure && sc == "2xx" && m.GetHistogram().GetSampleCount() == 1 {
			sawFork2xx = true
		}
		if proc == storagev1connect.StorageServiceForkDiskProcedure && sc == "4xx" && m.GetHistogram().GetSampleCount() == 1 {
			sawFork4xx = true
		}
		if proc == "other" && sc == "4xx" && m.GetHistogram().GetSampleCount() == 1 {
			sawOther4xx = true
		}
	}
	require.True(t, sawWrite2xx, "入口 HTTP 直方图必须记录 Write 2xx")
	require.True(t, sawRead2xx, "入口 HTTP 直方图必须记录 Read 2xx")
	require.True(t, sawFork2xx, "入口 HTTP 直方图必须记录 ForkDisk 2xx")
	require.True(t, sawFork4xx, "入口 HTTP 直方图必须记录 ForkDisk 4xx")
	require.True(t, sawOther4xx, "未知路径必须收敛至 procedure=other, status_class=4xx")

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

func assertEndToEndStorageSpans(
	t *testing.T,
	spans tracetest.SpanStubs,
	sourceDiskID string,
	forkedDiskID string,
	missingSourceDiskID string,
) {
	t.Helper()
	spanNames := make(map[string]int)
	var forkSuccessSpan, forkFailureSpan *tracetest.SpanStub
	for i := range spans {
		sp := &spans[i]
		spanNames[sp.Name]++
		if sp.Name == "storage.v1.StorageService/ForkDisk" || sp.Name == storagev1connect.StorageServiceForkDiskProcedure {
			var dID, srcID string
			for _, attr := range sp.Attributes {
				switch string(attr.Key) {
				case attrKeyBowlDiskID:
					dID = attr.Value.AsString()
				case attrKeyBowlSourceDiskID:
					srcID = attr.Value.AsString()
				}
			}
			if dID == forkedDiskID {
				forkSuccessSpan = sp
			} else if srcID == missingSourceDiskID && dID == "" {
				forkFailureSpan = sp
			}
		}
	}
	require.NotNil(t, forkSuccessSpan, "ForkDisk 成功 Span 必须记录新建子盘 bowl.disk_id")
	var forkSuccessSrc string
	for _, attr := range forkSuccessSpan.Attributes {
		if string(attr.Key) == attrKeyBowlSourceDiskID {
			forkSuccessSrc = attr.Value.AsString()
		}
	}
	require.Equal(t, sourceDiskID, forkSuccessSrc, "ForkDisk 成功 Span 必须同时记录源盘 bowl.source_disk_id")
	require.NotNil(t, forkFailureSpan, "ForkDisk 失败 Span 必须独立记录源盘 bowl.source_disk_id 且不得混入 bowl.disk_id")

	for _, expected := range []string{
		"control.OpenDiskActor",
		"store.BlobWrite",
		"store.QueueWait",
		"store.Tick",
		"store.PhaseA",
		"store.PhaseB",
		"store.SyncBarrier",
		"store.KVGet",
		"store.KVScan",
	} {
		require.GreaterOrEqual(t, spanNames[expected], 1, "端到端 RPC 链路缺少子 Span %s", expected)
	}
}

func assertMetricsTextContainsAll(t *testing.T, metricsText string) {
	t.Helper()
	for _, wantMetric := range []string{
		"go_goroutines",
		"go_memstats_alloc_bytes",
		"go_gc_duration_seconds",
		"go_build_info",
		"target_info",
		"bowl_storage_disks_loaded",
		"bowl_storage_logical_bytes",
		"bowl_storage_inodes",
		"bowl_storage_http_request_duration_seconds_bucket",
		"bowl_storage_blob_sync_duration_seconds_bucket",
		"rpc_server_call_duration_seconds_bucket",
	} {
		require.Contains(t, metricsText, wantMetric, "/metrics 必须导出指标 %s", wantMetric)
	}
	if runtime.GOOS == "linux" {
		require.Contains(t, metricsText, "process_cpu_seconds_total")
		require.Contains(t, metricsText, "process_resident_memory_bytes")
	}
}

func assertPromotedForkSpan(t *testing.T, spans tracetest.SpanStubs, wantTraceID trace.TraceID) {
	t.Helper()
	var found bool
	for i := range spans {
		sp := &spans[i]
		if sp.Name == "storage.v1.StorageService/ForkDisk" || sp.Name == storagev1connect.StorageServiceForkDiskProcedure {
			require.Equal(t, wantTraceID, sp.SpanContext.TraceID(), "提权采样后必须沿用客户端 traceparent TraceID")
			found = true
		}
	}
	require.True(t, found, "携带未采样 traceparent 的控制面 ForkDisk 必须被处理端 100%% 提权录制")
}

func assertLogRecordExists(
	t *testing.T,
	records []map[string]any,
	wantLevel string,
	wantProcedure string,
	wantMsgSubstring string,
) map[string]any {
	t.Helper()
	for _, rec := range records {
		lvl, _ := rec["level"].(string)
		msg, _ := rec["msg"].(string)
		proc, _ := rec[slogx.DefaultProcedureKey].(string)
		if lvl == wantLevel && proc == wantProcedure && strings.Contains(msg, wantMsgSubstring) {
			return rec
		}
	}
	require.FailNowf(t, "缺少预期结构化日志记录", "wantLevel=%s wantProcedure=%s wantMsgSubstring=%s total=%d", wantLevel, wantProcedure, wantMsgSubstring, len(records))
	return nil
}

func assertNoSlowRPCWarning(t *testing.T, records []map[string]any, procedure string) {
	t.Helper()
	for _, rec := range records {
		lvl, _ := rec["level"].(string)
		msg, _ := rec["msg"].(string)
		proc, _ := rec[slogx.DefaultProcedureKey].(string)
		if lvl == "WARN" && proc == procedure && msg == "slow storage rpc" {
			require.FailNowf(t, "不应触发慢操作告警", "procedure=%s rec=%v", procedure, rec)
		}
	}
}

func assertManualReaderHasFineBuckets(t *testing.T, rm metricdata.ResourceMetrics) {
	t.Helper()
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != "rpc.server.call.duration" {
				continue
			}
			hist, ok := m.Data.(metricdata.Histogram[float64])
			require.True(t, ok, "rpc.server.call.duration 应为 Histogram[float64]")
			require.NotEmpty(t, hist.DataPoints, "Histogram 应至少含有一个数据点")
			dp := hist.DataPoints[0]
			require.Equal(t, SubMillisecondBuckets, dp.Bounds, "ManualReader 采集的桶配置必须匹配 SubMillisecondBuckets")
			require.GreaterOrEqual(t, dp.Count, uint64(1), "Histogram 样本计数应至少为 1")
			return
		}
	}
	require.FailNow(t, "ManualReader 未采集到 rpc.server.call.duration 指标")
}

func makeStepClock(t0 time.Time, delta time.Duration) func() time.Time {
	calls := 0
	return func() time.Time {
		calls++
		if calls == 1 {
			return t0
		}
		return t0.Add(delta)
	}
}

func TestObservability_EndToEndRPC_ExportsFineBucketMetricsSpansAndJSONLogs(t *testing.T) {

	ctx := context.Background()
	installOTelCleanup(t)

	obsCfg := observability.DefaultConfig()
	obsCfg.AdminAddr = "off"
	obsCfg.LogLevel = "debug"

	logBuf := &syncLogBuffer{}
	logger := slog.New(slog.NewJSONHandler(logBuf, &slog.HandlerOptions{Level: slog.LevelDebug}))

	httpMetrics := NewHTTPMetrics()
	storeMetrics := store.NewMetrics()
	mgr := control.New(
		db.NewMemStore(),
		control.WithStorageRoot(t.TempDir()),
		control.WithStoreMetrics(storeMetrics),
	)
	t.Cleanup(func() { _ = mgr.Close() })

	rt, err := observability.Setup(
		ctx,
		config.NewObservabilityConfig(&obsCfg),
		observability.WithLogWriter(io.Discard),
		observability.WithSetDefaultLogger(false),
		observability.WithCollectors(httpMetrics, storeMetrics, mgr.Collector()),
	)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, rt.Shutdown(context.Background())) })

	ready, err := ConfigureFineBucketMeterProvider(FineBucketParams{
		Runtime:   rt,
		Telemetry: obsCfg,
		AdminCfg:  obsCfg,
	})
	require.NoError(t, err)
	fineMP, ok := otel.GetMeterProvider().(*sdkmetric.MeterProvider)
	require.True(t, ok)
	t.Cleanup(func() { require.NoError(t, fineMP.Shutdown(context.Background())) })

	spanExporter := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithSyncer(spanExporter),
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
	)
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.TraceContext{})
	t.Cleanup(func() { _ = tp.Shutdown(context.Background()) })

	srv, err := New(Params{
		Config: config.ServerConfig{
			ServerPort:         0,
			InternalServerPort: 9090,
			ServerReadMaxBytes: 8 << 20,
		},
		Control:       mgr,
		Observability: obsCfg,
		Telemetry:     obsCfg,
		AdminCfg:      obsCfg,
		Gatherer:      rt.Gatherer(),
		FineBuckets:   ready,
		HTTPMetrics:   httpMetrics,
		Logger:        logger,
	})
	require.NoError(t, err)
	require.NotNil(t, srv.AdminHandler())

	httpClient := &http.Client{Transport: memoryRoundTripper{handler: srv.Handler()}}
	client := storagev1connect.NewStorageServiceClient(httpClient, "http://storage.local")

	createDiskResp, err := client.CreateDisk(ctx, connect.NewRequest(&storagev1.CreateDiskRequest{
		Name:     "obs-e2e-disk",
		MaxBytes: 64 << 20,
	}))
	require.NoError(t, err)
	diskID := createDiskResp.Msg.GetDisk().GetDiskId()

	mountResp, err := client.MountDisk(ctx, connect.NewRequest(&storagev1.MountDiskRequest{DiskId: diskID}))
	require.NoError(t, err)
	epoch := mountResp.Msg.GetEpoch()

	createFileResp, err := client.Create(ctx, connect.NewRequest(&storagev1.CreateRequest{
		DiskId:    diskID,
		Epoch:     epoch,
		ParentIno: store.RootIno,
		Name:      []byte("payload.bin"),
		Mode:      store.S_IFREG | 0o644,
	}))
	require.NoError(t, err)
	fileIno := createFileResp.Msg.GetInode().GetIno()

	const ioSize = 512 * 1024
	payload := generateXorshiftPayload(ioSize)
	_, err = client.Write(ctx, connect.NewRequest(&storagev1.WriteRequest{
		DiskId: diskID,
		Epoch:  epoch,
		Ino:    fileIno,
		Offset: 0,
		Data:   payload,
	}))
	require.NoError(t, err)

	_, err = client.Fsync(ctx, connect.NewRequest(&storagev1.FsyncRequest{
		DiskId: diskID,
		Epoch:  epoch,
		Ino:    fileIno,
	}))
	require.NoError(t, err)

	readResp, err := client.Read(ctx, connect.NewRequest(&storagev1.ReadRequest{
		DiskId: diskID,
		Ino:    fileIno,
		Offset: 0,
		Size:   ioSize,
	}))
	require.NoError(t, err)
	require.Equal(t, payload, readResp.Msg.GetData())
	require.Zero(t, leasedWritePayloadCount())
	require.Zero(t, leasedReadPayloadCount())

	forkResp, err := client.ForkDisk(ctx, connect.NewRequest(&storagev1.ForkDiskRequest{
		SourceDiskId: diskID,
		Name:         "obs-e2e-forked-disk",
	}))
	require.NoError(t, err)
	forkedDiskID := forkResp.Msg.GetDisk().GetDiskId()

	const missingSourceDiskID = "018f0000-0000-7000-8000-000000009999"
	_, forkMissingErr := client.ForkDisk(ctx, connect.NewRequest(&storagev1.ForkDiskRequest{
		SourceDiskId: missingSourceDiskID,
		Name:         "obs-e2e-missing-fork",
	}))
	require.Equal(t, connect.CodeNotFound, connect.CodeOf(forkMissingErr))

	_, lookupMissingErr := client.Lookup(ctx, connect.NewRequest(&storagev1.LookupRequest{
		DiskId:    diskID,
		ParentIno: store.RootIno,
		Name:      []byte("absent-file"),
	}))
	require.Equal(t, connect.CodeNotFound, connect.CodeOf(lookupMissingErr))

	probeResp, err := httpClient.Post("http://storage.local/malicious/random-path-12345", "application/proto", bytes.NewReader([]byte("x")))
	require.NoError(t, err)
	_ = probeResp.Body.Close()

	fams, err := rt.Gatherer().Gather()
	require.NoError(t, err)
	assertMetricFamiliesFineBuckets(t, fams, float64(ioSize), float64(ioSize))

	assertEndToEndStorageSpans(t, spanExporter.GetSpans(), diskID, forkedDiskID, missingSourceDiskID)

	adminRec := httptest.NewRecorder()
	srv.AdminHandler().ServeHTTP(adminRec, httptest.NewRequest(http.MethodGet, "/metrics", http.NoBody))
	require.Equal(t, http.StatusOK, adminRec.Code)
	assertMetricsTextContainsAll(t, adminRec.Body.String())

	records := logBuf.Records(t)
	assertLogRecordExists(t, records, "INFO", storagev1connect.StorageServiceForkDiskProcedure, "")
	assertLogRecordExists(t, records, "WARN", storagev1connect.StorageServiceForkDiskProcedure, "")
	assertLogRecordExists(t, records, "DEBUG", storagev1connect.StorageServiceLookupProcedure, "")
}

func TestObservability_ManualMetricReader_CollectsSubMillisecondBuckets(t *testing.T) {

	ctx := context.Background()
	installOTelCleanup(t)

	reader := sdkmetric.NewManualReader()
	explicitAgg := sdkmetric.Stream{
		Aggregation: sdkmetric.AggregationExplicitBucketHistogram{
			Boundaries: SubMillisecondBuckets,
		},
	}
	mp := sdkmetric.NewMeterProvider(
		sdkmetric.WithReader(reader),
		sdkmetric.WithView(
			sdkmetric.NewView(sdkmetric.Instrument{Name: "rpc.server.call.duration"}, explicitAgg),
		),
	)
	otel.SetMeterProvider(mp)
	t.Cleanup(func() { _ = mp.Shutdown(context.Background()) })

	mgr := control.New(db.NewMemStore(), control.WithStorageRoot(t.TempDir()))
	t.Cleanup(func() { _ = mgr.Close() })

	obsCfg := observability.DefaultConfig()
	obsCfg.OtelEnabled = true
	srv, err := New(Params{
		Config: config.ServerConfig{
			ServerPort:         0,
			InternalServerPort: 0,
			ServerReadMaxBytes: 8 << 20,
		},
		Control:       mgr,
		Observability: obsCfg,
		Telemetry:     obsCfg,
		FineBuckets:   FineBucketsReady{},
	})
	require.NoError(t, err)

	httpClient := &http.Client{Transport: memoryRoundTripper{handler: srv.Handler()}}
	client := storagev1connect.NewStorageServiceClient(httpClient, "http://storage.local")

	_, err = client.ListDisks(ctx, connect.NewRequest(&storagev1.ListDisksRequest{}))
	require.NoError(t, err)

	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(ctx, &rm))

	assertManualReaderHasFineBuckets(t, rm)
}

type slowClockStubHandler struct {
	storagev1connect.UnimplementedStorageServiceHandler
}

func (slowClockStubHandler) Read(context.Context, *connect.Request[storagev1.ReadRequest]) (*connect.Response[storagev1.ReadResponse], error) {
	return connect.NewResponse(&storagev1.ReadResponse{}), nil
}

func (slowClockStubHandler) CreateDisk(context.Context, *connect.Request[storagev1.CreateDiskRequest]) (*connect.Response[storagev1.CreateDiskResponse], error) {
	return connect.NewResponse(&storagev1.CreateDiskResponse{}), nil
}

func (slowClockStubHandler) ForkDisk(context.Context, *connect.Request[storagev1.ForkDiskRequest]) (*connect.Response[storagev1.ForkDiskResponse], error) {
	return connect.NewResponse(&storagev1.ForkDiskResponse{}), nil
}

func TestObservability_SlowRPCAlert_RespectsDataAndControlThresholds(t *testing.T) {
	baseTime := time.Date(2026, 11, 13, 12, 0, 0, 0, time.UTC)

	cases := []struct {
		name          string
		procedure     string
		elapsed       time.Duration
		invoke        func(context.Context, storagev1connect.StorageServiceClient) error
		wantSlowAlert bool
		wantDiskID    string
		wantRequestID string
	}{
		{
			name:      "数据面未超50ms阈值不告警",
			procedure: storagev1connect.StorageServiceReadProcedure,
			elapsed:   40 * time.Millisecond,
			invoke: func(ctx context.Context, client storagev1connect.StorageServiceClient) error {
				_, err := client.Read(ctx, connect.NewRequest(&storagev1.ReadRequest{DiskId: "disk-fast-read"}))
				return err
			},
			wantSlowAlert: false,
		},
		{
			name:      "数据面超过50ms阈值输出慢告警与业务属性",
			procedure: storagev1connect.StorageServiceReadProcedure,
			elapsed:   60 * time.Millisecond,
			invoke: func(ctx context.Context, client storagev1connect.StorageServiceClient) error {
				r := connect.NewRequest(&storagev1.ReadRequest{DiskId: "disk-slow-read"})
				r.Header().Set(headerRequestID, "req-slow-001")
				_, err := client.Read(ctx, r)
				return err
			},
			wantSlowAlert: true,
			wantDiskID:    "disk-slow-read",
			wantRequestID: "req-slow-001",
		},
		{
			name:      "控制面120ms未超200ms阈值不告警",
			procedure: storagev1connect.StorageServiceCreateDiskProcedure,
			elapsed:   120 * time.Millisecond,
			invoke: func(ctx context.Context, client storagev1connect.StorageServiceClient) error {
				_, err := client.CreateDisk(ctx, connect.NewRequest(&storagev1.CreateDiskRequest{Name: "normal-create"}))
				return err
			},
			wantSlowAlert: false,
		},
		{
			name:      "控制面超过200ms阈值输出慢告警",
			procedure: storagev1connect.StorageServiceForkDiskProcedure,
			elapsed:   250 * time.Millisecond,
			invoke: func(ctx context.Context, client storagev1connect.StorageServiceClient) error {
				_, err := client.ForkDisk(ctx, connect.NewRequest(&storagev1.ForkDiskRequest{SourceDiskId: "disk-src-slow"}))
				return err
			},
			wantSlowAlert: true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			runSlowRPCTestCase(
				t,
				baseTime,
				tc.procedure,
				tc.elapsed,
				tc.invoke,
				tc.wantSlowAlert,
				tc.wantDiskID,
				tc.wantRequestID,
			)
		})
	}
}

func runSlowRPCTestCase(
	t *testing.T,
	baseTime time.Time,
	procedure string,
	elapsed time.Duration,
	invoke func(context.Context, storagev1connect.StorageServiceClient) error,
	wantSlowAlert bool,
	wantDiskID string,
	wantRequestID string,
) {
	t.Helper()
	logBuf := &syncLogBuffer{}
	logger := slog.New(slog.NewJSONHandler(logBuf, &slog.HandlerOptions{Level: slog.LevelDebug}))

	interceptor := newStorageObservabilityInterceptorWithClock(logger, makeStepClock(baseTime, elapsed))
	mux := http.NewServeMux()
	path, h := storagev1connect.NewStorageServiceHandler(slowClockStubHandler{}, connect.WithInterceptors(interceptor))
	mux.Handle(path, h)

	httpClient := &http.Client{Transport: memoryRoundTripper{handler: mux}}
	client := storagev1connect.NewStorageServiceClient(httpClient, "http://storage.local")
	require.NoError(t, invoke(context.Background(), client))

	records := logBuf.Records(t)
	if !wantSlowAlert {
		assertNoSlowRPCWarning(t, records, procedure)
		return
	}
	rec := assertLogRecordExists(t, records, "WARN", procedure, "slow storage rpc")
	if wantDiskID != "" {
		require.Equal(t, wantDiskID, rec[logKeyDiskID], "慢操作告警必须携带 disk_id")
	}
	if wantRequestID != "" {
		require.Equal(t, wantRequestID, rec[logKeyRequestID], "慢操作告警必须携带 request_id")
	}
}

func TestObservability_StorageSampler_PromotesControlPlaneOverUnsampledParent(t *testing.T) {

	ctx := context.Background()
	installOTelCleanup(t)

	spanExporter := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithSyncer(spanExporter),
		sdktrace.WithSampler(NewStorageSampler(0.0001)),
	)
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.TraceContext{})
	t.Cleanup(func() { _ = tp.Shutdown(context.Background()) })

	mgr := control.New(db.NewMemStore(), control.WithStorageRoot(t.TempDir()))
	t.Cleanup(func() { _ = mgr.Close() })

	obsCfg := observability.DefaultConfig()
	obsCfg.OtelEnabled = true
	obsCfg.OtelTrustRemote = true

	srv, err := New(Params{
		Config: config.ServerConfig{
			ServerPort:         0,
			InternalServerPort: 0,
			ServerReadMaxBytes: 8 << 20,
		},
		Control:       mgr,
		Observability: obsCfg,
		Telemetry:     obsCfg,
	})
	require.NoError(t, err)

	httpClient := &http.Client{Transport: memoryRoundTripper{handler: srv.Handler()}}
	client := storagev1connect.NewStorageServiceClient(httpClient, "http://storage.local")

	const traceIDHex = "0af7651916cd43dd8448eb211c80319c"
	const parentSpanHex = "b7ad6b7169203331"
	req := connect.NewRequest(&storagev1.ForkDiskRequest{
		SourceDiskId: "018f0000-0000-7000-8000-000000009998",
		Name:         "unsampled-parent-fork",
	})
	req.Header().Set("Traceparent", "00-"+traceIDHex+"-"+parentSpanHex+"-00")

	_, forkErr := client.ForkDisk(ctx, req)

	require.Equal(t, connect.CodeNotFound, connect.CodeOf(forkErr))
	wantTraceID, err := trace.TraceIDFromHex(traceIDHex)
	require.NoError(t, err)
	assertPromotedForkSpan(t, spanExporter.GetSpans(), wantTraceID)
}

func TestObservability_TracerProviderFactory_HandlesExporters(t *testing.T) {
	ctx := context.Background()

	tpNil, err := NewStorageTracerProvider(ctx, nil)
	require.NoError(t, err)
	require.NotNil(t, tpNil)
	require.NoError(t, tpNil.Shutdown(ctx))

	cfgNone := telemetry.EnvConfig{
		OtelEnabled:        true,
		OtelServiceName:    "bowl-storage-test",
		OtelTracesExporter: telemetry.ExporterNone,
		OtelSampleRatio:    0.5,
	}
	tpNone, err := NewStorageTracerProvider(ctx, cfgNone)
	require.NoError(t, err)
	require.NotNil(t, tpNone)
	require.NoError(t, tpNone.Shutdown(ctx))

	cfgOTLP := telemetry.EnvConfig{
		OtelEnabled:        true,
		OtelServiceName:    "bowl-storage-test",
		OtelTracesExporter: telemetry.ExporterOTLP,
		OtelEndpoint:       "http://127.0.0.1:4317",
		OtelInsecure:       true,
		OtelSampleRatio:    1.0,
	}
	tpOTLP, err := NewStorageTracerProvider(ctx, cfgOTLP)
	require.NoError(t, err)
	require.NotNil(t, tpOTLP)
	require.NoError(t, tpOTLP.Shutdown(ctx))

	cfgBad := telemetry.EnvConfig{
		OtelEnabled:        true,
		OtelServiceName:    "bowl-storage-test",
		OtelTracesExporter: "unsupported-exporter",
	}
	_, err = NewStorageTracerProvider(ctx, cfgBad)
	require.ErrorContains(t, err, "unsupported traces exporter")
}

func TestObservability_FxModule_ExportsMetricsAndStopsCleanly(t *testing.T) {

	ctx := context.Background()
	installOTelCleanup(t)

	cfg := &config.Config{
		ServerConfig: serverx.ServerConfig{
			ServerHost:         "127.0.0.1",
			ServerPort:         0,
			InternalServerPort: 0,
			ServerReadMaxBytes: 8 << 20,
		},
		EnvConfig: observability.DefaultConfig(),
		Meta: config.MetaConfig{
			StorageDataDirVal: t.TempDir(),
		},
	}
	cfg.LogLevel = "error"
	cfg.AdminAddr = "off"
	cfg.OtelEnabled = true
	cfg.OtelTracesExporter = telemetry.ExporterNone

	var (
		srv *Server
		rt  *observability.Runtime
	)
	app := fx.New(
		fx.Supply(cfg),
		config.Module,
		obsfx.Module,
		fx.Provide(
			obsfx.AsOption(func() observability.Option { return observability.WithLogWriter(io.Discard) }),
			obsfx.AsOption(func() observability.Option { return observability.WithSetDefaultLogger(false) }),
			func() db.Store { return db.NewMemStore() },
		),
		control.Module,
		Module,
		fx.Populate(&srv, &rt),
		fx.NopLogger,
	)

	require.NoError(t, app.Start(ctx))
	require.Nil(t, rt.MeterProvider(), "obsfx Runtime 默认 MeterProvider 必须被抑制以避免二次 Shutdown")
	require.Nil(t, rt.TracerProvider(), "obsfx Runtime 默认 TracerProvider 必须被抑制以避免二次 Shutdown")

	httpClient := &http.Client{Transport: memoryRoundTripper{handler: srv.Handler()}}
	client := storagev1connect.NewStorageServiceClient(httpClient, "http://storage.local")
	_, err := client.ListDisks(ctx, connect.NewRequest(&storagev1.ListDisksRequest{}))
	require.NoError(t, err)

	adminMux := http.NewServeMux()
	observability.RegisterAdminRoutes(adminMux, rt.Gatherer(), cfg)
	rec := httptest.NewRecorder()
	adminMux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", http.NoBody))
	require.Equal(t, http.StatusOK, rec.Code)

	body := rec.Body.String()
	require.Contains(t, body, "go_goroutines")
	require.Contains(t, body, "bowl_storage_http_request_duration_seconds_bucket")
	require.Contains(t, body, "rpc_server_call_duration_seconds_bucket")

	require.NoError(t, app.Stop(ctx), "Fx 应用优雅停机必须零错误退出")
}

func TestObservability_ClassifyStorageLogLevel_MapsProceduresAndCodes(t *testing.T) {
	cases := []struct {
		name      string
		procedure string
		code      connect.Code
		err       error
		want      slog.Level
	}{
		{name: "Write成功降级Debug", procedure: storagev1connect.StorageServiceWriteProcedure, want: slog.LevelDebug},
		{name: "Read成功降级Debug", procedure: storagev1connect.StorageServiceReadProcedure, want: slog.LevelDebug},
		{name: "ListDisks只读控制面降级Debug", procedure: storagev1connect.StorageServiceListDisksProcedure, want: slog.LevelDebug},
		{name: "GetDisk只读控制面降级Debug", procedure: storagev1connect.StorageServiceGetDiskProcedure, want: slog.LevelDebug},
		{name: "ForkDisk变更控制面保持Info", procedure: storagev1connect.StorageServiceForkDiskProcedure, want: slog.LevelInfo},
		{name: "CreateDisk变更控制面保持Info", procedure: storagev1connect.StorageServiceCreateDiskProcedure, want: slog.LevelInfo},
		{name: "UpdateDisk变更控制面保持Info", procedure: storagev1connect.StorageServiceUpdateDiskProcedure, want: slog.LevelInfo},
		{name: "DeleteDisk变更控制面保持Info", procedure: storagev1connect.StorageServiceDeleteDiskProcedure, want: slog.LevelInfo},
		{name: "UnmountDisk变更控制面保持Info", procedure: storagev1connect.StorageServiceUnmountDiskProcedure, want: slog.LevelInfo},
		{name: "ListMounts只读控制面降级Debug", procedure: storagev1connect.StorageServiceListMountsProcedure, want: slog.LevelDebug},
		{name: "EvictMounts变更控制面保持Info", procedure: storagev1connect.StorageServiceEvictMountsProcedure, want: slog.LevelInfo},
		{name: "RenewMount高频续期成功降级Debug", procedure: storagev1connect.StorageServiceRenewMountProcedure, want: slog.LevelDebug},
		{name: "MergeDisk变更控制面保持Info", procedure: storagev1connect.StorageServiceMergeDiskProcedure, want: slog.LevelInfo},
		{name: "DiffDisk只读控制面降级Debug", procedure: storagev1connect.StorageServiceDiffDiskProcedure, want: slog.LevelDebug},
		{name: "ReadCommitNode只读控制面降级Debug", procedure: storagev1connect.StorageServiceReadCommitNodeProcedure, want: slog.LevelDebug},
		{name: "Lookup未命中降级Debug", procedure: storagev1connect.StorageServiceLookupProcedure, code: connect.CodeNotFound, err: connect.NewError(connect.CodeNotFound, nil), want: slog.LevelDebug},
		{name: "Read客户端取消降级Debug", procedure: storagev1connect.StorageServiceReadProcedure, code: connect.CodeCanceled, err: connect.NewError(connect.CodeCanceled, context.Canceled), want: slog.LevelDebug},
		{name: "ForkDisk未命中保持Warn", procedure: storagev1connect.StorageServiceForkDiskProcedure, code: connect.CodeNotFound, err: connect.NewError(connect.CodeNotFound, nil), want: slog.LevelWarn},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, classifyStorageLogLevel(tc.procedure, tc.code, tc.err))
		})
	}
}

func TestObservability_MergeDiffProcedures_RegisteredInMetricsControlAndSampler(t *testing.T) {
	require.Equal(t, storagev1connect.StorageServiceDiffDiskProcedure, normalizeProcedure(storagev1connect.StorageServiceDiffDiskProcedure))
	require.Equal(t, storagev1connect.StorageServiceReadCommitNodeProcedure, normalizeProcedure(storagev1connect.StorageServiceReadCommitNodeProcedure))
	require.Equal(t, storagev1connect.StorageServiceMergeDiskProcedure, normalizeProcedure(storagev1connect.StorageServiceMergeDiskProcedure))
	require.Equal(t, storagev1connect.StorageServiceListMountsProcedure, normalizeProcedure(storagev1connect.StorageServiceListMountsProcedure))
	require.Equal(t, storagev1connect.StorageServiceEvictMountsProcedure, normalizeProcedure(storagev1connect.StorageServiceEvictMountsProcedure))

	require.True(t, isControlPlaneProcedure(storagev1connect.StorageServiceDiffDiskProcedure))
	require.True(t, isControlPlaneProcedure(storagev1connect.StorageServiceReadCommitNodeProcedure))
	require.True(t, isControlPlaneProcedure(storagev1connect.StorageServiceMergeDiskProcedure))
	require.True(t, isControlPlaneProcedure(storagev1connect.StorageServiceRenewMountProcedure))
	require.True(t, isControlPlaneProcedure(storagev1connect.StorageServiceUnmountDiskProcedure))
	require.True(t, isControlPlaneProcedure(storagev1connect.StorageServiceListMountsProcedure))
	require.True(t, isControlPlaneProcedure(storagev1connect.StorageServiceEvictMountsProcedure))

	require.True(t, isSampledControlSpanName(storagev1connect.StorageServiceMergeDiskProcedure))
	require.True(t, isSampledControlSpanName(storagev1connect.StorageServiceUnmountDiskProcedure))
	require.True(t, isSampledControlSpanName(storagev1connect.StorageServiceEvictMountsProcedure))
	require.False(t, isSampledControlSpanName(storagev1connect.StorageServiceListMountsProcedure))
	require.False(t, isSampledControlSpanName(storagev1connect.StorageServiceRenewMountProcedure))
	require.False(t, isSampledControlSpanName(storagev1connect.StorageServiceDiffDiskProcedure))
	require.False(t, isSampledControlSpanName(storagev1connect.StorageServiceReadCommitNodeProcedure))
}

func TestObservability_DiffAndMerge_ExtractsTargetDiskIDIntoLogsAndSpans(t *testing.T) {

	installOTelCleanup(t)
	exporter := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter))
	otel.SetTracerProvider(tp)

	var logBuf bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&logBuf, &slog.HandlerOptions{Level: slog.LevelDebug}))

	mgr := control.New(db.NewMemStore(), control.WithStorageRoot(t.TempDir()), control.WithLogger(logger))
	t.Cleanup(func() { _ = mgr.Close() })

	srv, err := New(Params{
		Config: config.ServerConfig{
			ServerPort:         0,
			InternalServerPort: 0,
			ServerReadMaxBytes: 8 << 20,
		},
		Control: mgr,
		Logger:  logger,
	})
	require.NoError(t, err)

	ctx := context.Background()
	httpClient := &http.Client{Transport: memoryRoundTripper{handler: srv.Handler()}}
	client := storagev1connect.NewStorageServiceClient(httpClient, "http://storage.local")

	mainResp, err := client.CreateDisk(ctx, connect.NewRequest(&storagev1.CreateDiskRequest{Name: "main", MaxBytes: 16 << 20}))
	require.NoError(t, err)
	mainID := mainResp.Msg.GetDisk().GetDiskId()
	forkResp, err := client.ForkDisk(ctx, connect.NewRequest(&storagev1.ForkDiskRequest{SourceDiskId: mainID, Name: "feat"}))
	require.NoError(t, err)
	featID := forkResp.Msg.GetDisk().GetDiskId()

	exporter.Reset()
	logBuf.Reset()

	_, err = client.DiffDisk(ctx, connect.NewRequest(&storagev1.DiffDiskRequest{
		SourceDiskId: featID,
		TargetDiskId: &mainID,
	}))
	require.NoError(t, err)

	_, err = client.MergeDisk(ctx, connect.NewRequest(&storagev1.MergeDiskRequest{
		SourceDiskId: featID,
		TargetDiskId: mainID,
		DryRun:       true,
	}))
	require.NoError(t, err)
	require.NoError(t, tp.ForceFlush(ctx))

	spans := exporter.GetSpans()
	var sawDiffTarget, sawDryMergeTarget bool
	for i := range spans {
		sp := &spans[i]
		var dID, srcID string
		for _, attr := range sp.Attributes {
			switch string(attr.Key) {
			case attrKeyBowlDiskID:
				dID = attr.Value.AsString()
			case attrKeyBowlSourceDiskID:
				srcID = attr.Value.AsString()
			}
		}
		if sp.Name == "storage.v1.StorageService/DiffDisk" && dID == mainID && srcID == featID {
			sawDiffTarget = true
		}
		if sp.Name == "storage.v1.StorageService/MergeDisk" && dID == mainID && srcID == featID {
			sawDryMergeTarget = true
		}
	}
	require.True(t, sawDiffTarget, "三路 DiffDisk Span 必须提取目标盘 bowl.disk_id")
	require.True(t, sawDryMergeTarget, "DryRun MergeDisk Span 必须提取目标盘 bowl.disk_id")

	logText := logBuf.String()
	require.Contains(t, logText, `"disk_id":"`+mainID+`"`)
	require.Contains(t, logText, `"source_disk_id":"`+featID+`"`)
}
