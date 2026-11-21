package store_test

import (
	"bytes"
	"context"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"

	"github.com/starsrc/bowl/services/storage/internal/store"
)

type lockedLogBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *lockedLogBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *lockedLogBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

func setupTestTracer(t *testing.T, sampler sdktrace.Sampler) (*tracetest.InMemoryExporter, *sdktrace.TracerProvider) {
	t.Helper()
	prev := otel.GetTracerProvider()
	exp := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithSyncer(exp),
		sdktrace.WithSampler(sampler),
	)
	otel.SetTracerProvider(tp)
	t.Cleanup(func() {
		_ = tp.Shutdown(context.Background())
		otel.SetTracerProvider(prev)
	})
	return exp, tp
}

func spanNamesSet(spans tracetest.SpanStubs) map[string]*tracetest.SpanStub {
	out := make(map[string]*tracetest.SpanStub, len(spans))
	for i := range spans {
		s := &spans[i]
		out[s.Name] = s
	}
	return out
}

func requireSpansPresent(t *testing.T, spans tracetest.SpanStubs, want []string) map[string]*tracetest.SpanStub {
	t.Helper()
	byName := spanNamesSet(spans)
	for _, name := range want {
		_, ok := byName[name]
		require.Truef(t, ok, "缺少预期 OTel Span %q: got=%v", name, spanNameList(spans))
	}
	return byName
}

func spanNameList(spans tracetest.SpanStubs) []string {
	names := make([]string, len(spans))
	for i := range spans {
		names[i] = spans[i].Name
	}
	return names
}

func findHistogramExemplarTraceID(t *testing.T, reg *prometheus.Registry, name string, labels map[string]string) string {
	t.Helper()
	m := findMetricByLabels(gatherMetricFamilies(t, reg), name, labels)
	require.NotNilf(t, m, "未找到目标 Histogram 指标: name=%s labels=%v", name, labels)
	h := m.GetHistogram()
	require.NotNil(t, h)
	for _, b := range h.GetBucket() {
		ex := b.GetExemplar()
		if ex == nil {
			continue
		}
		for _, lp := range ex.GetLabel() {
			if lp.GetName() == "trace_id" && lp.GetValue() != "" {
				return lp.GetValue()
			}
		}
	}
	return ""
}

func requireAllReadOpHistogramsRecorded(t *testing.T, reg *prometheus.Registry, ops []string) {
	t.Helper()
	for _, op := range ops {
		cnt := histogramSampleCount(t, reg, "bowl_storage_read_op_duration_seconds", map[string]string{"op": op})
		require.GreaterOrEqualf(t, cnt, uint64(1), "读操作 %q 未记录时长直方图", op)
	}
}

func requireMetricLabelAbsent(t *testing.T, reg *prometheus.Registry, name, labelKey, forbiddenVal string) {
	t.Helper()
	m := findMetricByLabels(gatherMetricFamilies(t, reg), name, map[string]string{labelKey: forbiddenVal})
	require.Nilf(t, m, "已废弃指标标签 %s=%q 不得再出现", labelKey, forbiddenVal)
}

func readFileMustContainAll(t *testing.T, path string, tokens []string) string {
	t.Helper()
	raw, err := os.ReadFile(path)
	require.NoErrorf(t, err, "读取日志文件失败: path=%s", path)
	text := string(raw)
	for _, tok := range tokens {
		require.Containsf(t, text, tok, "日志 %s 缺少预期事件字段 %q", path, tok)
	}
	return text
}

func mustStatFSNoAllocErr(t *testing.T, ctx context.Context, actor *store.StoreActor) {
	if _, err := actor.StatFS(ctx); err != nil {
		t.Fatalf("StatFS 失败: %v", err)
	}
}

func makeTraceCapturingSegmentAlloc(step uint64, seenTraceID *string) func(context.Context, uint64) (uint64, uint64, error) {
	return func(actx context.Context, minNext uint64) (first, last uint64, err error) {
		if sc := trace.SpanFromContext(actx).SpanContext(); sc.IsValid() {
			*seenTraceID = sc.TraceID().String()
		}
		return minNext, minNext + step - 1, nil
	}
}

func TestGivenUnsampledContextWhenCallingStatFSThenZeroAllocsAndNoSpans(t *testing.T) {
	ctx := context.Background()
	exp, _ := setupTestTracer(t, sdktrace.NeverSample())
	actor := openTestActor(t, &store.Options{DiskID: "obs-unsampled"})

	_, err := actor.StatFS(ctx)
	require.NoError(t, err)
	exp.Reset()

	allocs := testing.AllocsPerRun(50, func() {
		mustStatFSNoAllocErr(t, ctx, actor)
	})

	require.Emptyf(t, exp.GetSpans(), "未采样 ctx 下不得产生 OTel Span")
	require.LessOrEqualf(t, allocs, 1.0, "未采样 StatFS 不得出现额外堆分配: got=%v", allocs)
}

func TestGivenSampledContextWhenRunningStoreAndForkOpsThenSpansLinksAndExemplarEmitted(t *testing.T) {
	ctx := context.Background()
	exp, tp := setupTestTracer(t, sdktrace.AlwaysSample())

	var seenInoAllocTraceID string
	var seenPackAllocTraceID string
	srcDir := t.TempDir()
	cpDir := filepath.Join(t.TempDir(), "cp-obs")
	childDir := filepath.Join(t.TempDir(), "child-obs")

	actor, reg := newMeteredActor(t, &store.Options{
		Dir:           srcDir,
		DiskID:        "obs-sampled",
		MaxBytes:      32 << 20,
		MaxPackSize:   store.BlobSize,
		InoAllocator:  makeTraceCapturingSegmentAlloc(16, &seenInoAllocTraceID),
		PackAllocator: makeTraceCapturingSegmentAlloc(store.PackSegment, &seenPackAllocTraceID),
	})

	rootCtx, rootSpan := tp.Tracer("obs-test").Start(ctx, "test.root")
	wantTraceID := rootSpan.SpanContext().TraceID().String()

	f, err := actor.Create(rootCtx, store.CreateReq{ParentIno: store.RootIno, Name: []byte("obs.bin"), Mode: store.S_IFREG | 0o644})
	require.NoError(t, err)
	_, _, err = actor.Write(rootCtx, f.Ino, 0, makePatternBytes(store.BlobSize, 0x3C))
	require.NoError(t, err)
	require.NoError(t, actor.Fsync(rootCtx, f.Ino))

	require.NoError(t, actor.Close())
	coldActor := openTestActor(t, &store.Options{Dir: srcDir, DiskID: "obs-sampled", Metrics: store.NewMetrics()})
	_, err = coldActor.Read(rootCtx, f.Ino, 0, 4096)
	require.NoError(t, err)
	_, err = coldActor.ReadDir(rootCtx, store.RootIno, 0, 16)
	require.NoError(t, err)

	require.NoError(t, coldActor.Close())
	cpActor, cpReg := newMeteredActor(t, &store.Options{Dir: srcDir, DiskID: "obs-sampled", MaxPackSize: store.BlobSize})
	cpCtx, cpSpan := tp.Tracer("obs-test").Start(ctx, "test.checkpoint")
	wantCPTraceID := cpSpan.SpanContext().TraceID().String()
	_, err = cpActor.Checkpoint(cpCtx, cpDir)
	require.NoError(t, err)
	cpSpan.End()

	require.NoError(t, store.CloneCheckpointDir(rootCtx, cpDir, childDir))
	_, err = store.SanitizeCheckpoint(rootCtx, store.SanitizeOptions{Dir: childDir, SnapshotID: "obs-c1"})
	require.NoError(t, err)
	rootSpan.End()

	spans := exp.GetSpans()
	byName := requireSpansPresent(t, spans, []string{
		"store.QueueWait",
		"store.Tick",
		"store.PhaseA",
		"store.PhaseB",
		"store.SyncBarrier",
		"store.BlobWrite",
		"store.KVGet",
		"store.KVScan",
		"store.BlobPread",
		"store.Checkpoint",
		"store.CloneCheckpointDir",
		"store.SanitizeCheckpoint",
	})
	require.NotEmptyf(t, byName["store.Tick"].Links, "store.Tick 必须携带指向合批调用方的 SpanLink")
	require.Equalf(t, wantTraceID, seenInoAllocTraceID, "InoAllocator 应收到上游采样 TraceID")
	require.Equalf(t, wantTraceID, seenPackAllocTraceID, "PackAllocator 应收到上游采样 TraceID")
	require.GreaterOrEqualf(t, histogramSampleCount(t, reg, "bowl_storage_actor_tick_phase_duration_seconds", map[string]string{"phase": "phase_a_validate"}), uint64(1), "应记录 phase_a_validate 指标")

	gotExemplarTraceID := findHistogramExemplarTraceID(t, cpReg, "bowl_storage_actor_tick_phase_duration_seconds", map[string]string{"phase": "checkpoint"})
	require.Equalf(t, wantCPTraceID, gotExemplarTraceID, "checkpoint 阶段直方图 Exemplar trace_id 不符: want=%s got=%s", wantCPTraceID, gotExemplarTraceID)
}

func TestGivenAllReadAndMetaOpsWhenExecutedThenPrometheusHistogramsAndCountersAccurate(t *testing.T) {
	ctx := context.Background()
	actor, reg := newMeteredActor(t, &store.Options{DiskID: "obs-metrics"})

	f, err := actor.Create(ctx, store.CreateReq{ParentIno: store.RootIno, Name: []byte("f.bin"), Mode: store.S_IFREG | 0o644})
	require.NoError(t, err)
	_, _, err = actor.Write(ctx, f.Ino, 0, []byte("metrics-payload"))
	require.NoError(t, err)
	require.NoError(t, actor.SetXattr(ctx, f.Ino, []byte("user.k"), []byte("v"), 0))
	sym, err := actor.Symlink(ctx, store.SymlinkReq{ParentIno: store.RootIno, Name: []byte("s"), Target: []byte("/target")})
	require.NoError(t, err)

	_, err = actor.Read(ctx, f.Ino, 0, 8)
	require.NoError(t, err)
	_, _, err = actor.Lookup(ctx, store.RootIno, []byte("f.bin"))
	require.NoError(t, err)
	_, err = actor.GetAttr(ctx, f.Ino)
	require.NoError(t, err)
	_, err = actor.ReadDir(ctx, store.RootIno, 0, 16)
	require.NoError(t, err)
	_, err = actor.ReadDirPlus(ctx, store.RootIno, 0, 16)
	require.NoError(t, err)
	_, err = actor.Readlink(ctx, sym.Ino)
	require.NoError(t, err)
	_, err = actor.GetXattr(ctx, f.Ino, []byte("user.k"))
	require.NoError(t, err)
	_, err = actor.ListXattr(ctx, f.Ino)
	require.NoError(t, err)
	_, err = actor.StatFS(ctx)
	require.NoError(t, err)

	_, _, err = actor.Write(ctx, f.Ino, 0, []byte("x"), 99)
	require.ErrorIs(t, err, store.ErrStaleEpoch)

	requireAllReadOpHistogramsRecorded(t, reg, []string{
		"read", "lookup", "getattr", "readdir", "readdirplus", "readlink", "getxattr", "listxattr", "statfs",
	})
	require.GreaterOrEqualf(t, histogramSampleCount(t, reg, "bowl_storage_kv_read_duration_seconds", map[string]string{"op": "point_get"}), uint64(1), "point_get 未记录")
	require.GreaterOrEqualf(t, histogramSampleCount(t, reg, "bowl_storage_kv_read_duration_seconds", map[string]string{"op": "prefix_scan"}), uint64(1), "prefix_scan 未记录")
	require.GreaterOrEqualf(t, histogramSampleCount(t, reg, "bowl_storage_actor_tick_phase_duration_seconds", map[string]string{"phase": "phase_a_validate"}), uint64(1), "phase_a_validate 未记录")
	require.GreaterOrEqualf(t, histogramSampleCount(t, reg, "bowl_storage_actor_tick_phase_duration_seconds", map[string]string{"phase": "phase_b_mutate"}), uint64(1), "phase_b_mutate 未记录")
	requireMetricLabelAbsent(t, reg, "bowl_storage_actor_tick_phase_duration_seconds", "phase", "validate_mutate")
	require.InDeltaf(t, 1, counterValue(t, reg, "bowl_storage_epoch_fenced_total", nil), 0.001, "epoch_fenced_total 应计数 1 次")
}

func TestGivenSlowThresholdAndWarnLoggerWhenRunningThenStructuredLogsAndPebbleEventsPersisted(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	cpDir := filepath.Join(t.TempDir(), "cp-pebble-log")
	sanDir := filepath.Join(t.TempDir(), "san-pebble-log")

	var logBuf lockedLogBuffer
	logger := slog.New(slog.NewJSONHandler(&logBuf, &slog.HandlerOptions{Level: slog.LevelWarn}))

	actor := openTestActor(t, &store.Options{
		Dir:               dir,
		DiskID:            "obs-slog",
		MaxBytes:          16 << 20,
		Logger:            logger,
		SlowTickThreshold: time.Nanosecond,
	})

	f, err := actor.Create(ctx, store.CreateReq{ParentIno: store.RootIno, Name: []byte("log.bin"), Mode: store.S_IFREG | 0o644})
	require.NoError(t, err)
	_, _, err = actor.Write(ctx, f.Ino, 0, makePatternBytes(store.BlobSize, 0x7E))
	require.NoError(t, err)
	_, _, err = actor.Write(ctx, f.Ino, 0, []byte("bad"), 999)
	require.ErrorIs(t, err, store.ErrStaleEpoch)

	_, err = actor.Checkpoint(ctx, cpDir)
	require.NoError(t, err)
	require.NoError(t, store.CloneCheckpointDir(ctx, cpDir, sanDir))
	_, err = store.SanitizeCheckpoint(ctx, store.SanitizeOptions{
		Dir:        sanDir,
		SnapshotID: "snapshot-obs-log",
		Logger:     logger,
	})
	require.NoError(t, err)
	require.NoError(t, actor.Close())

	logs := logBuf.String()
	require.Contains(t, logs, "slow actor tick", "应记录 slow actor tick 告警日志")
	require.Contains(t, logs, "stale writer epoch fenced", "应记录 stale writer epoch fenced 告警日志")

	readFileMustContainAll(t, filepath.Join(dir, store.PebbleEventsLogFileName), []string{
		"open_begin",
		"wal_created",
		"close",
	})
	readFileMustContainAll(t, filepath.Join(sanDir, store.PebbleEventsLogFileName), []string{
		"snapshot:snapshot-obs-log",
		"open_begin",
		"close",
	})
}
