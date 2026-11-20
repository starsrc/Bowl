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

	"github.com/hanwen/go-fuse/v2/fuse"
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
	return obs, newAdminMux(obs, rawFS, nil)
}

func TestStats_GivenHelpAndInvalidFlags_ReturnsExpectedExitCodes(t *testing.T) {

	requireHelpExit0(t, "bowl stats [mountpoint]", "stats", "-h")
	requireHelpExit0(t, "bowl stats [mountpoint]", "stats", "--help")

	requireCLIError2(t, "accepts at most 1 arg(s), received 2", "stats", "/mnt/a", "/mnt/b")
	requireCLIError2(t, "--json and --raw cannot be used together", "stats", "--json", "--raw")
	requireCLIError2(t, `unknown flag: --bogus`, "stats", "--bogus")
	requireCLIError2(t, "flag needs an argument: --admin-addr", "stats", "--admin-addr")
	requireCLIError2(t, "invalid --admin-addr", "stats", "--admin-addr", "off")
	requireCLIError2(t, "invalid --admin-addr", "stats", "--admin-addr", "invalid-host")
	requireCLIError2(t, "invalid --admin-addr", "stats", "--admin-addr", "")
	requireCLIError2(t, "Error:", "stats", "--json=bad")
	requireCLIError2(t, "Error:", "stats", "--raw=bad")
}

func TestStats_GivenNoOrAmbiguousMounts_ReturnsExit1WithActionableMessage(t *testing.T) {

	metaDir := t.TempDir()
	withTempMountMetaDirs(t, metaDir)

	code, _, errOut := runCLI("stats")
	require.Equal(t, 1, code)
	require.Contains(t, errOut, "no active bowl mounts found")

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

	code, _, errOut = runCLI("stats")
	require.Equal(t, 1, code)
	require.Contains(t, errOut, "multiple active mounts found")
	require.Contains(t, errOut, "/mnt/a")
	require.Contains(t, errOut, "/mnt/b")
	require.NotContains(t, errOut, "/mnt/dead")

	code, _, errOut = runCLI("stats", "/mnt/missing")
	require.Equal(t, 1, code)
	require.Contains(t, errOut, "no active bowl mount process found for mountpoint")
}

func TestStats_GivenSingleActiveMountAndSymlink_DiscoversAdminAndRendersFormats(t *testing.T) {

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

	code, textOut, errOut := runCLI("stats")
	require.Equal(t, 0, code)
	require.Empty(t, errOut)
	require.Equal(t, "http://127.0.0.1:19100/debug/stats?format=text", *lastURL)
	require.NotEmpty(t, textOut)

	code, falseOut, errOut := runCLI("stats", "--json=false", "--raw=false")
	require.Equal(t, 0, code)
	require.Empty(t, errOut)
	require.Equal(t, "http://127.0.0.1:19100/debug/stats?format=text", *lastURL)
	require.Contains(t, falseOut, "Uptime:")

	code, jsonOut, errOut := runCLI("stats", symMP, "--json")
	require.Equal(t, 0, code)
	require.Empty(t, errOut)
	require.Equal(t, "http://127.0.0.1:19100/debug/stats?format=json", *lastURL)
	var snap fusefs.Snapshot
	require.NoError(t, json.Unmarshal([]byte(jsonOut), &snap))
	require.Len(t, snap.FUSE.Ops, fusefs.OpCount)

	code, rawOut, errOut := runCLI("stats", "--admin-addr", "http://127.0.0.1:19200/", "--raw")
	require.Equal(t, 0, code)
	require.Empty(t, errOut)
	require.Equal(t, "http://127.0.0.1:19200/debug/stats?format=stats", *lastURL)
	require.NotEmpty(t, rawOut)
}

func TestStats_GivenDisabledAdminOrHTTPError_ReturnsExit1(t *testing.T) {

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
	require.Contains(t, errOut, "admin endpoint is disabled")

	withStatsErrorTransport(t, errors.New("connection refused"))
	code, _, errOut = runCLI("stats", "--admin-addr", "127.0.0.1:19300")
	require.Equal(t, 1, code)
	require.Contains(t, errOut, "failed to query admin endpoint 127.0.0.1:19300")

	withStatsMuxTransport(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "internal error", http.StatusInternalServerError)
	}))
	code, _, errOut = runCLI("stats", "--admin-addr", "127.0.0.1:19300")
	require.Equal(t, 1, code)
	require.Contains(t, errOut, "admin endpoint returned 500")
}

func TestAdminObservability_GivenBareOTLPEndpoint_NormalizesSchemeWithoutExternalDial(t *testing.T) {

	require.Empty(t, normalizeOTLPEndpoint("   "))
	require.Equal(t, "http://172.16.1.205:4317", normalizeOTLPEndpoint("172.16.1.205:4317"))
	require.Equal(t, "http://172.16.1.205:4317", normalizeOTLPEndpoint("  172.16.1.205:4317  "))
	require.Equal(t, "http://127.0.0.1:4317", normalizeOTLPEndpoint("http://127.0.0.1:4317"))
	require.Equal(t, "https://collector.example.com:4317", normalizeOTLPEndpoint("https://collector.example.com:4317"))

	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "172.16.1.205:4317")
	t.Setenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", "")

	cfg := buildObsConfig(&mountOptions{
		logLevel:         "info",
		logFormat:        "json",
		traceSampleRatio: 0.001,
	})

	require.Equal(t, "http://172.16.1.205:4317", os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT"))
	require.Equal(t, "http://172.16.1.205:4317", cfg.Endpoint())
	require.True(t, cfg.Insecure())
}

func TestAdminObservability_GivenPriorityOpSampler_BoostsBarriersAndLinkedSpans(t *testing.T) {

	exporter := tracetest.NewInMemoryExporter()
	tp := newTestSamplerProvider(exporter, 1e-12)
	defer func() { _ = tp.Shutdown(context.Background()) }()
	tracer := tp.Tracer("bowl/client/test")

	_, spFsync := tracer.Start(context.Background(), "fuse.Fsync")
	require.True(t, spFsync.SpanContext().IsSampled())
	spFsync.End()

	_, spFsyncDir := tracer.Start(context.Background(), "fuse.FsyncDir")
	require.True(t, spFsyncDir.SpanContext().IsSampled())
	spFsyncDir.End()

	_, spUnmount := tracer.Start(context.Background(), "fuse.OnUnmount")
	require.True(t, spUnmount.SpanContext().IsSampled())
	spUnmount.End()

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

	zeroTP := newTestSamplerProvider(exporter, 0)
	defer func() { _ = zeroTP.Shutdown(context.Background()) }()
	_, zeroSpan := zeroTP.Tracer("bowl/client/test").Start(context.Background(), "fuse.Fsync")
	require.False(t, zeroSpan.SpanContext().IsSampled())
	zeroSpan.End()
}

func TestAdminMux_GivenSubMsMetricsAndInvalidStatsFormat_ServesExpectedResponsesWithoutNetwork(t *testing.T) {

	obs, mux := setupTestObsAndMux(t)
	hist, err := obs.meterProvider.Meter("connectrpc").Float64Histogram("rpc.client.duration")
	require.NoError(t, err)
	hist.Record(context.Background(), 0.00003)

	metricsReq := httptest.NewRequest(http.MethodGet, "/metrics", http.NoBody)
	metricsRec := httptest.NewRecorder()
	mux.ServeHTTP(metricsRec, metricsReq)

	require.Equal(t, http.StatusOK, metricsRec.Code)
	metricsText := metricsRec.Body.String()
	require.Contains(t, metricsText, `le="2.5e-05"} 0`)
	require.Contains(t, metricsText, `le="5e-05"} 1`)
	require.Contains(t, metricsText, "bowl_client_stage_writes_total")
	require.Contains(t, metricsText, "bowl_client_fuse_connection_waiting")

	badReq := httptest.NewRequest(http.MethodGet, "/debug/stats?format=xml", http.NoBody)
	badRec := httptest.NewRecorder()
	mux.ServeHTTP(badRec, badReq)
	require.Equal(t, http.StatusBadRequest, badRec.Code)

	disabled, startErr := startAdminServer("off", obs, nil, nil)
	require.NoError(t, startErr)
	require.Nil(t, disabled)
	require.Equal(t, "off", disabled.addr())
	disabled.shutdown(context.Background())
}

func TestAdminMux_GivenDebugUnmountEndpoint_ReturnsJSONProgressWithoutAlteringStats(t *testing.T) {

	obs, mux := setupTestObsAndMux(t)

	unmountReq := httptest.NewRequest(http.MethodGet, "/debug/unmount", http.NoBody)
	unmountRec := httptest.NewRecorder()
	mux.ServeHTTP(unmountRec, unmountReq)

	require.Equal(t, http.StatusOK, unmountRec.Code)
	require.Equal(t, "application/json; charset=utf-8", unmountRec.Header().Get("Content-Type"))
	var prog fusefs.UnmountProgress
	require.NoError(t, json.Unmarshal(unmountRec.Body.Bytes(), &prog))
	require.Equal(t, fusefs.UnmountProgress{Active: false, Completed: 0, Total: 0}, prog)

	statsReq := httptest.NewRequest(http.MethodGet, "/debug/stats?format=json", http.NoBody)
	statsRec := httptest.NewRecorder()
	mux.ServeHTTP(statsRec, statsReq)
	require.Equal(t, http.StatusOK, statsRec.Code)
	var rawStats map[string]any
	require.NoError(t, json.Unmarshal(statsRec.Body.Bytes(), &rawStats))
	require.NotContains(t, rawStats, "active")
	require.NotContains(t, rawStats, "completed")
	require.NotContains(t, rawStats, "total")
	require.NotContains(t, rawStats, "unmount")

	nilMux := newAdminMux(obs, nil, nil)
	nilReq := httptest.NewRequest(http.MethodGet, "/debug/unmount", http.NoBody)
	nilRec := httptest.NewRecorder()
	nilMux.ServeHTTP(nilRec, nilReq)
	require.Equal(t, http.StatusOK, nilRec.Code)
	require.Equal(t, "application/json; charset=utf-8", nilRec.Header().Get("Content-Type"))
	require.NoError(t, json.Unmarshal(nilRec.Body.Bytes(), &prog))
	require.Equal(t, fusefs.UnmountProgress{}, prog)
}

func TestMountMeta_GivenSymlinkMountpoint_CanonicalizesWriteAndListsCleanly(t *testing.T) {

	realMP := filepath.Join(t.TempDir(), "real-mp")
	require.NoError(t, os.MkdirAll(realMP, 0o755))
	canonMP := canonicalMountpoint(realMP)
	symMP := filepath.Join(t.TempDir(), "sym-mp")
	require.NoError(t, os.Symlink(realMP, symMP))

	metaDir := t.TempDir()
	withTempMountMetaDirs(t, metaDir)

	writtenPath, err := writeMountMeta(&mountMeta{
		Mountpoint: symMP,
		PID:        os.Getpid(),
		DiskID:     testDiskID1,
		AdminAddr:  "127.0.0.1:19401",
		StartedAt:  time.Now().UTC(),
	})
	require.NoError(t, err)

	require.Equal(t, filepath.Join(metaDir, mountMetaFileName(canonMP)), writtenPath)
	data, err := os.ReadFile(writtenPath)
	require.NoError(t, err)
	var stored mountMeta
	require.NoError(t, json.Unmarshal(data, &stored))
	require.Equal(t, canonMP, stored.Mountpoint)

	metas := listMountMetas()
	require.Len(t, metas, 1)
	require.Equal(t, canonMP, metas[0].Mountpoint)
	require.Equal(t, "127.0.0.1:19401", metas[0].AdminAddr)
}

func TestStatsAndMountMeta_GivenWedgedAndHealthyMounts_AvoidsLstatOnActiveMountRoots(t *testing.T) {

	const (
		realWedgedMP = "/wedged-root/mnt"
		realOkMP     = "/ok-root/mnt"
		symWedgedMP  = "/tmp/sym-wedged"
		symOkMP      = "/tmp/sym-ok"
	)
	_, mux := setupTestObsAndMux(t)
	lastURL := withStatsMuxTransport(t, mux)
	metaDir := t.TempDir()
	withTempMountMetaDirs(t, metaDir)
	withMountInfoContent(t,
		"30 61 8:1 / / rw - ext4 /dev/sda1 rw\n"+
			"100 30 0:50 / "+realWedgedMP+" rw - fuse.bowl bowl:"+testDiskID1+" rw\n"+
			"101 30 0:51 / "+realOkMP+" rw - fuse.bowl bowl:"+testDiskID2+" rw\n",
	)
	lstatCalls := withMountInfoNoRootLstatStub(t, []string{realWedgedMP, realOkMP}, map[string]fakeFSNode{
		"/tmp":      {mode: os.ModeDir | 0o755},
		symWedgedMP: {mode: os.ModeSymlink | 0o777, target: realWedgedMP},
		symOkMP:     {mode: os.ModeSymlink | 0o777, target: realOkMP},
	})

	wedgedMetaPath, err := writeMountMeta(&mountMeta{
		Mountpoint: symWedgedMP,
		PID:        os.Getpid(),
		DiskID:     testDiskID1,
		AdminAddr:  "off",
		StartedAt:  time.Now().UTC(),
	})
	require.NoError(t, err)
	okMetaPath, err := writeMountMeta(&mountMeta{
		Mountpoint: symOkMP,
		PID:        os.Getpid(),
		DiskID:     testDiskID2,
		AdminAddr:  "127.0.0.1:19401",
		StartedAt:  time.Now().UTC(),
	})
	require.NoError(t, err)

	require.Equal(t, filepath.Join(metaDir, mountMetaFileName(realWedgedMP)), wedgedMetaPath)
	require.Equal(t, filepath.Join(metaDir, mountMetaFileName(realOkMP)), okMetaPath)

	realTarget := t.TempDir()
	hostSym := filepath.Join(t.TempDir(), "host-sym")
	require.NoError(t, os.Symlink(realTarget, hostSym))
	extraPath := filepath.Join(metaDir, mountMetaFileName(hostSym))
	extraBytes, err := json.Marshal(mountMeta{
		Mountpoint: hostSym + "/",
		PID:        os.Getpid(),
		DiskID:     testDiskID1,
		AdminAddr:  "off",
		StartedAt:  time.Now().UTC(),
	})
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(extraPath, extraBytes, 0o644))

	metas := listMountMetas()
	require.Len(t, metas, 3)
	gotMountpoints := []string{metas[0].Mountpoint, metas[1].Mountpoint, metas[2].Mountpoint}
	require.Contains(t, gotMountpoints, hostSym)
	require.NoError(t, os.Remove(extraPath))

	code, _, errOut := runCLI("stats")
	require.Equal(t, 1, code)
	require.Contains(t, errOut, "multiple active mounts found")
	require.Contains(t, errOut, realOkMP)
	require.Contains(t, errOut, realWedgedMP)

	code, jsonOut, errOut := runCLI("stats", symOkMP, "--json")
	require.Equal(t, 0, code, "stderr=%s", errOut)
	require.Empty(t, errOut)
	require.Equal(t, "http://127.0.0.1:19401/debug/stats?format=json", *lastURL)
	var snap fusefs.Snapshot
	require.NoError(t, json.Unmarshal([]byte(jsonOut), &snap))

	code, _, errOut = runCLI("stats", symWedgedMP)
	require.Equal(t, 1, code)
	require.Contains(t, errOut, "admin endpoint is disabled")
	require.Contains(t, errOut, realWedgedMP)

	require.NoError(t, os.Remove(okMetaPath))
	code, _, errOut = runCLI("stats")
	require.Equal(t, 1, code)
	require.Contains(t, errOut, "admin endpoint is disabled")
	require.Contains(t, errOut, realWedgedMP)

	require.NotContains(t, *lstatCalls, realWedgedMP)
	require.NotContains(t, *lstatCalls, realOkMP)
}

func TestAdminMux_Readyz_GivenHealthyExpiredHealedFencedAndLeaseFree_ReportsExpectedStatus(t *testing.T) {

	obs, _ := setupTestObsAndMux(t)
	rawFS := fusefs.NewRawFS(nil, fusefs.Config{
		DiskID:  testDiskID1,
		RootIno: 1,
	})
	t.Cleanup(rawFS.OnUnmount)

	now := time.Date(2026, 11, 19, 12, 0, 0, 0, time.UTC)
	tracker := newMountLeaseTracker(mountLeaseInfo{
		mountID:         "01950000-0000-7000-9000-000000000010",
		leaseTTLSeconds: 30,
	}, func() time.Time { return now })
	mux := newAdminMux(obs, rawFS, tracker)

	queryReadyz := func(m *http.ServeMux) (int, string) {
		req := httptest.NewRequest(http.MethodGet, "/readyz", http.NoBody)
		rec := httptest.NewRecorder()
		m.ServeHTTP(rec, req)
		return rec.Code, rec.Body.String()
	}

	code, body := queryReadyz(mux)
	require.Equal(t, http.StatusOK, code, "body=%s", body)

	now = now.Add(30 * time.Second)
	code, body = queryReadyz(mux)
	require.Equal(t, http.StatusServiceUnavailable, code, "body=%s", body)
	require.Contains(t, body, "mount_lease")

	tracker.refresh(30)
	code, body = queryReadyz(mux)
	require.Equal(t, http.StatusOK, code, "body=%s", body)

	rawFS.Fence(fuse.EBUSY, "writer epoch superseded")
	code, body = queryReadyz(mux)
	require.Equal(t, http.StatusServiceUnavailable, code, "body=%s", body)
	require.Contains(t, body, "writer epoch superseded")

	leaseFreeFS := fusefs.NewRawFS(nil, fusefs.Config{DiskID: testDiskID1, RootIno: 1})
	t.Cleanup(leaseFreeFS.OnUnmount)
	leaseFreeTracker := newMountLeaseTracker(mountLeaseInfo{}, func() time.Time { return now })
	leaseFreeMux := newAdminMux(obs, leaseFreeFS, leaseFreeTracker)
	code, body = queryReadyz(leaseFreeMux)
	require.Equal(t, http.StatusOK, code, "body=%s", body)
}
