package rpc

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	otelprom "go.opentelemetry.io/otel/exporters/prometheus"
	"go.opentelemetry.io/otel/propagation"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"google.golang.org/genproto/googleapis/rpc/errdetails"

	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
	"github.com/starsrc/bowl/gen/go/storage/v1/storagev1connect"
)

func setupTestOTel(t *testing.T) (*prometheus.Registry, *sdkmetric.ManualReader, *tracetest.InMemoryExporter, *sdktrace.TracerProvider) {
	t.Helper()
	prevMP := otel.GetMeterProvider()
	prevTP := otel.GetTracerProvider()
	prevProp := otel.GetTextMapPropagator()

	reg := prometheus.NewRegistry()
	promExp, err := otelprom.New(otelprom.WithRegisterer(reg), otelprom.WithoutScopeInfo())
	require.NoError(t, err)
	manualReader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(
		sdkmetric.WithReader(promExp),
		sdkmetric.WithReader(manualReader),
	)
	otel.SetMeterProvider(mp)

	spanExp := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithSyncer(spanExp),
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
	)
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.TraceContext{})

	t.Cleanup(func() {
		otel.SetMeterProvider(prevMP)
		otel.SetTracerProvider(prevTP)
		otel.SetTextMapPropagator(prevProp)
		_ = mp.Shutdown(context.Background())
		_ = tp.Shutdown(context.Background())
	})
	return reg, manualReader, spanExp, tp
}

func TestObservability_GivenRetryAndOTelProviders_ExportsSubMsHistogramsAndRetryMetrics(t *testing.T) {

	reg, manualReader, spanExp, tp := setupTestOTel(t)
	clk := newAutoAdvanceClock(time.Date(2026, 11, 13, 10, 0, 0, 0, time.UTC))
	rpcMetrics := NewMetrics()
	require.NoError(t, reg.Register(rpcMetrics))

	handler := &scriptedReadHandler{
		steps: []func(context.Context, *connect.Request[storagev1.ReadRequest]) (*connect.Response[storagev1.ReadResponse], error){
			func(context.Context, *connect.Request[storagev1.ReadRequest]) (*connect.Response[storagev1.ReadResponse], error) {
				return nil, connect.NewError(connect.CodeUnavailable, errors.New("transient"))
			},
			func(context.Context, *connect.Request[storagev1.ReadRequest]) (*connect.Response[storagev1.ReadResponse], error) {
				return connect.NewResponse(&storagev1.ReadResponse{Data: []byte("sub-ms-check")}), nil
			},
		},
	}

	client, err := newClientWithClock(ClientConfig{
		Endpoint:   "http://127.0.0.1:9000",
		HTTPClient: newInMemoryHTTPClient(handler),
		Metrics:    rpcMetrics,
		RetryConfig: &RetryConfig{
			InitialInterval: 5 * time.Millisecond,
			MaxInterval:     10 * time.Millisecond,
			MaxElapsedTime:  time.Second,
			RandFloat64:     func() float64 { return 1.0 },
		},
	}, clk)
	require.NoError(t, err)

	ctx, rootSpan := tp.Tracer("test-caller").Start(context.Background(), "caller.read")
	ctx = WithRequestID(ctx, 4242)

	resp, err := client.Read(ctx, connect.NewRequest(&storagev1.ReadRequest{
		Ino:    1,
		Offset: 0,
		Size:   12,
		DiskId: "11111111-1111-4111-8111-111111111111",
	}))
	rootSpan.End()

	require.NoError(t, err)
	payload := TakeReadPayload(resp.Msg)
	require.Equal(t, []byte("sub-ms-check"), payload.Data)
	payload.Release()

	require.Equal(t, "4242", handler.lastRequestID)
	require.Contains(t, handler.lastTraceparent, rootSpan.SpanContext().TraceID().String())

	mfs, err := reg.Gather()
	require.NoError(t, err)
	logicalMF := requireMetricFamily(t, mfs, "bowl_client_rpc_logical_duration_seconds")
	requirePrometheusHistogramTotalCount(t, logicalMF, 1, len(SubMillisecondDurationBuckets), 0.000025)
	callMF := requireMetricFamily(t, mfs, "rpc_client_call_duration_seconds")
	requirePrometheusHistogramTotalCount(t, callMF, 2, len(SubMillisecondDurationBuckets), 0.000025)
	retriesMF := requireMetricFamily(t, mfs, "bowl_client_rpc_retries_total")
	require.InDelta(t, 1.0, retriesMF.GetMetric()[0].GetCounter().GetValue(), 1e-9)

	var rm metricdata.ResourceMetrics
	require.NoError(t, manualReader.Collect(context.Background(), &rm))
	requireOTelMetricHistogram(t, rm, "rpc.client.call.duration", 2, 0.000025)

	spans := spanExp.GetSpans()
	requireSpanByName(t, spans, "storage.v1.StorageService/Read")
}

func TestObservability_GivenStaleEpochError_ExportsCounterErrorReasonAndStructuredLog(t *testing.T) {

	reg, _, spanExp, tp := setupTestOTel(t)
	clk := newAutoAdvanceClock(time.Date(2026, 11, 13, 10, 0, 0, 0, time.UTC))
	rpcMetrics := NewMetrics()
	require.NoError(t, reg.Register(rpcMetrics))

	var logBuf bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&logBuf, nil))

	handler := &scriptedReadHandler{
		steps: []func(context.Context, *connect.Request[storagev1.ReadRequest]) (*connect.Response[storagev1.ReadResponse], error){
			func(context.Context, *connect.Request[storagev1.ReadRequest]) (*connect.Response[storagev1.ReadResponse], error) {
				cErr := connect.NewError(connect.CodeFailedPrecondition, errors.New("stale writer epoch"))
				detail, dErr := connect.NewErrorDetail(&errdetails.ErrorInfo{
					Domain: "bowl.storage",
					Reason: "EBUSY",
				})
				require.NoError(t, dErr)
				cErr.AddDetail(detail)
				return nil, cErr
			},
		},
	}

	client, err := newClientWithClock(ClientConfig{
		Endpoint:   "http://127.0.0.1:9000",
		HTTPClient: newInMemoryHTTPClient(handler),
		Logger:     logger,
		Metrics:    rpcMetrics,
	}, clk)
	require.NoError(t, err)

	ctx, rootSpan := tp.Tracer("test-caller").Start(context.Background(), "caller.write")

	_, err = client.Write(ctx, connect.NewRequest(&storagev1.WriteRequest{
		DiskId: "11111111-1111-4111-8111-111111111111",
		Epoch:  99,
		Ino:    7,
		Data:   []byte("payload"),
	}))
	rootSpan.End()

	require.Error(t, err)
	require.NotEmpty(t, handler.lastRequestID)
	require.NotEqual(t, "1", handler.lastRequestID)

	mfs, err := reg.Gather()
	require.NoError(t, err)
	staleMF := requireMetricFamily(t, mfs, "bowl_client_stale_epoch_total")
	require.InDelta(t, 1.0, staleMF.GetMetric()[0].GetCounter().GetValue(), 1e-9)

	var logEntry map[string]any
	require.NoError(t, json.Unmarshal(logBuf.Bytes(), &logEntry))
	require.Equal(t, "writer_epoch_fenced", logEntry["msg"])
	require.InDelta(t, float64(99), logEntry["local_epoch"], 1e-9)
	require.Equal(t, rootSpan.SpanContext().TraceID().String(), logEntry["trace_id"])

	clientSpan := requireSpanByName(t, spanExp.GetSpans(), "storage.v1.StorageService/Write")
	requireSpanAttr(t, clientSpan, "rpc.error_reason", "EBUSY")
}

func TestObservability_GivenSlowAttemptAndExhaustedRetry_EmitsSlowAndExhaustedLogsWithoutRealSleep(t *testing.T) {

	reg, _, _, _ := setupTestOTel(t)
	clk := newAutoAdvanceClock(time.Date(2026, 11, 13, 10, 0, 0, 0, time.UTC))
	rpcMetrics := NewMetrics()
	require.NoError(t, reg.Register(rpcMetrics))

	var logBuf bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&logBuf, nil))

	handler := &scriptedReadHandler{
		onCall: func() {
			clk.Advance(1200 * time.Millisecond)
		},
		steps: []func(context.Context, *connect.Request[storagev1.ReadRequest]) (*connect.Response[storagev1.ReadResponse], error){
			func(context.Context, *connect.Request[storagev1.ReadRequest]) (*connect.Response[storagev1.ReadResponse], error) {
				return nil, connect.NewError(connect.CodeUnavailable, errors.New("slow backend"))
			},
		},
	}

	client, err := newClientWithClock(ClientConfig{
		Endpoint:   "http://127.0.0.1:9000",
		HTTPClient: newInMemoryHTTPClient(handler),
		Logger:     logger,
		Metrics:    rpcMetrics,
		RetryConfig: &RetryConfig{
			InitialInterval: 100 * time.Millisecond,
			MaxElapsedTime:  1250 * time.Millisecond,
			RandFloat64:     func() float64 { return 1.0 },
		},
	}, clk)
	require.NoError(t, err)

	_, err = client.Read(context.Background(), connect.NewRequest(&storagev1.ReadRequest{Ino: 1, Size: 4}))

	require.Error(t, err)
	require.Equal(t, connect.CodeDeadlineExceeded, connect.CodeOf(err))

	mfs, err := reg.Gather()
	require.NoError(t, err)
	exhaustedMF := requireMetricFamily(t, mfs, "bowl_client_rpc_retry_exhausted_total")
	require.InDelta(t, 1.0, exhaustedMF.GetMetric()[0].GetCounter().GetValue(), 1e-9)

	logOutput := logBuf.String()
	require.Contains(t, logOutput, `"msg":"slow_rpc"`)
	require.Contains(t, logOutput, `"msg":"rpc_retry_exhausted_eio"`)
}

type scriptedMountBranchHandler struct {
	storagev1connect.UnimplementedStorageServiceHandler
	mountErr error
}

func (h *scriptedMountBranchHandler) MountBranch(
	_ context.Context,
	_ *connect.Request[storagev1.MountBranchRequest],
) (*connect.Response[storagev1.MountBranchResponse], error) {
	return nil, h.mountErr
}

func TestObservability_GivenAbortedEBUSYError_DoesNotTriggerStaleEpochMetricOrLog(t *testing.T) {

	reg, _, _, _ := setupTestOTel(t)
	clk := newAutoAdvanceClock(time.Date(2026, 11, 13, 10, 0, 0, 0, time.UTC))
	rpcMetrics := NewMetrics()
	require.NoError(t, reg.Register(rpcMetrics))

	var logBuf bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&logBuf, nil))

	abortedBusyErr := connect.NewError(connect.CodeAborted, errors.New("branch is currently mounted"))
	detail, err := connect.NewErrorDetail(&errdetails.ErrorInfo{
		Domain: "bowl.storage",
		Reason: "EBUSY",
	})
	require.NoError(t, err)
	abortedBusyErr.AddDetail(detail)

	client, err := newClientWithClock(ClientConfig{
		Endpoint:   "http://127.0.0.1:9000",
		HTTPClient: newInMemoryHTTPClient(&scriptedMountBranchHandler{mountErr: abortedBusyErr}),
		Logger:     logger,
		Metrics:    rpcMetrics,
	}, clk)
	require.NoError(t, err)

	_, err = client.MountBranch(context.Background(), connect.NewRequest(&storagev1.MountBranchRequest{
		DiskId: "11111111-1111-4111-8111-111111111111",
		Branch: "main",
	}))

	require.Error(t, err)
	require.Equal(t, connect.CodeAborted, connect.CodeOf(err))

	mfs, err := reg.Gather()
	require.NoError(t, err)
	staleMF := requireMetricFamily(t, mfs, "bowl_client_stale_epoch_total")
	require.InDelta(t, 0.0, staleMF.GetMetric()[0].GetCounter().GetValue(), 1e-9)
	require.NotContains(t, logBuf.String(), "writer_epoch_fenced")
}

type scriptedFailedPreconditionEBUSYHandler struct {
	storagev1connect.UnimplementedStorageServiceHandler
	err error
}

func (h *scriptedFailedPreconditionEBUSYHandler) DeleteCommit(
	_ context.Context,
	_ *connect.Request[storagev1.DeleteCommitRequest],
) (*connect.Response[storagev1.DeleteCommitResponse], error) {
	return nil, h.err
}

func (h *scriptedFailedPreconditionEBUSYHandler) RenewMount(
	_ context.Context,
	_ *connect.Request[storagev1.RenewMountRequest],
) (*connect.Response[storagev1.RenewMountResponse], error) {
	return nil, h.err
}

func (h *scriptedFailedPreconditionEBUSYHandler) Write(
	_ context.Context,
	_ *connect.Request[storagev1.WriteRequest],
) (*connect.Response[storagev1.WriteResponse], error) {
	return nil, h.err
}

func TestObservability_GivenDeleteCommitEBUSYAndEpochRequests_OnlyFencesEpochCarryingRequests(t *testing.T) {
	reg, _, _, _ := setupTestOTel(t)
	clk := newAutoAdvanceClock(time.Date(2026, 11, 13, 10, 0, 0, 0, time.UTC))
	rpcMetrics := NewMetrics()
	require.NoError(t, reg.Register(rpcMetrics))

	var logBuf bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&logBuf, nil))

	preconditionBusyErr := connect.NewError(connect.CodeFailedPrecondition, errors.New("resource busy"))
	detail, err := connect.NewErrorDetail(&errdetails.ErrorInfo{
		Domain: "bowl.storage",
		Reason: "EBUSY",
	})
	require.NoError(t, err)
	preconditionBusyErr.AddDetail(detail)

	client, err := newClientWithClock(ClientConfig{
		Endpoint:   "http://127.0.0.1:9000",
		HTTPClient: newInMemoryHTTPClient(&scriptedFailedPreconditionEBUSYHandler{err: preconditionBusyErr}),
		Logger:     logger,
		Metrics:    rpcMetrics,
	}, clk)
	require.NoError(t, err)

	_, err = client.DeleteCommit(context.Background(), connect.NewRequest(&storagev1.DeleteCommitRequest{
		DiskId:   "11111111-1111-4111-8111-111111111111",
		CommitId: 1,
	}))
	require.Error(t, err)
	require.Equal(t, connect.CodeFailedPrecondition, connect.CodeOf(err))

	mfs, err := reg.Gather()
	require.NoError(t, err)
	staleMF := requireMetricFamily(t, mfs, "bowl_client_stale_epoch_total")
	require.InDelta(t, 0.0, staleMF.GetMetric()[0].GetCounter().GetValue(), 1e-9, "DeleteCommit EBUSY must not increment stale epoch counter")
	require.NotContains(t, logBuf.String(), "writer_epoch_fenced")

	_, err = client.RenewMount(context.Background(), connect.NewRequest(&storagev1.RenewMountRequest{
		DiskId:  "11111111-1111-4111-8111-111111111111",
		MountId: "mount-1",
		Epoch:   5,
	}))
	require.Error(t, err)
	require.Equal(t, connect.CodeFailedPrecondition, connect.CodeOf(err))

	clk.Advance(2 * time.Second)

	_, err = client.Write(context.Background(), connect.NewRequest(&storagev1.WriteRequest{
		DiskId: "11111111-1111-4111-8111-111111111111",
		Epoch:  99,
		Ino:    7,
		Data:   []byte("payload"),
	}))
	require.Error(t, err)
	require.Equal(t, connect.CodeFailedPrecondition, connect.CodeOf(err))

	mfs, err = reg.Gather()
	require.NoError(t, err)
	staleMF = requireMetricFamily(t, mfs, "bowl_client_stale_epoch_total")
	require.InDelta(t, 2.0, staleMF.GetMetric()[0].GetCounter().GetValue(), 1e-9)

	lines := strings.Split(strings.TrimSpace(logBuf.String()), "\n")
	require.Len(t, lines, 2)

	var renewLog map[string]any
	require.NoError(t, json.Unmarshal([]byte(lines[0]), &renewLog))
	require.Equal(t, "writer_epoch_fenced", renewLog["msg"])
	require.Equal(t, storagev1connect.StorageServiceRenewMountProcedure, renewLog["procedure"])
	require.InDelta(t, float64(5), renewLog["local_epoch"], 1e-9)

	var writeLog map[string]any
	require.NoError(t, json.Unmarshal([]byte(lines[1]), &writeLog))
	require.Equal(t, "writer_epoch_fenced", writeLog["msg"])
	require.Equal(t, storagev1connect.StorageServiceWriteProcedure, writeLog["procedure"])
	require.InDelta(t, float64(99), writeLog["local_epoch"], 1e-9)
}
