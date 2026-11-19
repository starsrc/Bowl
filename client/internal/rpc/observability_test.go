package rpc

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
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
	// Given: 装配真实 Prometheus Exporter + ManualReader + InMemory SpanExporter 与虚拟时钟
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

	// When
	resp, err := client.Read(ctx, connect.NewRequest(&storagev1.ReadRequest{
		Ino:    1,
		Offset: 0,
		Size:   12,
		DiskId: "11111111-1111-4111-8111-111111111111",
	}))
	rootSpan.End()

	// Then
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
	// Given
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

	// When
	_, err = client.Write(ctx, connect.NewRequest(&storagev1.WriteRequest{
		DiskId: "11111111-1111-4111-8111-111111111111",
		Epoch:  99,
		Ino:    7,
		Data:   []byte("payload"),
	}))
	rootSpan.End()

	// Then
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
	// Given: handler.onCall 推进虚拟时钟 1200ms (> DefaultSlowRPCThreshold 1s)，并触发重试预算耗尽
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

	// When
	_, err = client.Read(context.Background(), connect.NewRequest(&storagev1.ReadRequest{Ino: 1, Size: 4}))

	// Then
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

type scriptedMountDiskHandler struct {
	storagev1connect.UnimplementedStorageServiceHandler
	mountErr error
}

func (h *scriptedMountDiskHandler) MountDisk(
	_ context.Context,
	_ *connect.Request[storagev1.MountDiskRequest],
) (*connect.Response[storagev1.MountDiskResponse], error) {
	return nil, h.mountErr
}

func TestObservability_GivenAbortedEBUSYError_DoesNotTriggerStaleEpochMetricOrLog(t *testing.T) {
	// Given: server returns CodeAborted + ErrorInfo{Domain: "bowl.storage", Reason: "EBUSY"} (ErrDiskMounted / ErrConcurrentMerge).
	reg, _, _, _ := setupTestOTel(t)
	clk := newAutoAdvanceClock(time.Date(2026, 11, 13, 10, 0, 0, 0, time.UTC))
	rpcMetrics := NewMetrics()
	require.NoError(t, reg.Register(rpcMetrics))

	var logBuf bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&logBuf, nil))

	abortedBusyErr := connect.NewError(connect.CodeAborted, errors.New("disk is currently mounted"))
	detail, err := connect.NewErrorDetail(&errdetails.ErrorInfo{
		Domain: "bowl.storage",
		Reason: "EBUSY",
	})
	require.NoError(t, err)
	abortedBusyErr.AddDetail(detail)

	client, err := newClientWithClock(ClientConfig{
		Endpoint:   "http://127.0.0.1:9000",
		HTTPClient: newInMemoryHTTPClient(&scriptedMountDiskHandler{mountErr: abortedBusyErr}),
		Logger:     logger,
		Metrics:    rpcMetrics,
	}, clk)
	require.NoError(t, err)

	// When: calling MountDisk and receiving CodeAborted + EBUSY.
	_, err = client.MountDisk(context.Background(), connect.NewRequest(&storagev1.MountDiskRequest{
		DiskId: "11111111-1111-4111-8111-111111111111",
	}))

	// Then: error propagates as CodeAborted, but stale_epoch counter stays 0 and no writer_epoch_fenced log is emitted.
	require.Error(t, err)
	require.Equal(t, connect.CodeAborted, connect.CodeOf(err))

	mfs, err := reg.Gather()
	require.NoError(t, err)
	staleMF := requireMetricFamily(t, mfs, "bowl_client_stale_epoch_total")
	require.InDelta(t, 0.0, staleMF.GetMetric()[0].GetCounter().GetValue(), 1e-9)
	require.NotContains(t, logBuf.String(), "writer_epoch_fenced")
}
