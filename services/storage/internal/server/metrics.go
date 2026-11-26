package server

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"go.opentelemetry.io/otel"
	otelprom "go.opentelemetry.io/otel/exporters/prometheus"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"

	"github.com/starsrc/pkg/observability"
	"github.com/starsrc/pkg/observability/telemetry"

	"github.com/starsrc/bowl/gen/go/storage/v1/storagev1connect"
)

var SubMillisecondBuckets = []float64{
	0.000025, 0.00005, 0.0001, 0.00025, 0.0005,
	0.001, 0.0025, 0.005, 0.01, 0.025,
	0.05, 0.1, 0.25, 0.5, 1.0,
	2.5, 5.0, 10.0,
}

const (
	httpStatus2xx   = "2xx"
	httpStatus4xx   = "4xx"
	httpStatus5xx   = "5xx"
	httpStatusOther = "other"
	procedureOther  = "other"
)

var knownProcedures = map[string]string{
	storagev1connect.StorageServiceLookupProcedure:         storagev1connect.StorageServiceLookupProcedure,
	storagev1connect.StorageServiceGetAttrProcedure:        storagev1connect.StorageServiceGetAttrProcedure,
	storagev1connect.StorageServiceSetAttrProcedure:        storagev1connect.StorageServiceSetAttrProcedure,
	storagev1connect.StorageServiceMknodProcedure:          storagev1connect.StorageServiceMknodProcedure,
	storagev1connect.StorageServiceMkdirProcedure:          storagev1connect.StorageServiceMkdirProcedure,
	storagev1connect.StorageServiceSymlinkProcedure:        storagev1connect.StorageServiceSymlinkProcedure,
	storagev1connect.StorageServiceReadlinkProcedure:       storagev1connect.StorageServiceReadlinkProcedure,
	storagev1connect.StorageServiceLinkProcedure:           storagev1connect.StorageServiceLinkProcedure,
	storagev1connect.StorageServiceUnlinkProcedure:         storagev1connect.StorageServiceUnlinkProcedure,
	storagev1connect.StorageServiceRmdirProcedure:          storagev1connect.StorageServiceRmdirProcedure,
	storagev1connect.StorageServiceRenameProcedure:         storagev1connect.StorageServiceRenameProcedure,
	storagev1connect.StorageServiceCreateProcedure:         storagev1connect.StorageServiceCreateProcedure,
	storagev1connect.StorageServiceCreateTmpfileProcedure:  storagev1connect.StorageServiceCreateTmpfileProcedure,
	storagev1connect.StorageServiceReadProcedure:           storagev1connect.StorageServiceReadProcedure,
	storagev1connect.StorageServiceWriteProcedure:          storagev1connect.StorageServiceWriteProcedure,
	storagev1connect.StorageServiceFsyncProcedure:          storagev1connect.StorageServiceFsyncProcedure,
	storagev1connect.StorageServiceFsyncDirProcedure:       storagev1connect.StorageServiceFsyncDirProcedure,
	storagev1connect.StorageServiceReadDirProcedure:        storagev1connect.StorageServiceReadDirProcedure,
	storagev1connect.StorageServiceReadDirPlusProcedure:    storagev1connect.StorageServiceReadDirPlusProcedure,
	storagev1connect.StorageServiceFallocateProcedure:      storagev1connect.StorageServiceFallocateProcedure,
	storagev1connect.StorageServiceSetXattrProcedure:       storagev1connect.StorageServiceSetXattrProcedure,
	storagev1connect.StorageServiceGetXattrProcedure:       storagev1connect.StorageServiceGetXattrProcedure,
	storagev1connect.StorageServiceListXattrProcedure:      storagev1connect.StorageServiceListXattrProcedure,
	storagev1connect.StorageServiceRemoveXattrProcedure:    storagev1connect.StorageServiceRemoveXattrProcedure,
	storagev1connect.StorageServiceStatFSProcedure:         storagev1connect.StorageServiceStatFSProcedure,
	storagev1connect.StorageServiceReclaimOrphanProcedure:  storagev1connect.StorageServiceReclaimOrphanProcedure,
	storagev1connect.StorageServiceMountBranchProcedure:    storagev1connect.StorageServiceMountBranchProcedure,
	storagev1connect.StorageServiceRenewMountProcedure:     storagev1connect.StorageServiceRenewMountProcedure,
	storagev1connect.StorageServiceUnmountBranchProcedure:  storagev1connect.StorageServiceUnmountBranchProcedure,
	storagev1connect.StorageServiceListMountsProcedure:     storagev1connect.StorageServiceListMountsProcedure,
	storagev1connect.StorageServiceEvictMountsProcedure:    storagev1connect.StorageServiceEvictMountsProcedure,
	storagev1connect.StorageServiceCreateDiskProcedure:     storagev1connect.StorageServiceCreateDiskProcedure,
	storagev1connect.StorageServiceGetDiskProcedure:        storagev1connect.StorageServiceGetDiskProcedure,
	storagev1connect.StorageServiceListDisksProcedure:      storagev1connect.StorageServiceListDisksProcedure,
	storagev1connect.StorageServiceUpdateDiskProcedure:     storagev1connect.StorageServiceUpdateDiskProcedure,
	storagev1connect.StorageServiceDeleteDiskProcedure:     storagev1connect.StorageServiceDeleteDiskProcedure,
	storagev1connect.StorageServiceForkBranchProcedure:     storagev1connect.StorageServiceForkBranchProcedure,
	storagev1connect.StorageServiceListBranchesProcedure:   storagev1connect.StorageServiceListBranchesProcedure,
	storagev1connect.StorageServiceDeleteBranchProcedure:   storagev1connect.StorageServiceDeleteBranchProcedure,
	storagev1connect.StorageServiceResetBranchProcedure:    storagev1connect.StorageServiceResetBranchProcedure,
	storagev1connect.StorageServiceMergeBranchProcedure:    storagev1connect.StorageServiceMergeBranchProcedure,
	storagev1connect.StorageServiceCreateCommitProcedure:   storagev1connect.StorageServiceCreateCommitProcedure,
	storagev1connect.StorageServiceListCommitsProcedure:    storagev1connect.StorageServiceListCommitsProcedure,
	storagev1connect.StorageServiceDeleteCommitProcedure:   storagev1connect.StorageServiceDeleteCommitProcedure,
	storagev1connect.StorageServicePruneCommitsProcedure:   storagev1connect.StorageServicePruneCommitsProcedure,
	storagev1connect.StorageServiceReadCommitNodeProcedure: storagev1connect.StorageServiceReadCommitNodeProcedure,
	storagev1connect.StorageServiceDiffBranchesProcedure:   storagev1connect.StorageServiceDiffBranchesProcedure,
	storagev1connect.StorageServiceDiffCommitsProcedure:    storagev1connect.StorageServiceDiffCommitsProcedure,
	storagev1connect.StorageServiceDiffWorkProcedure:       storagev1connect.StorageServiceDiffWorkProcedure,
}

func normalizeProcedure(path string) string {
	if canonical, ok := knownProcedures[path]; ok {
		return canonical
	}
	return procedureOther
}

func classifyHTTPStatus(code int) string {
	switch {
	case code >= 200 && code < 300:
		return httpStatus2xx
	case code >= 400 && code < 500:
		return httpStatus4xx
	case code >= 500 && code < 600:
		return httpStatus5xx
	default:
		return httpStatusOther
	}
}

func NewFineBucketMeterProvider(cfg telemetry.Config, reg *prometheus.Registry) (*sdkmetric.MeterProvider, error) {
	if reg == nil {
		reg = telemetry.NewRegistry()
	}

	if err := telemetry.RegisterRuntimeCollectors(reg); err != nil {
		return nil, err
	}

	res, err := telemetry.NewResource(cfg)
	if err != nil {
		return nil, err
	}

	promExporter, err := otelprom.New(
		otelprom.WithRegisterer(reg),
		otelprom.WithoutScopeInfo(),
	)
	if err != nil {
		return nil, fmt.Errorf("server: create fine-bucket prometheus exporter: %w", err)
	}

	explicitAgg := sdkmetric.Stream{
		Aggregation: sdkmetric.AggregationExplicitBucketHistogram{
			Boundaries: SubMillisecondBuckets,
		},
	}

	mp := sdkmetric.NewMeterProvider(
		sdkmetric.WithResource(res),
		sdkmetric.WithReader(promExporter),
		sdkmetric.WithView(
			sdkmetric.NewView(sdkmetric.Instrument{Name: "rpc.server.call.duration"}, explicitAgg),
			sdkmetric.NewView(sdkmetric.Instrument{Name: "rpc.server.duration"}, explicitAgg),
		),
	)
	return mp, nil
}

func ConfigureFineBucketMeterProvider(
	rt *observability.Runtime,
	telCfg telemetry.Config,
	adminCfg observability.AdminRouteConfig,
) (*sdkmetric.MeterProvider, error) {
	if rt == nil {
		return nil, nil
	}
	if adminCfg != nil && !adminCfg.IsMetricsEnabled() {
		return nil, nil
	}
	if rt.MeterProvider() != nil {
		return nil, errors.New("server: runtime default meter provider must be disabled before configuring fine-bucket meter provider")
	}

	mp, err := NewFineBucketMeterProvider(telCfg, rt.Registry())
	if err != nil {
		return nil, err
	}
	otel.SetMeterProvider(mp)
	return mp, nil
}

var _ prometheus.Collector = (*HTTPMetrics)(nil)

type HTTPMetrics struct {
	reqDuration *prometheus.HistogramVec
	reqBytes    *prometheus.CounterVec
	respBytes   *prometheus.CounterVec
}

func NewHTTPMetrics() *HTTPMetrics {
	return &HTTPMetrics{
		reqDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "bowl_storage_http_request_duration_seconds",
			Help:    "End-to-end HTTP request wall-clock duration including body read, protobuf codec, handler, and response write in seconds.",
			Buckets: SubMillisecondBuckets,
		}, []string{"procedure", "status_class"}),
		reqBytes: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "bowl_storage_http_request_bytes_total",
			Help: "Total HTTP request body bytes read by the storage server by procedure.",
		}, []string{"procedure"}),
		respBytes: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "bowl_storage_http_response_bytes_total",
			Help: "Total HTTP response body bytes written by the storage server by procedure.",
		}, []string{"procedure"}),
	}
}

func (m *HTTPMetrics) Describe(ch chan<- *prometheus.Desc) {
	if m == nil {
		return
	}
	m.reqDuration.Describe(ch)
	m.reqBytes.Describe(ch)
	m.respBytes.Describe(ch)
}

func (m *HTTPMetrics) Collect(ch chan<- prometheus.Metric) {
	if m == nil {
		return
	}
	m.reqDuration.Collect(ch)
	m.reqBytes.Collect(ch)
	m.respBytes.Collect(ch)
}

func (m *HTTPMetrics) observe(procedure string, statusCode int, d time.Duration, rxBytes, txBytes int64) {
	if m == nil {
		return
	}
	m.reqDuration.WithLabelValues(procedure, classifyHTTPStatus(statusCode)).Observe(max(0, d.Seconds()))
	if rxBytes > 0 {
		m.reqBytes.WithLabelValues(procedure).Add(float64(rxBytes))
	}
	if txBytes > 0 {
		m.respBytes.WithLabelValues(procedure).Add(float64(txBytes))
	}
}

type countingReadCloser struct {
	io.ReadCloser
	readBytes int64
}

func (r *countingReadCloser) Read(p []byte) (int, error) {
	n, err := r.ReadCloser.Read(p)
	if n > 0 {
		r.readBytes += int64(n)
	}
	return n, err
}

type countingResponseWriter struct {
	http.ResponseWriter
	statusCode  int
	wroteHeader bool
	wroteBytes  int64
}

func (w *countingResponseWriter) WriteHeader(statusCode int) {
	if !w.wroteHeader {
		w.statusCode = statusCode
		w.wroteHeader = true
	}
	w.ResponseWriter.WriteHeader(statusCode)
}

func (w *countingResponseWriter) Write(p []byte) (int, error) {
	if !w.wroteHeader {
		w.statusCode = http.StatusOK
		w.wroteHeader = true
	}
	n, err := w.ResponseWriter.Write(p)
	if n > 0 {
		w.wroteBytes += int64(n)
	}
	return n, err
}

func (w *countingResponseWriter) Flush() {
	if f, ok := w.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

func (w *countingResponseWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	if hj, ok := w.ResponseWriter.(http.Hijacker); ok {
		return hj.Hijack()
	}
	return nil, nil, errors.New("http.Hijacker not supported")
}

func (w *countingResponseWriter) Unwrap() http.ResponseWriter {
	return w.ResponseWriter
}

func WrapHTTPHandler(next http.Handler, metrics *HTTPMetrics) http.Handler {
	if next == nil || metrics == nil {
		return next
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		procedure := normalizeProcedure(r.URL.Path)

		var bodyReader *countingReadCloser
		if r.Body != nil && r.Body != http.NoBody {
			bodyReader = &countingReadCloser{ReadCloser: r.Body}
			r.Body = bodyReader
		}

		cw := &countingResponseWriter{
			ResponseWriter: w,
			statusCode:     http.StatusOK,
		}

		defer func() {
			var rx int64
			if bodyReader != nil {
				rx = bodyReader.readBytes
			}
			metrics.observe(procedure, cw.statusCode, time.Since(start), rx, cw.wroteBytes)
		}()

		next.ServeHTTP(cw, r)
	})
}
