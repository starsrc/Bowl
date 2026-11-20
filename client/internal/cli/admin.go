package cli

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/starsrc/pkg/observability"
	"github.com/starsrc/pkg/observability/slogx"
	"github.com/starsrc/pkg/observability/telemetry"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	otelprom "go.opentelemetry.io/otel/exporters/prometheus"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"

	"github.com/starsrc/bowl/client/internal/fusefs"
	"github.com/starsrc/bowl/client/internal/rpc"
	"github.com/starsrc/bowl/client/internal/version"
)

const clientServiceName = "bowl-client"

func normalizeOTLPEndpoint(raw string) string {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return ""
	}
	if !strings.Contains(trimmed, "://") {
		return "http://" + trimmed
	}
	return trimmed
}

func normalizeOTLPEnv() {
	for _, key := range []string{"OTEL_EXPORTER_OTLP_ENDPOINT", "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT"} {
		raw := strings.TrimSpace(os.Getenv(key))
		if raw == "" {
			continue
		}
		if normalized := normalizeOTLPEndpoint(raw); normalized != raw {
			_ = os.Setenv(key, normalized)
		}
	}
}

type priorityOpSampler struct {
	base sdktrace.Sampler
}

func (s priorityOpSampler) ShouldSample(p sdktrace.SamplingParameters) sdktrace.SamplingResult {
	switch p.Name {
	case "fuse.Fsync", "fuse.FsyncDir", "fuse.OnUnmount":
		return sdktrace.SamplingResult{
			Decision:   sdktrace.RecordAndSample,
			Tracestate: trace.SpanContextFromContext(p.ParentContext).TraceState(),
		}
	}
	for _, link := range p.Links {
		if link.SpanContext.IsSampled() {
			return sdktrace.SamplingResult{
				Decision:   sdktrace.RecordAndSample,
				Tracestate: trace.SpanContextFromContext(p.ParentContext).TraceState(),
			}
		}
	}
	return s.base.ShouldSample(p)
}

func (s priorityOpSampler) Description() string {
	return "PriorityOpSampler{" + s.base.Description() + "}"
}

func newClientTraceSampler(ratio float64) sdktrace.Sampler {
	if ratio <= 0 {
		return sdktrace.ParentBased(sdktrace.NeverSample())
	}
	return sdktrace.ParentBased(priorityOpSampler{
		base: sdktrace.TraceIDRatioBased(ratio),
	})
}

func newClientMetricView() sdkmetric.View {
	return sdkmetric.NewView(
		sdkmetric.Instrument{
			Name: "rpc.client.*",
			Kind: sdkmetric.InstrumentKindHistogram,
		},
		sdkmetric.Stream{
			Aggregation: sdkmetric.AggregationExplicitBucketHistogram{
				Boundaries: rpc.SubMillisecondDurationBuckets,
			},
		},
	)
}

func buildObsConfig(opts *mountOptions) *observability.EnvConfig {
	normalizeOTLPEnv()

	cfg := observability.DefaultConfig()
	cfg.LogConfig = slogx.LogConfig{
		LogLevel:          opts.logLevel,
		LogFormat:         opts.logFormat,
		LogConnectMessage: slogx.DefaultConnectMessage,
	}
	serviceName := strings.TrimSpace(os.Getenv("OTEL_SERVICE_NAME"))
	if serviceName == "" {
		serviceName = clientServiceName
	}
	cfg.OtelEnabled = true
	cfg.OtelServiceName = serviceName
	cfg.OtelServiceVersion = version.Version
	cfg.OtelSampleRatio = opts.traceSampleRatio
	cfg.OtelTracesExporter = telemetry.ExporterNone

	endpoint := firstNonEmptyEnv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", "OTEL_EXPORTER_OTLP_ENDPOINT")
	if endpoint != "" {
		normalized := normalizeOTLPEndpoint(endpoint)
		cfg.OtelTracesExporter = telemetry.ExporterOTLP
		cfg.OtelEndpoint = normalized
		cfg.OtelInsecure = strings.HasPrefix(strings.ToLower(normalized), "http://")
		if rawInsecure := strings.TrimSpace(os.Getenv("OTEL_EXPORTER_OTLP_INSECURE")); rawInsecure != "" {
			if parsed, err := strconv.ParseBool(rawInsecure); err == nil {
				cfg.OtelInsecure = parsed
			}
		}
	}
	cfg.AdminAddr = ""
	return &cfg
}

type clientRuntime struct {
	logger         *slog.Logger
	registry       *prometheus.Registry
	meterProvider  *sdkmetric.MeterProvider
	tracerProvider *sdktrace.TracerProvider
	shutdownOnce   sync.Once
	shutdownErr    error
}

func (r *clientRuntime) Logger() *slog.Logger {
	return r.logger
}

func (r *clientRuntime) TracerProvider() *sdktrace.TracerProvider {
	return r.tracerProvider
}

func (r *clientRuntime) RegisterCollector(cs ...prometheus.Collector) error {
	for _, c := range cs {
		if c == nil {
			continue
		}
		if err := r.registry.Register(c); err != nil {
			return err
		}
	}
	return nil
}

func (r *clientRuntime) MountAdminRoutes(mux *http.ServeMux, opts ...observability.AdminOption) {
	observability.RegisterAdminRoutes(mux, r.registry, observability.AdminConfig{
		MetricsEnabled: true,
		PprofEnabled:   true,
	}, opts...)
}

func (r *clientRuntime) Shutdown(ctx context.Context) error {
	r.shutdownOnce.Do(func() {
		var errs []error
		if r.tracerProvider != nil {
			if err := r.tracerProvider.Shutdown(ctx); err != nil {
				errs = append(errs, err)
			}
		}
		if r.meterProvider != nil {
			if err := r.meterProvider.Shutdown(ctx); err != nil {
				errs = append(errs, err)
			}
		}
		r.shutdownErr = errors.Join(errs...)
	})
	return r.shutdownErr
}

func newClientTracerProvider(ctx context.Context, cfg *observability.EnvConfig, res *resource.Resource) (*sdktrace.TracerProvider, error) {
	sampler := newClientTraceSampler(cfg.SampleRatio())
	switch cfg.TracesExporter() {
	case telemetry.ExporterNone:
		return sdktrace.NewTracerProvider(
			sdktrace.WithResource(res),
			sdktrace.WithSampler(sampler),
		), nil
	case telemetry.ExporterOTLP:
		endpoint := normalizeOTLPEndpoint(cfg.Endpoint())
		otlpOpts := []otlptracegrpc.Option{
			otlptracegrpc.WithEndpointURL(endpoint),
		}
		if cfg.Insecure() && strings.HasPrefix(strings.ToLower(endpoint), "http://") {
			otlpOpts = append(otlpOpts, otlptracegrpc.WithInsecure())
		}
		exporter, err := otlptracegrpc.New(ctx, otlpOpts...)
		if err != nil {
			return nil, fmt.Errorf("create OTLP trace exporter: %w", err)
		}
		return sdktrace.NewTracerProvider(
			sdktrace.WithBatcher(exporter),
			sdktrace.WithResource(res),
			sdktrace.WithSampler(sampler),
		), nil
	default:
		return nil, fmt.Errorf("unsupported trace exporter %q", cfg.TracesExporter())
	}
}

func setupObservability(ctx context.Context, opts *mountOptions, logWriter io.Writer) (*clientRuntime, error) {
	cfg := buildObsConfig(opts)
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("initialize observability: %w", err)
	}

	var slogOpts []slogx.Option
	if logWriter != nil {
		slogOpts = append(slogOpts, slogx.WithWriter(logWriter))
	}
	slogOpts = append(slogOpts, slogx.WithSetDefault(true))
	logger := slogx.New(cfg, slogOpts...)

	reg := telemetry.NewRegistry()
	if err := telemetry.RegisterRuntimeCollectors(reg); err != nil {
		return nil, fmt.Errorf("initialize observability: %w", err)
	}

	res, err := telemetry.NewResource(cfg)
	if err != nil {
		return nil, fmt.Errorf("initialize observability: %w", err)
	}

	promExporter, err := otelprom.New(
		otelprom.WithRegisterer(reg),
		otelprom.WithoutScopeInfo(),
	)
	if err != nil {
		return nil, fmt.Errorf("initialize observability: %w", err)
	}

	mp := sdkmetric.NewMeterProvider(
		sdkmetric.WithResource(res),
		sdkmetric.WithReader(promExporter),
		sdkmetric.WithView(newClientMetricView()),
	)
	otel.SetMeterProvider(mp)

	tp, err := newClientTracerProvider(ctx, cfg, res)
	if err != nil {
		_ = mp.Shutdown(ctx)
		return nil, fmt.Errorf("initialize observability: %w", err)
	}
	telemetry.RegisterGlobalTracer(tp)

	return &clientRuntime{
		logger:         logger,
		registry:       reg,
		meterProvider:  mp,
		tracerProvider: tp,
	}, nil
}

func validateAdminAddr(addr string) error {
	if strings.EqualFold(addr, adminAddrOff) {
		return nil
	}
	if _, _, err := net.SplitHostPort(addr); err != nil {
		return fmt.Errorf("invalid --admin-addr %q: %w", addr, err)
	}
	return nil
}

type adminServer struct {
	ln  net.Listener
	srv *http.Server
}

func newAdminMux(obs *clientRuntime, fs *fusefs.RawFS, tracker *mountLeaseTracker) *http.ServeMux {
	mux := http.NewServeMux()
	obs.MountAdminRoutes(mux, observability.WithReadyCheck("mount_lease", func(context.Context) error {
		return checkMountLeaseReady(fs, tracker)
	}))
	mux.Handle("GET /debug/stats", statsHandler(fs))
	mux.Handle("GET /debug/unmount", unmountProgressHandler(fs))
	return mux
}

func startAdminServer(addr string, obs *clientRuntime, fs *fusefs.RawFS, tracker *mountLeaseTracker) (*adminServer, error) {
	if strings.EqualFold(addr, adminAddrOff) {
		return nil, nil
	}
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("listen on admin address %q: %w", addr, err)
	}
	mux := newAdminMux(obs, fs, tracker)

	srv := &http.Server{
		Handler:           mux,
		ReadHeaderTimeout: 10 * time.Second,
	}
	go func() {
		if serveErr := srv.Serve(ln); serveErr != nil && !errors.Is(serveErr, http.ErrServerClosed) {
			obs.Logger().Warn("admin_server_stopped", "error", serveErr.Error())
		}
	}()
	return &adminServer{ln: ln, srv: srv}, nil
}

func (a *adminServer) addr() string {
	if a == nil {
		return adminAddrOff
	}
	return a.ln.Addr().String()
}

func (a *adminServer) shutdown(ctx context.Context) {
	if a == nil {
		return
	}
	if err := a.srv.Shutdown(ctx); err != nil {
		_ = a.srv.Close()
	}
}

func statsHandler(fs *fusefs.RawFS) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		snap := fs.Snapshot()
		switch format := r.URL.Query().Get("format"); format {
		case "", "json":
			body, err := snap.FormatJSON()
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			w.Header().Set("Content-Type", "application/json; charset=utf-8")
			_, _ = w.Write(body)
		case "text":
			w.Header().Set("Content-Type", "text/plain; charset=utf-8")
			_, _ = io.WriteString(w, snap.FormatText())
		case "stats":
			w.Header().Set("Content-Type", "text/plain; charset=utf-8")
			_, _ = io.WriteString(w, snap.FormatStatsFile())
		default:
			http.Error(w, fmt.Sprintf("unsupported format %q (json|text|stats)", format), http.StatusBadRequest)
		}
	})
}

func unmountProgressHandler(fs *fusefs.RawFS) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		body, err := json.Marshal(fs.UnmountProgress())
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		_, _ = w.Write(body)
	})
}
