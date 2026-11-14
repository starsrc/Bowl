package server

import (
	"context"
	"errors"
	"fmt"
	"os"
	"runtime"
	"strconv"
	"strings"

	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/fx"

	"github.com/starsrc/pkg/observability"
	"github.com/starsrc/pkg/observability/telemetry"

	"github.com/starsrc/bowl/gen/go/storage/v1/storagev1connect"
)

// TracingReady 标记服务端自定义 OTel TracerProvider 与运行时锁/阻塞剖析参数已完成装配。
type TracingReady struct{}

// ConfigureRuntimeProfilesFromEnv 从环境变量按需开启 Mutex/Block 锁争用与阻塞剖析采样，
// 使管理面 /debug/pprof/mutex 与 /debug/pprof/block 在生产排障时可采集真实锁等待样本。
func ConfigureRuntimeProfilesFromEnv() {
	if raw := strings.TrimSpace(os.Getenv("PPROF_MUTEX_FRACTION")); raw != "" {
		if n, err := strconv.Atoi(raw); err == nil && n > 0 {
			runtime.SetMutexProfileFraction(n)
		}
	}
	if raw := strings.TrimSpace(os.Getenv("PPROF_BLOCK_RATE")); raw != "" {
		if n, err := strconv.Atoi(raw); err == nil && n > 0 {
			runtime.SetBlockProfileRate(n)
		}
	}
}

func isSampledControlSpanName(name string) bool {
	trimmed := strings.TrimPrefix(name, "/")
	switch trimmed {
	case strings.TrimPrefix(storagev1connect.StorageServiceCreateDiskProcedure, "/"),
		strings.TrimPrefix(storagev1connect.StorageServiceForkDiskProcedure, "/"),
		strings.TrimPrefix(storagev1connect.StorageServiceUpdateDiskProcedure, "/"),
		strings.TrimPrefix(storagev1connect.StorageServiceDeleteDiskProcedure, "/"),
		strings.TrimPrefix(storagev1connect.StorageServiceMountDiskProcedure, "/"),
		strings.TrimPrefix(storagev1connect.StorageServiceUnmountDiskProcedure, "/"),
		strings.TrimPrefix(storagev1connect.StorageServiceMergeDiskProcedure, "/"),
		strings.TrimPrefix(storagev1connect.StorageServicePushDiskProcedure, "/"),
		strings.TrimPrefix(storagev1connect.StorageServicePullDiskProcedure, "/"),
		strings.TrimPrefix(storagev1connect.StorageServiceReclaimOrphanProcedure, "/"):
		return true
	default:
		return false
	}
}

type storagePrioritySampler struct {
	base sdktrace.Sampler
}

//nolint:gocritic // sdktrace.Sampler 接口锁定按值传参签名
func (s storagePrioritySampler) ShouldSample(p sdktrace.SamplingParameters) sdktrace.SamplingResult {
	if isSampledControlSpanName(p.Name) {
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

func (s storagePrioritySampler) Description() string {
	return "StoragePrioritySampler{" + s.base.Description() + "}"
}

// NewStorageSampler 构建服务端链路采样器：
// - ratio <= 0 时关闭采样；
// - ratio > 0 时对低频高价值控制面写操作与围栏操作 100% 提权采样，其余请求按 TraceIDRatioBased 采样；
// - 对携带未采样远端 traceparent 的请求（如未开启导出器的短命 CLI），在保留同一 TraceID 前提下按本策略独立决策是否升级采样。
func NewStorageSampler(ratio float64) sdktrace.Sampler {
	if ratio <= 0 {
		return sdktrace.ParentBased(sdktrace.NeverSample())
	}
	priority := storagePrioritySampler{
		base: sdktrace.TraceIDRatioBased(ratio),
	}
	return sdktrace.ParentBased(
		priority,
		sdktrace.WithRemoteParentSampled(sdktrace.AlwaysSample()),
		sdktrace.WithRemoteParentNotSampled(priority),
	)
}

// NewStorageTracerProvider 构造装配了服务端控制面提权采样策略的 OTel TracerProvider。
func NewStorageTracerProvider(ctx context.Context, cfg telemetry.Config) (*sdktrace.TracerProvider, error) {
	if cfg == nil || !cfg.Enabled() {
		return sdktrace.NewTracerProvider(), nil
	}

	res, err := telemetry.NewResource(cfg)
	if err != nil {
		return nil, err
	}

	sampler := NewStorageSampler(cfg.SampleRatio())
	exporterType := strings.ToLower(strings.TrimSpace(cfg.TracesExporter()))
	if exporterType == "" {
		exporterType = telemetry.ExporterOTLP
	}

	switch exporterType {
	case telemetry.ExporterNone:
		return sdktrace.NewTracerProvider(
			sdktrace.WithResource(res),
			sdktrace.WithSampler(sampler),
		), nil
	case telemetry.ExporterOTLP:
		endpoint := cfg.Endpoint()
		var opts []otlptracegrpc.Option
		if strings.Contains(endpoint, "://") {
			opts = append(opts, otlptracegrpc.WithEndpointURL(endpoint))
		} else {
			opts = append(opts, otlptracegrpc.WithEndpoint(endpoint))
			if cfg.Insecure() {
				opts = append(opts, otlptracegrpc.WithInsecure())
			}
		}
		exporter, expErr := otlptracegrpc.New(ctx, opts...)
		if expErr != nil {
			return nil, fmt.Errorf("server: failed to create OTLP trace exporter: %w", expErr)
		}
		return sdktrace.NewTracerProvider(
			sdktrace.WithBatcher(exporter),
			sdktrace.WithResource(res),
			sdktrace.WithSampler(sampler),
		), nil
	default:
		return nil, fmt.Errorf("server: unsupported traces exporter %q", exporterType)
	}
}

// TracingParams 定义注册服务端控制面提权 OTel TracerProvider 所需的 Fx 可选参数。
type TracingParams struct {
	fx.In

	Lifecycle fx.Lifecycle           `optional:"true"`
	Runtime   *observability.Runtime `optional:"true"`
	Telemetry telemetry.Config       `optional:"true"`
	ObsReady  observability.Ready    `optional:"true"`
}

// ConfigureStorageTracerProvider 在 obsfx Runtime 就绪后配置全局 TracerProvider 与 W3C Propagator，
// 并绑定 Fx OnStop 优雅冲洗关闭。
func ConfigureStorageTracerProvider(p TracingParams) (TracingReady, error) {
	ConfigureRuntimeProfilesFromEnv()

	if p.Telemetry == nil || !p.Telemetry.Enabled() {
		return TracingReady{}, nil
	}
	if p.Runtime != nil && p.Runtime.TracerProvider() != nil {
		return TracingReady{}, errors.New("server: runtime default tracer provider must be disabled before configuring storage tracer provider")
	}

	tp, err := NewStorageTracerProvider(context.Background(), p.Telemetry)
	if err != nil {
		return TracingReady{}, err
	}
	telemetry.RegisterGlobalTracer(tp)

	if p.Lifecycle != nil {
		p.Lifecycle.Append(fx.Hook{
			OnStop: func(stopCtx context.Context) error {
				return tp.Shutdown(stopCtx)
			},
		})
	}
	return TracingReady{}, nil
}
