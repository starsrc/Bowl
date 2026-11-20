package cli

import (
	"context"
	"os"
	"strings"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	"go.opentelemetry.io/otel/trace"
)

const cliTracerServiceName = "bowl-cli"

func startDiskCLISpan(
	ctx context.Context,
	spanName string,
	attrs ...attribute.KeyValue,
) (context.Context, trace.Span, func()) {
	if ctx == nil {
		ctx = context.Background()
	}

	if _, isSDK := otel.GetTracerProvider().(*sdktrace.TracerProvider); isSDK {
		spanCtx, span := otel.Tracer(cliTracerServiceName).Start(ctx, spanName, trace.WithAttributes(attrs...))
		return spanCtx, span, func() { span.End() }
	}

	normalizeOTLPEnv()
	prevTP := otel.GetTracerProvider()
	prevProp := otel.GetTextMapPropagator()

	serviceName := strings.TrimSpace(os.Getenv("OTEL_SERVICE_NAME"))
	if serviceName == "" {
		serviceName = cliTracerServiceName
	}
	res := resource.NewWithAttributes("", semconv.ServiceName(serviceName))

	opts := []sdktrace.TracerProviderOption{
		sdktrace.WithResource(res),
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
	}

	exporterType := strings.ToLower(strings.TrimSpace(os.Getenv("OTEL_TRACES_EXPORTER")))
	endpoint := firstNonEmptyEnv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", "OTEL_EXPORTER_OTLP_ENDPOINT")
	otelEnabled := strings.EqualFold(strings.TrimSpace(os.Getenv("OTEL_ENABLED")), "true")
	if (otelEnabled || endpoint != "") && exporterType != "none" && endpoint != "" {
		normalized := normalizeOTLPEndpoint(endpoint)
		otlpOpts := []otlptracegrpc.Option{
			otlptracegrpc.WithEndpointURL(normalized),
		}
		if strings.HasPrefix(strings.ToLower(normalized), "http://") {
			otlpOpts = append(otlpOpts, otlptracegrpc.WithInsecure())
		}
		if exporter, err := otlptracegrpc.New(ctx, otlpOpts...); err == nil {
			opts = append(opts, sdktrace.WithBatcher(exporter))
		}
	}

	tp := sdktrace.NewTracerProvider(opts...)
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))

	spanCtx, span := tp.Tracer(cliTracerServiceName).Start(ctx, spanName, trace.WithAttributes(attrs...))
	cleanup := func() {
		span.End()
		flushCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		_ = tp.Shutdown(flushCtx)
		cancel()
		otel.SetTracerProvider(prevTP)
		otel.SetTextMapPropagator(prevProp)
	}
	return spanCtx, span, cleanup
}

func finishDiskCLISpanError(span trace.Span, err error) {
	if span == nil || err == nil {
		return
	}
	span.RecordError(err)
	span.SetStatus(codes.Error, err.Error())
}
