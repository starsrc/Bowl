package store

import (
	"context"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

const storeInstrumentationName = "github.com/starsrc/bowl/services/storage/internal/store"

func currentStoreTracer() trace.Tracer {
	return otel.GetTracerProvider().Tracer(storeInstrumentationName)
}

func isSpanRecording(ctx context.Context) bool {
	if ctx == nil {
		return false
	}
	return trace.SpanFromContext(ctx).IsRecording()
}

func sampledSpanContext(ctx context.Context) trace.SpanContext {
	if !isSpanRecording(ctx) {
		return trace.SpanContext{}
	}
	sc := trace.SpanFromContext(ctx).SpanContext()
	if !sc.IsValid() || !sc.IsSampled() {
		return trace.SpanContext{}
	}
	return sc
}

func startChildSpan(ctx context.Context, name string, attrs ...attribute.KeyValue) (context.Context, trace.Span) {
	tracer := currentStoreTracer()
	if len(attrs) > 0 {
		return tracer.Start(ctx, name, trace.WithAttributes(attrs...))
	}
	return tracer.Start(ctx, name)
}

func finishSpan(span trace.Span, err error) {
	if span == nil {
		return
	}
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	}
	span.End()
}

func recordWindowSpan(ctx context.Context, name string, start, end time.Time, err error, attrs ...attribute.KeyValue) {
	if end.Before(start) {
		end = start
	}
	opts := make([]trace.SpanStartOption, 0, 2)
	opts = append(opts, trace.WithTimestamp(start))
	if len(attrs) > 0 {
		opts = append(opts, trace.WithAttributes(attrs...))
	}
	_, span := currentStoreTracer().Start(ctx, name, opts...)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	}
	span.End(trace.WithTimestamp(end))
}
