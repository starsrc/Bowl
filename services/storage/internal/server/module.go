package server

import (
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/fx"

	"github.com/starsrc/pkg/observability/obsfx"
)

func provideHTTPMetricsCollector(m *HTTPMetrics) prometheus.Collector {
	return m
}

var Module = fx.Module("server",
	fx.Provide(
		NewHTTPMetrics,
		obsfx.AsCollector(provideHTTPMetricsCollector),
		ConfigureFineBucketMeterProvider,
		ConfigureStorageTracerProvider,
		New,
	),
	fx.Invoke(func(*Server) {}),
)
