package config

import (
	"time"

	"github.com/starsrc/Aether/pkg/observability/telemetry"
)

// TelemetryConfig 实现 telemetry.Config 契约，承载 OpenTelemetry 遥测与追踪配置。
type TelemetryConfig struct {
	EnabledVal         bool          `env:"OTEL_ENABLED" envDefault:"false"`
	ServiceNameVal     string        `env:"OTEL_SERVICE_NAME" envDefault:"bowl-storage"`
	EndpointVal        string        `env:"OTEL_EXPORTER_OTLP_ENDPOINT" envDefault:"localhost:4317"`
	TracesEndpointVal  string        `env:"OTEL_EXPORTER_OTLP_TRACES_ENDPOINT"`
	InsecureVal        bool          `env:"OTEL_EXPORTER_OTLP_INSECURE" envDefault:"true"`
	TracesInsecureVal  *bool         `env:"OTEL_EXPORTER_OTLP_TRACES_INSECURE"`
	SampleRatioVal     float64       `env:"OTEL_SAMPLE_RATIO" envDefault:"1.0"`
	ShutdownTimeoutVal time.Duration `env:"OTEL_SHUTDOWN_TIMEOUT" envDefault:"5s"`
	TrustRemoteVal     bool          `env:"OTEL_TRUST_REMOTE" envDefault:"false"`
}

// Enabled 返回是否开启链路追踪。
func (c TelemetryConfig) Enabled() bool {
	return c.EnabledVal
}

// ServiceName 返回遥测服务名称。
func (c TelemetryConfig) ServiceName() string {
	return c.ServiceNameVal
}

// Endpoint 返回 OTLP 收集端接入地址（优先 OTEL_EXPORTER_OTLP_TRACES_ENDPOINT，其次 OTEL_EXPORTER_OTLP_ENDPOINT）。
func (c TelemetryConfig) Endpoint() string {
	if c.TracesEndpointVal != "" {
		return c.TracesEndpointVal
	}
	return c.EndpointVal
}

// Insecure 返回是否使用非安全传输协议（优先 OTEL_EXPORTER_OTLP_TRACES_INSECURE，其次 OTEL_EXPORTER_OTLP_INSECURE）。
func (c TelemetryConfig) Insecure() bool {
	if c.TracesInsecureVal != nil {
		return *c.TracesInsecureVal
	}
	return c.InsecureVal
}

// SampleRatio 返回链路采样率。
func (c TelemetryConfig) SampleRatio() float64 {
	return c.SampleRatioVal
}

// ShutdownTimeout 返回遥测客户端关闭超时时间。
func (c TelemetryConfig) ShutdownTimeout() time.Duration {
	return c.ShutdownTimeoutVal
}

// TrustRemote 返回是否信任远端链路追踪上下文。
func (c TelemetryConfig) TrustRemote() bool {
	return c.TrustRemoteVal
}

// 编译期接口实现断言。
var (
	_ telemetry.Config = (*TelemetryConfig)(nil)
	_ telemetry.Config = TelemetryConfig{}
)
