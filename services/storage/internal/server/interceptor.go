package server

import (
	"fmt"
	"log/slog"

	"connectrpc.com/connect"

	"github.com/starsrc/Aether/pkg/observability/slogx"
	"github.com/starsrc/Aether/pkg/observability/telemetry"
	"github.com/starsrc/Aether/pkg/serverx"
	"github.com/starsrc/bowl/pkg/validator"
)

// telemetryObsAdapter 适配器，将 telemetry.Config 桥接为 serverx.ObservabilityConfig。
type telemetryObsAdapter struct {
	telemetry.Config
	slogx.LogConfig
}

// toObservabilityConfig 将 telemetry.Config 转换为 serverx.ObservabilityConfig 契约。
func toObservabilityConfig(cfg telemetry.Config) serverx.ObservabilityConfig {
	if cfg == nil {
		return nil
	}
	if oc, ok := cfg.(serverx.ObservabilityConfig); ok {
		return oc
	}
	return telemetryObsAdapter{
		Config: cfg,
		LogConfig: slogx.LogConfig{
			LogLevel:          "info",
			LogFormat:         "json",
			LogConnectMessage: "handled rpc",
		},
	}
}

// BuildInterceptors 委托 Aether/pkg/serverx 构建标准 ConnectRPC 拦截器链，并注入基于 pkg/validator 的校验器置于最内层。
// 洋葱模型顺序严格保证：
// 1. 最外层脱敏 (slogx.NewMaskInterceptor)：捕获内部未处理错误，脱敏敏感信息同时保留 ErrorInfo。
// 2. 次外层链路追踪 (otelconnect.NewInterceptor)：捕获原始根因并记录追踪 Span。
// 3. 中间层访问日志 (slogx.NewConnectInterceptor)：结构化输出 RPC 访问日志。
// 4. 最内层请求校验 (NewValidationInterceptor)：基于 pkg/validator 拦截非法入参并附加 POSIX ErrorInfo。
func BuildInterceptors(
	telemetryCfg telemetry.Config,
	logInterceptor connect.UnaryInterceptorFunc,
	logger *slog.Logger,
) ([]connect.Interceptor, error) {
	val, err := validator.NewStorageValidator()
	if err != nil {
		return nil, fmt.Errorf("create storage validator: %w", err)
	}
	valInterceptor := NewValidationInterceptor(val)

	var extra []connect.Interceptor
	if logInterceptor != nil {
		extra = append(extra, logInterceptor)
	}

	return serverx.NewInterceptorsWithValidator(
		logger,
		toObservabilityConfig(telemetryCfg),
		valInterceptor,
		extra...,
	)
}

func buildInterceptors(
	telemetryCfg telemetry.Config,
	logInterceptor connect.UnaryInterceptorFunc,
	logger *slog.Logger,
) ([]connect.Interceptor, error) {
	return BuildInterceptors(telemetryCfg, logInterceptor, logger)
}
