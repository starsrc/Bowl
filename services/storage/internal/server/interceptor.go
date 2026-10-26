package server

import (
	"fmt"
	"log/slog"

	"connectrpc.com/connect"
	"connectrpc.com/otelconnect"

	"github.com/starsrc/Aether/pkg/observability/slogx"
	"github.com/starsrc/Aether/pkg/observability/telemetry"
	"github.com/starsrc/bowl/pkg/validator"
)

// BuildInterceptors 按洋葱模型组装 ConnectRPC 拦截器链：
// 1. 最外层脱敏 (slogx.NewMaskInterceptor)：捕获内部未处理错误，脱敏敏感信息同时保留 ErrorInfo。
// 2. 次外层链路追踪 (otelconnect.NewInterceptor)：捕获原始根因并记录追踪 Span。
// 3. 中间层访问日志 (slogx.NewConnectInterceptor)：结构化输出 RPC 访问日志。
// 4. 最内层请求校验 (NewValidationInterceptor)：基于 pkg/validator 拦截非法入参并附加 POSIX ErrorInfo。
func BuildInterceptors(
	telemetryCfg telemetry.Config,
	logInterceptor connect.UnaryInterceptorFunc,
	logger *slog.Logger,
) ([]connect.Interceptor, error) {
	otelOpts := []otelconnect.Option{
		otelconnect.WithoutMetrics(),
	}
	if telemetryCfg != nil && telemetryCfg.TrustRemote() {
		otelOpts = append(otelOpts, otelconnect.WithTrustRemote())
	}

	otelInterceptor, err := otelconnect.NewInterceptor(otelOpts...)
	if err != nil {
		return nil, fmt.Errorf("create otelconnect interceptor: %w", err)
	}

	val, err := validator.NewStorageValidator()
	if err != nil {
		return nil, fmt.Errorf("create storage validator: %w", err)
	}
	valInterceptor := NewValidationInterceptor(val)

	interceptors := make([]connect.Interceptor, 0, 4)

	// 1. 最外层脱敏：无条件挂载（slogx.NewMaskInterceptor 原生支持 nil logger）
	interceptors = append(interceptors, slogx.NewMaskInterceptor(logger))

	// 2. 链路追踪
	interceptors = append(interceptors, otelInterceptor)

	// 3. 访问日志：优先使用显式注入的 logInterceptor，其次当 logger != nil 时构造
	if logInterceptor != nil {
		interceptors = append(interceptors, logInterceptor)
	} else if logger != nil {
		interceptors = append(interceptors, slogx.NewConnectInterceptor(nil, logger))
	}

	// 4. 最内层请求校验
	interceptors = append(interceptors, valInterceptor)

	return interceptors, nil
}

func buildInterceptors(
	telemetryCfg telemetry.Config,
	logInterceptor connect.UnaryInterceptorFunc,
	logger *slog.Logger,
) ([]connect.Interceptor, error) {
	return BuildInterceptors(telemetryCfg, logInterceptor, logger)
}
