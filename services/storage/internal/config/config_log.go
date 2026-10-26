package config

import (
	"log/slog"
	"strings"

	"github.com/starsrc/Aether/pkg/observability/slogx"
)

// LogConfig 承载日志全局配置。
type LogConfig struct {
	LevelVal  string `env:"LOG_LEVEL" envDefault:"info"`
	FormatVal string `env:"LOG_FORMAT" envDefault:"json"`
	LogOtelConfig
	LogConnectConfig
}

// Level 返回日志级别原始字符串（如 debug, info, warn, error）。
func (c LogConfig) Level() string {
	return c.LevelVal
}

// Format 返回日志格式字符串（如 json, text）。
func (c LogConfig) Format() string {
	return c.FormatVal
}

// SlogLevel 将配置的日志级别转换为 slog.Level。
// 支持 "debug", "info", "warn", "warning", "error"（不区分大小写），未知级别默认回退至 slog.LevelInfo。
func (c LogConfig) SlogLevel() slog.Level {
	switch strings.ToLower(strings.TrimSpace(c.LevelVal)) {
	case "debug":
		return slog.LevelDebug
	case "warn", "warning":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}

// LogConnectConfig 承载 slogx RPC 访问日志配置。
type LogConnectConfig struct {
	MessageVal string `env:"LOG_CONNECT_MESSAGE" envDefault:"handled rpc"`
}

// Message 返回 RPC 请求处理完成时的日志消息文本。
func (c LogConnectConfig) Message() string {
	return c.MessageVal
}

// LogOtelConfig 承载 slogx 链路日志注入配置。
type LogOtelConfig struct {
	RecordTraceFlagsVal bool `env:"LOG_OTEL_RECORD_TRACE_FLAGS" envDefault:"false"`
	SampledOnlyVal      bool `env:"LOG_OTEL_SAMPLED_ONLY" envDefault:"false"`
}

// RecordTraceFlags 返回是否在日志中记录 trace_flags。
func (c LogOtelConfig) RecordTraceFlags() bool {
	return c.RecordTraceFlagsVal
}

// SampledOnly 返回是否仅对已采样 Span 注入追踪属性。
func (c LogOtelConfig) SampledOnly() bool {
	return c.SampledOnlyVal
}

// 编译期接口实现断言。
var (
	_ slogx.Config = LogConfig{}
	_ slogx.Config = (*LogConfig)(nil)
)
