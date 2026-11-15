package rpc

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"log/slog"
	"math"
	"net/http"
	"strconv"
	"sync/atomic"
	"time"

	"connectrpc.com/connect"
	"github.com/prometheus/client_golang/prometheus"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"

	"github.com/starsrc/bowl/pkg/rpcerr"
)

const (
	// HeaderRequestID 为跨进程透传单调请求关联 ID 的 HTTP Header 名称。
	HeaderRequestID = "X-Request-Id"

	// DefaultSlowRPCThreshold 为单次客户端 RPC 慢调用告警阈值（默认 1s）。
	DefaultSlowRPCThreshold = time.Second

	// defaultLogRateLimitInterval 为异常告警结构化日志的默认限频窗口（1 次/秒）。
	defaultLogRateLimitInterval = time.Second
)

type requestIDContextKey struct{}

var fallbackRequestIDSeq atomic.Uint64

func init() {
	var b [8]byte
	if _, err := rand.Read(b[:]); err == nil {
		// 清零最高位并保留低 8 位步进余量，既保证跨短命 CLI 进程高熵唯一性又规避有符号 64 位溢出。
		seed := binary.LittleEndian.Uint64(b[:]) & 0x3fffffffffffff00
		if seed == 0 {
			seed = 1 << 20
		}
		fallbackRequestIDSeq.Store(seed)
		return
	}
	fallbackRequestIDSeq.Store(1 << 20)
}

// WithRequestID 将非零请求关联 ID 注入 context.Context（供上游如 fusefs.RawFS.RPCContext 传入 NextRequestID()）。
// 若 reqID == 0，保持原上下文不变（出站拦截器将自动使用包内原子序列兜底生成）。
func WithRequestID(ctx context.Context, reqID uint64) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	if reqID == 0 {
		return ctx
	}
	return context.WithValue(ctx, requestIDContextKey{}, reqID)
}

// RequestIDFromContext 从 context.Context 提取已注入的非零请求关联 ID。
func RequestIDFromContext(ctx context.Context) (uint64, bool) {
	if ctx == nil {
		return 0, false
	}
	reqID, ok := ctx.Value(requestIDContextKey{}).(uint64)
	if !ok || reqID == 0 {
		return 0, false
	}
	return reqID, true
}

func nextFallbackRequestID() uint64 {
	for {
		id := fallbackRequestIDSeq.Add(1) & math.MaxInt64
		if id != 0 {
			return id
		}
	}
}

func ensureRequestID(ctx context.Context) (outCtx context.Context, reqID uint64) {
	if existingID, ok := RequestIDFromContext(ctx); ok {
		return ctx, existingID
	}
	reqID = nextFallbackRequestID()
	return WithRequestID(ctx, reqID), reqID
}

func writeRequestIDHeader(header http.Header, reqID uint64) {
	var buf [20]byte
	header.Set(HeaderRequestID, string(strconv.AppendUint(buf[:0], reqID, 10)))
}

// SubMillisecondDurationBuckets 定义客户端与服务端统一的 18 个亚毫秒至 10s 延迟直方图桶边界（25µs ~ 10s）。
var SubMillisecondDurationBuckets = []float64{
	0.000025, 0.00005, 0.0001, 0.00025, 0.0005,
	0.001, 0.0025, 0.005, 0.01, 0.025, 0.05,
	0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0,
}

// Metrics 封装客户端 RPC 传输层的自定义 Prometheus 指标收集器。
// 实例级隔离（不触碰全局 DefaultRegisterer），且全部方法对 nil 接收者安全。
type Metrics struct {
	retries         *prometheus.CounterVec
	retryExhausted  *prometheus.CounterVec
	staleEpoch      prometheus.Counter
	logicalDuration *prometheus.HistogramVec
}

var _ prometheus.Collector = (*Metrics)(nil)

// NewMetrics 创建全新的隔离客户端 RPC 指标收集器实例。
func NewMetrics() *Metrics {
	return &Metrics{
		retries: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "bowl_client_rpc_retries_total",
				Help: "Total number of client RPC backoff retries triggered.",
			},
			[]string{"procedure"},
		),
		retryExhausted: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "bowl_client_rpc_retry_exhausted_total",
				Help: "Total number of client RPC calls that exhausted retry budget and mapped to EIO.",
			},
			[]string{"procedure"},
		),
		staleEpoch: prometheus.NewCounter(
			prometheus.CounterOpts{
				Name: "bowl_client_stale_epoch_total",
				Help: "Total number of client RPC calls fenced by server due to stale writer epoch (EBUSY).",
			},
		),
		logicalDuration: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Name:    "bowl_client_rpc_logical_duration_seconds",
				Help:    "Logical end-to-end client RPC call duration in seconds, including retry attempts and backoff sleeps.",
				Buckets: SubMillisecondDurationBuckets,
			},
			[]string{"procedure", "code"},
		),
	}
}

// Describe 实现 prometheus.Collector 接口；对 nil 接收者安全。
func (m *Metrics) Describe(ch chan<- *prometheus.Desc) {
	if m == nil {
		return
	}
	if m.retries != nil {
		m.retries.Describe(ch)
	}
	if m.retryExhausted != nil {
		m.retryExhausted.Describe(ch)
	}
	if m.staleEpoch != nil {
		m.staleEpoch.Describe(ch)
	}
	if m.logicalDuration != nil {
		m.logicalDuration.Describe(ch)
	}
}

// Collect 实现 prometheus.Collector 接口；对 nil 接收者安全。
func (m *Metrics) Collect(ch chan<- prometheus.Metric) {
	if m == nil {
		return
	}
	if m.retries != nil {
		m.retries.Collect(ch)
	}
	if m.retryExhausted != nil {
		m.retryExhausted.Collect(ch)
	}
	if m.staleEpoch != nil {
		m.staleEpoch.Collect(ch)
	}
	if m.logicalDuration != nil {
		m.logicalDuration.Collect(ch)
	}
}

func (m *Metrics) incRetry(procedure string) {
	if m == nil || m.retries == nil {
		return
	}
	m.retries.WithLabelValues(procedure).Inc()
}

func (m *Metrics) incRetryExhausted(procedure string) {
	if m == nil || m.retryExhausted == nil {
		return
	}
	m.retryExhausted.WithLabelValues(procedure).Inc()
}

func (m *Metrics) incStaleEpoch() {
	if m == nil || m.staleEpoch == nil {
		return
	}
	m.staleEpoch.Inc()
}

func (m *Metrics) observeLogicalDuration(procedure, code string, d time.Duration) {
	if m == nil || m.logicalDuration == nil {
		return
	}
	m.logicalDuration.WithLabelValues(procedure, code).Observe(d.Seconds())
}

// logRateLimiter 提供实例级无锁 CAS 原子限频器。
type logRateLimiter struct {
	intervalNs int64
	lastUnixNs atomic.Int64
}

func newLogRateLimiter(interval time.Duration) *logRateLimiter {
	return &logRateLimiter{intervalNs: interval.Nanoseconds()}
}

func (l *logRateLimiter) allow(now time.Time) bool {
	if l == nil || l.intervalNs <= 0 {
		return true
	}
	nowNs := now.UnixNano()
	for {
		last := l.lastUnixNs.Load()
		if last != 0 && nowNs-last < l.intervalNs {
			return false
		}
		if l.lastUnixNs.CompareAndSwap(last, nowNs) {
			return true
		}
	}
}

func isStaleEpochError(err error) bool {
	reason, ok := rpcerr.ExtractReason(err)
	return ok && reason == rpcerr.ReasonBusy
}

func extractRequestEpoch(req connect.AnyRequest) uint64 {
	if req == nil {
		return 0
	}
	if eg, ok := req.Any().(interface{ GetEpoch() uint64 }); ok {
		return eg.GetEpoch()
	}
	return 0
}

var w3cTracePropagator = propagation.TraceContext{}

// extractTraceID 依次从活跃上下文、出站请求头（otelconnect 注入）或服务端错误响应头中提取 W3C TraceID。
func extractTraceID(ctx context.Context, req connect.AnyRequest, err error) string {
	if ctx != nil {
		if sc := trace.SpanContextFromContext(ctx); sc.IsValid() {
			return sc.TraceID().String()
		}
	}
	if req != nil {
		extracted := w3cTracePropagator.Extract(context.Background(), propagation.HeaderCarrier(req.Header()))
		if sc := trace.SpanContextFromContext(extracted); sc.IsValid() {
			return sc.TraceID().String()
		}
	}
	if err != nil {
		if connectErr, ok := errors.AsType[*connect.Error](err); ok {
			extracted := w3cTracePropagator.Extract(context.Background(), propagation.HeaderCarrier(connectErr.Meta()))
			if sc := trace.SpanContextFromContext(extracted); sc.IsValid() {
				return sc.TraceID().String()
			}
		}
	}
	return ""
}

// newRequestObservabilityInterceptor 构建第 2 层洋葱拦截器（位于 Retry 内层、otelconnect 外层）：
// 1. 确保 100% 出站请求通过栈缓冲写入 X-Request-Id；
// 2. 识别服务端旧 Epoch 围栏错误，即 Domain 为 rpcerr.Domain 且 Reason 为 rpcerr.ReasonBusy，递增 stale_epoch_total 并限频输出 writer_epoch_fenced；
// 3. 单次尝试耗时超过 slowThreshold（默认 1s）时输出 slow_rpc 结构化告警。
func newRequestObservabilityInterceptor(
	logger *slog.Logger,
	metrics *Metrics,
	slowThreshold time.Duration,
	clk rpcClock,
) connect.UnaryInterceptorFunc {
	if clk == nil {
		clk = realClock{}
	}
	if slowThreshold <= 0 {
		slowThreshold = DefaultSlowRPCThreshold
	}
	epochFenceLimiter := newLogRateLimiter(defaultLogRateLimitInterval)

	return func(next connect.UnaryFunc) connect.UnaryFunc {
		return func(ctx context.Context, req connect.AnyRequest) (connect.AnyResponse, error) {
			ctx, reqID := ensureRequestID(ctx)
			writeRequestIDHeader(req.Header(), reqID)

			start := clk.Now()
			resp, err := next(ctx, req)
			duration := clk.Since(start)

			if err != nil && isStaleEpochError(err) {
				metrics.incStaleEpoch()
				if logger != nil && epochFenceLimiter.allow(clk.Now()) {
					traceID := extractTraceID(ctx, req, err)
					logger.LogAttrs(
						context.Background(),
						slog.LevelError,
						"writer_epoch_fenced",
						slog.String("procedure", req.Spec().Procedure),
						slog.Uint64("local_epoch", extractRequestEpoch(req)),
						slog.Uint64("req_id", reqID),
						slog.String("trace_id", traceID),
						slog.String("err", err.Error()),
					)
				}
			}

			if logger != nil && duration > slowThreshold {
				traceID := extractTraceID(ctx, req, err)
				code := "ok"
				if err != nil {
					code = connect.CodeOf(err).String()
				}
				attrs := make([]slog.Attr, 0, 6)
				attrs = append(
					attrs,
					slog.String("procedure", req.Spec().Procedure),
					slog.Int64("duration_ms", duration.Milliseconds()),
					slog.String("code", code),
					slog.Uint64("req_id", reqID),
					slog.String("trace_id", traceID),
				)
				if err != nil {
					attrs = append(attrs, slog.String("err", err.Error()))
				}
				logger.LogAttrs(context.Background(), slog.LevelWarn, "slow_rpc", attrs...)
			}

			return resp, err
		}
	}
}
