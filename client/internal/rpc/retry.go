package rpc

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"math/rand/v2"
	"net"
	"os"
	"syscall"
	"time"

	"connectrpc.com/connect"

	"github.com/starsrc/bowl/gen/go/storage/v1/storagev1connect"
	"github.com/starsrc/bowl/pkg/rpcerr"
)

const (
	// DefaultMaxElapsedTime 为重试总预算上限，默认为 30s。
	DefaultMaxElapsedTime = 30 * time.Second

	// DefaultInitialInterval 为初始退避等待时长，默认为 100ms。
	DefaultInitialInterval = 100 * time.Millisecond

	// DefaultMultiplier 为指数退避乘数，默认为 2.0。
	DefaultMultiplier = 2.0

	// DefaultMaxInterval 为单次退避上限，默认为 1s。
	DefaultMaxInterval = 1 * time.Second
)

// RetryConfig 定义传输层指数退避重试参数。
type RetryConfig struct {
	// Disable 为 true 时彻底禁用重试拦截器。
	Disable bool

	// MaxElapsedTime 为重试总预算上限（默认 30s）。
	MaxElapsedTime time.Duration

	// InitialInterval 为初始退避等待时长（默认 100ms）。
	InitialInterval time.Duration

	// Multiplier 为指数退避乘数（默认 2.0）。
	Multiplier float64

	// MaxInterval 为单次退避上限（默认 1s）。
	MaxInterval time.Duration

	// RandFloat64 为可选的随机浮点数生成函数（返回 [0.0, 1.0)），用于单测固定抖动。
	// 若为 nil 则使用内置全局随机生成器。
	RandFloat64 func() float64
}

// withDefaults 返回填充默认值后的 RetryConfig 副本。
func (c RetryConfig) withDefaults() RetryConfig {
	if c.MaxElapsedTime <= 0 {
		c.MaxElapsedTime = DefaultMaxElapsedTime
	}
	if c.InitialInterval <= 0 {
		c.InitialInterval = DefaultInitialInterval
	}
	if c.Multiplier <= 0 {
		c.Multiplier = DefaultMultiplier
	}
	if c.MaxInterval <= 0 {
		c.MaxInterval = DefaultMaxInterval
	}
	return c
}

// idempotentProcedures 记录 17 个只读与幂等 RPC 白名单集合。
var idempotentProcedures = map[string]struct{}{
	storagev1connect.StorageServiceGetDiskProcedure:        {},
	storagev1connect.StorageServiceListDisksProcedure:      {},
	storagev1connect.StorageServiceDiffDiskProcedure:       {},
	storagev1connect.StorageServiceReadCommitNodeProcedure: {},
	storagev1connect.StorageServiceRenewMountProcedure:     {},
	storagev1connect.StorageServiceUnmountDiskProcedure:    {},
	storagev1connect.StorageServiceLookupProcedure:         {},
	storagev1connect.StorageServiceGetAttrProcedure:        {},
	storagev1connect.StorageServiceReadlinkProcedure:       {},
	storagev1connect.StorageServiceReadProcedure:           {},
	storagev1connect.StorageServiceFsyncProcedure:          {},
	storagev1connect.StorageServiceFsyncDirProcedure:       {},
	storagev1connect.StorageServiceReadDirProcedure:        {},
	storagev1connect.StorageServiceReadDirPlusProcedure:    {},
	storagev1connect.StorageServiceGetXattrProcedure:       {},
	storagev1connect.StorageServiceListXattrProcedure:      {},
	storagev1connect.StorageServiceStatFSProcedure:         {},
}

// IsIdempotentProcedure 判定指定 procedure 是否属于 17 个只读与幂等 RPC 白名单。
func IsIdempotentProcedure(procedure string) bool {
	_, ok := idempotentProcedures[procedure]
	return ok
}

// hasErrorInfo 检查错误是否解包出 Domain == rpcerr.Domain 且 Reason 非空的业务 ErrorInfo。
// 任何携带服务端业务 ErrorInfo 的错误表明服务端已明确处理并拒绝，必须绝不重试。
func hasErrorInfo(err error) bool {
	_, ok := rpcerr.ExtractReason(err)
	return ok
}

// isNeverRetryCode 检查 Connect 错误状态码是否属于绝不重试的状态码。
func isNeverRetryCode(code connect.Code) bool {
	switch code {
	case connect.CodeInvalidArgument,
		connect.CodeNotFound,
		connect.CodeAlreadyExists,
		connect.CodePermissionDenied,
		connect.CodeFailedPrecondition,
		connect.CodeResourceExhausted,
		connect.CodeAborted,
		connect.CodeUnimplemented:
		return true
	default:
		return false
	}
}

// isDialError 判定错误是否明确属于请求尚未送达服务端的建连阶段失败（如 ECONNREFUSED 或 dial 失败）。
func isDialError(err error) bool {
	if err == nil {
		return false
	}

	if errors.Is(err, syscall.ECONNREFUSED) {
		return true
	}

	if opErr, ok := errors.AsType[*net.OpError](err); ok {
		if opErr.Op == "dial" {
			return true
		}
	}

	if sysErr, ok := errors.AsType[*os.SyscallError](err); ok {
		if sysErr.Syscall == "connect" {
			return true
		}
	}

	return false
}

// isTransportError 判定错误是否为底层网络传输错误（连接重置、意外 EOF、broken pipe 等）。
func isTransportError(err error) bool {
	if err == nil {
		return false
	}

	if errors.Is(err, syscall.ECONNRESET) ||
		errors.Is(err, io.EOF) ||
		errors.Is(err, io.ErrUnexpectedEOF) ||
		errors.Is(err, syscall.EPIPE) {
		return true
	}

	var opErr *net.OpError
	return errors.As(err, &opErr)
}

// shouldRetry 根据 procedure 与捕获的错误决定是否允许重试。
func shouldRetry(procedure string, err error) bool {
	if err == nil {
		return false
	}

	// 1. 任何携带业务 ErrorInfo 的错误，绝不重试
	if hasErrorInfo(err) {
		return false
	}

	// 2. 特定客户端或业务状态码，绝不重试
	var connectErr *connect.Error
	if errors.As(err, &connectErr) {
		if isNeverRetryCode(connectErr.Code()) {
			return false
		}
	}

	// 3. 建连阶段失败（ECONNREFUSED / dial error）：请求尚未送达服务端，幂等与非幂等均允许重试
	if isDialError(err) {
		return true
	}

	// 4. 针对 15 个只读与幂等 RPC 白名单：
	// 在遇到 CodeUnavailable（且未携带 ErrorInfo）或底层传输错误时允许重试
	if IsIdempotentProcedure(procedure) {
		if connectErr != nil && connectErr.Code() == connect.CodeUnavailable {
			return true
		}
		if isTransportError(err) {
			return true
		}
	}

	// 5. 其余情况（包含非幂等 RPC 在建连已完成后发生的中断/CodeUnavailable 等）严禁重试
	return false
}

// computeJitteredInterval 计算带随机抖动的退避等待时长（区间为 [0.5 * interval, 1.0 * interval]）。
func computeJitteredInterval(interval time.Duration, randFn func() float64) time.Duration {
	var factor float64
	if randFn != nil {
		factor = 0.5 + randFn()*0.5
	} else {
		factor = 0.5 + rand.Float64()*0.5
	}
	jittered := time.Duration(float64(interval) * factor)
	if jittered <= 0 {
		return interval
	}
	return jittered
}

// wrapBudgetExceededError 将重试总预算耗尽的底层错误统一包装为 CodeDeadlineExceeded（若未携带业务 ErrorInfo），
// 确保预算耗尽出口恒映射为 syscall.EIO。
func wrapBudgetExceededError(err error) error {
	if err == nil {
		return connect.NewError(connect.CodeDeadlineExceeded, context.DeadlineExceeded)
	}
	if hasErrorInfo(err) {
		return err
	}
	var connectErr *connect.Error
	if errors.As(err, &connectErr) && connectErr.Code() == connect.CodeDeadlineExceeded {
		return err
	}
	return connect.NewError(connect.CodeDeadlineExceeded, err)
}

type rpcTimer interface {
	Chan() <-chan time.Time
	Stop() bool
}

type rpcClock interface {
	Now() time.Time
	Since(t time.Time) time.Duration
	NewTimer(d time.Duration) rpcTimer
	WithDeadline(parent context.Context, deadline time.Time) (context.Context, context.CancelFunc)
}

type stdTimer struct {
	*time.Timer
}

func (t stdTimer) Chan() <-chan time.Time {
	return t.C
}

type realClock struct{}

func (realClock) Now() time.Time {
	return time.Now()
}

func (realClock) Since(t time.Time) time.Duration {
	return time.Since(t)
}

func (realClock) NewTimer(d time.Duration) rpcTimer {
	return stdTimer{Timer: time.NewTimer(d)}
}

func (realClock) WithDeadline(parent context.Context, deadline time.Time) (context.Context, context.CancelFunc) {
	return context.WithDeadline(parent, deadline)
}

// NewRetryInterceptor 创建遵循有界指数退避策略的 ConnectRPC 客户端一元拦截器。
func NewRetryInterceptor(cfg RetryConfig) connect.UnaryInterceptorFunc {
	return newRetryInterceptor(cfg, nil, nil, realClock{})
}

func newRetryInterceptor(cfg RetryConfig, logger *slog.Logger, metrics *Metrics, clk rpcClock) connect.UnaryInterceptorFunc {
	if clk == nil {
		clk = realClock{}
	}
	config := cfg.withDefaults()
	retryWarnLimiter := newLogRateLimiter(defaultLogRateLimitInterval)

	return func(next connect.UnaryFunc) connect.UnaryFunc {
		return func(ctx context.Context, req connect.AnyRequest) (resp connect.AnyResponse, retErr error) {
			procedure := req.Spec().Procedure
			startTime := clk.Now()
			if metrics != nil {
				defer func() {
					code := "ok"
					if retErr != nil {
						code = connect.CodeOf(retErr).String()
					}
					metrics.observeLogicalDuration(procedure, code, clk.Since(startTime))
				}()
			}

			if config.Disable {
				return next(ctx, req)
			}

			ctx, reqID := ensureRequestID(ctx)
			interval := config.InitialInterval
			var (
				lastErr          error
				lastRetryableErr error
			)
			attempts := 0
			retryCount := 0

			exhaustBudget := func(cause error) error {
				wrapped := wrapBudgetExceededError(cause)
				if !hasErrorInfo(cause) {
					metrics.incRetryExhausted(procedure)
					if logger != nil {
						traceID := extractTraceID(ctx, req, cause)
						errMsg := wrapped.Error()
						if cause != nil {
							errMsg = cause.Error()
						}
						logger.LogAttrs(
							context.Background(),
							slog.LevelError,
							"rpc_retry_exhausted_eio",
							slog.String("procedure", procedure),
							slog.Int("attempts", attempts),
							slog.Int64("elapsed_ms", clk.Since(startTime).Milliseconds()),
							slog.Uint64("req_id", reqID),
							slog.String("trace_id", traceID),
							slog.String("err", errMsg),
						)
					}
				}
				return wrapped
			}

			attemptCtx := ctx
			if config.MaxElapsedTime > 0 {
				var cancel context.CancelFunc
				attemptCtx, cancel = clk.WithDeadline(ctx, startTime.Add(config.MaxElapsedTime))
				defer cancel()
			}

			for {
				if err := ctx.Err(); err != nil {
					return nil, err
				}
				if err := attemptCtx.Err(); err != nil {
					if lastRetryableErr != nil {
						return nil, exhaustBudget(lastRetryableErr)
					}
					if lastErr != nil {
						return nil, exhaustBudget(lastErr)
					}
					return nil, exhaustBudget(err)
				}

				attempts++
				resp, err := next(attemptCtx, req)
				if err == nil {
					return resp, nil
				}

				lastErr = err

				// 若父上下文已取消或超时，直接返回原始错误
				if ctx.Err() != nil {
					return nil, err
				}

				// 若父上下文未取消但重试总预算上下文已耗尽，停止重试并返回错误（映射为 EIO）；
				// 若本次尝试因预算到期被本地截断且此前已有真实可重试故障，保留真实故障作为根因以便排障留痕。
				if attemptCtx.Err() != nil {
					if !hasErrorInfo(err) && lastRetryableErr != nil {
						return nil, exhaustBudget(lastRetryableErr)
					}
					return nil, exhaustBudget(err)
				}

				// 若不满足重试条件，立即终止并返回原始错误
				if !shouldRetry(procedure, err) {
					return nil, err
				}
				lastRetryableErr = err

				// 计算已过去的总耗时，若超出总预算则终止重试
				elapsed := clk.Since(startTime)
				if elapsed >= config.MaxElapsedTime {
					return nil, exhaustBudget(lastErr)
				}

				sleepDuration := computeJitteredInterval(interval, config.RandFloat64)
				remaining := config.MaxElapsedTime - elapsed
				if sleepDuration > remaining {
					sleepDuration = remaining
				}

				retryCount++
				metrics.incRetry(procedure)
				if retryCount == 1 && logger != nil && retryWarnLimiter.allow(clk.Now()) {
					traceID := extractTraceID(ctx, req, err)
					logger.LogAttrs(
						context.Background(),
						slog.LevelWarn,
						"rpc_retry_triggered",
						slog.String("procedure", procedure),
						slog.Int("attempt", attempts),
						slog.Int64("backoff_ms", sleepDuration.Milliseconds()),
						slog.Uint64("req_id", reqID),
						slog.String("trace_id", traceID),
						slog.String("err", err.Error()),
					)
				}

				timer := clk.NewTimer(sleepDuration)
				select {
				case <-ctx.Done():
					timer.Stop()
					return nil, ctx.Err()
				case <-timer.Chan():
				}

				// 指数退避增长并钳位在 MaxInterval
				interval = min(time.Duration(float64(interval)*config.Multiplier), config.MaxInterval)
			}
		}
	}
}
