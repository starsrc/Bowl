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
	"github.com/starsrc/pkg/serverx"

	"github.com/starsrc/bowl/gen/go/storage/v1/storagev1connect"
)

const (
	DefaultMaxElapsedTime = 30 * time.Second

	DefaultInitialInterval = 100 * time.Millisecond

	DefaultMultiplier = 2.0

	DefaultMaxInterval = 1 * time.Second
)

type RetryConfig struct {
	Disable bool

	MaxElapsedTime time.Duration

	InitialInterval time.Duration

	Multiplier float64

	MaxInterval time.Duration

	RandFloat64 func() float64
}

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

var idempotentProcedures = map[string]struct{}{
	storagev1connect.StorageServiceGetDiskProcedure:        {},
	storagev1connect.StorageServiceListDisksProcedure:      {},
	storagev1connect.StorageServiceDiffDiskProcedure:       {},
	storagev1connect.StorageServiceReadCommitNodeProcedure: {},
	storagev1connect.StorageServiceRenewMountProcedure:     {},
	storagev1connect.StorageServiceUnmountDiskProcedure:    {},
	storagev1connect.StorageServiceListMountsProcedure:     {},
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

func IsIdempotentProcedure(procedure string) bool {
	_, ok := idempotentProcedures[procedure]
	return ok
}

func hasErrorInfo(err error) bool {
	info := serverx.ExtractErrorInfo(err)
	return info != nil && info.Reason != ""
}

func isNeverRetryCode(code connect.Code) bool {
	switch code {
	case connect.CodeInvalidArgument,
		connect.CodeNotFound,
		connect.CodeAlreadyExists,
		connect.CodePermissionDenied,
		connect.CodeUnauthenticated,
		connect.CodeFailedPrecondition,
		connect.CodeResourceExhausted,
		connect.CodeAborted,
		connect.CodeUnimplemented:
		return true
	default:
		return false
	}
}

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

func shouldRetry(procedure string, err error) bool {
	if err == nil {
		return false
	}

	if hasErrorInfo(err) {
		return false
	}

	var connectErr *connect.Error
	if errors.As(err, &connectErr) {
		if isNeverRetryCode(connectErr.Code()) {
			return false
		}
	}

	if isDialError(err) {
		return true
	}

	if IsIdempotentProcedure(procedure) {
		if connectErr != nil && connectErr.Code() == connect.CodeUnavailable {
			return true
		}
		if isTransportError(err) {
			return true
		}
	}

	return false
}

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

func rpcStatusCode(err error) string {
	if connectErr, ok := errors.AsType[*connect.Error](err); ok {
		return connectErr.Code().String()
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return connect.CodeDeadlineExceeded.String()
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, ErrKernelInterrupted) {
		return connect.CodeCanceled.String()
	}
	return connect.CodeOf(err).String()
}

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
						code = rpcStatusCode(retErr)
					}
					metrics.observeLogicalDuration(procedure, code, clk.Since(startTime))
				}()
			}

			if config.Disable {
				return next(ctx, req)
			}

			ctx, reqID := ensureRequestID(ctx)
			interval := config.InitialInterval
			var lastRetryableErr error
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

			callerDeadline, hasCallerDeadline := ctx.Deadline()
			attemptCtx := ctx
			var (
				effectiveDeadline time.Time
				hasDeadline       bool
			)
			if hasCallerDeadline {
				effectiveDeadline = callerDeadline
				hasDeadline = true
			} else if config.MaxElapsedTime > 0 {
				effectiveDeadline = startTime.Add(config.MaxElapsedTime)
				hasDeadline = true
				var cancel context.CancelFunc
				attemptCtx, cancel = clk.WithDeadline(ctx, effectiveDeadline)
				defer cancel()
			}

			for {
				if err := ctx.Err(); err != nil {
					if errors.Is(err, context.DeadlineExceeded) && lastRetryableErr != nil {
						return nil, exhaustBudget(lastRetryableErr)
					}
					return nil, err
				}
				if err := attemptCtx.Err(); err != nil {
					if errors.Is(err, context.DeadlineExceeded) && lastRetryableErr != nil {
						return nil, exhaustBudget(lastRetryableErr)
					}
					return nil, err
				}

				attempts++
				resp, err := next(attemptCtx, req)
				if err == nil {
					return resp, nil
				}

				retryable := shouldRetry(procedure, err)
				if retryable {
					lastRetryableErr = err
				}

				if ctxErr := ctx.Err(); ctxErr != nil && !errors.Is(ctxErr, context.DeadlineExceeded) {
					return nil, err
				}

				if errors.Is(ctx.Err(), context.DeadlineExceeded) || errors.Is(attemptCtx.Err(), context.DeadlineExceeded) {
					if !hasErrorInfo(err) && lastRetryableErr != nil {
						return nil, exhaustBudget(lastRetryableErr)
					}
					return nil, err
				}

				if !retryable {
					return nil, err
				}

				sleepDuration := computeJitteredInterval(interval, config.RandFloat64)
				if hasDeadline {
					remaining := effectiveDeadline.Sub(clk.Now())
					if remaining <= 0 || sleepDuration >= remaining {
						return nil, exhaustBudget(lastRetryableErr)
					}
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
					if errors.Is(ctx.Err(), context.DeadlineExceeded) {
						return nil, exhaustBudget(lastRetryableErr)
					}
					return nil, ctx.Err()
				case <-attemptCtx.Done():
					timer.Stop()
					if ctxErr := ctx.Err(); ctxErr != nil && !errors.Is(ctxErr, context.DeadlineExceeded) {
						return nil, ctxErr
					}
					if errors.Is(attemptCtx.Err(), context.DeadlineExceeded) {
						return nil, exhaustBudget(lastRetryableErr)
					}
					return nil, attemptCtx.Err()
				case <-timer.Chan():
				}

				interval = min(time.Duration(float64(interval)*config.Multiplier), config.MaxInterval)
			}
		}
	}
}
