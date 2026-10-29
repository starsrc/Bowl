package rpc

import (
	"context"
	"errors"
	"io"
	"math/rand"
	"net"
	"os"
	"syscall"
	"time"

	"connectrpc.com/connect"
	"github.com/starsrc/bowl/gen/go/storage/v1/storagev1connect"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
)

const (
	// DefaultMaxElapsedTime 为重试总预算上限，默认为 5s。
	DefaultMaxElapsedTime = 5 * time.Second

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

	// MaxElapsedTime 为重试总预算上限（默认 5s）。
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

// idempotentProcedures 记录 16 个只读与幂等 RPC 白名单集合。
var idempotentProcedures = map[string]struct{}{
	storagev1connect.StorageServiceGetDiskProcedure:      {},
	storagev1connect.StorageServiceListDisksProcedure:    {},
	storagev1connect.StorageServiceLookupProcedure:       {},
	storagev1connect.StorageServiceGetAttrProcedure:      {},
	storagev1connect.StorageServiceReadlinkProcedure:     {},
	storagev1connect.StorageServiceReadProcedure:         {},
	storagev1connect.StorageServiceFsyncProcedure:        {},
	storagev1connect.StorageServiceFsyncDirProcedure:     {},
	storagev1connect.StorageServiceReadDirProcedure:      {},
	storagev1connect.StorageServiceReadDirPlusProcedure:  {},
	storagev1connect.StorageServiceGetXattrProcedure:     {},
	storagev1connect.StorageServiceListXattrProcedure:    {},
	storagev1connect.StorageServiceGetLkProcedure:        {},
	storagev1connect.StorageServiceStatFSProcedure:       {},
	storagev1connect.StorageServiceRenewSessionProcedure: {},
	storagev1connect.StorageServiceCloseSessionProcedure: {},
}

// IsIdempotentProcedure 判定指定 procedure 是否属于 16 个只读与幂等 RPC 白名单。
func IsIdempotentProcedure(procedure string) bool {
	_, ok := idempotentProcedures[procedure]
	return ok
}

// hasErrorInfo 检查错误是否解包出非空业务 ErrorInfo。
// 任何携带业务 ErrorInfo 的错误表明服务端已明确处理并拒绝，必须绝不重试。
func hasErrorInfo(err error) bool {
	var connectErr *connect.Error
	if errors.As(err, &connectErr) {
		for _, detail := range connectErr.Details() {
			msg, detailErr := detail.Value()
			if detailErr != nil {
				continue
			}
			if info, ok := msg.(*errdetails.ErrorInfo); ok {
				if (info.Domain == "bowl.storage" || info.Reason != "") && info.Reason != "" {
					return true
				}
			}
		}
	}
	return false
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

	var opErr *net.OpError
	if errors.As(err, &opErr) {
		if opErr.Op == "dial" {
			return true
		}
	}

	var sysErr *os.SyscallError
	if errors.As(err, &sysErr) {
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
	if errors.As(err, &opErr) {
		return true
	}

	return false
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

	// 4. 针对 16 个只读与幂等 RPC 白名单：
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

// NewRetryInterceptor 创建遵循有界指数退避策略的 ConnectRPC 客户端一元拦截器。
func NewRetryInterceptor(cfg RetryConfig) connect.UnaryInterceptorFunc {
	config := cfg.withDefaults()

	return func(next connect.UnaryFunc) connect.UnaryFunc {
		return func(ctx context.Context, req connect.AnyRequest) (connect.AnyResponse, error) {
			if config.Disable {
				return next(ctx, req)
			}

			startTime := time.Now()
			interval := config.InitialInterval
			var lastErr error

			for {
				if err := ctx.Err(); err != nil {
					return nil, err
				}

				resp, err := next(ctx, req)
				if err == nil {
					return resp, nil
				}

				lastErr = err

				// 若不满足重试条件，立即终止并返回原始错误
				if !shouldRetry(req.Spec().Procedure, err) {
					return nil, err
				}

				// 计算已过去的总耗时，若超出总预算则终止重试
				elapsed := time.Since(startTime)
				if elapsed >= config.MaxElapsedTime {
					return nil, lastErr
				}

				sleepDuration := computeJitteredInterval(interval, config.RandFloat64)
				remaining := config.MaxElapsedTime - elapsed
				if sleepDuration > remaining {
					sleepDuration = remaining
				}

				timer := time.NewTimer(sleepDuration)
				select {
				case <-ctx.Done():
					timer.Stop()
					return nil, ctx.Err()
				case <-timer.C:
				}

				// 指数退避增长并钳位在 MaxInterval
				interval = time.Duration(float64(interval) * config.Multiplier)
				if interval > config.MaxInterval {
					interval = config.MaxInterval
				}
			}
		}
	}
}
