package server

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"runtime/pprof"
	"strings"
	"time"

	"connectrpc.com/connect"
	"connectrpc.com/otelconnect"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"

	"github.com/starsrc/pkg/observability/slogx"
	"github.com/starsrc/pkg/observability/telemetry"

	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
	"github.com/starsrc/bowl/gen/go/storage/v1/storagev1connect"
)

const (
	SlowStorageRPCThreshold = 50 * time.Millisecond

	SlowControlRPCThreshold = 200 * time.Millisecond

	headerRequestID = "X-Request-Id"

	attrKeyRPCErrorReason = "rpc.error_reason"
	attrKeyBowlDiskID     = "bowl.disk_id"
	attrKeyBowlBranch     = "bowl.branch"
	attrKeyBowlEpoch      = "bowl.epoch"
	attrKeyBowlIno        = "bowl.ino"
	attrKeyBowlRequestID  = "bowl.request_id"

	logKeyDiskID    = "disk_id"
	logKeyBranch    = "branch"
	logKeyEpoch     = "epoch"
	logKeyIno       = "ino"
	logKeyRequestID = "request_id"
)

type ObservabilityConfig interface {
	slogx.Config
	TrustRemote() bool
}

type (
	diskIDGetter    interface{ GetDiskId() string }
	branchGetter    interface{ GetBranch() string }
	epochGetter     interface{ GetEpoch() uint64 }
	inoGetter       interface{ GetIno() uint64 }
	diskRespGetter  interface{ GetDisk() *storagev1.Disk }
	inodeRespGetter interface {
		GetInode() *storagev1.Inode
	}
	rootInodeRespGetter interface {
		GetRootInode() *storagev1.Inode
	}
)

type telemetryObsAdapter struct {
	telemetry.Config
	slogx.LogConfig
}

func toObservabilityConfig(cfg telemetry.Config) ObservabilityConfig {
	if cfg == nil {
		return nil
	}
	if oc, ok := cfg.(ObservabilityConfig); ok {
		return oc
	}
	return telemetryObsAdapter{
		Config: cfg,
		LogConfig: slogx.LogConfig{
			LogLevel:          "info",
			LogFormat:         "json",
			LogConnectMessage: slogx.DefaultConnectMessage,
		},
	}
}

type errorReasonInterceptor struct{}

func recordErrorReason(ctx context.Context, err error) {
	if err == nil {
		return
	}
	if cErr, ok := errors.AsType[*connect.Error](err); ok && cErr.Meta().Get("traceparent") == "" {
		otel.GetTextMapPropagator().Inject(ctx, propagation.HeaderCarrier(cErr.Meta()))
	}
	info := ExtractErrorInfo(err)
	if info == nil || info.GetReason() == "" {
		return
	}
	attr := attribute.String(attrKeyRPCErrorReason, info.GetReason())
	if span := trace.SpanFromContext(ctx); span.IsRecording() {
		span.SetAttributes(attr)
	}
	if labeler, ok := otelconnect.LabelerFromContext(ctx); ok {
		labeler.Add(attr)
	}
}

func (errorReasonInterceptor) WrapUnary(next connect.UnaryFunc) connect.UnaryFunc {
	return func(ctx context.Context, req connect.AnyRequest) (connect.AnyResponse, error) {
		res, err := next(ctx, req)
		if err != nil {
			recordErrorReason(ctx, err)
		}
		return res, err
	}
}

func (errorReasonInterceptor) WrapStreamingClient(next connect.StreamingClientFunc) connect.StreamingClientFunc {
	return func(ctx context.Context, spec connect.Spec) connect.StreamingClientConn {
		return next(ctx, spec)
	}
}

func (errorReasonInterceptor) WrapStreamingHandler(next connect.StreamingHandlerFunc) connect.StreamingHandlerFunc {
	return func(ctx context.Context, conn connect.StreamingHandlerConn) error {
		err := next(ctx, conn)
		if err != nil {
			recordErrorReason(ctx, err)
		}
		return err
	}
}

func classifyStorageLogLevel(procedure string, code connect.Code, err error) slog.Level {
	if err == nil {
		switch procedure {
		case storagev1connect.StorageServiceCreateDiskProcedure, "CreateDisk",
			storagev1connect.StorageServiceUpdateDiskProcedure, "UpdateDisk",
			storagev1connect.StorageServiceDeleteDiskProcedure, "DeleteDisk",
			storagev1connect.StorageServiceMountBranchProcedure, "MountBranch",
			storagev1connect.StorageServiceUnmountBranchProcedure, "UnmountBranch",
			storagev1connect.StorageServiceEvictMountsProcedure, "EvictMounts",
			storagev1connect.StorageServiceForkBranchProcedure, "ForkBranch",
			storagev1connect.StorageServiceDeleteBranchProcedure, "DeleteBranch",
			storagev1connect.StorageServiceResetBranchProcedure, "ResetBranch",
			storagev1connect.StorageServiceMergeBranchProcedure, "MergeBranch",
			storagev1connect.StorageServiceCreateCommitProcedure, "CreateCommit",
			storagev1connect.StorageServiceDeleteCommitProcedure, "DeleteCommit",
			storagev1connect.StorageServicePruneCommitsProcedure, "PruneCommits",
			storagev1connect.StorageServiceReclaimOrphanProcedure, "ReclaimOrphan":
			return slog.LevelInfo
		default:
			return slog.LevelDebug
		}
	}
	if code == connect.CodeCanceled {
		return slog.LevelDebug
	}
	if code == connect.CodeNotFound {
		switch procedure {
		case storagev1connect.StorageServiceLookupProcedure, "Lookup",
			storagev1connect.StorageServiceGetXattrProcedure, "GetXattr":
			return slog.LevelDebug
		}
	}
	return slogx.DefaultLevelClassifier(code, err)
}

func isControlPlaneProcedure(procedure string) bool {
	switch procedure {
	case storagev1connect.StorageServiceCreateDiskProcedure, "CreateDisk",
		storagev1connect.StorageServiceGetDiskProcedure, "GetDisk",
		storagev1connect.StorageServiceListDisksProcedure, "ListDisks",
		storagev1connect.StorageServiceUpdateDiskProcedure, "UpdateDisk",
		storagev1connect.StorageServiceDeleteDiskProcedure, "DeleteDisk",
		storagev1connect.StorageServiceMountBranchProcedure, "MountBranch",
		storagev1connect.StorageServiceRenewMountProcedure, "RenewMount",
		storagev1connect.StorageServiceUnmountBranchProcedure, "UnmountBranch",
		storagev1connect.StorageServiceListMountsProcedure, "ListMounts",
		storagev1connect.StorageServiceEvictMountsProcedure, "EvictMounts",
		storagev1connect.StorageServiceForkBranchProcedure, "ForkBranch",
		storagev1connect.StorageServiceListBranchesProcedure, "ListBranches",
		storagev1connect.StorageServiceDeleteBranchProcedure, "DeleteBranch",
		storagev1connect.StorageServiceResetBranchProcedure, "ResetBranch",
		storagev1connect.StorageServiceMergeBranchProcedure, "MergeBranch",
		storagev1connect.StorageServiceCreateCommitProcedure, "CreateCommit",
		storagev1connect.StorageServiceListCommitsProcedure, "ListCommits",
		storagev1connect.StorageServiceDeleteCommitProcedure, "DeleteCommit",
		storagev1connect.StorageServicePruneCommitsProcedure, "PruneCommits",
		storagev1connect.StorageServiceReadCommitNodeProcedure, "ReadCommitNode",
		storagev1connect.StorageServiceDiffBranchesProcedure, "DiffBranches",
		storagev1connect.StorageServiceDiffCommitsProcedure, "DiffCommits",
		storagev1connect.StorageServiceDiffWorkProcedure, "DiffWork",
		storagev1connect.StorageServiceReclaimOrphanProcedure, "ReclaimOrphan":
		return true
	default:
		return false
	}
}

func extractStorageFields(req connect.AnyRequest, res connect.AnyResponse, err error) (diskID, branch string, epoch, ino uint64) {
	if req != nil {
		msg := req.Any()
		if g, ok := msg.(diskIDGetter); ok {
			diskID = g.GetDiskId()
		}
		if g, ok := msg.(branchGetter); ok {
			branch = g.GetBranch()
		}
		if g, ok := msg.(epochGetter); ok {
			epoch = g.GetEpoch()
		}
		if g, ok := msg.(inoGetter); ok {
			ino = g.GetIno()
		}
	}
	if err == nil && res != nil {
		msg := res.Any()
		if diskID == "" {
			if g, ok := msg.(diskRespGetter); ok {
				diskID = g.GetDisk().GetDiskId()
			}
		}
		if epoch == 0 {
			if g, ok := msg.(epochGetter); ok {
				epoch = g.GetEpoch()
			}
		}
		if ino == 0 {
			switch g := msg.(type) {
			case inodeRespGetter:
				ino = g.GetInode().GetIno()
			case rootInodeRespGetter:
				ino = g.GetRootInode().GetIno()
			}
		}
	}
	return diskID, branch, epoch, ino
}

func extractRequestID(req connect.AnyRequest) string {
	if req == nil {
		return ""
	}
	return strings.TrimSpace(req.Header().Get(headerRequestID))
}

func extractStorageAttrs(_ context.Context, req connect.AnyRequest, res connect.AnyResponse, err error) []slog.Attr {
	diskID, branch, epoch, ino := extractStorageFields(req, res, err)
	reqID := extractRequestID(req)
	if diskID == "" && branch == "" && epoch == 0 && ino == 0 && reqID == "" {
		return nil
	}
	attrs := make([]slog.Attr, 0, 5)
	if diskID != "" {
		attrs = append(attrs, slog.String(logKeyDiskID, diskID))
	}
	if branch != "" {
		attrs = append(attrs, slog.String(logKeyBranch, branch))
	}
	if epoch != 0 {
		attrs = append(attrs, slog.Uint64(logKeyEpoch, epoch))
	}
	if ino != 0 {
		attrs = append(attrs, slog.Uint64(logKeyIno, ino))
	}
	if reqID != "" {
		attrs = append(attrs, slog.String(logKeyRequestID, reqID))
	}
	return attrs
}

func NewStorageObservabilityInterceptor(logger *slog.Logger) connect.UnaryInterceptorFunc {
	return newStorageObservabilityInterceptorWithClock(logger, time.Now)
}

func newStorageObservabilityInterceptorWithClock(
	logger *slog.Logger,
	now func() time.Time,
) connect.UnaryInterceptorFunc {
	return func(next connect.UnaryFunc) connect.UnaryFunc {
		return func(ctx context.Context, req connect.AnyRequest) (connect.AnyResponse, error) {
			var procedure string
			if req != nil {
				procedure = req.Spec().Procedure
			}

			start := now()
			var (
				res connect.AnyResponse
				err error
			)
			pprof.Do(ctx, pprof.Labels("rpc", procedure), func(pprofCtx context.Context) {
				res, err = next(pprofCtx, req)
			})
			duration := now().Sub(start)

			if span := trace.SpanFromContext(ctx); span.IsRecording() {
				diskID, branch, epoch, ino := extractStorageFields(req, res, err)
				reqID := extractRequestID(req)
				var kv [5]attribute.KeyValue
				n := 0
				if diskID != "" {
					kv[n] = attribute.String(attrKeyBowlDiskID, diskID)
					n++
				}
				if branch != "" {
					kv[n] = attribute.String(attrKeyBowlBranch, branch)
					n++
				}
				if epoch != 0 && epoch <= math.MaxInt64 {
					kv[n] = attribute.Int64(attrKeyBowlEpoch, int64(epoch))
					n++
				}
				if ino != 0 && ino <= math.MaxInt64 {
					kv[n] = attribute.Int64(attrKeyBowlIno, int64(ino))
					n++
				}
				if reqID != "" {
					kv[n] = attribute.String(attrKeyBowlRequestID, reqID)
					n++
				}
				if n > 0 {
					span.SetAttributes(kv[:n]...)
				}
			}

			isCtrl := isControlPlaneProcedure(procedure)
			if (!isCtrl && duration > SlowStorageRPCThreshold) || (isCtrl && duration > SlowControlRPCThreshold) {
				logSlowStorageRPC(ctx, logger, procedure, duration, req, res, err)
			}

			return res, err
		}
	}
}

func logSlowStorageRPC(
	ctx context.Context,
	logger *slog.Logger,
	procedure string,
	duration time.Duration,
	req connect.AnyRequest,
	res connect.AnyResponse,
	err error,
) {
	target := logger
	if target == nil {
		target = slog.Default()
	}
	if !target.Enabled(ctx, slog.LevelWarn) {
		return
	}

	code := "ok"
	if err != nil {
		code = connect.CodeOf(err).String()
	}
	attrs := make([]slog.Attr, 0, 8)
	attrs = append(attrs,
		slog.String(slogx.DefaultProcedureKey, procedure),
		slog.Duration(slogx.DefaultDurationKey, duration),
		slog.String(slogx.DefaultCodeKey, code),
	)
	if err != nil {
		attrs = append(attrs, slog.String(slogx.DefaultErrorKey, err.Error()))
	}
	if extra := extractStorageAttrs(ctx, req, res, err); len(extra) > 0 {
		attrs = append(attrs, extra...)
	}
	target.LogAttrs(ctx, slog.LevelWarn, "slow storage rpc", attrs...)
}

func BuildInterceptors(
	obsCfg ObservabilityConfig,
	logger *slog.Logger,
) ([]connect.Interceptor, error) {
	val, err := NewStorageValidator()
	if err != nil {
		return nil, fmt.Errorf("create storage validator: %w", err)
	}

	otelOpts := []otelconnect.Option{
		otelconnect.WithPropagateResponseHeader(),
	}
	if obsCfg != nil && obsCfg.TrustRemote() {
		otelOpts = append(otelOpts, otelconnect.WithTrustRemote())
	}
	otelInterceptor, err := otelconnect.NewInterceptor(otelOpts...)
	if err != nil {
		return nil, fmt.Errorf("create otelconnect interceptor: %w", err)
	}

	return []connect.Interceptor{
		slogx.NewMaskInterceptor(logger),
		otelInterceptor,
		errorReasonInterceptor{},
		slogx.NewConnectInterceptor(
			obsCfg,
			logger,
			slogx.WithLevelClassifier(classifyStorageLogLevel),
			slogx.WithAttrExtractor(extractStorageAttrs),
		),
		NewStorageObservabilityInterceptor(logger),
		NewValidationInterceptor(val),
	}, nil
}
