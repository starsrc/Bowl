package cli

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"connectrpc.com/connect"
	"github.com/google/uuid"
	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/spf13/cobra"
	"github.com/starsrc/pkg/serverx"

	"github.com/starsrc/bowl/client/internal/fusefs"
	"github.com/starsrc/bowl/client/internal/rpc"
	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
	"github.com/starsrc/bowl/gen/go/storage/v1/storagev1connect"
)

const (
	defaultAdminAddr               = "127.0.0.1:0"
	adminAddrOff                   = "off"
	defaultTraceSampleRatio        = 0.001
	mountUnmountRPCTimeout         = 3 * time.Second
	mountRenewRPCTimeout           = 5 * time.Second
	fuseSignalUnmountRetryInterval = 500 * time.Millisecond
)

type mountOptions struct {
	endpoint                 string
	token                    string
	logFile                  string
	adminAddr                string
	logLevel                 string
	logFormat                string
	traceSampleRatio         float64
	mode                     string
	stageSizeRaw             string
	stageDirRaw              string
	stageSizeBytes           uint64
	stageDir                 string
	unmountTimeout           time.Duration
	unmountStageTimeout      time.Duration
	unmountStageStallTimeout time.Duration
	unmountFsyncTimeout      time.Duration
	force                    bool
}

type mountFlagPresence struct {
	logFile                  bool
	adminAddr                bool
	logLevel                 bool
	logFormat                bool
	traceSampleRatio         bool
	mode                     bool
	stageSize                bool
	stageDir                 bool
	unmountTimeout           bool
	unmountStageTimeout      bool
	unmountStageStallTimeout bool
	unmountFsyncTimeout      bool
}

var (
	serveFUSE                 = defaultServeFUSE
	mountHTTPClient           *http.Client
	mountRenewIntervalFn      = defaultMountRenewInterval
	mountRenewRetryIntervalFn = defaultMountRenewRetryInterval
	mountNowFn                = time.Now
	stageDirIsTmpfs           = func(s *fusefs.StageDirSession) bool { return s.IsTmpfs() }
)

type mountLeaseInfo struct {
	mountID         string
	leaseTTLSeconds uint32
}

type mountLeaseTracker struct {
	mu             sync.Mutex
	active         bool
	leaseExpiresAt time.Time
	now            func() time.Time
}

func newMountLeaseTracker(lease mountLeaseInfo, nowFn func() time.Time) *mountLeaseTracker {
	if nowFn == nil {
		nowFn = mountNowFn
	}
	t := &mountLeaseTracker{
		active: lease.mountID != "",
		now:    nowFn,
	}
	if t.active {
		t.leaseExpiresAt = nowFn().Add(time.Duration(lease.leaseTTLSeconds) * time.Second)
	}
	return t
}

func (t *mountLeaseTracker) refresh(ttlSeconds uint32) {
	if t == nil {
		return
	}
	t.mu.Lock()
	if t.active {
		t.leaseExpiresAt = t.now().Add(time.Duration(ttlSeconds) * time.Second)
	}
	t.mu.Unlock()
}

func checkMountLeaseReady(rawFS *fusefs.RawFS, tracker *mountLeaseTracker) error {
	if rawFS != nil && rawFS.IsFenced() {
		if reason := rawFS.FenceReason(); reason != "" {
			return fmt.Errorf("mount fenced: %s", reason)
		}
		return errors.New("mount fenced")
	}
	if tracker != nil {
		tracker.mu.Lock()
		active := tracker.active
		expiresAt := tracker.leaseExpiresAt
		now := tracker.now()
		tracker.mu.Unlock()
		if active && !now.Before(expiresAt) {
			return fmt.Errorf("mount lease expired at %s", expiresAt.UTC().Format(time.RFC3339))
		}
	}
	return nil
}

func defaultMountRenewInterval(leaseTTLSeconds uint32) time.Duration {
	d := time.Duration(leaseTTLSeconds) * time.Second / 3
	if d < time.Second {
		return time.Second
	}
	return d
}

func defaultMountRenewRetryInterval(normalInterval time.Duration) time.Duration {
	if normalInterval > 0 && normalInterval < time.Second {
		return normalInterval
	}
	return time.Second
}

func isDiskMountedError(err error) bool {
	connectErr, ok := errors.AsType[*connect.Error](err)
	if !ok || connectErr.Code() != connect.CodeAborted {
		return false
	}
	info := serverx.ExtractErrorInfo(err)
	return info != nil && info.GetDomain() == "bowl.storage" && info.GetReason() == "EBUSY"
}

func mountHandshake(ctx context.Context, client storagev1connect.StorageServiceClient, baseCfg *fusefs.Config, force bool) (*fusefs.Config, mountLeaseInfo, error) {
	rpcCtx, rpcCancel := context.WithTimeout(ctx, 10*time.Second)
	defer rpcCancel()

	resp, err := client.MountDisk(rpcCtx, connect.NewRequest(&storagev1.MountDiskRequest{
		DiskId:   baseCfg.DiskID,
		ReadOnly: baseCfg.IsReadOnly(),
		Force:    force,
	}))
	if err != nil {
		if connectErr, ok := errors.AsType[*connect.Error](err); ok && connectErr.Code() == connect.CodeNotFound {
			return nil, mountLeaseInfo{}, fmt.Errorf("disk not found: %s", baseCfg.DiskID)
		}
		if isDiskMountedError(err) {
			return nil, mountLeaseInfo{}, fmt.Errorf("disk %s already has an active read-write mount; unmount it first or pass --force to preempt: %w", baseCfg.DiskID, err)
		}
		return nil, mountLeaseInfo{}, fmt.Errorf("mount handshake failed: %w", err)
	}

	rootInode := resp.Msg.GetRootInode()
	if rootInode == nil || rootInode.GetIno() == 0 {
		return nil, mountLeaseInfo{}, errors.New("server returned invalid root directory metadata")
	}

	epoch := resp.Msg.GetEpoch()
	if !baseCfg.IsReadOnly() && epoch == 0 {
		return nil, mountLeaseInfo{}, errors.New("server did not return a valid write mount epoch")
	}

	cfg := *baseCfg
	cfg.Epoch = epoch
	cfg.RootIno = rootInode.GetIno()
	cfg.RootInode = rootInode
	cfg.EntryTimeoutSeconds = resp.Msg.GetEntryTimeoutSeconds()
	cfg.AttrTimeoutSeconds = resp.Msg.GetAttrTimeoutSeconds()
	lease := mountLeaseInfo{
		mountID:         resp.Msg.GetMountId(),
		leaseTTLSeconds: resp.Msg.GetLeaseTtlSeconds(),
	}
	return &cfg, lease, nil
}

func permanentRenewFenceStatus(err error) (fuse.Status, connect.Code, bool) {
	connectErr, ok := errors.AsType[*connect.Error](err)
	if !ok {
		return fuse.OK, 0, false
	}
	code := connectErr.Code()
	switch code {
	case connect.CodeFailedPrecondition,
		connect.CodeNotFound,
		connect.CodeUnauthenticated,
		connect.CodePermissionDenied,
		connect.CodeInvalidArgument,
		connect.CodeUnimplemented:
		return fuse.Status(rpc.MapError(err)), code, true
	default:
		return fuse.OK, code, false
	}
}

func startMountLeaseRenewer(
	client storagev1connect.StorageServiceClient,
	cfg *fusefs.Config,
	lease mountLeaseInfo,
	rawFS *fusefs.RawFS,
	tracker *mountLeaseTracker,
	logger *slog.Logger,
) func() {
	if lease.mountID == "" {
		return func() {}
	}
	renewCtx, renewCancel := context.WithCancel(context.Background())
	var fencedCh <-chan struct{}
	if rawFS != nil {
		fencedCh = rawFS.FencedCh()
	}
	if fencedCh != nil {
		go func() {
			select {
			case <-fencedCh:
				renewCancel()
			case <-renewCtx.Done():
			}
		}()
	}

	done := make(chan struct{})
	currentTTL := lease.leaseTTLSeconds

	go func() {
		defer close(done)
		if rawFS != nil && rawFS.IsFenced() {
			return
		}
		normalInterval := mountRenewIntervalFn(currentTTL)
		timer := time.NewTimer(normalInterval)
		defer timer.Stop()

		for {
			select {
			case <-renewCtx.Done():
				return
			case <-fencedCh:
				return
			case <-timer.C:
			}

			rpcCtx, rpcCancel := context.WithTimeout(renewCtx, mountRenewRPCTimeout)
			resp, err := client.RenewMount(rpcCtx, connect.NewRequest(&storagev1.RenewMountRequest{
				DiskId:   cfg.DiskID,
				MountId:  lease.mountID,
				ReadOnly: cfg.IsReadOnly(),
				Epoch:    cfg.Epoch,
			}))
			rpcCancel()

			if err != nil {
				if renewCtx.Err() != nil || (rawFS != nil && rawFS.IsFenced()) {
					return
				}
				if fenceStatus, code, permanent := permanentRenewFenceStatus(err); permanent {
					logger.Warn("mount_lease_renew_stopped",
						slog.String("disk_id", cfg.DiskID),
						slog.String("mount_id", lease.mountID),
						slog.String("code", code.String()),
						slog.String("error", err.Error()),
					)
					rawFS.Fence(fenceStatus, err.Error())
					return
				}
				logger.Warn("mount_lease_renew_failed",
					slog.String("disk_id", cfg.DiskID),
					slog.String("mount_id", lease.mountID),
					slog.String("error", err.Error()),
				)
				timer.Reset(mountRenewRetryIntervalFn(normalInterval))
				continue
			}

			if rawFS != nil && rawFS.IsFenced() {
				return
			}
			if nextTTL := resp.Msg.GetLeaseTtlSeconds(); nextTTL > 0 {
				currentTTL = nextTTL
			}
			tracker.refresh(currentTTL)
			normalInterval = mountRenewIntervalFn(currentTTL)
			timer.Reset(normalInterval)
		}
	}()

	return func() {
		renewCancel()
		<-done
	}
}

func unmountMountLease(client storagev1connect.StorageServiceClient, diskID, mountID string, timeout time.Duration, logger *slog.Logger) {
	if mountID == "" || timeout <= 0 {
		return
	}
	unmountCtx, unmountCancel := context.WithTimeout(context.Background(), timeout)
	defer unmountCancel()
	if _, err := client.UnmountDisk(unmountCtx, connect.NewRequest(&storagev1.UnmountDiskRequest{
		DiskId:  diskID,
		MountId: mountID,
	})); err != nil {
		logger.Warn("unmount_disk_rpc_failed",
			slog.String("disk_id", diskID),
			slog.String("mount_id", mountID),
			slog.String("error", err.Error()),
		)
	}
}

func runFUSESignalUnmountLoop(
	ctx context.Context,
	sigCh <-chan os.Signal,
	stopWait <-chan struct{},
	retryInterval time.Duration,
	mountpoint string,
	logger *slog.Logger,
	unmount func() error,
) {
	if logger == nil {
		logger = slog.Default()
	}
	if retryInterval <= 0 {
		retryInterval = fuseSignalUnmountRetryInterval
	}

	var ctxDone <-chan struct{}
	if ctx != nil {
		ctxDone = ctx.Done()
	}

	var (
		ticker  *time.Ticker
		retryCh <-chan time.Time
	)
	defer func() {
		if ticker != nil {
			ticker.Stop()
		}
	}()

	attemptUnmount := func() bool {
		err := unmount()
		if err == nil {
			if ticker != nil {
				ticker.Stop()
				ticker = nil
				retryCh = nil
			}
			return true
		}
		logger.Warn("mount_unmount_busy",
			slog.String("mountpoint", mountpoint),
			slog.String("error", err.Error()),
		)
		if ticker == nil {
			ticker = time.NewTicker(retryInterval)
			retryCh = ticker.C
		}
		return false
	}

	for {
		select {
		case <-stopWait:
			return
		default:
		}
		select {
		case <-stopWait:
			return
		case <-sigCh:
			if attemptUnmount() {
				return
			}
		case <-ctxDone:
			ctxDone = nil
			if attemptUnmount() {
				return
			}
		case <-retryCh:
			if attemptUnmount() {
				return
			}
		}
	}
}

func defaultServeFUSE(ctx context.Context, fs *fusefs.RawFS, cfg *fusefs.Config, mountpoint string, ready func()) error {
	opts := fusefs.BuildMountOptions(*cfg)

	server, err := fuse.NewServer(fs, mountpoint, opts)
	if err != nil {
		return fmt.Errorf("create FUSE server: %w", err)
	}

	serveDone := make(chan struct{})
	go func() {
		defer close(serveDone)
		server.Serve()
	}()

	if waitErr := server.WaitMount(); waitErr != nil {
		_ = server.Unmount()
		<-serveDone
		return fmt.Errorf("wait for mount readiness: %w", waitErr)
	}
	fs.BindMountpoint(mountpoint)
	ready()

	sigCh := make(chan os.Signal, 2)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	defer signal.Stop(sigCh)

	stopWait := make(chan struct{})
	loopDone := make(chan struct{})
	go func() {
		defer close(loopDone)
		runFUSESignalUnmountLoop(ctx, sigCh, stopWait, fuseSignalUnmountRetryInterval, mountpoint, cfg.Logger, server.Unmount)
	}()

	<-serveDone
	close(stopWait)
	<-loopDone
	return nil
}

func runMount(ctx context.Context, opts *mountOptions, baseCfg *fusefs.Config, mountpoint string, logWriter io.Writer, onReady func()) error {
	obs, err := setupObservability(ctx, opts, logWriter)
	if err != nil {
		return err
	}
	logger := obs.Logger()
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if shutdownErr := obs.Shutdown(shutdownCtx); shutdownErr != nil {
			logger.Warn("observability_shutdown_failed", slog.String("error", shutdownErr.Error()))
		}
	}()

	var (
		stageSession   *fusefs.StageDirSession
		closeStageOnce sync.Once
	)
	closeStage := func() {
		closeStageOnce.Do(func() {
			if stageSession != nil {
				_ = stageSession.Close()
			}
		})
	}
	if baseCfg.IsStaged() {
		var stageErr error
		stageSession, stageErr = fusefs.OpenStageDirSession(baseCfg.StageDir)
		if stageErr != nil {
			return fmt.Errorf("initialize stage directory: %w", stageErr)
		}
		defer closeStage()
		if stageDirIsTmpfs(stageSession) {
			logger.Warn("stage_dir_on_tmpfs", slog.String("stage_dir", baseCfg.StageDir))
		}
	}

	rpcMetrics := rpc.NewMetrics()
	client, err := rpc.NewClientFromConfig(rpc.ClientConfig{
		Endpoint:   opts.endpoint,
		Token:      opts.token,
		HTTPClient: mountHTTPClient,
		Logger:     logger,
		Metrics:    rpcMetrics,
	})
	if err != nil {
		return fmt.Errorf("create RPC client: %w", err)
	}

	cfg, lease, err := mountHandshake(ctx, client, baseCfg, opts.force)
	if err != nil {
		return err
	}
	leaseTracker := newMountLeaseTracker(lease, mountNowFn)
	cfg.Logger = logger
	cfg.Metrics = fusefs.NewMetrics()
	cfg.TracerProvider = obs.TracerProvider()
	if cfg.IsStaged() {
		cfg.StageSession = stageSession
		if cfg.StageSize == 0 {
			cfg.StageSize = fusefs.DefaultStageSize
		}
	}

	rawFS := fusefs.NewRawFS(client, *cfg)
	defer rawFS.OnUnmount()
	if regErr := obs.RegisterCollector(rpcMetrics, rawFS.Collector()); regErr != nil {
		unmountMountLease(client, cfg.DiskID, lease.mountID, mountUnmountRPCTimeout, logger)
		return fmt.Errorf("register metrics collector: %w", regErr)
	}

	admin, err := startAdminServer(opts.adminAddr, obs, rawFS, leaseTracker)
	if err != nil {
		unmountMountLease(client, cfg.DiskID, lease.mountID, mountUnmountRPCTimeout, logger)
		return err
	}
	shutdownAdmin := func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		admin.shutdown(shutdownCtx)
	}
	defer shutdownAdmin()

	var (
		metaPath  string
		startedAt time.Time
		selfPID   = os.Getpid()
	)
	ready := func() {
		startedAt = time.Now().UTC()
		meta := mountMeta{
			Mountpoint:       mountpoint,
			PID:              selfPID,
			DiskID:           cfg.DiskID,
			AdminAddr:        admin.addr(),
			StartedAt:        startedAt,
			UnmountTimeoutMs: rawFS.Config().UnmountTimeout.Milliseconds(),
		}
		path, werr := writeMountMeta(&meta)
		if werr != nil {
			logger.Warn("mount_meta_write_failed", slog.String("mountpoint", mountpoint), slog.String("error", werr.Error()))
		}
		metaPath = path
		logger.Info("mount_ready",
			slog.String("mountpoint", mountpoint),
			slog.String("server_addr", opts.endpoint),
			slog.String("disk_id", cfg.DiskID),
			slog.Uint64("epoch", cfg.Epoch),
			slog.Uint64("root_ino", cfg.RootIno),
			slog.String("admin_addr", admin.addr()),
			slog.Int("pid", selfPID),
		)
		if onReady != nil {
			onReady()
		}
	}

	stopRenew := startMountLeaseRenewer(client, cfg, lease, rawFS, leaseTracker, logger)
	serveErr := serveFUSE(ctx, rawFS, cfg, mountpoint, ready)
	stopRenew()
	if rawFS.IsFenced() {
		logger.Info("unmount_disk_rpc_skipped_fenced",
			slog.String("disk_id", cfg.DiskID),
			slog.String("mount_id", lease.mountID),
			slog.String("fence_status", rawFS.FenceStatus().String()),
			slog.String("fence_reason", rawFS.FenceReason()),
		)
	} else if remaining := rawFS.UnmountRemainingBudget(); remaining <= 0 {
		logger.Warn("unmount_disk_rpc_skipped_timeout",
			slog.String("disk_id", cfg.DiskID),
			slog.String("mount_id", lease.mountID),
		)
	} else {
		unmountMountLease(client, cfg.DiskID, lease.mountID, min(mountUnmountRPCTimeout, remaining), logger)
	}
	shutdownAdmin()
	closeStage()
	if metaPath != "" {
		_ = os.Remove(metaPath)
	}
	if serveErr != nil {
		return serveErr
	}
	var uptimeMs int64
	if !startedAt.IsZero() {
		uptimeMs = time.Since(startedAt).Milliseconds()
	}
	logger.Info("unmount_complete",
		slog.String("mountpoint", mountpoint),
		slog.String("server_addr", opts.endpoint),
		slog.String("disk_id", cfg.DiskID),
		slog.Uint64("epoch", cfg.Epoch),
		slog.Int("pid", selfPID),
		slog.Int64("uptime_ms", uptimeMs),
	)
	return nil
}

func firstNonEmptyEnv(keys ...string) string {
	for _, k := range keys {
		if v := strings.TrimSpace(os.Getenv(k)); v != "" {
			return v
		}
	}
	return ""
}

func applyEnvAndValidateOptions(o *mountOptions, set mountFlagPresence, foreground bool) error {
	if set.logFile {
		if foreground {
			return errors.New("--log-file cannot be set with -f/--foreground")
		}
		if strings.TrimSpace(o.logFile) == "" {
			return errors.New("--log-file cannot be empty")
		}
		absLogFile, err := resolveAbsFlagPath("--log-file", o.logFile, false)
		if err != nil {
			return err
		}
		o.logFile = absLogFile
	}
	if !set.adminAddr {
		if env := firstNonEmptyEnv("BOWL_ADMIN_ADDR", "OBS_ADMIN_ADDR"); env != "" {
			o.adminAddr = env
		}
	}
	if !set.logLevel {
		if env := firstNonEmptyEnv("BOWL_LOG_LEVEL", "LOG_LEVEL"); env != "" {
			o.logLevel = env
		}
	}
	if !set.logFormat {
		if env := firstNonEmptyEnv("BOWL_LOG_FORMAT", "LOG_FORMAT"); env != "" {
			o.logFormat = env
		}
	}
	if !set.traceSampleRatio {
		if env := firstNonEmptyEnv("OTEL_TRACES_SAMPLER_ARG"); env != "" {
			parsed, err := strconv.ParseFloat(env, 64)
			if err != nil {
				return fmt.Errorf("invalid environment variable OTEL_TRACES_SAMPLER_ARG=%q: %w", env, err)
			}
			o.traceSampleRatio = parsed
		}
	}
	if err := validateAdminAddr(o.adminAddr); err != nil {
		return err
	}
	switch strings.ToLower(strings.TrimSpace(o.logLevel)) {
	case "debug", "info", "warn", "warning", "error":
	default:
		return fmt.Errorf("invalid --log-level %q: must be debug, info, warn, or error", o.logLevel)
	}
	switch strings.ToLower(strings.TrimSpace(o.logFormat)) {
	case "json", "text":
	default:
		return fmt.Errorf("invalid --log-format %q: must be json or text", o.logFormat)
	}
	if math.IsNaN(o.traceSampleRatio) || math.IsInf(o.traceSampleRatio, 0) || o.traceSampleRatio < 0 || o.traceSampleRatio > 1 {
		return fmt.Errorf("invalid --trace-sample-ratio %v: must be in [0, 1]", o.traceSampleRatio)
	}
	if err := applyStageOptions(o, set); err != nil {
		return err
	}
	return applyUnmountOptions(o, set)
}

func resolvePositiveDurationOption(flagName, envKey string, flagSet bool, flagVal, defaultVal time.Duration) (val time.Duration, explicit bool, err error) {
	if flagSet {
		if flagVal <= 0 {
			return 0, false, fmt.Errorf("invalid %s %v: must be > 0", flagName, flagVal)
		}
		return flagVal, true, nil
	}
	if env := firstNonEmptyEnv(envKey); env != "" {
		parsed, parseErr := time.ParseDuration(env)
		if parseErr != nil {
			return 0, false, fmt.Errorf("invalid environment variable %s=%q: %w", envKey, env, parseErr)
		}
		if parsed <= 0 {
			return 0, false, fmt.Errorf("invalid environment variable %s=%q: must be > 0", envKey, env)
		}
		return parsed, true, nil
	}
	return defaultVal, false, nil
}

func applyUnmountOptions(o *mountOptions, set mountFlagPresence) error {
	if o.mode == fusefs.MountModeRO || o.mode == fusefs.MountModeSync {
		if set.unmountStageTimeout {
			return fmt.Errorf("--unmount-stage-timeout cannot be set when --mode is %s", o.mode)
		}
		if set.unmountStageStallTimeout {
			return fmt.Errorf("--unmount-stage-stall-timeout cannot be set when --mode is %s", o.mode)
		}
	}

	total, _, err := resolvePositiveDurationOption(
		"--unmount-timeout",
		"BOWL_UNMOUNT_TIMEOUT",
		set.unmountTimeout,
		o.unmountTimeout,
		fusefs.DefaultUnmountTimeout,
	)
	if err != nil {
		return err
	}
	o.unmountTimeout = total

	fsyncTimeout, fsyncExplicit, err := resolvePositiveDurationOption(
		"--unmount-fsync-timeout",
		"BOWL_UNMOUNT_FSYNC_TIMEOUT",
		set.unmountFsyncTimeout,
		o.unmountFsyncTimeout,
		fusefs.DefaultUnmountFsyncTimeout,
	)
	if err != nil {
		return err
	}
	if fsyncTimeout > total {
		if fsyncExplicit {
			return fmt.Errorf("--unmount-fsync-timeout (%s) cannot exceed --unmount-timeout (%s)", fsyncTimeout, total)
		}
		fsyncTimeout = total
	}
	o.unmountFsyncTimeout = fsyncTimeout

	if o.mode != fusefs.MountModeStaged {
		o.unmountStageTimeout = 0
		o.unmountStageStallTimeout = 0
		return nil
	}

	stageTimeout, stageExplicit, err := resolvePositiveDurationOption(
		"--unmount-stage-timeout",
		"BOWL_UNMOUNT_STAGE_TIMEOUT",
		set.unmountStageTimeout,
		o.unmountStageTimeout,
		fusefs.DefaultUnmountStageTimeout,
	)
	if err != nil {
		return err
	}
	if stageTimeout > total {
		if stageExplicit {
			return fmt.Errorf("--unmount-stage-timeout (%s) cannot exceed --unmount-timeout (%s)", stageTimeout, total)
		}
		stageTimeout = total
	}
	o.unmountStageTimeout = stageTimeout

	stallTimeout, stallExplicit, err := resolvePositiveDurationOption(
		"--unmount-stage-stall-timeout",
		"BOWL_UNMOUNT_STAGE_STALL_TIMEOUT",
		set.unmountStageStallTimeout,
		o.unmountStageStallTimeout,
		fusefs.DefaultUnmountStageStallTimeout,
	)
	if err != nil {
		return err
	}
	if stallTimeout > total {
		if stallExplicit {
			return fmt.Errorf("--unmount-stage-stall-timeout (%s) cannot exceed --unmount-timeout (%s)", stallTimeout, total)
		}
		stallTimeout = total
	}
	if stallTimeout > stageTimeout {
		if stallExplicit {
			return fmt.Errorf("--unmount-stage-stall-timeout (%s) cannot exceed --unmount-stage-timeout (%s)", stallTimeout, stageTimeout)
		}
		stallTimeout = stageTimeout
	}
	o.unmountStageStallTimeout = stallTimeout
	return nil
}

func applyStageOptions(o *mountOptions, set mountFlagPresence) error {
	if !set.mode {
		if env := firstNonEmptyEnv("BOWL_MOUNT_MODE"); env != "" {
			o.mode = env
		}
	}
	mode := strings.ToLower(strings.TrimSpace(o.mode))
	switch mode {
	case fusefs.MountModeStaged, fusefs.MountModeSync, fusefs.MountModeRO:
		o.mode = mode
	default:
		return fmt.Errorf("invalid --mode %q: must be %s, %s, or %s", o.mode, fusefs.MountModeStaged, fusefs.MountModeSync, fusefs.MountModeRO)
	}

	switch o.mode {
	case fusefs.MountModeRO, fusefs.MountModeSync:
		if o.mode == fusefs.MountModeRO && o.force {
			return errors.New("--force cannot be set with --mode=ro")
		}
		if set.stageSize {
			return fmt.Errorf("--stage-size cannot be set when --mode is %s", o.mode)
		}
		if set.stageDir {
			return fmt.Errorf("--stage-dir cannot be set when --mode is %s", o.mode)
		}
		o.stageSizeBytes = 0
		o.stageDir = ""
		return nil
	}

	stageSize := fusefs.DefaultStageSize
	if set.stageSize {
		parsed, err := ParseSize(o.stageSizeRaw)
		if err != nil {
			return fmt.Errorf("invalid --stage-size %q: %w", o.stageSizeRaw, err)
		}
		stageSize = parsed
	} else if env := firstNonEmptyEnv("BOWL_STAGE_SIZE"); env != "" {
		parsed, err := ParseSize(env)
		if err != nil {
			return fmt.Errorf("invalid environment variable BOWL_STAGE_SIZE=%q: %w", env, err)
		}
		stageSize = parsed
	}
	if stageSize < fusefs.MinStageSize {
		return fmt.Errorf("invalid --stage-size %s: minimum is %s", FormatSize(stageSize), FormatSize(fusefs.MinStageSize))
	}
	o.stageSizeBytes = stageSize

	var rawStageDir string
	if set.stageDir {
		if strings.TrimSpace(o.stageDirRaw) == "" {
			return errors.New("--stage-dir cannot be empty")
		}
		rawStageDir = o.stageDirRaw
	} else if env := firstNonEmptyEnv("BOWL_STAGE_DIR"); env != "" {
		rawStageDir = env
	} else {
		rawStageDir = fusefs.DefaultStageDir
	}

	absStageDir, err := resolveAbsFlagPath("--stage-dir", rawStageDir, true)
	if err != nil {
		return err
	}
	o.stageDir = absStageDir
	return nil
}

func resolveAbsFlagPath(flagName, rawPath string, wantDir bool) (string, error) {
	absPath, err := filepath.Abs(rawPath)
	if err != nil {
		return "", fmt.Errorf("failed to resolve %s path: %w", flagName, err)
	}
	absPath = filepath.Clean(absPath)
	if info, statErr := os.Stat(absPath); statErr == nil {
		if wantDir && !info.IsDir() {
			return "", fmt.Errorf("%s %q already exists and is not a directory", flagName, absPath)
		}
		if !wantDir && info.IsDir() {
			return "", fmt.Errorf("%s %q already exists and is a directory", flagName, absPath)
		}
	} else if errors.Is(statErr, syscall.ENOTDIR) || errors.Is(statErr, syscall.ELOOP) {
		return "", fmt.Errorf("%s %q has invalid path: %w", flagName, absPath, statErr)
	}
	return absPath, nil
}

func resolvePathWithSymlinks(absPath string) (string, error) {
	clean := filepath.Clean(absPath)
	if resolved, err := filepath.EvalSymlinks(clean); err == nil {
		return filepath.Clean(resolved), nil
	}
	cur := clean
	var tail []string
	for hops := 0; hops < 40; {
		resolved, err := filepath.EvalSymlinks(cur)
		if err == nil {
			for _, part := range slices.Backward(tail) {
				resolved = filepath.Join(resolved, part)
			}
			return filepath.Clean(resolved), nil
		}
		if !errors.Is(err, os.ErrNotExist) {
			return "", err
		}
		if info, lErr := os.Lstat(cur); lErr == nil && info.Mode()&os.ModeSymlink != 0 {
			target, readErr := os.Readlink(cur)
			if readErr != nil {
				return "", readErr
			}
			if !filepath.IsAbs(target) {
				target = filepath.Join(filepath.Dir(cur), target)
			}
			cur = filepath.Clean(target)
			hops++
			continue
		}
		parent := filepath.Dir(cur)
		if parent == cur {
			return clean, nil
		}
		tail = append(tail, filepath.Base(cur))
		cur = parent
	}
	return "", fmt.Errorf("too many levels of symbolic links: %q", absPath)
}

func isSameOrSubpath(base, target string) bool {
	rel, err := filepath.Rel(base, target)
	if err != nil {
		return false
	}
	return rel == "." || (rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator)))
}

func validatePathNotInMountpoint(flagName, targetPath, rawMP, canonMP string) error {
	if isSameOrSubpath(rawMP, targetPath) || isSameOrSubpath(canonMP, targetPath) {
		return fmt.Errorf("%s %q cannot equal or be inside mountpoint %q", flagName, targetPath, canonMP)
	}
	canonTarget, errTarget := resolvePathWithSymlinks(targetPath)
	if errTarget != nil {
		return fmt.Errorf("failed to resolve %s %q: %w", flagName, targetPath, errTarget)
	}
	if isSameOrSubpath(canonMP, canonTarget) {
		return fmt.Errorf("%s %q cannot equal or be inside mountpoint %q", flagName, targetPath, canonMP)
	}
	return nil
}

func newMountCmd(rawArgs []string) *cobra.Command {
	var (
		foreground bool
		allowOther bool
		directIO   bool
		debug      bool
		statsFile  bool
	)
	opts := mountOptions{
		adminAddr:                defaultAdminAddr,
		logLevel:                 "info",
		logFormat:                "json",
		traceSampleRatio:         defaultTraceSampleRatio,
		mode:                     fusefs.MountModeStaged,
		unmountTimeout:           fusefs.DefaultUnmountTimeout,
		unmountStageTimeout:      fusefs.DefaultUnmountStageTimeout,
		unmountStageStallTimeout: fusefs.DefaultUnmountStageStallTimeout,
		unmountFsyncTimeout:      fusefs.DefaultUnmountFsyncTimeout,
	}

	cmd := &cobra.Command{
		Use:   "mount <disk-id> <mountpoint>",
		Short: "Mount a remote virtual disk to a local directory",
		Args:  cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, posArgs []string) error {
			flags := cmd.Flags()
			set := mountFlagPresence{
				logFile:                  flags.Changed("log-file"),
				adminAddr:                flags.Changed("admin-addr"),
				logLevel:                 flags.Changed("log-level"),
				logFormat:                flags.Changed("log-format"),
				traceSampleRatio:         flags.Changed("trace-sample-ratio"),
				mode:                     flags.Changed("mode"),
				stageSize:                flags.Changed("stage-size"),
				stageDir:                 flags.Changed("stage-dir"),
				unmountTimeout:           flags.Changed("unmount-timeout"),
				unmountStageTimeout:      flags.Changed("unmount-stage-timeout"),
				unmountStageStallTimeout: flags.Changed("unmount-stage-stall-timeout"),
				unmountFsyncTimeout:      flags.Changed("unmount-fsync-timeout"),
			}
			return executeMountValidated(cmd, posArgs, &opts, set, foreground, allowOther, directIO, debug, statsFile, rawArgs)
		},
	}

	flags := cmd.Flags()
	flags.StringVar(&opts.endpoint, "endpoint", "", "StorageService base URL, defaults to $BOWL_STORAGE_ENDPOINT or http://127.0.0.1:9000")
	flags.StringVar(&opts.token, "token", "", "Access token, defaults to $BOWL_TOKEN")
	flags.BoolVarP(&foreground, "foreground", "f", false, "Run in foreground, log to stderr, and exit on SIGINT or SIGTERM")
	flags.BoolVar(&opts.force, "force", false, "Preempt an active read-write mount lease on the disk")
	flags.StringVar(&opts.logFile, "log-file", "", "Log file path for background daemon mode")
	flags.BoolVar(&allowOther, "allow-other", false, "Allow other local users to access the mountpoint")
	flags.StringVar(&opts.mode, "mode", fusefs.MountModeStaged, "Mount mode: staged, sync, or ro; defaults to $BOWL_MOUNT_MODE or staged")
	flags.StringVar(&opts.stageSizeRaw, "stage-size", "", "Local staging capacity limit, minimum 4MiB; defaults to $BOWL_STAGE_SIZE or 512MiB")
	flags.StringVar(&opts.stageDirRaw, "stage-dir", "", "Local staging root directory, defaults to $BOWL_STAGE_DIR or /var/tmp/bowl-stage")
	flags.DurationVar(&opts.unmountTimeout, "unmount-timeout", fusefs.DefaultUnmountTimeout, "Total unmount drain timeout budget, defaults to $BOWL_UNMOUNT_TIMEOUT or 5m")
	flags.DurationVar(&opts.unmountStageTimeout, "unmount-stage-timeout", fusefs.DefaultUnmountStageTimeout, "Staged upload drain budget during unmount (staged mode only), defaults to $BOWL_UNMOUNT_STAGE_TIMEOUT or 4m")
	flags.DurationVar(&opts.unmountStageStallTimeout, "unmount-stage-stall-timeout", fusefs.DefaultUnmountStageStallTimeout, "Staged upload stall timeout without chunk commit progress during unmount (staged mode only), defaults to $BOWL_UNMOUNT_STAGE_STALL_TIMEOUT or 30s")
	flags.DurationVar(&opts.unmountFsyncTimeout, "unmount-fsync-timeout", fusefs.DefaultUnmountFsyncTimeout, "Dirty inode fsync timeout budget during unmount, defaults to $BOWL_UNMOUNT_FSYNC_TIMEOUT or 45s")
	flags.BoolVar(&directIO, "direct-io", false, "Bypass kernel page cache for direct data I/O")
	flags.BoolVar(&debug, "debug", false, "Enable FUSE protocol debug logging")
	flags.BoolVar(&statsFile, "stats-file", false, "Expose hidden virtual .stats file at the mount root")
	flags.StringVar(&opts.adminAddr, "admin-addr", defaultAdminAddr, "Local admin HTTP listen address or off to disable; defaults to $BOWL_ADMIN_ADDR or $OBS_ADMIN_ADDR")
	flags.StringVar(&opts.logLevel, "log-level", "info", "Log level: debug, info, warn, or error; defaults to $BOWL_LOG_LEVEL or $LOG_LEVEL")
	flags.StringVar(&opts.logFormat, "log-format", "json", "Log format: json or text; defaults to $BOWL_LOG_FORMAT or $LOG_FORMAT")
	flags.Float64Var(&opts.traceSampleRatio, "trace-sample-ratio", defaultTraceSampleRatio, "Trace sampling ratio in [0, 1], defaults to $OTEL_TRACES_SAMPLER_ARG")

	return cmd
}

func prepareMountExecution(
	posArgs []string,
	opts *mountOptions,
	set mountFlagPresence,
	foreground, allowOther, directIO, debug, statsFile bool,
) (string, fusefs.Config, error) {
	if valErr := applyEnvAndValidateOptions(opts, set, foreground); valErr != nil {
		return "", fusefs.Config{}, usageExitErr(valErr, false)
	}

	diskIDStr := posArgs[0]
	mountpoint := posArgs[1]

	parsedUUID, err := uuid.Parse(diskIDStr)
	if err != nil || parsedUUID == uuid.Nil {
		return "", fusefs.Config{}, usageExitErr(fmt.Errorf("invalid disk-id %q: must be a valid non-nil UUID", diskIDStr), false)
	}

	absMountpoint, err := filepath.Abs(mountpoint)
	if err != nil {
		return "", fusefs.Config{}, usageExitErr(fmt.Errorf("failed to resolve mountpoint path: %w", err), false)
	}
	absMountpoint = filepath.Clean(absMountpoint)

	canonMountpoint, mounted, err := ResolveMountPoint(absMountpoint)
	if err != nil {
		return "", fusefs.Config{}, runtimeExitErr(fmt.Errorf("failed to check mountpoint status: %w", err))
	}
	if !mounted {
		fi, statErr := os.Stat(absMountpoint)
		if statErr != nil {
			return "", fusefs.Config{}, usageExitErr(fmt.Errorf("mountpoint %q does not exist or cannot be accessed: %w", absMountpoint, statErr), false)
		}
		if !fi.IsDir() {
			return "", fusefs.Config{}, usageExitErr(fmt.Errorf("mountpoint %q is not a directory", absMountpoint), false)
		}
		canonMountpoint, err = filepath.EvalSymlinks(absMountpoint)
		if err != nil {
			return "", fusefs.Config{}, usageExitErr(fmt.Errorf("failed to resolve mountpoint path: %w", err), false)
		}
		canonMountpoint = filepath.Clean(canonMountpoint)
	}

	if opts.mode == fusefs.MountModeStaged {
		if stageErr := validatePathNotInMountpoint("--stage-dir", opts.stageDir, absMountpoint, canonMountpoint); stageErr != nil {
			return "", fusefs.Config{}, usageExitErr(stageErr, false)
		}
	}
	if set.logFile {
		if logErr := validatePathNotInMountpoint("--log-file", opts.logFile, absMountpoint, canonMountpoint); logErr != nil {
			return "", fusefs.Config{}, usageExitErr(logErr, false)
		}
	}

	if mounted {
		return "", fusefs.Config{}, runtimeExitErr(fmt.Errorf("mountpoint %q is already mounted", canonMountpoint))
	}

	opts.endpoint, err = rpc.ResolveEndpoint(opts.endpoint)
	if err != nil {
		return "", fusefs.Config{}, usageExitErr(fmt.Errorf("invalid endpoint: %w", err), false)
	}
	opts.token = rpc.ResolveToken(opts.token)

	baseCfg := fusefs.Config{
		DiskID:                   diskIDStr,
		Mode:                     opts.mode,
		DirectIO:                 directIO,
		AllowOther:               allowOther,
		Debug:                    debug,
		EnableStatsFile:          statsFile,
		StageSize:                opts.stageSizeBytes,
		StageDir:                 opts.stageDir,
		UnmountTimeout:           opts.unmountTimeout,
		UnmountStageTimeout:      opts.unmountStageTimeout,
		UnmountStageStallTimeout: opts.unmountStageStallTimeout,
		UnmountFsyncTimeout:      opts.unmountFsyncTimeout,
	}

	normalizeOTLPEnv()
	return canonMountpoint, baseCfg, nil
}

func executeMountValidated(
	cmd *cobra.Command,
	posArgs []string,
	opts *mountOptions,
	set mountFlagPresence,
	foreground, allowOther, directIO, debug, statsFile bool,
	rawArgs []string,
) error {
	isWorker := os.Getenv("BOWL_DAEMON_WORKER") == "1"
	canonMountpoint, baseCfg, err := prepareMountExecution(posArgs, opts, set, foreground, allowOther, directIO, debug, statsFile)
	if err != nil {
		if isWorker {
			writeDaemonReadinessError(err)
			return &cliExitError{code: 1, silent: true}
		}
		return err
	}

	stderr := cmd.ErrOrStderr()
	if isWorker {
		if code := runDaemonWorker(context.Background(), opts, &baseCfg, canonMountpoint); code != 0 {
			return &cliExitError{code: code, silent: true}
		}
		return nil
	}

	if !foreground {
		if code := spawnDaemon(rawArgs, stderr); code != 0 {
			return &cliExitError{code: code, silent: true}
		}
		return nil
	}

	runnerCtx, runnerCancel := context.WithCancel(context.Background())
	defer runnerCancel()

	if mountErr := runMount(runnerCtx, opts, &baseCfg, canonMountpoint, stderr, nil); mountErr != nil {
		return runtimeExitErr(fmt.Errorf("mount failed: %w", mountErr))
	}
	return nil
}
