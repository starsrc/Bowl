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
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"connectrpc.com/connect"
	"github.com/google/uuid"
	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/starsrc/pkg/serverx"

	"github.com/starsrc/bowl/client/internal/fusefs"
	"github.com/starsrc/bowl/client/internal/rpc"
	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
	"github.com/starsrc/bowl/gen/go/storage/v1/storagev1connect"
)

const mountUsageHelp = `用法:
  bowl mount <disk-id> <mountpoint> [flags]

参数:
  <disk-id>       目标虚拟磁盘 UUID（必填，非 Nil UUID）
  <mountpoint>    本地挂载点目录路径（必填，已存在的目录）

标志:
  --endpoint string              StorageService 基础地址 (默认读取 $BOWL_STORAGE_ENDPOINT 或 http://127.0.0.1:9000)
  --token string                 访问令牌 (默认读取 $BOWL_TOKEN)
  -f, --foreground               前台阻塞模式运行，输出日志到标准错误并响应 SIGINT/SIGTERM 退出
  --log-file string              日志文件路径 (仅用于后台守护模式)
  --allow-other                  允许本机其他非 root 用户访问挂载点
  --ro                           只读挂载
  --force                        强制抢占当前持有未过期写租约的活跃读写挂载（不可与 --ro 同时使用）
  --direct-io                    绕过内核页缓存直通数据
  --debug                        开启 FUSE 协议调试日志
  --admin-addr string            本地管理面 HTTP 监听地址，"off" 关闭 (默认读取 $BOWL_ADMIN_ADDR/$OBS_ADMIN_ADDR 或 127.0.0.1:0)
  --stats-file                   在挂载根目录暴露隐藏虚拟文件 .stats (juicefs bench 兼容)
  --log-level string             日志级别: debug|info|warn|error (默认读取 $BOWL_LOG_LEVEL/$LOG_LEVEL 或 info)
  --log-format string            日志格式: json|text (默认读取 $BOWL_LOG_FORMAT/$LOG_FORMAT 或 json)
  --trace-sample-ratio float     链路采样率 [0,1] (默认读取 $OTEL_TRACES_SAMPLER_ARG 或 0.001)
  -h, --help                     查看帮助信息
`

const (
	defaultAdminAddr        = "127.0.0.1:0"
	adminAddrOff            = "off"
	defaultTraceSampleRatio = 0.001
	mountUnmountRPCTimeout  = 3 * time.Second
	mountRenewRPCTimeout    = 5 * time.Second
	mountRenewRetryInterval = 2 * time.Second
)

// mountOptions 汇总 mount 命令解析后的非 fusefs 参数（daemon 子进程通过原样透传 argv 获得同一组值）。
type mountOptions struct {
	endpoint         string
	token            string
	logFile          string
	adminAddr        string
	logLevel         string
	logFormat        string
	traceSampleRatio float64
	force            bool
}

type mountFlagPresence struct {
	adminAddr        bool
	logLevel         bool
	logFormat        bool
	traceSampleRatio bool
}

// serveFUSE、mountHTTPClient、mountRenewIntervalFn 与 mountRenewRetryIntervalFn 为测试接缝：
// ready 必须在内核挂载就绪后调用一次。
var (
	serveFUSE                 = defaultServeFUSE
	mountHTTPClient           *http.Client
	mountRenewIntervalFn      = defaultMountRenewInterval
	mountRenewRetryIntervalFn = defaultMountRenewRetryInterval
)

type mountLeaseInfo struct {
	leaseTTLSeconds uint32
}

func defaultMountRenewInterval(leaseTTLSeconds uint32) time.Duration {
	d := time.Duration(leaseTTLSeconds) * time.Second / 3
	if d < time.Second {
		return time.Second
	}
	return d
}

func defaultMountRenewRetryInterval(normalInterval time.Duration) time.Duration {
	if normalInterval < mountRenewRetryInterval {
		return normalInterval
	}
	return mountRenewRetryInterval
}

// mountHandshake 向服务端发起单次 MountDisk 握手，返回注入了权威 Epoch、RootIno 与缓存超时配置的 fusefs.Config 及租约信息。
func mountHandshake(ctx context.Context, client storagev1connect.StorageServiceClient, baseCfg *fusefs.Config, force bool) (*fusefs.Config, mountLeaseInfo, error) {
	rpcCtx, rpcCancel := context.WithTimeout(ctx, 10*time.Second)
	defer rpcCancel()

	resp, err := client.MountDisk(rpcCtx, connect.NewRequest(&storagev1.MountDiskRequest{
		DiskId:   baseCfg.DiskID,
		ReadOnly: baseCfg.ReadOnly,
		Force:    force,
	}))
	if err != nil {
		if connectErr, ok := errors.AsType[*connect.Error](err); ok {
			switch connectErr.Code() {
			case connect.CodeNotFound:
				return nil, mountLeaseInfo{}, fmt.Errorf("磁盘不存在: %s", baseCfg.DiskID)
			case connect.CodeAborted:
				info := serverx.ExtractErrorInfo(err)
				if info == nil || info.GetReason() == "" || info.GetReason() == "EBUSY" {
					return nil, mountLeaseInfo{}, fmt.Errorf("磁盘 %s 存在未过期的活跃读写挂载，请先卸载或使用 --force 抢占", baseCfg.DiskID)
				}
			}
		}
		return nil, mountLeaseInfo{}, fmt.Errorf("挂载磁盘握手失败: %s", singleLineError(err))
	}

	rootInode := resp.Msg.GetRootInode()
	if rootInode == nil || rootInode.GetIno() == 0 {
		return nil, mountLeaseInfo{}, errors.New("服务端返回无效的磁盘根目录元数据")
	}

	epoch := resp.Msg.GetEpoch()
	ttlSeconds := resp.Msg.GetLeaseTtlSeconds()
	if !baseCfg.ReadOnly {
		if epoch == 0 {
			return nil, mountLeaseInfo{}, errors.New("服务端未返回有效的写挂载世代号")
		}
		if ttlSeconds == 0 {
			return nil, mountLeaseInfo{}, errors.New("服务端未返回有效的写挂载租约时长")
		}
	}

	cfg := *baseCfg
	cfg.Epoch = epoch
	cfg.RootIno = rootInode.GetIno()
	cfg.RootInode = rootInode
	cfg.EntryTimeoutSeconds = resp.Msg.GetEntryTimeoutSeconds()
	cfg.AttrTimeoutSeconds = resp.Msg.GetAttrTimeoutSeconds()
	lease := mountLeaseInfo{
		leaseTTLSeconds: ttlSeconds,
	}
	return &cfg, lease, nil
}

// startMountLeaseRenewer 启动后台租约续期协程，并在收到停止函数调用时阻塞等待协程安全退出。
// 只读挂载跳过续期；遇到临时错误时调度短间隔重试，遇到终态拒绝时调用 onLeaseLost 触发下线。
func startMountLeaseRenewer(
	client storagev1connect.StorageServiceClient,
	cfg *fusefs.Config,
	lease mountLeaseInfo,
	logger *slog.Logger,
	onLeaseLost func(),
) func() {
	if cfg == nil || cfg.ReadOnly || cfg.Epoch == 0 || lease.leaseTTLSeconds == 0 {
		return func() {}
	}
	renewCtx, renewCancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	currentTTL := lease.leaseTTLSeconds
	interval := mountRenewIntervalFn(currentTTL)

	go func() {
		defer close(done)
		timer := time.NewTimer(interval)
		defer timer.Stop()

		for {
			select {
			case <-renewCtx.Done():
				return
			case <-timer.C:
			}

			rpcCtx, rpcCancel := context.WithTimeout(renewCtx, mountRenewRPCTimeout)
			resp, err := client.RenewMount(rpcCtx, connect.NewRequest(&storagev1.RenewMountRequest{
				DiskId: cfg.DiskID,
				Epoch:  cfg.Epoch,
			}))
			rpcCancel()

			if err != nil {
				if renewCtx.Err() != nil {
					return
				}
				if connectErr, ok := errors.AsType[*connect.Error](err); ok {
					code := connectErr.Code()
					if code == connect.CodeFailedPrecondition || code == connect.CodeNotFound {
						logger.Warn("mount_lease_renew_stopped",
							slog.String("disk_id", cfg.DiskID),
							slog.Uint64("epoch", cfg.Epoch),
							slog.String("code", code.String()),
							slog.String("error", err.Error()),
						)
						if onLeaseLost != nil {
							onLeaseLost()
						}
						return
					}
				}
				logger.Warn("mount_lease_renew_failed",
					slog.String("disk_id", cfg.DiskID),
					slog.Uint64("epoch", cfg.Epoch),
					slog.String("error", err.Error()),
				)
				timer.Reset(mountRenewRetryIntervalFn(interval))
				continue
			}

			if nextTTL := resp.Msg.GetLeaseTtlSeconds(); nextTTL > 0 && nextTTL != currentTTL {
				currentTTL = nextTTL
				interval = mountRenewIntervalFn(currentTTL)
			}
			timer.Reset(interval)
		}
	}()

	return func() {
		renewCancel()
		<-done
	}
}

// unmountMountLease 使用独立超时上下文释放读写挂载写租约（只读挂载自动跳过）。
func unmountMountLease(client storagev1connect.StorageServiceClient, cfg *fusefs.Config, logger *slog.Logger) {
	if cfg == nil || cfg.ReadOnly || cfg.Epoch == 0 {
		return
	}
	unmountCtx, unmountCancel := context.WithTimeout(context.Background(), mountUnmountRPCTimeout)
	defer unmountCancel()
	if _, err := client.UnmountDisk(unmountCtx, connect.NewRequest(&storagev1.UnmountDiskRequest{
		DiskId: cfg.DiskID,
		Epoch:  cfg.Epoch,
	})); err != nil {
		logger.Warn("unmount_disk_rpc_failed",
			slog.String("disk_id", cfg.DiskID),
			slog.Uint64("epoch", cfg.Epoch),
			slog.String("error", err.Error()),
		)
	}
}

type fuseMountServer interface {
	Serve()
	WaitMount() error
	Unmount() error
}

func unmountServerWithLazyFallback(server fuseMountServer, mountpoint string, logger *slog.Logger) {
	err := server.Unmount()
	if err == nil {
		return
	}
	if logger != nil {
		logger.Warn("fuse_unmount_failed_fallback_lazy",
			slog.String("mountpoint", mountpoint),
			slog.String("error", err.Error()),
		)
	}
	binPath, cmdArgs, found := findUmountCommand(mountpoint, true)
	if !found {
		if logger != nil {
			logger.Warn("fuse_lazy_unmount_tool_missing", slog.String("mountpoint", mountpoint))
		}
		return
	}
	if out, cmdErr := runCommand(binPath, cmdArgs...); cmdErr != nil && logger != nil {
		logger.Warn("fuse_lazy_unmount_failed",
			slog.String("mountpoint", mountpoint),
			slog.String("error", cmdErr.Error()),
			slog.String("output", string(out)),
		)
	}
}

// defaultServeFUSE 启动 FUSE 服务，就绪后调用 ready，并阻塞到卸载（信号或 ctx 取消触发）。
func defaultServeFUSE(ctx context.Context, fs *fusefs.RawFS, cfg *fusefs.Config, mountpoint string, ready func()) error {
	opts := fusefs.BuildMountOptions(*cfg)

	server, err := fuse.NewServer(fs, mountpoint, opts)
	if err != nil {
		return fmt.Errorf("创建 FUSE 服务失败: %w", err)
	}

	return serveFUSEWithServer(ctx, server, fs, mountpoint, cfg.Logger, nil, ready)
}

func serveFUSEWithServer(
	ctx context.Context,
	server fuseMountServer,
	fs *fusefs.RawFS,
	mountpoint string,
	logger *slog.Logger,
	sigCh chan os.Signal,
	ready func(),
) error {
	if sigCh == nil {
		sigCh = make(chan os.Signal, 2)
		signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
		defer signal.Stop(sigCh)
	}

	stopWait := make(chan struct{})
	forceExitCh := make(chan struct{})
	watchDone := make(chan struct{})
	ctxDone := ctx.Done()
	var unmountTriggered bool

	go func() {
		defer close(watchDone)
		for {
			select {
			case <-sigCh:
				if unmountTriggered {
					mounted, mErr := IsMountPoint(mountpoint)
					if mErr == nil && !mounted {
						close(forceExitCh)
						return
					}
				}
				unmountTriggered = true
				unmountServerWithLazyFallback(server, mountpoint, logger)
			case <-ctxDone:
				unmountTriggered = true
				unmountServerWithLazyFallback(server, mountpoint, logger)
				ctxDone = nil
			case <-stopWait:
				return
			}
		}
	}()

	serveDone := make(chan struct{})
	go func() {
		defer close(serveDone)
		server.Serve()
	}()

	if waitErr := server.WaitMount(); waitErr != nil {
		close(stopWait)
		<-watchDone
		if !unmountTriggered {
			unmountServerWithLazyFallback(server, mountpoint, logger)
		}
		select {
		case <-serveDone:
		case <-forceExitCh:
		}
		return fmt.Errorf("等待挂载就绪失败: %w", waitErr)
	}
	if fs != nil {
		fs.BindMountpoint(mountpoint)
	}
	if ready != nil {
		ready()
	}

	select {
	case <-serveDone:
		close(stopWait)
		<-watchDone
	case <-forceExitCh:
		close(stopWait)
		<-watchDone
	}
	return nil
}

// runMount 装配可观测性运行时、RPC 客户端、管理面与 FUSE 服务，执行单次 MountDisk 握手后阻塞到卸载。
// logWriter 为结构化日志输出目标；onReady 在挂载就绪且发现元数据写入后调用（daemon 用于回传握手）。
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

	rpcMetrics := rpc.NewMetrics()
	client, err := rpc.NewClientFromConfig(rpc.ClientConfig{
		Endpoint:   opts.endpoint,
		Token:      opts.token,
		HTTPClient: mountHTTPClient,
		Logger:     logger,
		Metrics:    rpcMetrics,
	})
	if err != nil {
		return fmt.Errorf("创建 RPC 客户端失败: %w", err)
	}

	cfg, lease, err := mountHandshake(ctx, client, baseCfg, opts.force)
	if err != nil {
		return err
	}
	cfg.Logger = logger
	cfg.Metrics = fusefs.NewMetrics()
	cfg.TracerProvider = obs.TracerProvider()

	rawFS := fusefs.NewRawFS(client, *cfg)
	if regErr := obs.RegisterCollector(rpcMetrics, rawFS.Collector()); regErr != nil {
		unmountMountLease(client, cfg, logger)
		return fmt.Errorf("注册指标收集器失败: %w", regErr)
	}

	admin, err := startAdminServer(opts.adminAddr, obs, rawFS)
	if err != nil {
		unmountMountLease(client, cfg, logger)
		return err
	}
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		admin.shutdown(shutdownCtx)
	}()

	var (
		metaPath  string
		startedAt time.Time
		selfPID   = os.Getpid()
	)
	ready := func() {
		startedAt = time.Now().UTC()
		meta := mountMeta{
			Mountpoint: mountpoint,
			PID:        selfPID,
			DiskID:     cfg.DiskID,
			AdminAddr:  admin.addr(),
			StartedAt:  startedAt,
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

	serveCtx, serveCancel := context.WithCancel(ctx)
	defer serveCancel()

	var leaseLost atomic.Bool
	onLeaseLost := func() {
		leaseLost.Store(true)
		rawFS.MarkLeaseLost()
		serveCancel()
	}

	stopRenew := startMountLeaseRenewer(client, cfg, lease, logger, onLeaseLost)
	serveErr := serveFUSE(serveCtx, rawFS, cfg, mountpoint, ready)
	stopRenew()
	if !leaseLost.Load() {
		unmountMountLease(client, cfg, logger)
	}
	if metaPath != "" && readMountMetaPID(metaPath) == selfPID {
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

// takeFlagValue 读取 "--name=value" 或 "--name value" 形式的标志取值。
func takeFlagValue(args []string, i *int, name, val string, hasVal bool) (string, error) {
	if hasVal {
		return val, nil
	}
	if *i+1 < len(args) {
		*i++
		return args[*i], nil
	}
	return "", fmt.Errorf("--%s 需要指定参数值", name)
}

func firstNonEmptyEnv(keys ...string) string {
	for _, k := range keys {
		if v := strings.TrimSpace(os.Getenv(k)); v != "" {
			return v
		}
	}
	return ""
}

// applyEnvAndValidateOptions 应用环境变量兜底并校验可观测性相关标志。
func applyEnvAndValidateOptions(o *mountOptions, set mountFlagPresence) error {
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
				return fmt.Errorf("无效的环境变量 OTEL_TRACES_SAMPLER_ARG=%q: %w", env, err)
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
		return fmt.Errorf("无效的 --log-level \"%s\"，可选 debug|info|warn|error", o.logLevel)
	}
	switch strings.ToLower(strings.TrimSpace(o.logFormat)) {
	case "json", "text":
	default:
		return fmt.Errorf("无效的 --log-format \"%s\"，可选 json|text", o.logFormat)
	}
	if math.IsNaN(o.traceSampleRatio) || math.IsInf(o.traceSampleRatio, 0) || o.traceSampleRatio < 0 || o.traceSampleRatio > 1 {
		return fmt.Errorf("无效的 --trace-sample-ratio %v，取值范围 [0,1]", o.traceSampleRatio)
	}
	return nil
}

// executeMount 解析 mount 命令参数并启动挂载流程。
func executeMount(args []string, stdout, stderr io.Writer) int {
	var (
		foreground bool
		allowOther bool
		ro         bool
		force      bool
		directIO   bool
		debug      bool
		statsFile  bool
		help       bool
		set        mountFlagPresence
	)
	opts := mountOptions{
		adminAddr:        defaultAdminAddr,
		logLevel:         "info",
		logFormat:        "json",
		traceSampleRatio: defaultTraceSampleRatio,
	}

	var posArgs []string
	for i := 0; i < len(args); i++ {
		arg := args[i]
		if arg == "--" {
			posArgs = append(posArgs, args[i+1:]...)
			break
		}
		if !strings.HasPrefix(arg, "-") {
			posArgs = append(posArgs, arg)
			continue
		}
		name, val, hasVal := strings.Cut(strings.TrimLeft(arg, "-"), "=")
		var err error
		switch name {
		case "h", "help":
			help, err = parseBoolFlag(val, hasVal)
		case "f", "foreground":
			foreground, err = parseBoolFlag(val, hasVal)
		case "allow-other":
			allowOther, err = parseBoolFlag(val, hasVal)
		case "ro":
			ro, err = parseBoolFlag(val, hasVal)
		case "force":
			force, err = parseBoolFlag(val, hasVal)
		case "direct-io":
			directIO, err = parseBoolFlag(val, hasVal)
		case "debug":
			debug, err = parseBoolFlag(val, hasVal)
		case "stats-file":
			statsFile, err = parseBoolFlag(val, hasVal)
		case "endpoint":
			opts.endpoint, err = takeFlagValue(args, &i, name, val, hasVal)
		case "token":
			opts.token, err = takeFlagValue(args, &i, name, val, hasVal)
		case "log-file":
			opts.logFile, err = takeFlagValue(args, &i, name, val, hasVal)
		case "admin-addr":
			set.adminAddr = true
			opts.adminAddr, err = takeFlagValue(args, &i, name, val, hasVal)
		case "log-level":
			set.logLevel = true
			opts.logLevel, err = takeFlagValue(args, &i, name, val, hasVal)
		case "log-format":
			set.logFormat = true
			opts.logFormat, err = takeFlagValue(args, &i, name, val, hasVal)
		case "trace-sample-ratio":
			set.traceSampleRatio = true
			var raw string
			if raw, err = takeFlagValue(args, &i, name, val, hasVal); err == nil {
				opts.traceSampleRatio, err = strconv.ParseFloat(raw, 64)
			}
		default:
			fmt.Fprintf(stderr, "错误: 未知标志 \"%s\"\n\n%s", arg, mountUsageHelp)
			return 2
		}
		if err != nil {
			fmt.Fprintf(stderr, "错误: 标志 \"%s\" 取值无效: %v\n", arg, err)
			return 2
		}
	}

	if help {
		fmt.Fprint(stdout, mountUsageHelp)
		return 0
	}

	if ro && force {
		fmt.Fprintln(stderr, "错误: --ro 与 --force 不能同时使用")
		return 2
	}

	if len(posArgs) != 2 {
		fmt.Fprintf(stderr, "错误: mount 命令需要且仅需要 2 个位置参数 (<disk-id> 与 <mountpoint>)\n\n%s", mountUsageHelp)
		return 2
	}

	opts.force = force

	if valErr := applyEnvAndValidateOptions(&opts, set); valErr != nil {
		fmt.Fprintf(stderr, "错误: %v\n", valErr)
		return 2
	}

	diskIDStr := posArgs[0]
	mountpoint := posArgs[1]

	// 1. 静态前置校验: disk-id 必须为合法且非 Nil UUID
	parsedUUID, err := uuid.Parse(diskIDStr)
	if err != nil || parsedUUID == uuid.Nil {
		fmt.Fprintf(stderr, "错误: 无效的 disk-id \"%s\"，必须为合法的非 Nil UUID\n", diskIDStr)
		return 2
	}

	// 2. 静态前置校验: 挂载点必须为已存在的目录
	absMountpoint, err := filepath.Abs(mountpoint)
	if err != nil {
		fmt.Fprintf(stderr, "错误: 挂载点路径解析失败: %v\n", err)
		return 2
	}
	absMountpoint = filepath.Clean(absMountpoint)

	fi, err := os.Stat(absMountpoint)
	if err != nil {
		fmt.Fprintf(stderr, "错误: 挂载点 \"%s\" 不存在或无法访问: %v\n", absMountpoint, err)
		return 2
	}
	if !fi.IsDir() {
		fmt.Fprintf(stderr, "错误: 挂载点 \"%s\" 不是目录\n", absMountpoint)
		return 2
	}

	// 挂载前重复挂载校验：解析 /proc/self/mountinfo，若目标 <mountpoint> 已是活跃挂载点，在前台立即报错
	mounted, err := IsMountPoint(absMountpoint)
	if err != nil {
		fmt.Fprintf(stderr, "错误: 检查挂载点状态失败: %v\n", err)
		return 1
	}
	if mounted {
		fmt.Fprintf(stderr, "错误: 目标挂载点 \"%s\" 当前已挂载\n", absMountpoint)
		return 1
	}

	// 3. 解析 endpoint 与 token
	opts.endpoint, err = rpc.ResolveEndpoint(opts.endpoint)
	if err != nil {
		fmt.Fprintf(stderr, "错误: 无效的 endpoint: %v\n", err)
		return 2
	}
	opts.token = rpc.ResolveToken(opts.token)

	baseCfg := fusefs.Config{
		DiskID:          diskIDStr,
		ReadOnly:        ro,
		DirectIO:        directIO,
		AllowOther:      allowOther,
		Debug:           debug,
		EnableStatsFile: statsFile,
	}

	// 4. 归一化 OTLP 环境变量（对缺 http:///https:// 的裸 host:port 自动补齐 http://，确保守护子进程同享）
	normalizeOTLPEnv()

	// 5. 模式选择：检测是否为内部后台工作子进程
	if os.Getenv("BOWL_DAEMON_WORKER") == "1" {
		return runDaemonWorker(context.Background(), &opts, &baseCfg, absMountpoint)
	}

	// 若未指定 -f / --foreground，执行默认后台守护派生与就绪握手（原样透传 argv，子进程重新解析同一组标志）
	if !foreground {
		return spawnDaemon(args, stderr)
	}

	runnerCtx, runnerCancel := context.WithCancel(context.Background())
	defer runnerCancel()

	if mountErr := runMount(runnerCtx, &opts, &baseCfg, absMountpoint, stderr, nil); mountErr != nil {
		fmt.Fprintf(stderr, "错误: 挂载执行失败: %v\n", mountErr)
		return 1
	}

	return 0
}

func parseBoolFlag(val string, hasVal bool) (bool, error) {
	if !hasVal {
		return true, nil
	}
	return strconv.ParseBool(val)
}
