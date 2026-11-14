package cli

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"google.golang.org/genproto/googleapis/rpc/errdetails"

	"github.com/starsrc/bowl/client/internal/fusefs"
	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
)

func withCleanOTelAndLogger(t *testing.T) {
	t.Helper()
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "")
	t.Setenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", "")
	prevMP := otel.GetMeterProvider()
	prevTP := otel.GetTracerProvider()
	prevProp := otel.GetTextMapPropagator()
	prevLogger := slog.Default()
	t.Cleanup(func() {
		otel.SetMeterProvider(prevMP)
		otel.SetTracerProvider(prevTP)
		otel.SetTextMapPropagator(prevProp)
		slog.SetDefault(prevLogger)
	})
}

func withServeFUSEStub(t *testing.T, fn func(context.Context, *fusefs.RawFS, *fusefs.Config, string, func()) error) {
	t.Helper()
	prev := serveFUSE
	serveFUSE = fn
	t.Cleanup(func() { serveFUSE = prev })
}

func withSpawnDaemonStub(t *testing.T, fn func([]string, io.Writer) int) {
	t.Helper()
	prev := spawnDaemon
	spawnDaemon = fn
	t.Cleanup(func() { spawnDaemon = prev })
}

func TestMount_GivenHelpAndInvalidArgs_ReturnsExpectedExitCodes(t *testing.T) {
	// Given / When / Then
	requireHelpExit0(t, "bowl mount <disk-id> <mountpoint>", "mount", "-h")
	requireHelpExit0(t, "bowl mount <disk-id> <mountpoint>", "mount", "--help")

	requireCLIError2(t, "需要且仅需要 2 个位置参数", "mount")
	requireCLIError2(t, "需要且仅需要 2 个位置参数", "mount", testDiskID1)
	requireCLIError2(t, "需要且仅需要 2 个位置参数", "mount", testDiskID1, "/tmp", "extra")
	requireCLIError2(t, `未知标志 "--bogus"`, "mount", testDiskID1, "/tmp", "--bogus")
	requireCLIError2(t, "取值无效", "mount", testDiskID1, "/tmp", "--endpoint")
	requireCLIError2(t, "取值无效", "mount", testDiskID1, "/tmp", "--stats-file=not-bool")
	requireCLIError2(t, "无效的 --trace-sample-ratio", "mount", testDiskID1, "/tmp", "--trace-sample-ratio", "-0.1")
	requireCLIError2(t, "无效的 --trace-sample-ratio", "mount", testDiskID1, "/tmp", "--trace-sample-ratio", "1.5")
	requireCLIError2(t, "取值无效", "mount", testDiskID1, "/tmp", "--trace-sample-ratio", "NaN-bad")
	requireCLIError2(t, "无效的 --log-level", "mount", testDiskID1, "/tmp", "--log-level", "verbose")
	requireCLIError2(t, "无效的 --log-format", "mount", testDiskID1, "/tmp", "--log-format", "yaml")
	requireCLIError2(t, "无效的 --admin-addr", "mount", testDiskID1, "/tmp", "--admin-addr", "bad-addr")
	requireCLIError2(t, "无效的 disk-id", "mount", "not-a-uuid", t.TempDir())
	requireCLIError2(t, "无效的 disk-id", "mount", testNilUUID, t.TempDir())

	t.Setenv("OTEL_TRACES_SAMPLER_ARG", "not-float")
	requireCLIError2(t, "无效的环境变量 OTEL_TRACES_SAMPLER_ARG", "mount", testDiskID1, t.TempDir())
	t.Setenv("OTEL_TRACES_SAMPLER_ARG", "")

	// 不存在的挂载点与非目录挂载点
	requireCLIError2(t, "不存在或无法访问", "mount", testDiskID1, filepath.Join(t.TempDir(), "missing"))
	filePath := filepath.Join(t.TempDir(), "not-a-dir")
	require.NoError(t, os.WriteFile(filePath, []byte("x"), 0o600))
	requireCLIError2(t, "不是目录", "mount", testDiskID1, filePath)

	// 非法 endpoint
	requireCLIError2(t, "无效的 endpoint", "mount", testDiskID1, t.TempDir(), "--endpoint", "ftp://127.0.0.1:9000")
}

func TestMount_GivenAlreadyMountedMountpoint_RejectsWithExit1(t *testing.T) {
	// Given
	mp := t.TempDir()
	withMountInfoContent(t, "100 30 0:50 / "+mp+" rw - fuse.bowl bowl:"+testDiskID1+" rw\n")

	// When
	code, out, errOut := runCLI("mount", testDiskID1, mp, "-f")

	// Then
	require.Equal(t, 1, code)
	require.Empty(t, out)
	require.Contains(t, errOut, "当前已挂载")
}

func TestMount_GivenForegroundRW_CompletesHandshakeWritesAndCleansMetaAndLogsLifecycle(t *testing.T) {
	// Given
	withCleanOTelAndLogger(t)
	mp := t.TempDir()
	metaDir := t.TempDir()
	withMissingMountInfo(t)
	withTempMountMetaDirs(t, metaDir)

	storage := newFakeStorage()
	storage.seedDisk(testDiskID1, "mount-disk", 1<<30, 0)
	withMountStorageFake(t, storage)

	var (
		capturedCfg  fusefs.Config
		metaInReady  mountMeta
		metaFoundNow bool
	)
	withServeFUSEStub(t, func(_ context.Context, _ *fusefs.RawFS, cfg *fusefs.Config, gotMP string, ready func()) error {
		capturedCfg = *cfg
		require.Equal(t, mp, gotMP)
		ready()
		metaInReady, metaFoundNow = findMountMeta(mp)
		return nil
	})

	// When
	code, out, errOut := runCLI(
		"mount", testDiskID1, mp,
		"-f", "--ro=false", "--direct-io", "--allow-other", "--debug", "--stats-file",
		"--admin-addr", "off",
		"--log-level", "debug",
		"--log-format", "json",
		"--trace-sample-ratio", "0.5",
		"--token", "mount-tok",
	)

	// Then
	require.Equal(t, 0, code, "stderr=%s", errOut)
	require.Empty(t, out)
	require.Equal(t, "Bearer mount-tok", storage.lastAuth)
	require.Equal(t, uint64(1), capturedCfg.Epoch)
	require.Equal(t, uint64(1), capturedCfg.RootIno)
	require.True(t, capturedCfg.DirectIO)
	require.True(t, capturedCfg.AllowOther)
	require.True(t, capturedCfg.Debug)
	require.True(t, capturedCfg.EnableStatsFile)
	require.True(t, metaFoundNow, "ready 回调后必须写入活跃 mountMeta 文件")
	require.Equal(t, testDiskID1, metaInReady.DiskID)
	require.Equal(t, "off", metaInReady.AdminAddr)
	_, metaAfterExit := findMountMeta(mp)
	require.False(t, metaAfterExit, "挂载退出后必须清理 mountMeta 文件")
	require.Contains(t, errOut, `"msg":"mount_ready"`)
	require.Contains(t, errOut, `"msg":"unmount_complete"`)
}

func TestMount_GivenReadOnlyWithZeroEpochAndEnvFallbacks_Succeeds(t *testing.T) {
	// Given
	withCleanOTelAndLogger(t)
	t.Setenv("BOWL_ADMIN_ADDR", "off")
	t.Setenv("BOWL_LOG_LEVEL", "warn")
	t.Setenv("BOWL_LOG_FORMAT", "text")
	t.Setenv("OTEL_TRACES_SAMPLER_ARG", "0")

	mp := t.TempDir()
	withMissingMountInfo(t)
	withTempMountMetaDirs(t, t.TempDir())

	storage := newFakeStorage()
	storage.seedDisk(testDiskID1, "ro-disk", 1<<30, 0)
	withMountStorageFake(t, storage)

	var gotReadOnly bool
	var gotEpoch uint64
	withServeFUSEStub(t, func(_ context.Context, _ *fusefs.RawFS, cfg *fusefs.Config, _ string, ready func()) error {
		gotReadOnly = cfg.ReadOnly
		gotEpoch = cfg.Epoch
		ready()
		return nil
	})

	// When
	code, _, errOut := runCLI("mount", testDiskID1, mp, "-f", "--ro")

	// Then
	require.Equal(t, 0, code, "stderr=%s", errOut)
	require.True(t, gotReadOnly)
	require.Zero(t, gotEpoch)
	require.Empty(t, storage.getRenewReqs(), "RO 挂载不得调用 RenewMount")
	require.Empty(t, storage.getUnmountReqs(), "RO 挂载不得调用 UnmountDisk")
}

func TestMount_GivenHandshakeFailuresOrServeError_ReturnsExit1(t *testing.T) {
	// Given: 磁盘不存在
	withCleanOTelAndLogger(t)
	mp := t.TempDir()
	withMissingMountInfo(t)
	withTempMountMetaDirs(t, t.TempDir())

	storage := newFakeStorage()
	withMountStorageFake(t, storage)

	// When / Then
	code, _, errOut := runCLI("mount", testDiskID1, mp, "-f", "--admin-addr", "off")
	require.Equal(t, 1, code)
	require.Contains(t, errOut, "磁盘不存在: "+testDiskID1)

	// Given: 返回空 RootInode
	storage.seedDisk(testDiskID1, "d1", 1<<30, 0)
	storage.mountZeroRoot = true
	code, _, errOut = runCLI("mount", testDiskID1, mp, "-f", "--admin-addr", "off")
	require.Equal(t, 1, code)
	require.Contains(t, errOut, "无效的磁盘根目录元数据")
	storage.mountZeroRoot = false

	// Given: 读写挂载返回 epoch=0
	storage.mountZeroEpoch = true
	code, _, errOut = runCLI("mount", testDiskID1, mp, "-f", "--admin-addr", "off")
	require.Equal(t, 1, code)
	require.Contains(t, errOut, "有效的写挂载世代号")
	storage.mountZeroEpoch = false

	// Given: serveFUSE 返回错误时仍应清理 mountMeta
	withServeFUSEStub(t, func(_ context.Context, _ *fusefs.RawFS, _ *fusefs.Config, _ string, ready func()) error {
		ready()
		return errors.New("fuse device failed")
	})
	code, _, errOut = runCLI("mount", testDiskID1, mp, "-f", "--admin-addr", "off")
	require.Equal(t, 1, code)
	require.Contains(t, errOut, "fuse device failed")
	_, remains := findMountMeta(mp)
	require.False(t, remains)
}

func TestMount_GivenBackgroundMode_DispatchesToDaemonSpawnerAndParsesHandshake(t *testing.T) {
	// Given
	mp := t.TempDir()
	withMissingMountInfo(t)
	var spawnedArgs []string
	withSpawnDaemonStub(t, func(args []string, _ io.Writer) int {
		spawnedArgs = append([]string(nil), args...)
		return 0
	})

	// When
	code, _, errOut := runCLI("mount", testDiskID1, mp, "--ro")

	// Then
	require.Equal(t, 0, code)
	require.Empty(t, errOut)
	require.Equal(t, []string{testDiskID1, mp, "--ro"}, spawnedArgs)

	// readDaemonHandshake 三类握手输出校验
	var errBuf strings.Builder
	require.Equal(t, 0, readDaemonHandshake(strings.NewReader("OK\n"), &errBuf))
	require.Empty(t, errBuf.String())

	errBuf.Reset()
	require.Equal(t, 1, readDaemonHandshake(strings.NewReader("ERR:mount failed\n"), &errBuf))
	require.Contains(t, errBuf.String(), "后台守护进程初始化失败: mount failed")

	errBuf.Reset()
	require.Equal(t, 1, readDaemonHandshake(strings.NewReader(""), &errBuf))
	require.Contains(t, errBuf.String(), "未完成挂载握手")
}

func TestResolveLogFile_GivenExplicitAndPermissionFallback_CreatesExpectedFile(t *testing.T) {
	// Given: 显式指定日志路径自动创建父目录
	explicitPath := filepath.Join(t.TempDir(), "nested", "logs", "custom.log")
	f1, gotPath1, err := ResolveLogFile(explicitPath, testDiskID1)
	require.NoError(t, err)
	require.NoError(t, f1.Close())
	require.Equal(t, explicitPath, gotPath1)

	// Given: 系统目录无写权限 (EACCES) 时回退至 $XDG_STATE_HOME/bowl/logs/mount-<id>.log
	prevOpenSys := openSystemLogFn
	openSystemLogFn = func(string) (*os.File, string, error) {
		return nil, "/var/log/bowl/mount.log", syscall.EACCES
	}
	t.Cleanup(func() { openSystemLogFn = prevOpenSys })
	xdgDir := t.TempDir()
	t.Setenv("XDG_STATE_HOME", xdgDir)

	// When
	f2, gotPath2, err := ResolveLogFile("", testDiskID1)

	// Then
	require.NoError(t, err)
	require.NoError(t, f2.Close())
	wantFallback := filepath.Join(xdgDir, "bowl", "logs", "mount-"+testDiskID1+".log")
	require.Equal(t, wantFallback, gotPath2)
}

func withMountRenewIntervalStub(t *testing.T, interval time.Duration) {
	t.Helper()
	prev := mountRenewIntervalFn
	mountRenewIntervalFn = func(uint32) time.Duration { return interval }
	t.Cleanup(func() { mountRenewIntervalFn = prev })
}

func TestRunMount_GivenServeLifecycleAndCanceledCtx_RenewsPeriodicallyAndUnmountsBeforeRemovingMeta(t *testing.T) {
	// Given
	withCleanOTelAndLogger(t)
	mp := t.TempDir()
	metaDir := t.TempDir()
	withMissingMountInfo(t)
	withTempMountMetaDirs(t, metaDir)
	withMountRenewIntervalStub(t, 15*time.Millisecond)

	storage := newFakeStorage()
	storage.seedDisk(testDiskID1, "lease-disk", 1<<30, 0)
	storage.mountLeaseTTL = 30
	withMountStorageFake(t, storage)

	var metaExistsAtUnmount bool
	storage.onUnmountDisk = func(_ *storagev1.UnmountDiskRequest) {
		_, metaExistsAtUnmount = findMountMeta(mp)
	}

	parentCtx, cancelParent := context.WithCancel(context.Background())
	defer cancelParent()

	withServeFUSEStub(t, func(ctx context.Context, _ *fusefs.RawFS, _ *fusefs.Config, _ string, ready func()) error {
		ready()
		require.Eventually(t, func() bool {
			return len(storage.getRenewReqs()) >= 2
		}, time.Second, 5*time.Millisecond, "serveFUSE 阻塞期间必须周期发送 RenewMount")
		// 模拟收到信号导致外层挂载 ctx 被取消后 serveFUSE 退出
		cancelParent()
		<-ctx.Done()
		return nil
	})

	opts := &mountOptions{
		endpoint:         "http://127.0.0.1:9000",
		adminAddr:        "off",
		logLevel:         "info",
		logFormat:        "json",
		traceSampleRatio: 0,
	}
	baseCfg := &fusefs.Config{DiskID: testDiskID1, ReadOnly: false}

	// When
	var logBuf bytes.Buffer
	err := runMount(parentCtx, opts, baseCfg, mp, &logBuf, nil)

	// Then
	require.NoError(t, err)
	renews := storage.getRenewReqs()
	require.GreaterOrEqual(t, len(renews), 2)
	for _, req := range renews {
		require.Equal(t, testDiskID1, req.GetDiskId())
		require.Equal(t, uint64(1), req.GetEpoch())
	}

	unmounts := storage.getUnmountReqs()
	require.Len(t, unmounts, 1, "serveFUSE 返回后必须调用一次 UnmountDisk（即使外层 ctx 已取消）")
	require.Equal(t, testDiskID1, unmounts[0].GetDiskId())
	require.Equal(t, uint64(1), unmounts[0].GetEpoch())
	require.True(t, metaExistsAtUnmount, "UnmountDisk 必须在清理 mountMeta 文件之前调用")
	_, metaAfter := findMountMeta(mp)
	require.False(t, metaAfter, "UnmountDisk 完成后必须移除 mountMeta 文件")
}

func TestRunMount_GivenRenewTerminalError_StopsSubsequentRenewRequests(t *testing.T) {
	ebusyErr := connect.NewError(connect.CodeFailedPrecondition, errors.New("stale epoch"))
	detail, err := connect.NewErrorDetail(&errdetails.ErrorInfo{Domain: "bowl.storage", Reason: "EBUSY"})
	require.NoError(t, err)
	ebusyErr.AddDetail(detail)

	cases := []struct {
		name     string
		renewErr error
	}{
		{name: "CodeFailedPrecondition_EBUSY", renewErr: ebusyErr},
		{name: "CodeNotFound", renewErr: connect.NewError(connect.CodeNotFound, errors.New("mount not found"))},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			withCleanOTelAndLogger(t)
			mp := t.TempDir()
			withMissingMountInfo(t)
			withTempMountMetaDirs(t, t.TempDir())
			withMountRenewIntervalStub(t, 10*time.Millisecond)

			storage := newFakeStorage()
			storage.seedDisk(testDiskID1, "lease-disk", 1<<30, 0)
			storage.renewErr = tc.renewErr
			withMountStorageFake(t, storage)

			var gotFS *fusefs.RawFS
			var canceledOnLeaseLoss bool
			withServeFUSEStub(t, func(serveCtx context.Context, fs *fusefs.RawFS, _ *fusefs.Config, _ string, ready func()) error {
				gotFS = fs
				ready()
				require.Eventually(t, func() bool {
					return len(storage.getRenewReqs()) == 1
				}, time.Second, 5*time.Millisecond)
				select {
				case <-serveCtx.Done():
					canceledOnLeaseLoss = true
				case <-time.After(time.Second):
				}
				return nil
			})

			opts := &mountOptions{
				endpoint:         "http://127.0.0.1:9000",
				adminAddr:        "off",
				logLevel:         "info",
				logFormat:        "json",
				traceSampleRatio: 0,
			}
			baseCfg := &fusefs.Config{DiskID: testDiskID1, ReadOnly: false}

			var logBuf bytes.Buffer
			runErr := runMount(context.Background(), opts, baseCfg, mp, &logBuf, nil)
			require.NoError(t, runErr)
			require.True(t, canceledOnLeaseLoss, "收到 %s 后必须触发 serveCtx 自动下线", tc.name)
			require.NotNil(t, gotFS)
			require.True(t, gotFS.IsLeaseLost(), "收到 %s 后必须标记 RawFS.leaseLost", tc.name)
			require.Len(t, storage.getRenewReqs(), 1, "收到 %s 后必须立即停止后续续期", tc.name)
			require.Empty(t, storage.getUnmountReqs(), "因租约失效自动下线时必须跳过多余的 UnmountDisk RPC")
		})
	}
}

func withMountRenewRetryIntervalStub(t *testing.T, interval time.Duration) {
	t.Helper()
	prev := mountRenewRetryIntervalFn
	mountRenewRetryIntervalFn = func(time.Duration) time.Duration { return interval }
	t.Cleanup(func() { mountRenewRetryIntervalFn = prev })
}

type fakeMountServer struct {
	waitMountFn func() error
	serveFn     func()
	unmountFn   func() error

	mu           sync.Mutex
	unmountCalls int
}

func (s *fakeMountServer) Serve() {
	if s.serveFn != nil {
		s.serveFn()
	}
}

func (s *fakeMountServer) WaitMount() error {
	if s.waitMountFn != nil {
		return s.waitMountFn()
	}
	return nil
}

func (s *fakeMountServer) Unmount() error {
	s.mu.Lock()
	s.unmountCalls++
	fn := s.unmountFn
	s.mu.Unlock()
	if fn != nil {
		return fn()
	}
	return nil
}

func (s *fakeMountServer) getUnmountCalls() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.unmountCalls
}

func TestDefaultMountRenewInterval_ClampsMinimumOneSecond(t *testing.T) {
	require.Equal(t, time.Second, defaultMountRenewInterval(0))
	require.Equal(t, time.Second, defaultMountRenewInterval(1))
	require.Equal(t, time.Second, defaultMountRenewInterval(3))
	require.Equal(t, 10*time.Second, defaultMountRenewInterval(30))
	require.Equal(t, 2*time.Second, defaultMountRenewRetryInterval(10*time.Second))
	require.Equal(t, 500*time.Millisecond, defaultMountRenewRetryInterval(500*time.Millisecond))
}

func TestMount_GivenReadOnlyAndForce_RejectsWithExit2BeforeSpawnDaemon(t *testing.T) {
	mp := t.TempDir()
	withMissingMountInfo(t)
	var spawnCalls int
	withSpawnDaemonStub(t, func([]string, io.Writer) int {
		spawnCalls++
		return 0
	})

	// 后台模式：必须在 spawnDaemon 之前直接以退出码 2 拦截
	code, _, errOut := runCLI("mount", testDiskID1, mp, "--ro", "--force")
	require.Equal(t, 2, code)
	require.Zero(t, spawnCalls, "--ro 与 --force 冲突时不得拉起后台守护进程")
	require.Contains(t, errOut, "--ro 与 --force 不能同时使用")

	// 前台模式：同样以退出码 2 拦截
	code, _, errOut = runCLI("mount", testDiskID1, mp, "-f", "--ro", "--force")
	require.Equal(t, 2, code)
	require.Zero(t, spawnCalls)
	require.Contains(t, errOut, "--ro 与 --force 不能同时使用")
}

func TestMount_GivenForceAndActiveLeaseConflict_SendsForceAndSingleLineError(t *testing.T) {
	withCleanOTelAndLogger(t)
	mp := t.TempDir()
	withMissingMountInfo(t)
	withTempMountMetaDirs(t, t.TempDir())

	storage := newFakeStorage()
	storage.seedDisk(testDiskID1, "fenced-disk", 1<<30, 0)
	storage.enforceRWLease = true
	storage.epochSeq = 1
	storage.activeRWLease[testDiskID1] = 1
	withMountStorageFake(t, storage)

	var gotEpoch uint64
	withServeFUSEStub(t, func(_ context.Context, _ *fusefs.RawFS, cfg *fusefs.Config, _ string, ready func()) error {
		gotEpoch = cfg.Epoch
		ready()
		return nil
	})

	// 1. 存在活跃写租约且未传 --force：返回退出码 1 与单行明确错误
	code, _, errOut := runCLI("mount", testDiskID1, mp, "-f", "--admin-addr", "off")
	require.Equal(t, 1, code)
	require.Contains(t, errOut, "存在未过期的活跃读写挂载，请先卸载或使用 --force 抢占")
	reqs := storage.getMountReqs()
	require.Len(t, reqs, 1)
	require.False(t, reqs[0].GetForce())

	// singleLineError 必须剥离换行，避免 readDaemonHandshake 截断报错
	multilineMsg := "line1\r\nline2\n"
	require.Equal(t, "line1 line2", singleLineError(errors.New(multilineMsg)))

	// 2. 传入 --force：抢占成功并递增 Epoch
	code, _, errOut = runCLI("mount", testDiskID1, mp, "-f", "--force", "--admin-addr", "off")
	require.Equal(t, 0, code, "stderr=%s", errOut)
	require.Equal(t, uint64(2), gotEpoch)
	reqs = storage.getMountReqs()
	require.Len(t, reqs, 2)
	require.True(t, reqs[1].GetForce())
}

func TestMount_GivenRWZeroLeaseTTL_FailsHandshakeWithExit1(t *testing.T) {
	withCleanOTelAndLogger(t)
	mp := t.TempDir()
	withMissingMountInfo(t)
	withTempMountMetaDirs(t, t.TempDir())

	storage := newFakeStorage()
	storage.seedDisk(testDiskID1, "no-ttl-disk", 1<<30, 0)
	storage.mountZeroTTL = true
	withMountStorageFake(t, storage)

	code, _, errOut := runCLI("mount", testDiskID1, mp, "-f", "--admin-addr", "off")
	require.Equal(t, 1, code)
	require.Contains(t, errOut, "有效的写挂载租约时长")
}

func TestRunMount_GivenRenewTransientError_SchedulesShortRetryAndKeepsServing(t *testing.T) {
	withCleanOTelAndLogger(t)
	mp := t.TempDir()
	withMissingMountInfo(t)
	withTempMountMetaDirs(t, t.TempDir())
	withMountRenewIntervalStub(t, 300*time.Millisecond)
	withMountRenewRetryIntervalStub(t, 15*time.Millisecond)

	storage := newFakeStorage()
	storage.seedDisk(testDiskID1, "retry-disk", 1<<30, 0)
	var renewCalls atomic.Int32
	var secondCallAt atomic.Int64
	var firstCallAt time.Time
	var mu sync.Mutex
	storage.onRenewMount = func(_ *storagev1.RenewMountRequest) (*connect.Response[storagev1.RenewMountResponse], error) {
		n := renewCalls.Add(1)
		if n == 1 {
			mu.Lock()
			firstCallAt = time.Now()
			mu.Unlock()
			return nil, connect.NewError(connect.CodeUnavailable, errors.New("transient network blip"))
		}
		if n == 2 {
			secondCallAt.Store(time.Now().UnixNano())
		}
		return connect.NewResponse(&storagev1.RenewMountResponse{LeaseTtlSeconds: 30}), nil
	}
	withMountStorageFake(t, storage)

	var gotFS *fusefs.RawFS
	withServeFUSEStub(t, func(serveCtx context.Context, fs *fusefs.RawFS, _ *fusefs.Config, _ string, ready func()) error {
		gotFS = fs
		ready()
		require.Eventually(t, func() bool {
			return renewCalls.Load() >= 2
		}, time.Second, 5*time.Millisecond)
		require.NoError(t, serveCtx.Err(), "临时续期失败不得触发 serveCtx 取消")
		return nil
	})

	opts := &mountOptions{
		endpoint:         "http://127.0.0.1:9000",
		adminAddr:        "off",
		logLevel:         "info",
		logFormat:        "json",
		traceSampleRatio: 0,
	}
	baseCfg := &fusefs.Config{DiskID: testDiskID1, ReadOnly: false}

	var logBuf bytes.Buffer
	err := runMount(context.Background(), opts, baseCfg, mp, &logBuf, nil)
	require.NoError(t, err)
	require.NotNil(t, gotFS)
	require.False(t, gotFS.IsLeaseLost())
	require.Len(t, storage.getUnmountReqs(), 1)

	mu.Lock()
	elapsed := time.Unix(0, secondCallAt.Load()).Sub(firstCallAt)
	mu.Unlock()
	require.Less(t, elapsed, 150*time.Millisecond, "临时错误后必须按短重试间隔调度续期，而非等满正常周期")
}

func TestRunMount_GivenForeignMountMetaPID_PreservesNewMetaOnExit(t *testing.T) {
	withCleanOTelAndLogger(t)
	mp := t.TempDir()
	withMissingMountInfo(t)
	withTempMountMetaDirs(t, t.TempDir())

	storage := newFakeStorage()
	storage.seedDisk(testDiskID1, "remount-meta-disk", 1<<30, 0)
	withMountStorageFake(t, storage)

	var metaPath string
	foreignPID := os.Getpid() + 10000
	withServeFUSEStub(t, func(_ context.Context, _ *fusefs.RawFS, _ *fusefs.Config, mountpoint string, ready func()) error {
		ready()
		p, ok := mountMetaFilePath(mountpoint)
		require.True(t, ok)
		metaPath = p
		// 模拟懒卸载后新守护进程覆盖了同路径 mountMeta 文件
		writtenPath, werr := writeMountMeta(&mountMeta{
			PID:        foreignPID,
			DiskID:     testDiskID1,
			Mountpoint: mountpoint,
		})
		require.NoError(t, werr)
		require.Equal(t, p, writtenPath)
		return nil
	})

	opts := &mountOptions{
		endpoint:         "http://127.0.0.1:9000",
		adminAddr:        "off",
		logLevel:         "info",
		logFormat:        "json",
		traceSampleRatio: 0,
	}
	baseCfg := &fusefs.Config{DiskID: testDiskID1, ReadOnly: false}

	var logBuf bytes.Buffer
	err := runMount(context.Background(), opts, baseCfg, mp, &logBuf, nil)
	require.NoError(t, err)
	require.NotEmpty(t, metaPath)
	require.Equal(t, foreignPID, readMountMetaPID(metaPath), "归属其他 PID 的 mountMeta 不得被旧进程误删")
}

func TestServeFUSEWithServer_GivenWaitMountSignalAndBusyUnmount_FallsBackToLazyAndUnblocks(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	t.Run("SignalBeforeWaitMountDone_TriggersUnmountImmediately", func(t *testing.T) {
		mp := t.TempDir()
		sigCh := make(chan os.Signal, 2)
		serveRelease := make(chan struct{})
		srv := &fakeMountServer{
			serveFn: func() { <-serveRelease },
			waitMountFn: func() error {
				sigCh <- syscall.SIGINT
				require.Eventually(t, func() bool {
					select {
					case <-serveRelease:
						return true
					default:
						return false
					}
				}, time.Second, 5*time.Millisecond)
				return nil
			},
			unmountFn: func() error {
				select {
				case <-serveRelease:
				default:
					close(serveRelease)
				}
				return nil
			},
		}

		err := serveFUSEWithServer(context.Background(), srv, nil, mp, logger, sigCh, nil)
		require.NoError(t, err)
		require.GreaterOrEqual(t, srv.getUnmountCalls(), 1)
	})

	t.Run("CtxCancelWithBusyUnmount_FallsBackToLazyUmountAndHandlesSecondSignal", func(t *testing.T) {
		mp := t.TempDir()
		withMissingMountInfo(t)
		calledName, calledArgs := withUmountToolStub(t, "fusermount3", "/usr/bin/fusermount3", nil, nil)

		sigCh := make(chan os.Signal, 2)
		serveRelease := make(chan struct{})
		t.Cleanup(func() { close(serveRelease) })

		srv := &fakeMountServer{
			serveFn:     func() { <-serveRelease },
			waitMountFn: func() error { return nil },
			unmountFn:   func() error { return errors.New("device or resource busy") },
		}

		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		ready := make(chan struct{})
		go func() {
			done <- serveFUSEWithServer(ctx, srv, nil, mp, logger, sigCh, func() { close(ready) })
		}()
		<-ready

		// 触发租约失活取消 ctx，因 server.Unmount() 失败必须回退到懒卸载工具
		cancel()
		require.Eventually(t, func() bool {
			return *calledName != ""
		}, time.Second, 5*time.Millisecond)
		require.Equal(t, "/usr/bin/fusermount3", *calledName)
		require.Equal(t, []string{"-uz", mp}, *calledArgs)

		// 模拟外层退出前再次发送 SIGTERM（此时挂载点已不可见），必须通过 forceExitCh 正常解除阻塞
		sigCh <- syscall.SIGTERM
		select {
		case err := <-done:
			require.NoError(t, err)
		case <-time.After(time.Second):
			t.Fatal("ctx 取消后继续监听后续信号不得卡死")
		}
	})
}
