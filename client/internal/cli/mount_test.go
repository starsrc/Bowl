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
	"syscall"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"google.golang.org/genproto/googleapis/rpc/errdetails"

	"github.com/starsrc/bowl/client/internal/fusefs"
	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
	"github.com/starsrc/bowl/pkg/rpcerr"
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
		require.Equal(t, "01950000-0000-7000-9000-000000000001", req.GetMountId())
		require.False(t, req.GetReadOnly())
		require.Equal(t, uint64(1), req.GetEpoch())
	}

	unmounts := storage.getUnmountReqs()
	require.Len(t, unmounts, 1, "serveFUSE 返回后必须调用一次 UnmountDisk（即使外层 ctx 已取消）")
	require.Equal(t, testDiskID1, unmounts[0].GetDiskId())
	require.Equal(t, "01950000-0000-7000-9000-000000000001", unmounts[0].GetMountId())
	require.True(t, metaExistsAtUnmount, "UnmountDisk 必须在清理 mountMeta 文件之前调用")
	_, metaAfter := findMountMeta(mp)
	require.False(t, metaAfter, "UnmountDisk 完成后必须移除 mountMeta 文件")
}

func TestRunMount_GivenRenewTerminalError_StopsSubsequentRenewRequests(t *testing.T) {
	ebusyErr := connect.NewError(connect.CodeFailedPrecondition, errors.New("stale epoch"))
	detail, err := connect.NewErrorDetail(&errdetails.ErrorInfo{Domain: rpcerr.Domain, Reason: rpcerr.ReasonBusy})
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

			withServeFUSEStub(t, func(_ context.Context, _ *fusefs.RawFS, _ *fusefs.Config, _ string, ready func()) error {
				ready()
				require.Eventually(t, func() bool {
					return len(storage.getRenewReqs()) == 1
				}, time.Second, 5*time.Millisecond)
				time.Sleep(60 * time.Millisecond)
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
			require.Len(t, storage.getRenewReqs(), 1, "收到 %s 后必须立即停止后续续期", tc.name)
			require.Len(t, storage.getUnmountReqs(), 1)
		})
	}
}

func TestDefaultMountRenewInterval_ClampsMinimumOneSecond(t *testing.T) {
	require.Equal(t, time.Second, defaultMountRenewInterval(0))
	require.Equal(t, time.Second, defaultMountRenewInterval(1))
	require.Equal(t, time.Second, defaultMountRenewInterval(3))
	require.Equal(t, 10*time.Second, defaultMountRenewInterval(30))
}
