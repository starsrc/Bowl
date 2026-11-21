package cli

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/stretchr/testify/require"

	"github.com/starsrc/bowl/client/internal/fusefs"
	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
)

func TestRunFUSESignalUnmountLoop_GivenEBUSYOnSignal_LogsWarningAndRetriesOnTickerUntilCleanUnmount(t *testing.T) {
	var logBuf syncBuffer
	logger := slog.New(slog.NewJSONHandler(&logBuf, &slog.HandlerOptions{Level: slog.LevelDebug}))

	sigCh := make(chan os.Signal, 2)
	stopWait := make(chan struct{})
	defer close(stopWait)

	var calls atomic.Int32
	done := make(chan struct{})
	go func() {
		defer close(done)
		runFUSESignalUnmountLoop(
			context.Background(),
			sigCh,
			stopWait,
			15*time.Millisecond,
			"/mnt/busy-ticker",
			logger,
			func() error {
				if calls.Add(1) < 3 {
					return syscall.EBUSY
				}
				return nil
			},
		)
	}()

	sigCh <- syscall.SIGTERM

	require.Eventually(t, func() bool {
		return calls.Load() >= 3
	}, time.Second, 5*time.Millisecond, "scene=ebusy_ticker_retry want>=3_calls got=%d", calls.Load())

	time.Sleep(50 * time.Millisecond)
	sigCh <- syscall.SIGINT
	time.Sleep(30 * time.Millisecond)

	require.Equal(t, int32(3), calls.Load(), "scene=stop_retry_after_nil want=3 got=%d", calls.Load())
	logs := logBuf.String()
	require.Contains(t, logs, `"level":"WARN"`)
	require.Contains(t, logs, `"msg":"mount_unmount_busy"`)
	require.Contains(t, logs, `"/mnt/busy-ticker"`)
	require.Contains(t, logs, syscall.EBUSY.Error())
}

func TestRunFUSESignalUnmountLoop_GivenEBUSYOnSignal_RetriesOnSubsequentSignalBeforeTicker(t *testing.T) {
	var logBuf syncBuffer
	logger := slog.New(slog.NewJSONHandler(&logBuf, &slog.HandlerOptions{Level: slog.LevelDebug}))

	sigCh := make(chan os.Signal, 2)
	stopWait := make(chan struct{})
	defer close(stopWait)

	firstFailed := make(chan struct{})
	var calls atomic.Int32
	done := make(chan struct{})
	go func() {
		defer close(done)
		runFUSESignalUnmountLoop(
			context.Background(),
			sigCh,
			stopWait,
			5*time.Second,
			"/mnt/busy-resignal",
			logger,
			func() error {
				if calls.Add(1) == 1 {
					close(firstFailed)
					return syscall.EBUSY
				}
				return nil
			},
		)
	}()

	sigCh <- syscall.SIGINT
	select {
	case <-firstFailed:
	case <-time.After(time.Second):
		t.Fatal("scene=first_signal want=unmount_called got=timeout")
	}

	sigCh <- syscall.SIGTERM
	require.Eventually(t, func() bool {
		return calls.Load() == 2
	}, time.Second, 5*time.Millisecond, "scene=second_signal_retry want=2 got=%d", calls.Load())

	logs := logBuf.String()
	require.Contains(t, logs, `"msg":"mount_unmount_busy"`)
	require.Contains(t, logs, `"/mnt/busy-resignal"`)
}

func TestRunFUSESignalUnmountLoop_GivenCanceledContextAndEBUSY_NilsCtxDoneWithoutBusySpinAndStopsOnStopWait(t *testing.T) {
	var logBuf syncBuffer
	logger := slog.New(slog.NewJSONHandler(&logBuf, &slog.HandlerOptions{Level: slog.LevelDebug}))

	ctx, cancel := context.WithCancel(context.Background())
	sigCh := make(chan os.Signal, 2)
	stopWait := make(chan struct{})

	var calls atomic.Int32
	done := make(chan struct{})
	go func() {
		defer close(done)
		runFUSESignalUnmountLoop(
			ctx,
			sigCh,
			stopWait,
			40*time.Millisecond,
			"/mnt/busy-ctx",
			logger,
			func() error {
				calls.Add(1)
				return syscall.EBUSY
			},
		)
	}()

	cancel()

	require.Eventually(t, func() bool {
		return calls.Load() >= 2
	}, time.Second, 5*time.Millisecond, "scene=ctx_cancel_retry want>=2 got=%d", calls.Load())

	time.Sleep(60 * time.Millisecond)
	close(stopWait)

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("scene=stopWait_exit want=loop_exited got=timeout")
	}

	finalCalls := calls.Load()
	require.LessOrEqual(t, finalCalls, int32(10), "scene=no_busy_spin want<=10 got=%d", finalCalls)

	time.Sleep(60 * time.Millisecond)
	require.Equal(t, finalCalls, calls.Load(), "scene=no_calls_after_stopWait want=%d got=%d", finalCalls, calls.Load())
	require.Contains(t, logBuf.String(), `"msg":"mount_unmount_busy"`)
}

type hookLogWriter struct {
	mu      sync.Mutex
	buf     strings.Builder
	onWrite func(line string)
}

func (w *hookLogWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	chunk := string(p)
	w.buf.WriteString(chunk)
	if w.onWrite != nil {
		w.onWrite(chunk)
	}
	return len(p), nil
}

func reserveLoopbackTCPAddr(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := ln.Addr().String()
	require.NoError(t, ln.Close())
	return addr
}

func TestRunMount_TeardownOrder_GivenFixedAdminAddrAndStagedMode_ClosesAdminAndStageBeforeRemovingMeta(t *testing.T) {
	withCleanOTelAndLogger(t)
	withStageDirTmpfsStub(t, false)
	mp := t.TempDir()
	stageDir := t.TempDir()
	withMissingMountInfo(t)
	withTempMountMetaDirs(t, t.TempDir())

	fixedAddr := reserveLoopbackTCPAddr(t)

	storage := newFakeStorage()
	storage.seedDisk(testDiskID1, "teardown-order-disk", 1<<30, 0)
	storage.mountLeaseTTL = 30
	withMountStorageFake(t, storage)

	var (
		stageInstDir           string
		metaExistsAtUnmount    bool
		readyStatusAtUnmount   int
		sawUnmountComplete     bool
		metaExistsAtComplete   bool
		rebindErrAtComplete    error
		stageStatErrAtComplete error
	)

	storage.onUnmountDisk = func(_ *storagev1.UnmountDiskRequest) {
		_, metaExistsAtUnmount = findMountMeta(mp)
		readyStatusAtUnmount, _ = fetchReadyzStatusAndBody(t, fixedAddr)
	}

	withServeFUSEStub(t, func(_ context.Context, _ *fusefs.RawFS, cfg *fusefs.Config, _ string, ready func()) error {
		require.NotNil(t, cfg.StageSession)
		stageInstDir = cfg.StageSession.InstanceDir()
		ready()
		status, _ := fetchReadyzStatusAndBody(t, fixedAddr)
		require.Equal(t, http.StatusOK, status)
		_, statErr := os.Stat(stageInstDir)
		require.NoError(t, statErr)
		return nil
	})

	logWriter := &hookLogWriter{
		onWrite: func(chunk string) {
			if !strings.Contains(chunk, "unmount_complete") {
				return
			}
			sawUnmountComplete = true
			_, metaExistsAtComplete = findMountMeta(mp)
			rebindLn, err := net.Listen("tcp", fixedAddr)
			rebindErrAtComplete = err
			if rebindLn != nil {
				_ = rebindLn.Close()
			}
			_, stageStatErrAtComplete = os.Stat(stageInstDir)
		},
	}

	opts := &mountOptions{
		endpoint:  "http://127.0.0.1:9000",
		adminAddr: fixedAddr,
		logLevel:  "info",
		logFormat: "json",
	}
	baseCfg := &fusefs.Config{
		DiskID:    testDiskID1,
		Mode:      fusefs.MountModeStaged,
		StageSize: fusefs.DefaultStageSize,
		StageDir:  stageDir,
	}

	err := runMount(context.Background(), opts, baseCfg, mp, logWriter, nil)

	require.NoError(t, err)
	require.True(t, metaExistsAtUnmount, "scene=during_unmount_rpc want=meta_exists got=false")
	require.Equal(t, http.StatusOK, readyStatusAtUnmount, "scene=during_unmount_rpc want=admin_running_200 got=%d", readyStatusAtUnmount)
	require.True(t, sawUnmountComplete, "scene=unmount_complete_logged want=true got=false")
	require.False(t, metaExistsAtComplete, "scene=at_unmount_complete want=meta_removed got=true")
	require.NoError(t, rebindErrAtComplete, "scene=at_meta_removal want=fixed_admin_addr_rebindable got=%v", rebindErrAtComplete)
	require.ErrorIs(t, stageStatErrAtComplete, os.ErrNotExist, "scene=at_meta_removal want=stage_session_closed got=%v", stageStatErrAtComplete)
}

func TestAdminServerShutdown_GivenRepeatedCallsAndNilReceiver_IsIdempotentAndReleasesPort(t *testing.T) {
	withCleanOTelAndLogger(t)
	var nilAdmin *adminServer
	nilAdmin.shutdown(context.Background())

	fixedAddr := reserveLoopbackTCPAddr(t)
	obs, err := setupObservability(context.Background(), &mountOptions{
		adminAddr: fixedAddr,
		logLevel:  "info",
		logFormat: "json",
	}, &syncBuffer{})
	require.NoError(t, err)
	defer func() { _ = obs.Shutdown(context.Background()) }()

	rawFS := fusefs.NewRawFS(nil, fusefs.Config{DiskID: testDiskID1, Mode: fusefs.MountModeRO})
	admin, err := startAdminServer(fixedAddr, obs, rawFS, nil)
	require.NoError(t, err)
	require.NotNil(t, admin)

	status, _ := fetchReadyzStatusAndBody(t, fixedAddr)
	require.Equal(t, http.StatusOK, status)

	admin.shutdown(context.Background())
	admin.shutdown(context.Background())

	ln, listenErr := net.Listen("tcp", fixedAddr)
	require.NoError(t, listenErr, "scene=after_double_shutdown want=port_rebindable got=%v", listenErr)
	require.NoError(t, ln.Close())
}

func readPipeWithDeadline(t *testing.T, r, w *os.File, timeout time.Duration) string {
	t.Helper()
	type readResult struct {
		data string
		err  error
	}
	ch := make(chan readResult, 1)
	go func() {
		b, err := io.ReadAll(r)
		ch <- readResult{data: string(b), err: err}
	}()
	select {
	case res := <-ch:
		require.NoError(t, res.err)
		return res.data
	case <-time.After(timeout):
		_ = w.Close()
		<-ch
		t.Fatalf("scene=readiness_pipe_eof want=writer_closed_within_%s got=timeout", timeout)
		return ""
	}
}

func TestMount_DaemonWorker_GivenPreWorkerValidationErrors_WritesERRToReadinessPipeAndExits1Silent(t *testing.T) {
	validMP := t.TempDir()
	missingMP := filepath.Join(t.TempDir(), "missing-mp")
	mountedMP := canonicalMountpoint(t.TempDir())

	cases := []struct {
		name       string
		mountInfo  string
		args       []string
		wantSubstr string
	}{
		{
			name:       "InvalidLogLevel",
			args:       []string{"mount", testDiskID1, validMP, "--log-level", "bogus"},
			wantSubstr: "invalid --log-level",
		},
		{
			name:       "InvalidDiskID",
			args:       []string{"mount", "not-a-uuid", validMP},
			wantSubstr: "invalid disk-id",
		},
		{
			name:       "MissingMountpoint",
			args:       []string{"mount", testDiskID1, missingMP},
			wantSubstr: "does not exist or cannot be accessed",
		},
		{
			name:       "AlreadyMountedTOCTOU",
			mountInfo:  "100 30 0:50 / " + mountedMP + " rw - fuse.bowl bowl:" + testDiskID1 + " rw\n",
			args:       []string{"mount", testDiskID1, mountedMP},
			wantSubstr: "already mounted",
		},
		{
			name:       "InvalidEndpoint",
			args:       []string{"mount", testDiskID1, validMP, "--endpoint", "ftp://127.0.0.1:9000"},
			wantSubstr: "invalid endpoint",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if tc.mountInfo != "" {
				withMountInfoContent(t, tc.mountInfo)
			} else {
				withMissingMountInfo(t)
			}

			r, w, err := os.Pipe()
			require.NoError(t, err)
			defer func() { _ = r.Close() }()
			withReadinessPipeStub(t, w)
			t.Setenv("BOWL_DAEMON_WORKER", "1")

			code, out, errOut := runCLI(tc.args...)

			require.Equal(t, 1, code, "scene=%s want=1 got=%d stderr=%s", tc.name, code, errOut)
			require.Empty(t, out, "scene=%s want=empty_stdout", tc.name)
			require.Empty(t, errOut, "scene=%s want=silent_stderr got=%q", tc.name, errOut)

			payload := readPipeWithDeadline(t, r, w, 500*time.Millisecond)
			require.True(t, strings.HasPrefix(payload, "ERR:"), "scene=%s want=ERR_prefix got=%q", tc.name, payload)
			require.Contains(t, payload, tc.wantSubstr, "scene=%s payload=%q", tc.name, payload)
			require.True(t, strings.HasSuffix(payload, "\n"), "scene=%s want=trailing_newline got=%q", tc.name, payload)
		})
	}
}

func TestMount_DaemonWorker_GivenValidOptions_DoesNotTouchReadinessPipeBeforeRunDaemonWorker(t *testing.T) {
	withCleanOTelAndLogger(t)
	withMissingMountInfo(t)
	withTempMountMetaDirs(t, t.TempDir())
	withStageDirTmpfsStub(t, false)

	mp := t.TempDir()
	logPath := filepath.Join(t.TempDir(), "happy-worker.log")
	storage := newFakeStorage()
	storage.seedDisk(testDiskID1, "happy-worker-disk", 1<<30, 0)
	withMountStorageFake(t, storage)
	stubMountServeFUSE(t, mp)
	withRedirectDaemonStdioStub(t, func(*os.File) {})
	withChdirStub(t, func(string) error { return nil })

	r, w, err := os.Pipe()
	require.NoError(t, err)
	defer func() { _ = r.Close() }()

	var pipeOpenCalls atomic.Int32
	prev := readinessPipeFile
	readinessPipeFile = func() *os.File {
		pipeOpenCalls.Add(1)
		return w
	}
	t.Cleanup(func() { readinessPipeFile = prev })
	t.Setenv("BOWL_DAEMON_WORKER", "1")

	code, out, errOut := runCLI("mount", testDiskID1, mp, "--mode=ro", "--log-file", logPath, "--admin-addr", "off")

	require.Equal(t, 0, code, "scene=worker_happy want=0 got=%d stderr=%s", code, errOut)
	require.Empty(t, out)
	require.Empty(t, errOut)
	require.Equal(t, int32(1), pipeOpenCalls.Load(), "scene=worker_happy want=single_pipe_open_in_runDaemonWorker got=%d", pipeOpenCalls.Load())
	payload := readPipeWithDeadline(t, r, w, 500*time.Millisecond)
	require.Equal(t, "OK\n", payload)
}

func TestDaemonHelperProcess(_ *testing.T) {
	mode := os.Getenv("BOWL_TEST_DAEMON_HELPER")
	if mode == "" {
		return
	}
	pipe := os.NewFile(3, "readiness-pipe")
	readyFile := os.Getenv("BOWL_TEST_DAEMON_READY_FILE")
	markerFile := os.Getenv("BOWL_TEST_DAEMON_MARKER_FILE")

	writeFileFlag := func(path, content string) {
		if path != "" {
			_ = os.WriteFile(path, []byte(content), 0o600)
		}
	}

	switch mode {
	case "ok":
		_, _ = fmt.Fprintln(pipe, "OK")
		_ = pipe.Close()
		time.Sleep(15 * time.Second)
		os.Exit(0)
	case "err":
		_, _ = fmt.Fprintln(pipe, "ERR:disk busy")
		_ = pipe.Close()
		os.Exit(1)
	case "eof":
		_ = pipe.Close()
		os.Exit(1)
	case "hang_sleep":
		writeFileFlag(readyFile, "ready")
		time.Sleep(15 * time.Second)
		os.Exit(0)
	case "hang_term":
		sigCh := make(chan os.Signal, 2)
		signal.Notify(sigCh, syscall.SIGTERM)
		writeFileFlag(readyFile, "ready")
		select {
		case <-sigCh:
			writeFileFlag(markerFile, "got_sigterm")
			os.Exit(0)
		case <-time.After(15 * time.Second):
			os.Exit(2)
		}
	case "hang_ignore_term":
		sigCh := make(chan os.Signal, 2)
		signal.Notify(sigCh, syscall.SIGTERM)
		writeFileFlag(readyFile, "ready")
		select {
		case <-sigCh:
			writeFileFlag(markerFile, "got_sigterm")
		case <-time.After(15 * time.Second):
		}
		time.Sleep(15 * time.Second)
		os.Exit(0)
	case "signal_double_term":
		sigCtx, stop := defaultMountSignalContext(context.Background())
		writeFileFlag(readyFile, "ready")
		select {
		case <-sigCtx.Done():
			writeFileFlag(markerFile, "first_canceled")
		case <-time.After(10 * time.Second):
			stop()
			os.Exit(2)
		}
		releaseFile := os.Getenv("BOWL_TEST_DAEMON_RELEASE_FILE")
		deadline := time.Now().Add(10 * time.Second)
		for time.Now().Before(deadline) {
			if _, err := os.Stat(releaseFile); err == nil {
				writeFileFlag(markerFile, "survived_second_signal")
				stop()
				os.Exit(0)
			}
			time.Sleep(10 * time.Millisecond)
		}
		stop()
		os.Exit(4)
	default:
		os.Exit(3)
	}
}

func withDaemonHelperCommand(t *testing.T, mode, readyFile, markerFile string) *atomic.Pointer[exec.Cmd] {
	t.Helper()
	var captured atomic.Pointer[exec.Cmd]
	prev := daemonCommandFn
	daemonCommandFn = func(_ []string, w *os.File) (*exec.Cmd, error) {
		cmd := exec.Command(os.Args[0], "-test.run=^TestDaemonHelperProcess$")
		cmd.ExtraFiles = []*os.File{w}
		cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
		cmd.Env = append(
			os.Environ(),
			"BOWL_TEST_DAEMON_HELPER="+mode,
			"BOWL_TEST_DAEMON_READY_FILE="+readyFile,
			"BOWL_TEST_DAEMON_MARKER_FILE="+markerFile,
		)
		captured.Store(cmd)
		return cmd, nil
	}
	t.Cleanup(func() {
		daemonCommandFn = prev
		if cmd := captured.Load(); cmd != nil && cmd.Process != nil && cmd.ProcessState == nil {
			_ = cmd.Process.Signal(syscall.SIGKILL)
			_ = cmd.Wait()
		}
	})
	return &captured
}

func withDaemonTimeouts(t *testing.T, handshake, killGrace time.Duration) {
	t.Helper()
	prevHandshake := daemonHandshakeTimeout
	prevKillGrace := daemonKillGrace
	daemonHandshakeTimeout = handshake
	daemonKillGrace = killGrace
	t.Cleanup(func() {
		daemonHandshakeTimeout = prevHandshake
		daemonKillGrace = prevKillGrace
	})
}

func withDaemonSignalStub(t *testing.T) (chan os.Signal, *atomic.Bool) {
	t.Helper()
	sigCh := make(chan os.Signal, 2)
	var stopped atomic.Bool
	prev := daemonSignalNotify
	daemonSignalNotify = func() (<-chan os.Signal, func()) {
		return sigCh, func() { stopped.Store(true) }
	}
	t.Cleanup(func() { daemonSignalNotify = prev })
	return sigCh, &stopped
}

func runSpawnWithDeadline(t *testing.T, deadline time.Duration) (code int, stderr string) {
	t.Helper()
	type spawnRes struct {
		code   int
		stderr string
	}
	ch := make(chan spawnRes, 1)
	go func() {
		var buf strings.Builder
		c := spawnDaemonProcess([]string{testDiskID1, "/mnt/test"}, &buf)
		ch <- spawnRes{code: c, stderr: buf.String()}
	}()
	select {
	case res := <-ch:
		return res.code, res.stderr
	case <-time.After(deadline):
		t.Fatalf("scene=spawn_deadline want=completed_within_%s got=hung", deadline)
		return 0, ""
	}
}

func TestDaemonDefaults_FixedHandshakeTimeoutAndKillGrace(t *testing.T) {
	require.Equal(t, 30*time.Second, daemonHandshakeTimeout)
	require.Equal(t, 5*time.Second, daemonKillGrace)
}

func TestTerminateAndReapDaemonChild_GivenNonPositiveGrace_UsesDaemonKillGraceFallback(t *testing.T) {
	readyFile := filepath.Join(t.TempDir(), "ready")
	markerFile := filepath.Join(t.TempDir(), "marker")
	withDaemonTimeouts(t, 10*time.Second, 150*time.Millisecond)

	cmd := exec.Command(os.Args[0], "-test.run=^TestDaemonHelperProcess$")
	cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
	cmd.Env = append(
		os.Environ(),
		"BOWL_TEST_DAEMON_HELPER=hang_ignore_term",
		"BOWL_TEST_DAEMON_READY_FILE="+readyFile,
		"BOWL_TEST_DAEMON_MARKER_FILE="+markerFile,
	)
	require.NoError(t, cmd.Start())
	t.Cleanup(func() {
		if cmd.Process != nil && cmd.ProcessState == nil {
			_ = cmd.Process.Signal(syscall.SIGKILL)
			_ = cmd.Wait()
		}
	})

	require.Eventually(t, func() bool {
		_, err := os.Stat(readyFile)
		return err == nil
	}, 2*time.Second, 10*time.Millisecond)

	start := time.Now()
	terminateAndReapDaemonChild(cmd, 0)
	elapsed := time.Since(start)

	require.Less(t, elapsed, 1*time.Second, "scene=grace_fallback_uses_daemonKillGrace want<1s got=%s", elapsed)
	require.NotNil(t, cmd.ProcessState)
	ws, ok := cmd.ProcessState.Sys().(syscall.WaitStatus)
	require.True(t, ok)
	require.Equal(t, syscall.SIGKILL, ws.Signal())
}

type cancelOnCloseReadCloser struct {
	io.ReadCloser
	onClose func()
}

func (c *cancelOnCloseReadCloser) Close() error {
	err := c.ReadCloser.Close()
	if c.onClose != nil {
		c.onClose()
	}
	return err
}

func TestRunMount_GivenCancelDuringHandshake_AbortsRPCAndCleansStageAndAdminListener(t *testing.T) {
	withCleanOTelAndLogger(t)
	withStageDirTmpfsStub(t, false)
	withMissingMountInfo(t)
	withTempMountMetaDirs(t, t.TempDir())

	mp := t.TempDir()
	stageDir := t.TempDir()
	fixedAddr := reserveLoopbackTCPAddr(t)

	ctx, cancel := context.WithCancel(context.Background())
	storage := newFakeStorage()
	storage.seedDisk(testDiskID1, "cancel-during-handshake", 1<<30, 0)
	storage.mountLeaseTTL = 30
	storage.onMountDisk = func(rpcCtx context.Context, _ *storagev1.MountDiskRequest) error {
		cancel()
		<-rpcCtx.Done()
		return rpcCtx.Err()
	}
	withMountStorageFake(t, storage)

	var serveCalled atomic.Bool
	withServeFUSEStub(t, func(_ context.Context, _ *fusefs.RawFS, _ *fusefs.Config, _ string, _ func()) error {
		serveCalled.Store(true)
		return nil
	})

	opts := &mountOptions{
		endpoint:  "http://127.0.0.1:9000",
		adminAddr: fixedAddr,
		logLevel:  "info",
		logFormat: "json",
	}
	baseCfg := &fusefs.Config{
		DiskID:    testDiskID1,
		Mode:      fusefs.MountModeStaged,
		StageSize: fusefs.DefaultStageSize,
		StageDir:  stageDir,
	}

	err := runMount(ctx, opts, baseCfg, mp, &syncBuffer{}, nil)

	require.Error(t, err)
	require.False(t, serveCalled.Load(), "scene=cancel_during_handshake want=serveFUSE_not_called got=true")
	requireEmptyDir(t, stageDir, "cancel_during_handshake_stage_cleanup")
	ln, listenErr := net.Listen("tcp", fixedAddr)
	require.NoError(t, listenErr, "scene=cancel_during_handshake want=admin_addr_released got=%v", listenErr)
	require.NoError(t, ln.Close())
}

func TestRunMount_GivenCancelImmediatelyAfterHandshake_ReleasesLeaseAndSkipsServeFUSE(t *testing.T) {
	withCleanOTelAndLogger(t)
	withStageDirTmpfsStub(t, false)
	withMissingMountInfo(t)
	withTempMountMetaDirs(t, t.TempDir())

	mp := t.TempDir()
	stageDir := t.TempDir()
	fixedAddr := reserveLoopbackTCPAddr(t)

	ctx, cancel := context.WithCancel(context.Background())
	storage := newFakeStorage()
	storage.seedDisk(testDiskID1, "cancel-after-handshake", 1<<30, 0)
	storage.mountLeaseTTL = 30
	withMountStorageFake(t, storage)

	baseTransport := mountHTTPClient.Transport
	mountHTTPClient = &http.Client{
		Transport: roundTripperFunc(func(req *http.Request) (*http.Response, error) {
			resp, err := baseTransport.RoundTrip(req)
			if err != nil {
				return nil, err
			}
			if strings.HasSuffix(req.URL.Path, "/MountDisk") {
				resp.Body = &cancelOnCloseReadCloser{
					ReadCloser: resp.Body,
					onClose:    cancel,
				}
			}
			return resp, nil
		}),
	}

	var serveCalled atomic.Bool
	withServeFUSEStub(t, func(_ context.Context, _ *fusefs.RawFS, _ *fusefs.Config, _ string, _ func()) error {
		serveCalled.Store(true)
		return nil
	})

	opts := &mountOptions{
		endpoint:  "http://127.0.0.1:9000",
		adminAddr: fixedAddr,
		logLevel:  "info",
		logFormat: "json",
	}
	baseCfg := &fusefs.Config{
		DiskID:    testDiskID1,
		Mode:      fusefs.MountModeStaged,
		StageSize: fusefs.DefaultStageSize,
		StageDir:  stageDir,
	}

	err := runMount(ctx, opts, baseCfg, mp, &syncBuffer{}, nil)

	require.ErrorIs(t, err, context.Canceled)
	require.False(t, serveCalled.Load(), "scene=cancel_after_handshake want=serveFUSE_not_called got=true")
	require.Len(t, storage.getMountReqs(), 1)
	require.Len(t, storage.getUnmountReqs(), 1, "scene=cancel_after_handshake want=lease_released_via_UnmountDisk")
	requireEmptyDir(t, stageDir, "cancel_after_handshake_stage_cleanup")
	ln, listenErr := net.Listen("tcp", fixedAddr)
	require.NoError(t, listenErr, "scene=cancel_after_handshake want=admin_addr_released got=%v", listenErr)
	require.NoError(t, ln.Close())
}

type fakeFUSEMountServer struct {
	unmountCh       chan struct{}
	unmountOnce     sync.Once
	unmountCnt      atomic.Int32
	serveExited     atomic.Bool
	waitMountFn     func(unmountCh <-chan struct{}) error
	busyUnmountLeft atomic.Int32
}

func newFakeFUSEMountServer(waitFn func(unmountCh <-chan struct{}) error) *fakeFUSEMountServer {
	return &fakeFUSEMountServer{
		unmountCh:   make(chan struct{}),
		waitMountFn: waitFn,
	}
}

func (s *fakeFUSEMountServer) Serve() {
	<-s.unmountCh
	s.serveExited.Store(true)
}

func (s *fakeFUSEMountServer) WaitMount() error {
	if s.waitMountFn != nil {
		return s.waitMountFn(s.unmountCh)
	}
	return nil
}

func (s *fakeFUSEMountServer) Unmount() error {
	s.unmountCnt.Add(1)
	if s.busyUnmountLeft.Add(-1) >= 0 {
		return syscall.EBUSY
	}
	s.unmountOnce.Do(func() { close(s.unmountCh) })
	return nil
}

func (s *fakeFUSEMountServer) forceRelease() {
	s.busyUnmountLeft.Store(0)
	s.unmountOnce.Do(func() { close(s.unmountCh) })
}

func withNewFUSEServerStub(t *testing.T, fn func(*fusefs.RawFS, string, *fuse.MountOptions) (fuseMountServer, error)) {
	t.Helper()
	prev := newFUSEServer
	newFUSEServer = fn
	t.Cleanup(func() { newFUSEServer = prev })
}

func withMountSignalContextStub(t *testing.T, fn func(context.Context) (context.Context, context.CancelFunc)) {
	t.Helper()
	prev := mountSignalContext
	mountSignalContext = fn
	t.Cleanup(func() { mountSignalContext = prev })
}

func TestDefaultServeFUSE_GivenCancelAcrossStartupWindows_UnmountsWithoutCallingReady(t *testing.T) {
	t.Run("CanceledBeforeNewServer", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		var created atomic.Bool
		withNewFUSEServerStub(t, func(*fusefs.RawFS, string, *fuse.MountOptions) (fuseMountServer, error) {
			created.Store(true)
			return newFakeFUSEMountServer(nil), nil
		})

		var readyCalled atomic.Bool
		err := defaultServeFUSE(ctx, nil, &fusefs.Config{DiskID: testDiskID1}, "/mnt/pre-cancel", func() {
			readyCalled.Store(true)
		})

		require.ErrorIs(t, err, context.Canceled)
		require.False(t, created.Load(), "scene=pre_new_server want=newFUSEServer_not_called")
		require.False(t, readyCalled.Load(), "scene=pre_new_server want=ready_not_called")
	})

	t.Run("CanceledDuringWaitMount", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		waitEntered := make(chan struct{})
		fakeSrv := newFakeFUSEMountServer(func(unmountCh <-chan struct{}) error {
			close(waitEntered)
			<-unmountCh
			return errors.New("unmounted during wait")
		})
		withNewFUSEServerStub(t, func(*fusefs.RawFS, string, *fuse.MountOptions) (fuseMountServer, error) {
			return fakeSrv, nil
		})

		var readyCalled atomic.Bool
		errCh := make(chan error, 1)
		go func() {
			errCh <- defaultServeFUSE(ctx, nil, &fusefs.Config{DiskID: testDiskID1}, "/mnt/wait-cancel", func() {
				readyCalled.Store(true)
			})
		}()

		select {
		case <-waitEntered:
		case <-time.After(time.Second):
			t.Fatal("scene=wait_mount_enter want=entered got=timeout")
		}
		cancel()

		select {
		case err := <-errCh:
			require.ErrorIs(t, err, context.Canceled)
		case <-time.After(time.Second):
			_ = fakeSrv.Unmount()
			t.Fatal("scene=wait_mount_cancel want=unblocked_within_1s got=hung")
		}
		require.False(t, readyCalled.Load(), "scene=wait_mount_cancel want=ready_not_called")
		require.GreaterOrEqual(t, fakeSrv.unmountCnt.Load(), int32(1))
		require.True(t, fakeSrv.serveExited.Load())
	})

	t.Run("CanceledAfterWaitMountBeforeReady", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		fakeSrv := newFakeFUSEMountServer(func(<-chan struct{}) error {
			cancel()
			return nil
		})
		withNewFUSEServerStub(t, func(*fusefs.RawFS, string, *fuse.MountOptions) (fuseMountServer, error) {
			return fakeSrv, nil
		})

		var readyCalled atomic.Bool
		errCh := make(chan error, 1)
		go func() {
			errCh <- defaultServeFUSE(ctx, nil, &fusefs.Config{DiskID: testDiskID1}, "/mnt/post-wait-cancel", func() {
				readyCalled.Store(true)
			})
		}()

		select {
		case err := <-errCh:
			require.ErrorIs(t, err, context.Canceled)
		case <-time.After(time.Second):
			_ = fakeSrv.Unmount()
			t.Fatal("scene=post_wait_cancel want=returned_within_1s got=hung")
		}
		require.False(t, readyCalled.Load(), "scene=post_wait_cancel want=ready_not_called")
		require.GreaterOrEqual(t, fakeSrv.unmountCnt.Load(), int32(1))
		require.True(t, fakeSrv.serveExited.Load())
	})
}

func TestDefaultServeFUSE_GivenCancelWithEBUSY_RetriesUntilUnmountSucceeds(t *testing.T) {
	t.Run("RetryIntervalsPreserveStep49AndStep57", func(t *testing.T) {
		require.Equal(t, 500*time.Millisecond, fuseSignalUnmountRetryInterval, "scene=post_ready_retry_interval want=500ms got=%s", fuseSignalUnmountRetryInterval)
		require.Equal(t, 200*time.Millisecond, fuseStartupUnmountRetryInterval, "scene=startup_retry_interval want=200ms got=%s", fuseStartupUnmountRetryInterval)
	})

	t.Run("CanceledDuringWaitMount", func(t *testing.T) {
		var logBuf syncBuffer
		logger := slog.New(slog.NewJSONHandler(&logBuf, &slog.HandlerOptions{Level: slog.LevelDebug}))

		ctx, cancel := context.WithCancel(context.Background())
		waitEntered := make(chan struct{})
		fakeSrv := newFakeFUSEMountServer(func(unmountCh <-chan struct{}) error {
			close(waitEntered)
			<-unmountCh
			return errors.New("unmounted during wait")
		})
		fakeSrv.busyUnmountLeft.Store(2)
		withNewFUSEServerStub(t, func(*fusefs.RawFS, string, *fuse.MountOptions) (fuseMountServer, error) {
			return fakeSrv, nil
		})

		var readyCalled atomic.Bool
		errCh := make(chan error, 1)
		go func() {
			errCh <- defaultServeFUSE(ctx, nil, &fusefs.Config{DiskID: testDiskID1, Logger: logger}, "/mnt/ebusy-wait-cancel", func() {
				readyCalled.Store(true)
			})
		}()

		select {
		case <-waitEntered:
		case <-time.After(time.Second):
			fakeSrv.forceRelease()
			t.Fatal("scene=ebusy_wait_mount_enter want=entered got=timeout")
		}
		cancel()

		select {
		case err := <-errCh:
			require.ErrorIs(t, err, context.Canceled)
		case <-time.After(1500 * time.Millisecond):
			fakeSrv.forceRelease()
			gotErr := <-errCh
			t.Fatalf("scene=ebusy_wait_cancel want=retried_until_unmount got=hung unmountCnt=%d err=%v", fakeSrv.unmountCnt.Load(), gotErr)
		}

		require.False(t, readyCalled.Load(), "scene=ebusy_wait_cancel want=ready_not_called")
		require.Equal(t, int32(3), fakeSrv.unmountCnt.Load(), "scene=ebusy_wait_cancel want=3_unmount_attempts got=%d", fakeSrv.unmountCnt.Load())
		require.True(t, fakeSrv.serveExited.Load())
		logs := logBuf.String()
		require.Contains(t, logs, `"msg":"mount_unmount_busy"`)
		require.Contains(t, logs, `"/mnt/ebusy-wait-cancel"`)
		require.Contains(t, logs, syscall.EBUSY.Error())
	})

	t.Run("WaitMountError", func(t *testing.T) {
		var logBuf syncBuffer
		logger := slog.New(slog.NewJSONHandler(&logBuf, &slog.HandlerOptions{Level: slog.LevelDebug}))

		fakeSrv := newFakeFUSEMountServer(func(<-chan struct{}) error {
			return errors.New("mount wait failed")
		})
		fakeSrv.busyUnmountLeft.Store(2)
		withNewFUSEServerStub(t, func(*fusefs.RawFS, string, *fuse.MountOptions) (fuseMountServer, error) {
			return fakeSrv, nil
		})

		var readyCalled atomic.Bool
		errCh := make(chan error, 1)
		go func() {
			errCh <- defaultServeFUSE(context.Background(), nil, &fusefs.Config{DiskID: testDiskID1, Logger: logger}, "/mnt/ebusy-wait-err", func() {
				readyCalled.Store(true)
			})
		}()

		select {
		case err := <-errCh:
			require.ErrorContains(t, err, "wait for mount readiness")
			require.ErrorContains(t, err, "mount wait failed")
		case <-time.After(1500 * time.Millisecond):
			fakeSrv.forceRelease()
			gotErr := <-errCh
			t.Fatalf("scene=ebusy_wait_err want=retried_until_unmount got=hung unmountCnt=%d err=%v", fakeSrv.unmountCnt.Load(), gotErr)
		}

		require.False(t, readyCalled.Load(), "scene=ebusy_wait_err want=ready_not_called")
		require.Equal(t, int32(3), fakeSrv.unmountCnt.Load(), "scene=ebusy_wait_err want=3_unmount_attempts got=%d", fakeSrv.unmountCnt.Load())
		require.True(t, fakeSrv.serveExited.Load())
		logs := logBuf.String()
		require.Contains(t, logs, `"msg":"mount_unmount_busy"`)
		require.Contains(t, logs, `"/mnt/ebusy-wait-err"`)
		require.Contains(t, logs, syscall.EBUSY.Error())
	})

	t.Run("CanceledAfterWaitMountBeforeReady", func(t *testing.T) {
		var logBuf syncBuffer
		logger := slog.New(slog.NewJSONHandler(&logBuf, &slog.HandlerOptions{Level: slog.LevelDebug}))

		ctx, cancel := context.WithCancel(context.Background())
		fakeSrv := newFakeFUSEMountServer(func(<-chan struct{}) error {
			cancel()
			return nil
		})
		fakeSrv.busyUnmountLeft.Store(2)
		withNewFUSEServerStub(t, func(*fusefs.RawFS, string, *fuse.MountOptions) (fuseMountServer, error) {
			return fakeSrv, nil
		})

		var readyCalled atomic.Bool
		errCh := make(chan error, 1)
		go func() {
			errCh <- defaultServeFUSE(ctx, nil, &fusefs.Config{DiskID: testDiskID1, Logger: logger}, "/mnt/ebusy-post-wait", func() {
				readyCalled.Store(true)
			})
		}()

		select {
		case err := <-errCh:
			require.ErrorIs(t, err, context.Canceled)
		case <-time.After(1500 * time.Millisecond):
			fakeSrv.forceRelease()
			gotErr := <-errCh
			t.Fatalf("scene=ebusy_post_wait want=retried_until_unmount got=hung unmountCnt=%d err=%v", fakeSrv.unmountCnt.Load(), gotErr)
		}

		require.False(t, readyCalled.Load(), "scene=ebusy_post_wait want=ready_not_called")
		require.Equal(t, int32(3), fakeSrv.unmountCnt.Load(), "scene=ebusy_post_wait want=3_unmount_attempts got=%d", fakeSrv.unmountCnt.Load())
		require.True(t, fakeSrv.serveExited.Load())
		logs := logBuf.String()
		require.Contains(t, logs, `"msg":"mount_unmount_busy"`)
		require.Contains(t, logs, `"/mnt/ebusy-post-wait"`)
		require.Contains(t, logs, syscall.EBUSY.Error())
	})

	t.Run("RunMountCleansLeaseAdminAndStageAfterEBUSYRetry", func(t *testing.T) {
		withCleanOTelAndLogger(t)
		withStageDirTmpfsStub(t, false)
		withMissingMountInfo(t)
		withTempMountMetaDirs(t, t.TempDir())

		mp := t.TempDir()
		stageDir := t.TempDir()
		fixedAddr := reserveLoopbackTCPAddr(t)

		storage := newFakeStorage()
		storage.seedDisk(testDiskID1, "ebusy-startup-disk", 1<<30, 0)
		storage.mountLeaseTTL = 30
		withMountStorageFake(t, storage)

		ctx, cancel := context.WithCancel(context.Background())
		waitEntered := make(chan struct{})
		fakeSrv := newFakeFUSEMountServer(func(unmountCh <-chan struct{}) error {
			close(waitEntered)
			<-unmountCh
			return errors.New("unmounted after retry")
		})
		fakeSrv.busyUnmountLeft.Store(2)
		withNewFUSEServerStub(t, func(*fusefs.RawFS, string, *fuse.MountOptions) (fuseMountServer, error) {
			return fakeSrv, nil
		})

		var logBuf syncBuffer
		var onReadyCalled atomic.Bool
		opts := &mountOptions{
			endpoint:  "http://127.0.0.1:9000",
			adminAddr: fixedAddr,
			logLevel:  "info",
			logFormat: "json",
		}
		baseCfg := &fusefs.Config{
			DiskID:    testDiskID1,
			Mode:      fusefs.MountModeStaged,
			StageSize: fusefs.DefaultStageSize,
			StageDir:  stageDir,
		}

		errCh := make(chan error, 1)
		go func() {
			errCh <- runMount(ctx, opts, baseCfg, mp, &logBuf, func() {
				onReadyCalled.Store(true)
			})
		}()

		select {
		case <-waitEntered:
		case <-time.After(time.Second):
			fakeSrv.forceRelease()
			t.Fatal("scene=run_mount_ebusy_wait_enter want=entered got=timeout")
		}
		cancel()

		select {
		case err := <-errCh:
			require.ErrorIs(t, err, context.Canceled)
		case <-time.After(1500 * time.Millisecond):
			fakeSrv.forceRelease()
			gotErr := <-errCh
			t.Fatalf("scene=run_mount_ebusy want=clean_teardown got=hung unmountCnt=%d err=%v", fakeSrv.unmountCnt.Load(), gotErr)
		}

		require.False(t, onReadyCalled.Load(), "scene=run_mount_ebusy want=onReady_not_called")
		require.Equal(t, int32(3), fakeSrv.unmountCnt.Load())
		require.Len(t, storage.getMountReqs(), 1)
		require.Len(t, storage.getUnmountReqs(), 1, "scene=run_mount_ebusy want=lease_released_via_UnmountDisk")
		requireEmptyDir(t, stageDir, "run_mount_ebusy_stage_cleanup")
		ln, listenErr := net.Listen("tcp", fixedAddr)
		require.NoError(t, listenErr, "scene=run_mount_ebusy want=admin_addr_released got=%v", listenErr)
		require.NoError(t, ln.Close())
		logs := logBuf.String()
		require.Contains(t, logs, `"msg":"mount_unmount_busy"`)
		require.Contains(t, logs, mp)
	})
}

func TestMount_EarlySignalContext_GivenForegroundAndDaemonWorker_InstallsBeforeHandshakeAndCleansUp(t *testing.T) {
	t.Run("Foreground", func(t *testing.T) {
		withCleanOTelAndLogger(t)
		withStageDirTmpfsStub(t, false)
		withMissingMountInfo(t)
		withTempMountMetaDirs(t, t.TempDir())

		mp := t.TempDir()
		stageDir := t.TempDir()

		var sigInstallCalls atomic.Int32
		var sigStopCalls atomic.Int32
		sigCtx, sigCancel := context.WithCancel(context.Background())
		withMountSignalContextStub(t, func(_ context.Context) (context.Context, context.CancelFunc) {
			sigInstallCalls.Add(1)
			ctx, cancel := context.WithCancel(sigCtx)
			return ctx, func() {
				sigStopCalls.Add(1)
				cancel()
			}
		})

		storage := newFakeStorage()
		storage.seedDisk(testDiskID1, "fg-early-sig", 1<<30, 0)
		withMountStorageFake(t, storage)
		baseTransport := mountHTTPClient.Transport
		mountHTTPClient = &http.Client{
			Transport: roundTripperFunc(func(req *http.Request) (*http.Response, error) {
				resp, err := baseTransport.RoundTrip(req)
				if err != nil {
					return nil, err
				}
				if strings.HasSuffix(req.URL.Path, "/MountDisk") {
					resp.Body = &cancelOnCloseReadCloser{ReadCloser: resp.Body, onClose: sigCancel}
				}
				return resp, nil
			}),
		}

		var serveCalled atomic.Bool
		withServeFUSEStub(t, func(context.Context, *fusefs.RawFS, *fusefs.Config, string, func()) error {
			serveCalled.Store(true)
			return nil
		})

		code, _, errOut := runCLI("mount", testDiskID1, mp, "-f", "--stage-dir", stageDir, "--admin-addr", "off")

		require.Equal(t, 1, code, "scene=fg_early_sig stderr=%s", errOut)
		require.Equal(t, int32(1), sigInstallCalls.Load(), "scene=fg_early_sig want=mountSignalContext_installed")
		require.Equal(t, int32(1), sigStopCalls.Load(), "scene=fg_early_sig want=mountSignalContext_stopped")
		require.False(t, serveCalled.Load())
		require.Len(t, storage.getUnmountReqs(), 1)
		requireEmptyDir(t, stageDir, "fg_early_sig_stage_cleanup")
	})

	t.Run("DaemonWorker", func(t *testing.T) {
		withCleanOTelAndLogger(t)
		withStageDirTmpfsStub(t, false)
		withMissingMountInfo(t)
		withTempMountMetaDirs(t, t.TempDir())
		withRedirectDaemonStdioStub(t, func(*os.File) {})
		withChdirStub(t, func(string) error { return nil })

		mp := t.TempDir()
		stageDir := t.TempDir()
		logPath := filepath.Join(t.TempDir(), "worker-early-sig.log")

		var sigInstallCalls atomic.Int32
		var sigStopCalls atomic.Int32
		sigCtx, sigCancel := context.WithCancel(context.Background())
		withMountSignalContextStub(t, func(_ context.Context) (context.Context, context.CancelFunc) {
			sigInstallCalls.Add(1)
			ctx, cancel := context.WithCancel(sigCtx)
			return ctx, func() {
				sigStopCalls.Add(1)
				cancel()
			}
		})

		storage := newFakeStorage()
		storage.seedDisk(testDiskID1, "worker-early-sig", 1<<30, 0)
		withMountStorageFake(t, storage)
		baseTransport := mountHTTPClient.Transport
		mountHTTPClient = &http.Client{
			Transport: roundTripperFunc(func(req *http.Request) (*http.Response, error) {
				resp, err := baseTransport.RoundTrip(req)
				if err != nil {
					return nil, err
				}
				if strings.HasSuffix(req.URL.Path, "/MountDisk") {
					resp.Body = &cancelOnCloseReadCloser{ReadCloser: resp.Body, onClose: sigCancel}
				}
				return resp, nil
			}),
		}

		var serveCalled atomic.Bool
		withServeFUSEStub(t, func(context.Context, *fusefs.RawFS, *fusefs.Config, string, func()) error {
			serveCalled.Store(true)
			return nil
		})

		r, w, err := os.Pipe()
		require.NoError(t, err)
		defer func() { _ = r.Close() }()
		withReadinessPipeStub(t, w)
		t.Setenv("BOWL_DAEMON_WORKER", "1")

		code, _, _ := runCLI("mount", testDiskID1, mp, "--stage-dir", stageDir, "--log-file", logPath, "--admin-addr", "off")

		require.Equal(t, 1, code)
		require.Equal(t, int32(1), sigInstallCalls.Load(), "scene=worker_early_sig want=mountSignalContext_installed")
		require.Equal(t, int32(1), sigStopCalls.Load(), "scene=worker_early_sig want=mountSignalContext_stopped")
		require.False(t, serveCalled.Load())
		require.Len(t, storage.getUnmountReqs(), 1)
		requireEmptyDir(t, stageDir, "worker_early_sig_stage_cleanup")
		payload := readPipeWithDeadline(t, r, w, 500*time.Millisecond)
		require.True(t, strings.HasPrefix(payload, "ERR:"), "scene=worker_early_sig payload=%q", payload)
	})
}

func TestMountSignalContext_GivenSecondSignalDuringTeardown_CapturesWithoutDefaultKill(t *testing.T) {
	dir := t.TempDir()
	readyFile := filepath.Join(dir, "ready")
	markerFile := filepath.Join(dir, "marker")
	releaseFile := filepath.Join(dir, "release")

	cmd := exec.Command(os.Args[0], "-test.run=^TestDaemonHelperProcess$")
	cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
	cmd.Env = append(
		os.Environ(),
		"BOWL_TEST_DAEMON_HELPER=signal_double_term",
		"BOWL_TEST_DAEMON_READY_FILE="+readyFile,
		"BOWL_TEST_DAEMON_MARKER_FILE="+markerFile,
		"BOWL_TEST_DAEMON_RELEASE_FILE="+releaseFile,
	)
	require.NoError(t, cmd.Start())
	t.Cleanup(func() {
		if cmd.Process != nil && cmd.ProcessState == nil {
			_ = cmd.Process.Signal(syscall.SIGKILL)
			_ = cmd.Wait()
		}
	})

	require.Eventually(t, func() bool {
		_, err := os.Stat(readyFile)
		return err == nil
	}, 2*time.Second, 10*time.Millisecond)

	require.NoError(t, cmd.Process.Signal(syscall.SIGTERM))
	require.Eventually(t, func() bool {
		b, err := os.ReadFile(markerFile)
		return err == nil && string(b) == "first_canceled"
	}, 2*time.Second, 10*time.Millisecond)

	require.NoError(t, cmd.Process.Signal(syscall.SIGTERM))
	time.Sleep(100 * time.Millisecond)
	require.NoError(t, os.WriteFile(releaseFile, []byte("go"), 0o600))

	err := cmd.Wait()
	require.NoError(t, err, "scene=second_signal_during_teardown want=clean_exit_0 got=%v", err)
	markerBytes, readErr := os.ReadFile(markerFile)
	require.NoError(t, readErr)
	require.Equal(t, "survived_second_signal", string(markerBytes))
}

func envEntriesForKey(env []string, key string) []string {
	prefix := key + "="
	var out []string
	for _, entry := range env {
		if strings.HasPrefix(entry, prefix) {
			out = append(out, entry)
		}
	}
	return out
}

func TestDefaultDaemonCommand_GivenTokenFlagsAndEnv_ScrubsArgvAndSetsBowlTokenEnv(t *testing.T) {
	cases := []struct {
		name             string
		inheritedToken   string
		setInherited     bool
		args             []string
		wantSubArgs      []string
		wantBowlTokenEnv []string
	}{
		{
			name:             "SplitTokenFlag",
			args:             []string{"--token", "secret-split", testDiskID1, "/mnt/data"},
			wantSubArgs:      []string{"mount", testDiskID1, "/mnt/data"},
			wantBowlTokenEnv: []string{"BOWL_TOKEN=secret-split"},
		},
		{
			name:             "EqualsTokenFlagOverridesInheritedEnv",
			inheritedToken:   "old-env-tok",
			setInherited:     true,
			args:             []string{"--token=  secret-eq  ", testDiskID1, "/mnt/data"},
			wantSubArgs:      []string{"mount", testDiskID1, "/mnt/data"},
			wantBowlTokenEnv: []string{"BOWL_TOKEN=secret-eq"},
		},
		{
			name:             "RepeatedTokenFlagsLastWins",
			inheritedToken:   "old-env-tok",
			setInherited:     true,
			args:             []string{"--token", "first", "--mode=ro", "--token=second", testDiskID1, "/mnt/data"},
			wantSubArgs:      []string{"mount", "--mode=ro", testDiskID1, "/mnt/data"},
			wantBowlTokenEnv: []string{"BOWL_TOKEN=second"},
		},
		{
			name:             "WhitespaceTokenLeavesInheritedEnvUntouched",
			inheritedToken:   "keep-env",
			setInherited:     true,
			args:             []string{"--token", "   ", testDiskID1, "/mnt/data"},
			wantSubArgs:      []string{"mount", testDiskID1, "/mnt/data"},
			wantBowlTokenEnv: []string{"BOWL_TOKEN=keep-env"},
		},
		{
			name:             "EmptyEqualsTokenLeavesInheritedEnvUntouched",
			inheritedToken:   "keep-env",
			setInherited:     true,
			args:             []string{"--token=", testDiskID1, "/mnt/data"},
			wantSubArgs:      []string{"mount", testDiskID1, "/mnt/data"},
			wantBowlTokenEnv: []string{"BOWL_TOKEN=keep-env"},
		},
		{
			name:             "RepeatedTokenEndingBlankLeavesInheritedEnvUntouched",
			inheritedToken:   "keep-env",
			setInherited:     true,
			args:             []string{"--token", "first", "--token", "   ", testDiskID1, "/mnt/data"},
			wantSubArgs:      []string{"mount", testDiskID1, "/mnt/data"},
			wantBowlTokenEnv: []string{"BOWL_TOKEN=keep-env"},
		},
		{
			name:             "NoTokenFlagPreservesInheritedEnv",
			inheritedToken:   "keep-env",
			setInherited:     true,
			args:             []string{"--mode=ro", testDiskID1, "/mnt/data"},
			wantSubArgs:      []string{"mount", "--mode=ro", testDiskID1, "/mnt/data"},
			wantBowlTokenEnv: []string{"BOWL_TOKEN=keep-env"},
		},
		{
			name:             "StopsScanningAtDoubleDash",
			args:             []string{"--token", "real-tok", "--", testDiskID1, "--token", "--token=literal"},
			wantSubArgs:      []string{"mount", "--", testDiskID1, "--token", "--token=literal"},
			wantBowlTokenEnv: []string{"BOWL_TOKEN=real-tok"},
		},
		{
			name:             "TokenAfterDoubleDashOnlyNotTouched",
			inheritedToken:   "keep-env",
			setInherited:     true,
			args:             []string{"--", testDiskID1, "--token=literal"},
			wantSubArgs:      []string{"mount", "--", testDiskID1, "--token=literal"},
			wantBowlTokenEnv: []string{"BOWL_TOKEN=keep-env"},
		},
		{
			name:             "OtherFlagValueMatchingTokenFlagNotScrubbed",
			args:             []string{"--log-file", "--token", "--token", "real-tok", testDiskID1, "/mnt/data"},
			wantSubArgs:      []string{"mount", "--log-file", "--token", testDiskID1, "/mnt/data"},
			wantBowlTokenEnv: []string{"BOWL_TOKEN=real-tok"},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if tc.setInherited {
				t.Setenv("BOWL_TOKEN", tc.inheritedToken)
			} else {
				prev, existed := os.LookupEnv("BOWL_TOKEN")
				require.NoError(t, os.Unsetenv("BOWL_TOKEN"))
				t.Cleanup(func() {
					if existed {
						_ = os.Setenv("BOWL_TOKEN", prev)
					} else {
						_ = os.Unsetenv("BOWL_TOKEN")
					}
				})
			}

			r, w, err := os.Pipe()
			require.NoError(t, err)
			defer func() {
				_ = r.Close()
				_ = w.Close()
			}()

			cmd, cmdErr := defaultDaemonCommand(tc.args, w)
			require.NoError(t, cmdErr)
			require.Equal(t, tc.wantSubArgs, cmd.Args[1:], "scene=%s", tc.name)
			require.Equal(t, tc.wantBowlTokenEnv, envEntriesForKey(cmd.Env, "BOWL_TOKEN"), "scene=%s", tc.name)
			require.Equal(t, []string{"BOWL_DAEMON_WORKER=1"}, envEntriesForKey(cmd.Env, "BOWL_DAEMON_WORKER"), "scene=%s", tc.name)
		})
	}
}

func TestMount_DaemonWorker_GivenScrubbedTokenInEnv_AuthenticatesWithRPCWithoutTokenFlag(t *testing.T) {
	withCleanOTelAndLogger(t)
	withMissingMountInfo(t)
	withTempMountMetaDirs(t, t.TempDir())
	withStageDirTmpfsStub(t, false)
	withRedirectDaemonStdioStub(t, func(*os.File) {})
	withChdirStub(t, func(string) error { return nil })

	mp := t.TempDir()
	logPath := filepath.Join(t.TempDir(), "token-worker.log")
	t.Setenv("BOWL_TOKEN", "old-env-secret")

	pr, pw, err := os.Pipe()
	require.NoError(t, err)
	defer func() { _ = pr.Close() }()

	cmd, cmdErr := defaultDaemonCommand([]string{testDiskID1, mp, "--mode=ro", "--log-file", logPath, "--admin-addr", "off", "--token", "daemon-secret"}, pw)
	require.NoError(t, cmdErr)
	require.NotContains(t, cmd.Args, "--token")
	require.NotContains(t, cmd.Args, "daemon-secret")
	require.Equal(t, []string{"BOWL_TOKEN=daemon-secret"}, envEntriesForKey(cmd.Env, "BOWL_TOKEN"))

	storage := newFakeStorage()
	storage.seedDisk(testDiskID1, "token-worker-disk", 1<<30, 0)
	withMountStorageFake(t, storage)
	stubMountServeFUSE(t, mp)
	withReadinessPipeStub(t, pw)
	t.Setenv("BOWL_TOKEN", "daemon-secret")
	t.Setenv("BOWL_DAEMON_WORKER", "1")

	code, _, errOut := runCLI(cmd.Args[1:]...)

	require.Equal(t, 0, code, "scene=worker_token_env stderr=%s", errOut)
	require.Equal(t, "Bearer daemon-secret", storage.lastAuth)
	require.Equal(t, "OK\n", readPipeWithDeadline(t, pr, pw, 500*time.Millisecond))
}

func TestSpawnDaemonProcess_GivenOK_ReturnsZeroAndLeavesDetachedChildRunning(t *testing.T) {
	withDaemonTimeouts(t, 5*time.Second, time.Second)
	_, stopped := withDaemonSignalStub(t)
	captured := withDaemonHelperCommand(t, "ok", "", "")

	code, errOut := runSpawnWithDeadline(t, 3*time.Second)

	require.Equal(t, 0, code, "scene=spawn_ok want=0 got=%d stderr=%s", code, errOut)
	require.Empty(t, errOut)
	require.True(t, stopped.Load(), "scene=spawn_ok want=signal_stop_called got=false")

	cmd := captured.Load()
	require.NotNil(t, cmd)
	require.NotNil(t, cmd.Process)
	require.Nil(t, cmd.ProcessState, "scene=spawn_ok want=child_not_waited_detached")
	require.NoError(t, syscall.Kill(cmd.Process.Pid, 0), "scene=spawn_ok want=detached_child_alive")
}

func TestSpawnDaemonProcess_GivenErrAndEOF_ReapsChildAndReturnsOne(t *testing.T) {
	cases := []struct {
		name       string
		mode       string
		wantSubstr string
	}{
		{
			name:       "ExplicitERR",
			mode:       "err",
			wantSubstr: "background daemon failed to initialize: disk busy",
		},
		{
			name:       "UnexpectedEOF",
			mode:       "eof",
			wantSubstr: "before completing mount handshake",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			withDaemonTimeouts(t, 5*time.Second, time.Second)
			_, stopped := withDaemonSignalStub(t)
			captured := withDaemonHelperCommand(t, tc.mode, "", "")

			code, errOut := runSpawnWithDeadline(t, 3*time.Second)

			require.Equal(t, 1, code, "scene=%s want=1 got=%d stderr=%s", tc.name, code, errOut)
			require.Contains(t, errOut, tc.wantSubstr, "scene=%s", tc.name)
			require.True(t, stopped.Load(), "scene=%s want=signal_stop_called got=false", tc.name)

			cmd := captured.Load()
			require.NotNil(t, cmd)
			require.NotNil(t, cmd.ProcessState, "scene=%s want=child_reaped_by_cmd_wait got=nil_process_state", tc.name)
			require.ErrorIs(t, syscall.Kill(cmd.Process.Pid, 0), syscall.ESRCH, "scene=%s want=child_reaped_no_zombie", tc.name)
		})
	}
}

func TestSpawnDaemonProcess_GivenHandshakeTimeout_TerminatesAndReapsChild(t *testing.T) {
	withDaemonTimeouts(t, 200*time.Millisecond, 200*time.Millisecond)
	_, stopped := withDaemonSignalStub(t)
	captured := withDaemonHelperCommand(t, "hang_sleep", "", "")

	code, errOut := runSpawnWithDeadline(t, 2*time.Second)

	require.Equal(t, 1, code, "scene=handshake_timeout want=1 got=%d stderr=%s", code, errOut)
	require.Contains(t, errOut, "timed out", "scene=handshake_timeout stderr=%s", errOut)
	require.True(t, stopped.Load(), "scene=handshake_timeout want=signal_stop_called got=false")

	cmd := captured.Load()
	require.NotNil(t, cmd)
	require.NotNil(t, cmd.ProcessState, "scene=handshake_timeout want=child_reaped")
	require.ErrorIs(t, syscall.Kill(cmd.Process.Pid, 0), syscall.ESRCH, "scene=handshake_timeout want=child_reaped_no_zombie")
}

func TestSpawnDaemonProcess_GivenParentSignalAndChildExitsOnSIGTERM_TerminatesGracefullyAndReapsChild(t *testing.T) {
	readyFile := filepath.Join(t.TempDir(), "ready")
	markerFile := filepath.Join(t.TempDir(), "marker")
	withDaemonTimeouts(t, 10*time.Second, 5*time.Second)
	sigCh, stopped := withDaemonSignalStub(t)
	captured := withDaemonHelperCommand(t, "hang_term", readyFile, markerFile)

	go func() {
		for range 200 {
			if _, err := os.Stat(readyFile); err == nil {
				sigCh <- syscall.SIGINT
				return
			}
			time.Sleep(10 * time.Millisecond)
		}
	}()

	start := time.Now()
	code, errOut := runSpawnWithDeadline(t, 3*time.Second)
	elapsed := time.Since(start)

	require.Equal(t, 1, code, "scene=parent_sig_graceful want=1 got=%d stderr=%s", code, errOut)
	require.Contains(t, errOut, "interrupted", "scene=parent_sig_graceful stderr=%s", errOut)
	require.True(t, stopped.Load(), "scene=parent_sig_graceful want=signal_stop_called got=false")
	require.Less(t, elapsed, 3*time.Second, "scene=parent_sig_graceful want=exited_before_kill_grace got=%s", elapsed)

	markerBytes, readErr := os.ReadFile(markerFile)
	require.NoError(t, readErr, "scene=parent_sig_graceful want=child_received_sigterm")
	require.Equal(t, "got_sigterm", string(markerBytes))

	cmd := captured.Load()
	require.NotNil(t, cmd)
	require.NotNil(t, cmd.ProcessState, "scene=parent_sig_graceful want=child_reaped")
	require.ErrorIs(t, syscall.Kill(cmd.Process.Pid, 0), syscall.ESRCH, "scene=parent_sig_graceful want=no_zombie")
}

func TestSpawnDaemonProcess_GivenParentSignalAndChildIgnoresSIGTERM_EscalatesToSIGKILLAndReapsChild(t *testing.T) {
	readyFile := filepath.Join(t.TempDir(), "ready")
	markerFile := filepath.Join(t.TempDir(), "marker")
	withDaemonTimeouts(t, 10*time.Second, 100*time.Millisecond)
	sigCh, stopped := withDaemonSignalStub(t)
	captured := withDaemonHelperCommand(t, "hang_ignore_term", readyFile, markerFile)

	go func() {
		for range 200 {
			if _, err := os.Stat(readyFile); err == nil {
				sigCh <- syscall.SIGTERM
				return
			}
			time.Sleep(10 * time.Millisecond)
		}
	}()

	code, errOut := runSpawnWithDeadline(t, 3*time.Second)

	require.Equal(t, 1, code, "scene=sigkill_escalation want=1 got=%d stderr=%s", code, errOut)
	require.Contains(t, errOut, "interrupted", "scene=sigkill_escalation stderr=%s", errOut)
	require.True(t, stopped.Load(), "scene=sigkill_escalation want=signal_stop_called got=false")

	markerBytes, readErr := os.ReadFile(markerFile)
	require.NoError(t, readErr, "scene=sigkill_escalation want=child_received_sigterm_first")
	require.Equal(t, "got_sigterm", string(markerBytes))

	cmd := captured.Load()
	require.NotNil(t, cmd)
	require.NotNil(t, cmd.ProcessState, "scene=sigkill_escalation want=child_reaped")
	ws, ok := cmd.ProcessState.Sys().(syscall.WaitStatus)
	require.True(t, ok)
	require.Equal(t, syscall.SIGKILL, ws.Signal(), "scene=sigkill_escalation want=killed_by_SIGKILL got=%v", ws.Signal())
	require.ErrorIs(t, syscall.Kill(cmd.Process.Pid, 0), syscall.ESRCH, "scene=sigkill_escalation want=no_zombie")
}
