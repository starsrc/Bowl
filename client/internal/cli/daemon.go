package cli

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"golang.org/x/sys/unix"

	"github.com/starsrc/bowl/client/internal/fusefs"
	"github.com/starsrc/bowl/client/internal/rpc"
)

type daemonHandshakeOutcome struct {
	ok     bool
	errMsg string
}

func readDaemonHandshakeOutcome(r io.Reader) daemonHandshakeOutcome {
	scanner := bufio.NewScanner(r)
	if scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "OK" {
			return daemonHandshakeOutcome{ok: true}
		}
		if after, ok := strings.CutPrefix(line, "ERR:"); ok {
			return daemonHandshakeOutcome{
				errMsg: fmt.Sprintf("Error: background daemon failed to initialize: %s\n", after),
			}
		}
	}
	return daemonHandshakeOutcome{
		errMsg: "Error: background daemon exited unexpectedly before completing mount handshake\n",
	}
}

func readDaemonHandshake(r io.Reader, stderr io.Writer) int {
	outcome := readDaemonHandshakeOutcome(r)
	if outcome.ok {
		return 0
	}
	_, _ = io.WriteString(stderr, outcome.errMsg)
	return 1
}

var (
	spawnDaemon            = spawnDaemonProcess
	chdirFn                = os.Chdir
	readinessPipeFile      = func() *os.File { return os.NewFile(3, "readiness-pipe") }
	redirectDaemonStdio    = defaultRedirectDaemonStdio
	daemonHandshakeTimeout = 30 * time.Second
	daemonKillGrace        = 5 * time.Second
	daemonSignalNotify     = defaultDaemonSignalNotify
	daemonCommandFn        = defaultDaemonCommand
)

func defaultDaemonSignalNotify() (sigCh <-chan os.Signal, stop func()) {
	ch := make(chan os.Signal, 2)
	signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM)
	return ch, func() { signal.Stop(ch) }
}

func mountFlagTakesSeparateValue(arg string) bool {
	switch arg {
	case "--endpoint",
		"--log-file",
		"--mode",
		"--stage-size",
		"--stage-dir",
		"--unmount-timeout",
		"--unmount-stage-timeout",
		"--unmount-stage-stall-timeout",
		"--unmount-fsync-timeout",
		"--admin-addr",
		"--log-level",
		"--log-format",
		"--trace-sample-ratio":
		return true
	default:
		return false
	}
}

func scrubDaemonTokenArgs(args []string) (filtered []string, token string) {
	filtered = make([]string, 0, len(args))
	for i := 0; i < len(args); i++ {
		arg := args[i]
		if arg == "--" {
			filtered = append(filtered, args[i:]...)
			break
		}
		if arg == "--token" {
			if i+1 < len(args) {
				token = args[i+1]
				i++
			}
			continue
		}
		if val, ok := strings.CutPrefix(arg, "--token="); ok {
			token = val
			continue
		}
		filtered = append(filtered, arg)
		if mountFlagTakesSeparateValue(arg) && i+1 < len(args) {
			i++
			filtered = append(filtered, args[i])
		}
	}
	return filtered, token
}

func buildDaemonEnv(baseEnv []string, token string) []string {
	trimmedToken := strings.TrimSpace(token)
	env := make([]string, 0, len(baseEnv)+2)
	for _, entry := range baseEnv {
		if trimmedToken != "" && strings.HasPrefix(entry, rpc.EnvToken+"=") {
			continue
		}
		if strings.HasPrefix(entry, "BOWL_DAEMON_WORKER=") {
			continue
		}
		env = append(env, entry)
	}
	if trimmedToken != "" {
		env = append(env, rpc.EnvToken+"="+trimmedToken)
	}
	env = append(env, "BOWL_DAEMON_WORKER=1")
	return env
}

func defaultDaemonCommand(args []string, w *os.File) (*exec.Cmd, error) {
	binPath, err := os.Executable()
	if err != nil {
		return nil, fmt.Errorf("failed to resolve executable path: %w", err)
	}
	scrubbedArgs, token := scrubDaemonTokenArgs(args)
	subArgs := append([]string{"mount"}, scrubbedArgs...)
	cmd := exec.Command(binPath, subArgs...)
	cmd.ExtraFiles = []*os.File{w}
	cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
	cmd.Env = buildDaemonEnv(os.Environ(), token)
	return cmd, nil
}

func defaultRedirectDaemonStdio(logF *os.File) {
	logFd := int(logF.Fd())
	_ = unix.Dup2(logFd, int(os.Stdout.Fd()))
	_ = unix.Dup2(logFd, int(os.Stderr.Fd()))
}

func terminateAndReapDaemonChild(cmd *exec.Cmd, grace time.Duration) {
	if cmd == nil || cmd.Process == nil {
		return
	}
	if grace <= 0 {
		grace = daemonKillGrace
	}
	if grace <= 0 {
		grace = 5 * time.Second
	}
	_ = cmd.Process.Signal(syscall.SIGTERM)

	waitDone := make(chan struct{})
	go func() {
		defer close(waitDone)
		_ = cmd.Wait()
	}()

	graceTimer := time.NewTimer(grace)
	defer graceTimer.Stop()

	select {
	case <-waitDone:
	case <-graceTimer.C:
		_ = cmd.Process.Signal(syscall.SIGKILL)
		<-waitDone
	}
}

func spawnDaemonProcess(args []string, stderr io.Writer) int {
	r, w, err := os.Pipe()
	if err != nil {
		fmt.Fprintf(stderr, "Error: failed to create readiness pipe: %v\n", err)
		return 1
	}
	defer func() { _ = r.Close() }()

	cmd, err := daemonCommandFn(args, w)
	if err != nil {
		fmt.Fprintf(stderr, "Error: %v\n", err)
		_ = w.Close()
		return 1
	}

	devNull, devNullErr := os.Open(os.DevNull)
	if devNullErr == nil {
		cmd.Stdin = devNull
		defer func() { _ = devNull.Close() }()
	}

	sigCh, stopSignals := daemonSignalNotify()
	defer stopSignals()

	if startErr := cmd.Start(); startErr != nil {
		_ = w.Close()
		fmt.Fprintf(stderr, "Error: failed to start background daemon: %v\n", startErr)
		return 1
	}

	_ = w.Close()

	timeout := daemonHandshakeTimeout
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	timer := time.NewTimer(timeout)
	defer timer.Stop()

	readDone := make(chan daemonHandshakeOutcome, 1)
	go func() {
		readDone <- readDaemonHandshakeOutcome(r)
	}()

	select {
	case outcome := <-readDone:
		_ = r.Close()
		if outcome.ok {
			return 0
		}
		_ = cmd.Wait()
		_, _ = io.WriteString(stderr, outcome.errMsg)
		return 1
	case <-timer.C:
		terminateAndReapDaemonChild(cmd, daemonKillGrace)
		_ = r.Close()
		<-readDone
		fmt.Fprintf(stderr, "Error: background daemon timed out after %s waiting for mount handshake\n", timeout)
		return 1
	case sig := <-sigCh:
		terminateAndReapDaemonChild(cmd, daemonKillGrace)
		_ = r.Close()
		<-readDone
		fmt.Fprintf(stderr, "Error: background daemon startup interrupted by signal %s\n", sig)
		return 1
	}
}

func sanitizeDaemonReadinessErr(err error) string {
	msg := strings.ReplaceAll(err.Error(), "\r", " ")
	return strings.ReplaceAll(msg, "\n", " ")
}

func writeDaemonReadinessError(err error) {
	pipeWriter := readinessPipeFile()
	if pipeWriter == nil {
		return
	}
	_, _ = fmt.Fprintf(pipeWriter, "ERR:%s\n", sanitizeDaemonReadinessErr(err))
	_ = pipeWriter.Close()
}

func runDaemonWorker(ctx context.Context, opts *mountOptions, baseCfg *fusefs.Config, mountpoint string) int {
	workerCtx, stopSignals := mountSignalContext(ctx)
	defer stopSignals()

	pipeWriter := readinessPipeFile()
	notifyErr := func(err error) {
		if pipeWriter != nil {
			_, _ = fmt.Fprintf(pipeWriter, "ERR:%s\n", sanitizeDaemonReadinessErr(err))
			_ = pipeWriter.Close()
			pipeWriter = nil
		}
	}
	notifyOK := func() {
		if pipeWriter != nil {
			fmt.Fprintln(pipeWriter, "OK")
			_ = pipeWriter.Close()
			pipeWriter = nil
		}
	}

	if err := workerCtx.Err(); err != nil {
		notifyErr(err)
		return 1
	}

	logF, _, err := ResolveLogFile(opts.logFile, baseCfg.DiskID)
	if err != nil {
		notifyErr(err)
		return 1
	}
	defer logF.Close()

	if chdirErr := chdirFn("/"); chdirErr != nil {
		notifyErr(fmt.Errorf("change daemon working directory to /: %w", chdirErr))
		return 1
	}

	redirectDaemonStdio(logF)

	if mountErr := runMount(workerCtx, opts, baseCfg, mountpoint, logF, notifyOK); mountErr != nil {
		notifyErr(mountErr)
		fmt.Fprintf(logF, "Error: mount failed: %v\n", mountErr)
		_ = logF.Sync()
		return 1
	}
	_ = logF.Sync()
	return 0
}
