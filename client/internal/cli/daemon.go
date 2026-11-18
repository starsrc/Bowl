package cli

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"
	"syscall"

	"golang.org/x/sys/unix"

	"github.com/starsrc/bowl/client/internal/fusefs"
)

// readDaemonHandshake blocks waiting for the readiness status line from the daemon child.
func readDaemonHandshake(r io.Reader, stderr io.Writer) int {
	scanner := bufio.NewScanner(r)
	if scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "OK" {
			return 0
		}
		if after, ok := strings.CutPrefix(line, "ERR:"); ok {
			fmt.Fprintf(stderr, "Error: background daemon failed to initialize: %s\n", after)
			return 1
		}
	}
	fmt.Fprintf(stderr, "Error: background daemon exited unexpectedly before completing mount handshake\n")
	return 1
}

var spawnDaemon = spawnDaemonProcess

// spawnDaemonProcess spawns a background mount daemon and waits for its readiness handshake.
func spawnDaemonProcess(args []string, stderr io.Writer) int {
	r, w, err := os.Pipe()
	if err != nil {
		fmt.Fprintf(stderr, "Error: failed to create readiness pipe: %v\n", err)
		return 1
	}
	defer r.Close()

	binPath, err := os.Executable()
	if err != nil {
		fmt.Fprintf(stderr, "Error: failed to resolve executable path: %v\n", err)
		_ = w.Close()
		return 1
	}

	subArgs := append([]string{"mount"}, args...)
	cmd := exec.Command(binPath, subArgs...)
	cmd.ExtraFiles = []*os.File{w}
	cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
	cmd.Env = append(os.Environ(), "BOWL_DAEMON_WORKER=1")

	devNull, devNullErr := os.Open(os.DevNull)
	if devNullErr == nil {
		cmd.Stdin = devNull
		defer devNull.Close()
	}
	if startErr := cmd.Start(); startErr != nil {
		_ = w.Close()
		fmt.Fprintf(stderr, "Error: failed to start background daemon: %v\n", startErr)
		return 1
	}

	// Close the parent write end immediately so child exit triggers EOF on r.
	_ = w.Close()

	return readDaemonHandshake(r, stderr)
}

// runDaemonWorker runs inside the background daemon child, redirects stdout/stderr to the log file, and signals readiness over fd 3.
func runDaemonWorker(ctx context.Context, opts *mountOptions, baseCfg *fusefs.Config, mountpoint string) int {
	pipeWriter := os.NewFile(3, "readiness-pipe")
	notifyErr := func(err error) {
		if pipeWriter != nil {
			fmt.Fprintf(pipeWriter, "ERR:%v\n", err)
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

	logF, _, err := ResolveLogFile(opts.logFile, baseCfg.DiskID)
	if err != nil {
		notifyErr(err)
		return 1
	}
	defer logF.Close()

	// 守护进程不能一直停在调用者的工作目录：它会持有该目录，使其所在文件系统的 umount 因
	// Resource busy 失败。挂载点与日志路径此时都已按原目录解析完毕，之后不再有相对路径。
	if chdirErr := os.Chdir("/"); chdirErr != nil {
		notifyErr(fmt.Errorf("切换守护进程工作目录失败: %w", chdirErr))
		return 1
	}

	logFd := int(logF.Fd())
	_ = unix.Dup2(logFd, int(os.Stdout.Fd()))
	_ = unix.Dup2(logFd, int(os.Stderr.Fd()))

	if mountErr := runMount(ctx, opts, baseCfg, mountpoint, logF, notifyOK); mountErr != nil {
		notifyErr(mountErr)
		fmt.Fprintf(logF, "Error: mount failed: %v\n", mountErr)
		_ = logF.Sync()
		return 1
	}
	_ = logF.Sync()
	return 0
}
