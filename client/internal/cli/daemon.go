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

// readDaemonHandshake 阻塞读取守护进程回传的挂载就绪状态。
func readDaemonHandshake(r io.Reader, stderr io.Writer) int {
	scanner := bufio.NewScanner(r)
	if scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "OK" {
			return 0
		}
		if after, ok := strings.CutPrefix(line, "ERR:"); ok {
			fmt.Fprintf(stderr, "错误: 后台守护进程初始化失败: %s\n", after)
			return 1
		}
	}
	fmt.Fprintf(stderr, "错误: 后台守护进程异常退出，未完成挂载握手\n")
	return 1
}

var spawnDaemon = spawnDaemonProcess

// spawnDaemonProcess 由前台父进程派生后台守护进程并阻塞等待就绪握手。
func spawnDaemonProcess(args []string, stderr io.Writer) int {
	r, w, err := os.Pipe()
	if err != nil {
		fmt.Fprintf(stderr, "错误: 创建就绪通知管道失败: %v\n", err)
		return 1
	}
	defer r.Close()

	binPath, err := os.Executable()
	if err != nil {
		fmt.Fprintf(stderr, "错误: 获取可执行文件路径失败: %v\n", err)
		_ = w.Close()
		return 1
	}

	subArgs := append([]string{"mount"}, args...)
	cmd := exec.Command(binPath, subArgs...)
	// 守护进程不能继承调用者的工作目录：它会一直持有该目录，导致同一挂载点所属
	// 文件系统上的 umount 因 Resource busy 失败。
	cmd.Dir = "/"
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
		fmt.Fprintf(stderr, "错误: 启动后台守护进程失败: %v\n", startErr)
		return 1
	}

	// 父进程必须立即关闭自身的管道写端，否则子进程退出时读 r 无法触发 EOF
	_ = w.Close()

	return readDaemonHandshake(r, stderr)
}

// runDaemonWorker 是子进程内部的后台挂载执行器（挂载期间维护后台租约续期并在卸载刷盘后显式释放租约）。
// 日志文件重定向标准输出/错误后，复用前台同一条挂载链路；挂载就绪时经 fd 3 管道回传 OK。
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

	// 1. 准备日志文件并重定向标准输出/错误
	logF, _, err := ResolveLogFile(opts.logFile, baseCfg.DiskID)
	if err != nil {
		notifyErr(err)
		return 1
	}
	defer logF.Close()

	logFd := int(logF.Fd())
	_ = unix.Dup2(logFd, int(os.Stdout.Fd()))
	_ = unix.Dup2(logFd, int(os.Stderr.Fd()))

	// 2. 执行挂载；就绪回调通知父进程退出 0
	if mountErr := runMount(ctx, opts, baseCfg, mountpoint, logF, notifyOK); mountErr != nil {
		notifyErr(mountErr)
		fmt.Fprintf(logF, "错误: 挂载执行失败: %v\n", mountErr)
		_ = logF.Sync()
		return 1
	}
	_ = logF.Sync()
	return 0
}
