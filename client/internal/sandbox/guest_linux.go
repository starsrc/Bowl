package sandbox

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"
	"sync"
	"syscall"

	"golang.org/x/sys/unix"
)

var guestEnv = []string{
	"PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin",
	"HOME=/root",
	"LANG=C.UTF-8",
}

func ServeGuest() error {
	for _, kv := range guestEnv {
		k, v, _ := strings.Cut(kv, "=")
		_ = os.Setenv(k, v)
	}
	if os.Getpid() == 1 {
		for _, m := range []struct{ src, dst, typ string }{
			{"proc", "/proc", "proc"},
			{"sysfs", "/sys", "sysfs"},
			{"tmpfs", "/dev/shm", "tmpfs"},
			{"devpts", "/dev/pts", "devpts"},
		} {
			_ = os.MkdirAll(m.dst, 0o755)
			data := ""
			if m.typ == "devpts" {
				data = "gid=5,mode=620,ptmxmode=666"
			}
			if err := unix.Mount(m.src, m.dst, m.typ, 0, data); err != nil {
				fmt.Fprintf(os.Stderr, "bowl guest: mount %s: %v\n", m.dst, err)
			}
		}
	}
	fd, err := unix.Socket(unix.AF_VSOCK, unix.SOCK_STREAM|unix.SOCK_CLOEXEC, 0)
	if err != nil {
		return fmt.Errorf("创建 vsock: %w", err)
	}
	if err := unix.Bind(fd, &unix.SockaddrVM{CID: unix.VMADDR_CID_ANY, Port: GuestPort}); err != nil {
		return fmt.Errorf("绑定 vsock 端口 %d: %w", GuestPort, err)
	}
	if err := unix.Listen(fd, 64); err != nil {
		return fmt.Errorf("监听 vsock: %w", err)
	}
	fmt.Fprintf(os.Stderr, "bowl guest: listening on vsock port %d\n", GuestPort)
	for {
		nfd, _, err := unix.Accept4(fd, unix.SOCK_CLOEXEC)
		if err != nil {
			if errors.Is(err, unix.EINTR) {
				continue
			}
			return fmt.Errorf("接受连接: %w", err)
		}
		go serveExec(os.NewFile(uintptr(nfd), "vsock-conn"))
	}
}

func serveExec(conn *os.File) {
	defer conn.Close()
	typ, p, err := readFrame(conn)
	if err != nil || typ != frameRequest {
		return
	}
	var req execRequest
	if err = json.Unmarshal(p, &req); err != nil || (len(req.Argv) == 0 && req.Reset == nil) {
		return
	}

	var mu sync.Mutex
	stdout := frameWriter{mu: &mu, w: conn, typ: frameStdout}
	stderr := frameWriter{mu: &mu, w: conn, typ: frameStderr}
	if req.Reset != nil {
		code := 0
		if err = applyReset(req.Reset); err != nil {
			code = 1
		}
		finishExec(&mu, conn, stderr, code, err)
		return
	}

	cmd := exec.Command(req.Argv[0], req.Argv[1:]...)
	cmd.Dir = req.Cwd
	cmd.Env = append(append([]string{}, guestEnv...), req.Env...)
	var stdin io.WriteCloser
	var ptyOut chan struct{}
	if req.TTY {
		ptmx, ptyErr := startWithPTY(cmd, req.Rows, req.Cols)
		if ptyErr != nil {
			finishExec(&mu, conn, stderr, 127, ptyErr)
			return
		}
		stdin = ptmx
		ptyOut = make(chan struct{})
		go func() {
			defer close(ptyOut)
			_, _ = io.Copy(stdout, ptmx)
		}()
	} else {
		cmd.Stdout, cmd.Stderr = stdout, stderr
		if stdin, err = cmd.StdinPipe(); err != nil {
			finishExec(&mu, conn, stderr, 1, err)
			return
		}
		if err = cmd.Start(); err != nil {
			finishExec(&mu, conn, stderr, 127, err)
			return
		}
	}

	go func() {
		if ptyOut == nil {
			defer stdin.Close()
		}
		for {
			typ, p, err := readFrame(conn)
			if err != nil {
				_ = cmd.Process.Kill()
				return
			}
			if typ != frameStdin {
				continue
			}
			if len(p) == 0 {
				return
			}
			if _, err := stdin.Write(p); err != nil {
				return
			}
		}
	}()

	code := 0
	waitErr := cmd.Wait()
	if ptyOut != nil {
		<-ptyOut
		_ = stdin.Close()
	}
	if err := waitErr; err != nil {
		var exitErr *exec.ExitError
		if !errors.As(err, &exitErr) {
			finishExec(&mu, conn, stderr, 1, err)
			return
		}
		code = exitErr.ExitCode()
		if ws, ok := exitErr.Sys().(syscall.WaitStatus); ok && ws.Signaled() {
			code = 128 + int(ws.Signal())
		}
	}
	finishExec(&mu, conn, nil, code, nil)
}

func finishExec(mu *sync.Mutex, conn, stderr io.Writer, code int, err error) {
	if err != nil && stderr != nil {
		fmt.Fprintf(stderr, "bowl guest: %v\n", err)
	}
	mu.Lock()
	defer mu.Unlock()
	_ = writeFrame(conn, frameExit, exitPayload(code))
}

func startWithPTY(cmd *exec.Cmd, rows, cols uint16) (*os.File, error) {
	ptmx, err := os.OpenFile("/dev/ptmx", os.O_RDWR|unix.O_NOCTTY|unix.O_CLOEXEC, 0)
	if err != nil {
		return nil, fmt.Errorf("打开 /dev/ptmx: %w", err)
	}
	fail := func(err error) (*os.File, error) {
		_ = ptmx.Close()
		return nil, err
	}
	fd := int(ptmx.Fd())
	if err = unix.IoctlSetPointerInt(fd, unix.TIOCSPTLCK, 0); err != nil {
		return fail(fmt.Errorf("解锁伪终端: %w", err))
	}
	n, err := unix.IoctlGetInt(fd, unix.TIOCGPTN)
	if err != nil {
		return fail(fmt.Errorf("取伪终端编号: %w", err))
	}
	pts, err := os.OpenFile(fmt.Sprintf("/dev/pts/%d", n), os.O_RDWR|unix.O_NOCTTY, 0)
	if err != nil {
		return fail(fmt.Errorf("打开伪终端从端: %w", err))
	}
	defer pts.Close()
	if rows > 0 && cols > 0 {
		_ = unix.IoctlSetWinsize(int(pts.Fd()), unix.TIOCSWINSZ, &unix.Winsize{Row: rows, Col: cols})
	}
	cmd.Stdin, cmd.Stdout, cmd.Stderr = pts, pts, pts
	cmd.Env = append(cmd.Env, "TERM=xterm-256color")
	cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true, Setctty: true, Ctty: 0}
	if err = cmd.Start(); err != nil {
		return fail(err)
	}
	return ptmx, nil
}
