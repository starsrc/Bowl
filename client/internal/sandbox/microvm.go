package sandbox

import (
	"bufio"
	"bytes"
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"golang.org/x/term"
)

type MicroVM struct {
	StateDir        string
	Kernel          string
	GuestAgent      string
	CloudHypervisor string
	CPUs, MemoryMiB int
	DNS             string
}

type CreateSpec struct {
	ID              string
	Template        string
	CPUs, MemoryMiB int
}

type Info struct {
	ID, Status, Template, Created, Address string
	PID, CPUs, MemoryMiB                   int
}

type ExecOptions struct {
	TTY bool
	Env []string
}

const (
	guestAgentPath = "/.bowl/guest"
	microVMWorkdir = "/root"
	readyTimeout   = 20 * time.Second
	runDir         = "/tmp/.bowl-exec"
	runIDLayout    = "20060102-150405"
)

var errUnexpectedReply = errors.New("应答不是 OK")

type vmState struct {
	ID        string `json:"id"`
	Template  string `json:"template"`
	Created   string `json:"created"`
	VMPid     int    `json:"vm_pid"`
	CPUs      int    `json:"cpus"`
	MemoryMiB int    `json:"memory_mib"`
	NetIndex  int    `json:"net_index"`
	Paused    bool   `json:"paused"`
}

func (m *MicroVM) stateDir() string {
	return cmp.Or(m.StateDir, "/var/lib/bowl-sandbox")
}

func (m *MicroVM) dir(id string) string { return filepath.Join(m.stateDir(), id) }

func (m *MicroVM) Create(ctx context.Context, spec *CreateSpec) error {
	id := spec.ID
	if !validName(id) {
		return fmt.Errorf("沙箱 ID %q 不合法", id)
	}
	src, err := m.templateImage(spec.Template)
	if err != nil {
		return err
	}
	dir := m.dir(id)
	if _, statErr := os.Stat(dir); statErr == nil {
		return fmt.Errorf("沙箱 %s 已存在", id)
	}
	if err = os.MkdirAll(dir, 0o700); err != nil {
		return fmt.Errorf("创建状态目录: %w", err)
	}
	st := vmState{
		ID: id, Template: spec.Template, Created: time.Now().UTC().Format(time.RFC3339),
		CPUs: cmp.Or(spec.CPUs, m.CPUs, 1), MemoryMiB: cmp.Or(spec.MemoryMiB, m.MemoryMiB, 1024),
	}
	boot := func() error { return m.start(ctx, &st) }
	tplDir := filepath.Dir(src)
	if snap := m.snapshotFor(tplDir, st.CPUs, st.MemoryMiB); snap != "" {
		src = filepath.Join(tplDir, snapshotDisk)
		boot = func() error { return m.restore(ctx, &st, snap) }
	}
	if err = m.cloneSystemDisk(ctx, src, id); err != nil {
		_ = os.RemoveAll(dir)
		return err
	}
	if err = boot(); err != nil {
		_ = m.Remove(context.WithoutCancel(ctx), id)
		return err
	}
	return nil
}

func (m *MicroVM) start(ctx context.Context, st *vmState) error {
	dir := m.dir(st.ID)
	if err := m.saveState(st); err != nil {
		return err
	}
	netArgs, ipArg, err := m.setupNetwork(st)
	if err != nil {
		return err
	}
	vm, err := startDetached(filepath.Join(dir, "vm.log"), m.cloudHypervisor(),
		"--kernel", m.kernel(),
		"--cmdline", "console=ttyS0 root=/dev/vda rw init="+guestAgentPath+" quiet "+ipArg,
		"--disk", "path="+m.systemDisk(st.ID)+",image_type=raw",
		"--memory", "size="+strconv.Itoa(st.MemoryMiB)+"M",
		"--cpus", "boot="+strconv.Itoa(st.CPUs),
		"--vsock", "cid=3,socket="+filepath.Join(dir, "vsock.sock"),
		"--api-socket", filepath.Join(dir, "api.sock"),
		"--net", netArgs,
		"--serial", "file="+filepath.Join(dir, "console.log"),
		"--console", "off",
	)
	if err != nil {
		return fmt.Errorf("启动虚机: %w", err)
	}
	st.VMPid = vm
	if err := m.saveState(st); err != nil {
		return err
	}
	ready := func() bool {
		code, err := m.Exec(ctx, st.ID, "/", []string{"true"}, ExecOptions{}, nil, io.Discard, io.Discard)
		return err == nil && code == 0
	}
	if err := waitFor(ctx, readyTimeout, ready); err != nil {
		return fmt.Errorf("虚机里的执行代理没有就绪: %w\n%s", err, tailFile(filepath.Join(dir, "console.log"), 15))
	}
	return nil
}

func (m *MicroVM) Remove(ctx context.Context, id string) error {
	if err := m.Stop(ctx, id); err != nil {
		return err
	}
	if err := m.removeSystemDisk(id); err != nil {
		return err
	}
	return os.RemoveAll(m.dir(id))
}

func (m *MicroVM) Stop(ctx context.Context, id string) error {
	st, err := m.loadState(id)
	if err != nil {
		return err
	}
	if !st.Paused && processAlive(st.VMPid) {
		m.guestSync(ctx, id)
	}
	stopProcess(st.VMPid)
	st.VMPid, st.Paused = 0, false
	return m.saveState(&st)
}

func (m *MicroVM) Start(ctx context.Context, id string) error {
	st, err := m.loadState(id)
	if err != nil {
		return err
	}
	if processAlive(st.VMPid) {
		return fmt.Errorf("沙箱 %s 已经在运行", id)
	}
	for _, sock := range []string{"vsock.sock", "api.sock"} {
		_ = os.Remove(filepath.Join(m.dir(id), sock))
	}
	if err := m.start(ctx, &st); err != nil {
		_ = m.Stop(context.WithoutCancel(ctx), id)
		return err
	}
	return nil
}

func (m *MicroVM) Exists(id string) bool {
	_, err := m.loadState(id)
	return err == nil
}

func (m *MicroVM) List() ([]Info, error) {
	ids, err := m.IDs()
	if err != nil {
		return nil, err
	}
	rows := make([]Info, 0, len(ids))
	for _, id := range ids {
		if info, infoErr := m.Info(id); infoErr == nil {
			rows = append(rows, info)
		}
	}
	return rows, nil
}

func (m *MicroVM) IDs() ([]string, error) {
	entries, err := os.ReadDir(m.stateDir())
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("读取状态目录: %w", err)
	}
	ids := make([]string, 0, len(entries))
	for _, e := range entries {
		if e.IsDir() && isRegularFile(filepath.Join(m.stateDir(), e.Name(), "state.json")) {
			ids = append(ids, e.Name())
		}
	}
	return ids, nil
}

func (m *MicroVM) Info(id string) (Info, error) {
	st, err := m.loadState(id)
	if err != nil {
		return Info{}, err
	}
	return Info{
		ID: id, Status: vmStatus(&st), Template: st.Template, Created: st.Created,
		PID: st.VMPid, Address: guestIP(st.NetIndex), CPUs: st.CPUs, MemoryMiB: st.MemoryMiB,
	}, nil
}

func (m *MicroVM) Exec(ctx context.Context, id, cwd string, argv []string, opts ExecOptions, in io.Reader, out, errOut io.Writer) (int, error) {
	if len(argv) == 0 {
		return 2, errors.New("缺少要执行的命令")
	}
	reqBody := execRequest{Argv: argv, Cwd: cmp.Or(cwd, microVMWorkdir), Env: opts.Env, TTY: opts.TTY}
	if f, ok := in.(*os.File); ok && opts.TTY && term.IsTerminal(int(f.Fd())) {
		fd := int(f.Fd())
		if cols, rows, sizeErr := term.GetSize(fd); sizeErr == nil {
			reqBody.Rows, reqBody.Cols = clampUint16(rows), clampUint16(cols)
		}
		if old, rawErr := term.MakeRaw(fd); rawErr == nil {
			defer func() { _ = term.Restore(fd, old) }()
		}
	}
	return m.run(ctx, id, &reqBody, in, out, errOut)
}

func clampUint16(n int) uint16 {
	return uint16(min(max(n, 0), 1<<16-1))
}

func (m *MicroVM) run(ctx context.Context, id string, reqBody *execRequest, in io.Reader, out, errOut io.Writer) (int, error) {
	conn, rd, err := dialGuest(ctx, filepath.Join(m.dir(id), "vsock.sock"))
	if err != nil {
		return 1, err
	}
	defer conn.Close()
	stop := context.AfterFunc(ctx, func() { _ = conn.Close() })
	defer stop()
	req, err := json.Marshal(reqBody)
	if err != nil {
		return 1, err
	}
	var mu sync.Mutex
	if err := writeFrame(conn, frameRequest, req); err != nil {
		return 1, fmt.Errorf("发送命令: %w", err)
	}
	go func() {
		stdin := frameWriter{mu: &mu, w: conn, typ: frameStdin}
		if in != nil {
			_, _ = io.Copy(stdin, in)
		}
		mu.Lock()
		_ = writeFrame(conn, frameStdin, nil)
		mu.Unlock()
	}()

	for {
		typ, p, err := readFrame(rd)
		if err != nil {
			if ctx.Err() != nil {
				return 1, ctx.Err()
			}
			return 1, fmt.Errorf("和虚机的连接断了: %w", err)
		}
		switch typ {
		case frameStdout:
			_, _ = out.Write(p)
		case frameStderr:
			_, _ = errOut.Write(p)
		case frameExit:
			return parseExit(p)
		}
	}
}

func dialGuest(ctx context.Context, sock string) (net.Conn, *bufio.Reader, error) {
	var d net.Dialer
	conn, err := d.DialContext(ctx, "unix", sock)
	if err != nil {
		return nil, nil, fmt.Errorf("连接虚机: %w", err)
	}
	if _, err = fmt.Fprintf(conn, "CONNECT %d\n", GuestPort); err != nil {
		_ = conn.Close()
		return nil, nil, fmt.Errorf("连接虚机: %w", err)
	}
	rd := bufio.NewReader(conn)
	_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	line, err := rd.ReadString('\n')
	_ = conn.SetReadDeadline(time.Time{})
	if err != nil || !strings.HasPrefix(line, "OK ") {
		_ = conn.Close()
		return nil, nil, fmt.Errorf("虚机里的代理没有应答 %q: %w", strings.TrimSpace(line), cmp.Or(err, errUnexpectedReply))
	}
	return conn, rd, nil
}

func (m *MicroVM) ExecBackground(ctx context.Context, id, cwd string, argv []string) (string, error) {
	if len(argv) == 0 {
		return "", errors.New("缺少要执行的命令")
	}
	runID := time.Now().Format(runIDLayout)
	dir := runDir + "/" + runID
	job := "( " + commandScript(cmp.Or(cwd, microVMWorkdir), argv) + " ) > " + dir + "/out.log 2>&1; echo $? > " + dir + "/exit"
	line := "mkdir -p " + dir + " && setsid sh -c " + shellQuote(job) + " </dev/null >/dev/null 2>&1 &"
	var errBuf bytes.Buffer
	code, err := m.Exec(ctx, id, "/", []string{"sh", "-c", line}, ExecOptions{}, nil, io.Discard, &errBuf)
	if err != nil {
		return "", err
	}
	if code != 0 {
		return "", fmt.Errorf("启动后台命令失败: %s", strings.TrimSpace(errBuf.String()))
	}
	return runID, nil
}

func (m *MicroVM) Logs(ctx context.Context, id, runID string, follow bool, out, errOut io.Writer) error {
	if runID == "" {
		latest, err := m.shellOutput(ctx, id, "ls -1 "+runDir+" 2>/dev/null | sort | tail -1")
		if err != nil {
			return err
		}
		if runID = strings.TrimSpace(latest); runID == "" {
			return errors.New("沙箱里还没有后台运行记录")
		}
	}
	dir := runDir + "/" + runID
	if follow {
		_, err := m.Exec(ctx, id, "/", []string{"tail", "-f", dir + "/out.log"}, ExecOptions{}, nil, out, errOut)
		return err
	}
	code, err := m.Exec(ctx, id, "/", []string{"cat", dir + "/out.log"}, ExecOptions{}, nil, out, errOut)
	if err != nil {
		return err
	}
	if code != 0 {
		return fmt.Errorf("读取日志失败(运行 %s)", runID)
	}
	if exit, err := m.shellOutput(ctx, id, "cat "+dir+"/exit 2>/dev/null"); err == nil && strings.TrimSpace(exit) != "" {
		fmt.Fprintf(errOut, "exit code %s\n", strings.TrimSpace(exit))
	}
	return nil
}

func (m *MicroVM) Wait(ctx context.Context, id, runID string) (int, error) {
	dir := runDir + "/" + runID
	out, err := m.shellOutput(ctx, id, "while [ ! -f "+dir+"/exit ]; do sleep 0.2; done; cat "+dir+"/exit")
	if err != nil {
		return 1, fmt.Errorf("等待运行结束失败: %w", err)
	}
	code, convErr := strconv.Atoi(strings.TrimSpace(out))
	if convErr != nil {
		return 1, fmt.Errorf("无法解析退出码 %q", strings.TrimSpace(out))
	}
	return code, nil
}

func (m *MicroVM) CopyFrom(ctx context.Context, id, sandboxPath, localPath string, errOut io.Writer) error {
	dir, name := splitPath(sandboxPath)
	if name == "" || name == "." || name == "/" {
		return fmt.Errorf("不能复制根目录 %s,请指定具体文件或目录", sandboxPath)
	}
	if code, err := m.Exec(ctx, id, "/", []string{"test", "-e", sandboxPath}, ExecOptions{}, nil, io.Discard, io.Discard); err != nil || code != 0 {
		return fmt.Errorf("沙箱 %s 里没有 %s", id, sandboxPath)
	}
	target := localPath
	if !strings.HasSuffix(target, "/") {
		target = filepath.Dir(target)
	}
	if err := os.MkdirAll(target, 0o755); err != nil {
		return fmt.Errorf("创建本地目录失败: %w", err)
	}

	unpack := localTar(errOut, "-x", "-f", "-", "-C", target)
	pr, pw := io.Pipe()
	unpack.Stdin = pr
	if err := unpack.Start(); err != nil {
		return fmt.Errorf("启动本机 tar: %w", err)
	}
	code, err := m.Exec(ctx, id, "/", []string{"tar", "-c", "-f", "-", "-C", dir, name}, ExecOptions{}, nil, pw, errOut)
	_ = pw.Close()
	unpackErr := unpack.Wait()
	if err != nil || code != 0 || unpackErr != nil {
		return fmt.Errorf("复制到本机失败(退出码 %d): %w", code, errors.Join(err, unpackErr))
	}
	finalName := strings.TrimSuffix(filepath.Base(localPath), "/")
	if finalName != name {
		if err := os.Rename(filepath.Join(target, name), filepath.Join(target, finalName)); err != nil {
			return fmt.Errorf("写入目标路径失败: %w", err)
		}
	}
	return nil
}

func (m *MicroVM) CopyInto(ctx context.Context, id, localPath, sandboxPath string, errOut io.Writer) error {
	if _, err := os.Stat(localPath); err != nil {
		return fmt.Errorf("本机没有 %s: %w", localPath, err)
	}
	srcDir, base := filepath.Dir(localPath), filepath.Base(localPath)
	dir, name := splitPath(sandboxPath)
	if name == "" || name == "." || name == "/" {
		return fmt.Errorf("目标必须是具体路径,不能是根目录 %s", sandboxPath)
	}
	pack := localTar(errOut, "-c", "-f", "-", "-C", srcDir, base)
	stdout, err := pack.StdoutPipe()
	if err != nil {
		return err
	}
	if err = pack.Start(); err != nil {
		return fmt.Errorf("启动本机 tar: %w", err)
	}
	code, err := m.Exec(ctx, id, "/", []string{"tar", "-x", "-f", "-", "-C", dir}, ExecOptions{}, stdout, io.Discard, errOut)
	packErr := pack.Wait()
	if err != nil || code != 0 || packErr != nil {
		return fmt.Errorf("复制到沙箱失败(退出码 %d): %w", code, errors.Join(err, packErr))
	}
	if name != base {
		if code, err := m.Exec(ctx, id, "/", []string{"mv", filepath.Join(dir, base), filepath.Join(dir, name)},
			ExecOptions{}, nil, io.Discard, errOut); err != nil || code != 0 {
			return fmt.Errorf("目标路径改名失败(退出码 %d): %w", code, err)
		}
	}
	return nil
}

func (m *MicroVM) shellOutput(ctx context.Context, id, script string) (string, error) {
	var out, errBuf bytes.Buffer
	code, err := m.Exec(ctx, id, "/", []string{"sh", "-c", script}, ExecOptions{}, nil, &out, &errBuf)
	if err != nil {
		return "", err
	}
	if code != 0 {
		return "", fmt.Errorf("退出码 %d: %s", code, strings.TrimSpace(errBuf.String()))
	}
	return out.String(), nil
}

func (m *MicroVM) saveState(st *vmState) error {
	b, err := json.MarshalIndent(st, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(m.dir(st.ID), "state.json"), b, 0o600)
}

func (m *MicroVM) loadState(id string) (vmState, error) {
	var st vmState
	if !validName(id) {
		return st, fmt.Errorf("沙箱 %s 不存在", id)
	}
	b, err := os.ReadFile(filepath.Join(m.dir(id), "state.json"))
	if err != nil {
		return st, fmt.Errorf("沙箱 %s 不存在", id)
	}
	if err := json.Unmarshal(b, &st); err != nil {
		return st, fmt.Errorf("沙箱 %s 的状态文件损坏: %w", id, err)
	}
	return st, nil
}

func validName(s string) bool {
	return s != "" && s != "." && s != ".." && !strings.ContainsAny(s, `/\`)
}

func (m *MicroVM) kernel() string { return cmp.Or(m.Kernel, filepath.Join(m.stateDir(), "vmlinux")) }
func (m *MicroVM) agentPath() string {
	return cmp.Or(m.GuestAgent, filepath.Join(m.stateDir(), "bowl-sandbox-guest"))
}
func (m *MicroVM) cloudHypervisor() string { return cmp.Or(m.CloudHypervisor, "cloud-hypervisor") }

func vmStatus(st *vmState) string {
	switch {
	case !processAlive(st.VMPid):
		return "stopped"
	case st.Paused:
		return "paused"
	default:
		return "running"
	}
}

func processAlive(pid int) bool {
	return pid > 0 && syscall.Kill(pid, 0) == nil
}

func startDetached(logPath, bin string, args ...string) (int, error) {
	logFile, err := os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return 0, err
	}
	defer logFile.Close()
	cmd := exec.Command(bin, args...)
	cmd.Stdout, cmd.Stderr = logFile, logFile
	cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
	if err := cmd.Start(); err != nil {
		return 0, err
	}
	pid := cmd.Process.Pid
	_ = cmd.Process.Release()
	return pid, nil
}

func stopProcess(pid int) {
	if !processAlive(pid) {
		return
	}
	_ = syscall.Kill(pid, syscall.SIGTERM)
	for range 20 {
		if !processAlive(pid) {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	_ = syscall.Kill(pid, syscall.SIGKILL)
}

func waitFor(ctx context.Context, timeout time.Duration, ok func() bool) error {
	deadline := time.Now().Add(timeout)
	for {
		if ok() {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("%s 内没有完成", timeout)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(5 * time.Millisecond):
		}
	}
}

func isRegularFile(p string) bool {
	info, err := os.Stat(p)
	return err == nil && info.Mode().IsRegular()
}

func installFile(src, dst string) error {
	data, err := os.ReadFile(src)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
		return err
	}
	return os.WriteFile(dst, data, 0o755)
}

func tailFile(p string, n int) string {
	b, err := os.ReadFile(p)
	if err != nil {
		return ""
	}
	lines := strings.Split(strings.TrimRight(string(b), "\n"), "\n")
	return strings.Join(lines[max(0, len(lines)-n):], "\n")
}

func (m *MicroVM) guestSync(ctx context.Context, id string) {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	_, _ = m.Exec(ctx, id, "/", []string{"sync"}, ExecOptions{}, nil, io.Discard, io.Discard)
}

func commandScript(cwd string, argv []string) string {
	quoted := make([]string, 0, len(argv))
	for _, a := range argv {
		quoted = append(quoted, shellQuote(a))
	}
	return "cd " + shellQuote(cwd) + " && exec " + strings.Join(quoted, " ")
}

func shellQuote(s string) string {
	return "'" + strings.ReplaceAll(s, "'", `'\''`) + "'"
}

func localTar(stderr io.Writer, args ...string) *exec.Cmd {
	cmd := exec.Command("tar", args...)
	cmd.Stderr = stderr
	return cmd
}

func splitPath(p string) (dir, name string) {
	trimmed := strings.TrimSuffix(p, "/")
	if trimmed == "" {
		return "/", ""
	}
	idx := strings.LastIndex(trimmed, "/")
	if idx < 0 {
		return ".", trimmed
	}
	if idx == 0 {
		return "/", trimmed[1:]
	}
	return trimmed[:idx], trimmed[idx+1:]
}
