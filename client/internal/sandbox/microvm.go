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

// MicroVM 用 Cloud Hypervisor 跑沙箱,系统盘就是这个沙箱的 Bowl 盘。
//
// 宿主机上把盘挂到 <StateDir>/<id>/root,virtiofsd 把这个目录当根传进虚机,虚机的 init 是
// 执行代理(bowl-sandbox-guest),宿主机经 vsock 和它通信。除了生命周期,其余动作
// (后台执行、日志、文件进出)都建在 Exec 上。
//
// 只能在 Linux 上以 root 运行:要用 /dev/kvm、挂 FUSE、以 --sandbox=none 跑 virtiofsd。
// 盘的建立和删除仍归调用方,这里只挂载和卸载。
type MicroVM struct {
	// StateDir 存放每个沙箱的运行状态,默认 /var/lib/bowl-sandbox。
	StateDir string
	// Kernel 是虚机内核(vmlinux),默认 <StateDir>/vmlinux。
	Kernel string
	// GuestAgent 是执行代理的 Linux 可执行文件,默认 <StateDir>/bowl-sandbox-guest。
	GuestAgent string
	// CloudHypervisor 和 Virtiofsd 是两个可执行文件,默认从 PATH 找,virtiofsd 再找 /usr/libexec。
	CloudHypervisor, Virtiofsd string
	// CPUs 和 MemoryMiB 是 CreateSpec 没指定时每台虚机的规格,默认 1 核 1024 MiB。
	CPUs, MemoryMiB int
	// DNS 是写进虚机 /etc/resolv.conf 的域名服务器,默认 1.1.1.1。
	DNS string
}

const (
	// guestAgentPath 是代理在虚机里的位置,也是内核的 init。
	guestAgentPath = "/.bowl/guest"
	// microVMWorkdir 是不指定工作目录时命令的默认目录。
	microVMWorkdir = "/root"
	readyTimeout   = 20 * time.Second
)

var errUnexpectedReply = errors.New("应答不是 OK")

// vmState 是 <StateDir>/<id>/state.json 的内容。
type vmState struct {
	ID           string `json:"id"`
	Template     string `json:"template"`
	Endpoint     string `json:"endpoint"`
	ClientBinary string `json:"client_binary"`
	Created      string `json:"created"`
	VMPid        int    `json:"vm_pid"`
	FSPid        int    `json:"fs_pid"`
	CPUs         int    `json:"cpus"`
	MemoryMiB    int    `json:"memory_mib"`
	NetIndex     int    `json:"net_index"`
	Paused       bool   `json:"paused"`
	// Block 为真时系统盘是盘里的 BlockImage 文件,以块设备交给虚机;否则整块盘经 virtio-fs 当根。
	Block bool `json:"block"`
	// Local 为真时系统盘是状态目录里的一个本机镜像文件,不经过 Bowl。
	Local bool `json:"local,omitempty"`
}

// BlockImage 是块设备模板盘里那个 ext4 镜像的文件名,放在盘的根上。
//
// ponytail: 这是 Bowl 原生块设备的替身。镜像文件放在 Bowl 盘上,fork 盘就是写时复制地 fork 镜像,
// 虚机看到普通磁盘。Bowl 在存储引擎上直接提供块设备之后,只换 attachSystemDisk 和 blockDevicePath。
const BlockImage = "root.img"

func (m *MicroVM) stateDir() string {
	if m.StateDir != "" {
		return m.StateDir
	}
	return "/var/lib/bowl-sandbox"
}

func (m *MicroVM) dir(id string) string { return filepath.Join(m.stateDir(), StripID(id)) }

// Create 挂盘、放代理、起 virtiofsd 和虚机,等代理能执行命令后返回。失败时尽力清掉已起的部分,盘不删。
// spec.Image 只作为模板名记录,系统内容来自盘本身(调用方从模板盘 fork 出来的)。
func (m *MicroVM) Create(ctx context.Context, spec *CreateSpec) error {
	if spec.ID == "" {
		return errors.New("沙箱 ID 不能为空")
	}
	if spec.ClientBinary == "" && spec.LocalImage == "" {
		return errors.New("缺少宿主机上的 bowl 客户端路径")
	}
	id := StripID(spec.ID)
	dir := m.dir(id)
	if _, err := os.Stat(dir); err == nil {
		return fmt.Errorf("沙箱 %s 已存在", id)
	}
	if err := os.MkdirAll(filepath.Join(dir, "root"), 0o700); err != nil {
		return fmt.Errorf("创建状态目录: %w", err)
	}
	st := vmState{
		ID: id, Template: spec.Image, Endpoint: spec.HostEndpoint, ClientBinary: spec.ClientBinary,
		Created: time.Now().UTC().Format(time.RFC3339),
		CPUs:    cmp.Or(spec.CPUs, m.CPUs, 1), MemoryMiB: cmp.Or(spec.MemoryMiB, m.MemoryMiB, 1024),
		Local: spec.LocalImage != "",
	}
	boot := func() error { return m.start(ctx, &st, spec.Token) }
	if st.Local {
		tplDir, src := filepath.Dir(spec.LocalImage), spec.LocalImage
		if snap := m.snapshotFor(tplDir, st.CPUs, st.MemoryMiB); snap != "" {
			src = filepath.Join(tplDir, snapshotDisk)
			boot = func() error { return m.restore(ctx, &st, snap) }
		}
		if err := copyImage(ctx, src, blockDevicePath(filepath.Join(dir, "root"))); err != nil {
			_ = os.RemoveAll(dir)
			return err
		}
	}
	if err := boot(); err != nil {
		_ = m.Remove(context.WithoutCancel(ctx), id)
		return err
	}
	return nil
}

func (m *MicroVM) start(ctx context.Context, st *vmState, token string) error {
	dir := m.dir(st.ID)
	root := filepath.Join(dir, "root")
	if err := m.saveState(st); err != nil {
		return err
	}
	if err := m.attachSystemDisk(ctx, st, token, root); err != nil {
		return err
	}
	st.Block = isRegularFile(blockDevicePath(root))
	var rootArgs []string
	// 块设备模板在做模板时就把代理和 resolv.conf 放进了镜像,这里只处理 virtio-fs 的根。
	dnsRoot := ""
	if !st.Block {
		if err := m.InstallAgent(root); err != nil {
			return fmt.Errorf("放入执行代理: %w", err)
		}
		dnsRoot = root
	}
	netArgs, ipArg, err := m.setupNetwork(st, dnsRoot)
	if err != nil {
		return err
	}

	if st.Block {
		rootArgs = []string{
			"--cmdline", "console=ttyS0 root=/dev/vda rw init=" + guestAgentPath + " quiet " + ipArg,
			"--disk", "path=" + blockDevicePath(root) + ",image_type=raw",
		}
	} else {
		fsSock := filepath.Join(dir, "fs.sock")
		fs, fsErr := startDetached(filepath.Join(dir, "virtiofsd.log"), m.virtiofsd(),
			"--socket-path="+fsSock, "--shared-dir="+root, "--cache=auto", "--sandbox=none", "--xattr")
		if fsErr != nil {
			return fmt.Errorf("启动 virtiofsd: %w", fsErr)
		}
		st.FSPid = fs
		if err = waitFor(ctx, 5*time.Second, func() bool { return isSocket(fsSock) }); err != nil {
			return fmt.Errorf("virtiofsd 没有就绪: %w", err)
		}
		rootArgs = []string{
			"--cmdline", "console=ttyS0 root=rootfs rootfstype=virtiofs rw init=" + guestAgentPath + " quiet " + ipArg,
			"--fs", "tag=rootfs,socket=" + fsSock,
		}
	}

	// virtiofsd 要和虚机共享内存;块设备根不需要,而且私有内存的快照才能写时复制地恢复。
	memShared := ",shared=on"
	if st.Block {
		memShared = ""
	}
	vm, err := startDetached(filepath.Join(dir, "vm.log"), m.cloudHypervisor(), append(append([]string{
		"--kernel", m.kernel()}, rootArgs...),
		"--memory", "size="+strconv.Itoa(st.MemoryMiB)+"M"+memShared,
		"--cpus", "boot="+strconv.Itoa(st.CPUs),
		"--vsock", "cid=3,socket="+filepath.Join(dir, "vsock.sock"),
		"--api-socket", filepath.Join(dir, "api.sock"),
		"--net", netArgs,
		"--serial", "file="+filepath.Join(dir, "console.log"),
		"--console", "off",
	)...)
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

// attachSystemDisk 把沙箱的盘接成虚机的系统盘:现在是宿主机挂载、再由 virtiofsd 传进去。
// M8 换成块设备时只改这里和 detachSystemDisk。
func (m *MicroVM) attachSystemDisk(ctx context.Context, st *vmState, token, root string) error {
	if st.Local {
		return nil
	}
	// --allow-other:virtiofsd 以虚机里进程的身份访问挂载点,不加的话虚机里非 root 用户全被 FUSE 拒掉。
	cmd := exec.CommandContext(ctx, st.ClientBinary, "mount", st.ID, root, "--allow-other", //nolint:gosec // 路径来自调用方配置
		"--endpoint", st.Endpoint, "--log-file", filepath.Join(m.dir(st.ID), "mount.log"))
	if token != "" {
		cmd.Env = append(os.Environ(), "BOWL_TOKEN="+token)
	}
	if out, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("挂载系统盘: %w: %s", err, strings.TrimSpace(string(out)))
	}
	return nil
}

// blockDevicePath 是块设备根在宿主机上的位置。
func blockDevicePath(root string) string { return filepath.Join(root, BlockImage) }

func (m *MicroVM) detachSystemDisk(ctx context.Context, st *vmState, root string) error {
	if !isMountpoint(root) {
		return nil
	}
	out, err := exec.CommandContext(ctx, st.ClientBinary, "umount", root).CombinedOutput() //nolint:gosec // 同上
	if err != nil {
		return fmt.Errorf("卸载系统盘: %w: %s", err, strings.TrimSpace(string(out)))
	}
	return nil
}

// Remove 停虚机和 virtiofsd、卸载系统盘(写入刷回存储节点)、删状态目录。盘本身不删。
func (m *MicroVM) Remove(ctx context.Context, id string) error {
	id = StripID(id)
	if err := m.Stop(ctx, id); err != nil {
		return err
	}
	root := filepath.Join(m.dir(id), "root")
	// 盘还挂着时删目录会删掉盘里的内容,所以再确认一次。
	if isMountpoint(root) {
		return fmt.Errorf("系统盘仍挂在 %s,没有删除状态目录", root)
	}
	return os.RemoveAll(m.dir(id))
}

// Stop 停虚机和 virtiofsd、卸载系统盘,状态目录留着,之后可以 Start。
func (m *MicroVM) Stop(ctx context.Context, id string) error {
	id = StripID(id)
	st, err := m.loadState(id)
	if err != nil {
		return err
	}
	if st.Block && !st.Paused && processAlive(st.VMPid) {
		// 块设备根的虚机自己有页缓存,停机前先让它写回,否则最后的写入会丢。
		m.guestSync(ctx, id)
	}
	stopProcess(st.VMPid)
	stopProcess(st.FSPid)
	if err := m.detachSystemDisk(ctx, &st, filepath.Join(m.dir(id), "root")); err != nil {
		return err
	}
	st.VMPid, st.FSPid, st.Paused = 0, 0, false
	return m.saveState(&st)
}

// Start 用原来的盘、规格和网段重新启动一台停掉的虚机,冷启动。令牌从 $BOWL_TOKEN 取。
func (m *MicroVM) Start(ctx context.Context, id string) error {
	id = StripID(id)
	st, err := m.loadState(id)
	if err != nil {
		return err
	}
	if processAlive(st.VMPid) {
		return fmt.Errorf("沙箱 %s 已经在运行", id)
	}
	for _, sock := range []string{"fs.sock", "vsock.sock", "api.sock"} {
		_ = os.Remove(filepath.Join(m.dir(id), sock))
	}
	if err := m.start(ctx, &st, ""); err != nil {
		_ = m.Stop(context.WithoutCancel(ctx), id)
		return err
	}
	return nil
}

// Sync 对系统盘的挂载点做一次 statfs:Bowl 客户端在 statfs 时会等暂存的写入全部上传完。
// 虚机里 virtio-fs 不做写回缓存,写下去的数据已经到了宿主机的挂载点。块设备根的虚机有自己的
// 页缓存,由 Pause 先在虚机里 sync,所以 fork 前的顺序 Pause、Sync 对两种根都成立。
func (m *MicroVM) Sync(_ context.Context, id string) error {
	var st syscall.Statfs_t
	if err := syscall.Statfs(filepath.Join(m.dir(StripID(id)), "root"), &st); err != nil {
		return fmt.Errorf("刷写系统盘: %w", err)
	}
	return nil
}

// Exists 判断这个沙箱的状态目录在不在。
func (m *MicroVM) Exists(_ context.Context, id string) bool {
	_, err := m.loadState(StripID(id))
	return err == nil
}

// List 从状态目录还原沙箱列表。
func (m *MicroVM) List(ctx context.Context) ([]Row, error) {
	ids, err := m.IDs(ctx)
	if err != nil {
		return nil, err
	}
	rows := make([]Row, 0, len(ids))
	for _, id := range ids {
		st, err := m.loadState(id)
		if err != nil {
			continue
		}
		rows = append(rows, Row{ID: id, Status: vmStatus(&st), Image: st.Template, Mount: "/", Created: st.Created, Endpoint: st.Endpoint})
	}
	return rows, nil
}

// IDs 列出状态目录里的所有沙箱 ID。
func (m *MicroVM) IDs(_ context.Context) ([]string, error) {
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

// Info 汇总沙箱状态。系统盘就是唯一的挂载,挂在根上。
func (m *MicroVM) Info(_ context.Context, id string) (Info, error) {
	id = StripID(id)
	st, err := m.loadState(id)
	if err != nil {
		return Info{}, err
	}
	return Info{
		ID: id, Status: vmStatus(&st), Image: st.Template, Created: st.Created,
		Container: "pid " + strconv.Itoa(st.VMPid), Endpoint: st.Endpoint, MountPath: "/",
		Address: guestIP(st.NetIndex), CPUs: st.CPUs, MemoryMiB: st.MemoryMiB, Block: st.Block, Local: st.Local,
		Mounts: []Mount{{DiskID: id, Path: "/"}},
	}, nil
}

// DefaultMount 返回命令的默认工作目录。系统盘就是根,所以没有单独的数据盘挂载点。
func (m *MicroVM) DefaultMount(context.Context, string) (string, error) { return microVMWorkdir, nil }

// Exec 经 vsock 让虚机里的代理执行命令,返回命令的退出码。
// opts.TTY 时虚机里分配伪终端;in 是本机终端时把它切到原始模式,结束后恢复。
//
// ponytail: 只在开始时传一次窗口大小,中途改窗口大小虚机里看不到。
func (m *MicroVM) Exec(ctx context.Context, id, cwd string, argv []string, opts ExecOptions, in io.Reader, out, errOut io.Writer) (int, error) {
	if len(argv) == 0 {
		return 2, errors.New("缺少要执行的命令")
	}
	cwd = cmp.Or(cwd, opts.Cwd, microVMWorkdir)
	reqBody := execRequest{Argv: argv, Cwd: cwd, Env: opts.Env, TTY: opts.TTY}
	if f, ok := in.(*os.File); ok && opts.TTY && term.IsTerminal(int(f.Fd())) { //nolint:gosec // fd 不会溢出 int
		fd := int(f.Fd()) //nolint:gosec // 同上
		if cols, rows, sizeErr := term.GetSize(fd); sizeErr == nil {
			reqBody.Rows, reqBody.Cols = uint16(rows), uint16(cols) //nolint:gosec // 终端尺寸在 uint16 范围内
		}
		if old, rawErr := term.MakeRaw(fd); rawErr == nil {
			defer func() { _ = term.Restore(fd, old) }()
		}
	}
	return m.run(ctx, id, &reqBody, in, out, errOut)
}

// run 把一个请求发给虚机里的代理,转发标准流,返回退出码。
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

// dialGuest 连上 Cloud Hypervisor 在宿主机上暴露的 vsock 套接字,并转接到代理的端口。
// 返回的 reader 已经读过握手应答,后续读取必须用它,不能直接读 conn。
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

// ExecBackground 在虚机里起一个脱离的后台命令,输出和退出码写进 runDir,立即返回运行 ID。
func (m *MicroVM) ExecBackground(ctx context.Context, id, cwd string, argv []string) (string, error) {
	if len(argv) == 0 {
		return "", errors.New("缺少要执行的命令")
	}
	runID := time.Now().Format(runIDLayout)
	dir := runDir + "/" + runID
	job := "( " + commandScript(cmp.Or(cwd, microVMWorkdir), argv) + " ) > " + dir + "/out.log 2>&1; echo $? > " + dir + "/exit"
	// 后台进程的三个标准流都要重定向走,否则代理会一直等着管道关闭。
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

// Logs 输出后台运行的日志;follow 时持续跟随。
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

// Wait 阻塞到后台运行结束,返回其退出码。
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

// CopyFrom 把虚机里的文件或目录拷到本机精确路径,走 tar 流。
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

	unpack := localTar(false, errOut, "-x", "-f", "-", "-C", target)
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

// CopyInto 把本机文件或目录拷到虚机里的精确路径,走 tar 流。
// 不直接写宿主机上的挂载目录:虚机那边有目录项缓存,刚写进去的文件可能要过一会儿才看得到。
func (m *MicroVM) CopyInto(ctx context.Context, id, localPath, sandboxPath string, errOut io.Writer) error {
	if _, err := os.Stat(localPath); err != nil {
		return fmt.Errorf("本机没有 %s: %w", localPath, err)
	}
	srcDir, base := filepath.Dir(localPath), filepath.Base(localPath)
	dir, name := splitPath(sandboxPath)
	if name == "" || name == "." || name == "/" {
		return fmt.Errorf("目标必须是具体路径,不能是根目录 %s", sandboxPath)
	}
	pack := localTar(true, errOut, "-c", "-f", "-", "-C", srcDir, base)
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

// --- 内部 ---

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
	b, err := os.ReadFile(filepath.Join(m.dir(id), "state.json"))
	if err != nil {
		return st, fmt.Errorf("沙箱 %s 不存在", id)
	}
	if err := json.Unmarshal(b, &st); err != nil {
		return st, fmt.Errorf("沙箱 %s 的状态文件损坏: %w", id, err)
	}
	return st, nil
}

func (m *MicroVM) kernel() string { return cmp.Or(m.Kernel, filepath.Join(m.stateDir(), "vmlinux")) }
func (m *MicroVM) agentPath() string {
	return cmp.Or(m.GuestAgent, filepath.Join(m.stateDir(), "bowl-sandbox-guest"))
}
func (m *MicroVM) cloudHypervisor() string { return cmp.Or(m.CloudHypervisor, "cloud-hypervisor") }

func (m *MicroVM) virtiofsd() string {
	if m.Virtiofsd != "" {
		return m.Virtiofsd
	}
	if p, err := exec.LookPath("virtiofsd"); err == nil {
		return p
	}
	return "/usr/libexec/virtiofsd"
}

// vmStatus 按虚机进程是否还活着判断,不信状态文件:宿主机重启后文件还在,进程已经没了。
//
// ponytail: 只看 pid 是否存在,pid 被别的进程复用时会误报 running;要准就比对 /proc/<pid>/cmdline。
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

// ponytail: startDetached 不回收子进程,同一个进程里起过的虚机退出后是僵尸,Kill(pid, 0) 仍成功,
// 会被当成 running。CLI 每次都退出所以没事;长期运行的控制面要改成持有 *os.Process 并 Wait。
func processAlive(pid int) bool {
	return pid > 0 && syscall.Kill(pid, 0) == nil
}

// startDetached 起一个和调用方脱离的后台进程:新会话、标准流写进日志文件,调用方退出后它还在。
func startDetached(logPath, bin string, args ...string) (int, error) {
	logFile, err := os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return 0, err
	}
	defer logFile.Close()
	cmd := exec.Command(bin, args...) //nolint:gosec // 可执行文件和参数由本包构造
	cmd.Stdout, cmd.Stderr = logFile, logFile
	cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
	if err := cmd.Start(); err != nil {
		return 0, err
	}
	pid := cmd.Process.Pid
	_ = cmd.Process.Release()
	return pid, nil
}

// stopProcess 先 SIGTERM,2 秒内没退出再 SIGKILL。
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

func isSocket(p string) bool {
	fi, err := os.Stat(p)
	return err == nil && fi.Mode()&os.ModeSocket != 0
}

// isMountpoint 判断 p 是不是挂载点:它和父目录不在同一个设备上。
// 这个判断决定删不删状态目录,判错会把盘里的内容一起删掉,所以出错时按"是挂载点"处理。
func isMountpoint(p string) bool {
	fi, err := os.Stat(p)
	if errors.Is(err, os.ErrNotExist) {
		return false
	}
	parent, perr := os.Stat(filepath.Dir(p))
	if err != nil || perr != nil {
		return true
	}
	a, ok1 := fi.Sys().(*syscall.Stat_t)
	b, ok2 := parent.Sys().(*syscall.Stat_t)
	return !ok1 || !ok2 || a.Dev != b.Dev
}

func installFile(src, dst string) error {
	data, err := os.ReadFile(src)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
		return err
	}
	return os.WriteFile(dst, data, 0o755) //nolint:gosec // 代理要可执行
}

func tailFile(p string, n int) string {
	b, err := os.ReadFile(p)
	if err != nil {
		return ""
	}
	lines := strings.Split(strings.TrimRight(string(b), "\n"), "\n")
	return strings.Join(lines[max(0, len(lines)-n):], "\n")
}

// guestSync 在虚机里执行 sync,把虚机页缓存里的写入推到块设备上。失败不致命,调用方接着做。
func (m *MicroVM) guestSync(ctx context.Context, id string) {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	_, _ = m.Exec(ctx, id, "/", []string{"sync"}, ExecOptions{}, nil, io.Discard, io.Discard)
}
