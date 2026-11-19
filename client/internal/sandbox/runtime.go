// Package sandbox 用本机 Docker 跑一个以 Bowl 磁盘为数据盘的沙箱。
//
// 这一层只负责"运行时"：容器生命周期、容器内挂载/卸载磁盘、命令执行、日志、
// 文件进出。它不解析命令行、不打印表格、不认识 StorageService——磁盘的建立与
// 销毁属于控制面，由调用方（CLI 或未来的 cloud sandbox 服务）自己完成。
//
// 依赖方向：调用方 → sandbox → os/exec。反过来不允许。
package sandbox

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"time"
)

const (
	// ContainerPrefix 是沙箱容器名的固定前缀，沙箱 ID 直接拼在后面。
	ContainerPrefix = "bowl-sbx-"
	// DefaultImage 是未指定镜像时使用的沙箱镜像。
	DefaultImage = "ubuntu:24.04"
	// DefaultMountPath 是磁盘在容器内的默认挂载点。
	DefaultMountPath = "/mnt/data"
	// ClientPath 是注入容器的 Linux 客户端路径。
	ClientPath = "/usr/local/bin/bowl"

	labelOwner = "bowl.sandbox"
	labelDisk  = "bowl.disk"
	labelMount = "bowl.mount"
	labelImage = "bowl.image"
	// labelEndpoint 记录这个沙箱的盘挂在哪个存储节点上（调用方本机视角的地址）。
	labelEndpoint = "bowl.endpoint"

	runDir      = "/tmp/.bowl-exec"
	runIDLayout = "20060102-150405"
	pollEvery   = 200 * time.Millisecond
)

// Runtime 执行所有容器侧动作。零值可用，Docker 为空时取 PATH 里的 docker。
type Runtime struct {
	// Docker 是 docker 可执行文件路径。
	Docker string
	// Host 指定容器跑在哪个 Docker 上：留空是本机默认的 Docker；带协议的地址
	// （ssh://user@host、tcp://host:2376）按 docker -H 使用；其余当作 docker context 名。
	// 沙箱跑在另一台机器上时，本包的所有动作都经由这个 Docker 的 API 完成，不依赖共享文件系统。
	Host string
}

// Row 是 docker ps 里一个沙箱的展示信息。
type Row struct {
	ID, Status, Image, Mount, Created string
	// Endpoint 是沙箱的盘所在的存储节点；早期创建、没有记录节点的沙箱为空。
	Endpoint string
}

// Mount 是容器内一个已挂载的 Bowl 磁盘。
type Mount struct {
	DiskID   string
	Path     string
	ReadOnly bool
}

// Info 是单个沙箱的状态快照。
type Info struct {
	ID, Status, Image, Created, Container string
	// Endpoint 是沙箱的盘所在的存储节点；MountPath 是默认挂载点；Network 是加入的 docker 网络（默认网络为空）。
	Endpoint, MountPath, Network string
	// Address 是从宿主机访问沙箱里端口用的 IP;拿不到时为空。
	Address string
	// CPUs 和 MemoryMiB 是创建时定的规格;0 表示没有限制或不知道。
	CPUs, MemoryMiB int
	Mounts          []Mount
}

// CreateSpec 描述起一个沙箱容器所需的全部参数。
type CreateSpec struct {
	// ID 同时是沙箱 ID 与容器名的后缀。
	ID string
	// Image 是容器镜像。
	Image string
	// Name 是容器主机名，可为空。
	Name string
	// MountPath 是磁盘挂载点，为空取 DefaultMountPath。
	MountPath string
	// Network 是加入的 docker 网络，可为空。
	Network string
	// HostEndpoint 是调用方本机看到的 StorageService 地址，容器内会改写成可达地址。
	HostEndpoint string
	// Token 是访问令牌。
	Token string
	// ClientBinary 是注入容器的 Linux 客户端路径（调用方本机上的文件）。
	ClientBinary string
	// Arch 是容器的 CPU 架构（arm64 / amd64），必须与 ClientBinary 一致；为空取本机架构。
	Arch string
	// CPUs 和 MemoryMiB 是规格上限,0 表示用后端的默认值。
	CPUs, MemoryMiB int
	// SandboxEndpoint 是沙箱内访问 StorageService 用的地址。为空时由 HostEndpoint 推出
	// （回环地址改写成 host.docker.internal）。算力和存储在同一内网时填内网地址。
	SandboxEndpoint string
}

// ExecOptions 描述一次命令执行。
type ExecOptions struct {
	// Cwd 是容器内工作目录。
	Cwd string
	// TTY 表示分配伪终端（仅前台执行有效）。
	TTY bool
	// Env 是追加给命令的环境变量，形如 KEY=VALUE。
	Env []string
}

// ContainerName 由沙箱 ID 派生容器名；同 ID 必然同名，无需额外映射。
func ContainerName(id string) string {
	return ContainerPrefix + StripID(id)
}

// StripID 接受裸 ID 或容器名，统一成沙箱 ID。
func StripID(raw string) string {
	id := strings.TrimSpace(raw)
	if rest, ok := strings.CutPrefix(id, ContainerPrefix); ok {
		return rest
	}
	return id
}

// Create 起容器并在容器内挂载磁盘。失败时会尽力删除半启动的容器。
func (rt *Runtime) Create(ctx context.Context, in *CreateSpec) error {
	spec := *in // 下面会补默认值，不改调用方的那份
	if spec.ID == "" {
		return errors.New("沙箱 ID 不能为空")
	}
	if spec.Image == "" {
		spec.Image = DefaultImage
	}
	if spec.MountPath == "" {
		spec.MountPath = DefaultMountPath
	}
	if spec.ClientBinary == "" {
		return errors.New("缺少注入沙箱的 Linux 客户端路径")
	}
	if spec.Arch == "" {
		spec.Arch = runtime.GOARCH
	}
	if spec.SandboxEndpoint == "" {
		spec.SandboxEndpoint = containerEndpoint(spec.HostEndpoint)
	}
	container := ContainerName(spec.ID)
	env := []string{"BOWL_STORAGE_ENDPOINT=" + spec.SandboxEndpoint}
	if spec.Token != "" {
		env = append(env, "BOWL_TOKEN="+spec.Token)
	}
	args := []string{"create", "--name", container,
		// 容器架构必须与注入的 Linux 客户端一致，否则会在模拟层里跑。
		"--platform", "linux/" + spec.Arch,
		"--label", labelOwner + "=1",
		"--label", labelDisk + "=" + spec.ID,
		"--label", labelMount + "=" + spec.MountPath,
		"--label", labelImage + "=" + spec.Image,
		"--label", labelEndpoint + "=" + spec.HostEndpoint,
		// 只给挂载 FUSE 需要的能力，绝不用 --privileged。
		"--cap-drop", "ALL", "--cap-add", "SYS_ADMIN",
		"--device", "/dev/fuse",
		"--security-opt", "no-new-privileges",
	}
	for _, kv := range env {
		args = append(args, "-e", kv)
	}
	if spec.Network != "" {
		args = append(args, "--network", spec.Network)
	}
	if spec.Name != "" {
		args = append(args, "--hostname", spec.Name)
	}
	if spec.CPUs > 0 {
		args = append(args, "--cpus", strconv.Itoa(spec.CPUs))
	}
	if spec.MemoryMiB > 0 {
		args = append(args, "--memory", strconv.Itoa(spec.MemoryMiB)+"m")
	}
	args = append(args, spec.Image, "sleep", "infinity")

	// 标准输出是容器 ID，用不上；错误直接抛给调用方。
	if err := rt.run(ctx, io.Discard, io.Discard, args...); err != nil {
		return fmt.Errorf("创建沙箱容器失败: %w", err)
	}
	// 客户端用 docker cp 拷进容器，而不是从本机 bind 挂载：容器可能跑在另一台机器的 Docker 上，
	// 那边没有这个文件。
	// ponytail: 每次建沙箱都拷一次客户端（约 40 MB），远程 Docker 上会慢几秒；量大了应把客户端做进镜像。
	if err := rt.run(ctx, io.Discard, io.Discard, "cp", spec.ClientBinary, container+":"+ClientPath); err != nil {
		_ = rt.Remove(context.WithoutCancel(ctx), spec.ID)
		return fmt.Errorf("向沙箱注入客户端失败: %w", err)
	}
	if err := rt.run(ctx, io.Discard, io.Discard, "start", container); err != nil {
		_ = rt.Remove(context.WithoutCancel(ctx), spec.ID)
		return fmt.Errorf("启动沙箱容器失败: %w", err)
	}
	if err := rt.mountInside(ctx, container, spec.ID, spec.MountPath); err != nil {
		_ = rt.Remove(context.WithoutCancel(ctx), spec.ID)
		return fmt.Errorf("容器内挂载磁盘失败: %w", err)
	}
	return nil
}

// Remove 删除沙箱容器。删除前先尽力卸载容器内的磁盘：正常卸载会把缓冲的写入刷到存储节点
// 并立即释放挂载租约；直接杀容器的话，租约要等超时才过期，期间这块盘既删不掉也交接不了。
func (rt *Runtime) Remove(ctx context.Context, id string) error {
	if mounts, err := rt.Mounts(ctx, id); err == nil {
		for _, m := range mounts {
			_ = rt.Unmount(ctx, id, m.Path)
		}
	}
	if err := rt.run(ctx, io.Discard, io.Discard, "rm", "-f", ContainerName(id)); err != nil {
		return fmt.Errorf("删除沙箱容器失败: %w", err)
	}
	return nil
}

// List 列出本机所有沙箱。
func (rt *Runtime) List(ctx context.Context) ([]Row, error) {
	format := strings.Join([]string{
		`{{.Label "` + labelDisk + `"}}`,
		"{{.Status}}",
		`{{.Label "` + labelImage + `"}}`,
		`{{.Label "` + labelMount + `"}}`,
		"{{.CreatedAt}}",
		`{{.Label "` + labelEndpoint + `"}}`,
	}, `\t`)
	out, err := rt.output(ctx, "ps", "-a", "--filter", "label="+labelOwner+"=1", "--format", format)
	if err != nil {
		return nil, fmt.Errorf("列出沙箱失败: %w", err)
	}
	return parseRows(out), nil
}

// IDs 列出本机所有沙箱 ID（每行一个）。
func (rt *Runtime) IDs(ctx context.Context) ([]string, error) {
	out, err := rt.output(ctx, "ps", "-a", "--filter", "label="+labelOwner+"=1",
		"--format", `{{.Label "`+labelDisk+`"}}`)
	if err != nil {
		return nil, fmt.Errorf("列出沙箱失败: %w", err)
	}
	return parseIDs(out), nil
}

// Info 汇总沙箱状态与当前挂载。
func (rt *Runtime) Info(ctx context.Context, id string) (Info, error) {
	container := ContainerName(id)
	out, err := rt.output(ctx, "inspect", "-f",
		`{{.State.Status}}|{{.Config.Image}}|{{.Created}}|{{index .Config.Labels "`+labelEndpoint+
			`"}}|{{index .Config.Labels "`+labelMount+`"}}|{{.HostConfig.NetworkMode}}`,
		container)
	if err != nil {
		return Info{}, fmt.Errorf("沙箱 %s 不存在: %w", id, err)
	}
	parts := strings.Split(strings.TrimSpace(out), "|")
	info := Info{ID: id, Container: container}
	if len(parts) > 0 {
		info.Status = parts[0]
	}
	if len(parts) > 1 {
		info.Image = parts[1]
	}
	if len(parts) > 2 {
		info.Created = strings.TrimSuffix(parts[2], " +0000 UTC")
	}
	if len(parts) > 5 {
		info.Endpoint, info.MountPath = parts[3], parts[4]
		if net := parts[5]; net != "default" && net != "bridge" {
			info.Network = net
		}
	}
	if info.MountPath == "" {
		info.MountPath = DefaultMountPath
	}
	if mounts, mountErr := rt.Mounts(ctx, id); mountErr == nil {
		info.Mounts = mounts
	}
	return info, nil
}

// DefaultMount 读取沙箱的默认挂载点（创建时记录在容器标签上）。
func (rt *Runtime) DefaultMount(ctx context.Context, id string) (string, error) {
	out, err := rt.output(ctx, "inspect", "-f",
		`{{index .Config.Labels "`+labelMount+`"}}`, ContainerName(id))
	if err != nil {
		return "", fmt.Errorf("沙箱 %s 不存在或容器已被删除: %w", id, err)
	}
	path := strings.TrimSpace(out)
	if path == "" || path == "<no value>" {
		return DefaultMountPath, nil
	}
	return path, nil
}

// Exec 在沙箱内前台执行命令，返回命令的退出码。
func (rt *Runtime) Exec(ctx context.Context, id, cwd string, argv []string, opts ExecOptions, in io.Reader, out, errOut io.Writer) (int, error) {
	if len(argv) == 0 {
		return 2, errors.New("缺少要执行的命令")
	}
	if cwd == "" {
		cwd = opts.Cwd
	}
	// 参数原样透传为 argv，不拼 shell 字符串：引号、管道与退出码都由用户自己的 shell 决定。
	args := []string{"exec", "-i", "-w", cwd}
	if opts.TTY {
		args = append(args, "-t")
	}
	for _, kv := range opts.Env {
		args = append(args, "-e", kv)
	}
	args = append(args, ContainerName(id))
	args = append(args, argv...)
	return rt.runCode(ctx, in, out, errOut, args...), nil
}

// ExecBackground 后台执行命令，立即返回运行 ID。
func (rt *Runtime) ExecBackground(ctx context.Context, id, cwd string, argv []string) (string, error) {
	if len(argv) == 0 {
		return "", errors.New("缺少要执行的命令")
	}
	runID := time.Now().Format(runIDLayout)
	dir := runDir + "/" + runID
	line := "mkdir -p " + dir +
		" && ( " + commandScript(cwd, argv) + " ) > " + dir + "/out.log 2>&1" +
		"; echo $? > " + dir + "/exit"
	if err := rt.run(ctx, io.Discard, io.Discard, "exec", "-d", ContainerName(id), "sh", "-c", line); err != nil {
		return "", fmt.Errorf("启动后台命令失败: %w", err)
	}
	return runID, nil
}

// Logs 输出后台运行的日志；follow 时持续跟随。
func (rt *Runtime) Logs(ctx context.Context, id, runID string, follow bool, out, errOut io.Writer) error {
	container := ContainerName(id)
	if runID == "" {
		latest, err := rt.latestRun(ctx, container)
		if err != nil {
			return err
		}
		runID = latest
	}
	dir := runDir + "/" + runID
	if follow {
		rt.runCode(ctx, os.Stdin, out, errOut, "exec", "-i", container, "sh", "-c", "tail -f "+dir+"/out.log")
		return nil
	}
	if code := rt.runCode(ctx, nil, out, errOut, "exec", container, "cat", dir+"/out.log"); code != 0 {
		return fmt.Errorf("读取日志失败（运行 %s）", runID)
	}
	if exitOut, err := rt.output(ctx, "exec", container, "cat", dir+"/exit"); err == nil {
		fmt.Fprintf(errOut, "exit code %s\n", strings.TrimSpace(exitOut))
	}
	return nil
}

// Wait 阻塞到后台运行结束，返回其退出码。
func (rt *Runtime) Wait(ctx context.Context, id, runID string) (int, error) {
	dir := runDir + "/" + runID
	out, err := rt.output(ctx, "exec", ContainerName(id), "sh", "-c",
		"while [ ! -f "+dir+"/exit ]; do sleep 0.2; done; cat "+dir+"/exit")
	if err != nil {
		return 1, fmt.Errorf("等待运行结束失败: %w", err)
	}
	code, convErr := strconv.Atoi(strings.TrimSpace(out))
	if convErr != nil {
		return 1, fmt.Errorf("无法解析退出码 %q", strings.TrimSpace(out))
	}
	return code, nil
}

// Mount 把一块磁盘挂进运行中的沙箱。
func (rt *Runtime) Mount(ctx context.Context, id, diskID, path string, opts MountOptions) error {
	if diskID == "" {
		return errors.New("缺少要挂载的磁盘 ID")
	}
	if path == "" {
		path = DefaultMountPath
	}
	container := ContainerName(id)
	if err := rt.run(ctx, io.Discard, io.Discard, "exec", container, "mkdir", "-p", path); err != nil {
		return fmt.Errorf("创建挂载点失败: %w", err)
	}
	args := []string{"exec", container, ClientPath, "mount", diskID, path}
	if opts.ReadOnly {
		args = append(args, "--ro")
	}
	if opts.Subdir != "" {
		args = append(args, "--subdir", opts.Subdir)
	}
	if err := rt.run(ctx, io.Discard, io.Discard, args...); err != nil {
		return fmt.Errorf("挂载磁盘失败: %w", err)
	}
	// /proc/mounts 第 0 列是 fsname（挂载时写成 bowl:<disk-id>），锚定行首免得和别的挂载混淆。
	probe := "grep -q '^bowl:" + diskID + " " + path + " ' /proc/mounts"
	for range 20 {
		if err := rt.run(ctx, io.Discard, io.Discard, "exec", container, "sh", "-c", probe); err == nil {
			return nil
		}
		time.Sleep(pollEvery)
	}
	return fmt.Errorf("挂载点 %s 在 4s 内没有就绪", path)
}

// MountOptions 控制挂载方式。
type MountOptions struct {
	ReadOnly bool
	Subdir   string
}

// Unmount 卸载沙箱里的一个挂载点。
func (rt *Runtime) Unmount(ctx context.Context, id, path string) error {
	if path == "" {
		return errors.New("缺少挂载点")
	}
	if err := rt.run(ctx, io.Discard, io.Discard, "exec", ContainerName(id), ClientPath, "umount", path); err != nil {
		return fmt.Errorf("卸载失败: %w", err)
	}
	return nil
}

// Mounts 列出沙箱里当前挂着的磁盘。
func (rt *Runtime) Mounts(ctx context.Context, id string) ([]Mount, error) {
	out, err := rt.output(ctx, "exec", ContainerName(id), "cat", "/proc/mounts")
	if err != nil {
		return nil, fmt.Errorf("读取挂载表失败: %w", err)
	}
	return parseMounts(out), nil
}

// Writer 记录一个正在以读写方式挂载某块盘的位置。
type Writer struct {
	SandboxID string
	Path      string
}

// Writers 找出本机正在以读写方式挂载该盘的所有沙箱。
//
// 服务端目前没有"谁在挂"的查询接口（MountDisk 只有挂载/续期/卸载），只能挨个沙箱读挂载表。
// ponytail: O(沙箱数) 次 docker exec，沙箱多时偏慢；等控制面提供租约查询再换掉。
func (rt *Runtime) Writers(ctx context.Context, diskID string) ([]Writer, error) {
	ids, err := rt.IDs(ctx)
	if err != nil {
		return nil, err
	}
	writers := make([]Writer, 0, 1)
	for _, id := range ids {
		mounts, mountErr := rt.Mounts(ctx, id)
		if mountErr != nil {
			continue // 容器正在退出等情况，跳过即可
		}
		for _, m := range mounts {
			if m.DiskID == diskID && !m.ReadOnly {
				writers = append(writers, Writer{SandboxID: id, Path: m.Path})
			}
		}
	}
	return writers, nil
}

// CopyFrom 把沙箱里的文件或目录拷到本机精确路径。
//
// 不能用 docker cp：它读的是容器 rootfs，看不到容器内 FUSE 挂载的磁盘。
// 这里走 tar 流（docker exec tar ↔ 本机 tar），两边都只依赖 tar。
func (rt *Runtime) CopyFrom(ctx context.Context, id, sandboxPath, localPath string, errOut io.Writer) error {
	container := ContainerName(id)
	dir, name := splitPath(sandboxPath)
	if name == "" || name == "." || name == "/" {
		return fmt.Errorf("不能复制根目录 %s，请指定具体文件或目录", sandboxPath)
	}
	if err := rt.run(ctx, io.Discard, io.Discard, "exec", container, "test", "-e", sandboxPath); err != nil {
		return fmt.Errorf("沙箱 %s 里没有 %s", id, sandboxPath)
	}
	target := localPath
	if !strings.HasSuffix(target, "/") {
		if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
			return fmt.Errorf("创建本地目录失败: %w", err)
		}
		target = filepath.Dir(target)
	} else if err := os.MkdirAll(target, 0o755); err != nil {
		return fmt.Errorf("创建本地目录失败: %w", err)
	}

	unpack := localTar(false, errOut, "-x", "-f", "-", "-C", target)
	code := pipeCommands(errOut,
		rt.command(ctx, "exec", container, "tar", "-c", "-f", "-", "-C", dir, name),
		unpack,
	)
	if code != 0 {
		return errors.New("复制到本机失败")
	}
	finalName := strings.TrimSuffix(filepath.Base(localPath), "/")
	if finalName != name {
		if err := os.Rename(filepath.Join(target, name), filepath.Join(target, finalName)); err != nil {
			return fmt.Errorf("写入目标路径失败: %w", err)
		}
	}
	return nil
}

// CopyInto 把本机文件或目录拷到沙箱里的精确路径。
func (rt *Runtime) CopyInto(ctx context.Context, id, localPath, sandboxPath string, errOut io.Writer) error {
	container := ContainerName(id)
	if _, err := os.Stat(localPath); err != nil {
		return fmt.Errorf("本机没有 %s: %w", localPath, err)
	}
	srcDir, base := filepath.Dir(localPath), filepath.Base(localPath)
	dir, name := splitPath(sandboxPath)
	if name == "" || name == "." || name == "/" {
		return fmt.Errorf("目标必须是具体路径，不能是根目录 %s", sandboxPath)
	}
	code := pipeCommands(errOut,
		localTar(true, errOut, "-c", "-f", "-", "-C", srcDir, base),
		rt.command(ctx, "exec", "-i", container, "tar", "-x", "-f", "-", "-C", dir),
	)
	if code != 0 {
		return errors.New("复制到沙箱失败")
	}
	if name != base {
		if err := rt.run(ctx, io.Discard, io.Discard, "exec", container, "mv",
			filepath.Join(dir, base), filepath.Join(dir, name)); err != nil {
			return fmt.Errorf("目标路径改名失败: %w", err)
		}
	}
	return nil
}

// --- 容器内部 ---

// mountInside 在容器内建挂载点并调用 bowl mount，返回前确认挂载已生效。
func (rt *Runtime) mountInside(ctx context.Context, container, diskID, path string) error {
	if err := rt.run(ctx, io.Discard, io.Discard, "exec", container, "mkdir", "-p", path); err != nil {
		return err
	}
	if err := rt.run(ctx, io.Discard, io.Discard, "exec", container, ClientPath, "mount", diskID, path); err != nil {
		return err
	}
	probe := "grep -q ' " + path + " fuse' /proc/mounts"
	for range 20 {
		if err := rt.run(ctx, io.Discard, io.Discard, "exec", container, "sh", "-c", probe); err == nil {
			return nil
		}
		time.Sleep(pollEvery * 2)
	}
	return fmt.Errorf("挂载点 %s 在 10s 内没有就绪", path)
}

// latestRun 取该沙箱最近一次后台运行的 ID。
func (rt *Runtime) latestRun(ctx context.Context, container string) (string, error) {
	out, err := rt.output(ctx, "exec", container, "sh", "-c", "ls -1 "+runDir+" 2>/dev/null | sort | tail -1")
	if err != nil {
		return "", fmt.Errorf("读取运行记录失败: %w", err)
	}
	id := strings.TrimSpace(out)
	if id == "" {
		return "", errors.New("沙箱里还没有后台运行记录")
	}
	return id, nil
}

// --- docker 调用 ---

func (rt *Runtime) docker() string {
	if rt.Docker != "" {
		return rt.Docker
	}
	return "docker"
}

func (rt *Runtime) command(ctx context.Context, args ...string) *exec.Cmd {
	return exec.CommandContext(ctx, rt.docker(), append(hostArgs(rt.Host), args...)...) //nolint:gosec // 参数由调用方按 argv 构造
}

// hostArgs 把 Host 翻译成 docker 的全局参数。
func hostArgs(host string) []string {
	switch {
	case host == "":
		return nil
	case strings.Contains(host, "://"):
		return []string{"-H", host}
	default:
		return []string{"--context", host}
	}
}

// Arch 返回这个 Docker 所在机器的 CPU 架构（arm64 / amd64）。
func (rt *Runtime) Arch(ctx context.Context) (string, error) {
	out, err := rt.output(ctx, "version", "--format", "{{.Server.Arch}}")
	if err != nil {
		return "", fmt.Errorf("连不上 Docker%s: %w", describeHost(rt.Host), err)
	}
	return normalizeArch(out), nil
}

// Stop 卸载容器里的盘再停容器,容器保留。
func (rt *Runtime) Stop(ctx context.Context, id string) error {
	if mounts, err := rt.Mounts(ctx, id); err == nil {
		for _, m := range mounts {
			if err := rt.Unmount(ctx, id, m.Path); err != nil {
				return err
			}
		}
	}
	return rt.run(ctx, io.Discard, io.Discard, "stop", ContainerName(id))
}

// Start 启动停掉的容器并重新挂上它的盘。
func (rt *Runtime) Start(ctx context.Context, id string) error {
	container := ContainerName(id)
	mountAt, err := rt.DefaultMount(ctx, id)
	if err != nil {
		return err
	}
	if err := rt.run(ctx, io.Discard, io.Discard, "start", container); err != nil {
		return fmt.Errorf("启动沙箱容器失败: %w", err)
	}
	return rt.mountInside(ctx, container, StripID(id), mountAt)
}

// Sync 在容器里对盘的挂载点做 statfs,Bowl 客户端会等暂存的写入全部上传完。
func (rt *Runtime) Sync(ctx context.Context, id string) error {
	mountAt, err := rt.DefaultMount(ctx, id)
	if err != nil {
		return err
	}
	return rt.run(ctx, io.Discard, io.Discard, "exec", ContainerName(id), "stat", "-f", mountAt)
}

// Pause 冻结容器里的所有进程,内存保留。
func (rt *Runtime) Pause(ctx context.Context, id string) error {
	return rt.run(ctx, io.Discard, io.Discard, "pause", ContainerName(id))
}

// Resume 让 Pause 冻结的进程继续运行。
func (rt *Runtime) Resume(ctx context.Context, id string) error {
	return rt.run(ctx, io.Discard, io.Discard, "unpause", ContainerName(id))
}

// Exists 判断这个 Docker 上有没有该沙箱的容器。
func (rt *Runtime) Exists(ctx context.Context, id string) bool {
	return rt.run(ctx, io.Discard, io.Discard, "inspect", "-f", "{{.Id}}", ContainerName(id)) == nil
}

func describeHost(host string) string {
	if host == "" {
		return ""
	}
	return " " + host
}

func normalizeArch(raw string) string {
	switch arch := strings.TrimSpace(raw); arch {
	case "aarch64":
		return "arm64"
	case "x86_64":
		return "amd64"
	default:
		return arch
	}
}

// run 执行 docker 子命令，标准输出丢弃。
func (rt *Runtime) run(ctx context.Context, stdout, stderr io.Writer, args ...string) error {
	cmd := rt.command(ctx, args...)
	cmd.Stdout = stdout
	var errBuf strings.Builder
	cmd.Stderr = &errBuf
	if err := cmd.Run(); err != nil {
		if msg := strings.TrimSpace(errBuf.String()); msg != "" {
			return errors.New(msg)
		}
		return err
	}
	_ = stderr
	return nil
}

// output 执行 docker 并捕获标准输出。
func (rt *Runtime) output(ctx context.Context, args ...string) (string, error) {
	var stdout, stderr strings.Builder
	cmd := rt.command(ctx, args...)
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		if msg := strings.TrimSpace(stderr.String()); msg != "" {
			return "", errors.New(msg)
		}
		return "", err
	}
	return stdout.String(), nil
}

// runCode 执行 docker 并把子进程退出码透传出去。
func (rt *Runtime) runCode(ctx context.Context, in io.Reader, stdout, stderr io.Writer, args ...string) int {
	cmd := rt.command(ctx, args...)
	cmd.Stdin = in
	cmd.Stdout = stdout
	cmd.Stderr = stderr
	if err := cmd.Run(); err != nil {
		if exitErr, ok := errors.AsType[*exec.ExitError](err); ok {
			return exitErr.ExitCode()
		}
		fmt.Fprintf(stderr, "错误: 执行命令失败: %v\n", err)
		return 1
	}
	return 0
}

// --- 解析 ---

// parseRows 解析 docker ps --format 的制表符分隔输出，跳过标签缺失的行。
func parseRows(out string) []Row {
	rows := make([]Row, 0, 4)
	for line := range strings.SplitSeq(strings.TrimSpace(out), "\n") {
		if strings.TrimSpace(line) == "" {
			continue
		}
		parts := strings.Split(line, "\t")
		if len(parts) < 5 || strings.TrimSpace(parts[0]) == "" {
			continue
		}
		row := Row{
			ID:      parts[0],
			Status:  parts[1],
			Image:   parts[2],
			Mount:   parts[3],
			Created: strings.TrimSuffix(parts[4], " +0000 UTC"),
		}
		if len(parts) > 5 {
			row.Endpoint = strings.TrimSpace(parts[5])
		}
		rows = append(rows, row)
	}
	return rows
}

// parseIDs 解析每行一个 ID 的输出。
func parseIDs(out string) []string {
	ids := make([]string, 0, 4)
	for line := range strings.SplitSeq(strings.TrimSpace(out), "\n") {
		if id := strings.TrimSpace(line); id != "" {
			ids = append(ids, id)
		}
	}
	return ids
}

// parseMounts 解析容器 /proc/mounts 里的 bowl 挂载。
func parseMounts(table string) []Mount {
	mounts := make([]Mount, 0, 2)
	for line := range strings.SplitSeq(table, "\n") {
		fields := strings.Fields(line)
		if len(fields) < 4 || !strings.HasPrefix(fields[0], "bowl:") {
			continue
		}
		readOnly := false
		for opt := range strings.SplitSeq(fields[3], ",") {
			if opt == "ro" {
				readOnly = true
			}
		}
		mounts = append(mounts, Mount{
			DiskID:   strings.TrimPrefix(fields[0], "bowl:"),
			Path:     fields[1],
			ReadOnly: readOnly,
		})
	}
	return mounts
}

// --- 组装 ---

// commandScript 用单引号逐参数引用，避免把用户的 argv 拼成一段会被二次解释的字符串。
// 命令放在子 shell 里执行：脚本末尾的 exec 会顶替那个子 shell，而不是顶替外层。
func commandScript(cwd string, argv []string) string {
	quoted := make([]string, 0, len(argv))
	for _, a := range argv {
		quoted = append(quoted, shellQuote(a))
	}
	return "cd " + shellQuote(cwd) + " && exec " + strings.Join(quoted, " ")
}

// shellQuote 用单引号包裹，内部单引号按 POSIX 方式转义。
func shellQuote(s string) string {
	return "'" + strings.ReplaceAll(s, "'", `'\''`) + "'"
}

// localTar 构造本机 tar 命令。
// macOS 的 bsdtar 会写入扩展属性和 AppleDouble 附带文件，进容器后 chown 又会被 FUSE 拒绝，
// 所以创建归档时统一把属主归零并关掉平台元数据。
func localTar(creating bool, stderr io.Writer, args ...string) *exec.Cmd {
	cmd := exec.Command("tar", args...)
	if runtime.GOOS == "darwin" {
		flags := []string{"--no-mac-metadata", "--no-xattrs"}
		if creating {
			flags = append(flags, "--uid", "0", "--gid", "0")
		}
		cmd = exec.Command("tar", append(flags, args...)...)
	}
	cmd.Env = append(os.Environ(), "LC_ALL=en_US.UTF-8", "COPYFILE_DISABLE=1")
	cmd.Stderr = stderr
	return cmd
}

// splitPath 拆出目录与末级名字，兼容以 / 结尾的目录写法。
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

// pipeCommands 把 producer 的标准输出接进 consumer 的标准输入，返回失败码。
func pipeCommands(stderr io.Writer, producer, consumer *exec.Cmd) int {
	pr, pw, err := os.Pipe()
	if err != nil {
		fmt.Fprintf(stderr, "错误: 创建管道失败: %v\n", err)
		return 1
	}
	producer.Stdout = pw
	producer.Stderr = stderr
	consumer.Stdin = pr
	if err := producer.Start(); err != nil {
		_ = pw.Close()
		_ = pr.Close()
		fmt.Fprintf(stderr, "错误: 启动复制失败: %v\n", err)
		return 1
	}
	if err := consumer.Start(); err != nil {
		_ = producer.Process.Kill()
		_ = pw.Close()
		_ = pr.Close()
		fmt.Fprintf(stderr, "错误: 启动复制失败: %v\n", err)
		return 1
	}
	_ = pw.Close()
	_ = pr.Close()
	consumerErr := consumer.Wait()
	producerErr := producer.Wait()
	if producerErr != nil {
		fmt.Fprintf(stderr, "错误: 读取源失败: %v\n", producerErr)
		return 1
	}
	if consumerErr != nil {
		if exitErr, ok := errors.AsType[*exec.ExitError](consumerErr); ok {
			return exitErr.ExitCode()
		}
		fmt.Fprintf(stderr, "错误: 写入目标失败: %v\n", consumerErr)
		return 1
	}
	return 0
}

// containerEndpoint 把只在本机可见的回环地址改写为容器可达的 host.docker.internal。
func containerEndpoint(raw string) string {
	u, err := url.Parse(raw)
	if err != nil || u.Host == "" {
		return raw
	}
	switch u.Hostname() {
	case "127.0.0.1", "localhost", "::1", "0.0.0.0":
		host := "host.docker.internal"
		if port := u.Port(); port != "" {
			host += ":" + port
		}
		u.Host = host
		return u.String()
	default:
		return raw
	}
}
