package cli

import (
	"bytes"
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"slices"
	"strings"
	"time"

	"connectrpc.com/connect"
	"golang.org/x/term"

	"github.com/starsrc/bowl/client/internal/rpc"
	"github.com/starsrc/bowl/client/internal/sandbox"
	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
	"github.com/starsrc/bowl/gen/go/storage/v1/storagev1connect"
)

// 这一层只负责命令行：解析参数、调用 StorageService 建盘/删盘、调用 sandbox 运行时、输出。
// 容器与挂载的全部机制在 internal/sandbox 里，那里不依赖 CLI，也不依赖 FS 层。
var sboxRuntime = &sandbox.Runtime{}

const sandboxUsageHelp = `bowl sandbox - 在本机容器里跑一个以 Bowl 磁盘为数据盘的沙箱

用法:
  bowl sandbox <command> [flags] [arguments]

可用命令:
  create      新建沙箱（可选从既有磁盘分叉），并把磁盘挂进容器
  list        列出本机沙箱
  exec        在沙箱内执行命令（-b 后台执行，默认工作目录为磁盘挂载点）
  logs        查看 / 跟随后台执行的输出
  wait        等待后台执行结束，以其退出码退出
  shell       进入沙箱交互式 shell
  cp          在本机与沙箱之间复制文件或目录（<sandbox-id>:<path>）
  mount       把磁盘挂进正在运行的沙箱（支持只读、子目录）
  unmount     卸载沙箱里的挂载点
  mounts      列出沙箱当前挂载的磁盘
  info        查看沙箱状态与挂载
  fork        从一个沙箱分叉出多个新沙箱（ID 一行一个）
  move        把沙箱连同它的盘切换到另一个存储节点（本地 ⇄ 云端），详见 move --help
  kill        销毁沙箱（--all 批量，--keep-disk 保留磁盘）
  help        查看帮助

create 标志:
  --from string        从既有磁盘分叉出沙箱磁盘（原磁盘不受影响）
  --size string        新建磁盘容量，仅在未指定 --from 时生效 (默认 1GiB)
  --image string       容器镜像 (默认 ` + sandbox.DefaultImage + `)
  --name string        磁盘名称与容器主机名
  --mount string       容器内磁盘挂载点 (默认 ` + sandbox.DefaultMountPath + `)
  --network string     把沙箱接入指定 docker 网络（访问同一网络里的其他服务用）
  --binary string      注入沙箱的 Linux bowl 客户端路径（默认自动编译并缓存）
  --rebuild            强制重新编译 Linux 客户端
  -q, --quiet          静默模式，仅输出沙箱 ID
  --endpoint string    StorageService 基础地址 (默认读取 $BOWL_STORAGE_ENDPOINT 或 http://127.0.0.1:9000)
  --token string       访问令牌 (默认读取 $BOWL_TOKEN)

exec 标志:
  -b, --background     后台执行，立即返回运行 ID
  --cwd string         工作目录（默认磁盘挂载点）

logs 标志:
  -f, --follow         跟随后台输出
  --exec string        指定运行 ID（默认最近一次后台运行）

mount 标志:
  --disk string        要挂载的磁盘 ID
  --path string        容器内挂载点 (默认 ` + sandbox.DefaultMountPath + `)
  --read-only, -r      只读挂载（挂的是挂载时刻的快照，不随别人写入更新）
  --subdir string      只挂载盘内某个子目录
  --force              已有写者时仍然接管（默认会拒绝）

kill 标志:
  --keep-disk          只删容器，保留磁盘
  -a, --all            销毁本机所有沙箱

说明:
  --endpoint 必须同时被本机和沙箱容器访问到。存储服务以容器形式部署时，用 compose 发布的
  宿主端口即可（例如 --endpoint http://127.0.0.1:9000），回环地址会自动改写为 host.docker.internal。
`

// executeSandbox 作为 cli.Execute 的 sandbox 子命令入口。
func executeSandbox(args []string, stdout, stderr io.Writer) int {
	if len(args) == 0 {
		fmt.Fprint(stderr, sandboxUsageHelp)
		return 2
	}
	switch args[0] {
	case "help", "--help", "-h":
		fmt.Fprint(stdout, sandboxUsageHelp)
		return 0
	case "create":
		return sandboxCreate(args[1:], stdout, stderr)
	case "list", "ls":
		return sandboxList(args[1:], stdout, stderr)
	case "exec":
		return sandboxExec(args[1:], stdout, stderr)
	case "logs", "log":
		return sandboxLogsCmd(args[1:], stdout, stderr)
	case "wait":
		return sandboxWaitCmd(args[1:], stdout, stderr)
	case "shell":
		return sandboxShellCmd(args[1:], stdout, stderr)
	case "cp", "copy":
		return sandboxCp(args[1:], stdout, stderr)
	case "mount":
		return sandboxMountCmd(args[1:], stdout, stderr)
	case "unmount", "umount":
		return sandboxUnmountCmd(args[1:], stdout, stderr)
	case "mounts":
		return sandboxMountsCmd(args[1:], stdout, stderr)
	case "info":
		return sandboxInfo(args[1:], stdout, stderr)
	case "fork":
		return sandboxFork(args[1:], stdout, stderr)
	case "move":
		return sandboxMove(args[1:], stdout, stderr)
	case "kill", "rm", "delete":
		return sandboxKill(args[1:], stdout, stderr)
	default:
		fmt.Fprintf(stderr, "错误: 未知命令 \"%s\"\n\n%s", args[0], sandboxUsageHelp)
		return 2
	}
}

// sandboxCreate 建盘（或分叉）、起容器、在容器内挂载磁盘。
func sandboxCreate(args []string, stdout, stderr io.Writer) int {
	fs := sboxFlagSet()
	var (
		from     = fs.String("from", "", "")
		size     = fs.String("size", "1GiB", "")
		image    = fs.String("image", sandbox.DefaultImage, "")
		name     = fs.String("name", "", "")
		mountAt  = fs.String("mount", sandbox.DefaultMountPath, "")
		network  = fs.String("network", "", "")
		binary   = fs.String("binary", "", "")
		rebuild  = fs.Bool("rebuild", false, "")
		quiet    = fs.Bool("quiet", false, "")
		endpoint = fs.String("endpoint", "", "")
		token    = fs.String("token", "", "")
	)
	fs.BoolVar(quiet, "q", false, "")
	if code, done := parseSboxFlags(fs, args, stdout, stderr); done {
		return code
	}
	if fs.NArg() > 0 {
		fmt.Fprintf(stderr, "错误: 多余的参数 %v\n", fs.Args())
		return 2
	}
	if !sboxHasDocker(stderr) {
		return 1
	}
	resolvedEndpoint, client, code := sboxDiskClient(*endpoint, *token, stderr)
	if code != 0 {
		return code
	}
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
	diskID, cliErr := sboxCreateDisk(ctx, client, *from, *size, *name)
	cancel()
	if cliErr != nil {
		fmt.Fprintf(stderr, "错误: %s\n", cliErr.msg)
		return cliErr.exitCode
	}

	bin, err := sandbox.EnsureClientBinary(*binary, *rebuild, stderr)
	if err != nil {
		sboxDropDisk(client, diskID)
		fmt.Fprintf(stderr, "错误: %v\n", err)
		return 1
	}
	startCtx, startCancel := context.WithTimeout(context.Background(), 90*time.Second)
	err = sboxRuntime.Create(startCtx, &sandbox.CreateSpec{
		ID: diskID, Image: *image, Name: *name, MountPath: *mountAt,
		Network: *network, HostEndpoint: resolvedEndpoint, Token: rpc.ResolveToken(*token), ClientBinary: bin,
	})
	startCancel()
	if err != nil {
		sboxDropDisk(client, diskID)
		fmt.Fprintf(stderr, "错误: %v\n", err)
		return 1
	}

	if *quiet {
		fmt.Fprintln(stdout, diskID)
		return 0
	}
	fmt.Fprintf(stdout, "沙箱 %s 已就绪\n", diskID)
	fmt.Fprintf(stdout, "  镜像   %s\n", *image)
	fmt.Fprintf(stdout, "  磁盘   %s (容器内 %s)\n", diskID, *mountAt)
	fmt.Fprintf(stdout, "  接入   bowl sandbox exec %s -- <命令>\n", diskID)
	return 0
}

// sandboxList 从容器标签还原沙箱列表，不依赖任何本地状态文件。
func sandboxList(args []string, stdout, stderr io.Writer) int {
	fs := sboxFlagSet()
	if code, done := parseSboxFlags(fs, args, stdout, stderr); done {
		return code
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	rows, err := sboxRuntime.List(ctx)
	if err != nil {
		fmt.Fprintf(stderr, "错误: %v\n", err)
		return 1
	}
	if len(rows) == 0 {
		fmt.Fprintln(stdout, "本机没有沙箱")
		return 0
	}
	fmt.Fprintf(stdout, "%-38s %-24s %-22s %-12s %s\n", "SANDBOX ID", "STATUS", "IMAGE", "MOUNT", "CREATED")
	for _, r := range rows {
		fmt.Fprintf(stdout, "%-38s %-24s %-22s %-12s %s\n", r.ID, r.Status, r.Image, r.Mount, r.Created)
	}
	return 0
}

// sandboxExec 在沙箱内执行命令，工作目录默认是磁盘挂载点，退出码透传给调用方。
// -b/--background 时立即返回运行 ID，日志与退出码用 logs / wait 取。
func sandboxExec(args []string, stdout, stderr io.Writer) int {
	background, cwd, rest, code, done := sandboxExecFlags(args)
	if done {
		fmt.Fprint(stderr, "用法: bowl sandbox exec [-b] [--cwd DIR] <sandbox-id> [--] <command> [args...]\n")
		return code
	}
	id, cmdArgs, code, done := sboxTarget(rest, "bowl sandbox exec <sandbox-id> [--] <command> [args...]", stdout, stderr)
	if done {
		return code
	}
	if len(cmdArgs) == 0 {
		fmt.Fprint(stderr, "错误: 缺少要执行的命令\n")
		return 2
	}
	ctx := context.Background()
	mountAt, err := sboxRuntime.DefaultMount(ctx, id)
	if err != nil {
		fmt.Fprintf(stderr, "错误: %v\n", err)
		return 1
	}
	workdir := mountAt
	if cwd != "" {
		workdir = cwd
	}
	if background {
		runID, bgErr := sboxRuntime.ExecBackground(ctx, id, workdir, cmdArgs)
		if bgErr != nil {
			fmt.Fprintf(stderr, "错误: %v\n", bgErr)
			return 1
		}
		fmt.Fprintln(stdout, runID)
		return 0
	}
	code, err = sboxRuntime.Exec(ctx, id, workdir, cmdArgs,
		sandbox.ExecOptions{TTY: fileIsTerminal(os.Stdin)}, os.Stdin, stdout, stderr)
	if err != nil {
		fmt.Fprintf(stderr, "错误: %v\n", err)
		return 1
	}
	return code
}

// sandboxShellCmd 进入沙箱的交互式 shell（有 bash 用 bash）。
func sandboxShellCmd(args []string, stdout, stderr io.Writer) int {
	id, rest, code, done := sboxTarget(args, "bowl sandbox shell <sandbox-id>", stdout, stderr)
	if done {
		return code
	}
	if len(rest) > 0 {
		fmt.Fprintf(stderr, "错误: 多余的参数 %v\n", rest)
		return 2
	}
	ctx := context.Background()
	mountAt, err := sboxRuntime.DefaultMount(ctx, id)
	if err != nil {
		fmt.Fprintf(stderr, "错误: %v\n", err)
		return 1
	}
	shell := "sh"
	var probe bytes.Buffer
	if probeCode, probeErr := sboxRuntime.Exec(ctx, id, mountAt, []string{"sh", "-c", "command -v bash"},
		sandbox.ExecOptions{}, nil, &probe, io.Discard); probeErr == nil && probeCode == 0 {
		shell = "bash"
	}
	code, err = sboxRuntime.Exec(ctx, id, mountAt, []string{shell},
		sandbox.ExecOptions{TTY: true}, os.Stdin, stdout, stderr)
	if err != nil {
		fmt.Fprintf(stderr, "错误: %v\n", err)
		return 1
	}
	return code
}

// sandboxCp 在本机与沙箱之间复制文件或目录。
func sandboxCp(args []string, stdout, stderr io.Writer) int {
	if len(args) == 1 && (args[0] == "help" || args[0] == "--help" || args[0] == "-h") {
		fmt.Fprint(stdout, sandboxUsageHelp)
		return 0
	}
	if len(args) != 2 {
		fmt.Fprint(stderr, "用法: bowl sandbox cp <sandbox-id>:<path> <local-path>\n"+
			"      bowl sandbox cp <local-path> <sandbox-id>:<path>\n")
		return 2
	}
	srcID, srcPath, srcInSandbox := sboxSplitCpSpec(args[0])
	dstID, dstPath, dstInSandbox := sboxSplitCpSpec(args[1])
	switch {
	case srcInSandbox && dstInSandbox:
		fmt.Fprint(stderr, "错误: 两端都是沙箱路径，至少一端必须是本机路径\n")
		return 2
	case !srcInSandbox && !dstInSandbox:
		fmt.Fprint(stderr, "错误: 至少一端要写成 <sandbox-id>:<path>，例如 <sandbox-id>:/mnt/data/note.txt\n")
		return 2
	}
	ctx := context.Background()
	var err error
	if srcInSandbox {
		err = sboxRuntime.CopyFrom(ctx, srcID, srcPath, dstPath, stderr)
	} else {
		err = sboxRuntime.CopyInto(ctx, dstID, srcPath, dstPath, stderr)
	}
	if err != nil {
		fmt.Fprintf(stderr, "错误: %v\n", err)
		return 1
	}
	return 0
}

// sandboxLogsCmd 输出后台运行的日志。
func sandboxLogsCmd(args []string, stdout, stderr io.Writer) int {
	var (
		follow  bool
		runID   string
		posArgs []string
	)
	for i := 0; i < len(args); i++ {
		switch arg := args[i]; {
		case arg == "help" || arg == "--help" || arg == "-h":
			fmt.Fprint(stdout, sandboxUsageHelp)
			return 0
		case arg == "-f" || arg == "--follow":
			follow = true
		case arg == "--exec" && i+1 < len(args):
			i++
			runID = args[i]
		case strings.HasPrefix(arg, "--exec="):
			runID = strings.TrimPrefix(arg, "--exec=")
		case strings.HasPrefix(arg, "-"):
			fmt.Fprintf(stderr, "错误: 未知标志 %s\n", arg)
			return 2
		default:
			posArgs = append(posArgs, arg)
		}
	}
	if len(posArgs) != 1 {
		fmt.Fprint(stderr, "用法: bowl sandbox logs <sandbox-id> [-f] [--exec <run-id>]\n")
		return 2
	}
	if err := sboxRuntime.Logs(context.Background(), sandbox.StripID(posArgs[0]), runID, follow, stdout, stderr); err != nil {
		fmt.Fprintf(stderr, "错误: %v\n", err)
		return 1
	}
	return 0
}

// sandboxWaitCmd 阻塞到后台运行结束，并以该命令的退出码结束自己。
func sandboxWaitCmd(args []string, stdout, stderr io.Writer) int {
	if len(args) != 2 {
		fmt.Fprint(stderr, "用法: bowl sandbox wait <sandbox-id> <run-id>\n")
		return 2
	}
	code, err := sboxRuntime.Wait(context.Background(), sandbox.StripID(args[0]), args[1])
	if err != nil {
		fmt.Fprintf(stderr, "错误: %v\n", err)
		return 1
	}
	fmt.Fprintln(stdout, code)
	return code
}

// sandboxMountCmd 把一块磁盘挂进正在运行的沙箱。
func sandboxMountCmd(args []string, stdout, stderr io.Writer) int {
	var (
		diskID, mountAt, subdir string
		readOnly, force         bool
		posArgs                 []string
	)
	for i := 0; i < len(args); i++ {
		switch arg := args[i]; {
		case arg == "help" || arg == "--help" || arg == "-h":
			fmt.Fprint(stdout, sandboxUsageHelp)
			return 0
		case (arg == "--disk" || arg == "--path" || arg == "--subdir") && i+1 < len(args):
			i++
			switch arg {
			case "--disk":
				diskID = args[i]
			case "--path":
				mountAt = args[i]
			case "--subdir":
				subdir = args[i]
			}
		case strings.HasPrefix(arg, "--disk="):
			diskID = strings.TrimPrefix(arg, "--disk=")
		case strings.HasPrefix(arg, "--path="):
			mountAt = strings.TrimPrefix(arg, "--path=")
		case strings.HasPrefix(arg, "--subdir="):
			subdir = strings.TrimPrefix(arg, "--subdir=")
		case arg == "--read-only" || arg == "-r":
			readOnly = true
		case arg == "--force":
			force = true
		case strings.HasPrefix(arg, "-"):
			fmt.Fprintf(stderr, "错误: 未知标志 %s\n", arg)
			return 2
		default:
			posArgs = append(posArgs, arg)
		}
	}
	if len(posArgs) != 1 || diskID == "" {
		fmt.Fprint(stderr, "用法: bowl sandbox mount <sandbox-id> --disk <disk-id> [--path /mnt/data] [--read-only] [--subdir /x]\n")
		return 2
	}
	if mountAt == "" {
		mountAt = sandbox.DefaultMountPath
	}
	id := sandbox.StripID(posArgs[0])
	ctx := context.Background()
	// 一块盘同时只能有一个写者。已有写者时服务端不报错、直接静默接管，
	// 被抢占的一方要到下一次写入才会撞上 EBUSY，所以这里先挡住。
	if !readOnly && !force {
		writers, err := sboxRuntime.Writers(ctx, diskID)
		if err != nil {
			fmt.Fprintf(stderr, "警告: 无法检查写者冲突(%v)，继续挂载\n", err)
		}
		if w, conflicted := sboxConflictingWriter(writers, id, mountAt); conflicted {
			fmt.Fprintf(stderr, "错误: 磁盘 %s 正被沙箱 %s 以读写方式挂在 %s。\n", diskID, w.SandboxID, w.Path)
			fmt.Fprintf(stderr, "      接管会让对方的写入失败(EBUSY)。确认要接管请加 --force；只想看内容请用 --read-only。\n")
			return 1
		}
	}
	if err := sboxRuntime.Mount(ctx, id, diskID, mountAt,
		sandbox.MountOptions{ReadOnly: readOnly, Subdir: subdir}); err != nil {
		fmt.Fprintf(stderr, "错误: %v\n", err)
		return 1
	}
	fmt.Fprintf(stdout, "已把磁盘 %s 挂到 %s:%s\n", diskID, id, mountAt)
	return 0
}

// sandboxUnmountCmd 卸载沙箱里的一个挂载点。
func sandboxUnmountCmd(args []string, stdout, stderr io.Writer) int {
	var mountAt string
	var posArgs []string
	for i := 0; i < len(args); i++ {
		switch arg := args[i]; {
		case arg == "help" || arg == "--help" || arg == "-h":
			fmt.Fprint(stdout, sandboxUsageHelp)
			return 0
		case arg == "--path" && i+1 < len(args):
			i++
			mountAt = args[i]
		case strings.HasPrefix(arg, "--path="):
			mountAt = strings.TrimPrefix(arg, "--path=")
		case strings.HasPrefix(arg, "-"):
			fmt.Fprintf(stderr, "错误: 未知标志 %s\n", arg)
			return 2
		default:
			posArgs = append(posArgs, arg)
		}
	}
	if len(posArgs) != 1 || mountAt == "" {
		fmt.Fprint(stderr, "用法: bowl sandbox unmount <sandbox-id> --path /mnt/data\n")
		return 2
	}
	if err := sboxRuntime.Unmount(context.Background(), sandbox.StripID(posArgs[0]), mountAt); err != nil {
		fmt.Fprintf(stderr, "错误: %v\n", err)
		return 1
	}
	fmt.Fprintf(stdout, "已卸载 %s\n", mountAt)
	return 0
}

// sandboxMountsCmd 列出沙箱里当前挂着的磁盘。
func sandboxMountsCmd(args []string, stdout, stderr io.Writer) int {
	if len(args) != 1 {
		fmt.Fprint(stderr, "用法: bowl sandbox mounts <sandbox-id>\n")
		return 2
	}
	mounts, err := sboxRuntime.Mounts(context.Background(), sandbox.StripID(args[0]))
	if err != nil {
		fmt.Fprintf(stderr, "错误: %v\n", err)
		return 1
	}
	if len(mounts) == 0 {
		fmt.Fprintln(stdout, "该沙箱没有挂载任何磁盘")
		return 0
	}
	fmt.Fprintf(stdout, "%-38s %-20s %s\n", "DISK ID", "MOUNT PATH", "MODE")
	for _, m := range mounts {
		mode := "rw"
		if m.ReadOnly {
			mode = "ro"
		}
		fmt.Fprintf(stdout, "%-38s %-20s %s\n", m.DiskID, m.Path, mode)
	}
	return 0
}

// sandboxInfo 汇总沙箱状态、镜像与当前挂载。
func sandboxInfo(args []string, stdout, stderr io.Writer) int {
	if len(args) != 1 {
		fmt.Fprint(stderr, "用法: bowl sandbox info <sandbox-id>\n")
		return 2
	}
	info, err := sboxRuntime.Info(context.Background(), sandbox.StripID(args[0]))
	if err != nil {
		fmt.Fprintf(stderr, "错误: %v\n", err)
		return 1
	}
	fmt.Fprintf(stdout, "沙箱   %s\n", info.ID)
	fmt.Fprintf(stdout, "状态   %s\n", info.Status)
	fmt.Fprintf(stdout, "镜像   %s\n", info.Image)
	fmt.Fprintf(stdout, "创建   %s\n", info.Created)
	fmt.Fprintf(stdout, "容器   %s\n", info.Container)
	if info.Endpoint != "" {
		fmt.Fprintf(stdout, "节点   %s\n", info.Endpoint)
	}
	if len(info.Mounts) == 0 {
		fmt.Fprintln(stdout, "挂载   (无)")
	}
	for _, m := range info.Mounts {
		mode := "rw"
		if m.ReadOnly {
			mode = "ro"
		}
		fmt.Fprintf(stdout, "挂载   %s → %s (%s)\n", m.DiskID, m.Path, mode)
	}
	return 0
}

// sandboxFork 从一个沙箱（或磁盘）分叉出多个新沙箱，ID 一行一个打印。
func sandboxFork(args []string, stdout, stderr io.Writer) int {
	fs := sboxFlagSet()
	var (
		count    = fs.Int("count", 1, "")
		image    = fs.String("image", sandbox.DefaultImage, "")
		name     = fs.String("name", "", "")
		mountAt  = fs.String("mount", sandbox.DefaultMountPath, "")
		network  = fs.String("network", "", "")
		binary   = fs.String("binary", "", "")
		quiet    = fs.Bool("quiet", false, "")
		endpoint = fs.String("endpoint", "", "")
		token    = fs.String("token", "", "")
	)
	fs.BoolVar(quiet, "q", false, "")
	args = sboxHoistFlags(args, []string{"count", "image", "name", "mount", "network", "binary", "endpoint", "token"})
	if code, done := parseSboxFlags(fs, args, stdout, stderr); done {
		return code
	}
	if fs.NArg() != 1 {
		fmt.Fprint(stderr, "用法: bowl sandbox fork <sandbox-id> [--count N] [--image IMG]\n")
		return 2
	}
	if *count < 1 {
		fmt.Fprint(stderr, "错误: --count 至少为 1\n")
		return 2
	}
	source := sandbox.StripID(fs.Arg(0))
	resolvedEndpoint, client, code := sboxDiskClient(*endpoint, *token, stderr)
	if code != 0 {
		return code
	}
	bin, err := sandbox.EnsureClientBinary(*binary, false, stderr)
	if err != nil {
		fmt.Fprintf(stderr, "错误: %v\n", err)
		return 1
	}
	for i := 0; i < *count; i++ {
		forkName := *name
		if forkName != "" && *count > 1 {
			forkName = fmt.Sprintf("%s-%d", forkName, i+1)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
		diskID, cliErr := sboxCreateDisk(ctx, client, source, "1GiB", forkName)
		cancel()
		if cliErr != nil {
			fmt.Fprintf(stderr, "错误: %s\n", cliErr.msg)
			return cliErr.exitCode
		}
		startCtx, startCancel := context.WithTimeout(context.Background(), 90*time.Second)
		err := sboxRuntime.Create(startCtx, &sandbox.CreateSpec{
			ID: diskID, Image: *image, Name: forkName, MountPath: *mountAt,
			Network: *network, HostEndpoint: resolvedEndpoint, Token: rpc.ResolveToken(*token), ClientBinary: bin,
		})
		startCancel()
		if err != nil {
			sboxDropDisk(client, diskID)
			fmt.Fprintf(stderr, "错误: %v\n", err)
			return 1
		}
		if *quiet {
			fmt.Fprintln(stdout, diskID)
		} else {
			fmt.Fprintf(stdout, "沙箱 %s 已就绪（来自 %s）\n", diskID, source)
		}
	}
	return 0
}

// sandboxKill 删容器；默认连磁盘一起删，--keep-disk 只删容器。
func sandboxKill(args []string, stdout, stderr io.Writer) int {
	opts, positionals, code, done := sboxKillArgs(args, stdout, stderr)
	if done {
		return code
	}
	ctx := context.Background()
	if opts.all {
		return sboxKillAll(ctx, opts, stdout, stderr)
	}
	if len(positionals) != 1 {
		fmt.Fprint(stderr, "用法: bowl sandbox kill <sandbox-id> [--keep-disk]\n       bowl sandbox kill --all [--keep-disk]\n")
		return 2
	}
	id := sandbox.StripID(positionals[0])
	if opts.endpoint == "" {
		// 盘可能已经被 move 到别的节点：删盘要去它现在所在的节点删。
		if info, infoErr := sboxRuntime.Info(ctx, id); infoErr == nil {
			opts.endpoint = info.Endpoint
		}
	}
	if err := sboxRuntime.Remove(ctx, id); err != nil {
		fmt.Fprintf(stderr, "错误: %v\n", err)
		return 1
	}
	if opts.keepDisk {
		fmt.Fprintf(stdout, "沙箱 %s 的容器已删除，磁盘保留\n", id)
		return 0
	}
	if code := sboxDeleteDisk(opts.endpoint, opts.token, stderr, id); code != 0 {
		return code
	}
	fmt.Fprintf(stdout, "沙箱 %s 已删除\n", id)
	return 0
}

// sboxKillAll 批量销毁本机所有沙箱。
func sboxKillAll(ctx context.Context, opts sboxKillOptions, stdout, stderr io.Writer) int {
	ids, err := sboxRuntime.IDs(ctx)
	if err != nil {
		fmt.Fprintf(stderr, "错误: %v\n", err)
		return 1
	}
	if len(ids) == 0 {
		fmt.Fprintln(stdout, "本机没有沙箱")
		return 0
	}
	code := 0
	for _, id := range ids {
		if err := sboxRuntime.Remove(ctx, id); err != nil {
			fmt.Fprintf(stderr, "错误: %v\n", err)
			code = 1
			continue
		}
		if opts.keepDisk {
			fmt.Fprintf(stdout, "沙箱 %s 的容器已删除，磁盘保留\n", id)
			continue
		}
		if c := sboxDeleteDisk(opts.endpoint, opts.token, stderr, id); c != 0 {
			code = c
			continue
		}
		fmt.Fprintf(stdout, "沙箱 %s 已删除\n", id)
	}
	return code
}

type sboxKillOptions struct {
	keepDisk bool
	all      bool
	endpoint string
	token    string
}

// sboxKillArgs 手写解析 kill 的标志：flag 包遇到第一个位置参数就停止，
// 而 `kill <id> --keep-disk` 是最自然的写法。
func sboxKillArgs(args []string, stdout, stderr io.Writer) (opts sboxKillOptions, positionals []string, code int, done bool) {
	for i := 0; i < len(args); i++ {
		switch arg := args[i]; {
		case arg == "help" || arg == "--help" || arg == "-h":
			fmt.Fprint(stdout, sandboxUsageHelp)
			return opts, nil, 0, true
		case arg == "--keep-disk":
			opts.keepDisk = true
		case arg == "-a" || arg == "--all":
			opts.all = true
		case arg == "--endpoint" && i+1 < len(args):
			i++
			opts.endpoint = args[i]
		case arg == "--token" && i+1 < len(args):
			i++
			opts.token = args[i]
		case strings.HasPrefix(arg, "--endpoint="):
			opts.endpoint = strings.TrimPrefix(arg, "--endpoint=")
		case strings.HasPrefix(arg, "--token="):
			opts.token = strings.TrimPrefix(arg, "--token=")
		default:
			if strings.HasPrefix(arg, "-") {
				fmt.Fprintf(stderr, "错误: 未知标志 %s\n", arg)
				return opts, nil, 2, true
			}
			positionals = append(positionals, arg)
		}
	}
	return opts, positionals, 0, false
}

// --- 控制面：磁盘的建立与销毁留在 CLI，运行时不认识 StorageService ---

// sboxDiskClient 解析 endpoint 并创建 StorageService 客户端。
func sboxDiskClient(endpoint, token string, stderr io.Writer) (resolved string, client storagev1connect.StorageServiceClient, code int) {
	resolvedEndpoint, err := rpc.ResolveEndpoint(endpoint)
	if err != nil {
		fmt.Fprintf(stderr, "错误: 无效的 endpoint: %v\n", err)
		return "", nil, 2
	}
	client, code, err = newDiskStorageClient(resolvedEndpoint, token)
	if err != nil {
		fmt.Fprintf(stderr, "错误: %v\n", err)
		return "", nil, code
	}
	return resolvedEndpoint, client, 0
}

// sboxCreateDisk 按 from/size 派生或新建磁盘，返回磁盘 ID。
func sboxCreateDisk(
	ctx context.Context,
	client storagev1connect.StorageServiceClient,
	from, size, name string,
) (string, *diskCLIError) {
	if from != "" {
		resp, err := client.ForkDisk(ctx, connect.NewRequest(&storagev1.ForkDiskRequest{
			SourceDiskId: from,
			Name:         name,
		}))
		if err != nil {
			return "", mapDiskRPCError(err, diskRPCErrorPolicy{diskID: from, defaultOp: "分叉沙箱磁盘失败"})
		}
		return resp.Msg.GetDisk().GetDiskId(), nil
	}
	maxBytes, err := ParseSize(size)
	if err != nil {
		return "", &diskCLIError{exitCode: 2, msg: fmt.Sprintf("无效的 --size 参数: %v", err), cause: err}
	}
	if err = validateDiskName(name); err != nil {
		return "", &diskCLIError{exitCode: 2, msg: err.Error(), cause: err}
	}
	resp, err := client.CreateDisk(ctx, connect.NewRequest(&storagev1.CreateDiskRequest{
		Name:     name,
		MaxBytes: maxBytes,
	}))
	if err != nil {
		return "", mapDiskRPCError(err, diskRPCErrorPolicy{defaultOp: "创建沙箱磁盘失败"})
	}
	return resp.Msg.GetDisk().GetDiskId(), nil
}

// sboxDropDisk 在启动失败后尽力回收刚建的磁盘，错误只忽略：调用方已经在报错了。
func sboxDropDisk(client storagev1connect.StorageServiceClient, diskID string) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	_, _ = client.DeleteDisk(ctx, connect.NewRequest(&storagev1.DeleteDiskRequest{DiskId: diskID}))
}

// sboxDeleteDisk 删磁盘，并容忍容器刚被删除时挂载租约尚未过期导致的 EBUSY。
func sboxDeleteDisk(endpoint, token string, stderr io.Writer, diskID string) int {
	client, code, err := newDiskStorageClient(endpoint, token)
	if err != nil {
		fmt.Fprintf(stderr, "错误: %v\n", err)
		return code
	}
	for range 6 {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		_, err := client.DeleteDisk(ctx, connect.NewRequest(&storagev1.DeleteDiskRequest{DiskId: diskID}))
		cancel()
		if err == nil {
			return 0
		}
		if !isEBUSY(err) {
			fmt.Fprintf(stderr, "错误: 删除磁盘失败: %v\n", err)
			return 1
		}
		time.Sleep(2 * time.Second)
	}
	fmt.Fprintf(stderr, "错误: 磁盘 %s 仍被挂载会话占用，稍后用 bowl disk delete %s 重试\n", diskID, diskID)
	return 1
}

func isEBUSY(err error) bool {
	if connErr, ok := errors.AsType[*connect.Error](err); ok {
		return strings.Contains(connErr.Message(), "EBUSY") || strings.Contains(connErr.Message(), "still mounted")
	}
	return false
}

// --- 命令行辅助 ---

// sandboxExecFlags 解析 exec 的前置标志；沙箱 ID 之后的一切原样透传，所以标志必须写在 ID 前。
func sandboxExecFlags(args []string) (background bool, cwd string, rest []string, code int, done bool) {
	for i := 0; i < len(args); i++ {
		switch arg := args[i]; {
		case arg == "help" || arg == "--help" || arg == "-h":
			return false, "", args[i:], 0, true
		case arg == "-b" || arg == "--background":
			background = true
		case arg == "--cwd" && i+1 < len(args):
			i++
			cwd = args[i]
		case strings.HasPrefix(arg, "--cwd="):
			cwd = strings.TrimPrefix(arg, "--cwd=")
		case strings.HasPrefix(arg, "-"):
			return false, "", nil, 2, true
		default:
			return background, cwd, args[i:], 0, false
		}
	}
	return background, "", nil, 0, true
}

// sboxConflictingWriter 判断是否已有会冲突的写者：同一沙箱同一路径的重复挂载视为刷新，不算冲突。
func sboxConflictingWriter(writers []sandbox.Writer, id, path string) (sandbox.Writer, bool) {
	for _, w := range writers {
		if w.SandboxID == id && w.Path == path {
			continue
		}
		return w, true
	}
	return sandbox.Writer{}, false
}

// sboxSplitCpSpec 解析 `<sandbox-id>:<path>`；本机路径原样作为 path 返回。
func sboxSplitCpSpec(spec string) (id, path string, inSandbox bool) {
	left, right, ok := strings.Cut(spec, ":")
	if !ok || left == "" || strings.Contains(left, "/") {
		return "", spec, false
	}
	return sandbox.StripID(left), right, true
}

// sboxTarget 解析形如 `<sandbox-id> [--] rest...` 的参数。
func sboxTarget(args []string, usage string, stdout, stderr io.Writer) (id string, rest []string, code int, done bool) {
	if len(args) == 0 {
		fmt.Fprintf(stderr, "用法: %s\n", usage)
		return "", nil, 2, true
	}
	switch args[0] {
	case "help", "--help", "-h":
		fmt.Fprint(stdout, sandboxUsageHelp)
		return "", nil, 0, true
	}
	id = sandbox.StripID(args[0])
	rest = args[1:]
	if len(rest) > 0 && rest[0] == "--" {
		rest = rest[1:]
	}
	return id, rest, 0, false
}

// sboxHoistFlags 把标志（含需要取值的标志）挪到位置参数前面。
// flag 包遇到第一个位置参数就停止解析，而 `fork <id> --count 2` 是最自然的写法。
func sboxHoistFlags(args, valueFlags []string) []string {
	flags := make([]string, 0, len(args))
	pos := make([]string, 0, len(args))
	for i := 0; i < len(args); i++ {
		arg := args[i]
		if !strings.HasPrefix(arg, "-") {
			pos = append(pos, arg)
			continue
		}
		name := strings.TrimLeft(arg, "-")
		if strings.Contains(name, "=") {
			flags = append(flags, arg)
			continue
		}
		if slices.Contains(valueFlags, name) && i+1 < len(args) {
			flags = append(flags, arg, args[i+1])
			i++
			continue
		}
		flags = append(flags, arg)
	}
	return append(flags, pos...)
}

// sboxHasDocker 检查 docker 是否可用。
func sboxHasDocker(stderr io.Writer) bool {
	if _, err := exec.LookPath("docker"); err != nil {
		fmt.Fprintln(stderr, "错误: 未找到 docker 命令，sandbox 需要本机 Docker")
		return false
	}
	return true
}

// sboxFlagSet 构造静默输出的 FlagSet，帮助信息由 sandboxUsageHelp 统一给出。
func sboxFlagSet() *flag.FlagSet {
	fs := flag.NewFlagSet("bowl sandbox", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	fs.Usage = func() {}
	return fs
}

// parseSboxFlags 解析标志；-h 打印用法并结束。
func parseSboxFlags(fs *flag.FlagSet, args []string, stdout, stderr io.Writer) (int, bool) {
	err := fs.Parse(args)
	if err == nil {
		return 0, false
	}
	if errors.Is(err, flag.ErrHelp) {
		fmt.Fprint(stdout, sandboxUsageHelp)
		return 0, true
	}
	fmt.Fprintf(stderr, "错误: %v\n", err)
	return 2, true
}

func fileIsTerminal(f *os.File) bool {
	return term.IsTerminal(int(f.Fd()))
}
