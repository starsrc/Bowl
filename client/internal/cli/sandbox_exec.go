package cli

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"strings"

	"golang.org/x/term"

	"github.com/starsrc/bowl/client/internal/sandbox"
)

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

func fileIsTerminal(f *os.File) bool {
	return term.IsTerminal(int(f.Fd()))
}
