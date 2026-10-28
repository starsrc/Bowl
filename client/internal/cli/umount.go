package cli

import (
	"fmt"
	"io"
	"os/exec"
	"path/filepath"
	"strings"
)

const umountUsageHelp = `用法:
  bowl umount <mountpoint> [flags] (别名: unmount)

参数:
  <mountpoint>    已挂载的目录路径（必填）

标志:
  -f, --force     强制/懒卸载忙碌挂载点
  -h, --help      查看帮助信息
`

type toolLooker func(file string) (string, error)
type commandRunner func(name string, args ...string) ([]byte, error)

var (
	defaultToolLooker toolLooker    = exec.LookPath
	defaultCmdRunner  commandRunner = func(name string, args ...string) ([]byte, error) {
		return exec.Command(name, args...).CombinedOutput()
	}
)

// executeUmount 解析 umount / unmount 命令参数并调用系统卸载工具。
func executeUmount(args []string, stdout, stderr io.Writer) int {
	var (
		force bool
		help  bool
	)

	var posArgs []string
	for i := 0; i < len(args); i++ {
		arg := args[i]
		if arg == "--" {
			posArgs = append(posArgs, args[i+1:]...)
			break
		}
		if strings.HasPrefix(arg, "-") {
			switch arg {
			case "-h", "--help":
				help = true
			case "-f", "--force":
				force = true
			default:
				fmt.Fprintf(stderr, "错误: 未知标志 \"%s\"\n\n%s", arg, umountUsageHelp)
				return 2
			}
		} else {
			posArgs = append(posArgs, arg)
		}
	}

	if help {
		fmt.Fprint(stdout, umountUsageHelp)
		return 0
	}

	if len(posArgs) != 1 {
		fmt.Fprintf(stderr, "错误: umount 命令需要且仅需要 1 个位置参数 (<mountpoint>)\n\n%s", umountUsageHelp)
		return 2
	}

	mountpoint := posArgs[0]
	absMountpoint, err := filepath.Abs(mountpoint)
	if err != nil {
		fmt.Fprintf(stderr, "错误: 挂载点路径解析失败: %v\n", err)
		return 1
	}
	absMountpoint = filepath.Clean(absMountpoint)

	// 1. 校验目标是否为活跃挂载点
	mounted, err := IsMountPoint(absMountpoint)
	if err != nil {
		fmt.Fprintf(stderr, "错误: 检查挂载点状态失败: %v\n", err)
		return 1
	}
	if !mounted {
		fmt.Fprintf(stderr, "错误: 路径 \"%s\" 不是已挂载的文件系统\n", absMountpoint)
		return 1
	}

	// 2. 按优先级查找并执行系统卸载工具
	// 优先级 1: fusermount3 (-u, force: -uz)
	if path, err := defaultToolLooker("fusermount3"); err == nil {
		cmdArgs := []string{"-u", absMountpoint}
		if force {
			cmdArgs = []string{"-uz", absMountpoint}
		}
		if out, err := defaultCmdRunner(path, cmdArgs...); err != nil {
			fmt.Fprintf(stderr, "错误: 卸载执行失败: %v\n%s", err, string(out))
			return 1
		}
		return 0
	}

	// 优先级 2: fusermount (-u, force: -uz)
	if path, err := defaultToolLooker("fusermount"); err == nil {
		cmdArgs := []string{"-u", absMountpoint}
		if force {
			cmdArgs = []string{"-uz", absMountpoint}
		}
		if out, err := defaultCmdRunner(path, cmdArgs...); err != nil {
			fmt.Fprintf(stderr, "错误: 卸载执行失败: %v\n%s", err, string(out))
			return 1
		}
		return 0
	}

	// 优先级 3: umount (常规 <mp>, force: -l <mp>)
	if path, err := defaultToolLooker("umount"); err == nil {
		var cmdArgs []string
		if force {
			cmdArgs = []string{"-l", absMountpoint}
		} else {
			cmdArgs = []string{absMountpoint}
		}
		if out, err := defaultCmdRunner(path, cmdArgs...); err != nil {
			fmt.Fprintf(stderr, "错误: 卸载执行失败: %v\n%s", err, string(out))
			return 1
		}
		return 0
	}

	fmt.Fprintln(stderr, "错误: 未找到系统卸载工具 (fusermount3/fusermount/umount)")
	return 1
}
