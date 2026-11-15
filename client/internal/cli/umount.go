package cli

import (
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"
)

const umountUsageHelp = `用法:
  bowl umount <mountpoint> [flags] (别名: unmount)

参数:
  <mountpoint>    已挂载的目录路径（必填）

标志:
  -f, --force     强制/懒卸载忙碌挂载点
  -h, --help      查看帮助信息
`

// runCommand、lookPath 与排空等待参数允许在单元测试中替身注入。
var (
	lookPath   = exec.LookPath
	runCommand = func(name string, args ...string) ([]byte, error) {
		return exec.Command(name, args...).CombinedOutput()
	}
	// 卸载命令返回后挂载进程还要刷盘并释放租约，耗时随盘上文件数增长（1.8 万个小文件实测 34 秒）。
	// 等待在挂载进程退出时立即结束，这个时限只用来兜住挂载进程卡死的情况。
	umountDrainTimeout      = 5 * time.Minute
	umountDrainPollInterval = 20 * time.Millisecond
)

func findUmountCommand(absMountpoint string, force bool) (binPath string, cmdArgs []string, found bool) {
	candidates := []struct {
		bin       string
		args      []string
		forceArgs []string
	}{
		{bin: "fusermount3", args: []string{"-u", absMountpoint}, forceArgs: []string{"-uz", absMountpoint}},
		{bin: "fusermount", args: []string{"-u", absMountpoint}, forceArgs: []string{"-uz", absMountpoint}},
		{bin: "umount", args: []string{absMountpoint}, forceArgs: []string{"-l", absMountpoint}},
	}
	for _, c := range candidates {
		path, err := lookPath(c.bin)
		if err != nil {
			continue
		}
		if force {
			return path, c.forceArgs, true
		}
		return path, c.args, true
	}
	return "", nil, false
}

func waitMountMetaDrained(metaPath string, pid int) error {
	deadline := time.Now().Add(umountDrainTimeout)
	for {
		if _, err := os.Stat(metaPath); errors.Is(err, os.ErrNotExist) {
			return nil
		}
		if pid > 0 && !processAlive(pid) {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("卸载命令已返回，但等待挂载进程完成刷盘与租约释放超时 (%s)", metaPath)
		}
		time.Sleep(umountDrainPollInterval)
	}
}

// executeUmount 解析 umount / unmount 命令参数并调用系统卸载工具。
func executeUmount(args []string, stdout, stderr io.Writer) int {
	var (
		force bool
		help  bool
	)

	var posArgs []string
	for i := range args {
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
	binPath, cmdArgs, found := findUmountCommand(absMountpoint, force)
	if !found {
		fmt.Fprintln(stderr, "错误: 未找到系统卸载工具 (fusermount3/fusermount/umount)")
		return 1
	}

	var (
		metaPath string
		hasMeta  bool
		metaPID  int
	)
	if !force {
		metaPath, hasMeta = mountMetaFilePath(absMountpoint)
		if hasMeta {
			metaPID = readMountMetaPID(metaPath)
		}
	}

	if out, err := runCommand(binPath, cmdArgs...); err != nil {
		fmt.Fprintf(stderr, "错误: 卸载执行失败: %v\n%s", err, string(out))
		return 1
	}

	if !force && hasMeta {
		if drainErr := waitMountMetaDrained(metaPath, metaPID); drainErr != nil {
			fmt.Fprintf(stderr, "错误: %v\n", drainErr)
			return 1
		}
	}
	return 0
}
