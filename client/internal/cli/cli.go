// Package cli 实现 bowl 客户端命令行参数解析、磁盘控制面操作与 FUSE 挂载命令路由。
package cli

import (
	"fmt"
	"io"

	"github.com/starsrc/bowl/client/internal/version"
)

const usageHelp = `bowl - 分布式虚拟磁盘 Linux FUSE 客户端

用法:
  bowl <command> [flags] [arguments]

可用命令:
  disk        管理虚拟磁盘生命周期 (创建、列表、详情、更新/扩缩容、删除)
  sandbox     在本机容器里跑以虚拟磁盘为数据盘的沙箱 (创建、列表、执行、销毁)
  mount       挂载远端虚拟磁盘至本地目录
  umount      卸载本地已挂载的虚拟磁盘 (别名: unmount)
  stats       查看本机挂载进程的 FUSE 统计视图
  version     打印客户端版本信息
  help        查看命令帮助

全局标志:
  -h, --help      查看帮助信息
  -v, --version   查看版本信息
`

// Execute 执行 bowl CLI 命令行。
// 参数 args 为命令行入参列表（不含可执行文件路径）。
// 返回进程退出码 (0 表示成功，非 0 表示异常或参数错误)。
func Execute(args []string, stdout, stderr io.Writer) int {
	if len(args) == 0 {
		fmt.Fprint(stderr, usageHelp)
		return 2
	}

	cmd := args[0]
	switch cmd {
	case "version", "--version", "-v":
		fmt.Fprintf(stdout, "bowl version %s\n", version.Version)
		return 0
	case "help", "--help", "-h":
		fmt.Fprint(stdout, usageHelp)
		return 0
	case "disk":
		return executeDisk(args[1:], stdout, stderr)
	case "sandbox":
		return executeSandbox(args[1:], stdout, stderr)
	case "mount":
		return executeMount(args[1:], stdout, stderr)
	case "umount", "unmount":
		return executeUmount(args[1:], stdout, stderr)
	case "stats":
		return executeStats(args[1:], stdout, stderr)
	default:
		fmt.Fprintf(stderr, "错误: 未知命令 \"%s\"\n\n%s", cmd, usageHelp)
		return 2
	}
}
