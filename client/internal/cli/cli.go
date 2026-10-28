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
  mount       挂载远端虚拟磁盘至本地目录
  umount      卸载本地已挂载的虚拟磁盘 (别名: unmount)
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
	case "mount":
		// mount 子命令由后续工单 (STARSRC-509/510) 接入
		fmt.Fprintln(stderr, "错误: mount 子命令尚未就绪")
		return 1
	case "umount", "unmount":
		// umount 子命令由后续工单 (STARSRC-510) 接入
		fmt.Fprintln(stderr, "错误: umount 子命令尚未就绪")
		return 1
	default:
		fmt.Fprintf(stderr, "错误: 未知命令 \"%s\"\n\n%s", cmd, usageHelp)
		return 2
	}
}
