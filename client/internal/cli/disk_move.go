package cli

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"time"
)

const diskMoveUsageHelp = `用法:
  bowl disk move <disk-id> --to <节点或地址> [--from <节点或地址>] [--force]

把一块盘的写权限从一个存储节点交给另一个：原节点同步并交出，目标节点拉取并认领。
用于不走沙箱、直接把盘挂在目录上的情形。先在原节点卸载这块盘（bowl umount <目录>），
切换完成后在目标节点重新挂载（bowl mount）。带沙箱的切换用 bowl sandbox move。

标志:
  --to string       目标存储节点（必填）
  --from string     当前持有写权限的存储节点 (默认读取 $BOWL_STORAGE_ENDPOINT 或 http://127.0.0.1:9000)
  --force           原节点不可达时强行接管：跳过原节点，目标节点从远端最后一次同步的状态接手。
                    原节点上次同步之后的写入不会带过来
  --token string    访问令牌 (默认读取 $BOWL_TOKEN)，两个节点使用同一个令牌
  -h, --help        查看帮助信息
`

// executeDiskMove 把 disk push --release 与 disk pull 串成一步。
func executeDiskMove(args []string, stdout, stderr io.Writer) int {
	fs := sboxFlagSet()
	var (
		to    = fs.String("to", "", "")
		from  = fs.String("from", "", "")
		force = fs.Bool("force", false, "")
		token = fs.String("token", "", "")
	)
	if err := fs.Parse(sboxHoistFlags(args, []string{"to", "from", "token"})); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			fmt.Fprint(stdout, diskMoveUsageHelp)
			return 0
		}
		fmt.Fprintf(stderr, "错误: %v\n\n%s", err, diskMoveUsageHelp)
		return 2
	}
	if fs.NArg() != 1 || *to == "" {
		fmt.Fprint(stderr, diskMoveUsageHelp)
		return 2
	}
	diskID := fs.Arg(0)
	target, err := resolveEndpoint(*to)
	if err != nil {
		fmt.Fprintf(stderr, "错误: 无效的 --to: %v\n", err)
		return 2
	}
	source, err := resolveEndpoint(*from)
	if err != nil {
		fmt.Fprintf(stderr, "错误: 无效的 --from: %v\n", err)
		return 2
	}
	if source == target {
		fmt.Fprintf(stderr, "错误: --from 和 --to 是同一个节点 %s\n", target)
		return 2
	}

	started := time.Now()
	if !*force {
		began := time.Now()
		pushed, code := sboxHandoffStep(source, "push", diskID, handoffFlags{release: true, token: *token}, stderr)
		if code != 0 {
			return code
		}
		fmt.Fprintf(stdout, "  %-28s %d ms\n    %s\n", "同步到远端并交出写权限", time.Since(began).Milliseconds(), pushed)
	}
	began := time.Now()
	pulled, code := sboxHandoffStep(target, "pull", diskID, handoffFlags{force: *force, token: *token}, stderr)
	if code != 0 {
		if !*force {
			fmt.Fprintf(stderr, "盘已交出但目标节点没接住，现在没有写者。在任一节点执行 bowl disk pull %s 即可认领。\n", diskID)
		}
		return code
	}
	fmt.Fprintf(stdout, "  %-28s %d ms\n    %s\n", "目标节点拉取并认领", time.Since(began).Milliseconds(), pulled)
	fmt.Fprintf(stdout, "磁盘 %s 的写权限已切换到 %s（共 %d ms）\n", diskID, target, time.Since(started).Milliseconds())
	return 0
}
