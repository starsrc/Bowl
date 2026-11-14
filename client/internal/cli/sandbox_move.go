package cli

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/starsrc/bowl/client/internal/rpc"
	"github.com/starsrc/bowl/client/internal/sandbox"
)

const sandboxMoveUsageHelp = `用法:
  bowl sandbox move <sandbox-id> --to <endpoint> [flags]

把一个沙箱连同它的盘从当前存储节点切换到另一个节点（例如本地 → 云端，或再切回来）。
沙箱 ID、挂载点、盘里的全部内容保持不变；切换的是"谁在跑、谁能写"。

正常切换的步骤:
  1) 在原节点卸载磁盘并停掉沙箱容器
  2) 原节点把盘的最新状态同步到远端并交出写权限
  3) 目标节点拉取并认领写权限
  4) 在目标节点重新起沙箱并挂上同一块盘

注意: 进程不会跟着走。沙箱里正在跑的命令会被停掉，需要在切换后重新启动。

标志:
  --to string       目标存储节点地址（必填）
  --from string     当前存储节点地址（默认取沙箱创建时记录的节点）
  --force           原节点不可达时强行接管：跳过第 1、2 步，目标节点从远端最后一次同步的状态接手。
                    原节点上次同步之后的写入不会带过来
  --image string    切换后使用的容器镜像（默认沿用原沙箱的镜像）
  --binary string   注入沙箱的 Linux bowl 客户端路径
  -h, --help        查看帮助信息
`

// sandboxMove 把沙箱切换到另一个存储节点。
//
// 原型里"目标节点上的算力"仍然是本机 Docker，只是容器连的是另一个存储节点；
// 真正的云端算力接进来时，第 4 步换成调用云端起沙箱，其余步骤不变。
func sandboxMove(args []string, stdout, stderr io.Writer) int {
	fs := sboxFlagSet()
	var (
		to     = fs.String("to", "", "")
		from   = fs.String("from", "", "")
		force  = fs.Bool("force", false, "")
		image  = fs.String("image", "", "")
		binary = fs.String("binary", "", "")
	)
	args = sboxHoistFlags(args, []string{"to", "from", "image", "binary"})
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			fmt.Fprint(stdout, sandboxMoveUsageHelp)
			return 0
		}
		fmt.Fprintf(stderr, "错误: %v\n\n%s", err, sandboxMoveUsageHelp)
		return 2
	}
	if fs.NArg() != 1 || *to == "" {
		fmt.Fprint(stderr, sandboxMoveUsageHelp)
		return 2
	}
	id := sandbox.StripID(fs.Arg(0))
	ctx := context.Background()

	target, err := rpc.ResolveEndpoint(*to)
	if err != nil {
		fmt.Fprintf(stderr, "错误: 无效的 --to: %v\n", err)
		return 2
	}

	info, infoErr := sboxRuntime.Info(ctx, id)
	hasContainer := infoErr == nil
	source := *from
	if source == "" {
		source = info.Endpoint
	}
	if source == "" && !*force {
		fmt.Fprintln(stderr, "错误: 不知道这个沙箱当前在哪个存储节点上，请用 --from 指定")
		return 2
	}
	if source != "" {
		if source, err = rpc.ResolveEndpoint(source); err != nil {
			fmt.Fprintf(stderr, "错误: 无效的 --from: %v\n", err)
			return 2
		}
		if source == target {
			fmt.Fprintf(stderr, "错误: 沙箱 %s 已经在 %s 上\n", id, target)
			return 2
		}
	}
	if !hasContainer && !*force {
		fmt.Fprintf(stderr, "错误: %v\n", infoErr)
		return 1
	}
	spec := sandbox.CreateSpec{
		ID: id, Image: info.Image, MountPath: info.MountPath, Network: info.Network, HostEndpoint: target,
	}
	if *image != "" {
		spec.Image = *image
	}

	started := time.Now()
	step := func(label string, began time.Time) {
		fmt.Fprintf(stdout, "  %-28s %d ms\n", label, time.Since(began).Milliseconds())
	}

	if !*force {
		began := time.Now()
		for _, m := range info.Mounts {
			if err = sboxRuntime.Unmount(ctx, id, m.Path); err != nil {
				fmt.Fprintf(stderr, "错误: %v\n", err)
				return 1
			}
		}
		if err = sboxRuntime.Remove(ctx, id); err != nil {
			fmt.Fprintf(stderr, "错误: %v\n", err)
			return 1
		}
		step("停掉原沙箱并卸载", began)

		began = time.Now()
		pushed, code := sboxHandoffStep(source, "push", id, true, false, stderr)
		if code != 0 {
			fmt.Fprintf(stderr, "沙箱容器已停，盘仍归原节点。修复后可重新执行，或用 bowl sandbox create --from 另起。\n")
			return code
		}
		step("同步到远端并交出写权限", began)
		fmt.Fprintf(stdout, "    %s\n", pushed)
	} else if hasContainer {
		// 原型里新旧沙箱在同一个 Docker 上同名，必须先让位；真实场景下旧沙箱在另一台机器上。
		_ = sboxRuntime.Remove(ctx, id)
	}

	began := time.Now()
	pulled, code := sboxHandoffStep(target, "pull", id, false, *force, stderr)
	if code != 0 {
		if !*force {
			fmt.Fprintf(stderr, "盘已交出但目标节点没接住，现在没有写者。在任一节点执行 bowl disk pull %s 即可认领。\n", id)
		}
		return code
	}
	step("目标节点拉取并认领", began)
	fmt.Fprintf(stdout, "    %s\n", pulled)

	began = time.Now()
	bin, err := sandbox.EnsureClientBinary(*binary, false, stderr)
	if err != nil {
		fmt.Fprintf(stderr, "错误: %v\n", err)
		return 1
	}
	spec.ClientBinary = bin
	startCtx, cancel := context.WithTimeout(ctx, 90*time.Second)
	err = sboxRuntime.Create(startCtx, &spec)
	cancel()
	if err != nil {
		fmt.Fprintf(stderr, "错误: %v\n盘已在目标节点就绪，只是沙箱没起来。\n", err)
		return 1
	}
	step("在目标节点起沙箱并挂盘", began)

	fmt.Fprintf(stdout, "沙箱 %s 已切换到 %s（共 %d ms）\n", id, target, time.Since(started).Milliseconds())
	return 0
}

// sboxHandoffStep 对一个存储节点执行一次 push 或 pull，返回传输量摘要；失败时打印可操作的原因。
func sboxHandoffStep(endpoint, verb, diskID string, release, force bool, stderr io.Writer) (summary string, code int) {
	raw, status, err := callDiskHandoff(endpoint, verb, diskID, handoffFlags{release: release, force: force})
	if err != nil {
		fmt.Fprintf(stderr, "错误: 请求存储节点 %s 失败: %v\n", endpoint, err)
		return "", 1
	}
	if status != http.StatusOK {
		fmt.Fprintf(stderr, "错误: 节点 %s: %s\n", endpoint, describeHandoffFailure(raw, status))
		return "", 1
	}
	var res diskHandoffResult
	if json.Unmarshal(raw, &res) != nil {
		return "", 0
	}
	return res.transferSummary(verb), 0
}
