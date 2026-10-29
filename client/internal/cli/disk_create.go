package cli

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"time"

	"connectrpc.com/connect"

	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
)

const diskCreateUsageHelp = `用法:
  bowl disk create --size <size> [flags]

标志:
  --size string        配额容量上限，必填 (支持 B/K/KB/KiB/M/MB/MiB/G/GB/GiB/T/TB/TiB/P/PB/PiB，如 10GiB)
  --name string        虚拟磁盘名称 (可选，最大 255 字节)
  -q, --quiet          静默模式，仅输出新建磁盘的 <disk-id>
  --json               以 JSON 格式输出新建磁盘详情
  --endpoint string    StorageService 基础地址 (默认读取 $BOWL_STORAGE_ENDPOINT 或 http://127.0.0.1:9000)
  --token string       访问令牌 (默认读取 $BOWL_TOKEN)
  -h, --help           查看帮助信息
`

// executeDiskCreate 解析并执行 bowl disk create 命令。
func executeDiskCreate(args []string, stdout, stderr io.Writer) int {
	var (
		sizeRaw  string
		name     string
		quiet    bool
		jsonOut  bool
		endpoint string
		token    string
		help     bool
	)

	var posArgs []string
	for i := 0; i < len(args); i++ {
		arg := args[i]
		if arg == "--" {
			posArgs = append(posArgs, args[i+1:]...)
			break
		}
		if strings.HasPrefix(arg, "-") {
			flagName := strings.TrimLeft(arg, "-")
			hasVal := strings.Contains(flagName, "=")
			var val string
			if hasVal {
				parts := strings.SplitN(flagName, "=", 2)
				flagName = parts[0]
				val = parts[1]
			}
			switch flagName {
			case "h", "help":
				help = true
			case "q", "quiet":
				quiet = true
			case "json":
				jsonOut = true
			case "size":
				if hasVal {
					sizeRaw = val
				} else if i+1 < len(args) {
					i++
					sizeRaw = args[i]
				} else {
					fmt.Fprintln(stderr, "错误: --size 需要指定参数值")
					return 2
				}
			case "name":
				if hasVal {
					name = val
				} else if i+1 < len(args) {
					i++
					name = args[i]
				} else {
					fmt.Fprintln(stderr, "错误: --name 需要指定参数值")
					return 2
				}
			case "endpoint":
				if hasVal {
					endpoint = val
				} else if i+1 < len(args) {
					i++
					endpoint = args[i]
				} else {
					fmt.Fprintln(stderr, "错误: --endpoint 需要指定参数值")
					return 2
				}
			case "token":
				if hasVal {
					token = val
				} else if i+1 < len(args) {
					i++
					token = args[i]
				} else {
					fmt.Fprintln(stderr, "错误: --token 需要指定参数值")
					return 2
				}
			default:
				fmt.Fprintf(stderr, "错误: 未知标志 \"%s\"\n\n%s", arg, diskCreateUsageHelp)
				return 2
			}
		} else {
			posArgs = append(posArgs, arg)
		}
	}

	if help {
		fmt.Fprint(stdout, diskCreateUsageHelp)
		return 0
	}

	if len(posArgs) > 0 {
		fmt.Fprintf(stderr, "错误: create 命令不接受位置参数 (%v)\n\n%s", posArgs, diskCreateUsageHelp)
		return 2
	}

	if quiet && jsonOut {
		fmt.Fprintln(stderr, "错误: -q/--quiet 与 --json 不能同时使用")
		return 2
	}

	if strings.TrimSpace(sizeRaw) == "" {
		fmt.Fprintf(stderr, "错误: 必须指定 --size 参数\n\n%s", diskCreateUsageHelp)
		return 2
	}

	maxBytes, err := ParseSize(sizeRaw)
	if err != nil {
		fmt.Fprintf(stderr, "错误: 无效的 --size 参数: %v\n", err)
		return 2
	}

	if len(name) > 255 {
		fmt.Fprintf(stderr, "错误: 磁盘名称不能超过 255 字节 (当前 %d 字节)\n", len(name))
		return 2
	}
	if strings.Contains(name, "\x00") {
		fmt.Fprintln(stderr, "错误: 磁盘名称不能包含空字节 (NUL)")
		return 2
	}

	client, exitCode, err := newDiskStorageClient(endpoint, token)
	if err != nil {
		fmt.Fprintf(stderr, "错误: %v\n", err)
		return exitCode
	}

	rpcCtx, rpcCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer rpcCancel()

	resp, err := client.CreateDisk(rpcCtx, connect.NewRequest(&storagev1.CreateDiskRequest{
		Name:     name,
		MaxBytes: maxBytes,
	}))
	if err != nil {
		fmt.Fprintf(stderr, "错误: 创建虚拟磁盘失败: %v\n", err)
		return 1
	}

	disk := resp.Msg.GetDisk()
	if disk == nil {
		fmt.Fprintln(stderr, "错误: 服务端返回空的虚拟磁盘元数据")
		return 1
	}

	if quiet {
		fmt.Fprintln(stdout, disk.GetDiskId())
		return 0
	}

	if jsonOut {
		enc := json.NewEncoder(stdout)
		enc.SetIndent("", "  ")
		if err := enc.Encode(toDiskJSON(disk)); err != nil {
			fmt.Fprintf(stderr, "错误: 序列化 JSON 输出失败: %v\n", err)
			return 1
		}
		return 0
	}

	printDiskDetail(stdout, disk)
	return 0
}
