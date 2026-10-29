package cli

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"connectrpc.com/connect"
	"github.com/google/uuid"

	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
)

const diskGetUsageHelp = `用法:
  bowl disk get <disk-id> [flags]

参数:
  <disk-id>            目标虚拟磁盘 UUID（必填，非 Nil UUID）

标志:
  --json               以 JSON 格式输出虚拟磁盘详情
  --endpoint string    StorageService 基础地址 (默认读取 $BOWL_STORAGE_ENDPOINT 或 http://127.0.0.1:9000)
  --token string       访问令牌 (默认读取 $BOWL_TOKEN)
  -h, --help           查看帮助信息
`

// executeDiskGet 解析并执行 bowl disk get 命令。
func executeDiskGet(args []string, stdout, stderr io.Writer) int {
	var (
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
			case "json":
				jsonOut = true
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
				fmt.Fprintf(stderr, "错误: 未知标志 \"%s\"\n\n%s", arg, diskGetUsageHelp)
				return 2
			}
		} else {
			posArgs = append(posArgs, arg)
		}
	}

	if help {
		fmt.Fprint(stdout, diskGetUsageHelp)
		return 0
	}

	if len(posArgs) != 1 {
		fmt.Fprintf(stderr, "错误: get 命令需要且仅需要 1 个位置参数 (<disk-id>)\n\n%s", diskGetUsageHelp)
		return 2
	}

	diskIDStr := posArgs[0]
	parsedUUID, err := uuid.Parse(diskIDStr)
	if err != nil || parsedUUID == uuid.Nil {
		fmt.Fprintf(stderr, "错误: 无效的 disk-id \"%s\"，必须为合法的非 Nil UUID\n", diskIDStr)
		return 2
	}

	client, exitCode, err := newDiskStorageClient(endpoint, token)
	if err != nil {
		fmt.Fprintf(stderr, "错误: %v\n", err)
		return exitCode
	}

	rpcCtx, rpcCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer rpcCancel()

	resp, err := client.GetDisk(rpcCtx, connect.NewRequest(&storagev1.GetDiskRequest{
		DiskId: diskIDStr,
	}))
	if err != nil {
		var connectErr *connect.Error
		if errors.As(err, &connectErr) && connectErr.Code() == connect.CodeNotFound {
			fmt.Fprintf(stderr, "错误: 磁盘不存在: %s\n", diskIDStr)
		} else {
			fmt.Fprintf(stderr, "错误: 获取磁盘元数据失败: %v\n", err)
		}
		return 1
	}

	disk := resp.Msg.GetDisk()
	if disk == nil {
		fmt.Fprintln(stderr, "错误: 服务端返回空的虚拟磁盘元数据")
		return 1
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
