package cli

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"connectrpc.com/connect"
	"github.com/google/uuid"
	"google.golang.org/genproto/googleapis/rpc/errdetails"

	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
)

var defaultStdin io.Reader = os.Stdin

const diskDeleteUsageHelp = `用法:
  bowl disk delete <disk-id> [flags]

参数:
  <disk-id>            目标虚拟磁盘 UUID（必填，非 Nil UUID）

标志:
  -y, --yes            跳过交互式确认提示，直接执行删除
  --json               以 JSON 格式输出删除结果
  --endpoint string    StorageService 基础地址 (默认读取 $BOWL_STORAGE_ENDPOINT 或 http://127.0.0.1:9000)
  --token string       访问令牌 (默认读取 $BOWL_TOKEN)
  -h, --help           查看帮助信息
`

type diskDeleteJSON struct {
	DiskID  string `json:"disk_id"`
	Deleted bool   `json:"deleted"`
}

func extractErrorReason(err error) string {
	var connectErr *connect.Error
	if !errors.As(err, &connectErr) {
		return ""
	}
	for _, detail := range connectErr.Details() {
		msg, detailErr := detail.Value()
		if detailErr != nil {
			continue
		}
		if info, ok := msg.(*errdetails.ErrorInfo); ok && info.GetReason() != "" {
			return info.GetReason()
		}
	}
	return ""
}

// executeDiskDelete 解析并执行 bowl disk delete 命令。
func executeDiskDelete(args []string, stdout, stderr io.Writer) int {
	var (
		yes      bool
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
			case "y", "yes":
				yes = true
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
				fmt.Fprintf(stderr, "错误: 未知标志 \"%s\"\n\n%s", arg, diskDeleteUsageHelp)
				return 2
			}
		} else {
			posArgs = append(posArgs, arg)
		}
	}

	if help {
		fmt.Fprint(stdout, diskDeleteUsageHelp)
		return 0
	}

	if len(posArgs) != 1 {
		fmt.Fprintf(stderr, "错误: delete 命令需要且仅需要 1 个位置参数 (<disk-id>)\n\n%s", diskDeleteUsageHelp)
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

	// 步骤 1：本机挂载表快检
	mountpoint, mounted, err := FindDiskMountpoint(diskIDStr)
	if err != nil {
		fmt.Fprintf(stderr, "错误: 检查本机挂载表失败: %v\n", err)
		return 1
	}
	if mounted {
		fmt.Fprintf(stderr, "错误: 磁盘 %s 当前正在本机挂载点 %s 处于挂载状态，请先执行 bowl umount %s 卸载后再删除\n", diskIDStr, mountpoint, mountpoint)
		return 1
	}

	// 步骤 2：交互式二次确认
	if !yes {
		fmt.Fprintf(stderr, "确认删除虚拟磁盘 %s？此操作不可逆 [y/N]: ", diskIDStr)
		line, readErr := bufio.NewReader(defaultStdin).ReadString('\n')
		if readErr != nil && !errors.Is(readErr, io.EOF) {
			fmt.Fprintf(stderr, "错误: 读取终端确认输入失败: %v\n", readErr)
			return 1
		}
		answer := strings.ToLower(strings.TrimSpace(line))
		if answer != "y" && answer != "yes" {
			fmt.Fprintln(stderr, "操作已取消")
			return 1
		}
	}

	// 步骤 3：发起 DeleteDisk RPC（预留服务端 1.5s 探活窗口余量）
	rpcCtx, rpcCancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer rpcCancel()

	_, err = client.DeleteDisk(rpcCtx, connect.NewRequest(&storagev1.DeleteDiskRequest{
		DiskId: diskIDStr,
	}))
	if err != nil {
		var connectErr *connect.Error
		if errors.As(err, &connectErr) {
			switch connectErr.Code() {
			case connect.CodeNotFound:
				fmt.Fprintf(stderr, "错误: 磁盘不存在: %s\n", diskIDStr)
				return 1
			case connect.CodeFailedPrecondition:
				fmt.Fprintf(stderr, "错误: 磁盘 %s 当前仍处于挂载状态（存在活跃客户端会话），请先卸载后再删除\n", diskIDStr)
				return 1
			case connect.CodeInvalidArgument:
				fmt.Fprintf(stderr, "错误: 无效的请求参数: %v\n", err)
				return 2
			}
		}
		if extractErrorReason(err) == "EBUSY" {
			fmt.Fprintf(stderr, "错误: 磁盘 %s 当前仍处于挂载状态（存在活跃客户端会话），请先卸载后再删除\n", diskIDStr)
			return 1
		}
		fmt.Fprintf(stderr, "错误: 删除虚拟磁盘失败: %v\n", err)
		return 1
	}

	if jsonOut {
		enc := json.NewEncoder(stdout)
		enc.SetIndent("", "  ")
		if err := enc.Encode(diskDeleteJSON{
			DiskID:  diskIDStr,
			Deleted: true,
		}); err != nil {
			fmt.Fprintf(stderr, "错误: 序列化 JSON 输出失败: %v\n", err)
			return 1
		}
		return 0
	}

	fmt.Fprintf(stdout, "已删除虚拟磁盘 %s\n", diskIDStr)
	return 0
}
