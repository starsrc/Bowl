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

// MaxDiskNameBytes 定义虚拟磁盘名称的最大字节数上限。
const MaxDiskNameBytes = 255

const diskUpdateUsageHelp = `用法:
  bowl disk update <disk-id> [flags]
  bowl disk resize <disk-id> [flags]

参数:
  <disk-id>            目标虚拟磁盘 UUID（必填，非 Nil UUID）

标志:
  --size string        更新后的配额容量上限 (支持 B/K/KB/KiB/M/MB/MiB/G/GB/GiB/T/TB/TiB/P/PB/PiB，如 20GiB)
  --name string        更新后的虚拟磁盘名称 (最大 255 字节，可传入 "" 清空名称)
  --json               以 JSON 格式输出更新后的虚拟磁盘详情
  --endpoint string    StorageService 基础地址 (默认读取 $BOWL_STORAGE_ENDPOINT 或 http://127.0.0.1:9000)
  --token string       访问令牌 (默认读取 $BOWL_TOKEN)
  -h, --help           查看帮助信息
`

func validateDiskName(name string) error {
	if len(name) > MaxDiskNameBytes {
		return fmt.Errorf("磁盘名称不能超过 %d 字节 (当前 %d 字节)", MaxDiskNameBytes, len(name))
	}
	if strings.Contains(name, "\x00") {
		return fmt.Errorf("磁盘名称不能包含空字节 (NUL)")
	}
	return nil
}

// executeDiskUpdate 解析并执行 bowl disk update / resize 命令。
func executeDiskUpdate(args []string, stdout, stderr io.Writer) int {
	var (
		sizeRaw  *string
		namePtr  *string
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
			case "size":
				if hasVal {
					v := val
					sizeRaw = &v
				} else if i+1 < len(args) {
					i++
					v := args[i]
					sizeRaw = &v
				} else {
					fmt.Fprintln(stderr, "错误: --size 需要指定参数值")
					return 2
				}
			case "name":
				if hasVal {
					v := val
					namePtr = &v
				} else if i+1 < len(args) {
					i++
					v := args[i]
					namePtr = &v
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
				fmt.Fprintf(stderr, "错误: 未知标志 \"%s\"\n\n%s", arg, diskUpdateUsageHelp)
				return 2
			}
		} else {
			posArgs = append(posArgs, arg)
		}
	}

	if help {
		fmt.Fprint(stdout, diskUpdateUsageHelp)
		return 0
	}

	if len(posArgs) != 1 {
		fmt.Fprintf(stderr, "错误: update 命令需要且仅需要 1 个位置参数 (<disk-id>)\n\n%s", diskUpdateUsageHelp)
		return 2
	}

	diskIDStr := posArgs[0]
	parsedUUID, err := uuid.Parse(diskIDStr)
	if err != nil || parsedUUID == uuid.Nil {
		fmt.Fprintf(stderr, "错误: 无效的 disk-id \"%s\"，必须为合法的非 Nil UUID\n", diskIDStr)
		return 2
	}

	var sizePtr *uint64
	if sizeRaw != nil {
		maxBytes, err := ParseSize(*sizeRaw)
		if err != nil {
			fmt.Fprintf(stderr, "错误: 无效的 --size 参数: %v\n", err)
			return 2
		}
		sizePtr = &maxBytes
	}

	if namePtr != nil {
		if err := validateDiskName(*namePtr); err != nil {
			fmt.Fprintf(stderr, "错误: %v\n", err)
			return 2
		}
	}

	if sizePtr == nil && namePtr == nil {
		fmt.Fprintf(stderr, "错误: 必须至少指定 --size 或 --name 之一\n\n%s", diskUpdateUsageHelp)
		return 2
	}

	client, exitCode, err := newDiskStorageClient(endpoint, token)
	if err != nil {
		fmt.Fprintf(stderr, "错误: %v\n", err)
		return exitCode
	}

	rpcCtx, rpcCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer rpcCancel()

	// 第一层：当指定 --size 时，执行客户端 GetDisk 缩容水位线前置预检
	if sizePtr != nil {
		newMaxBytes := *sizePtr
		getResp, err := client.GetDisk(rpcCtx, connect.NewRequest(&storagev1.GetDiskRequest{
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

		currentDisk := getResp.Msg.GetDisk()
		if currentDisk == nil {
			fmt.Fprintln(stderr, "错误: 服务端返回空的虚拟磁盘元数据")
			return 1
		}

		if newMaxBytes < currentDisk.GetUsedBytes() {
			fmt.Fprintf(
				stderr,
				"错误: 目标容量 %d (%s) 小于当前已用容量 %d (%s)，不允许缩容至已用水位以下\n",
				newMaxBytes, FormatSize(newMaxBytes),
				currentDisk.GetUsedBytes(), FormatSize(currentDisk.GetUsedBytes()),
			)
			return 1
		}
	}

	// 第二层：调用 UpdateDisk 由服务端事务终审
	updateResp, err := client.UpdateDisk(rpcCtx, connect.NewRequest(&storagev1.UpdateDiskRequest{
		DiskId:   diskIDStr,
		Name:     namePtr,
		MaxBytes: sizePtr,
	}))
	if err != nil {
		var connectErr *connect.Error
		if errors.As(err, &connectErr) {
			switch connectErr.Code() {
			case connect.CodeNotFound:
				fmt.Fprintf(stderr, "错误: 磁盘不存在: %s\n", diskIDStr)
				return 1
			case connect.CodeFailedPrecondition:
				fmt.Fprintf(stderr, "错误: 缩容失败，目标容量小于磁盘当前已用容量: %v\n", err)
				return 1
			}
		}
		fmt.Fprintf(stderr, "错误: 更新磁盘元数据失败: %v\n", err)
		return 1
	}

	updatedDisk := updateResp.Msg.GetDisk()
	if updatedDisk == nil {
		fmt.Fprintln(stderr, "错误: 服务端返回空的虚拟磁盘元数据")
		return 1
	}

	if jsonOut {
		enc := json.NewEncoder(stdout)
		enc.SetIndent("", "  ")
		if err := enc.Encode(toDiskJSON(updatedDisk)); err != nil {
			fmt.Fprintf(stderr, "错误: 序列化 JSON 输出失败: %v\n", err)
			return 1
		}
		return 0
	}

	printDiskDetail(stdout, updatedDisk)
	return 0
}
