package cli

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strconv"
	"strings"
	"text/tabwriter"
	"time"

	"connectrpc.com/connect"

	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
)

const diskListUsageHelp = `用法:
  bowl disk list [flags]

标志:
  --limit int          单页最大返回条数，取值范围 [1, 1000] (默认 50)
  --page-token string  游标分页令牌
  --json               以 JSON 格式输出虚拟磁盘列表与下一页游标
  --endpoint string    StorageService 基础地址 (默认读取 $BOWL_STORAGE_ENDPOINT 或 http://127.0.0.1:9000)
  --token string       访问令牌 (默认读取 $BOWL_TOKEN)
  -h, --help           查看帮助信息
`

type diskListJSON struct {
	Disks         []diskJSON `json:"disks"`
	NextPageToken string     `json:"next_page_token"`
}

// executeDiskList 解析并执行 bowl disk list 命令。
func executeDiskList(args []string, stdout, stderr io.Writer) int {
	var (
		limitRaw  string
		limitSet  bool
		pageToken string
		jsonOut   bool
		endpoint  string
		token     string
		help      bool
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
			case "limit":
				limitSet = true
				if hasVal {
					limitRaw = val
				} else if i+1 < len(args) {
					i++
					limitRaw = args[i]
				} else {
					fmt.Fprintln(stderr, "错误: --limit 需要指定参数值")
					return 2
				}
			case "page-token":
				if hasVal {
					pageToken = val
				} else if i+1 < len(args) {
					i++
					pageToken = args[i]
				} else {
					fmt.Fprintln(stderr, "错误: --page-token 需要指定参数值")
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
				fmt.Fprintf(stderr, "错误: 未知标志 \"%s\"\n\n%s", arg, diskListUsageHelp)
				return 2
			}
		} else {
			posArgs = append(posArgs, arg)
		}
	}

	if help {
		fmt.Fprint(stdout, diskListUsageHelp)
		return 0
	}

	if len(posArgs) > 0 {
		fmt.Fprintf(stderr, "错误: list 命令不接受位置参数 (%v)\n\n%s", posArgs, diskListUsageHelp)
		return 2
	}

	var pageSize uint32
	if limitSet {
		parsedLimit, err := strconv.Atoi(limitRaw)
		if err != nil || parsedLimit < 1 || parsedLimit > 1000 {
			fmt.Fprintf(stderr, "错误: 无效的 --limit 参数 \"%s\"，必须为 [1, 1000] 范围内的整数\n", limitRaw)
			return 2
		}
		pageSize = uint32(parsedLimit)
	}

	client, exitCode, err := newDiskStorageClient(endpoint, token)
	if err != nil {
		fmt.Fprintf(stderr, "错误: %v\n", err)
		return exitCode
	}

	rpcCtx, rpcCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer rpcCancel()

	resp, err := client.ListDisks(rpcCtx, connect.NewRequest(&storagev1.ListDisksRequest{
		PageSize:  pageSize,
		PageToken: pageToken,
	}))
	if err != nil {
		fmt.Fprintf(stderr, "错误: 列出虚拟磁盘失败: %v\n", err)
		return 1
	}

	disks := resp.Msg.GetDisks()
	nextPageToken := resp.Msg.GetNextPageToken()

	if jsonOut {
		items := make([]diskJSON, 0, len(disks))
		for _, d := range disks {
			items = append(items, toDiskJSON(d))
		}
		out := diskListJSON{
			Disks:         items,
			NextPageToken: nextPageToken,
		}
		enc := json.NewEncoder(stdout)
		enc.SetIndent("", "  ")
		if err := enc.Encode(out); err != nil {
			fmt.Fprintf(stderr, "错误: 序列化 JSON 输出失败: %v\n", err)
			return 1
		}
		return 0
	}

	tw := tabwriter.NewWriter(stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "DISK ID\tNAME\tUSED\tMAX\tUSAGE%\tINODES\tCREATED AT")
	for _, d := range disks {
		dj := toDiskJSON(d)
		var usagePct float64
		if dj.MaxBytes > 0 {
			usagePct = float64(dj.UsedBytes) * 100.0 / float64(dj.MaxBytes)
		}
		fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%.2f%%\t%d\t%s\n",
			dj.DiskID,
			dj.Name,
			FormatSize(dj.UsedBytes),
			FormatSize(dj.MaxBytes),
			usagePct,
			dj.UsedInodes,
			dj.CreatedAt,
		)
	}
	_ = tw.Flush()

	if nextPageToken != "" {
		fmt.Fprintf(stdout, "\nNext Page Token: %s\n", nextPageToken)
	}

	return 0
}
