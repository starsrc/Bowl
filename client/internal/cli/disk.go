package cli

import (
	"fmt"
	"io"
	"time"

	"github.com/starsrc/bowl/client/internal/rpc"
	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
	"github.com/starsrc/bowl/gen/go/storage/v1/storagev1connect"
)

const diskUsageHelp = `用法:
  bowl disk <subcommand> [flags] [arguments]

可用子命令:
  create      创建新的虚拟磁盘实例
  list        分页列出虚拟磁盘实例
  get         查看指定虚拟磁盘详情
  update      更新虚拟磁盘名称或配额容量 (别名: resize)
  delete      安全删除指定虚拟磁盘实例
  help        查看命令帮助

标志:
  -h, --help  查看帮助信息
`

// diskJSON 定义磁盘实体在 --json 模式下的结构化输出格式。
type diskJSON struct {
	DiskID     string `json:"disk_id"`
	Name       string `json:"name"`
	RootIno    uint64 `json:"root_ino"`
	MaxBytes   uint64 `json:"max_bytes"`
	UsedBytes  uint64 `json:"used_bytes"`
	UsedInodes uint64 `json:"used_inodes"`
	CreatedAt  string `json:"created_at"`
	UpdatedAt  string `json:"updated_at"`
}

func toDiskJSON(d *storagev1.Disk) diskJSON {
	var createdAt, updatedAt string
	if d.GetCreatedAt() != nil {
		createdAt = d.GetCreatedAt().AsTime().UTC().Format(time.RFC3339)
	}
	if d.GetUpdatedAt() != nil {
		updatedAt = d.GetUpdatedAt().AsTime().UTC().Format(time.RFC3339)
	}
	return diskJSON{
		DiskID:     d.GetDiskId(),
		Name:       d.GetName(),
		RootIno:    d.GetRootIno(),
		MaxBytes:   d.GetMaxBytes(),
		UsedBytes:  d.GetUsedBytes(),
		UsedInodes: d.GetUsedInodes(),
		CreatedAt:  createdAt,
		UpdatedAt:  updatedAt,
	}
}

// printDiskDetail 向指定 Writer 打印人类可读的虚拟磁盘详情视图。
func printDiskDetail(w io.Writer, d *storagev1.Disk) {
	dj := toDiskJSON(d)
	fmt.Fprintf(w, "Disk ID:      %s\n", dj.DiskID)
	fmt.Fprintf(w, "Name:         %s\n", dj.Name)
	fmt.Fprintf(w, "Root Ino:     %d\n", dj.RootIno)
	fmt.Fprintf(w, "Max Bytes:    %d (%s)\n", dj.MaxBytes, FormatSize(dj.MaxBytes))
	fmt.Fprintf(w, "Used Bytes:   %d (%s)\n", dj.UsedBytes, FormatSize(dj.UsedBytes))
	fmt.Fprintf(w, "Used Inodes:  %d\n", dj.UsedInodes)
	fmt.Fprintf(w, "Created At:   %s\n", dj.CreatedAt)
	fmt.Fprintf(w, "Updated At:   %s\n", dj.UpdatedAt)
}

// newDiskStorageClient 解析 endpoint 与 token 并创建 StorageServiceClient。
// 若 endpoint 配置非法，返回退出码 2；若创建客户端异常，返回退出码 1。
func newDiskStorageClient(endpoint, token string) (storagev1connect.StorageServiceClient, int, error) {
	resolvedEndpoint, err := rpc.ResolveEndpoint(endpoint)
	if err != nil {
		return nil, 2, fmt.Errorf("无效的 endpoint: %w", err)
	}
	resolvedToken := rpc.ResolveToken(token)

	client, err := rpc.NewClientFromConfig(rpc.ClientConfig{
		Endpoint: resolvedEndpoint,
		Token:    resolvedToken,
	})
	if err != nil {
		return nil, 1, fmt.Errorf("创建 RPC 客户端失败: %w", err)
	}
	return client, 0, nil
}

// executeDisk 路由 bowl disk 控制面子命令族。
func executeDisk(args []string, stdout, stderr io.Writer) int {
	if len(args) == 0 {
		fmt.Fprint(stderr, diskUsageHelp)
		return 2
	}

	subcmd := args[0]
	switch subcmd {
	case "help", "--help", "-h":
		fmt.Fprint(stdout, diskUsageHelp)
		return 0
	case "create":
		return executeDiskCreate(args[1:], stdout, stderr)
	default:
		fmt.Fprintf(stderr, "错误: 未知 disk 子命令 \"%s\"\n\n%s", subcmd, diskUsageHelp)
		return 2
	}
}
