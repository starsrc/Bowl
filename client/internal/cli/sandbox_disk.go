package cli

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"connectrpc.com/connect"

	"github.com/starsrc/bowl/client/internal/rpc"
	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
	"github.com/starsrc/bowl/gen/go/storage/v1/storagev1connect"
)

// sboxDiskClient 解析 endpoint 并创建 StorageService 客户端。
func sboxDiskClient(endpoint, token string, stderr io.Writer) (resolved string, client storagev1connect.StorageServiceClient, code int) {
	resolvedEndpoint, err := rpc.ResolveEndpoint(endpoint)
	if err != nil {
		fmt.Fprintf(stderr, "错误: 无效的 endpoint: %v\n", err)
		return "", nil, 2
	}
	client, code, err = newDiskStorageClient(resolvedEndpoint, token)
	if err != nil {
		fmt.Fprintf(stderr, "错误: %v\n", err)
		return "", nil, code
	}
	return resolvedEndpoint, client, 0
}

// sboxCreateDisk 按 from/size 派生或新建磁盘，返回磁盘 ID。
func sboxCreateDisk(
	ctx context.Context,
	client storagev1connect.StorageServiceClient,
	from, size, name string,
) (string, *diskCLIError) {
	if from != "" {
		resp, err := client.ForkDisk(ctx, connect.NewRequest(&storagev1.ForkDiskRequest{
			SourceDiskId: from,
			Name:         name,
		}))
		if err != nil {
			return "", mapDiskRPCError(err, diskRPCErrorPolicy{diskID: from, defaultOp: "分叉沙箱磁盘失败"})
		}
		return resp.Msg.GetDisk().GetDiskId(), nil
	}
	maxBytes, err := ParseSize(size)
	if err != nil {
		return "", &diskCLIError{exitCode: 2, msg: fmt.Sprintf("无效的 --size 参数: %v", err), cause: err}
	}
	if err = validateDiskName(name); err != nil {
		return "", &diskCLIError{exitCode: 2, msg: err.Error(), cause: err}
	}
	resp, err := client.CreateDisk(ctx, connect.NewRequest(&storagev1.CreateDiskRequest{
		Name:     name,
		MaxBytes: maxBytes,
	}))
	if err != nil {
		return "", mapDiskRPCError(err, diskRPCErrorPolicy{defaultOp: "创建沙箱磁盘失败"})
	}
	return resp.Msg.GetDisk().GetDiskId(), nil
}

// sboxDropDisk 在启动失败后尽力回收刚建的磁盘，错误只忽略：调用方已经在报错了。
func sboxDropDisk(client storagev1connect.StorageServiceClient, diskID string) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	_, _ = client.DeleteDisk(ctx, connect.NewRequest(&storagev1.DeleteDiskRequest{DiskId: diskID}))
}

// sboxDeleteDisk 删磁盘，并容忍容器刚被删除时挂载租约尚未过期导致的 EBUSY。
func sboxDeleteDisk(endpoint, token string, stderr io.Writer, diskID string) int {
	client, code, err := newDiskStorageClient(endpoint, token)
	if err != nil {
		fmt.Fprintf(stderr, "错误: %v\n", err)
		return code
	}
	for range 6 {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		_, err := client.DeleteDisk(ctx, connect.NewRequest(&storagev1.DeleteDiskRequest{DiskId: diskID}))
		cancel()
		if err == nil {
			return 0
		}
		if !isEBUSY(err) {
			fmt.Fprintf(stderr, "错误: 删除磁盘失败: %v\n", err)
			return 1
		}
		time.Sleep(2 * time.Second)
	}
	fmt.Fprintf(stderr, "错误: 磁盘 %s 仍被挂载会话占用，稍后用 bowl disk delete %s 重试\n", diskID, diskID)
	return 1
}

func isEBUSY(err error) bool {
	if connErr, ok := errors.AsType[*connect.Error](err); ok {
		return strings.Contains(connErr.Message(), "EBUSY") || strings.Contains(connErr.Message(), "still mounted")
	}
	return false
}

// --- 命令行辅助 ---
