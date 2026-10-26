//go:build e2e

package e2e_test

import (
	"context"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/starsrc/bowl/e2e/testutil"
	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
)

func TestDiskLifecycleSmoke(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	client := testutil.NewStorageClient()

	// 1. CreateDisk: 创建新虚拟磁盘
	diskName := "e2e-smoke-disk"
	initMaxBytes := uint64(10 * 1024 * 1024 * 1024) // 10 GiB

	createResp, err := client.CreateDisk(ctx, connect.NewRequest(&storagev1.CreateDiskRequest{
		Name:     diskName,
		MaxBytes: initMaxBytes,
	}))
	require.NoError(t, err, "CreateDisk 应该成功")
	require.NotNil(t, createResp.Msg.GetDisk(), "返回的 Disk 实体不能为空")

	disk := createResp.Msg.GetDisk()
	require.NotEmpty(t, disk.GetDiskId(), "DiskId 不能为空")
	assert.Equal(t, diskName, disk.GetName())
	assert.Greater(t, disk.GetRootIno(), uint64(0), "新建磁盘 root_ino 必须大于 0")
	assert.Equal(t, initMaxBytes, disk.GetMaxBytes())
	assert.Equal(t, uint64(0), disk.GetUsedBytes(), "初始已用字节数必须为 0")
	assert.Equal(t, uint64(1), disk.GetUsedInodes(), "初始已用 Inode 数为 1（包含根目录 Inode）")
	assert.NotNil(t, disk.GetCreatedAt())
	assert.NotNil(t, disk.GetUpdatedAt())

	// 2. GetDisk: 查询刚创建的磁盘详情
	getResp, err := client.GetDisk(ctx, connect.NewRequest(&storagev1.GetDiskRequest{
		DiskId: disk.GetDiskId(),
	}))
	require.NoError(t, err, "GetDisk 应该成功")
	require.NotNil(t, getResp.Msg.GetDisk())
	assert.Equal(t, disk.GetDiskId(), getResp.Msg.GetDisk().GetDiskId())
	assert.Equal(t, diskName, getResp.Msg.GetDisk().GetName())
	assert.Equal(t, disk.GetRootIno(), getResp.Msg.GetDisk().GetRootIno())
	assert.Equal(t, initMaxBytes, getResp.Msg.GetDisk().GetMaxBytes())

	// 3. UpdateDisk: 扩容与缩容保护 EINVAL 验证
	// 3.1 扩容测试 (10 GiB -> 20 GiB 并重命名)
	expandedMaxBytes := uint64(20 * 1024 * 1024 * 1024) // 20 GiB
	renamed := "e2e-smoke-disk-renamed"

	updateResp, err := client.UpdateDisk(ctx, connect.NewRequest(&storagev1.UpdateDiskRequest{
		DiskId:   disk.GetDiskId(),
		Name:     &renamed,
		MaxBytes: &expandedMaxBytes,
	}))
	require.NoError(t, err, "UpdateDisk 扩容应成功")
	require.NotNil(t, updateResp.Msg.GetDisk())
	assert.Equal(t, renamed, updateResp.Msg.GetDisk().GetName())
	assert.Equal(t, expandedMaxBytes, updateResp.Msg.GetDisk().GetMaxBytes())

	// 3.2 缩容保护验证：通过在磁盘根目录下创建常规文件并扩容其大小，产生 UsedBytes > 0
	createFileResp, err := client.Create(ctx, connect.NewRequest(&storagev1.CreateRequest{
		ParentIno: disk.GetRootIno(),
		Name:      []byte("smoke_quota_file.dat"),
		Mode:      0o644,
	}))
	require.NoError(t, err, "在根目录下创建配额测试文件应成功")
	require.NotNil(t, createFileResp.Msg.GetInode())
	fileIno := createFileResp.Msg.GetInode().GetIno()

	// 调整文件物理大小为 4096 字节，占用磁盘配额
	fileSize := uint64(4096)
	_, err = client.SetAttr(ctx, connect.NewRequest(&storagev1.SetAttrRequest{
		Ino:  fileIno,
		Size: &fileSize,
	}))
	require.NoError(t, err, "调整文件大小至 4096 字节应成功")

	// 缩容至 2048 字节（低于当前已用容量 4096 字节），应触发缩容保护并返回 EINVAL
	shrinkBelowUsage := uint64(2048)
	_, err = client.UpdateDisk(ctx, connect.NewRequest(&storagev1.UpdateDiskRequest{
		DiskId:   disk.GetDiskId(),
		MaxBytes: &shrinkBelowUsage,
	}))
	testutil.RequireConnectError(t, err, connect.CodeFailedPrecondition, "EINVAL")

	// 3.3 边界参数保护：非法 max_bytes = 0 应被静态校验拦截并返回 EINVAL
	invalidZeroBytes := uint64(0)
	_, err = client.UpdateDisk(ctx, connect.NewRequest(&storagev1.UpdateDiskRequest{
		DiskId:   disk.GetDiskId(),
		MaxBytes: &invalidZeroBytes,
	}))
	testutil.RequireConnectError(t, err, connect.CodeInvalidArgument, "EINVAL")

	// 4. StatFS: 查询文件系统统计信息
	statResp, err := client.StatFS(ctx, connect.NewRequest(&storagev1.StatFSRequest{
		Ino: disk.GetRootIno(),
	}))
	require.NoError(t, err, "StatFS 应成功")
	require.NotNil(t, statResp.Msg)
	assert.Equal(t, expandedMaxBytes, statResp.Msg.GetTotalBytes())
	assert.Equal(t, fileSize, statResp.Msg.GetUsedBytes(), "已用字节数应为 4096")
	assert.Equal(t, expandedMaxBytes-fileSize, statResp.Msg.GetAvailBytes())
	assert.Equal(t, uint32(4096), statResp.Msg.GetBsize())
	assert.Equal(t, uint32(4096), statResp.Msg.GetFrsize())
	assert.Equal(t, expandedMaxBytes/4096, statResp.Msg.GetBlocks())
	assert.Equal(t, (expandedMaxBytes-fileSize)/4096, statResp.Msg.GetBfree())

	// 5. DeleteDisk: 级联删除磁盘
	delResp, err := client.DeleteDisk(ctx, connect.NewRequest(&storagev1.DeleteDiskRequest{
		DiskId: disk.GetDiskId(),
	}))
	require.NoError(t, err, "DeleteDisk 应成功")
	require.NotNil(t, delResp)

	// 6. 再次 GetDisk 验证 ENOENT
	_, err = client.GetDisk(ctx, connect.NewRequest(&storagev1.GetDiskRequest{
		DiskId: disk.GetDiskId(),
	}))
	testutil.RequireConnectError(t, err, connect.CodeNotFound, "ENOENT")

	// 再次 DeleteDisk 验证 ENOENT
	_, err = client.DeleteDisk(ctx, connect.NewRequest(&storagev1.DeleteDiskRequest{
		DiskId: disk.GetDiskId(),
	}))
	testutil.RequireConnectError(t, err, connect.CodeNotFound, "ENOENT")
}
