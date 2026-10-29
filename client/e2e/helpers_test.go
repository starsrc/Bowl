//go:build e2e && linux

package e2e_test

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/require"

	"github.com/starsrc/bowl/client/internal/rpc"
	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
	"github.com/starsrc/bowl/gen/go/storage/v1/storagev1connect"
)

// getStorageEndpoint 获取测试环境的 StorageService 接入地址。
func getStorageEndpoint() string {
	ep := os.Getenv("BOWL_STORAGE_ENDPOINT")
	if ep != "" {
		return ep
	}
	return "http://storage-service:9000"
}

// getBowlBin 获取 bowl CLI 二进制文件路径。
func getBowlBin() string {
	bin := os.Getenv("BOWL_BIN")
	if bin != "" {
		return bin
	}
	return "/tmp/bowl"
}

// newTestStorageClient 创建直连 StorageService 的 RPC 客户端。
func newTestStorageClient(t *testing.T) storagev1connect.StorageServiceClient {
	client, err := rpc.NewClientFromConfig(rpc.ClientConfig{
		Endpoint: getStorageEndpoint(),
	})
	require.NoError(t, err, "连接 StorageService 失败")
	return client
}

// createDiskWithRootInoGT1 连续创建磁盘直到获取 root_ino > 1 的磁盘，确保真实验证 1 <-> root_ino 非恒等映射。
func createDiskWithRootInoGT1(t *testing.T, client storagev1connect.StorageServiceClient) *storagev1.Disk {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	for i := 0; i < 5; i++ {
		resp, err := client.CreateDisk(ctx, connect.NewRequest(&storagev1.CreateDiskRequest{
			Name:     fmt.Sprintf("e2e-disk-%d-%d", time.Now().UnixNano(), i),
			MaxBytes: 10 * 1024 * 1024 * 1024, // 10 GiB
		}))
		require.NoError(t, err, "创建磁盘失败")

		disk := resp.Msg.GetDisk()
		if disk.GetRootIno() == 1 {
			_, delErr := client.DeleteDisk(ctx, connect.NewRequest(&storagev1.DeleteDiskRequest{
				DiskId: disk.GetDiskId(),
			}))
			require.NoError(t, delErr, "清理过渡磁盘 (root_ino == 1) 失败")
			continue
		}
		if disk.GetRootIno() > 1 {
			diskID := disk.GetDiskId()
			t.Cleanup(func() {
				cleanCtx, cleanCancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cleanCancel()
				_, delErr := client.DeleteDisk(cleanCtx, connect.NewRequest(&storagev1.DeleteDiskRequest{
					DiskId: diskID,
				}))
				require.NoError(t, delErr, "清理测试磁盘失败: %s", diskID)
				_, getErr := client.GetDisk(cleanCtx, connect.NewRequest(&storagev1.GetDiskRequest{
					DiskId: diskID,
				}))
				require.Error(t, getErr, "删除磁盘后 GetDisk 必须返回错误: %s", diskID)
				require.Equal(t, connect.CodeNotFound, connect.CodeOf(getErr), "删除磁盘后 GetDisk 应返回 CodeNotFound")
			})
			return disk
		}
	}
	t.Fatal("未能在 5 次重试内创建出 root_ino > 1 的磁盘")
	return nil
}

// isMounted 通过读取 /proc/self/mountinfo 确认目标路径是否为已生效的挂载点。
func isMounted(mountpoint string) bool {
	data, err := os.ReadFile("/proc/self/mountinfo")
	if err != nil {
		return false
	}
	target := filepath.Clean(mountpoint)
	lines := strings.Split(string(data), "\n")
	for _, line := range lines {
		fields := strings.Fields(line)
		if len(fields) >= 5 {
			if filepath.Clean(fields[4]) == target {
				return true
			}
		}
	}
	return false
}

// waitForMount 轮询等待挂载点出现在 /proc/self/mountinfo 中。
func waitForMount(mountpoint string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if isMounted(mountpoint) {
			return nil
		}
		time.Sleep(50 * time.Millisecond)
	}
	return fmt.Errorf("挂载点 %s 在 %v 内未完成就绪", mountpoint, timeout)
}

// waitForUnmount 轮询等待挂载点从 /proc/self/mountinfo 中消失。
func waitForUnmount(mountpoint string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if !isMounted(mountpoint) {
			return nil
		}
		time.Sleep(50 * time.Millisecond)
	}
	return fmt.Errorf("挂载点 %s 在 %v 后仍残留在 mountinfo 中", mountpoint, timeout)
}

// startBowlMount 启动 bowl mount -f 前台子进程。
func startBowlMount(t *testing.T, endpoint, diskID, mountpoint string, extraFlags ...string) (*exec.Cmd, *bytes.Buffer, *bytes.Buffer) {
	args := []string{"mount", "-f", "--endpoint", endpoint, diskID, mountpoint}
	args = append(args, extraFlags...)

	cmd := exec.Command(getBowlBin(), args...)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	err := cmd.Start()
	require.NoError(t, err, "启动 bowl mount 进程失败")

	return cmd, &stdout, &stderr
}

// stopBowlMountGracefully 向 bowl mount 前台子进程发送 SIGTERM 并验证优雅退出与无残留卸载。
func stopBowlMountGracefully(t *testing.T, cmd *exec.Cmd, mountpoint string, timeout time.Duration) {
	require.NoError(t, cmd.Process.Signal(syscall.SIGTERM), "发送 SIGTERM 失败")

	done := make(chan error, 1)
	go func() {
		done <- cmd.Wait()
	}()

	select {
	case err := <-done:
		require.NoError(t, err, "bowl mount 进程应以退出码 0 干净退出")
	case <-time.After(timeout):
		_ = cmd.Process.Kill()
		t.Fatalf("bowl mount 进程在 %v 内未响应 SIGTERM 退出", timeout)
	}

	require.NoError(t, waitForUnmount(mountpoint, 5*time.Second), "卸载后 mountinfo 不应残留该挂载点")
}
