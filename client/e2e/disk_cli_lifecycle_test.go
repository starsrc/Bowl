//go:build e2e && linux

package e2e_test

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"

	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
)

func TestE2E_DiskCLI_FullLifecycleAndSafeDelete(t *testing.T) {
	client := newTestStorageClient(t)
	endpoint := getStorageEndpoint()
	bowlBin := getBowlBin()

	// 1. Create: bowl disk create --endpoint <ep> --size 1GiB --name e2e-lifecycle --json
	createCmd := exec.Command(bowlBin, "disk", "create", "--endpoint", endpoint, "--size", "1GiB", "--name", "e2e-lifecycle", "--json")
	createOut, err := createCmd.CombinedOutput()
	require.NoError(t, err, "bowl disk create 失败: %s", string(createOut))

	var created struct {
		DiskID   string `json:"disk_id"`
		Name     string `json:"name"`
		RootIno  uint64 `json:"root_ino"`
		MaxBytes uint64 `json:"max_bytes"`
	}
	require.NoError(t, json.Unmarshal(createOut, &created), "解析 create JSON 失败: %s", string(createOut))
	require.NotEmpty(t, created.DiskID)
	require.Equal(t, "e2e-lifecycle", created.Name)
	require.Equal(t, uint64(1073741824), created.MaxBytes)
	diskID := created.DiskID

	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, _ = client.DeleteDisk(ctx, connect.NewRequest(&storagev1.DeleteDiskRequest{DiskId: diskID}))
	})

	// 2. Get & List
	getCmd := exec.Command(bowlBin, "disk", "get", "--endpoint", endpoint, diskID, "--json")
	getOut, err := getCmd.CombinedOutput()
	require.NoError(t, err, "bowl disk get 失败: %s", string(getOut))

	var got struct {
		DiskID   string `json:"disk_id"`
		Name     string `json:"name"`
		MaxBytes uint64 `json:"max_bytes"`
	}
	require.NoError(t, json.Unmarshal(getOut, &got))
	require.Equal(t, diskID, got.DiskID)
	require.Equal(t, "e2e-lifecycle", got.Name)

	listCmd := exec.Command(bowlBin, "disk", "list", "--endpoint", endpoint, "--limit", "20", "--json")
	listOut, err := listCmd.CombinedOutput()
	require.NoError(t, err, "bowl disk list 失败: %s", string(listOut))

	var listed struct {
		Disks []struct {
			DiskID string `json:"disk_id"`
			Name   string `json:"name"`
		} `json:"disks"`
	}
	require.NoError(t, json.Unmarshal(listOut, &listed))
	var foundInList bool
	for _, d := range listed.Disks {
		if d.DiskID == diskID {
			foundInList = true
			require.Equal(t, "e2e-lifecycle", d.Name)
			break
		}
	}
	require.True(t, foundInList, "bowl disk list 应包含新建的 disk_id %s", diskID)

	// 3. Mount & Write
	mountpoint := t.TempDir()
	mountCmd := exec.Command(bowlBin, "mount", "--endpoint", endpoint, diskID, mountpoint)
	mountOut, err := mountCmd.CombinedOutput()
	require.NoError(t, err, "bowl mount 失败: %s", string(mountOut))

	daemonPID, err := findBowlDaemonPID(diskID, mountpoint)
	require.NoError(t, err, "查找守护进程 PID 失败")

	testFilePath := filepath.Join(mountpoint, "lifecycle_test.bin")
	payload := bytes.Repeat([]byte("BOWL-E2E-LIFECYCLE-DATA-"), 2731) // ~64 KiB
	payload = payload[:65536]
	f, err := os.OpenFile(testFilePath, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0o644)
	require.NoError(t, err)
	_, err = f.Write(payload)
	require.NoError(t, err)
	require.NoError(t, f.Sync())
	require.NoError(t, f.Close())

	// 等待后台 WatchSession 流在服务端完成注册
	time.Sleep(150 * time.Millisecond)

	// 4. Update (在线扩容成功 & 缩容低于已用空间被拦截)
	updateCmd := exec.Command(bowlBin, "disk", "update", "--endpoint", endpoint, diskID, "--size", "2GiB", "--name", "e2e-resized", "--json")
	updateOut, err := updateCmd.CombinedOutput()
	require.NoError(t, err, "bowl disk update 在线扩容失败: %s", string(updateOut))

	var updated struct {
		DiskID    string `json:"disk_id"`
		Name      string `json:"name"`
		MaxBytes  uint64 `json:"max_bytes"`
		UsedBytes uint64 `json:"used_bytes"`
	}
	require.NoError(t, json.Unmarshal(updateOut, &updated))
	require.Equal(t, diskID, updated.DiskID)
	require.Equal(t, "e2e-resized", updated.Name)
	require.Equal(t, uint64(2147483648), updated.MaxBytes)
	require.GreaterOrEqual(t, updated.UsedBytes, uint64(65536))

	// 缩容低于已用空间（1B < 64KiB）应被拦截并返回非零退出码
	shrinkCmd := exec.Command(bowlBin, "disk", "update", "--endpoint", endpoint, diskID, "--size", "1B")
	shrinkOut, shrinkErr := shrinkCmd.CombinedOutput()
	require.Error(t, shrinkErr, "缩容至已用空间以下必须被拦截: %s", string(shrinkOut))

	// 5. 挂载态删盘拦截（本机快检 + 远端 WatchSession 探活拦截 EBUSY）
	// 5a. 本机 CLI 删盘：被 /proc/self/mountinfo 快检拦截
	localDelCmd := exec.Command(bowlBin, "disk", "delete", "--endpoint", endpoint, diskID, "-y")
	localDelOut, localDelErr := localDelCmd.CombinedOutput()
	require.Error(t, localDelErr, "本机挂载状态下 bowl disk delete 必须被拦截")
	var exitErr *exec.ExitError
	require.ErrorAs(t, localDelErr, &exitErr)
	require.Equal(t, 1, exitErr.ExitCode())
	require.Contains(t, string(localDelOut), "bowl umount")

	// 5b. 模拟异机/远端直接调用服务端 RPC 删盘：服务端通过 WatchSession 发送 Ping 并收到 Pong，返回 FailedPrecondition (EBUSY)
	rpcCtx, rpcCancel := context.WithTimeout(context.Background(), 5*time.Second)
	_, remoteDelErr := client.DeleteDisk(rpcCtx, connect.NewRequest(&storagev1.DeleteDiskRequest{
		DiskId: diskID,
	}))
	rpcCancel()
	require.Error(t, remoteDelErr, "活跃挂载期间远端直接调用 DeleteDisk RPC 必须被服务端探活拦截")
	require.Equal(t, connect.CodeFailedPrecondition, connect.CodeOf(remoteDelErr))
	require.Contains(t, remoteDelErr.Error(), "busy")

	// 验证挂载点内的测试文件依然完好可读
	readBack, err := os.ReadFile(testFilePath)
	require.NoError(t, err)
	require.Equal(t, payload, readBack)

	// 6. 正常卸载（bowl umount）后立即删盘成功（无需等待 60s TTL）
	umountCmd := exec.Command(bowlBin, "umount", mountpoint)
	umountOut, err := umountCmd.CombinedOutput()
	require.NoError(t, err, "bowl umount 失败: %s", string(umountOut))
	require.NoError(t, waitForUnmount(mountpoint, 5*time.Second))
	require.NoError(t, waitForProcessExit(t, daemonPID, 5*time.Second))

	delStart := time.Now()
	delCmd := exec.Command(bowlBin, "disk", "delete", "--endpoint", endpoint, diskID, "-y", "--json")
	delOut, err := delCmd.CombinedOutput()
	delElapsed := time.Since(delStart)
	require.NoError(t, err, "卸载后立即执行 bowl disk delete 失败: %s", string(delOut))
	require.Less(t, delElapsed, 2*time.Second, "正常卸载后删盘无需等待 60s TTL，应在 2s 内完成")

	var delRes struct {
		DiskID  string `json:"disk_id"`
		Deleted bool   `json:"deleted"`
	}
	require.NoError(t, json.Unmarshal(delOut, &delRes))
	require.Equal(t, diskID, delRes.DiskID)
	require.True(t, delRes.Deleted)

	// 再次 get 应返回退出码 1（不存在）
	getAfterDelCmd := exec.Command(bowlBin, "disk", "get", "--endpoint", endpoint, diskID)
	getAfterOut, getAfterErr := getAfterDelCmd.CombinedOutput()
	require.Error(t, getAfterErr)
	require.ErrorAs(t, getAfterErr, &exitErr)
	require.Equal(t, 1, exitErr.ExitCode())
	require.Contains(t, string(getAfterOut), "磁盘不存在")

	// 7. 守护进程异常崩溃（kill -9）后 WatchSession 断连感知与立即删盘成功
	createCmd2 := exec.Command(bowlBin, "disk", "create", "--endpoint", endpoint, "--size", "1GiB", "--name", "e2e-kill9-disk", "-q")
	createOut2, err := createCmd2.CombinedOutput()
	require.NoError(t, err, "创建第二块测试盘失败: %s", string(createOut2))
	disk2ID := string(bytes.TrimSpace(createOut2))
	require.NotEmpty(t, disk2ID)

	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, _ = client.DeleteDisk(ctx, connect.NewRequest(&storagev1.DeleteDiskRequest{DiskId: disk2ID}))
	})

	mountpoint2 := t.TempDir()
	mountCmd2 := exec.Command(bowlBin, "mount", "--endpoint", endpoint, disk2ID, mountpoint2)
	mountOut2, err := mountCmd2.CombinedOutput()
	require.NoError(t, err, "挂载第二块测试盘失败: %s", string(mountOut2))

	daemonPID2, err := findBowlDaemonPID(disk2ID, mountpoint2)
	require.NoError(t, err)

	// 等待 WatchSession 流建立
	time.Sleep(150 * time.Millisecond)

	// 直接发送 SIGKILL (kill -9) 杀死守护进程
	require.NoError(t, syscall.Kill(daemonPID2, syscall.SIGKILL))
	require.NoError(t, waitForProcessExit(t, daemonPID2, 5*time.Second))

	// 清理内核残留挂载点
	_ = unix.Unmount(mountpoint2, unix.MNT_DETACH)
	require.NoError(t, waitForUnmount(mountpoint2, 5*time.Second))

	// 立即（无需等待 60s 租约过期）执行 bowl disk delete
	delCmd2 := exec.Command(bowlBin, "disk", "delete", "--endpoint", endpoint, disk2ID, "-y", "--json")
	delOut2, err := delCmd2.CombinedOutput()
	require.NoError(t, err, "kill -9 后立即删盘应成功: %s", string(delOut2))

	var delRes2 struct {
		DiskID  string `json:"disk_id"`
		Deleted bool   `json:"deleted"`
	}
	require.NoError(t, json.Unmarshal(delOut2, &delRes2))
	require.Equal(t, disk2ID, delRes2.DiskID)
	require.True(t, delRes2.Deleted)
}
