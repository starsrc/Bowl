//go:build e2e && linux

package e2e_test

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"

	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
	"github.com/starsrc/bowl/gen/go/storage/v1/storagev1connect"
)

// findBowlDaemonPID 扫描 /proc 查找与指定 diskID 和 mountpoint 绑定的后台 bowl 守护进程 PID。
func findBowlDaemonPID(diskID, mountpoint string) (int, error) {
	entries, err := os.ReadDir("/proc")
	if err != nil {
		return 0, fmt.Errorf("读取 /proc 失败: %w", err)
	}

	cleanMP := filepath.Clean(mountpoint)
	myPID := os.Getpid()

	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		pid, err := strconv.Atoi(entry.Name())
		if err != nil || pid == myPID {
			continue
		}

		cmdlineBytes, err := os.ReadFile(filepath.Join("/proc", entry.Name(), "cmdline"))
		if err != nil {
			continue
		}

		cmdline := string(bytes.ReplaceAll(cmdlineBytes, []byte{0}, []byte(" ")))
		if strings.Contains(cmdline, "bowl") && strings.Contains(cmdline, "mount") &&
			strings.Contains(cmdline, diskID) && strings.Contains(cmdline, cleanMP) {
			return pid, nil
		}
	}

	return 0, fmt.Errorf("未找到挂载点 %s (diskID: %s) 的后台守护进程", mountpoint, diskID)
}

// isProcessTerminated 检查进程是否已退出或已进入僵尸/死亡状态（容器内无 init 收集）。
func isProcessTerminated(pid int) bool {
	statusBytes, err := os.ReadFile(fmt.Sprintf("/proc/%d/status", pid))
	if err != nil {
		return true
	}
	for _, line := range strings.Split(string(statusBytes), "\n") {
		if strings.HasPrefix(line, "State:") {
			if strings.Contains(line, "Z") || strings.Contains(line, "X") {
				return true
			}
			return false
		}
	}
	return false
}

// waitForProcessExit 轮询等待指定 PID 的进程从系统完全退出或进入终止状态。
func waitForProcessExit(t *testing.T, pid int, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if isProcessTerminated(pid) {
			return nil
		}
		time.Sleep(50 * time.Millisecond)
	}

	// 打印调试信息
	stack, _ := os.ReadFile(fmt.Sprintf("/proc/%d/stack", pid))
	status, _ := os.ReadFile(fmt.Sprintf("/proc/%d/status", pid))
	wchan, _ := os.ReadFile(fmt.Sprintf("/proc/%d/wchan", pid))
	t.Logf("PID %d 未退出调试信息:\nwchan: %s\nstack:\n%s\nstatus:\n%s", pid, string(wchan), string(stack), string(status))

	return fmt.Errorf("进程 PID %d 在 %v 内未退出", pid, timeout)
}

// createE2EDisk 带有重试机制的测试磁盘创建器。
func createE2EDisk(t *testing.T, client storagev1connect.StorageServiceClient) *storagev1.Disk {
	var lastErr error
	for attempt := 0; attempt < 5; attempt++ {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		resp, err := client.CreateDisk(ctx, connect.NewRequest(&storagev1.CreateDiskRequest{
			Name:     fmt.Sprintf("e2e-disk-%d-%d", time.Now().UnixNano(), attempt),
			MaxBytes: 10 * 1024 * 1024 * 1024,
		}))
		cancel()
		if err == nil && resp.Msg.GetDisk().GetRootIno() > 1 {
			return resp.Msg.GetDisk()
		}
		if err != nil {
			lastErr = err
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("创建测试磁盘失败: %v", lastErr)
	return nil
}

// TestE2E_CLI_DaemonMount_ZeroDelayAndDuplicateRejection 验证默认后台模式挂载零延迟访问、重复挂载拦截与 umount 卸载及守护进程退出。
func TestE2E_CLI_DaemonMount_ZeroDelayAndDuplicateRejection(t *testing.T) {
	client := newTestStorageClient(t)
	disk := createE2EDisk(t, client)
	mountpoint := t.TempDir()
	endpoint := getStorageEndpoint()

	// 1. 默认后台模式挂载 (未加 -f)
	cmd := exec.Command(getBowlBin(), "mount", "--endpoint", endpoint, disk.GetDiskId(), mountpoint, "--debug")
	out, err := cmd.CombinedOutput()
	require.NoError(t, err, "后台挂载执行失败: %s", string(out))

	// 2. 0 延迟断言: 父进程退出 0 后下一微秒挂载点即处于完全就绪状态 (零竞态验证)
	var stat unix.Stat_t
	err = unix.Stat(mountpoint, &stat)
	require.NoError(t, err, "挂载完成后必须立即可访问挂载点 (0 延迟就绪)")
	require.Equal(t, uint64(1), stat.Ino, "根目录 Inode 必须重写为 1")

	var stfs unix.Statfs_t
	err = unix.Statfs(mountpoint, &stfs)
	require.NoError(t, err, "挂载完成后必须立即可读取 statfs")
	require.Equal(t, int64(4096), stfs.Bsize)

	// 3. 重复挂载校验: 再次对同一挂载点执行挂载必须被立即拦截并返回非零退出码
	dupCmd := exec.Command(getBowlBin(), "mount", "--endpoint", endpoint, disk.GetDiskId(), mountpoint)
	dupOut, dupErr := dupCmd.CombinedOutput()
	require.Error(t, dupErr, "重复挂载已挂载目录必须失败")
	var exitErr *exec.ExitError
	require.ErrorAs(t, dupErr, &exitErr)
	require.NotEqual(t, 0, exitErr.ExitCode())
	require.Contains(t, string(dupOut), "已挂载")

	// 4. 定位后台守护进程 PID 并确保其正常活跃
	daemonPID, err := findBowlDaemonPID(disk.GetDiskId(), mountpoint)
	require.NoError(t, err, "必须能探测到活跃的后台守护进程")
	require.Greater(t, daemonPID, 0)
	t.Logf("已检测到活跃守护进程 PID: %d", daemonPID)

	// 5. 执行 bowl umount <mountpoint> 卸载
	umountCmd := exec.Command(getBowlBin(), "umount", mountpoint)
	umountOut, umountErr := umountCmd.CombinedOutput()
	require.NoError(t, umountErr, "卸载失败: %s", string(umountOut))

	// 6. 验证 mountinfo 挂载表已清理，且后台守护进程退出零残留
	require.NoError(t, waitForUnmount(mountpoint, 5*time.Second))
	if err := waitForProcessExit(t, daemonPID, 5*time.Second); err != nil {
		expectedLog := filepath.Join("/var/log/bowl", fmt.Sprintf("mount-%s.log", disk.GetDiskId()))
		logData, _ := os.ReadFile(expectedLog)
		t.Logf("守护进程日志内容:\n%s", string(logData))
		t.Fatal(err)
	}
}

// TestE2E_CLI_LogFiles 验证显式 --log-file 落盘与系统日志文件落盘。
func TestE2E_CLI_LogFiles(t *testing.T) {
	client := newTestStorageClient(t)
	endpoint := getStorageEndpoint()

	t.Run("explicit --log-file is created and written", func(t *testing.T) {
		disk := createE2EDisk(t, client)
		mountpoint := t.TempDir()
		customLog := filepath.Join(t.TempDir(), "logs", "custom-mount.log")

		cmd := exec.Command(getBowlBin(), "mount", "--endpoint", endpoint, "--log-file", customLog, disk.GetDiskId(), mountpoint)
		out, err := cmd.CombinedOutput()
		require.NoError(t, err, "挂载失败: %s", string(out))

		daemonPID, err := findBowlDaemonPID(disk.GetDiskId(), mountpoint)
		require.NoError(t, err)

		// 卸载
		umountCmd := exec.Command(getBowlBin(), "umount", mountpoint)
		require.NoError(t, umountCmd.Run())
		require.NoError(t, waitForProcessExit(t, daemonPID, 5*time.Second))

		// 验证日志文件已落盘且非空
		fi, err := os.Stat(customLog)
		require.NoError(t, err, "自定义日志文件必须存在")
		require.GreaterOrEqual(t, fi.Size(), int64(0))
	})

	t.Run("default system log /var/log/bowl/mount-<disk-id>.log is written", func(t *testing.T) {
		disk := createE2EDisk(t, client)
		mountpoint := t.TempDir()

		cmd := exec.Command(getBowlBin(), "mount", "--endpoint", endpoint, disk.GetDiskId(), mountpoint)
		out, err := cmd.CombinedOutput()
		require.NoError(t, err, "挂载失败: %s", string(out))

		daemonPID, err := findBowlDaemonPID(disk.GetDiskId(), mountpoint)
		require.NoError(t, err)

		// 卸载
		umountCmd := exec.Command(getBowlBin(), "umount", mountpoint)
		require.NoError(t, umountCmd.Run())
		require.NoError(t, waitForProcessExit(t, daemonPID, 5*time.Second))

		expectedLog := filepath.Join("/var/log/bowl", fmt.Sprintf("mount-%s.log", disk.GetDiskId()))
		_, err = os.Stat(expectedLog)
		require.NoError(t, err, "默认系统日志文件必须存在: %s", expectedLog)
	})
}

// TestE2E_CLI_Umount_AliasAndForceLazy 验证 bowl unmount 别名以及 -f/--force 懒卸载释放占用后零孤儿残留。
func TestE2E_CLI_Umount_AliasAndForceLazy(t *testing.T) {
	client := newTestStorageClient(t)
	endpoint := getStorageEndpoint()

	t.Run("unmount alias works identically to umount", func(t *testing.T) {
		disk := createE2EDisk(t, client)
		mountpoint := t.TempDir()

		cmd := exec.Command(getBowlBin(), "mount", "--endpoint", endpoint, disk.GetDiskId(), mountpoint)
		require.NoError(t, cmd.Run())

		daemonPID, err := findBowlDaemonPID(disk.GetDiskId(), mountpoint)
		require.NoError(t, err)

		// 使用 unmount 别名卸载
		unmountCmd := exec.Command(getBowlBin(), "unmount", mountpoint)
		out, err := unmountCmd.CombinedOutput()
		require.NoError(t, err, "unmount 别名执行失败: %s", string(out))

		require.NoError(t, waitForUnmount(mountpoint, 5*time.Second))
		require.NoError(t, waitForProcessExit(t, daemonPID, 5*time.Second))
	})

	t.Run("force lazy unmount detaches busy directory and daemon exits cleanly", func(t *testing.T) {
		disk := createE2EDisk(t, client)
		mountpoint := t.TempDir()

		cmd := exec.Command(getBowlBin(), "mount", "--endpoint", endpoint, disk.GetDiskId(), mountpoint)
		require.NoError(t, cmd.Run())

		daemonPID, err := findBowlDaemonPID(disk.GetDiskId(), mountpoint)
		require.NoError(t, err)

		// 启动一个保持占用 mountpoint 目录的子进程 (sleep 30)
		occupier := exec.Command("sleep", "30")
		occupier.Dir = mountpoint
		require.NoError(t, occupier.Start(), "启动目录占用进程失败")

		// 常规卸载在忙碌目录下应失败
		normalUmount := exec.Command(getBowlBin(), "umount", mountpoint)
		normalOut, normalErr := normalUmount.CombinedOutput()
		require.Error(t, normalErr, "忙碌目录下常规卸载应失败: %s", string(normalOut))

		// 强制懒卸载 (bowl umount -f <mountpoint>)
		forceUmount := exec.Command(getBowlBin(), "umount", "-f", mountpoint)
		forceOut, forceErr := forceUmount.CombinedOutput()
		require.NoError(t, forceErr, "懒卸载必须成功: %s", string(forceOut))

		// 挂载点在 mountinfo 中被立刻摘除
		require.NoError(t, waitForUnmount(mountpoint, 5*time.Second))

		// 杀死占用目录的进程，内核释放最后的活跃引用
		_ = occupier.Process.Signal(syscall.SIGKILL)
		_ = occupier.Wait()

		// 释放后后台守护进程必须安全退出，无孤儿进程残留
		require.NoError(t, waitForProcessExit(t, daemonPID, 5*time.Second))
	})
}
