//go:build e2e && linux

package e2e_test

import (
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

// TestE2E_Mount_ValidationFailures 验证非法 UUID、Nil UUID 与不存在的 Disk 快速失败并返回非零退出码。
func TestE2E_Mount_ValidationFailures(t *testing.T) {
	mountpoint := t.TempDir()
	endpoint := getStorageEndpoint()

	t.Run("invalid disk uuid returns exit code 2", func(t *testing.T) {
		cmd := exec.Command(getBowlBin(), "mount", "-f", "--endpoint", endpoint, "invalid-uuid-format", mountpoint)
		output, err := cmd.CombinedOutput()
		require.Error(t, err, "非法 UUID 应失败")
		var exitErr *exec.ExitError
		require.ErrorAs(t, err, &exitErr)
		require.Equal(t, 2, exitErr.ExitCode(), "非法 UUID 退出码必须为 2")
		require.Contains(t, string(output), "UUID")
	})

	t.Run("nil uuid returns exit code 2", func(t *testing.T) {
		nilUUID := uuid.Nil.String()
		cmd := exec.Command(getBowlBin(), "mount", "-f", "--endpoint", endpoint, nilUUID, mountpoint)
		output, err := cmd.CombinedOutput()
		require.Error(t, err, "Nil UUID 应失败")
		var exitErr *exec.ExitError
		require.ErrorAs(t, err, &exitErr)
		require.Equal(t, 2, exitErr.ExitCode(), "Nil UUID 退出码必须为 2")
		require.Contains(t, string(output), "UUID")
	})

	t.Run("nonexistent disk returns exit code 1", func(t *testing.T) {
		randomUUID := uuid.New().String()
		cmd := exec.Command(getBowlBin(), "mount", "-f", "--endpoint", endpoint, randomUUID, mountpoint)
		output, err := cmd.CombinedOutput()
		require.Error(t, err, "不存在的 Disk 应失败")
		var exitErr *exec.ExitError
		require.ErrorAs(t, err, &exitErr)
		require.Equal(t, 1, exitErr.ExitCode(), "不存在的 Disk 退出码必须为 1")
		require.Contains(t, string(output), "不存在")
	})
}

// TestE2E_Mount_TracerBullet 完整验证 root_ino > 1 磁盘挂载、根目录 Stat (Ino == 1)、Statfs 配额、子项 ENOENT 与 SIGTERM 优雅卸载。
func TestE2E_Mount_TracerBullet(t *testing.T) {
	client := newTestStorageClient(t)
	disk := createDiskWithRootInoGT1(t, client)
	t.Logf("已创建测试磁盘: id=%s, root_ino=%d, max_bytes=%d", disk.GetDiskId(), disk.GetRootIno(), disk.GetMaxBytes())
	require.Greater(t, disk.GetRootIno(), uint64(1), "必须确保远端 root_ino > 1 以真实检验 1 <-> root_ino 映射")

	mountpoint := t.TempDir()
	endpoint := getStorageEndpoint()

	// 1. 启动 bowl mount -f 前台挂载子进程
	cmd, stdout, stderr := startBowlMount(t, endpoint, disk.GetDiskId(), mountpoint, "--debug")

	// 2. 轮询等待挂载点生效
	err := waitForMount(mountpoint, 10*time.Second)
	if err != nil {
		t.Logf("stdout: %s", stdout.String())
		t.Logf("stderr: %s", stderr.String())
		_ = cmd.Process.Kill()
		t.Fatalf("等待挂载就绪失败: %v", err)
	}
	t.Logf("挂载点已就绪: %s", mountpoint)

	// 3. 验证根目录 Stat: 内核观察到的 Ino 必须被重写为 1 (fuse.FUSE_ROOT_ID)
	var stat unix.Stat_t
	err = unix.Stat(mountpoint, &stat)
	require.NoError(t, err, "根目录 unix.Stat 必须成功")
	require.Equal(t, uint64(1), stat.Ino, "服务端 root_ino (%d) 必须映射为内核根节点 1", disk.GetRootIno())
	require.Equal(t, uint32(syscall.S_IFDIR), stat.Mode&syscall.S_IFMT, "根目录类型必须为目录")

	// 4. 验证不存在的子路径 Stat: 必须返回 ENOENT (os.ErrNotExist)
	nonExistentPath := filepath.Join(mountpoint, "nonexistent-bullet.tmp")
	_, err = os.Stat(nonExistentPath)
	require.Error(t, err, "访问不存在的文件必须报错")
	require.ErrorIs(t, err, os.ErrNotExist, "错误必须为 ENOENT")

	// 5. 验证 unix.Statfs: 精确反映 4096 字节块配额、可用空间与已用 Inode 数
	var stfs unix.Statfs_t
	err = unix.Statfs(mountpoint, &stfs)
	require.NoError(t, err, "unix.Statfs 必须成功")
	require.Equal(t, int64(4096), stfs.Bsize, "块大小固定为 4096 字节")
	expectedBlocks := disk.GetMaxBytes() / 4096
	require.Equal(t, expectedBlocks, uint64(stfs.Blocks), "Blocks 必须匹配磁盘 max_bytes / 4096")
	require.Greater(t, stfs.Bavail, uint64(0), "可用块数必须大于 0")
	require.GreaterOrEqual(t, stfs.Files, uint64(1), "已用 Inode 数至少为 1 (根目录)")

	// 6. 发送 SIGTERM 优雅卸载并断言退出码 0 且无残留
	stopBowlMountGracefully(t, cmd, mountpoint, 10*time.Second)
	t.Logf("优雅卸载完成，验证退出码为 0 且 mountinfo 无残留")
}
