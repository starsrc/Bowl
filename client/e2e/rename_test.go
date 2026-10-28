//go:build e2e && linux

package e2e_test

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"

	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
)

// createRegularFile 辅助函数：通过 unix.Mknod 创建常规空文件。
func createRegularFile(t *testing.T, path string) {
	t.Helper()
	err := unix.Mknod(path, syscall.S_IFREG|0644, 0)
	require.NoError(t, err, "创建常规文件失败: %s", path)
}

// TestE2E_Rename_BasicAndCrossDir 验证同目录改名、跨目录移动、标准覆盖重命名与非法覆盖（ENOTEMPTY/EISDIR/ENOTDIR）。
func TestE2E_Rename_BasicAndCrossDir(t *testing.T) {
	client := newTestStorageClient(t)
	disk := createDiskWithRootInoGT1(t, client)
	mountpoint := t.TempDir()
	endpoint := getStorageEndpoint()

	cmd, stdout, stderr := startBowlMount(t, endpoint, disk.GetDiskId(), mountpoint, "--debug")
	require.NoError(t, waitForMount(mountpoint, 10*time.Second), "挂载失败: stdout=%s stderr=%s", stdout.String(), stderr.String())
	defer stopBowlMountGracefully(t, cmd, mountpoint, 5*time.Second)

	// 1. 同目录改名：在根目录下创建文件并改名
	fileA := filepath.Join(mountpoint, "file_a.txt")
	fileB := filepath.Join(mountpoint, "file_b.txt")
	createRegularFile(t, fileA)

	err := os.Rename(fileA, fileB)
	require.NoError(t, err, "同目录重命名文件应成功")

	_, err = os.Stat(fileA)
	require.True(t, os.IsNotExist(err), "重命名后源文件不应存在")
	_, err = os.Stat(fileB)
	require.NoError(t, err, "重命名后目标文件应存在")

	// 2. 跨目录移动：将文件移入子目录
	subDir1 := filepath.Join(mountpoint, "subdir1")
	require.NoError(t, os.Mkdir(subDir1, 0755), "创建子目录1应成功")

	fileInSub1 := filepath.Join(subDir1, "file_in_sub1.txt")
	err = os.Rename(fileB, fileInSub1)
	require.NoError(t, err, "跨目录移动文件应成功")

	_, err = os.Stat(fileB)
	require.True(t, os.IsNotExist(err), "移动后原根目录文件不应存在")
	_, err = os.Stat(fileInSub1)
	require.NoError(t, err, "移动后目标子目录文件应存在")

	// 3. 跨不同子目录移动
	subDir2 := filepath.Join(mountpoint, "subdir2")
	require.NoError(t, os.Mkdir(subDir2, 0755), "创建子目录2应成功")

	fileInSub2 := filepath.Join(subDir2, "file_in_sub2.txt")
	err = os.Rename(fileInSub1, fileInSub2)
	require.NoError(t, err, "跨子目录移动文件应成功")

	_, err = os.Stat(fileInSub1)
	require.True(t, os.IsNotExist(err), "移动后原子目录文件不应存在")
	_, err = os.Stat(fileInSub2)
	require.NoError(t, err, "移动后新宿主子目录文件应存在")

	// 4. 标准覆盖重命名：覆盖已有常规文件
	createRegularFile(t, fileA)
	err = os.Rename(fileA, fileInSub2)
	require.NoError(t, err, "标准重命名覆盖已有常规文件应成功")
	_, err = os.Stat(fileA)
	require.True(t, os.IsNotExist(err))
	_, err = os.Stat(fileInSub2)
	require.NoError(t, err)

	// 5. 试图用常规文件覆盖目录必须失败（EISDIR 或 EEXIST）
	nonEmptyDir := filepath.Join(mountpoint, "non_empty_dst")
	require.NoError(t, os.Mkdir(nonEmptyDir, 0755))
	createRegularFile(t, filepath.Join(nonEmptyDir, "child.txt"))

	createRegularFile(t, fileA)
	err = os.Rename(fileA, nonEmptyDir)
	require.Error(t, err)
	require.True(t, errors.Is(err, syscall.EISDIR) || errors.Is(err, syscall.EEXIST), "用常规文件覆盖目录应返回 EISDIR 或 EEXIST, 实际: %v", err)

	// 6. 试图用目录覆盖常规文件必须失败（ENOTDIR 或 EEXIST）
	srcDir := filepath.Join(mountpoint, "src_dir")
	require.NoError(t, os.Mkdir(srcDir, 0755))

	err = os.Rename(srcDir, fileA)
	require.Error(t, err)
	require.True(t, errors.Is(err, syscall.ENOTDIR) || errors.Is(err, syscall.EEXIST), "用目录覆盖常规文件应返回 ENOTDIR 或 EEXIST, 实际: %v", err)

	// 7. 试图用目录覆盖非空目录必须返回 ENOTEMPTY (或 EEXIST)
	err = os.Rename(srcDir, nonEmptyDir)
	require.Error(t, err)
	require.True(t, errors.Is(err, syscall.ENOTEMPTY) || errors.Is(err, syscall.EEXIST), "覆盖非空目录应返回 ENOTEMPTY (或 EEXIST)")
}

// TestE2E_Rename_NoReplace 验证 RENAME_NOREPLACE 标志位行为。
func TestE2E_Rename_NoReplace(t *testing.T) {
	client := newTestStorageClient(t)
	disk := createDiskWithRootInoGT1(t, client)
	mountpoint := t.TempDir()
	endpoint := getStorageEndpoint()

	cmd, stdout, stderr := startBowlMount(t, endpoint, disk.GetDiskId(), mountpoint, "--debug")
	require.NoError(t, waitForMount(mountpoint, 10*time.Second), "挂载失败: stdout=%s stderr=%s", stdout.String(), stderr.String())
	defer stopBowlMountGracefully(t, cmd, mountpoint, 5*time.Second)

	fileA := filepath.Join(mountpoint, "noreplace_a.txt")
	fileB := filepath.Join(mountpoint, "noreplace_b.txt")
	createRegularFile(t, fileA)

	// 1. 目标不存在时重命名成功
	err := unix.Renameat2(unix.AT_FDCWD, fileA, unix.AT_FDCWD, fileB, unix.RENAME_NOREPLACE)
	require.NoError(t, err, "RENAME_NOREPLACE 在目标不存在时应成功")
	_, err = os.Stat(fileA)
	require.True(t, os.IsNotExist(err))
	_, err = os.Stat(fileB)
	require.NoError(t, err)

	// 2. 重新创建源文件，再次执行 RENAME_NOREPLACE，目标已存在必须返回 EEXIST
	createRegularFile(t, fileA)
	err = unix.Renameat2(unix.AT_FDCWD, fileA, unix.AT_FDCWD, fileB, unix.RENAME_NOREPLACE)
	require.Error(t, err)
	require.ErrorIs(t, err, syscall.EEXIST, "RENAME_NOREPLACE 在目标已存在时必须返回 EEXIST")

	// 验证源文件和目标文件均保持完整
	_, err = os.Stat(fileA)
	require.NoError(t, err, "冲突失败后源文件应保持存在")
	_, err = os.Stat(fileB)
	require.NoError(t, err, "冲突失败后目标文件应保持存在")

	// 3. 源文件不存在时返回 ENOENT
	missingFile := filepath.Join(mountpoint, "missing.txt")
	dstFile := filepath.Join(mountpoint, "dst.txt")
	err = unix.Renameat2(unix.AT_FDCWD, missingFile, unix.AT_FDCWD, dstFile, unix.RENAME_NOREPLACE)
	require.Error(t, err)
	require.ErrorIs(t, err, syscall.ENOENT, "源文件不存在应返回 ENOENT")
}

// TestE2E_Rename_Exchange 验证 RENAME_EXCHANGE 常规文件与非空目录双向原子原地互换。
func TestE2E_Rename_Exchange(t *testing.T) {
	client := newTestStorageClient(t)
	disk := createDiskWithRootInoGT1(t, client)
	mountpoint := t.TempDir()
	endpoint := getStorageEndpoint()

	cmd, stdout, stderr := startBowlMount(t, endpoint, disk.GetDiskId(), mountpoint, "--debug")
	require.NoError(t, waitForMount(mountpoint, 10*time.Second), "挂载失败: stdout=%s stderr=%s", stdout.String(), stderr.String())
	defer stopBowlMountGracefully(t, cmd, mountpoint, 5*time.Second)

	// 创建常规文件 file_swap
	filePath := filepath.Join(mountpoint, "file_swap")
	createRegularFile(t, filePath)

	// 创建非空目录 dir_swap 并在其中放置子文件
	dirPath := filepath.Join(mountpoint, "dir_swap")
	require.NoError(t, os.Mkdir(dirPath, 0755))
	childPath := filepath.Join(dirPath, "child.txt")
	createRegularFile(t, childPath)

	// 获取互换前的属性
	fileInfoBefore, err := os.Stat(filePath)
	require.NoError(t, err)
	require.False(t, fileInfoBefore.IsDir())

	dirInfoBefore, err := os.Stat(dirPath)
	require.NoError(t, err)
	require.True(t, dirInfoBefore.IsDir())

	// 1. 执行 RENAME_EXCHANGE 原子双向交换
	err = unix.Renameat2(unix.AT_FDCWD, filePath, unix.AT_FDCWD, dirPath, unix.RENAME_EXCHANGE)
	require.NoError(t, err, "RENAME_EXCHANGE 原子互换常规文件与非空目录应成功")

	// 2. 验证互换后的类型与属性
	infoAfterAtFilePath, err := os.Stat(filePath)
	require.NoError(t, err)
	require.True(t, infoAfterAtFilePath.IsDir(), "原文件路径现在应变为目录")

	infoAfterAtDirPath, err := os.Stat(dirPath)
	require.NoError(t, err)
	require.False(t, infoAfterAtDirPath.IsDir(), "原目录路径现在应变为常规文件")

	// 验证互换后目录内部的子项仍完好无损
	swappedChildPath := filepath.Join(filePath, "child.txt")
	_, err = os.Stat(swappedChildPath)
	require.NoError(t, err, "互换后原目录内部子项应正常存在于新目录路径下")

	// 3. 再次互换回原位
	err = unix.Renameat2(unix.AT_FDCWD, filePath, unix.AT_FDCWD, dirPath, unix.RENAME_EXCHANGE)
	require.NoError(t, err, "再次 RENAME_EXCHANGE 应成功恢复")

	infoRestoredFile, err := os.Stat(filePath)
	require.NoError(t, err)
	require.False(t, infoRestoredFile.IsDir())

	infoRestoredDir, err := os.Stat(dirPath)
	require.NoError(t, err)
	require.True(t, infoRestoredDir.IsDir())

	// 4. 任一路径不存在时返回 ENOENT
	missingPath := filepath.Join(mountpoint, "missing_for_exchange")
	err = unix.Renameat2(unix.AT_FDCWD, filePath, unix.AT_FDCWD, missingPath, unix.RENAME_EXCHANGE)
	require.Error(t, err)
	require.ErrorIs(t, err, syscall.ENOENT, "目标不存在时 RENAME_EXCHANGE 必须返回 ENOENT")

	err = unix.Renameat2(unix.AT_FDCWD, missingPath, unix.AT_FDCWD, filePath, unix.RENAME_EXCHANGE)
	require.Error(t, err)
	require.ErrorIs(t, err, syscall.ENOENT, "源不存在时 RENAME_EXCHANGE 必须返回 ENOENT")
}

// TestE2E_Rename_AncestorCycle 验证将父目录移入自身子孙的祖先环拒绝（EINVAL）。
func TestE2E_Rename_AncestorCycle(t *testing.T) {
	client := newTestStorageClient(t)
	disk := createDiskWithRootInoGT1(t, client)
	mountpoint := t.TempDir()
	endpoint := getStorageEndpoint()

	cmd, stdout, stderr := startBowlMount(t, endpoint, disk.GetDiskId(), mountpoint, "--debug")
	require.NoError(t, waitForMount(mountpoint, 10*time.Second), "挂载失败: stdout=%s stderr=%s", stdout.String(), stderr.String())
	defer stopBowlMountGracefully(t, cmd, mountpoint, 5*time.Second)

	// 创建 parent/child/grandchild 结构
	parent := filepath.Join(mountpoint, "ancestor_parent")
	child := filepath.Join(parent, "child")
	grandchild := filepath.Join(child, "grandchild")

	require.NoError(t, os.Mkdir(parent, 0755))
	require.NoError(t, os.Mkdir(child, 0755))
	require.NoError(t, os.Mkdir(grandchild, 0755))

	// 试图将 parent 移入其子孙 grandchild 内部产生环
	targetLoop := filepath.Join(grandchild, "parent_cycle")
	err := os.Rename(parent, targetLoop)
	require.Error(t, err, "将祖先目录移入子孙目录必须失败")
	require.ErrorIs(t, err, syscall.EINVAL, "祖先环检测必须返回 EINVAL")

	// 验证原有目录树完整无损
	_, err = os.Stat(parent)
	require.NoError(t, err)
	_, err = os.Stat(child)
	require.NoError(t, err)
	_, err = os.Stat(grandchild)
	require.NoError(t, err)
	_, err = os.Stat(targetLoop)
	require.True(t, os.IsNotExist(err))
}

// TestE2E_Rename_Whiteout 验证 RENAME_WHITEOUT 标志位被拒绝（EOPNOTSUPP 或 EINVAL）。
func TestE2E_Rename_Whiteout(t *testing.T) {
	client := newTestStorageClient(t)
	disk := createDiskWithRootInoGT1(t, client)
	mountpoint := t.TempDir()
	endpoint := getStorageEndpoint()

	cmd, stdout, stderr := startBowlMount(t, endpoint, disk.GetDiskId(), mountpoint, "--debug")
	require.NoError(t, waitForMount(mountpoint, 10*time.Second), "挂载失败: stdout=%s stderr=%s", stdout.String(), stderr.String())
	defer stopBowlMountGracefully(t, cmd, mountpoint, 5*time.Second)

	srcFile := filepath.Join(mountpoint, "whiteout_src.txt")
	dstFile := filepath.Join(mountpoint, "whiteout_dst.txt")
	createRegularFile(t, srcFile)

	err := unix.Renameat2(unix.AT_FDCWD, srcFile, unix.AT_FDCWD, dstFile, unix.RENAME_WHITEOUT)
	require.Error(t, err, "RENAME_WHITEOUT 必须被拒绝")
	require.True(t, errors.Is(err, syscall.EOPNOTSUPP) || errors.Is(err, syscall.EINVAL),
		"RENAME_WHITEOUT 应返回 EOPNOTSUPP 或 EINVAL，实际错误: %v", err)
}

// TestE2E_Rename_ReadOnly 验证只读模式下 Rename 直接拦截返回 EROFS。
func TestE2E_Rename_ReadOnly(t *testing.T) {
	client := newTestStorageClient(t)
	disk := createDiskWithRootInoGT1(t, client)
	endpoint := getStorageEndpoint()

	// 1. 直连 StorageService 在磁盘中预置文件
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := client.Mknod(ctx, connect.NewRequest(&storagev1.MknodRequest{
		ParentIno: disk.GetRootIno(),
		Name:      []byte("ro_rename_src.txt"),
		Mode:      uint32(syscall.S_IFREG | 0644),
	}))
	require.NoError(t, err, "在服务端预置文件应成功")

	// 2. 以只读模式挂载
	mountpoint := t.TempDir()
	cmd, stdout, stderr := startBowlMount(t, endpoint, disk.GetDiskId(), mountpoint, "--ro", "--debug")
	require.NoError(t, waitForMount(mountpoint, 10*time.Second), "挂载失败: stdout=%s stderr=%s", stdout.String(), stderr.String())
	defer stopBowlMountGracefully(t, cmd, mountpoint, 5*time.Second)

	srcPath := filepath.Join(mountpoint, "ro_rename_src.txt")
	dstPath := filepath.Join(mountpoint, "ro_rename_dst.txt")

	// 3. 执行重命名，必须返回 EROFS
	err = os.Rename(srcPath, dstPath)
	require.Error(t, err, "只读文件系统下重命名必须失败")
	require.ErrorIs(t, err, syscall.EROFS, "只读文件系统必须返回 EROFS")

	// 4. 执行 Renameat2 RENAME_NOREPLACE，同样必须返回 EROFS
	err = unix.Renameat2(unix.AT_FDCWD, srcPath, unix.AT_FDCWD, dstPath, unix.RENAME_NOREPLACE)
	require.Error(t, err)
	require.ErrorIs(t, err, syscall.EROFS, "只读文件系统下 Renameat2 必须返回 EROFS")
}
