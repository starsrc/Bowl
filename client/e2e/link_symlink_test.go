//go:build e2e && linux

package e2e_test

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"

	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
)

// TestE2E_Symlink_Readlink 验证符号链接创建、相对/绝对路径、长路径与非UTF8字节读取、悬空符号链接及对非符号链接执行 Readlink 返回 EINVAL。
func TestE2E_Symlink_Readlink(t *testing.T) {
	client := newTestStorageClient(t)
	disk := createDiskWithRootInoGT1(t, client)
	mountpoint := t.TempDir()
	endpoint := getStorageEndpoint()

	cmd, stdout, stderr := startBowlMount(t, endpoint, disk.GetDiskId(), mountpoint, "--debug")
	require.NoError(t, waitForMount(mountpoint, 10*time.Second), "挂载失败: stdout=%s stderr=%s", stdout.String(), stderr.String())
	defer stopBowlMountGracefully(t, cmd, mountpoint, 5*time.Second)

	// 1. 创建常规目标文件用于链接验证
	regPath := filepath.Join(mountpoint, "real_file.txt")
	err := unix.Mknod(regPath, syscall.S_IFREG|0644, 0)
	require.NoError(t, err, "mknod 普通文件失败")

	var regStat unix.Stat_t
	err = unix.Stat(regPath, &regStat)
	require.NoError(t, err)

	// 2. 验证相对路径符号链接创建与读取
	symRel := filepath.Join(mountpoint, "sym_rel")
	err = os.Symlink("real_file.txt", symRel)
	require.NoError(t, err, "创建相对路径符号链接失败")

	// Lstat 验证符号链接属性与 0777 权限
	var relLstat unix.Stat_t
	err = unix.Lstat(symRel, &relLstat)
	require.NoError(t, err)
	require.Equal(t, uint32(syscall.S_IFLNK), relLstat.Mode&syscall.S_IFMT, "文件类型必须为 S_IFLNK")
	require.Equal(t, uint32(0777), relLstat.Mode&0777, "符号链接权限位必须为 0777")

	// Readlink 验证相对路径目标
	targetRel, err := os.Readlink(symRel)
	require.NoError(t, err)
	require.Equal(t, "real_file.txt", targetRel)

	// 跟随符号链接 Stat 验证指向原文件
	var statFollowed unix.Stat_t
	err = unix.Stat(symRel, &statFollowed)
	require.NoError(t, err)
	require.Equal(t, regStat.Ino, statFollowed.Ino, "跟随符号链接访问得到的 Inode 必须与原文件一致")

	// 3. 验证悬空符号链接（Dangling Symlink，指向不存在的目标）
	symDangling := filepath.Join(mountpoint, "sym_dangling")
	err = os.Symlink("/nonexistent/target/path/to/ghost", symDangling)
	require.NoError(t, err, "创建悬空符号链接必须成功")

	var danglingLstat unix.Stat_t
	err = unix.Lstat(symDangling, &danglingLstat)
	require.NoError(t, err, "悬空符号链接 Lstat 必须成功")
	require.Equal(t, uint32(syscall.S_IFLNK), danglingLstat.Mode&syscall.S_IFMT)

	targetDangling, err := os.Readlink(symDangling)
	require.NoError(t, err)
	require.Equal(t, "/nonexistent/target/path/to/ghost", targetDangling)

	// Stat 跟随悬空符号链接必须返回 ENOENT
	err = unix.Stat(symDangling, &statFollowed)
	require.Error(t, err)
	require.ErrorIs(t, err, syscall.ENOENT, "跟随悬空符号链接必须返回 ENOENT")

	// 4. 验证高达 4095 字节的长目标路径符号链接
	longTarget := strings.Repeat("x", 4095)
	symLong := filepath.Join(mountpoint, "sym_long")
	err = os.Symlink(longTarget, symLong)
	require.NoError(t, err, "创建 4095 字节长路径符号链接失败")

	targetLong, err := os.Readlink(symLong)
	require.NoError(t, err)
	require.Equal(t, longTarget, targetLong, "4095 字节长路径符号链接内容必须无损返回")
	require.Equal(t, 4095, len(targetLong))

	// 5. 验证包含非 UTF-8 任意非空字节的目标路径（无损传输）
	rawBytes := []byte{'r', 'a', 'w', '_', 0xff, 0xfe, 0x80, '_', 'p', 'a', 't', 'h'}
	symNonUTF8 := filepath.Join(mountpoint, "sym_non_utf8")
	err = unix.Symlink(string(rawBytes), symNonUTF8)
	require.NoError(t, err, "创建非 UTF-8 符号链接失败")

	buf := make([]byte, 256)
	n, err := unix.Readlink(symNonUTF8, buf)
	require.NoError(t, err)
	require.Equal(t, rawBytes, buf[:n], "非 UTF-8 字节目标必须原样无损读回")

	// 6. 验证对非符号链接执行 Readlink 返回 EINVAL
	_, err = unix.Readlink(regPath, buf)
	require.Error(t, err)
	require.ErrorIs(t, err, syscall.EINVAL, "对普通文件执行 Readlink 必须返回 EINVAL")

	// 7. 验证符号链接同名冲突返回 EEXIST
	err = os.Symlink("conflict_target", regPath)
	require.Error(t, err)
	require.ErrorIs(t, err, syscall.EEXIST, "与已有文件同名创建符号链接必须返回 EEXIST")
}

// TestE2E_Link_Hardlink 验证同盘多硬链接创建、Nlink 递增、解链后剩余硬链接存活与 Ino 一致性，以及目录硬链接 EPERM 与同名冲突 EEXIST。
func TestE2E_Link_Hardlink(t *testing.T) {
	client := newTestStorageClient(t)
	disk := createDiskWithRootInoGT1(t, client)
	mountpoint := t.TempDir()
	endpoint := getStorageEndpoint()

	cmd, stdout, stderr := startBowlMount(t, endpoint, disk.GetDiskId(), mountpoint, "--debug")
	require.NoError(t, waitForMount(mountpoint, 10*time.Second), "挂载失败: stdout=%s stderr=%s", stdout.String(), stderr.String())
	defer stopBowlMountGracefully(t, cmd, mountpoint, 5*time.Second)

	// 1. 创建源常规文件
	origPath := filepath.Join(mountpoint, "orig_file.txt")
	err := unix.Mknod(origPath, syscall.S_IFREG|0644, 0)
	require.NoError(t, err, "创建初始源文件失败")

	var origStat unix.Stat_t
	err = unix.Stat(origPath, &origStat)
	require.NoError(t, err)
	require.Equal(t, uint64(1), uint64(origStat.Nlink), "新建文件初始 nlink 必须为 1")
	origIno := origStat.Ino

	// 2. 在根目录下创建第 1 个硬链接，验证 nlink 递增为 2 且 Ino 一致
	link1 := filepath.Join(mountpoint, "link1.txt")
	err = os.Link(origPath, link1)
	require.NoError(t, err, "创建第 1 个硬链接失败")

	var stat1 unix.Stat_t
	err = unix.Stat(link1, &stat1)
	require.NoError(t, err)
	require.Equal(t, origIno, stat1.Ino, "硬链接 Inode 编号必须与源文件完全相同")
	require.Equal(t, uint64(2), uint64(stat1.Nlink), "创建第 1 个硬链接后 nlink 必须递增为 2")

	// 再次检查原文件的 stat，nlink 亦同步为 2
	err = unix.Stat(origPath, &origStat)
	require.NoError(t, err)
	require.Equal(t, uint64(2), uint64(origStat.Nlink))

	// 3. 在非根子目录下创建第 2 个硬链接，验证跨目录同盘硬链接创建与 nlink 递增为 3
	subDir := filepath.Join(mountpoint, "subdir")
	err = os.Mkdir(subDir, 0755)
	require.NoError(t, err)

	link2 := filepath.Join(subDir, "link2.txt")
	err = os.Link(origPath, link2)
	require.NoError(t, err, "在子目录下创建第 2 个硬链接失败")

	var stat2 unix.Stat_t
	err = unix.Stat(link2, &stat2)
	require.NoError(t, err)
	require.Equal(t, origIno, stat2.Ino)
	require.Equal(t, uint64(3), uint64(stat2.Nlink), "创建第 2 个硬链接后 nlink 必须递增为 3")

	// 4. 删除原文件 orig_file.txt，验证剩余硬链接依然存活可读且 nlink 递减为 2
	err = os.Remove(origPath)
	require.NoError(t, err, "删除原文件失败")

	// 原文件路径已不存在
	err = unix.Stat(origPath, &origStat)
	require.ErrorIs(t, err, syscall.ENOENT)

	// link1 仍然存活
	err = unix.Stat(link1, &stat1)
	require.NoError(t, err, "删除原文件后 link1 必须仍然正常存活")
	require.Equal(t, origIno, stat1.Ino)
	require.Equal(t, uint64(2), uint64(stat1.Nlink), "原文件解链后 link1 的 nlink 必须变为 2")

	// link2 亦存活
	err = unix.Stat(link2, &stat2)
	require.NoError(t, err)
	require.Equal(t, origIno, stat2.Ino)
	require.Equal(t, uint64(2), uint64(stat2.Nlink))

	// 5. 删除 link1，验证 link2 的 nlink 变为 1
	err = os.Remove(link1)
	require.NoError(t, err)

	err = unix.Stat(link2, &stat2)
	require.NoError(t, err)
	require.Equal(t, origIno, stat2.Ino)
	require.Equal(t, uint64(1), uint64(stat2.Nlink), "删除 link1 后 link2 的 nlink 必须为 1")

	// 6. 错误边界：对目录创建硬链接必须返回 EPERM
	dirLink := filepath.Join(mountpoint, "dir_hardlink")
	err = os.Link(subDir, dirLink)
	require.Error(t, err)
	require.ErrorIs(t, err, syscall.EPERM, "对目录创建硬链接必须返回 EPERM")

	// 7. 错误边界：同名冲突创建硬链接必须返回 EEXIST
	err = os.Link(link2, link2)
	require.Error(t, err)
	require.ErrorIs(t, err, syscall.EEXIST, "硬链接名称与已有文件冲突必须返回 EEXIST")

	// 8. 错误边界：源文件不存在创建硬链接返回 ENOENT
	err = os.Link(filepath.Join(mountpoint, "ghost_file.txt"), filepath.Join(mountpoint, "ghost_link.txt"))
	require.Error(t, err)
	require.ErrorIs(t, err, syscall.ENOENT, "源文件不存在创建硬链接必须返回 ENOENT")
}

// TestE2E_Link_Symlink_ReadOnly 验证只读挂载模式下拦截 Symlink 与 Link 返回 EROFS，但允许 Readlink。
func TestE2E_Link_Symlink_ReadOnly(t *testing.T) {
	client := newTestStorageClient(t)
	disk := createDiskWithRootInoGT1(t, client)
	endpoint := getStorageEndpoint()

	// 1. 直连 StorageService 预置一个普通文件和一个符号链接
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	mknodResp, err := client.Mknod(ctx, connect.NewRequest(&storagev1.MknodRequest{
		ParentIno: disk.GetRootIno(),
		Name:      []byte("pre_reg.txt"),
		Mode:      uint32(syscall.S_IFREG | 0644),
		Uid:       1000,
		Gid:       1000,
	}))
	require.NoError(t, err, "预置普通文件失败")

	symResp, err := client.Symlink(ctx, connect.NewRequest(&storagev1.SymlinkRequest{
		ParentIno: disk.GetRootIno(),
		Name:      []byte("pre_symlink"),
		Target:    []byte("/existing/pre_target"),
		Uid:       1000,
		Gid:       1000,
	}))
	require.NoError(t, err, "预置符号链接失败")
	require.NotNil(t, mknodResp)
	require.NotNil(t, symResp)

	// 2. 以只读挂载模式启动 bowl mount
	mountpoint := t.TempDir()
	cmd, stdout, stderr := startBowlMount(t, endpoint, disk.GetDiskId(), mountpoint, "--ro", "--debug")
	require.NoError(t, waitForMount(mountpoint, 10*time.Second), "挂载只读失败: stdout=%s stderr=%s", stdout.String(), stderr.String())
	defer stopBowlMountGracefully(t, cmd, mountpoint, 5*time.Second)

	// 3. 验证只读挂载下 Symlink 拦截并返回 EROFS
	err = os.Symlink("target", filepath.Join(mountpoint, "new_symlink"))
	require.Error(t, err)
	require.ErrorIs(t, err, syscall.EROFS, "只读模式下 Symlink 必须返回 EROFS")

	// 4. 验证只读挂载下 Link 拦截并返回 EROFS
	err = os.Link(filepath.Join(mountpoint, "pre_reg.txt"), filepath.Join(mountpoint, "new_hardlink"))
	require.Error(t, err)
	require.ErrorIs(t, err, syscall.EROFS, "只读模式下 Link 必须返回 EROFS")

	// 5. 验证只读挂载下允许 Readlink 正常读取既有符号链接
	target, err := os.Readlink(filepath.Join(mountpoint, "pre_symlink"))
	require.NoError(t, err, "只读模式下 Readlink 必须正常工作")
	require.Equal(t, "/existing/pre_target", target)
}
