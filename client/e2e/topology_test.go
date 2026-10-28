//go:build e2e && linux

package e2e_test

import (
	"context"
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

// TestE2E_Topology_MkdirRmdir 验证多层目录创建移除、父目录 nlink 增减联动以及 ENOTEMPTY / EEXIST / ENOENT 错误边界。
func TestE2E_Topology_MkdirRmdir(t *testing.T) {
	client := newTestStorageClient(t)
	disk := createDiskWithRootInoGT1(t, client)
	mountpoint := t.TempDir()
	endpoint := getStorageEndpoint()

	cmd, stdout, stderr := startBowlMount(t, endpoint, disk.GetDiskId(), mountpoint, "--debug")
	require.NoError(t, waitForMount(mountpoint, 10*time.Second), "挂载失败: stdout=%s stderr=%s", stdout.String(), stderr.String())
	defer stopBowlMountGracefully(t, cmd, mountpoint, 5*time.Second)

	// 1. 获取根目录初始状态
	var rootStatBefore unix.Stat_t
	err := unix.Stat(mountpoint, &rootStatBefore)
	require.NoError(t, err)
	initNlink := rootStatBefore.Nlink

	// 2. 创建一级子目录 dir1 并验证父目录 nlink + 1
	dir1 := filepath.Join(mountpoint, "dir1")
	err = os.Mkdir(dir1, 0755)
	require.NoError(t, err, "创建一级子目录必须成功")

	var rootStatAfter unix.Stat_t
	err = unix.Stat(mountpoint, &rootStatAfter)
	require.NoError(t, err)
	require.Equal(t, initNlink+1, rootStatAfter.Nlink, "根目录 nlink 应因创建子目录而递增 1")

	// 3. 验证创建重复目录返回 EEXIST
	err = os.Mkdir(dir1, 0755)
	require.Error(t, err)
	require.ErrorIs(t, err, syscall.EEXIST, "创建已存在目录必须返回 EEXIST")

	// 4. 多层嵌套目录创建 dir1/dir2/dir3
	dir2 := filepath.Join(dir1, "dir2")
	dir3 := filepath.Join(dir2, "dir3")
	err = os.MkdirAll(dir3, 0755)
	require.NoError(t, err, "os.MkdirAll 多层嵌套创建必须成功")

	// 5. 移除非空目录 dir1 必须返回 ENOTEMPTY
	err = syscall.Rmdir(dir1)
	require.Error(t, err)
	require.ErrorIs(t, err, syscall.ENOTEMPTY, "移除非空目录必须返回 ENOTEMPTY")

	// 6. 逆序清理空目录并验证成功
	err = syscall.Rmdir(dir3)
	require.NoError(t, err, "移除叶子空目录 dir3 必须成功")

	err = syscall.Rmdir(dir2)
	require.NoError(t, err, "移除空目录 dir2 必须成功")

	err = syscall.Rmdir(dir1)
	require.NoError(t, err, "移除空目录 dir1 必须成功")

	// 7. 再次 stat 根目录，nlink 恢复初始值
	var rootStatFinal unix.Stat_t
	err = unix.Stat(mountpoint, &rootStatFinal)
	require.NoError(t, err)
	require.Equal(t, initNlink, rootStatFinal.Nlink, "所有子目录移除后根目录 nlink 必须恢复初始值")

	// 8. 移除不存在的目录返回 ENOENT
	err = syscall.Rmdir(dir1)
	require.Error(t, err)
	require.ErrorIs(t, err, syscall.ENOENT, "移除不存在的目录必须返回 ENOENT")
}

// TestE2E_Topology_SgidInheritance 验证父目录包含 S_ISGID 时，通过 FUSE 创建子目录自动继承父目录 GID 与 S_ISGID 位。
func TestE2E_Topology_SgidInheritance(t *testing.T) {
	client := newTestStorageClient(t)
	disk := createDiskWithRootInoGT1(t, client)
	endpoint := getStorageEndpoint()

	// 1. 直连 StorageService 创建带有 S_ISGID 权限位与特定 GID 的父目录（解耦对未实现 SetAttr 的依赖）
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	parentName := "sgid_parent"
	mkdirResp, err := client.Mkdir(ctx, connect.NewRequest(&storagev1.MkdirRequest{
		ParentIno: disk.GetRootIno(),
		Name:      []byte(parentName),
		Mode:      uint32(syscall.S_IFDIR | 02755), // S_ISGID | 0755
		Uid:       1000,
		Gid:       2000,
	}))
	require.NoError(t, err, "直连 StorageService 创建 SGID 父目录失败")
	require.NotNil(t, mkdirResp.Msg.GetInode())

	mountpoint := t.TempDir()
	cmd, stdout, stderr := startBowlMount(t, endpoint, disk.GetDiskId(), mountpoint, "--debug")
	require.NoError(t, waitForMount(mountpoint, 10*time.Second), "挂载失败: stdout=%s stderr=%s", stdout.String(), stderr.String())
	defer stopBowlMountGracefully(t, cmd, mountpoint, 5*time.Second)

	parentDir := filepath.Join(mountpoint, parentName)
	var parentStat unix.Stat_t
	err = unix.Stat(parentDir, &parentStat)
	require.NoError(t, err, "获取 SGID 父目录 Stat 必须成功")
	require.NotZero(t, parentStat.Mode&unix.S_ISGID, "父目录必须包含 S_ISGID 标志")
	require.Equal(t, uint32(2000), parentStat.Gid, "父目录 GID 必须为 2000")

	// 2. 通过 FUSE 挂载点在父目录下创建子目录 child
	childDir := filepath.Join(parentDir, "child")
	err = os.Mkdir(childDir, 0755)
	require.NoError(t, err, "在 SGID 父目录下创建子目录必须成功")

	// 3. 校验子目录自动继承父目录 GID 与 S_ISGID
	var childStat unix.Stat_t
	err = unix.Stat(childDir, &childStat)
	require.NoError(t, err)
	require.Equal(t, uint32(2000), childStat.Gid, "子目录必须继承父目录 GID")
	require.NotZero(t, childStat.Mode&unix.S_ISGID, "子目录必须继承父目录 S_ISGID 位")

	// 清理
	require.NoError(t, syscall.Rmdir(childDir))
	require.NoError(t, syscall.Rmdir(parentDir))
}

// TestE2E_Topology_MknodUnlink 验证 FIFO 特殊节点创建、stat 校验、重复创建 EEXIST、EISDIR 与 ENOTDIR 边界及解链。
func TestE2E_Topology_MknodUnlink(t *testing.T) {
	client := newTestStorageClient(t)
	disk := createDiskWithRootInoGT1(t, client)
	mountpoint := t.TempDir()
	endpoint := getStorageEndpoint()

	cmd, stdout, stderr := startBowlMount(t, endpoint, disk.GetDiskId(), mountpoint, "--debug")
	require.NoError(t, waitForMount(mountpoint, 10*time.Second), "挂载失败: stdout=%s stderr=%s", stdout.String(), stderr.String())
	defer stopBowlMountGracefully(t, cmd, mountpoint, 5*time.Second)

	fifoPath := filepath.Join(mountpoint, "test_fifo")

	// 1. 创建命名管道 FIFO
	err := unix.Mkfifo(fifoPath, 0640)
	require.NoError(t, err, "创建 FIFO 命名管道必须成功")

	// 2. stat 校验节点类型为 FIFO
	var stat unix.Stat_t
	err = unix.Stat(fifoPath, &stat)
	require.NoError(t, err)
	require.Equal(t, uint32(syscall.S_IFIFO), stat.Mode&syscall.S_IFMT, "节点类型必须为 S_IFIFO")
	require.Equal(t, uint32(0640), stat.Mode&0777, "权限位必须匹配 0640")

	// 3. 重复创建相同 FIFO 返回 EEXIST
	err = unix.Mkfifo(fifoPath, 0640)
	require.Error(t, err)
	require.ErrorIs(t, err, syscall.EEXIST, "重复创建 FIFO 必须返回 EEXIST")

	// 4. 对非目录 (FIFO) 调用 Rmdir 返回 ENOTDIR
	err = syscall.Rmdir(fifoPath)
	require.Error(t, err)
	require.ErrorIs(t, err, syscall.ENOTDIR, "对非目录执行 Rmdir 必须返回 ENOTDIR")

	// 5. 对目录调用 Unlink 返回 EISDIR
	subDir := filepath.Join(mountpoint, "test_dir")
	require.NoError(t, os.Mkdir(subDir, 0755))
	err = syscall.Unlink(subDir)
	require.Error(t, err)
	require.ErrorIs(t, err, syscall.EISDIR, "对目录执行 Unlink 必须返回 EISDIR")
	require.NoError(t, syscall.Rmdir(subDir))

	// 6. 成功解链 FIFO
	err = syscall.Unlink(fifoPath)
	require.NoError(t, err, "解链 FIFO 必须成功")

	// 7. 解链后再次访问返回 ENOENT
	_, err = os.Stat(fifoPath)
	require.Error(t, err)
	require.ErrorIs(t, err, os.ErrNotExist, "解链后 stat 必须返回 ENOENT")

	// 8. 再次解链不存在的文件返回 ENOENT
	err = syscall.Unlink(fifoPath)
	require.Error(t, err)
	require.ErrorIs(t, err, syscall.ENOENT, "解链不存在文件必须返回 ENOENT")
}

// TestE2E_Topology_ReadOnly 验证 --ro 只读挂载下 Mkdir、Mknod、Rmdir、Unlink 均直接被客户端拦截返回 EROFS。
func TestE2E_Topology_ReadOnly(t *testing.T) {
	client := newTestStorageClient(t)
	disk := createDiskWithRootInoGT1(t, client)
	endpoint := getStorageEndpoint()

	// 1. 读写模式挂载准备已有目录与文件
	initMountpoint := t.TempDir()
	cmdRW, stdoutRW, stderrRW := startBowlMount(t, endpoint, disk.GetDiskId(), initMountpoint, "--debug")
	require.NoError(t, waitForMount(initMountpoint, 10*time.Second), "RW挂载失败: stdout=%s stderr=%s", stdoutRW.String(), stderrRW.String())

	existDir := filepath.Join(initMountpoint, "exist_dir")
	require.NoError(t, os.Mkdir(existDir, 0755))

	existFifo := filepath.Join(initMountpoint, "exist_fifo")
	require.NoError(t, unix.Mkfifo(existFifo, 0644))

	stopBowlMountGracefully(t, cmdRW, initMountpoint, 5*time.Second)

	// 2. 以 --ro 只读模式重新挂载
	roMountpoint := t.TempDir()
	cmdRO, stdoutRO, stderrRO := startBowlMount(t, endpoint, disk.GetDiskId(), roMountpoint, "--ro", "--debug")
	require.NoError(t, waitForMount(roMountpoint, 10*time.Second), "RO挂载失败: stdout=%s stderr=%s", stdoutRO.String(), stderrRO.String())
	defer stopBowlMountGracefully(t, cmdRO, roMountpoint, 5*time.Second)

	// 3. 验证 Mkdir 返回 EROFS
	err := os.Mkdir(filepath.Join(roMountpoint, "new_dir"), 0755)
	require.Error(t, err)
	require.ErrorIs(t, err, syscall.EROFS, "只读挂载下 Mkdir 必须返回 EROFS")

	// 4. 验证 Mknod (Mkfifo) 返回 EROFS
	err = unix.Mkfifo(filepath.Join(roMountpoint, "new_fifo"), 0644)
	require.Error(t, err)
	require.ErrorIs(t, err, syscall.EROFS, "只读挂载下 Mknod 必须返回 EROFS")

	// 5. 验证 Rmdir 返回 EROFS
	err = syscall.Rmdir(filepath.Join(roMountpoint, "exist_dir"))
	require.Error(t, err)
	require.ErrorIs(t, err, syscall.EROFS, "只读挂载下 Rmdir 必须返回 EROFS")

	// 6. 验证 Unlink 返回 EROFS
	err = syscall.Unlink(filepath.Join(roMountpoint, "exist_fifo"))
	require.Error(t, err)
	require.ErrorIs(t, err, syscall.EROFS, "只读挂载下 Unlink 必须返回 EROFS")
}
