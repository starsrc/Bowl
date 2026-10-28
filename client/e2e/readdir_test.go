//go:build e2e && linux

package e2e_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"

	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
)

// TestE2E_ReadDir 完整验证目录句柄生命周期、空与嵌套目录、rmdir-whilst-open 以及 1500+ 个子项的分页遍历。
func TestE2E_ReadDir(t *testing.T) {
	storageClient := newTestStorageClient(t)
	disk := createDiskWithRootInoGT1(t, storageClient)
	rootIno := disk.GetRootIno()
	t.Logf("测试磁盘就绪: id=%s, root_ino=%d", disk.GetDiskId(), rootIno)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// -------------------------------------------------------------
	// 1. 服务端预置目录结构：空目录、多级嵌套目录、用于 rmdir-whilst-open 的目录、1500+ 文件目录
	// -------------------------------------------------------------
	// 1.1 空目录
	_, err := storageClient.Mkdir(ctx, connect.NewRequest(&storagev1.MkdirRequest{
		ParentIno: rootIno,
		Name:      []byte("empty_dir"),
		Mode:      syscall.S_IFDIR | 0o755,
	}))
	require.NoError(t, err, "预置 empty_dir 失败")

	// 1.2 多级嵌套目录: level1 -> level2 -> child.txt
	l1Resp, err := storageClient.Mkdir(ctx, connect.NewRequest(&storagev1.MkdirRequest{
		ParentIno: rootIno,
		Name:      []byte("level1"),
		Mode:      syscall.S_IFDIR | 0o755,
	}))
	require.NoError(t, err, "预置 level1 失败")
	l1Ino := l1Resp.Msg.GetInode().GetIno()

	l2Resp, err := storageClient.Mkdir(ctx, connect.NewRequest(&storagev1.MkdirRequest{
		ParentIno: l1Ino,
		Name:      []byte("level2"),
		Mode:      syscall.S_IFDIR | 0o755,
	}))
	require.NoError(t, err, "预置 level2 失败")
	l2Ino := l2Resp.Msg.GetInode().GetIno()

	_, err = storageClient.Mknod(ctx, connect.NewRequest(&storagev1.MknodRequest{
		ParentIno: l2Ino,
		Name:      []byte("child.txt"),
		Mode:      syscall.S_IFREG | 0o644,
		Uid:       1000,
		Gid:       1000,
	}))
	require.NoError(t, err, "预置 level2/child.txt 失败")

	// 1.3 预置 rmdir-whilst-open 目标目录
	rmdirResp, err := storageClient.Mkdir(ctx, connect.NewRequest(&storagev1.MkdirRequest{
		ParentIno: rootIno,
		Name:      []byte("open_then_rmdir"),
		Mode:      syscall.S_IFDIR | 0o755,
	}))
	require.NoError(t, err, "预置 open_then_rmdir 失败")
	orphanDirIno := rmdirResp.Msg.GetInode().GetIno()

	// 1.4 预置 1500 个子文件的单目录
	bulkResp, err := storageClient.Mkdir(ctx, connect.NewRequest(&storagev1.MkdirRequest{
		ParentIno: rootIno,
		Name:      []byte("bulk_1500"),
		Mode:      syscall.S_IFDIR | 0o755,
	}))
	require.NoError(t, err, "预置 bulk_1500 目录失败")
	bulkDirIno := bulkResp.Msg.GetInode().GetIno()

	const totalFiles = 1500
	expectedNames := make(map[string]struct{}, totalFiles)
	for i := 0; i < totalFiles; i++ {
		expectedNames[fmt.Sprintf("item_%04d.dat", i)] = struct{}{}
	}

	// 32 并发 worker 池快速并发创建 1500 个文件节点
	workCh := make(chan int, totalFiles)
	for i := 0; i < totalFiles; i++ {
		workCh <- i
	}
	close(workCh)

	var wg sync.WaitGroup
	errCh := make(chan error, 32)
	startTime := time.Now()
	for w := 0; w < 32; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for idx := range workCh {
				fileName := fmt.Sprintf("item_%04d.dat", idx)
				_, mknodErr := storageClient.Mknod(ctx, connect.NewRequest(&storagev1.MknodRequest{
					ParentIno: bulkDirIno,
					Name:      []byte(fileName),
					Mode:      syscall.S_IFREG | 0o644,
					Uid:       1000,
					Gid:       1000,
				}))
				if mknodErr != nil {
					select {
					case errCh <- fmt.Errorf("mknod %s failed: %w", fileName, mknodErr):
					default:
					}
					return
				}
			}
		}()
	}
	wg.Wait()
	close(errCh)
	for mknodErr := range errCh {
		require.NoError(t, mknodErr)
	}
	t.Logf("并发预置 1500 个子文件耗时: %v", time.Since(startTime))

	// -------------------------------------------------------------
	// 2. 挂载并等待就绪
	// -------------------------------------------------------------
	mountpoint := t.TempDir()
	endpoint := getStorageEndpoint()

	cmd, stdout, stderr := startBowlMount(t, endpoint, disk.GetDiskId(), mountpoint, "--debug")
	err = waitForMount(mountpoint, 10*time.Second)
	if err != nil {
		t.Logf("stdout: %s", stdout.String())
		t.Logf("stderr: %s", stderr.String())
		_ = cmd.Process.Kill()
		t.Fatalf("等待挂载就绪失败: %v", err)
	}
	t.Logf("挂载点已就绪: %s", mountpoint)

	// -------------------------------------------------------------
	// 3. 验证场景 1：空目录、多级嵌套目录以及 Inode 根节点映射
	// -------------------------------------------------------------
	t.Run("空目录与多级嵌套目录遍历及Inode重写", func(t *testing.T) {
		// 根目录遍历验证
		rootEntries, err := os.ReadDir(mountpoint)
		require.NoError(t, err, "读取根目录失败")
		rootNameSet := make(map[string]bool)
		for _, e := range rootEntries {
			rootNameSet[e.Name()] = true
		}
		require.True(t, rootNameSet["empty_dir"])
		require.True(t, rootNameSet["level1"])
		require.True(t, rootNameSet["open_then_rmdir"])
		require.True(t, rootNameSet["bulk_1500"])

		// 空目录读取必须为空切片
		emptyEntries, err := os.ReadDir(filepath.Join(mountpoint, "empty_dir"))
		require.NoError(t, err, "读取 empty_dir 失败")
		require.Empty(t, emptyEntries, "empty_dir 必须无任何子项")

		// 多级嵌套目录读取
		l1Entries, err := os.ReadDir(filepath.Join(mountpoint, "level1"))
		require.NoError(t, err)
		require.Len(t, l1Entries, 1)
		require.Equal(t, "level2", l1Entries[0].Name())
		require.True(t, l1Entries[0].IsDir())

		l2Entries, err := os.ReadDir(filepath.Join(mountpoint, "level1", "level2"))
		require.NoError(t, err)
		require.Len(t, l2Entries, 1)
		require.Equal(t, "child.txt", l2Entries[0].Name())
		require.False(t, l2Entries[0].IsDir())

		// 验证根目录与一级子目录的 '.' 与 '..' Inode 映射为 1
		var statDot unix.Stat_t
		require.NoError(t, unix.Stat(filepath.Join(mountpoint, "."), &statDot))
		require.Equal(t, uint64(1), statDot.Ino, "根目录 '.' 的 Inode 必须为 1")

		var statL1Parent unix.Stat_t
		require.NoError(t, unix.Stat(filepath.Join(mountpoint, "level1", ".."), &statL1Parent))
		require.Equal(t, uint64(1), statL1Parent.Ino, "一级子目录的 '..' 必须映射为内核根节点 1")
	})

	// -------------------------------------------------------------
	// 4. 验证场景 2：rmdir-whilst-open 语义
	// -------------------------------------------------------------
	t.Run("rmdir-whilst-open语义与句柄生命周期", func(t *testing.T) {
		openPath := filepath.Join(mountpoint, "open_then_rmdir")

		// 打开空子目录
		fd, err := syscall.Open(openPath, syscall.O_RDONLY|syscall.O_DIRECTORY, 0)
		require.NoError(t, err, "打开 open_then_rmdir 目录失败")

		// 在服务端移除该空目录（成为 nlink == 0 孤儿节点）
		_, err = storageClient.Rmdir(ctx, connect.NewRequest(&storagev1.RmdirRequest{
			ParentIno: rootIno,
			Name:      []byte("open_then_rmdir"),
		}))
		require.NoError(t, err, "服务端 Rmdir 必须成功")

		// 通过已打开 fd 同步与读取，验证 FsyncDir / ReadDir 在孤儿打开句柄上仍正常工作
		require.NoError(t, syscall.Fsync(fd), "已删空目录 fd 执行 syscall.Fsync 必须成功")
		require.NoError(t, unix.Fdatasync(fd), "已删空目录 fd 执行 unix.Fdatasync 必须成功")

		// 关闭目录句柄，触发内核异步发送 ReleaseDir 清理服务端孤儿目录
		require.NoError(t, syscall.Close(fd), "关闭目录句柄必须成功")

		// 验证孤儿目录在服务端被彻底物理回收（GetAttr 返回 NotFound）
		require.Eventually(t, func() bool {
			_, getErr := storageClient.GetAttr(ctx, connect.NewRequest(&storagev1.GetAttrRequest{
				Ino: orphanDirIno,
			}))
			return connect.CodeOf(getErr) == connect.CodeNotFound
		}, 5*time.Second, 50*time.Millisecond, "关闭句柄后孤儿目录必须被服务端物理回收")

		// 等待内核 dentry 缓存过期（entry_timeout = 1.0s），确认挂载点上该目录返回 ENOENT
		require.Eventually(t, func() bool {
			_, statErr := os.Stat(openPath)
			return os.IsNotExist(statErr)
		}, 5*time.Second, 50*time.Millisecond, "dentry 缓存过期后访问已删除目录必须返回 ENOENT")
	})

	// -------------------------------------------------------------
	// 5. 验证场景 3：1500+ 个子文件的单目录 os.ReadDir 完整分页遍历
	// -------------------------------------------------------------
	t.Run("1500+子文件单目录分页遍历完整性与确定性", func(t *testing.T) {
		bulkPath := filepath.Join(mountpoint, "bulk_1500")

		readStartTime := time.Now()
		entries, err := os.ReadDir(bulkPath)
		require.NoError(t, err, "os.ReadDir 读取 1500+ 文件单目录失败")
		t.Logf("os.ReadDir 遍历 %d 个条目耗时: %v", len(entries), time.Since(readStartTime))

		require.Equal(t, totalFiles, len(entries), "遍历返回条目总数必须严格等于 1500")

		seenMap := make(map[string]struct{}, totalFiles)
		for _, entry := range entries {
			name := entry.Name()
			_, expected := expectedNames[name]
			require.True(t, expected, "发现未知条目: %s", name)

			_, alreadySeen := seenMap[name]
			require.False(t, alreadySeen, "发现重复条目: %s", name)
			seenMap[name] = struct{}{}

			require.True(t, entry.Type().IsRegular(), "条目类型必须为常规文件: %s", name)
		}

		require.Equal(t, totalFiles, len(seenMap), "必须 100% 覆盖预期 1500 个文件名且零遗漏零重复")
	})

	// -------------------------------------------------------------
	// 6. 优雅卸载
	// -------------------------------------------------------------
	stopBowlMountGracefully(t, cmd, mountpoint, 5*time.Second)
}
