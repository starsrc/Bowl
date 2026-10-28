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

func TestE2E_FileHandle(t *testing.T) {
	client := newTestStorageClient(t)
	endpoint := getStorageEndpoint()

	t.Run("create_open_sync_and_excl_conflict", func(t *testing.T) {
		disk := createDiskWithRootInoGT1(t, client)
		mountpoint := t.TempDir()

		cmd, _, _ := startBowlMount(t, endpoint, disk.GetDiskId(), mountpoint, "--debug")
		require.NoError(t, waitForMount(mountpoint, 10*time.Second), "挂载点就绪")
		defer stopBowlMountGracefully(t, cmd, mountpoint, 5*time.Second)

		filePath := filepath.Join(mountpoint, "file-test.txt")

		// 1. O_CREAT 创建文件并成功打开
		f, err := os.OpenFile(filePath, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0644)
		require.NoError(t, err, "O_CREATE|O_EXCL 创建新文件必须成功")
		defer func() {
			if f != nil {
				_ = f.Close()
			}
		}()

		// 2. O_CREAT|O_EXCL 冲突断言 EEXIST
		_, errConflict := os.OpenFile(filePath, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0644)
		require.Error(t, errConflict, "排他创建已存在文件必须报错")
		require.True(t, errors.Is(errConflict, os.ErrExist) || errors.Is(errConflict, syscall.EEXIST), "排他创建冲突必须返回 EEXIST")

		// 3. f.Sync() 与 fdatasync
		require.NoError(t, f.Sync(), "f.Sync 必须成功")
		require.NoError(t, unix.Fdatasync(int(f.Fd())), "unix.Fdatasync 必须成功")

		// 4. 关闭文件句柄
		require.NoError(t, f.Close(), "关闭常规文件句柄必须成功")
		f = nil

		// 5. 对目录调用非目录写打开错误边界 (EISDIR)
		_, errDir := os.OpenFile(mountpoint, os.O_WRONLY, 0)
		require.Error(t, errDir, "写模式打开目录必须报错")
		require.True(t, errors.Is(errDir, syscall.EISDIR), "错误必须为 EISDIR")
	})

	t.Run("readonly_mount_blocks_write_allows_read", func(t *testing.T) {
		disk := createDiskWithRootInoGT1(t, client)

		// 1. 预先通过服务端 RPC 在该磁盘根目录下创建常规文件
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		createResp, err := client.Create(ctx, connect.NewRequest(&storagev1.CreateRequest{
			ParentIno: disk.GetRootIno(),
			Name:      []byte("ro-target.txt"),
			Mode:      0o644 | syscall.S_IFREG,
			Uid:       1000,
			Gid:       1000,
			Flags:     0o2, // O_RDWR
		}))
		require.NoError(t, err, "服务端初始化文件必须成功")

		// 释放初始文件句柄
		_, err = client.Release(ctx, connect.NewRequest(&storagev1.ReleaseRequest{
			Ino: createResp.Msg.GetInode().GetIno(),
			Fh:  createResp.Msg.GetFh(),
		}))
		require.NoError(t, err, "释放初始句柄必须成功")

		mountpoint := t.TempDir()

		// 2. 以 --ro 只读模式挂载
		cmd, _, _ := startBowlMount(t, endpoint, disk.GetDiskId(), mountpoint, "--ro", "--debug")
		require.NoError(t, waitForMount(mountpoint, 10*time.Second), "只读挂载点就绪")
		defer stopBowlMountGracefully(t, cmd, mountpoint, 5*time.Second)

		existingFile := filepath.Join(mountpoint, "ro-target.txt")
		newFile := filepath.Join(mountpoint, "new-file.txt")

		// 3. 禁止创建新文件 (EROFS)
		_, errCreate := os.OpenFile(newFile, os.O_CREATE|os.O_WRONLY, 0644)
		require.Error(t, errCreate, "只读挂载禁止创建新文件")
		require.True(t, errors.Is(errCreate, syscall.EROFS), "错误必须为 EROFS")

		// 4. 禁止写模式打开既有文件 (EROFS)
		_, errWrite := os.OpenFile(existingFile, os.O_WRONLY, 0)
		require.Error(t, errWrite, "只读挂载禁止 O_WRONLY 打开")
		require.True(t, errors.Is(errWrite, syscall.EROFS), "错误必须为 EROFS")

		_, errRW := os.OpenFile(existingFile, os.O_RDWR, 0)
		require.Error(t, errRW, "只读挂载禁止 O_RDWR 打开")
		require.True(t, errors.Is(errRW, syscall.EROFS), "错误必须为 EROFS")

		// 5. 允许只读模式打开既有文件
		roF, errRO := os.OpenFile(existingFile, os.O_RDONLY, 0)
		require.NoError(t, errRO, "只读挂载允许 O_RDONLY 打开")
		require.NoError(t, roF.Close(), "关闭只读文件句柄")
	})

	t.Run("unlink_whilst_open_orphan_lifecycle", func(t *testing.T) {
		disk := createDiskWithRootInoGT1(t, client)
		mountpoint := t.TempDir()

		cmd, _, _ := startBowlMount(t, endpoint, disk.GetDiskId(), mountpoint, "--debug")
		require.NoError(t, waitForMount(mountpoint, 10*time.Second), "挂载点就绪")
		defer stopBowlMountGracefully(t, cmd, mountpoint, 5*time.Second)

		targetName := "orphan-unlink.txt"
		filePath := filepath.Join(mountpoint, targetName)

		// 1. 通过挂载点打开并创建常规文件
		f, err := os.OpenFile(filePath, os.O_CREATE|os.O_RDWR, 0644)
		require.NoError(t, err, "挂载点打开创建常规文件必须成功")
		defer func() {
			if f != nil {
				_ = f.Close()
			}
		}()

		// 2. 通过 RPC 查询该文件在服务端的 Ino
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		lookupResp, err := client.Lookup(ctx, connect.NewRequest(&storagev1.LookupRequest{
			ParentIno: disk.GetRootIno(),
			Name:      []byte(targetName),
		}))
		require.NoError(t, err, "Lookup 查找新创建文件必须成功")
		serverIno := lookupResp.Msg.GetInode().GetIno()
		require.Greater(t, serverIno, uint64(0))

		// 3. 在服务端解链该目录项 (Unlink)
		_, err = client.Unlink(ctx, connect.NewRequest(&storagev1.UnlinkRequest{
			ParentIno: disk.GetRootIno(),
			Name:      []byte(targetName),
		}))
		require.NoError(t, err, "服务端解链目录项必须成功")

		// 4. 断言已打开孤儿句柄 f.Stat() 与 f.Sync() 依然成功，且内核感知的 Nlink 最终变为 0
		require.NoError(t, f.Sync(), "孤儿句柄 f.Sync() 必须成功")
		fi, err := f.Stat()
		require.NoError(t, err, "已解链已打开句柄 f.Stat() 必须依然成功")
		require.Equal(t, targetName, fi.Name())

		require.Eventually(t, func() bool {
			var stat unix.Stat_t
			if err := unix.Fstat(int(f.Fd()), &stat); err == nil {
				return stat.Nlink == 0
			}
			return false
		}, 3*time.Second, 100*time.Millisecond, "孤儿句柄的 Nlink 必须变为 0")

		// 5. 验证关闭句柄前服务端 Inode 仍存活且 Nlink == 0
		attrResp, err := client.GetAttr(ctx, connect.NewRequest(&storagev1.GetAttrRequest{
			Ino: serverIno,
		}))
		require.NoError(t, err, "关闭句柄前服务端孤儿 Inode 仍应存活")
		require.Equal(t, uint32(0), attrResp.Msg.GetInode().GetAttr().GetNlink())

		// 6. 关闭句柄，触发 Flush 与 Release
		require.NoError(t, f.Close(), "关闭孤儿句柄必须成功")
		f = nil

		// 7. 轮询断言服务端孤儿节点被物理回收 (返回 CodeNotFound / ENOENT)
		require.Eventually(t, func() bool {
			_, getErr := client.GetAttr(ctx, connect.NewRequest(&storagev1.GetAttrRequest{
				Ino: serverIno,
			}))
			if getErr != nil {
				var connectErr *connect.Error
				if errors.As(getErr, &connectErr) && connectErr.Code() == connect.CodeNotFound {
					return true
				}
			}
			return false
		}, 5*time.Second, 100*time.Millisecond, "关闭孤儿全部活跃句柄后服务端 Inode 必须被物理回收")
	})
}
