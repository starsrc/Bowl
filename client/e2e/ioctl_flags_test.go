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

// Linux VFS inode 标志位常量。
const (
	fsImmutableFl = 0x00000010 // FS_IMMUTABLE_FL
	fsAppendFl    = 0x00000020 // FS_APPEND_FL

	protoFlagImmutable = 0x1
	protoFlagAppend    = 0x2
)

// getIoctlFlags 读取文件描述符关联的文件系统标志位（FS_IOC_GETFLAGS）。
func getIoctlFlags(fd int) (int, error) {
	return unix.IoctlGetInt(fd, unix.FS_IOC_GETFLAGS)
}

// setIoctlFlags 设置文件描述符关联的文件系统标志位（FS_IOC_SETFLAGS）。
func setIoctlFlags(fd int, flags int) error {
	return unix.IoctlSetPointerInt(fd, unix.FS_IOC_SETFLAGS, flags)
}

func TestE2E_Ioctl(t *testing.T) {
	client := newTestStorageClient(t)
	endpoint := getStorageEndpoint()

	t.Run("initial_flags_zero_and_get_set_immutable", func(t *testing.T) {
		disk := createDiskWithRootInoGT1(t, client)
		mountpoint := t.TempDir()

		cmd, stdout, stderr := startBowlMount(t, endpoint, disk.GetDiskId(), mountpoint, "--debug")
		require.NoError(t, waitForMount(mountpoint, 10*time.Second), "挂载点就绪")
		defer func() {
			if t.Failed() {
				t.Logf("bowl stdout:\n%s", stdout.String())
				t.Logf("bowl stderr:\n%s", stderr.String())
			}
			stopBowlMountGracefully(t, cmd, mountpoint, 5*time.Second)
		}()

		fileName := "immutable-test.txt"
		filePath := filepath.Join(mountpoint, fileName)
		require.NoError(t, os.WriteFile(filePath, []byte("initial immutable test data"), 0644))

		// 获取该文件对应的 Inode
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		lookupResp, err := client.Lookup(ctx, connect.NewRequest(&storagev1.LookupRequest{
			ParentIno: disk.GetRootIno(),
			Name:      []byte(fileName),
		}))
		require.NoError(t, err)
		fileIno := lookupResp.Msg.GetInode().GetIno()

		// 1. 打开文件验证初始 GETFLAGS 为 0
		f, err := os.Open(filePath)
		require.NoError(t, err)
		flags, err := getIoctlFlags(int(f.Fd()))
		require.NoError(t, err)
		require.Equal(t, 0, flags, "初始标志位应为 0")
		require.NoError(t, f.Close())

		// 2. 通过服务端 RPC 预置 ProtoFlagImmutable，在挂载点上通过 FS_IOC_GETFLAGS 读取并验证精准映射为 FS_IMMUTABLE_FL (0x10)
		immFlag := uint32(protoFlagImmutable)
		_, err = client.SetAttr(ctx, connect.NewRequest(&storagev1.SetAttrRequest{
			Ino:   fileIno,
			Flags: &immFlag,
		}))
		require.NoError(t, err)

		f, err = os.Open(filePath)
		require.NoError(t, err)
		readFlags, err := getIoctlFlags(int(f.Fd()))
		require.NoError(t, err)
		require.Equal(t, fsImmutableFl, readFlags, "通过 RPC 设置后应精确读回 FS_IMMUTABLE_FL (0x10)")
		require.NoError(t, f.Close())

		// 3. 通过服务端 RPC 清除标志位，验证读回恢复为 0
		zeroFlag := uint32(0)
		_, err = client.SetAttr(ctx, connect.NewRequest(&storagev1.SetAttrRequest{
			Ino:   fileIno,
			Flags: &zeroFlag,
		}))
		require.NoError(t, err)

		f, err = os.Open(filePath)
		require.NoError(t, err)
		clearedFlags, err := getIoctlFlags(int(f.Fd()))
		require.NoError(t, err)
		require.Equal(t, 0, clearedFlags, "清除后读回应恢复为 0")
		require.NoError(t, f.Close())

		// 4. 若环境具备 CAP_LINUX_IMMUTABLE 能力，验证通过客户端 FS_IOC_SETFLAGS 设置与清除的双向连通性
		fRw, err := os.OpenFile(filePath, os.O_RDWR, 0644)
		require.NoError(t, err)
		defer fRw.Close()

		setErr := setIoctlFlags(int(fRw.Fd()), fsImmutableFl)
		if setErr != nil {
			if errors.Is(setErr, syscall.EPERM) {
				t.Logf("当前环境未被赋予 CAP_LINUX_IMMUTABLE 能力，内核 VFS 拒绝 FS_IOC_SETFLAGS 设置高特权标志位 (符合预期)")
			} else {
				require.NoError(t, setErr, "非 EPERM 错误")
			}
		} else {
			// 具备 CAP_LINUX_IMMUTABLE 能力时，验证读回与服务端 RPC
			rf, err := getIoctlFlags(int(fRw.Fd()))
			require.NoError(t, err)
			require.Equal(t, fsImmutableFl, rf, "客户端 SETFLAGS 后精确读回 FS_IMMUTABLE_FL")

			getResp, err := client.GetAttr(ctx, connect.NewRequest(&storagev1.GetAttrRequest{Ino: fileIno}))
			require.NoError(t, err)
			require.Equal(t, uint32(protoFlagImmutable), getResp.Msg.GetInode().GetAttr().GetFlags(), "服务端 Flags 应同步更新为 0x1")

			// 清除标志位
			require.NoError(t, setIoctlFlags(int(fRw.Fd()), 0))
			cf, err := getIoctlFlags(int(fRw.Fd()))
			require.NoError(t, err)
			require.Equal(t, 0, cf)
		}
	})

	t.Run("append_flag_get_and_set", func(t *testing.T) {
		disk := createDiskWithRootInoGT1(t, client)
		mountpoint := t.TempDir()

		cmd, stdout, stderr := startBowlMount(t, endpoint, disk.GetDiskId(), mountpoint, "--debug")
		require.NoError(t, waitForMount(mountpoint, 10*time.Second), "挂载点就绪")
		defer func() {
			if t.Failed() {
				t.Logf("bowl stdout:\n%s", stdout.String())
				t.Logf("bowl stderr:\n%s", stderr.String())
			}
			stopBowlMountGracefully(t, cmd, mountpoint, 5*time.Second)
		}()

		fileName := "append-test.txt"
		filePath := filepath.Join(mountpoint, fileName)
		require.NoError(t, os.WriteFile(filePath, []byte("line 1\n"), 0644))

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		lookupResp, err := client.Lookup(ctx, connect.NewRequest(&storagev1.LookupRequest{
			ParentIno: disk.GetRootIno(),
			Name:      []byte(fileName),
		}))
		require.NoError(t, err)
		fileIno := lookupResp.Msg.GetInode().GetIno()

		// 1. 通过服务端 RPC 预置 ProtoFlagAppend，验证挂载点精确读回 FS_APPEND_FL (0x20)
		appFlag := uint32(protoFlagAppend)
		_, err = client.SetAttr(ctx, connect.NewRequest(&storagev1.SetAttrRequest{
			Ino:   fileIno,
			Flags: &appFlag,
		}))
		require.NoError(t, err)

		f, err := os.Open(filePath)
		require.NoError(t, err)
		readFlags, err := getIoctlFlags(int(f.Fd()))
		require.NoError(t, err)
		require.Equal(t, fsAppendFl, readFlags, "通过 RPC 设置后应精确读回 FS_APPEND_FL (0x20)")
		require.NoError(t, f.Close())

		// 2. 验证追加写文件正常
		fAppend, err := os.OpenFile(filePath, os.O_WRONLY|os.O_APPEND, 0)
		require.NoError(t, err)
		_, err = fAppend.WriteString("line 2\n")
		require.NoError(t, err)
		require.NoError(t, fAppend.Close())

		content, err := os.ReadFile(filePath)
		require.NoError(t, err)
		require.Equal(t, "line 1\nline 2\n", string(content))

		// 3. 通过服务端 RPC 设置组合标志 ProtoFlagImmutable | ProtoFlagAppend (0x3)
		bothFlags := uint32(protoFlagImmutable | protoFlagAppend)
		_, err = client.SetAttr(ctx, connect.NewRequest(&storagev1.SetAttrRequest{
			Ino:   fileIno,
			Flags: &bothFlags,
		}))
		require.NoError(t, err)

		f, err = os.Open(filePath)
		require.NoError(t, err)
		bothReadFlags, err := getIoctlFlags(int(f.Fd()))
		require.NoError(t, err)
		require.Equal(t, fsImmutableFl|fsAppendFl, bothReadFlags, "精确读回 FS_IMMUTABLE_FL | FS_APPEND_FL (0x30)")
		require.NoError(t, f.Close())

		// 4. 清除标志位
		zeroFlag := uint32(0)
		_, err = client.SetAttr(ctx, connect.NewRequest(&storagev1.SetAttrRequest{
			Ino:   fileIno,
			Flags: &zeroFlag,
		}))
		require.NoError(t, err)

		f, err = os.Open(filePath)
		require.NoError(t, err)
		clearedFlags, err := getIoctlFlags(int(f.Fd()))
		require.NoError(t, err)
		require.Equal(t, 0, clearedFlags)
		require.NoError(t, f.Close())

		// 5. 若环境具备 CAP_LINUX_IMMUTABLE 能力，验证通过客户端 FS_IOC_SETFLAGS 设置 FS_APPEND_FL
		fRw, err := os.OpenFile(filePath, os.O_RDWR, 0644)
		require.NoError(t, err)
		defer fRw.Close()

		setErr := setIoctlFlags(int(fRw.Fd()), fsAppendFl)
		if setErr != nil {
			if errors.Is(setErr, syscall.EPERM) {
				t.Logf("当前环境未被赋予 CAP_LINUX_IMMUTABLE 能力，内核 VFS 拒绝 FS_IOC_SETFLAGS 设置高特权标志位 (符合预期)")
			} else {
				require.NoError(t, setErr)
			}
		} else {
			rf, err := getIoctlFlags(int(fRw.Fd()))
			require.NoError(t, err)
			require.Equal(t, fsAppendFl, rf)

			getResp, err := client.GetAttr(ctx, connect.NewRequest(&storagev1.GetAttrRequest{Ino: fileIno}))
			require.NoError(t, err)
			require.Equal(t, uint32(protoFlagAppend), getResp.Msg.GetInode().GetAttr().GetFlags())

			require.NoError(t, setIoctlFlags(int(fRw.Fd()), 0))
		}
	})

	t.Run("unsupported_flags_and_unknown_cmd", func(t *testing.T) {
		disk := createDiskWithRootInoGT1(t, client)
		mountpoint := t.TempDir()

		cmd, stdout, stderr := startBowlMount(t, endpoint, disk.GetDiskId(), mountpoint, "--debug")
		require.NoError(t, waitForMount(mountpoint, 10*time.Second), "挂载点就绪")
		defer func() {
			if t.Failed() {
				t.Logf("bowl stdout:\n%s", stdout.String())
				t.Logf("bowl stderr:\n%s", stderr.String())
			}
			stopBowlMountGracefully(t, cmd, mountpoint, 5*time.Second)
		}()

		filePath := filepath.Join(mountpoint, "unsupported-test.txt")
		require.NoError(t, os.WriteFile(filePath, []byte("data"), 0644))

		f, err := os.OpenFile(filePath, os.O_RDWR, 0)
		require.NoError(t, err)
		defer f.Close()

		// 传入不支持标志位 (0x00000040 FS_NODUMP_FL) 应返回 EOPNOTSUPP
		err = setIoctlFlags(int(f.Fd()), 0x00000040)
		require.True(t, errors.Is(err, syscall.EOPNOTSUPP), "不支持标志位应返回 EOPNOTSUPP: %v", err)

		// 发送未知 ioctl 命令（例如 unix.TIOCGWINSZ）应返回 ENOTTY
		_, err = unix.IoctlGetInt(int(f.Fd()), unix.TIOCGWINSZ)
		require.True(t, errors.Is(err, syscall.ENOTTY), "未知 ioctl 命令应返回 ENOTTY: %v", err)
	})

	t.Run("readonly_mount_setflags_returns_erofs", func(t *testing.T) {
		disk := createDiskWithRootInoGT1(t, client)

		// 预先通过 RPC 创建一个常规文件
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		_, err := client.Create(ctx, connect.NewRequest(&storagev1.CreateRequest{
			ParentIno: disk.GetRootIno(),
			Name:      []byte("ro-test.txt"),
			Mode:      0o644,
			Uid:       1000,
			Gid:       1000,
			Flags:     0,
		}))
		require.NoError(t, err)

		mountpoint := t.TempDir()
		cmd, _, _ := startBowlMount(t, endpoint, disk.GetDiskId(), mountpoint, "--ro", "--debug")
		require.NoError(t, waitForMount(mountpoint, 10*time.Second), "只读挂载点就绪")
		defer stopBowlMountGracefully(t, cmd, mountpoint, 5*time.Second)

		filePath := filepath.Join(mountpoint, "ro-test.txt")
		f, err := os.Open(filePath)
		require.NoError(t, err)
		defer f.Close()

		// 在 --ro 挂载下执行 SETFLAGS 应返回 EROFS
		err = setIoctlFlags(int(f.Fd()), fsImmutableFl)
		require.True(t, errors.Is(err, syscall.EROFS), "只读挂载下 SETFLAGS 应返回 EROFS: %v", err)

		// 在 --ro 挂载下执行 GETFLAGS 应正常返回 0
		readFlags, err := getIoctlFlags(int(f.Fd()))
		require.NoError(t, err)
		require.Equal(t, 0, readFlags, "只读挂载下 GETFLAGS 应正常读回 0")
	})
}
