//go:build e2e && linux

package e2e_test

import (
	"bytes"
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

func TestE2E_SetAttr(t *testing.T) {
	client := newTestStorageClient(t)
	endpoint := getStorageEndpoint()

	t.Run("chmod_and_chown_strip_privilege_bits", func(t *testing.T) {
		disk := createDiskWithRootInoGT1(t, client)
		mountpoint := t.TempDir()

		cmd, _, _ := startBowlMount(t, endpoint, disk.GetDiskId(), mountpoint, "--debug")
		require.NoError(t, waitForMount(mountpoint, 10*time.Second), "挂载点就绪")
		defer stopBowlMountGracefully(t, cmd, mountpoint, 5*time.Second)

		filePath := filepath.Join(mountpoint, "priv-test.txt")
		require.NoError(t, os.WriteFile(filePath, []byte("privileged content"), 0644))

		// 1. 设置特权位 S_ISUID | S_ISGID (04755)
		require.NoError(t, os.Chmod(filePath, 0755|os.ModeSetuid|os.ModeSetgid))
		info, err := os.Stat(filePath)
		require.NoError(t, err)
		require.Equal(t, os.FileMode(0755), info.Mode().Perm())
		require.True(t, (info.Mode()&os.ModeSetuid) != 0, "Setuid 特权位必须存在")
		require.True(t, (info.Mode()&os.ModeSetgid) != 0, "Setgid 特权位必须存在")

		// 2. 执行 chown，验证服务端遵循 POSIX 规范自动剥离特权位
		require.NoError(t, os.Chown(filePath, 1001, 1002))
		infoAfterChown, err := os.Stat(filePath)
		require.NoError(t, err)
		require.Equal(t, os.FileMode(0755), infoAfterChown.Mode().Perm())
		require.Equal(t, os.FileMode(0), infoAfterChown.Mode()&(os.ModeSetuid|os.ModeSetgid), "chown 后 Setuid/Setgid 特权位必须被自动剥离")

		var st syscall.Stat_t
		require.NoError(t, syscall.Stat(filePath, &st))
		require.Equal(t, uint32(1001), st.Uid)
		require.Equal(t, uint32(1002), st.Gid)
		require.Equal(t, uint32(0), uint32(st.Mode)&(syscall.S_ISUID|syscall.S_ISGID), "内核 stat 的 Mode 也必须剥离特权位")

		// 3. 再次显式 chmod 为普通权限
		require.NoError(t, os.Chmod(filePath, 0600))
		infoFinal, err := os.Stat(filePath)
		require.NoError(t, err)
		require.Equal(t, os.FileMode(0600), infoFinal.Mode().Perm())
	})

	t.Run("utimensat_explicit_and_now", func(t *testing.T) {
		disk := createDiskWithRootInoGT1(t, client)
		mountpoint := t.TempDir()

		cmd, _, _ := startBowlMount(t, endpoint, disk.GetDiskId(), mountpoint, "--debug")
		require.NoError(t, waitForMount(mountpoint, 10*time.Second), "挂载点就绪")
		defer stopBowlMountGracefully(t, cmd, mountpoint, 5*time.Second)

		filePath := filepath.Join(mountpoint, "time-test.txt")
		require.NoError(t, os.WriteFile(filePath, []byte("time stamps"), 0644))

		// 1. 显式设置纳秒时间戳（PostgreSQL TIMESTAMPTZ 为微秒精度，纳秒末三位微秒对齐）
		expAtime := unix.Timespec{Sec: 1680000000, Nsec: 123456000}
		expMtime := unix.Timespec{Sec: 1680000010, Nsec: 987654000}
		require.NoError(t, unix.UtimesNanoAt(unix.AT_FDCWD, filePath, []unix.Timespec{expAtime, expMtime}, 0))

		var st syscall.Stat_t
		require.NoError(t, syscall.Stat(filePath, &st))
		require.Equal(t, expAtime.Sec, int64(st.Atim.Sec))
		require.Equal(t, expAtime.Nsec, int64(st.Atim.Nsec))
		require.Equal(t, expMtime.Sec, int64(st.Mtim.Sec))
		require.Equal(t, expMtime.Nsec, int64(st.Mtim.Nsec))

		// 2. 使用 UTIME_NOW 更新时间戳
		nowBefore := time.Now().Unix()
		timesNow := []unix.Timespec{
			{Sec: 0, Nsec: unix.UTIME_NOW},
			{Sec: 0, Nsec: unix.UTIME_NOW},
		}
		require.NoError(t, unix.UtimesNanoAt(unix.AT_FDCWD, filePath, timesNow, 0))

		var stNow syscall.Stat_t
		require.NoError(t, syscall.Stat(filePath, &stNow))
		require.GreaterOrEqual(t, int64(stNow.Atim.Sec), nowBefore-2)
		require.GreaterOrEqual(t, int64(stNow.Mtim.Sec), nowBefore-2)
	})

	t.Run("truncate_and_ftruncate_sparse_and_shrink", func(t *testing.T) {
		disk := createDiskWithRootInoGT1(t, client)
		mountpoint := t.TempDir()

		cmd, _, _ := startBowlMount(t, endpoint, disk.GetDiskId(), mountpoint, "--debug")
		require.NoError(t, waitForMount(mountpoint, 10*time.Second), "挂载点就绪")
		defer stopBowlMountGracefully(t, cmd, mountpoint, 5*time.Second)

		filePath := filepath.Join(mountpoint, "trunc-test.txt")

		// 初始写入 4096 字节非零数据
		initialData := bytes.Repeat([]byte("A"), 4096)
		require.NoError(t, os.WriteFile(filePath, initialData, 0644))

		// 1. os.Truncate 扩容至 16384 字节（稀疏空洞填充零）
		require.NoError(t, os.Truncate(filePath, 16384))
		info, err := os.Stat(filePath)
		require.NoError(t, err)
		require.Equal(t, int64(16384), info.Size())

		readBack, err := os.ReadFile(filePath)
		require.NoError(t, err)
		require.Len(t, readBack, 16384)
		require.Equal(t, initialData, readBack[:4096])
		require.Equal(t, make([]byte, 16384-4096), readBack[4096:], "扩容区间必须全部为全零字节")

		// 2. 打开句柄通过 f.Truncate 进行句柄绑定的缩容截断
		f, err := os.OpenFile(filePath, os.O_RDWR, 0644)
		require.NoError(t, err)
		defer func() {
			if f != nil {
				_ = f.Close()
			}
		}()

		require.NoError(t, f.Truncate(2048))
		infoShrink, err := f.Stat()
		require.NoError(t, err)
		require.Equal(t, int64(2048), infoShrink.Size())

		bufShrink := make([]byte, 2048)
		_, err = f.ReadAt(bufShrink, 0)
		require.NoError(t, err)
		require.Equal(t, bytes.Repeat([]byte("A"), 2048), bufShrink)

		// 3. 句柄再次扩容截断
		require.NoError(t, f.Truncate(8192))
		infoGrow, err := f.Stat()
		require.NoError(t, err)
		require.Equal(t, int64(8192), infoGrow.Size())

		bufGrow := make([]byte, 8192)
		_, err = f.ReadAt(bufGrow, 0)
		require.NoError(t, err)
		require.Equal(t, bytes.Repeat([]byte("A"), 2048), bufGrow[:2048])
		require.Equal(t, make([]byte, 8192-2048), bufGrow[2048:])

		require.NoError(t, f.Close())
		f = nil
	})

	t.Run("fallocate_modes_and_punch_hole", func(t *testing.T) {
		disk := createDiskWithRootInoGT1(t, client)
		mountpoint := t.TempDir()

		cmd, _, _ := startBowlMount(t, endpoint, disk.GetDiskId(), mountpoint, "--debug")
		require.NoError(t, waitForMount(mountpoint, 10*time.Second), "挂载点就绪")
		defer stopBowlMountGracefully(t, cmd, mountpoint, 5*time.Second)

		filePath := filepath.Join(mountpoint, "falloc-test.bin")
		f, err := os.OpenFile(filePath, os.O_CREATE|os.O_RDWR, 0644)
		require.NoError(t, err)
		defer func() {
			if f != nil {
				_ = f.Close()
			}
		}()

		// 构造 32 KiB 测试数据（前 8 KiB 为 'A'，中 8 KiB 为 'B'，后 16 KiB 为 'C'）
		partA := bytes.Repeat([]byte{'A'}, 8192)
		partB := bytes.Repeat([]byte{'B'}, 8192)
		partC := bytes.Repeat([]byte{'C'}, 16384)
		var initialData []byte
		initialData = append(initialData, partA...)
		initialData = append(initialData, partB...)
		initialData = append(initialData, partC...)
		_, err = f.Write(initialData)
		require.NoError(t, err)

		// 1. 默认 mode 0 扩容分配至 64 KiB
		require.NoError(t, unix.Fallocate(int(f.Fd()), 0, 0, 64*1024))
		st1, err := f.Stat()
		require.NoError(t, err)
		require.Equal(t, int64(64*1024), st1.Size())

		read64k := make([]byte, 64*1024)
		_, err = f.ReadAt(read64k, 0)
		require.NoError(t, err)
		require.Equal(t, initialData, read64k[:32*1024])
		require.Equal(t, make([]byte, 32*1024), read64k[32*1024:])

		// 2. FALLOC_FL_KEEP_SIZE (0x1) 预留空间，不改变文件逻辑大小
		require.NoError(t, unix.Fallocate(int(f.Fd()), unix.FALLOC_FL_KEEP_SIZE, 0, 128*1024))
		st2, err := f.Stat()
		require.NoError(t, err)
		require.Equal(t, int64(64*1024), st2.Size(), "KEEP_SIZE 模式下文件逻辑大小不得改变")

		// 3. 打洞 FALLOC_FL_PUNCH_HOLE | FALLOC_FL_KEEP_SIZE (0x3)：
		// 对 offset 8192, len 8192 (原 'B' 区域) 执行打洞
		require.NoError(t, unix.Fallocate(int(f.Fd()), unix.FALLOC_FL_PUNCH_HOLE|unix.FALLOC_FL_KEEP_SIZE, 8192, 8192))
		st3, err := f.Stat()
		require.NoError(t, err)
		require.Equal(t, int64(64*1024), st3.Size(), "打洞后文件大小保持不变")

		// 验证打洞区间变为全零，前后两侧数据完好
		readPunched := make([]byte, 32*1024)
		_, err = f.ReadAt(readPunched, 0)
		require.NoError(t, err)
		require.Equal(t, partA, readPunched[:8192], "打洞前侧数据保持不变")
		require.Equal(t, make([]byte, 8192), readPunched[8192:16384], "打洞区间必须读取为全零字节")
		require.Equal(t, partC, readPunched[16384:32*1024], "打洞后侧数据保持不变")

		// 4. 不支持模式（如 FALLOC_FL_ZERO_RANGE 0x10 或单独 PUNCH_HOLE 0x02）返回 EOPNOTSUPP
		errZeroRange := unix.Fallocate(int(f.Fd()), unix.FALLOC_FL_ZERO_RANGE, 0, 4096)
		require.Error(t, errZeroRange)
		require.True(t, errors.Is(errZeroRange, syscall.EOPNOTSUPP) || errors.Is(errZeroRange, unix.EOPNOTSUPP), "不支持的模式必须返回 EOPNOTSUPP")

		errPunchSolo := unix.Fallocate(int(f.Fd()), unix.FALLOC_FL_PUNCH_HOLE, 0, 4096)
		require.Error(t, errPunchSolo)
		require.True(t, errors.Is(errPunchSolo, syscall.EOPNOTSUPP) || errors.Is(errPunchSolo, unix.EOPNOTSUPP), "单独 PUNCH_HOLE 必须返回 EOPNOTSUPP")

		require.NoError(t, f.Close())
		f = nil
	})

	t.Run("readonly_mount_blocks_mutations", func(t *testing.T) {
		disk := createDiskWithRootInoGT1(t, client)

		// 1. 服务端先创建目标文件
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		createResp, err := client.Create(ctx, connect.NewRequest(&storagev1.CreateRequest{
			ParentIno: disk.GetRootIno(),
			Name:      []byte("ro-test.txt"),
			Mode:      0o644 | syscall.S_IFREG,
			Uid:       1000,
			Gid:       1000,
			Flags:     0o2,
		}))
		require.NoError(t, err)

		// 释放服务端打开句柄
		_, _ = client.Release(ctx, connect.NewRequest(&storagev1.ReleaseRequest{
			Ino: createResp.Msg.GetInode().GetIno(),
			Fh:  createResp.Msg.GetFh(),
		}))

		// 2. 以 --ro 挂载
		mountpoint := t.TempDir()
		cmd, _, _ := startBowlMount(t, endpoint, disk.GetDiskId(), mountpoint, "--debug", "--ro")
		require.NoError(t, waitForMount(mountpoint, 10*time.Second), "挂载点就绪")
		defer stopBowlMountGracefully(t, cmd, mountpoint, 5*time.Second)

		filePath := filepath.Join(mountpoint, "ro-test.txt")

		// 3. 只读挂载下 chmod 必须返回 EROFS
		errChmod := os.Chmod(filePath, 0600)
		require.Error(t, errChmod)
		require.True(t, errors.Is(errChmod, syscall.EROFS), "只读模式下 chmod 必须返回 EROFS")

		// 4. 只读挂载下 truncate 必须返回 EROFS
		errTrunc := os.Truncate(filePath, 1024)
		require.Error(t, errTrunc)
		require.True(t, errors.Is(errTrunc, syscall.EROFS), "只读模式下 truncate 必须返回 EROFS")

		// 5. 只读挂载下打开常规文件并尝试 fallocate 必须被拦截返回 EROFS 或写句柄拒绝
		fRO, errOpenRO := os.Open(filePath)
		require.NoError(t, errOpenRO)
		defer fRO.Close()

		errFalloc := unix.Fallocate(int(fRO.Fd()), 0, 0, 4096)
		require.Error(t, errFalloc)
		require.True(t, errors.Is(errFalloc, syscall.EROFS) || errors.Is(errFalloc, syscall.EBADF), "只读模式下 fallocate 必须返回 EROFS 或 EBADF")
	})
}
