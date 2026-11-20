//go:build e2e && linux

package e2e_test

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

// TestVFS_PosixRWDirectMmapExecAndTmpfile 验证 Linux FUSE 内核挂载下大文件跨分片读写、
// 并发 `O_APPEND`、`mmap` 共享写回、直接执行 ELF 二进制、`O_TMPFILE` 打洞具名化持久化与 `SetAttr` 语义。
func TestVFS_PosixRWDirectMmapExecAndTmpfile(t *testing.T) {
	client := newTestStorageClient(t)
	endpoint := getStorageEndpoint()
	disk := createTestDisk(t, client)
	diskID := disk.GetDiskId()
	mountpoint, stop := mountWithCleanup(t, endpoint, diskID, "--debug")

	// 1. 跨 4 MiB StageCoordinator 分片槽与 256 KiB Blob 块的大文件写入、跨分片覆写及 SHA-256 校验
	const totalSize = 10 * 1024 * 1024
	model := deterministicPayload("posix-10m", totalSize)
	largePath := filepath.Join(mountpoint, "large-10m.bin")
	require.NoError(t, os.WriteFile(largePath, model, 0o644))

	f, err := os.OpenFile(largePath, os.O_WRONLY, 0o644)
	require.NoError(t, err)
	patch := []byte("STRADDLE_4MIB_BOUNDARY_PATCH_1234567890")
	const patchOff = 4*1024*1024 - 10
	_, err = f.WriteAt(patch, patchOff)
	require.NoError(t, err)
	copy(model[patchOff:patchOff+len(patch)], patch)
	require.NoError(t, f.Sync())
	require.NoError(t, f.Close())

	gotLarge, err := os.ReadFile(largePath)
	require.NoError(t, err)
	require.Equal(t, sha256.Sum256(model), sha256.Sum256(gotLarge), "10 MiB 跨块覆写读回 SHA-256 必须一致")

	// 2. 并发 O_APPEND 原子追加
	appendPath := filepath.Join(mountpoint, "append.log")
	initAppend, err := os.OpenFile(appendPath, os.O_CREATE|os.O_WRONLY|os.O_EXCL, 0o644)
	require.NoError(t, err)
	require.NoError(t, initAppend.Close())

	firstA, err := os.OpenFile(appendPath, os.O_WRONLY|os.O_APPEND, 0o644)
	require.NoError(t, err)
	secondA, err := os.OpenFile(appendPath, os.O_WRONLY|os.O_APPEND, 0o644)
	require.NoError(t, err)
	const lineA = "W00-ATOMIC-APPEND-LINE"
	const lineB = "W01-ATOMIC-APPEND-LINE"
	errs := make(chan error, 2)
	go func() { _, wErr := firstA.WriteString(lineA + "\n"); errs <- wErr }()
	go func() { _, wErr := secondA.WriteString(lineB + "\n"); errs <- wErr }()
	require.NoError(t, errors.Join(<-errs, <-errs))
	require.NoError(t, firstA.Close())
	require.NoError(t, secondA.Close())

	appendContent, err := os.ReadFile(appendPath)
	require.NoError(t, err)
	records := strings.Split(strings.TrimRight(string(appendContent), "\n"), "\n")
	require.ElementsMatch(t, []string{lineA, lineB}, records)

	// 3. mmap MAP_SHARED 修改与 Msync(MS_SYNC) 落盘
	mmapPath := filepath.Join(mountpoint, "mmap.bin")
	require.NoError(t, os.WriteFile(mmapPath, bytes.Repeat([]byte{'A'}, 8192), 0o644))
	mf, err := os.OpenFile(mmapPath, os.O_RDWR, 0o644)
	require.NoError(t, err)
	mmapBuf, err := unix.Mmap(int(mf.Fd()), 0, 8192, unix.PROT_READ|unix.PROT_WRITE, unix.MAP_SHARED)
	require.NoError(t, err)
	copy(mmapBuf[100:111], "MMAP_MUTATE")
	copy(mmapBuf[5000:5011], "MMAP_PAGE2_")
	require.NoError(t, unix.Msync(mmapBuf, unix.MS_SYNC))
	require.NoError(t, unix.Munmap(mmapBuf))
	require.NoError(t, mf.Close())

	persistedMmap, err := os.ReadFile(mmapPath)
	require.NoError(t, err)
	require.Equal(t, []byte("MMAP_MUTATE"), persistedMmap[100:111])
	require.Equal(t, []byte("MMAP_PAGE2_"), persistedMmap[5000:5011])

	// 4. 在挂载点直接加载执行 Linux ELF 可执行文件
	binBytes, err := os.ReadFile(getBowlBin())
	require.NoError(t, err)
	execPath := filepath.Join(mountpoint, "bowl-exec")
	require.NoError(t, os.WriteFile(execPath, binBytes, 0o755))
	execOut, err := exec.Command(execPath, "version").CombinedOutput()
	require.NoError(t, err, "挂载点执行 ELF 二进制失败: %s", string(execOut))
	require.Contains(t, string(execOut), "bowl version")

	// 5. SetAttr：Chown 剥离 SUID/SGID、显式纳秒时间戳、Truncate 缩扩
	attrPath := filepath.Join(mountpoint, "setattr.txt")
	require.NoError(t, os.WriteFile(attrPath, bytes.Repeat([]byte("X"), 4096), 0o644))
	require.NoError(t, os.Chmod(attrPath, 0o755|os.ModeSetuid|os.ModeSetgid))
	require.NoError(t, os.Chown(attrPath, 1001, 1002))
	var stAttr syscall.Stat_t
	require.NoError(t, syscall.Stat(attrPath, &stAttr))
	require.Equal(t, uint32(1001), stAttr.Uid)
	require.Equal(t, uint32(1002), stAttr.Gid)
	require.Zero(t, stAttr.Mode&(syscall.S_ISUID|syscall.S_ISGID), "`Chown` 必须自动剥离 SUID/SGID")

	expAtime := unix.Timespec{Sec: 1680000000, Nsec: 123456000}
	expMtime := unix.Timespec{Sec: 1680000010, Nsec: 987654000}
	require.NoError(t, unix.UtimesNanoAt(unix.AT_FDCWD, attrPath, []unix.Timespec{expAtime, expMtime}, 0))
	require.NoError(t, syscall.Stat(attrPath, &stAttr))
	require.Equal(t, expMtime.Sec, stAttr.Mtim.Sec)
	require.Equal(t, expMtime.Nsec, stAttr.Mtim.Nsec)

	require.NoError(t, os.Truncate(attrPath, 8192))
	truncData, err := os.ReadFile(attrPath)
	require.NoError(t, err)
	require.Len(t, truncData, 8192)
	require.Equal(t, make([]byte, 4096), truncData[4096:])

	// 6. O_TMPFILE 写入 + PUNCH_HOLE 打洞 + Linkat 具名化并跨重挂持久化
	tmpFD, err := unix.Open(mountpoint, unix.O_TMPFILE|unix.O_RDWR, 0o600)
	require.NoError(t, err)
	tmpData := deterministicPayload("tmpfile-hole", 128*1024)
	writtenTmp, err := unix.Pwrite(tmpFD, tmpData, 0)
	require.NoError(t, err)
	require.Equal(t, len(tmpData), writtenTmp)
	require.NoError(t, unix.Fallocate(tmpFD, unix.FALLOC_FL_PUNCH_HOLE|unix.FALLOC_FL_KEEP_SIZE, 32*1024, 32*1024))
	clear(tmpData[32*1024 : 64*1024])

	namedTmp := filepath.Join(mountpoint, "tmpfile-named.bin")
	require.NoError(t, unix.Linkat(unix.AT_FDCWD, fmt.Sprintf("/proc/self/fd/%d", tmpFD), unix.AT_FDCWD, namedTmp, unix.AT_SYMLINK_FOLLOW))
	require.NoError(t, unix.Close(tmpFD))
	stop()

	remountMP, stopRemount := mountWithCleanup(t, endpoint, diskID)
	persistedTmp, err := os.ReadFile(filepath.Join(remountMP, "tmpfile-named.bin"))
	require.NoError(t, err)
	require.Equal(t, tmpData, persistedTmp, "重挂后 `O_TMPFILE` 具名化稀疏数据必须完整持久")
	stopRemount()
}

// TestVFS_PosixDirLinksRenameAndXattr 验证目录树与 `SGID` 继承、硬/软链接、
// `Rename`/`Renameat2`（`NOREPLACE`/`EXCHANGE`）与扩展属性（含 `security.capability` 写剥离）主路径。
func TestVFS_PosixDirLinksRenameAndXattr(t *testing.T) {
	client := newTestStorageClient(t)
	endpoint := getStorageEndpoint()
	disk := createTestDisk(t, client)
	mountpoint, stop := mountWithCleanup(t, endpoint, disk.GetDiskId(), "--debug")

	// 1. 目录 Nlink 守恒、SGID 属组继承、ReadDir 与 FIFO 节点
	var rootBefore unix.Stat_t
	require.NoError(t, unix.Stat(mountpoint, &rootBefore))

	sgidParent := filepath.Join(mountpoint, "sgid_parent")
	require.NoError(t, os.Mkdir(sgidParent, 0o755))
	require.NoError(t, os.Chown(sgidParent, 1000, 2000))
	require.NoError(t, os.Chmod(sgidParent, 0o755|os.ModeSetgid))

	var rootDuring unix.Stat_t
	require.NoError(t, unix.Stat(mountpoint, &rootDuring))
	require.Equal(t, rootBefore.Nlink+1, rootDuring.Nlink)

	sgidChild := filepath.Join(sgidParent, "child")
	require.NoError(t, os.Mkdir(sgidChild, 0o755))
	var childStat unix.Stat_t
	require.NoError(t, unix.Stat(sgidChild, &childStat))
	require.Equal(t, uint32(2000), childStat.Gid)
	require.NotZero(t, childStat.Mode&unix.S_ISGID)

	fifoPath := filepath.Join(mountpoint, "pipe.fifo")
	require.NoError(t, unix.Mkfifo(fifoPath, 0o640))
	var fifoStat unix.Stat_t
	require.NoError(t, unix.Stat(fifoPath, &fifoStat))
	require.Equal(t, uint32(syscall.S_IFIFO), fifoStat.Mode&syscall.S_IFMT)
	require.NoError(t, syscall.Unlink(fifoPath))

	// 2. 硬链接与符号链接
	origPath := filepath.Join(mountpoint, "orig.txt")
	require.NoError(t, os.WriteFile(origPath, []byte("link-payload"), 0o644))
	var origStat unix.Stat_t
	require.NoError(t, unix.Stat(origPath, &origStat))

	hardlinkPath := filepath.Join(sgidChild, "hardlink.txt")
	require.NoError(t, os.Link(origPath, hardlinkPath))
	var hardStat unix.Stat_t
	require.NoError(t, unix.Stat(hardlinkPath, &hardStat))
	require.Equal(t, origStat.Ino, hardStat.Ino)
	require.Equal(t, uint64(2), uint64(hardStat.Nlink))

	symPath := filepath.Join(mountpoint, "sym.txt")
	require.NoError(t, os.Symlink("orig.txt", symPath))
	target, err := os.Readlink(symPath)
	require.NoError(t, err)
	require.Equal(t, "orig.txt", target)
	symData, err := os.ReadFile(symPath)
	require.NoError(t, err)
	require.Equal(t, []byte("link-payload"), symData)

	// 3. 跨目录 Rename、RENAME_NOREPLACE 与 RENAME_EXCHANGE
	movedPath := filepath.Join(sgidParent, "moved.txt")
	require.NoError(t, os.Rename(origPath, movedPath))
	require.ErrorIs(t, unix.Renameat2(unix.AT_FDCWD, movedPath, unix.AT_FDCWD, hardlinkPath, unix.RENAME_NOREPLACE), syscall.EEXIST)

	swapDir := filepath.Join(mountpoint, "swap_dir")
	require.NoError(t, os.Mkdir(swapDir, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(swapDir, "inside.txt"), []byte("in"), 0o644))
	require.NoError(t, unix.Renameat2(unix.AT_FDCWD, movedPath, unix.AT_FDCWD, swapDir, unix.RENAME_EXCHANGE))
	swapInfo, err := os.Stat(movedPath)
	require.NoError(t, err)
	require.True(t, swapInfo.IsDir())
	_, err = os.Stat(filepath.Join(movedPath, "inside.txt"))
	require.NoError(t, err)

	// 4. 扩展属性增删改查与 security.capability 写剥离及重挂持久化
	xattrFile := filepath.Join(mountpoint, "xattr.bin")
	require.NoError(t, os.WriteFile(xattrFile, []byte("base"), 0o755))
	binVal := []byte("prefix\x00middle\xff\x00suffix")
	require.NoError(t, unix.Setxattr(xattrFile, "user.bin", binVal, unix.XATTR_CREATE))
	gotVal := make([]byte, len(binVal)+4)
	n, err := unix.Getxattr(xattrFile, "user.bin", gotVal)
	require.NoError(t, err)
	require.Equal(t, binVal, gotVal[:n])

	capVal := []byte{0x01, 0x00, 0x00, 0x02, 0x00, 0x20, 0x00, 0x00, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}
	requireXattrAbsent(t, xattrFile, "security.capability")
	require.NoError(t, unix.Setxattr(xattrFile, "security.capability", capVal, 0))
	requireXattrPresent(t, xattrFile, "security.capability")
	require.NoError(t, os.WriteFile(xattrFile, []byte("killpriv"), 0o755))
	requireXattrAbsent(t, xattrFile, "security.capability")
	stop()

	remountMP, stopRemount := mountWithCleanup(t, endpoint, disk.GetDiskId())
	remountXattr := filepath.Join(remountMP, "xattr.bin")
	requireXattrAbsent(t, remountXattr, "security.capability")
	nAfter, err := unix.Getxattr(remountXattr, "user.bin", gotVal)
	require.NoError(t, err)
	require.Equal(t, binVal, gotVal[:nAfter])
	require.NoError(t, unix.Removexattr(remountXattr, "user.bin"))
	requireXattrAbsent(t, remountXattr, "user.bin")
	stopRemount()
}

// TestVFS_RenameDirNlinkAndParentCoherence verifies parent st_nlink and ".." coherence across
// same-parent empty-dir overwrite, cross-parent empty-dir overwrite, and cross-parent RENAME_EXCHANGE.
func TestVFS_RenameDirNlinkAndParentCoherence(t *testing.T) {
	client := newTestStorageClient(t)
	endpoint := getStorageEndpoint()
	disk := createTestDisk(t, client)
	mountpoint, stop := mountWithCleanup(t, endpoint, disk.GetDiskId())
	defer stop()

	statOf := func(p string) unix.Stat_t {
		t.Helper()
		var st unix.Stat_t
		require.NoError(t, unix.Stat(p, &st), "stat %s", p)
		return st
	}

	// 1. Same-parent directory overwrite of empty directory decrements parent nlink (4 -> 3).
	sameParent := filepath.Join(mountpoint, "same-parent")
	require.NoError(t, os.Mkdir(sameParent, 0o755))
	sameSrc := filepath.Join(sameParent, "src")
	sameDst := filepath.Join(sameParent, "dst")
	require.NoError(t, os.Mkdir(sameSrc, 0o755))
	require.NoError(t, os.Mkdir(sameDst, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(sameSrc, "keep.txt"), []byte("ok"), 0o644))
	require.Equal(t, uint64(4), uint64(statOf(sameParent).Nlink))

	require.NoError(t, unix.Rename(sameSrc, sameDst))
	require.Equal(t, uint64(3), uint64(statOf(sameParent).Nlink), "same-parent dir overwrite must decrement parent nlink")
	require.Equal(t, statOf(sameParent).Ino, statOf(filepath.Join(sameDst, "..")).Ino)
	gotKeep, err := os.ReadFile(filepath.Join(sameDst, "keep.txt"))
	require.NoError(t, err)
	require.Equal(t, []byte("ok"), gotKeep)

	// 2. Cross-parent directory overwrite of empty directory decrements old parent (3 -> 2) and keeps new parent unchanged (3 -> 3).
	crossOld := filepath.Join(mountpoint, "cross-old")
	crossNew := filepath.Join(mountpoint, "cross-new")
	require.NoError(t, os.Mkdir(crossOld, 0o755))
	require.NoError(t, os.Mkdir(crossNew, 0o755))
	crossSrc := filepath.Join(crossOld, "moving")
	crossDst := filepath.Join(crossNew, "victim")
	require.NoError(t, os.Mkdir(crossSrc, 0o755))
	require.NoError(t, os.Mkdir(crossDst, 0o755))
	require.Equal(t, uint64(3), uint64(statOf(crossOld).Nlink))
	require.Equal(t, uint64(3), uint64(statOf(crossNew).Nlink))

	require.NoError(t, unix.Rename(crossSrc, crossDst))
	require.Equal(t, uint64(2), uint64(statOf(crossOld).Nlink), "cross-parent dir overwrite must decrement old parent nlink")
	require.Equal(t, uint64(3), uint64(statOf(crossNew).Nlink), "cross-parent dir overwrite must keep new parent nlink unchanged")
	require.Equal(t, statOf(crossNew).Ino, statOf(filepath.Join(crossDst, "..")).Ino)

	// 3. Cross-parent RENAME_EXCHANGE between dir and file updates both parents' nlink and ".." in both directions.
	exOld := filepath.Join(mountpoint, "ex-old")
	exNew := filepath.Join(mountpoint, "ex-new")
	require.NoError(t, os.Mkdir(exOld, 0o755))
	require.NoError(t, os.Mkdir(exNew, 0o755))
	exA := filepath.Join(exOld, "slot-a")
	exB := filepath.Join(exNew, "slot-b")
	require.NoError(t, os.Mkdir(exA, 0o755))
	require.NoError(t, os.WriteFile(exB, []byte("file-b"), 0o644))
	require.Equal(t, uint64(3), uint64(statOf(exOld).Nlink))
	require.Equal(t, uint64(2), uint64(statOf(exNew).Nlink))

	require.NoError(t, unix.Renameat2(unix.AT_FDCWD, exA, unix.AT_FDCWD, exB, unix.RENAME_EXCHANGE))
	require.Equal(t, uint64(2), uint64(statOf(exOld).Nlink), "dir->file cross-parent exchange must decrement old parent nlink")
	require.Equal(t, uint64(3), uint64(statOf(exNew).Nlink), "dir->file cross-parent exchange must increment new parent nlink")
	require.Equal(t, statOf(exNew).Ino, statOf(filepath.Join(exB, "..")).Ino)

	require.NoError(t, unix.Renameat2(unix.AT_FDCWD, exA, unix.AT_FDCWD, exB, unix.RENAME_EXCHANGE))
	require.Equal(t, uint64(3), uint64(statOf(exOld).Nlink), "file->dir cross-parent exchange must restore old parent nlink")
	require.Equal(t, uint64(2), uint64(statOf(exNew).Nlink), "file->dir cross-parent exchange must restore new parent nlink")
	require.Equal(t, statOf(exOld).Ino, statOf(filepath.Join(exA, "..")).Ino)
}

// TestVFS_PosixFlockAndMultiUserPermissions 验证内核 `flock` 跨独立句柄及 `dup` 互斥语义，
// 以及 `--allow-other` 挂载下的多用户权限与 Sticky Bit 隔离。
func TestVFS_PosixFlockAndMultiUserPermissions(t *testing.T) {
	client := newTestStorageClient(t)
	disk := createTestDisk(t, client)

	mountpoint := t.TempDir()
	require.NoError(t, os.Chmod(filepath.Dir(mountpoint), 0o755))
	require.NoError(t, os.Chmod(mountpoint, 0o755))
	cmd, _, stderr := startBowlMount(t, getStorageEndpoint(), disk.GetDiskId(), mountpoint, "--allow-other")
	waitForMount(t, mountpoint, "挂载失败: %s", stderr.String())
	t.Cleanup(func() { stopBowlMountGracefully(t, cmd, mountpoint) })

	// 1. flock 跨独立打开句柄与 dup 描述符互斥验证
	flockPath := filepath.Join(mountpoint, "flock.txt")
	require.NoError(t, os.WriteFile(flockPath, nil, 0o644))
	fA, err := os.OpenFile(flockPath, os.O_RDWR, 0)
	require.NoError(t, err)
	fB, err := os.OpenFile(flockPath, os.O_RDWR, 0)
	require.NoError(t, err)
	t.Cleanup(func() { _ = fB.Close() })

	require.NoError(t, unix.Flock(int(fA.Fd()), unix.LOCK_EX|unix.LOCK_NB))
	dupFD, err := unix.Dup(int(fA.Fd()))
	require.NoError(t, err)
	require.ErrorIs(t, unix.Flock(int(fB.Fd()), unix.LOCK_EX|unix.LOCK_NB), syscall.EAGAIN)
	require.NoError(t, unix.Close(dupFD))
	require.ErrorIs(t, unix.Flock(int(fB.Fd()), unix.LOCK_EX|unix.LOCK_NB), syscall.EAGAIN)
	require.NoError(t, fA.Close())
	require.Eventually(t, func() bool {
		return unix.Flock(int(fB.Fd()), unix.LOCK_EX|unix.LOCK_NB) == nil
	}, 5*time.Second, 10*time.Millisecond, "关闭全部持锁描述符后 `fB` 必须成功获取排他 `flock`")

	// 2. 多用户权限与 Sticky Bit 隔离
	secretPath := filepath.Join(mountpoint, "secret.txt")
	require.NoError(t, os.WriteFile(secretPath, []byte("ROOT_SECRET"), 0o600))
	roDir := filepath.Join(mountpoint, "ro-dir")
	require.NoError(t, os.Mkdir(roDir, 0o755))
	stickyDir := filepath.Join(mountpoint, "sticky")
	require.NoError(t, os.Mkdir(stickyDir, 0o777|os.ModeSticky))
	require.NoError(t, unix.Chmod(stickyDir, 0o1777))

	codeRead, _, errRead := runAs(t, 1000, 1000, "cat", secretPath)
	require.Equal(t, 1, codeRead)
	require.Contains(t, errRead, "Permission denied")

	codeWrite, _, errWrite := runAsWithStdin(t, 1000, 1000, "x\n", "tee", "-a", secretPath)
	require.Equal(t, 1, codeWrite)
	require.Contains(t, errWrite, "Permission denied")

	codeCreateRO, _, errCreateRO := runAs(t, 1000, 1000, "touch", filepath.Join(roDir, "denied.txt"))
	require.Equal(t, 1, codeCreateRO)
	require.Contains(t, errCreateRO, "Permission denied")

	u1000File := filepath.Join(stickyDir, "u1000.txt")
	codeCreateSticky, _, errCreateSticky := runAs(t, 1000, 1000, "touch", u1000File)
	require.Equal(t, 0, codeCreateSticky, "uid=1000 在 01777 目录下创建失败: %s", errCreateSticky)

	codeRemoveOther, _, errRemoveOther := runAs(t, 1001, 1001, "rm", u1000File)
	require.Equal(t, 1, codeRemoveOther)
	require.Contains(t, errRemoveOther, "Operation not permitted")
	require.FileExists(t, u1000File)

	codeRenameOther, _, errRenameOther := runAs(t, 1001, 1001, "mv", u1000File, filepath.Join(stickyDir, "stolen.txt"))
	require.Equal(t, 1, codeRenameOther)
	require.Contains(t, errRenameOther, "Operation not permitted")

	codeRemoveOwn, _, errRemoveOwn := runAs(t, 1000, 1000, "rm", u1000File)
	require.Equal(t, 0, codeRemoveOwn, "属主删除失败: %s", errRemoveOwn)
	require.NoFileExists(t, u1000File)
}

func readXattrNames(t *testing.T, targetPath string) []string {
	t.Helper()
	size, err := unix.Listxattr(targetPath, nil)
	require.NoError(t, err)
	buf := make([]byte, size)
	read, err := unix.Listxattr(targetPath, buf)
	require.NoError(t, err)
	return strings.Split(strings.TrimSuffix(string(buf[:read]), "\x00"), "\x00")
}

func requireXattrPresent(t *testing.T, targetPath, name string) {
	t.Helper()
	require.Contains(t, readXattrNames(t, targetPath), name)
}

func requireXattrAbsent(t *testing.T, targetPath, name string) {
	t.Helper()
	require.NotContains(t, readXattrNames(t, targetPath), name)
}
