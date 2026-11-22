//go:build e2e && linux

package e2e_test

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

func TestVFS_PosixRWDirectMmapExecAndTmpfile(t *testing.T) {
	client := newTestStorageClient(t)
	endpoint := getStorageEndpoint()
	disk := createTestDisk(t, client)
	diskID := disk.GetDiskId()
	mountpoint, stop := mountWithCleanup(t, endpoint, diskID, "--debug")

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

	binBytes, err := os.ReadFile(getBowlBin())
	require.NoError(t, err)
	execPath := filepath.Join(mountpoint, "bowl-exec")
	require.NoError(t, os.WriteFile(execPath, binBytes, 0o755))
	execOut, err := exec.Command(execPath, "version").CombinedOutput()
	require.NoError(t, err, "挂载点执行 ELF 二进制失败: %s", string(execOut))
	require.Contains(t, string(execOut), "bowl version")

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

func TestVFS_PosixDirLinksRenameAndXattr(t *testing.T) {
	client := newTestStorageClient(t)
	endpoint := getStorageEndpoint()
	disk := createTestDisk(t, client)
	mountpoint, stop := mountWithCleanup(t, endpoint, disk.GetDiskId(), "--debug")

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

func TestVFS_PosixFlockAndMultiUserPermissions(t *testing.T) {
	client := newTestStorageClient(t)
	disk := createTestDisk(t, client)

	mountpoint := t.TempDir()
	require.NoError(t, os.Chmod(filepath.Dir(mountpoint), 0o755))
	require.NoError(t, os.Chmod(mountpoint, 0o755))
	cmd, _, stderr := startBowlMount(t, getStorageEndpoint(), disk.GetDiskId(), mountpoint, "--allow-other")
	waitForMount(t, mountpoint, "挂载失败: %s", stderr.String())
	t.Cleanup(func() { stopBowlMountGracefully(t, cmd, mountpoint) })

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

func parseLockTypeChar(s string) (int16, error) {
	switch s {
	case "R":
		return unix.F_RDLCK, nil
	case "W":
		return unix.F_WRLCK, nil
	case "U":
		return unix.F_UNLCK, nil
	default:
		return 0, fmt.Errorf("unknown lock type: %s", s)
	}
}

func formatErrnoReply(err error) string {
	if err == nil {
		return "OK"
	}
	if errno, ok := errors.AsType[syscall.Errno](err); ok {
		return fmt.Sprintf("ERR %d", int(errno))
	}
	return "ERR_MSG " + err.Error()
}

func TestLockWorkerHelper(_ *testing.T) {
	if os.Getenv("BOWL_E2E_LOCK_WORKER") != "1" {
		return
	}
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()

	fds := make(map[string]int)
	defer func() {
		for _, fd := range fds {
			_ = unix.Close(fd)
		}
	}()

	_, _ = fmt.Fprintf(os.Stdout, "READY %d %d\n", os.Getpid(), unix.Gettid())

	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		fields := strings.Fields(scanner.Text())
		if len(fields) == 0 {
			continue
		}
		switch fields[0] {
		case "open":
			if len(fields) != 3 {
				_, _ = fmt.Fprintln(os.Stdout, "ERR_MSG bad open args")
				continue
			}
			fd, err := unix.Open(fields[2], unix.O_RDWR, 0)
			if err != nil {
				_, _ = fmt.Fprintln(os.Stdout, formatErrnoReply(err))
				continue
			}
			fds[fields[1]] = fd
			_, _ = fmt.Fprintln(os.Stdout, "OK")
		case "close":
			if len(fields) != 2 {
				_, _ = fmt.Fprintln(os.Stdout, "ERR_MSG bad close args")
				continue
			}
			fd, ok := fds[fields[1]]
			if !ok {
				_, _ = fmt.Fprintln(os.Stdout, "ERR_MSG unknown key")
				continue
			}
			delete(fds, fields[1])
			_, _ = fmt.Fprintln(os.Stdout, formatErrnoReply(unix.Close(fd)))
		case "setlk", "setlkw":
			if len(fields) != 5 {
				_, _ = fmt.Fprintln(os.Stdout, "ERR_MSG bad setlk args")
				continue
			}
			fd, ok := fds[fields[1]]
			if !ok {
				_, _ = fmt.Fprintln(os.Stdout, "ERR_MSG unknown key")
				continue
			}
			lkTyp, err := parseLockTypeChar(fields[2])
			if err != nil {
				_, _ = fmt.Fprintln(os.Stdout, "ERR_MSG "+err.Error())
				continue
			}
			start, err1 := strconv.ParseInt(fields[3], 10, 64)
			length, err2 := strconv.ParseInt(fields[4], 10, 64)
			if err1 != nil || err2 != nil {
				_, _ = fmt.Fprintln(os.Stdout, "ERR_MSG bad range")
				continue
			}
			cmd := unix.F_SETLK
			if fields[0] == "setlkw" {
				cmd = unix.F_SETLKW
				_, _ = fmt.Fprintln(os.Stdout, "WAITING")
			}
			lk := unix.Flock_t{
				Type:   lkTyp,
				Whence: io.SeekStart,
				Start:  start,
				Len:    length,
			}
			_, _ = fmt.Fprintln(os.Stdout, formatErrnoReply(unix.FcntlFlock(uintptr(fd), cmd, &lk)))
		case "getlk":
			if len(fields) != 5 {
				_, _ = fmt.Fprintln(os.Stdout, "ERR_MSG bad getlk args")
				continue
			}
			fd, ok := fds[fields[1]]
			if !ok {
				_, _ = fmt.Fprintln(os.Stdout, "ERR_MSG unknown key")
				continue
			}
			lkTyp, err := parseLockTypeChar(fields[2])
			if err != nil {
				_, _ = fmt.Fprintln(os.Stdout, "ERR_MSG "+err.Error())
				continue
			}
			start, err1 := strconv.ParseInt(fields[3], 10, 64)
			length, err2 := strconv.ParseInt(fields[4], 10, 64)
			if err1 != nil || err2 != nil {
				_, _ = fmt.Fprintln(os.Stdout, "ERR_MSG bad range")
				continue
			}
			lk := unix.Flock_t{
				Type:   lkTyp,
				Whence: io.SeekStart,
				Start:  start,
				Len:    length,
			}
			if err := unix.FcntlFlock(uintptr(fd), unix.F_GETLK, &lk); err != nil {
				_, _ = fmt.Fprintln(os.Stdout, formatErrnoReply(err))
				continue
			}
			_, _ = fmt.Fprintf(os.Stdout, "LOCK %d %d %d %d\n", lk.Type, lk.Pid, lk.Start, lk.Len)
		case "quit":
			_, _ = fmt.Fprintln(os.Stdout, "BYE")
			return
		default:
			_, _ = fmt.Fprintln(os.Stdout, "ERR_MSG unknown cmd")
		}
	}
}

type lockWorkerProc struct {
	cmd    *exec.Cmd
	cancel context.CancelFunc
	stdin  io.WriteCloser
	lines  <-chan string
	stderr *bytes.Buffer
	pid    int
	tid    int
}

func startLockWorker(t *testing.T) *lockWorkerProc {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), helperProcessTimeout)
	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestLockWorkerHelper$")
	cmd.Env = append(os.Environ(), "BOWL_E2E_LOCK_WORKER=1")
	stdin, err := cmd.StdinPipe()
	require.NoError(t, err)
	stdout, err := cmd.StdoutPipe()
	require.NoError(t, err)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	require.NoError(t, cmd.Start())

	lines := make(chan string, 16)
	go func() {
		defer close(lines)
		sc := bufio.NewScanner(stdout)
		for sc.Scan() {
			lines <- sc.Text()
		}
	}()

	w := &lockWorkerProc{
		cmd:    cmd,
		cancel: cancel,
		stdin:  stdin,
		lines:  lines,
		stderr: &stderr,
	}
	t.Cleanup(func() {
		_ = w.stdin.Close()
		w.cancel()
		_ = w.cmd.Wait()
	})

	readyLine := w.readLineWithTimeout(t, 5*time.Second, "worker READY")
	fields := strings.Fields(readyLine)
	require.Len(t, fields, 3, "unexpected READY line=%q stderr=%s", readyLine, stderr.String())
	require.Equal(t, "READY", fields[0])
	w.pid, err = strconv.Atoi(fields[1])
	require.NoError(t, err)
	w.tid, err = strconv.Atoi(fields[2])
	require.NoError(t, err)
	return w
}

func (w *lockWorkerProc) send(t *testing.T, line string) {
	t.Helper()
	_, err := io.WriteString(w.stdin, line+"\n")
	require.NoError(t, err)
}

func (w *lockWorkerProc) readLineWithTimeout(t *testing.T, timeout time.Duration, label string) string {
	t.Helper()
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case line, ok := <-w.lines:
		require.True(t, ok, "%s: worker exited early stderr=%s", label, w.stderr.String())
		return line
	case <-timer.C:
		w.cancel()
		t.Fatalf("%s: timed out after %v stderr=%s", label, timeout, w.stderr.String())
		return ""
	}
}

func (w *lockWorkerProc) execOK(t *testing.T, cmdLine string) {
	t.Helper()
	w.send(t, cmdLine)
	got := w.readLineWithTimeout(t, 5*time.Second, cmdLine)
	require.Equal(t, "OK", got, "cmd=%q stderr=%s", cmdLine, w.stderr.String())
}

func (w *lockWorkerProc) waitBlockedInFcntl(t *testing.T) {
	t.Helper()
	syscallPath := fmt.Sprintf("/proc/%d/task/%d/syscall", w.pid, w.tid)
	wantPrefix := strconv.Itoa(unix.SYS_FCNTL) + " "
	require.Eventually(t, func() bool {
		raw, err := os.ReadFile(syscallPath)
		if err != nil {
			return false
		}
		return strings.HasPrefix(strings.TrimSpace(string(raw)), wantPrefix)
	}, 5*time.Second, 5*time.Millisecond, "worker pid=%d tid=%d did not enter fcntl syscall", w.pid, w.tid)
	time.Sleep(50 * time.Millisecond)
}

func TestVFS_KernelNativePosixAndFlockLocks(t *testing.T) {
	client := newTestStorageClient(t)
	disk := createTestDisk(t, client)
	mountpoint, stop := mountWithCleanup(t, getStorageEndpoint(), disk.GetDiskId())
	defer stop()

	t.Run("BSDFlockDupReleaseAndDirectoryFlock", func(t *testing.T) {
		flockFile := filepath.Join(mountpoint, "native-flock.bin")
		require.NoError(t, os.WriteFile(flockFile, []byte("flock"), 0o644))
		f1, err := os.OpenFile(flockFile, os.O_RDWR, 0)
		require.NoError(t, err)
		f2, err := os.OpenFile(flockFile, os.O_RDWR, 0)
		require.NoError(t, err)
		defer func() { _ = f2.Close() }()

		require.NoError(t, unix.Flock(int(f1.Fd()), unix.LOCK_EX|unix.LOCK_NB))
		dupFD, err := unix.Dup(int(f1.Fd()))
		require.NoError(t, err)
		require.ErrorIs(t, unix.Flock(int(f2.Fd()), unix.LOCK_EX|unix.LOCK_NB), syscall.EAGAIN)
		require.NoError(t, unix.Close(dupFD))
		require.ErrorIs(t, unix.Flock(int(f2.Fd()), unix.LOCK_EX|unix.LOCK_NB), syscall.EAGAIN)
		require.NoError(t, f1.Close())
		require.NoError(t, unix.Flock(int(f2.Fd()), unix.LOCK_EX|unix.LOCK_NB))
		require.NoError(t, unix.Flock(int(f2.Fd()), unix.LOCK_UN))

		flockDir := filepath.Join(mountpoint, "flock-dir")
		require.NoError(t, os.Mkdir(flockDir, 0o755))
		d1, err := unix.Open(flockDir, unix.O_RDONLY|unix.O_DIRECTORY, 0)
		require.NoError(t, err)
		d2, err := unix.Open(flockDir, unix.O_RDONLY|unix.O_DIRECTORY, 0)
		require.NoError(t, err)
		defer func() { _ = unix.Close(d2) }()

		require.NoError(t, unix.Flock(d1, unix.LOCK_EX|unix.LOCK_NB), "directory flock LOCK_EX must succeed natively in kernel VFS")
		require.ErrorIs(t, unix.Flock(d2, unix.LOCK_EX|unix.LOCK_NB), syscall.EAGAIN)
		require.NoError(t, unix.Close(d1))
		require.NoError(t, unix.Flock(d2, unix.LOCK_EX|unix.LOCK_NB))
	})

	t.Run("PosixByteRangeSplitMergeAndFGetLkAcrossProcesses", func(t *testing.T) {
		rangeFile := filepath.Join(mountpoint, "posix-range.bin")
		require.NoError(t, os.WriteFile(rangeFile, bytes.Repeat([]byte("R"), 512), 0o644))

		wA := startLockWorker(t)
		wB := startLockWorker(t)
		wA.execOK(t, "open f "+rangeFile)
		wB.execOK(t, "open f "+rangeFile)

		wA.execOK(t, "setlk f W 0 300")
		wA.execOK(t, "setlk f U 100 100")
		wA.execOK(t, "setlk f R 50 30")
		wA.execOK(t, "setlk f W 80 40")

		wB.send(t, "getlk f W 120 80")
		holeReply := wB.readLineWithTimeout(t, 5*time.Second, "getlk unlocked hole")
		require.Equal(t, fmt.Sprintf("LOCK %d 0 120 80", unix.F_UNLCK), holeReply)

		wB.send(t, "getlk f R 50 30")
		sharedOK := wB.readLineWithTimeout(t, 5*time.Second, "getlk compatible read")
		require.Equal(t, fmt.Sprintf("LOCK %d 0 50 30", unix.F_UNLCK), sharedOK)

		wB.send(t, "getlk f W 50 30")
		rdConflict := wB.readLineWithTimeout(t, 5*time.Second, "getlk read conflict")
		require.Equal(t, fmt.Sprintf("LOCK %d %d 50 30", unix.F_RDLCK, wA.pid), rdConflict)

		wB.send(t, "getlk f W 80 40")
		mergedConflict := wB.readLineWithTimeout(t, 5*time.Second, "getlk merged write conflict")
		require.Equal(t, fmt.Sprintf("LOCK %d %d 80 40", unix.F_WRLCK, wA.pid), mergedConflict)

		wB.send(t, "getlk f W 200 100")
		tailConflict := wB.readLineWithTimeout(t, 5*time.Second, "getlk tail write conflict")
		require.Equal(t, fmt.Sprintf("LOCK %d %d 200 100", unix.F_WRLCK, wA.pid), tailConflict)
	})

	t.Run("CrossInodePosixABBADeadlockReturnsEDEADLK", func(t *testing.T) {
		fileA := filepath.Join(mountpoint, "deadlock-a.bin")
		fileB := filepath.Join(mountpoint, "deadlock-b.bin")
		require.NoError(t, os.WriteFile(fileA, []byte("aaaaaaaaaa"), 0o644))
		require.NoError(t, os.WriteFile(fileB, []byte("bbbbbbbbbb"), 0o644))

		wA := startLockWorker(t)
		wB := startLockWorker(t)
		wA.execOK(t, "open a "+fileA)
		wA.execOK(t, "open b "+fileB)
		wB.execOK(t, "open a "+fileA)
		wB.execOK(t, "open b "+fileB)

		wA.execOK(t, "setlk a W 0 10")
		wB.execOK(t, "setlk b W 0 10")

		wA.send(t, "setlkw b W 0 10")
		require.Equal(t, "WAITING", wA.readLineWithTimeout(t, 5*time.Second, "worker A waiting on fileB"))
		wA.waitBlockedInFcntl(t)

		wB.send(t, "setlkw a W 0 10")
		require.Equal(t, "WAITING", wB.readLineWithTimeout(t, 5*time.Second, "worker B entering fileA"))
		deadlockReply := wB.readLineWithTimeout(t, 3*time.Second, "worker B cross-inode AB-BA EDEADLK")
		require.Equal(t, fmt.Sprintf("ERR %d", int(syscall.EDEADLK)), deadlockReply, "cross-inode AB-BA deadlock must return EDEADLK")

		wB.execOK(t, "setlk b U 0 10")
		wAReply := wA.readLineWithTimeout(t, 5*time.Second, "worker A wakes after B unlocks fileB")
		require.Equal(t, "OK", wAReply)
	})

	t.Run("SoleHolderPosixReadToWriteUpgradeWithWaiterSucceedsImmediately", func(t *testing.T) {
		upgradeFile := filepath.Join(mountpoint, "upgrade.bin")
		require.NoError(t, os.WriteFile(upgradeFile, []byte("uuuuuuuuuu"), 0o644))

		wA := startLockWorker(t)
		wB := startLockWorker(t)
		wA.execOK(t, "open f "+upgradeFile)
		wB.execOK(t, "open f "+upgradeFile)

		wA.execOK(t, "setlk f R 0 10")

		wB.send(t, "setlkw f W 0 10")
		require.Equal(t, "WAITING", wB.readLineWithTimeout(t, 5*time.Second, "worker B waiting for write lock"))
		wB.waitBlockedInFcntl(t)

		wA.send(t, "setlkw f W 0 10")
		require.Equal(t, "WAITING", wA.readLineWithTimeout(t, 5*time.Second, "worker A upgrading read->write"))
		upgradeReply := wA.readLineWithTimeout(t, 3*time.Second, "worker A sole-holder upgrade")
		require.Equal(t, "OK", upgradeReply, "sole holder F_RDLCK->F_WRLCK upgrade must succeed immediately without EAGAIN/EDEADLK")

		wA.execOK(t, "setlk f U 0 10")
		wBReply := wB.readLineWithTimeout(t, 5*time.Second, "worker B acquires write lock after A unlocks")
		require.Equal(t, "OK", wBReply)
	})

	t.Run("LinuxOFDLockAcrossTwoOpenFileDescriptionsInSameProcess", func(t *testing.T) {
		ofdFile := filepath.Join(mountpoint, "ofd.bin")
		require.NoError(t, os.WriteFile(ofdFile, []byte("oooooooooo"), 0o644))

		fd1, err := unix.Open(ofdFile, unix.O_RDWR, 0)
		require.NoError(t, err)
		fd2, err := unix.Open(ofdFile, unix.O_RDWR, 0)
		require.NoError(t, err)
		defer func() { _ = unix.Close(fd2) }()

		lkSet1 := unix.Flock_t{Type: unix.F_WRLCK, Whence: io.SeekStart, Start: 0, Len: 10, Pid: 0}
		require.NoError(t, unix.FcntlFlock(uintptr(fd1), unix.F_OFD_SETLK, &lkSet1))

		lkSet2 := unix.Flock_t{Type: unix.F_WRLCK, Whence: io.SeekStart, Start: 0, Len: 10, Pid: 0}
		require.ErrorIs(t, unix.FcntlFlock(uintptr(fd2), unix.F_OFD_SETLK, &lkSet2), syscall.EAGAIN)

		lkGet := unix.Flock_t{Type: unix.F_WRLCK, Whence: io.SeekStart, Start: 0, Len: 10, Pid: 0}
		require.NoError(t, unix.FcntlFlock(uintptr(fd2), unix.F_OFD_GETLK, &lkGet))
		require.Equal(t, int16(unix.F_WRLCK), lkGet.Type)
		require.Equal(t, int32(-1), lkGet.Pid)

		dupFD, err := unix.Dup(fd1)
		require.NoError(t, err)
		require.NoError(t, unix.Close(dupFD))
		require.ErrorIs(t, unix.FcntlFlock(uintptr(fd2), unix.F_OFD_SETLK, &lkSet2), syscall.EAGAIN)

		require.NoError(t, unix.Close(fd1))
		require.NoError(t, unix.FcntlFlock(uintptr(fd2), unix.F_OFD_SETLK, &lkSet2))
	})
}

func TestVFS_RemovedOpenDir_RmdirAndOverwriteRenameSemantics(t *testing.T) {
	client := newTestStorageClient(t)
	endpoint := getStorageEndpoint()
	disk := createTestDisk(t, client)
	mountpoint, stop := mountWithCleanup(t, endpoint, disk.GetDiskId())
	defer stop()

	verifyRemovedOpenDir := func(t *testing.T, label string, dirFile *os.File) {
		t.Helper()
		closed := false
		defer func() {
			if !closed {
				_ = dirFile.Close()
			}
		}()

		var st unix.Stat_t
		require.NoError(t, unix.Fstat(int(dirFile.Fd()), &st), "%s fstat", label)
		require.Zero(t, st.Nlink, "%s removed open dir nlink must be 0", label)
		require.NoError(t, dirFile.Sync(), "%s fsync on removed open dir", label)

		entries, err := dirFile.Readdir(-1)
		require.Empty(t, entries, "%s Readdir on removed open dir must be empty", label)
		if err != nil {
			require.ErrorIs(t, err, syscall.ENOENT, "%s Readdir on removed open dir may only return EOF or VFS IS_DEADDIR ENOENT", label)
		}

		tmpFD, tmpErr := unix.Openat(int(dirFile.Fd()), ".", unix.O_TMPFILE|unix.O_RDWR, 0o600)
		if tmpFD >= 0 {
			_ = unix.Close(tmpFD)
		}
		require.ErrorIs(t, tmpErr, syscall.ENOENT, "%s O_TMPFILE on removed open dir must return ENOENT", label)
		closed = true
		require.NoError(t, dirFile.Close(), "%s close removed open dir", label)
	}

	t.Run("RmdirOnOpenEmptyDir", func(t *testing.T) {
		dirPath := filepath.Join(mountpoint, "open-rmdir-dir")
		require.NoError(t, os.Mkdir(dirPath, 0o755))
		dirFile, err := os.Open(dirPath)
		require.NoError(t, err)
		require.NoError(t, os.Remove(dirPath))
		verifyRemovedOpenDir(t, "rmdir", dirFile)
	})

	t.Run("OverwriteRenameOnOpenEmptyDir", func(t *testing.T) {
		srcPath := filepath.Join(mountpoint, "open-rename-src")
		dstPath := filepath.Join(mountpoint, "open-rename-dst")
		require.NoError(t, os.Mkdir(srcPath, 0o755))
		require.NoError(t, os.WriteFile(filepath.Join(srcPath, "marker.txt"), []byte("new-dir"), 0o644))
		require.NoError(t, os.Mkdir(dstPath, 0o755))
		dstFile, err := os.Open(dstPath)
		require.NoError(t, err)
		require.NoError(t, unix.Rename(srcPath, dstPath))
		verifyRemovedOpenDir(t, "overwrite-rename", dstFile)

		gotMarker, err := os.ReadFile(filepath.Join(dstPath, "marker.txt"))
		require.NoError(t, err)
		require.Equal(t, []byte("new-dir"), gotMarker)
	})
}
