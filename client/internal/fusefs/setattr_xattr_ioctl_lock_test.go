package fusefs_test

import (
	"context"
	"encoding/binary"
	"runtime"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/stretchr/testify/require"

	"github.com/starsrc/bowl/client/internal/fusefs"
)

const (
	xattrCreateFlag  uint32 = 0x1
	xattrReplaceFlag uint32 = 0x2
	fsxattrStructLen        = 28
	lockWaitDeadline        = 3 * time.Second
)

func waitUntilLockConflictsAtLeast(t *testing.T, fs *fusefs.RawFS, target uint64) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), lockWaitDeadline)
	defer cancel()
	for fs.Snapshot().Locks.Conflicts < target {
		if ctx.Err() != nil {
			t.Fatalf("等待锁冲突登记超时: want>=%d got=%d", target, fs.Snapshot().Locks.Conflicts)
		}
		runtime.Gosched()
	}
}

func waitUntilStatusChan(t *testing.T, ch <-chan fuse.Status, label string) fuse.Status {
	t.Helper()
	timer := time.NewTimer(lockWaitDeadline)
	defer timer.Stop()
	select {
	case st := <-ch:
		return st
	case <-timer.C:
		t.Fatalf("%s 等待阻塞操作返回超时", label)
		return fuse.EIO
	}
}

func mustSetLock(t *testing.T, fs *fusefs.RawFS, in *fuse.LkIn) {
	t.Helper()
	requireStatus(t, "SetLk", fuse.OK, fs.SetLk(nil, in))
}

func TestSetAttr_字段修改时间戳优先序句柄校验与写回排空(t *testing.T) {
	t.Run("精确更新Mode与Uid与Gid与Size以及ATIME_NOW优先覆盖", func(t *testing.T) {

		srv := newFakeStorage(t, withTTL(2.0, 2.0))
		fs := newTestRawFS(t, srv, withDirectIO(false))
		created := mustCreateFile(t, fs, fuse.FUSE_ROOT_ID, "attr.txt", syscall.O_RDWR, 0o644)
		mustWriteAt(t, fs, created.NodeId, created.Fh, 0, []byte("0123456789"))

		var noopOut fuse.AttrOut
		noopSt := fs.SetAttr(nil, &fuse.SetAttrIn{
			SetAttrInCommon: fuse.SetAttrInCommon{InHeader: fuse.InHeader{NodeId: created.NodeId}},
		}, &noopOut)
		setCallsAfterNoop := srv.setAttrCalls.Load()

		beforeNow := time.Now().Unix() - 5
		var updatedOut fuse.AttrOut
		setSt := fs.SetAttr(nil, &fuse.SetAttrIn{
			SetAttrInCommon: fuse.SetAttrInCommon{
				InHeader: fuse.InHeader{NodeId: created.NodeId},
				Valid: fuse.FATTR_MODE | fuse.FATTR_UID | fuse.FATTR_GID | fuse.FATTR_SIZE |
					fuse.FATTR_ATIME | fuse.FATTR_ATIME_NOW | fuse.FATTR_MTIME | fuse.FATTR_MTIME_NOW | fuse.FATTR_CTIME,
				Mode:  0o750,
				Owner: fuse.Owner{Uid: 1001, Gid: 2002},
				Size:  4,
				Atime: 100,
				Mtime: 200,
			},
		}, &updatedOut)

		requireStatus(t, "无位 SetAttr 委派 GetAttr", fuse.OK, noopSt)
		require.Zero(t, setCallsAfterNoop, "Valid=0 时不得发起 SetAttr RPC")
		requireStatus(t, "全字段 SetAttr", fuse.OK, setSt)
		require.Equal(t, uint32(syscall.S_IFREG|0o750), updatedOut.Mode)
		require.Equal(t, uint32(1001), updatedOut.Uid)
		require.Equal(t, uint32(2002), updatedOut.Gid)
		require.Equal(t, uint64(4), updatedOut.Size)
		require.GreaterOrEqual(t, int64(updatedOut.Atime), beforeNow, "FATTR_ATIME_NOW 必须优先于显式 FATTR_ATIME")
		require.GreaterOrEqual(t, int64(updatedOut.Mtime), beforeNow, "FATTR_MTIME_NOW 必须优先于显式 FATTR_MTIME")

		stored, _ := srv.fileBytes(created.NodeId)
		requireBytesMatch(t, "SetAttr 截断后存储内容", []byte("0123"), stored)
		fs.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: created.NodeId}, Fh: created.Fh})
	})

	t.Run("只读句柄FATTR_FH截断返回EBADF且只读挂载返回EROFS", func(t *testing.T) {

		srv := newFakeStorage(t, withTTL(1.0, 1.0))
		ino := srv.seedFile(1, "ro-fh.txt", 0o644, []byte("abcdef"))
		fs := newTestRawFS(t, srv)
		roFS := newTestRawFS(t, srv, withMode(fusefs.MountModeRO))
		rdHandle := mustOpenFile(t, fs, ino, syscall.O_RDONLY)

		var out fuse.AttrOut
		ebadfSt := fs.SetAttr(nil, &fuse.SetAttrIn{
			SetAttrInCommon: fuse.SetAttrInCommon{
				InHeader: fuse.InHeader{NodeId: ino},
				Valid:    fuse.FATTR_FH | fuse.FATTR_SIZE,
				Fh:       rdHandle.Fh,
				Size:     2,
			},
		}, &out)
		erofsSt := roFS.SetAttr(nil, &fuse.SetAttrIn{
			SetAttrInCommon: fuse.SetAttrInCommon{
				InHeader: fuse.InHeader{NodeId: ino},
				Valid:    fuse.FATTR_MODE,
				Mode:     0o600,
			},
		}, &out)

		requireStatus(t, "只读句柄 FATTR_FH+FATTR_SIZE", fuse.EBADF, ebadfSt)
		requireStatus(t, "只读挂载 SetAttr", fuse.Status(syscall.EROFS), erofsSt)
		fs.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: ino}, Fh: rdHandle.Fh})
	})
}

func TestFallocate_空间预分配保持尺寸打洞与错误拦截(t *testing.T) {

	srv := newFakeStorage(t, withTTL(1.0, 1.0))
	fs := newTestRawFS(t, srv)
	roFS := newTestRawFS(t, srv, withMode(fusefs.MountModeRO))
	rw := mustCreateFile(t, fs, fuse.FUSE_ROOT_ID, "alloc.bin", syscall.O_RDWR, 0o644)
	mustWriteAt(t, fs, rw.NodeId, rw.Fh, 0, []byte("HELLO-WORLD"))
	rd := mustOpenFile(t, fs, rw.NodeId, syscall.O_RDONLY)

	requireStatus(t, "Fallocate mode=0", fuse.OK, fs.Fallocate(nil, &fuse.FallocateIn{
		InHeader: fuse.InHeader{NodeId: rw.NodeId}, Fh: rw.Fh, Offset: 11, Length: 5, Mode: 0,
	}))
	require.Equal(t, uint64(16), mustGetAttr(t, fs, rw.NodeId).Size)

	requireStatus(t, "Fallocate KEEP_SIZE", fuse.OK, fs.Fallocate(nil, &fuse.FallocateIn{
		InHeader: fuse.InHeader{NodeId: rw.NodeId}, Fh: rw.Fh, Offset: 16, Length: 64, Mode: fallocFlKeepSize,
	}))
	require.Equal(t, uint64(16), mustGetAttr(t, fs, rw.NodeId).Size)

	requireStatus(t, "Fallocate PUNCH_HOLE", fuse.OK, fs.Fallocate(nil, &fuse.FallocateIn{
		InHeader: fuse.InHeader{NodeId: rw.NodeId}, Fh: rw.Fh, Offset: 6, Length: 5, Mode: fallocFlPunchHole | fallocFlKeepSize,
	}))
	requireBytesMatch(t, "打洞清零区间", []byte("HELLO-\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00"), mustReadAt(t, fs, rw.NodeId, rd.Fh, 0, 16))

	requireStatus(t, "非法 Fallocate 标志", fuse.Status(syscall.EOPNOTSUPP), fs.Fallocate(nil, &fuse.FallocateIn{
		InHeader: fuse.InHeader{NodeId: rw.NodeId}, Fh: rw.Fh, Offset: 0, Length: 4, Mode: 0x8,
	}))
	requireStatus(t, "只读句柄 Fallocate", fuse.EBADF, fs.Fallocate(nil, &fuse.FallocateIn{
		InHeader: fuse.InHeader{NodeId: rw.NodeId}, Fh: rd.Fh, Offset: 0, Length: 4, Mode: 0,
	}))
	requireStatus(t, "只读挂载 Fallocate", fuse.Status(syscall.EROFS), roFS.Fallocate(nil, &fuse.FallocateIn{
		InHeader: fuse.InHeader{NodeId: rw.NodeId}, Fh: rw.Fh, Offset: 0, Length: 4, Mode: 0,
	}))

	fs.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: rw.NodeId}, Fh: rw.Fh})
	fs.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: rw.NodeId}, Fh: rd.Fh})
}

func TestXAttr_创建替换长度探测列表序与Capability负缓存短路及失效(t *testing.T) {
	t.Run("Set与Get与List与Remove覆盖CREATE_REPLACE_ERANGE契约", func(t *testing.T) {

		srv := newFakeStorage(t, withTTL(1.0, 1.0))
		ino := srv.seedFile(1, "xa.txt", 0o644, nil)
		fs := newTestRawFS(t, srv)

		requireStatus(t, "不存在时 REPLACE", fuse.Status(syscall.ENODATA), fs.SetXAttr(nil, &fuse.SetXAttrIn{
			InHeader: fuse.InHeader{NodeId: ino}, Flags: xattrReplaceFlag,
		}, "user.b", []byte("v1")))
		requireStatus(t, "CREATE 新建 user.b", fuse.OK, fs.SetXAttr(nil, &fuse.SetXAttrIn{
			InHeader: fuse.InHeader{NodeId: ino}, Flags: xattrCreateFlag,
		}, "user.b", []byte("val-b")))
		requireStatus(t, "已存在时 CREATE", fuse.Status(syscall.EEXIST), fs.SetXAttr(nil, &fuse.SetXAttrIn{
			InHeader: fuse.InHeader{NodeId: ino}, Flags: xattrCreateFlag,
		}, "user.b", []byte("dup")))
		requireStatus(t, "新建 user.a", fuse.OK, fs.SetXAttr(nil, &fuse.SetXAttrIn{
			InHeader: fuse.InHeader{NodeId: ino},
		}, "user.a", []byte("12")))

		probeSz, probeSt := fs.GetXAttr(nil, &fuse.InHeader{NodeId: ino}, "user.b", nil)
		requireStatus(t, "GetXAttr 探测长度", fuse.OK, probeSt)
		require.Equal(t, uint32(5), probeSz)
		_, rangeSt := fs.GetXAttr(nil, &fuse.InHeader{NodeId: ino}, "user.b", make([]byte, 3))
		requireStatus(t, "GetXAttr 过小缓冲", fuse.Status(syscall.ERANGE), rangeSt)
		valBuf := make([]byte, 16)
		gotSz, gotSt := fs.GetXAttr(nil, &fuse.InHeader{NodeId: ino}, "user.b", valBuf)
		requireStatus(t, "GetXAttr 正常读取", fuse.OK, gotSt)
		require.Equal(t, "val-b", string(valBuf[:gotSz]))

		listSz, listProbeSt := fs.ListXAttr(nil, &fuse.InHeader{NodeId: ino}, nil)
		requireStatus(t, "ListXAttr 长度探测", fuse.OK, listProbeSt)
		listBuf := make([]byte, listSz)
		gotListSz, listSt := fs.ListXAttr(nil, &fuse.InHeader{NodeId: ino}, listBuf)
		requireStatus(t, "ListXAttr 读取", fuse.OK, listSt)
		require.Equal(t, "user.a\x00user.b\x00", string(listBuf[:gotListSz]))
		require.Equal(t, []string{"user.a", "user.b", ""}, strings.Split(string(listBuf[:gotListSz]), "\x00"))

		requireStatus(t, "RemoveXAttr", fuse.OK, fs.RemoveXAttr(nil, &fuse.InHeader{NodeId: ino}, "user.a"))
		_, afterRmSt := fs.GetXAttr(nil, &fuse.InHeader{NodeId: ino}, "user.a", make([]byte, 8))
		requireStatus(t, "删除后 GetXAttr", fuse.Status(syscall.ENODATA), afterRmSt)
	})

	t.Run("security.capability负缓存短路并在SetXAttr与Write与SetAttr与Fallocate间切换状态", func(t *testing.T) {

		srv := newFakeStorage(t, withTTL(1.0, 1.0))
		fs := newTestRawFS(t, srv, withDirectIO(false))
		fresh := mustCreateFile(t, fs, fuse.FUSE_ROOT_ID, "fresh-cap.bin", syscall.O_RDWR, 0o644)
		_, freshSt := fs.GetXAttr(nil, &fuse.InHeader{NodeId: fresh.NodeId}, "security.capability", make([]byte, 32))
		requireStatus(t, "新建文件 capability 预置负缓存", fuse.Status(syscall.ENODATA), freshSt)
		require.Zero(t, srv.getXattrCalls.Load(), "Create 预置已知缺失后紧邻查询不得穿透存储")
		fs.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: fresh.NodeId}, Fh: fresh.Fh})

		seededIno := srv.seedFile(1, "cap.bin", 0o644, nil)
		f := mustOpenFile(t, fs, seededIno, syscall.O_RDWR)

		_, st1 := fs.GetXAttr(nil, &fuse.InHeader{NodeId: seededIno}, "security.capability", make([]byte, 32))
		_, st2 := fs.GetXAttr(nil, &fuse.InHeader{NodeId: seededIno}, "security.capability", make([]byte, 32))
		_, st3 := fs.GetXAttr(nil, &fuse.InHeader{NodeId: seededIno}, "security.capability", make([]byte, 32))
		requireStatus(t, "首次 capability 查询", fuse.Status(syscall.ENODATA), st1)
		requireStatus(t, "第二次 capability 负缓存命中", fuse.Status(syscall.ENODATA), st2)
		requireStatus(t, "第三次 capability 负缓存命中", fuse.Status(syscall.ENODATA), st3)
		require.Equal(t, int64(1), srv.getXattrCalls.Load(), "重复查询缺失 capability 只能产生 1 次 GetXattr RPC")

		requireStatus(t, "SetXAttr security.capability", fuse.OK, fs.SetXAttr(nil, &fuse.SetXAttrIn{
			InHeader: fuse.InHeader{NodeId: seededIno},
		}, "security.capability", []byte("capv2")))
		capBuf := make([]byte, 16)
		sz, gotCapSt := fs.GetXAttr(nil, &fuse.InHeader{NodeId: seededIno}, "security.capability", capBuf)
		requireStatus(t, "SetXAttr 后读取 capability", fuse.OK, gotCapSt)
		require.Equal(t, "capv2", string(capBuf[:sz]))
		require.Equal(t, int64(2), srv.getXattrCalls.Load())

		mustWriteAt(t, fs, seededIno, f.Fh, 0, []byte("x"))
		_, afterWriteSt := fs.GetXAttr(nil, &fuse.InHeader{NodeId: seededIno}, "security.capability", make([]byte, 32))
		requireStatus(t, "Write 剥离后 capability 应直接返回 ENODATA", fuse.Status(syscall.ENODATA), afterWriteSt)
		require.Equal(t, int64(2), srv.getXattrCalls.Load(), "Write 标回已知缺失后不得发起 GetXattr RPC")

		requireStatus(t, "重设 capability", fuse.OK, fs.SetXAttr(nil, &fuse.SetXAttrIn{
			InHeader: fuse.InHeader{NodeId: seededIno},
		}, "security.capability", []byte("capv3")))
		_, _ = fs.GetXAttr(nil, &fuse.InHeader{NodeId: seededIno}, "security.capability", capBuf)
		require.Equal(t, int64(3), srv.getXattrCalls.Load())
		requireStatus(t, "SetAttr", fuse.OK, fs.SetAttr(nil, &fuse.SetAttrIn{
			SetAttrInCommon: fuse.SetAttrInCommon{
				InHeader: fuse.InHeader{NodeId: seededIno},
				Valid:    fuse.FATTR_MODE,
				Mode:     0o755,
			},
		}, &fuse.AttrOut{}))
		_, afterSetAttrSt := fs.GetXAttr(nil, &fuse.InHeader{NodeId: seededIno}, "security.capability", make([]byte, 32))
		requireStatus(t, "SetAttr 剥离后 capability 应直接返回 ENODATA", fuse.Status(syscall.ENODATA), afterSetAttrSt)
		require.Equal(t, int64(3), srv.getXattrCalls.Load())

		requireStatus(t, "再设 capability", fuse.OK, fs.SetXAttr(nil, &fuse.SetXAttrIn{
			InHeader: fuse.InHeader{NodeId: seededIno},
		}, "security.capability", []byte("capv4")))
		_, _ = fs.GetXAttr(nil, &fuse.InHeader{NodeId: seededIno}, "security.capability", capBuf)
		require.Equal(t, int64(4), srv.getXattrCalls.Load())
		requireStatus(t, "Fallocate", fuse.OK, fs.Fallocate(nil, &fuse.FallocateIn{
			InHeader: fuse.InHeader{NodeId: seededIno}, Fh: f.Fh, Offset: 0, Length: 4, Mode: 0,
		}))
		_, afterFallocSt := fs.GetXAttr(nil, &fuse.InHeader{NodeId: seededIno}, "security.capability", make([]byte, 32))
		requireStatus(t, "Fallocate 剥离后 capability 应直接返回 ENODATA", fuse.Status(syscall.ENODATA), afterFallocSt)
		require.Equal(t, int64(4), srv.getXattrCalls.Load())

		fs.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: seededIno}, Fh: f.Fh})
	})
}

func TestXAttr_ReadOnlyMountBypassesCapabilityCache(t *testing.T) {
	t.Run("GivenReadOnlyMount_WhenCapabilityAddedAfterENODATA_ThenGetXAttrQueriesServerAndObservesValue", func(t *testing.T) {

		srv := newFakeStorage(t, withTTL(2.0, 2.0))
		ino := srv.seedFile(1, "bin.elf", 0o755, []byte("elf"))
		rwFS := newTestRawFS(t, srv, withMode(fusefs.MountModeStaged))
		roFS := newTestRawFS(t, srv, withMode(fusefs.MountModeRO))

		buf := make([]byte, 32)
		_, st1 := roFS.GetXAttr(nil, &fuse.InHeader{NodeId: ino}, "security.capability", buf)
		requireStatus(t, "RO initial GetXAttr capability", fuse.Status(syscall.ENODATA), st1)
		require.Equal(t, int64(1), srv.getXattrCalls.Load())

		wantCap := []byte("cap-net-bind")
		requireStatus(t, "RW SetXAttr capability", fuse.OK, rwFS.SetXAttr(nil, &fuse.SetXAttrIn{
			InHeader: fuse.InHeader{NodeId: ino},
		}, "security.capability", wantCap))
		sz2, st2 := roFS.GetXAttr(nil, &fuse.InHeader{NodeId: ino}, "security.capability", buf)

		requireStatus(t, "RO GetXAttr after RW SetXAttr", fuse.OK, st2)
		require.Equal(t, wantCap, buf[:sz2])
		require.Equal(t, int64(2), srv.getXattrCalls.Load())

		requireStatus(t, "RW RemoveXAttr capability", fuse.OK, rwFS.RemoveXAttr(nil, &fuse.InHeader{NodeId: ino}, "security.capability"))
		_, st3 := roFS.GetXAttr(nil, &fuse.InHeader{NodeId: ino}, "security.capability", buf)
		requireStatus(t, "RO GetXAttr after RW RemoveXAttr", fuse.Status(syscall.ENODATA), st3)
		require.Equal(t, int64(3), srv.getXattrCalls.Load())

		capStats := roFS.Snapshot().CapabilityCache
		require.Zero(t, capStats.Entries, "RO mount must never insert entries into capabilityCache")
		require.Zero(t, capStats.Hits, "RO mount must never hit capabilityCache")
		require.Zero(t, capStats.Misses, "RO mount must never probe capabilityCache")
		require.Zero(t, capStats.Grants, "RO mount must never grant capabilityCache state")
	})

	t.Run("GivenWritableStagedAndSyncMounts_WhenCapabilityAbsent_ThenNegativeCacheStillShortCircuits", func(t *testing.T) {
		for _, mode := range []string{fusefs.MountModeStaged, fusefs.MountModeSync} {
			t.Run(mode, func(t *testing.T) {
				srv := newFakeStorage(t, withTTL(2.0, 2.0))
				ino := srv.seedFile(1, "plain.bin", 0o644, nil)
				fs := newTestRawFS(t, srv, withMode(mode))

				_, st1 := fs.GetXAttr(nil, &fuse.InHeader{NodeId: ino}, "security.capability", make([]byte, 32))
				_, st2 := fs.GetXAttr(nil, &fuse.InHeader{NodeId: ino}, "security.capability", make([]byte, 32))

				requireStatus(t, "first GetXAttr", fuse.Status(syscall.ENODATA), st1)
				requireStatus(t, "second cached GetXAttr", fuse.Status(syscall.ENODATA), st2)
				require.Equal(t, int64(1), srv.getXattrCalls.Load(), "writable mount must keep capability negative short-circuit")
				require.Equal(t, uint64(1), fs.Snapshot().CapabilityCache.Hits)
			})
		}
	})
}

func TestIoctl_标准与32位与FSXATTR标志读写校验及ENOTTY快速短路(t *testing.T) {

	srv := newFakeStorage(t, withTTL(2.0, 2.0))
	fs := newTestRawFS(t, srv, withDirectIO(false))
	roFS := newTestRawFS(t, srv, withMode(fusefs.MountModeRO))
	f := mustCreateFile(t, fs, fuse.FUSE_ROOT_ID, "flags.bin", syscall.O_RDWR, 0o644)

	mustWriteAt(t, fs, f.NodeId, f.Fh, 0, []byte("buffered"))
	var ioOut fuse.IoctlOut
	enottySt := fs.Ioctl(nil, &fuse.IoctlIn{InHeader: fuse.InHeader{NodeId: f.NodeId}, Cmd: 0x12345678}, nil, &ioOut, nil)
	requireStatus(t, "不支持的 ioctl 命令", fuse.Status(syscall.ENOTTY), enottySt)
	require.Zero(t, srv.writeCalls.Load(), "未知 ioctl 命令返回 ENOTTY 前不得无谓刷空 Stage")

	in64 := make([]byte, 8)
	binary.LittleEndian.PutUint64(in64, uint64(fusefs.FS_APPEND_FL))
	requireStatus(t, "SETFLAGS 64位", fuse.OK, fs.Ioctl(nil, &fuse.IoctlIn{InHeader: fuse.InHeader{NodeId: f.NodeId}, Cmd: fusefs.FS_IOC_SETFLAGS}, in64, &ioOut, nil))
	out64 := make([]byte, 8)
	requireStatus(t, "GETFLAGS 64位", fuse.OK, fs.Ioctl(nil, &fuse.IoctlIn{InHeader: fuse.InHeader{NodeId: f.NodeId}, Cmd: fusefs.FS_IOC_GETFLAGS}, nil, &ioOut, out64))
	require.Equal(t, uint64(fusefs.FS_APPEND_FL), binary.LittleEndian.Uint64(out64))

	in32 := make([]byte, 4)
	binary.LittleEndian.PutUint32(in32, fusefs.FS_IMMUTABLE_FL)
	requireStatus(t, "SETFLAGS 32位", fuse.OK, fs.Ioctl(nil, &fuse.IoctlIn{InHeader: fuse.InHeader{NodeId: f.NodeId}, Cmd: fusefs.FS_IOC32_SETFLAGS}, in32, &ioOut, nil))
	out32 := make([]byte, 4)
	requireStatus(t, "GETFLAGS 32位", fuse.OK, fs.Ioctl(nil, &fuse.IoctlIn{InHeader: fuse.InHeader{NodeId: f.NodeId}, Cmd: fusefs.FS_IOC32_GETFLAGS}, nil, &ioOut, out32))
	require.Equal(t, fusefs.FS_IMMUTABLE_FL, binary.LittleEndian.Uint32(out32))

	fsxIn := make([]byte, fsxattrStructLen)
	binary.LittleEndian.PutUint32(fsxIn[:4], fusefs.FS_XFLAG_APPEND)
	requireStatus(t, "FS_IOC_FSSETXATTR", fuse.OK, fs.Ioctl(nil, &fuse.IoctlIn{InHeader: fuse.InHeader{NodeId: f.NodeId}, Cmd: fusefs.FS_IOC_FSSETXATTR}, fsxIn, &ioOut, nil))
	fsxOut := make([]byte, fsxattrStructLen)
	requireStatus(t, "FS_IOC_FSGETXATTR", fuse.OK, fs.Ioctl(nil, &fuse.IoctlIn{InHeader: fuse.InHeader{NodeId: f.NodeId}, Cmd: fusefs.FS_IOC_FSGETXATTR}, nil, &ioOut, fsxOut))
	require.Equal(t, fusefs.FS_XFLAG_APPEND, binary.LittleEndian.Uint32(fsxOut[:4]))

	badFlag := make([]byte, 8)
	binary.LittleEndian.PutUint64(badFlag, 0x40000000)
	requireStatus(t, "未知标志 SETFLAGS", fuse.Status(syscall.EOPNOTSUPP), fs.Ioctl(nil, &fuse.IoctlIn{InHeader: fuse.InHeader{NodeId: f.NodeId}, Cmd: fusefs.FS_IOC_SETFLAGS}, badFlag, &ioOut, nil))
	requireStatus(t, "短缓冲 GETFLAGS", fuse.EINVAL, fs.Ioctl(nil, &fuse.IoctlIn{InHeader: fuse.InHeader{NodeId: f.NodeId}, Cmd: fusefs.FS_IOC_GETFLAGS}, nil, &ioOut, make([]byte, 2)))
	requireStatus(t, "只读挂载 SETFLAGS", fuse.Status(syscall.EROFS), roFS.Ioctl(nil, &fuse.IoctlIn{InHeader: fuse.InHeader{NodeId: f.NodeId}, Cmd: fusefs.FS_IOC_SETFLAGS}, in64, &ioOut, nil))

	fs.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: f.NodeId}, Fh: f.Fh})
}

func TestLock_POSIX区间锁拆分合并无符号最大值与阻塞唤醒死锁中断及OnUnmount(t *testing.T) {
	t.Run("锁类型与无符号全区间映射以及读写句柄权限和F_UNLCK校验", func(t *testing.T) {

		pRd, stRd := fusefs.ToProtoLockType(syscall.F_RDLCK)
		pWr, stWr := fusefs.ToProtoLockType(syscall.F_WRLCK)
		pUn, stUn := fusefs.ToProtoLockType(syscall.F_UNLCK)
		hRd, stHRd := fusefs.ToHostLockType(fusefs.ProtoLockTypeRead)
		hWr, stHWr := fusefs.ToHostLockType(fusefs.ProtoLockTypeWrite)
		hUn, stHUn := fusefs.ToHostLockType(fusefs.ProtoLockTypeUnlock)
		_, badProto := fusefs.ToProtoLockType(999)
		_, badHost := fusefs.ToHostLockType(999)

		normStart, normEnd, normSt := fusefs.NormalizeLockRange(0, 10, ^uint64(0))
		flockStart, flockEnd, flockSt := fusefs.NormalizeLockRange(fuse.LK_FLOCK, 123, 456)
		_, _, invRangeSt := fusefs.NormalizeLockRange(0, 50, 10)

		requireStatus(t, "ToProto RD", fuse.OK, stRd)
		requireStatus(t, "ToProto WR", fuse.OK, stWr)
		requireStatus(t, "ToProto UN", fuse.OK, stUn)
		require.Equal(t, fusefs.ProtoLockTypeRead, pRd)
		require.Equal(t, fusefs.ProtoLockTypeWrite, pWr)
		require.Equal(t, fusefs.ProtoLockTypeUnlock, pUn)
		requireStatus(t, "ToHost RD", fuse.OK, stHRd)
		requireStatus(t, "ToHost WR", fuse.OK, stHWr)
		requireStatus(t, "ToHost UN", fuse.OK, stHUn)
		require.Equal(t, uint32(syscall.F_RDLCK), hRd)
		require.Equal(t, uint32(syscall.F_WRLCK), hWr)
		require.Equal(t, uint32(syscall.F_UNLCK), hUn)
		requireStatus(t, "未知主机锁类型", fuse.EINVAL, badProto)
		requireStatus(t, "未知协议锁类型", fuse.EINVAL, badHost)
		requireStatus(t, "MaxUint64 归一化", fuse.OK, normSt)
		require.Equal(t, uint64(10), normStart)
		require.Equal(t, ^uint64(0), normEnd)
		requireStatus(t, "Flock 全区间归一化", fuse.OK, flockSt)
		require.Zero(t, flockStart)
		require.Equal(t, ^uint64(0), flockEnd)
		requireStatus(t, "逆序区间", fuse.EINVAL, invRangeSt)

		srv := newFakeStorage(t, withTTL(1.0, 1.0))
		fs := newTestRawFS(t, srv)
		f := mustCreateFile(t, fs, fuse.FUSE_ROOT_ID, "lk-perm.bin", syscall.O_RDWR, 0o644)
		rd := mustOpenFile(t, fs, f.NodeId, syscall.O_RDONLY)
		wr := mustOpenFile(t, fs, f.NodeId, syscall.O_WRONLY)

		requireStatus(t, "只读句柄加写锁", fuse.EBADF, fs.SetLk(nil, &fuse.LkIn{
			InHeader: fuse.InHeader{NodeId: f.NodeId}, Fh: rd.Fh, Owner: 1,
			Lk: fuse.FileLock{Start: 0, End: 10, Typ: syscall.F_WRLCK, Pid: 1},
		}))
		requireStatus(t, "只写句柄加读锁", fuse.EBADF, fs.SetLk(nil, &fuse.LkIn{
			InHeader: fuse.InHeader{NodeId: f.NodeId}, Fh: wr.Fh, Owner: 2,
			Lk: fuse.FileLock{Start: 0, End: 10, Typ: syscall.F_RDLCK, Pid: 2},
		}))
		var lkOut fuse.LkOut
		requireStatus(t, "GetLk 传入 F_UNLCK", fuse.EINVAL, fs.GetLk(nil, &fuse.LkIn{
			InHeader: fuse.InHeader{NodeId: f.NodeId}, Fh: f.Fh, Owner: 3,
			Lk: fuse.FileLock{Start: 0, End: 10, Typ: syscall.F_UNLCK, Pid: 3},
		}, &lkOut))

		fs.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: f.NodeId}, Fh: f.Fh})
		fs.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: f.NodeId}, Fh: rd.Fh})
		fs.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: f.NodeId}, Fh: wr.Fh})
	})

	t.Run("无符号最大值区间拆分合并与GetLk冲突探测及Flush或Release自动释放", func(t *testing.T) {

		srv := newFakeStorage(t, withTTL(1.0, 1.0))
		fs := newTestRawFS(t, srv)
		f := mustCreateFile(t, fs, fuse.FUSE_ROOT_ID, "split.bin", syscall.O_RDWR, 0o644)
		h2 := mustOpenFile(t, fs, f.NodeId, syscall.O_RDWR)

		mustSetLock(t, fs, &fuse.LkIn{
			InHeader: fuse.InHeader{NodeId: f.NodeId}, Fh: f.Fh, Owner: 1,
			Lk: fuse.FileLock{Start: 0, End: ^uint64(0), Typ: syscall.F_WRLCK, Pid: 10},
		})
		mustSetLock(t, fs, &fuse.LkIn{
			InHeader: fuse.InHeader{NodeId: f.NodeId}, Fh: f.Fh, Owner: 1,
			Lk: fuse.FileLock{Start: 100, End: 199, Typ: syscall.F_UNLCK, Pid: 10},
		})
		mustSetLock(t, fs, &fuse.LkIn{
			InHeader: fuse.InHeader{NodeId: f.NodeId}, Fh: f.Fh, Owner: 1,
			Lk: fuse.FileLock{Start: 50, End: 79, Typ: syscall.F_RDLCK, Pid: 10},
		})

		var holeOut, tailOut fuse.LkOut
		requireStatus(t, "GetLk 已拆解锁区间", fuse.OK, fs.GetLk(nil, &fuse.LkIn{
			InHeader: fuse.InHeader{NodeId: f.NodeId}, Fh: h2.Fh, Owner: 2,
			Lk: fuse.FileLock{Start: 100, End: 199, Typ: syscall.F_WRLCK, Pid: 20},
		}, &holeOut))
		require.Equal(t, uint32(syscall.F_UNLCK), holeOut.Lk.Typ)

		requireStatus(t, "GetLk 尾部写锁冲突", fuse.OK, fs.GetLk(nil, &fuse.LkIn{
			InHeader: fuse.InHeader{NodeId: f.NodeId}, Fh: h2.Fh, Owner: 2,
			Lk: fuse.FileLock{Start: 200, End: ^uint64(0), Typ: syscall.F_WRLCK, Pid: 20},
		}, &tailOut))
		require.Equal(t, uint32(syscall.F_WRLCK), tailOut.Lk.Typ)
		require.Equal(t, uint64(200), tailOut.Lk.Start)
		require.Equal(t, ^uint64(0), tailOut.Lk.End)

		requireStatus(t, "SetLk 冲突返回 EAGAIN", fuse.EAGAIN, fs.SetLk(nil, &fuse.LkIn{
			InHeader: fuse.InHeader{NodeId: f.NodeId}, Fh: h2.Fh, Owner: 2,
			Lk: fuse.FileLock{Start: 200, End: 250, Typ: syscall.F_WRLCK, Pid: 20},
		}))

		requireStatus(t, "Flush 释放 POSIX 锁", fuse.OK, fs.Flush(nil, &fuse.FlushIn{
			InHeader: fuse.InHeader{NodeId: f.NodeId}, Fh: f.Fh, LockOwner: 1,
		}))
		mustSetLock(t, fs, &fuse.LkIn{
			InHeader: fuse.InHeader{NodeId: f.NodeId}, Fh: h2.Fh, Owner: 2,
			Lk: fuse.FileLock{Start: 200, End: 250, Typ: syscall.F_WRLCK, Pid: 20},
		})

		flockIn1 := &fuse.LkIn{
			InHeader: fuse.InHeader{NodeId: f.NodeId}, Fh: f.Fh, Owner: 101, LkFlags: fuse.LK_FLOCK,
			Lk: fuse.FileLock{Typ: syscall.F_WRLCK, Pid: 101},
		}
		flockInBadFh := &fuse.LkIn{
			InHeader: fuse.InHeader{NodeId: f.NodeId}, Fh: 0, Owner: 202, LkFlags: fuse.LK_FLOCK,
			Lk: fuse.FileLock{Typ: syscall.F_WRLCK, Pid: 202},
		}
		flockIn2 := &fuse.LkIn{
			InHeader: fuse.InHeader{NodeId: f.NodeId}, Fh: h2.Fh, Owner: 202, LkFlags: fuse.LK_FLOCK,
			Lk: fuse.FileLock{Typ: syscall.F_WRLCK, Pid: 202},
		}
		mustSetLock(t, fs, flockIn1)
		requireStatus(t, "Flock 无效 Fh=0 返回 EBADF", fuse.EBADF, fs.SetLk(nil, flockInBadFh))
		requireStatus(t, "Flock 冲突返回 EAGAIN", fuse.EAGAIN, fs.SetLk(nil, flockIn2))
		fs.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: f.NodeId}, Fh: f.Fh, LockOwner: 1})
		mustSetLock(t, fs, flockIn2)

		fs.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: f.NodeId}, Fh: h2.Fh, LockOwner: 2})
	})

	t.Run("SetLkw解锁唤醒与ABBA死锁检测及取消与OnUnmount中断唤醒", func(t *testing.T) {

		srv := newFakeStorage(t, withTTL(1.0, 1.0))
		fs := newTestRawFS(t, srv)
		f := mustCreateFile(t, fs, fuse.FUSE_ROOT_ID, "wait.bin", syscall.O_RDWR, 0o644)
		h2 := mustOpenFile(t, fs, f.NodeId, syscall.O_RDWR)

		mustSetLock(t, fs, &fuse.LkIn{
			InHeader: fuse.InHeader{NodeId: f.NodeId}, Fh: f.Fh, Owner: 1,
			Lk: fuse.FileLock{Start: 0, End: 9, Typ: syscall.F_WRLCK, Pid: 1},
		})
		mustSetLock(t, fs, &fuse.LkIn{
			InHeader: fuse.InHeader{NodeId: f.NodeId}, Fh: h2.Fh, Owner: 2,
			Lk: fuse.FileLock{Start: 10, End: 19, Typ: syscall.F_WRLCK, Pid: 2},
		})
		ch1 := make(chan fuse.Status, 1)
		go func() {
			ch1 <- fs.SetLkw(nil, &fuse.LkIn{
				InHeader: fuse.InHeader{NodeId: f.NodeId}, Fh: f.Fh, Owner: 1,
				Lk: fuse.FileLock{Start: 10, End: 19, Typ: syscall.F_WRLCK, Pid: 1},
			})
		}()
		waitUntilLockConflictsAtLeast(t, fs, 1)
		deadSt := fs.SetLkw(nil, &fuse.LkIn{
			InHeader: fuse.InHeader{NodeId: f.NodeId}, Fh: h2.Fh, Owner: 2,
			Lk: fuse.FileLock{Start: 0, End: 9, Typ: syscall.F_WRLCK, Pid: 2},
		})
		requireStatus(t, "AB-BA 死锁检测", fuse.Status(syscall.EDEADLK), deadSt)

		requireStatus(t, "Flush 解锁唤醒阻塞等待者", fuse.OK, fs.Flush(nil, &fuse.FlushIn{
			InHeader: fuse.InHeader{NodeId: f.NodeId}, Fh: h2.Fh, LockOwner: 2,
		}))
		requireStatus(t, "被阻塞的 SetLkw 唤醒结果", fuse.OK, waitUntilStatusChan(t, ch1, "SetLkw 唤醒"))

		cancelCh := make(chan struct{})
		eintrCh := make(chan fuse.Status, 1)
		conflictsBeforeCancel := fs.Snapshot().Locks.Conflicts
		go func() {
			eintrCh <- fs.SetLkw(cancelCh, &fuse.LkIn{
				InHeader: fuse.InHeader{NodeId: f.NodeId}, Fh: h2.Fh, Owner: 2,
				Lk: fuse.FileLock{Start: 0, End: 9, Typ: syscall.F_WRLCK, Pid: 2},
			})
		}()
		waitUntilLockConflictsAtLeast(t, fs, conflictsBeforeCancel+1)
		close(cancelCh)
		requireStatus(t, "cancel 中断 SetLkw", fuse.EINTR, waitUntilStatusChan(t, eintrCh, "SetLkw EINTR"))

		unmountCh := make(chan fuse.Status, 1)
		conflictsBeforeUnmount := fs.Snapshot().Locks.Conflicts
		go func() {
			unmountCh <- fs.SetLkw(nil, &fuse.LkIn{
				InHeader: fuse.InHeader{NodeId: f.NodeId}, Fh: h2.Fh, Owner: 2,
				Lk: fuse.FileLock{Start: 0, End: 9, Typ: syscall.F_WRLCK, Pid: 2},
			})
		}()
		waitUntilLockConflictsAtLeast(t, fs, conflictsBeforeUnmount+1)
		fs.OnUnmount()
		require.NotEqual(t, fuse.OK, waitUntilStatusChan(t, unmountCh, "OnUnmount 中断 SetLkw"))

		fs.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: f.NodeId}, Fh: f.Fh, LockOwner: 1})
		fs.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: f.NodeId}, Fh: h2.Fh, LockOwner: 2})
	})
}
