package fusefs_test

import (
	"encoding/binary"
	"fmt"
	"strings"
	"syscall"
	"testing"
	"unsafe"

	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/stretchr/testify/require"

	"github.com/starsrc/bowl/client/internal/fusefs"
)

const (
	renameNoReplace   uint32 = 1 << 0
	renameExchange    uint32 = 1 << 1
	renameWhiteout    uint32 = 1 << 2
	direntHeaderLen          = 24
	entryOutHeaderLen        = int(unsafe.Sizeof(fuse.EntryOut{}))
)

type decodedDirEntry struct {
	ino  uint64
	off  uint64
	name string
}

func decodeDirBuffer(t *testing.T, raw []byte, plus bool) []decodedDirEntry {
	t.Helper()
	prefix := 0
	if plus {
		prefix = entryOutHeaderLen
	}
	var out []decodedDirEntry
	pos := 0
	for pos < len(raw) {
		head := pos + prefix
		require.LessOrEqual(t, head+direntHeaderLen, len(raw), "目录缓冲区头部截断: pos=%d len=%d", pos, len(raw))
		ino := binary.LittleEndian.Uint64(raw[head : head+8])
		off := binary.LittleEndian.Uint64(raw[head+8 : head+16])
		nameLen := int(binary.LittleEndian.Uint32(raw[head+16 : head+20]))
		nameStart := head + direntHeaderLen
		require.LessOrEqual(t, nameStart+nameLen, len(raw), "目录项名称越界: nameLen=%d len=%d", nameLen, len(raw))
		name := string(raw[nameStart : nameStart+nameLen])
		out = append(out, decodedDirEntry{ino: ino, off: off, name: name})
		padding := (8 - (nameLen & 7)) & 7
		pos = nameStart + nameLen + padding
	}
	return out
}

func readAllDirEntries(t *testing.T, fs *fusefs.RawFS, dirIno uint64, plus bool, pageBytes int) (entries []decodedDirEntry, pages int) {
	t.Helper()
	var openOut fuse.OpenOut
	requireStatus(t, fmt.Sprintf("OpenDir(ino=%d)", dirIno), fuse.OK, fs.OpenDir(nil, &fuse.OpenIn{InHeader: fuse.InHeader{NodeId: dirIno}}, &openOut))
	defer fs.ReleaseDir(&fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: dirIno}, Fh: openOut.Fh})

	var offset uint64
	buf := make([]byte, pageBytes)
	for {
		list := fuse.NewDirEntryList(buf, offset)
		in := &fuse.ReadIn{
			InHeader: fuse.InHeader{NodeId: dirIno},
			Fh:       openOut.Fh,
			Offset:   offset,
			Size:     uint32(pageBytes),
		}
		var st fuse.Status
		if plus {
			st = fs.ReadDirPlus(nil, in, list)
		} else {
			st = fs.ReadDir(nil, in, list)
		}
		requireStatus(t, fmt.Sprintf("ReadDir(plus=%v, offset=%d)", plus, offset), fuse.OK, st)
		raw := list.Bytes()
		if len(raw) == 0 {
			break
		}
		pages++
		chunk := decodeDirBuffer(t, raw, plus)
		require.NotEmpty(t, chunk)
		entries = append(entries, chunk...)
		offset = chunk[len(chunk)-1].off
	}
	return entries, pages
}

func collectEntryNameSet(entries []decodedDirEntry) map[string]uint64 {
	set := make(map[string]uint64, len(entries))
	for _, e := range entries {
		set[e.name] = e.ino
	}
	return set
}

func seedLargeDirectory(srv *fakeStorage, parentIno uint64, count int) {
	for i := range count {
		srv.seedFile(parentIno, fmt.Sprintf("item-%04d.dat", i), 0o644, nil)
	}
}

func TestMkdir与Mknod_创建节点可查询并拦截重名超长与只读挂载(t *testing.T) {
	t.Run("Mkdir递增父目录链接计数且Mknod支持特殊设备节点", func(t *testing.T) {
		// Given
		srv := newFakeStorage(t, withTTL(1.0, 1.0))
		fs := newTestRawFS(t, srv)
		beforeRoot := mustGetAttr(t, fs, fuse.FUSE_ROOT_ID)

		// When
		subOut := mustMkdir(t, fs, fuse.FUSE_ROOT_ID, "workspace", 0o755)
		var fifoOut fuse.EntryOut
		fifoSt := fs.Mknod(nil, &fuse.MknodIn{
			InHeader: fuse.InHeader{NodeId: subOut.NodeId},
			Mode:     syscall.S_IFIFO | 0o600,
			Rdev:     0x102,
		}, "pipe.fifo", &fifoOut)

		// Then
		requireStatus(t, "Mknod", fuse.OK, fifoSt)
		require.Equal(t, uint32(syscall.S_IFIFO|0o600), fifoOut.Mode)
		require.Equal(t, uint32(0x102), fifoOut.Rdev)
		looked := mustLookup(t, fs, subOut.NodeId, "pipe.fifo")
		require.Equal(t, fifoOut.NodeId, looked.NodeId)
		afterRoot := mustGetAttr(t, fs, fuse.FUSE_ROOT_ID)
		require.Equal(t, beforeRoot.Nlink+1, afterRoot.Nlink, "Mkdir 必须递增父目录 nlink")

		var dupOut, longOut fuse.EntryOut
		requireStatus(t, "重复 Mkdir", fuse.Status(syscall.EEXIST), fs.Mkdir(nil, &fuse.MkdirIn{
			InHeader: fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID},
			Mode:     0o755,
		}, "workspace", &dupOut))
		requireStatus(t, "超长名 Mknod", fuse.Status(syscall.ENAMETOOLONG), fs.Mknod(nil, &fuse.MknodIn{
			InHeader: fuse.InHeader{NodeId: subOut.NodeId},
			Mode:     syscall.S_IFREG | 0o644,
		}, strings.Repeat("n", fusefs.MaxFilenameBytes+1), &longOut))
	})

	t.Run("只读挂载拦截Mkdir与Mknod并返回EROFS", func(t *testing.T) {
		// Given
		srv := newFakeStorage(t)
		fs := newTestRawFS(t, srv, withMode(fusefs.MountModeRO))
		var entryOut fuse.EntryOut

		// When
		mkdirSt := fs.Mkdir(nil, &fuse.MkdirIn{InHeader: fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID}, Mode: 0o755}, "ro-dir", &entryOut)
		mknodSt := fs.Mknod(nil, &fuse.MknodIn{InHeader: fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID}, Mode: syscall.S_IFREG | 0o644}, "ro-node", &entryOut)

		// Then
		requireStatus(t, "只读 Mkdir", fuse.Status(syscall.EROFS), mkdirSt)
		requireStatus(t, "只读 Mknod", fuse.Status(syscall.EROFS), mknodSt)
		require.Zero(t, srv.mkdirCalls.Load())
		require.Zero(t, srv.mknodCalls.Load())
	})
}

func TestOpenDir与ReadDir_超单页大目录分页完整且校验非目录与已删除目录句柄(t *testing.T) {
	t.Run("600项大目录通过ReadDir与ReadDirPlus多页偏移完整遍历", func(t *testing.T) {
		// Given
		const totalFiles = 600
		srv := newFakeStorage(t, withTTL(1.0, 1.0))
		bigDir := srv.seedDir(1, "big", 0o755)
		seedLargeDirectory(srv, bigDir, totalFiles)
		fs := newTestRawFS(t, srv)

		// When: 使用受限页缓冲区强制触发 >512 条目且 offset != 0 的多次续页读取
		entriesPlain, plainPages := readAllDirEntries(t, fs, bigDir, false, 4096)
		entriesPlus, plusPages := readAllDirEntries(t, fs, bigDir, true, 8192)

		// Then
		require.Greater(t, plainPages, 1, "ReadDir 必须经过多页分页读取")
		require.Greater(t, plusPages, 1, "ReadDirPlus 必须经过多页分页读取")
		require.Len(t, entriesPlain, totalFiles+2)
		require.Len(t, entriesPlus, totalFiles+2)

		plainSet := collectEntryNameSet(entriesPlain)
		plusSet := collectEntryNameSet(entriesPlus)
		require.Len(t, plainSet, totalFiles+2)
		require.Len(t, plusSet, totalFiles+2)
		require.Equal(t, bigDir, plusSet["."])
		require.Equal(t, uint64(fuse.FUSE_ROOT_ID), plusSet[".."])
		require.NotZero(t, plusSet["item-0000.dat"])
		require.NotZero(t, plusSet["item-0599.dat"])
	})

	t.Run("非目录OpenDir返回ENOTDIR且目录已打开时Rmdir后GetAttr与FsyncDir保持语义", func(t *testing.T) {
		// Given
		srv := newFakeStorage(t, withTTL(1.0, 1.0))
		fileIno := srv.seedFile(1, "plain.txt", 0o644, []byte("abc"))
		dirOut := mustMkdir(t, newTestRawFS(t, srv), fuse.FUSE_ROOT_ID, "open-then-rmdir", 0o755)
		fs := newTestRawFS(t, srv)

		// When: 普通文件 OpenDir 返回 ENOTDIR；目录带写标志 Open 返回 EISDIR
		var dh fuse.OpenOut
		notDirSt := fs.OpenDir(nil, &fuse.OpenIn{InHeader: fuse.InHeader{NodeId: fileIno}}, &dh)
		var fileOpenOut fuse.OpenOut
		isDirSt := fs.Open(nil, &fuse.OpenIn{InHeader: fuse.InHeader{NodeId: dirOut.NodeId}, Flags: uint32(syscall.O_RDWR)}, &fileOpenOut)

		// 打开目录后再执行 Rmdir，仍持有的目录句柄 GetAttr/FsyncDir 按已删除目录快路径成功，ReleaseDir 后失效
		var dirHandle fuse.OpenOut
		requireStatus(t, "OpenDir", fuse.OK, fs.OpenDir(nil, &fuse.OpenIn{InHeader: fuse.InHeader{NodeId: dirOut.NodeId}}, &dirHandle))
		requireStatus(t, "Rmdir", fuse.OK, fs.Rmdir(nil, &fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID}, "open-then-rmdir"))
		var removedAttr fuse.AttrOut
		removedAttrSt := fs.GetAttr(nil, &fuse.GetAttrIn{InHeader: fuse.InHeader{NodeId: dirOut.NodeId}}, &removedAttr)
		fsyncRemovedSt := fs.FsyncDir(nil, &fuse.FsyncIn{InHeader: fuse.InHeader{NodeId: dirOut.NodeId}, Fh: dirHandle.Fh})
		fs.ReleaseDir(&fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: dirOut.NodeId}, Fh: dirHandle.Fh})
		var afterReleaseAttr fuse.AttrOut
		afterReleaseSt := fs.GetAttr(nil, &fuse.GetAttrIn{InHeader: fuse.InHeader{NodeId: dirOut.NodeId}}, &afterReleaseAttr)

		// Then
		requireStatus(t, "文件 OpenDir", fuse.Status(syscall.ENOTDIR), notDirSt)
		requireStatus(t, "目录写模式 Open", fuse.Status(syscall.EISDIR), isDirSt)
		requireStatus(t, "已删除打开目录 GetAttr", fuse.OK, removedAttrSt)
		require.Zero(t, removedAttr.Nlink, "已删除打开目录的 nlink 必须降为 0")
		requireStatus(t, "已删除打开目录 FsyncDir", fuse.OK, fsyncRemovedSt)
		requireStatus(t, "ReleaseDir 后 GetAttr", fuse.ENOENT, afterReleaseSt)
	})
}

func TestSymlink与Link_硬链接共享节点计数并拦截目录链接与只读写入(t *testing.T) {
	t.Run("Symlink与Link维护正确nlink并共享同一份文件数据", func(t *testing.T) {
		// Given
		srv := newFakeStorage(t, withTTL(1.0, 1.0))
		fs := newTestRawFS(t, srv)
		created := mustCreateFile(t, fs, fuse.FUSE_ROOT_ID, "orig.bin", syscall.O_RDWR, 0o644)
		payload := buildSeededBytes(128, 0x3A)
		mustWriteAt(t, fs, created.NodeId, created.Fh, 0, payload)
		fs.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: created.NodeId}, Fh: created.Fh})

		// When
		var linkOut, symOut fuse.EntryOut
		linkSt := fs.Link(nil, &fuse.LinkIn{
			InHeader:  fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID},
			Oldnodeid: created.NodeId,
		}, "hard.bin", &linkOut)
		symSt := fs.Symlink(nil, &fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID}, "hard.bin", "sym.bin", &symOut)

		// Then
		requireStatus(t, "Link", fuse.OK, linkSt)
		requireStatus(t, "Symlink", fuse.OK, symSt)
		require.Equal(t, created.NodeId, linkOut.NodeId)
		require.Equal(t, uint32(2), linkOut.Nlink)
		require.Equal(t, uint32(2), srv.nodeNlink(created.NodeId))

		openedHard := mustOpenFile(t, fs, linkOut.NodeId, syscall.O_RDONLY)
		readBack := mustReadAt(t, fs, linkOut.NodeId, openedHard.Fh, 0, len(payload))
		requireBytesMatch(t, "通过硬链接读取写入内容", payload, readBack)
		fs.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: linkOut.NodeId}, Fh: openedHard.Fh})

		target, readlinkSt := fs.Readlink(nil, &fuse.InHeader{NodeId: symOut.NodeId})
		requireStatus(t, "Readlink", fuse.OK, readlinkSt)
		require.Equal(t, "hard.bin", string(target))

		dir := mustMkdir(t, fs, fuse.FUSE_ROOT_ID, "d", 0o755)
		var dirLinkOut fuse.EntryOut
		requireStatus(t, "对目录创建硬链接", fuse.EPERM, fs.Link(nil, &fuse.LinkIn{
			InHeader:  fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID},
			Oldnodeid: dir.NodeId,
		}, "d-hard", &dirLinkOut))
	})

	t.Run("只读挂载拦截Symlink与Link并返回EROFS", func(t *testing.T) {
		// Given
		srv := newFakeStorage(t)
		ino := srv.seedFile(1, "target.txt", 0o644, []byte("x"))
		fs := newTestRawFS(t, srv, withMode(fusefs.MountModeRO))
		var out fuse.EntryOut

		// When
		symSt := fs.Symlink(nil, &fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID}, "target.txt", "ro-sym", &out)
		linkSt := fs.Link(nil, &fuse.LinkIn{InHeader: fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID}, Oldnodeid: ino}, "ro-hard", &out)

		// Then
		requireStatus(t, "只读 Symlink", fuse.Status(syscall.EROFS), symSt)
		requireStatus(t, "只读 Link", fuse.Status(syscall.EROFS), linkSt)
	})
}

func TestRmdir与Unlink_拦截非空目录删除并区分在途孤儿与硬链接脏位保留(t *testing.T) {
	t.Run("Rmdir拒绝非空目录与常规文件并在删除空目录时防父链接下溢", func(t *testing.T) {
		// Given
		srv := newFakeStorage(t, withTTL(1.0, 1.0))
		fs := newTestRawFS(t, srv)
		dir := mustMkdir(t, fs, fuse.FUSE_ROOT_ID, "non-empty", 0o755)
		child := mustCreateFile(t, fs, dir.NodeId, "child.txt", syscall.O_RDWR, 0o644)
		fs.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: child.NodeId}, Fh: child.Fh})

		// When & Then
		requireStatus(t, "非空目录 Rmdir", fuse.Status(syscall.ENOTEMPTY), fs.Rmdir(nil, &fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID}, "non-empty"))
		requireStatus(t, "对普通文件执行 Rmdir", fuse.Status(syscall.ENOTDIR), fs.Rmdir(nil, &fuse.InHeader{NodeId: dir.NodeId}, "child.txt"))

		requireStatus(t, "删除子文件", fuse.OK, fs.Unlink(nil, &fuse.InHeader{NodeId: dir.NodeId}, "child.txt"))
		requireStatus(t, "删除已清空目录", fuse.OK, fs.Rmdir(nil, &fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID}, "non-empty"))
		rootAttr := mustGetAttr(t, fs, fuse.FUSE_ROOT_ID)
		require.Equal(t, uint32(2), rootAttr.Nlink, "父目录 nlink 必须精准回落到 2")
	})

	t.Run("双句柄打开文件Unlink后延后至最后一次Release才触发孤儿回收", func(t *testing.T) {
		// Given
		srv := newFakeStorage(t, withTTL(1.0, 1.0))
		fs := newTestRawFS(t, srv)
		h1 := mustCreateFile(t, fs, fuse.FUSE_ROOT_ID, "orphan.bin", syscall.O_RDWR, 0o644)
		mustWriteAt(t, fs, h1.NodeId, h1.Fh, 0, []byte("keep-until-last-close"))
		h2 := mustOpenFile(t, fs, h1.NodeId, syscall.O_RDONLY)

		// When: Unlink 时仍有 2 个打开句柄，不得提前回收
		requireStatus(t, "Unlink 已打开文件", fuse.OK, fs.Unlink(nil, &fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID}, "orphan.bin"))
		require.Zero(t, srv.reclaimOrphanCalls.Load(), "仍有打开句柄时不得触发 ReclaimOrphan")
		require.True(t, srv.hasNode(h1.NodeId))

		// 释放第一个句柄后仍有一个句柄存活，仍可读且不得回收
		fs.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: h1.NodeId}, Fh: h1.Fh})
		require.Zero(t, srv.reclaimOrphanCalls.Load())
		requireBytesMatch(t, "半关闭后通过第二句柄继续读取", []byte("keep-until-last-close"), mustReadAt(t, fs, h1.NodeId, h2.Fh, 0, 21))

		// 释放最后一个句柄后立即回收孤儿节点，且 OnUnmount 不再发多余 Fsync
		fs.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: h1.NodeId}, Fh: h2.Fh})
		require.Equal(t, int64(1), srv.reclaimOrphanCalls.Load(), "最终句柄释放必须触发恰好 1 次 ReclaimOrphan")
		require.False(t, srv.hasNode(h1.NodeId))

		beforeFsync := srv.fsyncCalls.Load()
		fs.OnUnmount()
		require.Equal(t, beforeFsync, srv.fsyncCalls.Load(), "单链接孤儿删除后 OnUnmount 不得触发无效 Fsync")
	})

	t.Run("多硬链接删除一条链接后保留脏标记并在OnUnmount落盘", func(t *testing.T) {
		// Given
		srv := newFakeStorage(t, withTTL(1.0, 1.0))
		fs := newTestRawFS(t, srv)
		created := mustCreateFile(t, fs, fuse.FUSE_ROOT_ID, "link-a", syscall.O_RDWR, 0o644)
		mustWriteAt(t, fs, created.NodeId, created.Fh, 0, []byte("shared-dirty"))
		var linkOut fuse.EntryOut
		requireStatus(t, "Link", fuse.OK, fs.Link(nil, &fuse.LinkIn{
			InHeader:  fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID},
			Oldnodeid: created.NodeId,
		}, "link-b", &linkOut))
		// 先 Release 确保没有残留打开句柄，仅靠 dirtyInodes 验证残留链接语义
		fs.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: created.NodeId}, Fh: created.Fh})

		// When: 删除 link-a 时 link-b 仍存活（nlink=2 -> 1）
		fsyncBeforeUnlink := srv.fsyncCalls.Load()
		requireStatus(t, "Unlink 首条硬链接", fuse.OK, fs.Unlink(nil, &fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID}, "link-a"))

		// Then: 未触发孤儿回收且 OnUnmount 必须为存活硬链接执行 Fsync
		require.Zero(t, srv.reclaimOrphanCalls.Load())
		fs.OnUnmount()
		require.Equal(t, fsyncBeforeUnlink+1, srv.fsyncCalls.Load(), "多硬链接仅删除其一时必须保留脏位并在 OnUnmount 同步")
	})
}

func TestRename_跨目录迁移条件标志交换成环拦截与覆盖打开目标回收(t *testing.T) {
	t.Run("常规重命名与跨目录目录迁移精准维护双亲nlink", func(t *testing.T) {
		// Given
		srv := newFakeStorage(t, withTTL(1.0, 1.0))
		fs := newTestRawFS(t, srv)
		srcParent := mustMkdir(t, fs, fuse.FUSE_ROOT_ID, "src-p", 0o755)
		dstParent := mustMkdir(t, fs, fuse.FUSE_ROOT_ID, "dst-p", 0o755)
		movingDir := mustMkdir(t, fs, srcParent.NodeId, "sub", 0o755)
		require.Equal(t, uint32(3), mustGetAttr(t, fs, srcParent.NodeId).Nlink)
		require.Equal(t, uint32(2), mustGetAttr(t, fs, dstParent.NodeId).Nlink)

		// When: 跨父目录移动子目录，并执行同路径自重命名
		requireStatus(t, "跨父目录 Rename 目录", fuse.OK, fs.Rename(nil, &fuse.RenameIn{
			InHeader: fuse.InHeader{NodeId: srcParent.NodeId},
			Newdir:   dstParent.NodeId,
		}, "sub", "sub-moved"))
		requireStatus(t, "自重命名", fuse.OK, fs.Rename(nil, &fuse.RenameIn{
			InHeader: fuse.InHeader{NodeId: dstParent.NodeId},
			Newdir:   dstParent.NodeId,
		}, "sub-moved", "sub-moved"))

		// Then
		require.Equal(t, uint32(2), mustGetAttr(t, fs, srcParent.NodeId).Nlink, "移出目录后原父目录 nlink 减 1")
		require.Equal(t, uint32(3), mustGetAttr(t, fs, dstParent.NodeId).Nlink, "移入目录后新父目录 nlink 加 1")
		looked := mustLookup(t, fs, dstParent.NodeId, "sub-moved")
		require.Equal(t, movingDir.NodeId, looked.NodeId)
		var gone fuse.EntryOut
		requireStatus(t, "原目录项应返回 ENOENT", fuse.ENOENT, fs.Lookup(nil, &fuse.InHeader{NodeId: srcParent.NodeId}, "sub", &gone))
	})

	t.Run("NOREPLACE与EXCHANGE和WHITEOUT标志按契约生效", func(t *testing.T) {
		// Given
		srv := newFakeStorage(t, withTTL(1.0, 1.0))
		fs := newTestRawFS(t, srv)
		aIno := srv.seedFile(1, "a", 0o644, []byte("AAA"))
		bIno := srv.seedFile(1, "b", 0o644, []byte("BBB"))
		// 预热 Lookup 缓存以验证 Rename 后缓存同步切换
		_ = mustLookup(t, fs, fuse.FUSE_ROOT_ID, "a")
		_ = mustLookup(t, fs, fuse.FUSE_ROOT_ID, "b")

		// When & Then: NOREPLACE 冲突返回 EEXIST；EXCHANGE 原子互换；WHITEOUT 留下字符设备占位并穿透缓存
		requireStatus(t, "NOREPLACE 冲突", fuse.Status(syscall.EEXIST), fs.Rename(nil, &fuse.RenameIn{
			InHeader: fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID},
			Newdir:   fuse.FUSE_ROOT_ID,
			Flags:    renameNoReplace,
		}, "a", "b"))

		requireStatus(t, "EXCHANGE 互换", fuse.OK, fs.Rename(nil, &fuse.RenameIn{
			InHeader: fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID},
			Newdir:   fuse.FUSE_ROOT_ID,
			Flags:    renameExchange,
		}, "a", "b"))
		require.Equal(t, bIno, mustLookup(t, fs, fuse.FUSE_ROOT_ID, "a").NodeId)
		require.Equal(t, aIno, mustLookup(t, fs, fuse.FUSE_ROOT_ID, "b").NodeId)

		requireStatus(t, "WHITEOUT 重命名", fuse.OK, fs.Rename(nil, &fuse.RenameIn{
			InHeader: fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID},
			Newdir:   fuse.FUSE_ROOT_ID,
			Flags:    renameWhiteout,
		}, "a", "c"))
		wo := mustLookup(t, fs, fuse.FUSE_ROOT_ID, "a")
		require.Equal(t, uint32(syscall.S_IFCHR), wo.Mode&syscall.S_IFMT, "WHITEOUT 后必须穿透缓存查见字符设备占位节点")
		require.Equal(t, bIno, mustLookup(t, fs, fuse.FUSE_ROOT_ID, "c").NodeId)
	})

	t.Run("祖先成环返回EINVAL且覆盖已打开目标后延后回收并清除脏位", func(t *testing.T) {
		// Given
		srv := newFakeStorage(t, withTTL(1.0, 1.0))
		fs := newTestRawFS(t, srv)
		anc := mustMkdir(t, fs, fuse.FUSE_ROOT_ID, "anc", 0o755)
		desc := mustMkdir(t, fs, anc.NodeId, "desc", 0o755)

		src := mustCreateFile(t, fs, fuse.FUSE_ROOT_ID, "src.txt", syscall.O_RDWR, 0o644)
		fs.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: src.NodeId}, Fh: src.Fh})
		victim := mustCreateFile(t, fs, fuse.FUSE_ROOT_ID, "victim.txt", syscall.O_RDWR, 0o644)
		mustWriteAt(t, fs, victim.NodeId, victim.Fh, 0, []byte("victim-dirty"))

		// When
		cycleSt := fs.Rename(nil, &fuse.RenameIn{
			InHeader: fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID},
			Newdir:   desc.NodeId,
		}, "anc", "loop")
		overwriteSt := fs.Rename(nil, &fuse.RenameIn{
			InHeader: fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID},
			Newdir:   fuse.FUSE_ROOT_ID,
		}, "src.txt", "victim.txt")

		// Then
		requireStatus(t, "祖先成环 Rename", fuse.Status(syscall.EINVAL), cycleSt)
		requireStatus(t, "覆盖已打开目标 Rename", fuse.OK, overwriteSt)
		require.Zero(t, srv.reclaimOrphanCalls.Load(), "被覆盖打开目标未关闭前不得回收")

		fs.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: victim.NodeId}, Fh: victim.Fh})
		require.Equal(t, int64(1), srv.reclaimOrphanCalls.Load(), "被覆盖打开目标最终释放后必须回收")

		beforeFsync := srv.fsyncCalls.Load()
		fs.OnUnmount()
		require.Equal(t, beforeFsync, srv.fsyncCalls.Load(), "被覆盖单链接目标在回收后不得残留脏位")
	})
}

func TestRename_ParentNlinkAndDirCacheCoherence(t *testing.T) {
	t.Run("GivenCachedDirs_WhenSameParentDirOverwritesEmptyDir_ThenParentNlinkDecrementsAndOverwrittenDirPurged", func(t *testing.T) {
		// Given: parent has two cached subdirs (nlink=4), and target empty dir is marked complete in cache.
		srv := newFakeStorage(t, withTTL(2.0, 2.0))
		fs := newTestRawFS(t, srv)
		parent := mustMkdir(t, fs, fuse.FUSE_ROOT_ID, "parent", 0o755)
		_ = mustMkdir(t, fs, parent.NodeId, "src", 0o755)
		dst := mustMkdir(t, fs, parent.NodeId, "dst", 0o755)
		require.Equal(t, uint32(4), mustGetAttr(t, fs, parent.NodeId).Nlink)
		getAttrBefore := srv.getAttrCalls.Load()
		lookupBefore := srv.lookupCalls.Load()

		// When: src overwrites empty dir dst inside the same parent.
		requireStatus(t, "same-parent dir overwrite", fuse.OK, fs.Rename(nil, &fuse.RenameIn{
			InHeader: fuse.InHeader{NodeId: parent.NodeId},
			Newdir:   parent.NodeId,
		}, "src", "dst"))

		// Then: parent cached nlink decrements to 3 with zero extra GetAttr RPCs, and dst's completeDirs entry is purged.
		require.Equal(t, uint32(3), mustGetAttr(t, fs, parent.NodeId).Nlink, "same-parent dir overwrite must decrement cached parent nlink")
		require.Equal(t, getAttrBefore, srv.getAttrCalls.Load(), "known delta must keep parent inode cached without GetAttr RPC")
		var ghost fuse.EntryOut
		requireStatus(t, "lookup inside overwritten dir", fuse.ENOENT, fs.Lookup(nil, &fuse.InHeader{NodeId: dst.NodeId}, "ghost", &ghost))
		require.Equal(t, lookupBefore+1, srv.lookupCalls.Load(), "overwritten dir completeDirs must be purged instead of serving cached negative ENOENT")
	})

	t.Run("GivenCachedDirs_WhenCrossParentDirOverwritesEmptyDir_ThenOldParentDecrementsAndNewParentUnchanged", func(t *testing.T) {
		// Given: oldParent has src (nlink=3) and newParent has empty dst (nlink=3).
		srv := newFakeStorage(t, withTTL(2.0, 2.0))
		fs := newTestRawFS(t, srv)
		oldParent := mustMkdir(t, fs, fuse.FUSE_ROOT_ID, "old-p", 0o755)
		newParent := mustMkdir(t, fs, fuse.FUSE_ROOT_ID, "new-p", 0o755)
		_ = mustMkdir(t, fs, oldParent.NodeId, "src", 0o755)
		dst := mustMkdir(t, fs, newParent.NodeId, "dst", 0o755)
		require.Equal(t, uint32(3), mustGetAttr(t, fs, oldParent.NodeId).Nlink)
		require.Equal(t, uint32(3), mustGetAttr(t, fs, newParent.NodeId).Nlink)
		getAttrBefore := srv.getAttrCalls.Load()
		lookupBefore := srv.lookupCalls.Load()

		// When: old-p/src overwrites empty dir new-p/dst across parents.
		requireStatus(t, "cross-parent dir overwrite", fuse.OK, fs.Rename(nil, &fuse.RenameIn{
			InHeader: fuse.InHeader{NodeId: oldParent.NodeId},
			Newdir:   newParent.NodeId,
		}, "src", "dst"))

		// Then: oldParent nlink drops to 2, newParent nlink stays 3 (not 4), zero extra GetAttr RPCs, and dst completeDirs purged.
		require.Equal(t, uint32(2), mustGetAttr(t, fs, oldParent.NodeId).Nlink)
		require.Equal(t, uint32(3), mustGetAttr(t, fs, newParent.NodeId).Nlink)
		require.Equal(t, getAttrBefore, srv.getAttrCalls.Load())
		var ghost fuse.EntryOut
		requireStatus(t, "lookup inside cross-parent overwritten dir", fuse.ENOENT, fs.Lookup(nil, &fuse.InHeader{NodeId: dst.NodeId}, "ghost", &ghost))
		require.Equal(t, lookupBefore+1, srv.lookupCalls.Load())
	})

	t.Run("GivenCachedDirAndFile_WhenCrossParentExchange_ThenBothParentNlinksAdjustWithoutExtraGetAttr", func(t *testing.T) {
		// Given: oldParent holds subdir (nlink=3) and newParent holds regular file (nlink=2).
		srv := newFakeStorage(t, withTTL(2.0, 2.0))
		fs := newTestRawFS(t, srv)
		oldParent := mustMkdir(t, fs, fuse.FUSE_ROOT_ID, "ex-old", 0o755)
		newParent := mustMkdir(t, fs, fuse.FUSE_ROOT_ID, "ex-new", 0o755)
		_ = mustMkdir(t, fs, oldParent.NodeId, "dir-a", 0o755)
		f := mustCreateFile(t, fs, newParent.NodeId, "file-b", syscall.O_RDWR, 0o644)
		fs.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: f.NodeId}, Fh: f.Fh})
		require.Equal(t, uint32(3), mustGetAttr(t, fs, oldParent.NodeId).Nlink)
		require.Equal(t, uint32(2), mustGetAttr(t, fs, newParent.NodeId).Nlink)
		getAttrBefore := srv.getAttrCalls.Load()

		// When: exchange oldParent/dir-a with newParent/file-b.
		requireStatus(t, "cross-parent exchange dir<->file", fuse.OK, fs.Rename(nil, &fuse.RenameIn{
			InHeader: fuse.InHeader{NodeId: oldParent.NodeId},
			Newdir:   newParent.NodeId,
			Flags:    renameExchange,
		}, "dir-a", "file-b"))

		// Then: oldParent nlink drops to 2 and newParent nlink increments to 3 with zero GetAttr RPCs.
		require.Equal(t, uint32(2), mustGetAttr(t, fs, oldParent.NodeId).Nlink)
		require.Equal(t, uint32(3), mustGetAttr(t, fs, newParent.NodeId).Nlink)
		require.Equal(t, getAttrBefore, srv.getAttrCalls.Load())

		// When: exchange back (file-a in oldParent <-> dir-b in newParent) after re-caching exchanged node attrs.
		_ = mustGetAttr(t, fs, mustLookup(t, fs, oldParent.NodeId, "dir-a").NodeId)
		_ = mustGetAttr(t, fs, mustLookup(t, fs, newParent.NodeId, "file-b").NodeId)
		getAttrBeforeBack := srv.getAttrCalls.Load()
		requireStatus(t, "cross-parent exchange file<->dir", fuse.OK, fs.Rename(nil, &fuse.RenameIn{
			InHeader: fuse.InHeader{NodeId: oldParent.NodeId},
			Newdir:   newParent.NodeId,
			Flags:    renameExchange,
		}, "dir-a", "file-b"))

		// Then: oldParent nlink returns to 3 and newParent nlink returns to 2 with zero GetAttr RPCs.
		require.Equal(t, uint32(3), mustGetAttr(t, fs, oldParent.NodeId).Nlink)
		require.Equal(t, uint32(2), mustGetAttr(t, fs, newParent.NodeId).Nlink)
		require.Equal(t, getAttrBeforeBack, srv.getAttrCalls.Load())
	})

	t.Run("GivenCachedFile_WhenNormalRename_ThenParentInodeCacheStaysWarmWithoutGetAttr", func(t *testing.T) {
		// Given: cached file in oldParent and uncached target slot in newParent.
		srv := newFakeStorage(t, withTTL(2.0, 2.0))
		fs := newTestRawFS(t, srv)
		oldParent := mustMkdir(t, fs, fuse.FUSE_ROOT_ID, "f-old", 0o755)
		newParent := mustMkdir(t, fs, fuse.FUSE_ROOT_ID, "f-new", 0o755)
		f := mustCreateFile(t, fs, oldParent.NodeId, "doc.txt", syscall.O_RDWR, 0o644)
		fs.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: f.NodeId}, Fh: f.Fh})
		// Seed a file in newParent without caching it so targetKnown=false on rename.
		_ = srv.seedFile(newParent.NodeId, "uncached-target.txt", 0o644, []byte("x"))
		getAttrBefore := srv.getAttrCalls.Load()

		// When: move cached regular file over uncached target across parents.
		requireStatus(t, "cross-parent file rename", fuse.OK, fs.Rename(nil, &fuse.RenameIn{
			InHeader: fuse.InHeader{NodeId: oldParent.NodeId},
			Newdir:   newParent.NodeId,
		}, "doc.txt", "uncached-target.txt"))

		// Then: file cannot overwrite a dir, so both parents stay cached with nlink=2 and zero GetAttr RPCs.
		require.Equal(t, uint32(2), mustGetAttr(t, fs, oldParent.NodeId).Nlink)
		require.Equal(t, uint32(2), mustGetAttr(t, fs, newParent.NodeId).Nlink)
		require.Equal(t, getAttrBefore, srv.getAttrCalls.Load())
	})

	t.Run("GivenUncachedSourceOrTarget_WhenRenameSucceeds_ThenParentInodeDroppedAndGetAttrFetchesFreshNlink", func(t *testing.T) {
		// Case A: source dir is uncached in oldParent (seeded on server before GetAttr warms parent).
		srv := newFakeStorage(t, withTTL(2.0, 2.0))
		oldParentIno := srv.seedDir(1, "u-old", 0o755)
		newParentIno := srv.seedDir(1, "u-new", 0o755)
		_ = srv.seedDir(oldParentIno, "uncached-src", 0o755)
		fs := newTestRawFS(t, srv)
		require.Equal(t, uint32(3), mustGetAttr(t, fs, oldParentIno).Nlink)
		require.Equal(t, uint32(2), mustGetAttr(t, fs, newParentIno).Nlink)
		getAttrBefore := srv.getAttrCalls.Load()

		requireStatus(t, "uncached source cross-parent rename", fuse.OK, fs.Rename(nil, &fuse.RenameIn{
			InHeader: fuse.InHeader{NodeId: oldParentIno},
			Newdir:   newParentIno,
		}, "uncached-src", "moved-src"))

		require.Equal(t, uint32(2), mustGetAttr(t, fs, oldParentIno).Nlink, "dropped oldParent must refresh nlink from server")
		require.Equal(t, uint32(3), mustGetAttr(t, fs, newParentIno).Nlink, "dropped newParent must refresh nlink from server")
		require.Equal(t, getAttrBefore+2, srv.getAttrCalls.Load(), "unknown deltas must drop both parent inodes")

		// Case B: cached source dir overwrites uncached empty dir in newParent -> oldParent stays cached (-1), newParent dropped.
		srvB := newFakeStorage(t, withTTL(2.0, 2.0))
		oldB := srvB.seedDir(1, "b-old", 0o755)
		newB := srvB.seedDir(1, "b-new", 0o755)
		_ = srvB.seedDir(newB, "uncached-dst", 0o755)
		fsB := newTestRawFS(t, srvB)
		require.Equal(t, uint32(2), mustGetAttr(t, fsB, oldB).Nlink)
		require.Equal(t, uint32(3), mustGetAttr(t, fsB, newB).Nlink)
		_ = mustMkdir(t, fsB, oldB, "cached-src", 0o755)
		require.Equal(t, uint32(3), mustGetAttr(t, fsB, oldB).Nlink)
		getAttrBeforeB := srvB.getAttrCalls.Load()

		requireStatus(t, "cached src dir over uncached dst dir", fuse.OK, fsB.Rename(nil, &fuse.RenameIn{
			InHeader: fuse.InHeader{NodeId: oldB},
			Newdir:   newB,
		}, "cached-src", "uncached-dst"))

		require.Equal(t, uint32(2), mustGetAttr(t, fsB, oldB).Nlink, "oldParent delta -1 is known and stays cached")
		require.Equal(t, getAttrBeforeB, srvB.getAttrCalls.Load(), "oldParent must not miss GetAttr")
		require.Equal(t, uint32(3), mustGetAttr(t, fsB, newB).Nlink, "newParent must drop and refresh exact nlink=3")
		require.Equal(t, getAttrBeforeB+1, srvB.getAttrCalls.Load(), "only newParent should hit GetAttr")

		// Case C: cross-parent RENAME_EXCHANGE with uncached target drops both parents.
		srvC := newFakeStorage(t, withTTL(2.0, 2.0))
		oldC := srvC.seedDir(1, "c-old", 0o755)
		newC := srvC.seedDir(1, "c-new", 0o755)
		_ = srvC.seedFile(newC, "uncached-file", 0o644, []byte("f"))
		fsC := newTestRawFS(t, srvC)
		require.Equal(t, uint32(2), mustGetAttr(t, fsC, oldC).Nlink)
		require.Equal(t, uint32(2), mustGetAttr(t, fsC, newC).Nlink)
		_ = mustMkdir(t, fsC, oldC, "cached-dir", 0o755)
		getAttrBeforeC := srvC.getAttrCalls.Load()

		requireStatus(t, "cross-parent exchange with uncached target", fuse.OK, fsC.Rename(nil, &fuse.RenameIn{
			InHeader: fuse.InHeader{NodeId: oldC},
			Newdir:   newC,
			Flags:    renameExchange,
		}, "cached-dir", "uncached-file"))

		require.Equal(t, uint32(2), mustGetAttr(t, fsC, oldC).Nlink)
		require.Equal(t, uint32(3), mustGetAttr(t, fsC, newC).Nlink)
		require.Equal(t, getAttrBeforeC+2, srvC.getAttrCalls.Load(), "unknown exchange delta must drop both parents")

		// Case D: cross-parent RENAME_NOREPLACE of cached dir into uncached newParent keeps both parents cached (-1, +1).
		srvD := newFakeStorage(t, withTTL(2.0, 2.0))
		oldD := srvD.seedDir(1, "d-old", 0o755)
		newD := srvD.seedDir(1, "d-new", 0o755)
		fsD := newTestRawFS(t, srvD)
		require.Equal(t, uint32(2), mustGetAttr(t, fsD, oldD).Nlink)
		require.Equal(t, uint32(2), mustGetAttr(t, fsD, newD).Nlink)
		_ = mustMkdir(t, fsD, oldD, "noreplace-dir", 0o755)
		getAttrBeforeD := srvD.getAttrCalls.Load()

		requireStatus(t, "cross-parent RENAME_NOREPLACE dir", fuse.OK, fsD.Rename(nil, &fuse.RenameIn{
			InHeader: fuse.InHeader{NodeId: oldD},
			Newdir:   newD,
			Flags:    renameNoReplace,
		}, "noreplace-dir", "fresh-name"))

		require.Equal(t, uint32(2), mustGetAttr(t, fsD, oldD).Nlink)
		require.Equal(t, uint32(3), mustGetAttr(t, fsD, newD).Nlink)
		require.Equal(t, getAttrBeforeD, srvD.getAttrCalls.Load(), "RENAME_NOREPLACE guarantees no target so deltas are known")
	})

	t.Run("GivenUncachedMovedDir_WhenCrossParentRenameSucceeds_ThenStaleParentByDirPurged", func(t *testing.T) {
		// Given: child dir is seeded under oldParent (not cached in oldParent's entries),
		// then child's own ReadDirPlus + Create populates parentByDir[child]=oldParent and completeDirs[child].
		srv := newFakeStorage(t, withTTL(2.0, 2.0))
		fs := newTestRawFS(t, srv)
		oldParent := mustMkdir(t, fs, fuse.FUSE_ROOT_ID, "p-old", 0o755)
		newParent := mustMkdir(t, fs, fuse.FUSE_ROOT_ID, "p-new", 0o755)
		childIno := srv.seedDir(oldParent.NodeId, "orphan-entry-dir", 0o755)

		// Warm child's 'i', 'd', parentByDir[child]=oldParent, then Create a file to demote 'd' -> completeDirs + parentByDir.
		_ = mustGetAttr(t, fs, childIno)
		entriesBefore, _ := readAllDirEntries(t, fs, childIno, true, 4096)
		require.Equal(t, oldParent.NodeId, collectEntryNameSet(entriesBefore)[".."])
		cf := mustCreateFile(t, fs, childIno, "inner.txt", syscall.O_RDWR, 0o644)
		fs.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: cf.NodeId}, Fh: cf.Fh})

		// When: cross-parent rename moves child while source==0 (not cached in oldParent).
		requireStatus(t, "cross-parent rename with uncached source entry", fuse.OK, fs.Rename(nil, &fuse.RenameIn{
			InHeader: fuse.InHeader{NodeId: oldParent.NodeId},
			Newdir:   newParent.NodeId,
		}, "orphan-entry-dir", "moved-dir"))

		// Then: ReadDirPlus(child) must NOT synthesize stale ".." == oldParent; it refreshes ".." == newParent.
		entriesAfter, _ := readAllDirEntries(t, fs, childIno, true, 4096)
		require.Equal(t, newParent.NodeId, collectEntryNameSet(entriesAfter)[".."], "cross-parent rename with source==0 must purge stale parentByDir")

		// Sub-case: cross-parent RENAME_EXCHANGE with target==0 (target dir's entry in newParent is uncached).
		srvEx := newFakeStorage(t, withTTL(2.0, 2.0))
		fsEx := newTestRawFS(t, srvEx)
		exOld := mustMkdir(t, fsEx, fuse.FUSE_ROOT_ID, "ex-p1", 0o755)
		exNew := mustMkdir(t, fsEx, fuse.FUSE_ROOT_ID, "ex-p2", 0o755)
		fSrc := mustCreateFile(t, fsEx, exOld.NodeId, "f.txt", syscall.O_RDWR, 0o644)
		fsEx.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: fSrc.NodeId}, Fh: fSrc.Fh})
		targetDirIno := srvEx.seedDir(exNew.NodeId, "uncached-target-dir", 0o755)
		_ = mustGetAttr(t, fsEx, targetDirIno)
		exEntriesBefore, _ := readAllDirEntries(t, fsEx, targetDirIno, true, 4096)
		require.Equal(t, exNew.NodeId, collectEntryNameSet(exEntriesBefore)[".."])
		cf2 := mustCreateFile(t, fsEx, targetDirIno, "inner2.txt", syscall.O_RDWR, 0o644)
		fsEx.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: cf2.NodeId}, Fh: cf2.Fh})

		requireStatus(t, "cross-parent exchange with target==0", fuse.OK, fsEx.Rename(nil, &fuse.RenameIn{
			InHeader: fuse.InHeader{NodeId: exOld.NodeId},
			Newdir:   exNew.NodeId,
			Flags:    renameExchange,
		}, "f.txt", "uncached-target-dir"))

		exEntriesAfter, _ := readAllDirEntries(t, fsEx, targetDirIno, true, 4096)
		require.Equal(t, exOld.NodeId, collectEntryNameSet(exEntriesAfter)[".."], "cross-parent exchange with target==0 must purge stale parentByDir")
	})

	t.Run("GivenCachedSourceAndTarget_WhenRenameRuns_ThenPeekInodeDoesNotInflateAttrHits", func(t *testing.T) {
		// Given: cached source and target in root.
		srv := newFakeStorage(t, withTTL(2.0, 2.0))
		fs := newTestRawFS(t, srv)
		_ = mustMkdir(t, fs, fuse.FUSE_ROOT_ID, "peek-src", 0o755)
		_ = mustMkdir(t, fs, fuse.FUSE_ROOT_ID, "peek-dst", 0o755)
		attrHitsBefore := fs.Snapshot().NamespaceCache.AttrHits

		// When
		requireStatus(t, "rename cached dirs", fuse.OK, fs.Rename(nil, &fuse.RenameIn{
			InHeader: fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID},
			Newdir:   fuse.FUSE_ROOT_ID,
		}, "peek-src", "peek-dst"))

		// Then: Rename inspects cached attrs via peekInode without inflating AttrHits.
		require.Equal(t, attrHitsBefore, fs.Snapshot().NamespaceCache.AttrHits)
	})
}
