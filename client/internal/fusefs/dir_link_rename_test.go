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
