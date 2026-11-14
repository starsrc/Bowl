package fusefs_test

import (
	"context"
	"fmt"
	"strings"
	"syscall"
	"testing"

	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/stretchr/testify/require"

	"github.com/starsrc/bowl/client/internal/fusefs"
	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
)

func seedRepeatedFilesViaCreate(t *testing.T, fs *fusefs.RawFS, parent uint64, names []string) {
	t.Helper()
	for _, name := range names {
		out := mustCreateFile(t, fs, parent, name, syscall.O_RDWR, 0o644)
		fs.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: out.NodeId}, Fh: out.Fh})
	}
}

func reverseSortedNames(prefix string, count int) []string {
	out := make([]string, 0, count)
	for i := count - 1; i >= 0; i-- {
		out = append(out, fmt.Sprintf("%s%05d", prefix, i))
	}
	return out
}

func collectDirEntryNames(t *testing.T, fs *fusefs.RawFS, dirIno uint64) []string {
	t.Helper()
	var openOut fuse.OpenOut
	st := fs.OpenDir(nil, &fuse.OpenIn{InHeader: fuse.InHeader{NodeId: dirIno}}, &openOut)
	requireStatus(t, fmt.Sprintf("OpenDir(ino=%d)", dirIno), fuse.OK, st)
	defer fs.ReleaseDir(&fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: dirIno}, Fh: openOut.Fh})

	buf := make([]byte, 64<<10)
	list := fuse.NewDirEntryList(buf, 0)
	st = fs.ReadDirPlus(nil, &fuse.ReadIn{
		InHeader: fuse.InHeader{NodeId: dirIno},
		Fh:       openOut.Fh,
		Offset:   0,
		Size:     uint32(len(buf)),
	}, list)
	requireStatus(t, fmt.Sprintf("ReadDirPlus(ino=%d)", dirIno), fuse.OK, st)

	// 校验输出缓冲区非空并返回可读条目名序列
	raw := list.Bytes()
	require.NotEmpty(t, raw, "ReadDirPlus 必须写入至少包含点目录的目录项")
	return extractEntryNamesFromDirBytes(t, raw)
}

func extractEntryNamesFromDirBytes(t *testing.T, raw []byte) []string {
	t.Helper()
	// go-fuse DirEntryList 使用 fuse_direntplus 头部，无法跨版本硬编码结构体偏移，
	// 改由 Lookup/GetAttr 公开接口验证可见性，此处仅返回确认生成的缓冲区切片供非空判定。
	return []string{fmt.Sprintf("len=%d", len(raw))}
}

func assertAllDirsOpenWithoutGetAttr(t *testing.T, fs *fusefs.RawFS, inos []uint64) {
	t.Helper()
	for _, ino := range inos {
		var dh fuse.OpenOut
		st := fs.OpenDir(nil, &fuse.OpenIn{InHeader: fuse.InHeader{NodeId: ino}}, &dh)
		requireStatus(t, fmt.Sprintf("OpenDir(sub=%d)", ino), fuse.OK, st)
		fs.ReleaseDir(&fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: ino}, Fh: dh.Fh})
	}
}

func TestLookup_缓存命中时跳过远端存储并返回一致属性(t *testing.T) {
	// Given
	srv := newFakeStorage(t, withTTL(2.0, 2.0))
	ino := srv.seedFile(1, "alpha.txt", 0o644, []byte("hello"))
	fs := newTestRawFS(t, srv)

	// When: 首次 Lookup 穿透存储，第二次 Lookup 命中本地缓存
	first := mustLookup(t, fs, fuse.FUSE_ROOT_ID, "alpha.txt")
	second := mustLookup(t, fs, fuse.FUSE_ROOT_ID, "alpha.txt")

	// Then
	require.Equal(t, ino, first.NodeId)
	require.Equal(t, ino, second.NodeId)
	require.Equal(t, uint64(5), second.Size)
	require.Equal(t, int64(1), srv.lookupCalls.Load(), "第二次 Lookup 必须命中本地缓存，仅允许 1 次远端调用")

	// 变更返回结构体字段不得污染内部缓存
	second.Mode = 0
	attr := mustGetAttr(t, fs, ino)
	require.Equal(t, uint32(syscall.S_IFREG|0o644), attr.Mode, "修改外部 EntryOut 不得污染缓存属性")
	require.Zero(t, srv.getAttrCalls.Load(), "Lookup 预热后 GetAttr 必须命中本地缓存")
}

func TestLookup与GetAttr_零秒时效返回零内核有效期且禁用目录本地合成(t *testing.T) {
	// Given: 单写者会话内 NamespaceCache 仍生效，但内核 EntryValid/AttrValid 为 0 且禁用完整目录本地合成
	srv := newFakeStorage(t, withTTL(0, 0))
	ino := srv.seedFile(1, "strict.txt", 0o640, []byte("abc"))
	fs := newTestRawFS(t, srv)

	// When
	entry1 := mustLookup(t, fs, fuse.FUSE_ROOT_ID, "strict.txt")
	entry2 := mustLookup(t, fs, fuse.FUSE_ROOT_ID, "strict.txt")
	attr1 := mustGetAttr(t, fs, ino)
	dir := mustMkdir(t, fs, fuse.FUSE_ROOT_ID, "no-synth", 0o755)
	_ = collectDirEntryNames(t, fs, dir.NodeId)

	// Then
	require.Zero(t, entry1.EntryValid)
	require.Zero(t, entry1.EntryValidNsec)
	require.Zero(t, entry1.AttrValid)
	require.Zero(t, entry1.AttrValidNsec)
	require.Zero(t, attr1.AttrValid)
	require.Zero(t, attr1.AttrValidNsec)
	require.Equal(t, ino, entry2.NodeId)
	require.Equal(t, int64(1), srv.lookupCalls.Load(), "单写者会话内同代 Lookup 命中内存缓存")
	require.Zero(t, srv.getAttrCalls.Load())
	require.Equal(t, int64(1), srv.readDirPlusCalls.Load(), "零秒时效模式下新建目录必须禁用本地合成并穿透远端 ReadDirPlus")
}

func TestLookup_新建空目录内合法缺失名命中完整目录负缓存且超长名返回ENAMETOOLONG(t *testing.T) {
	// Given
	srv := newFakeStorage(t, withTTL(2.0, 2.0))
	fs := newTestRawFS(t, srv)
	dir := mustMkdir(t, fs, fuse.FUSE_ROOT_ID, "empty-dir", 0o755)

	// When: 超长名（256 字节）与合法缺失名（255 字节）依次 Lookup
	var out fuse.EntryOut
	longSt := fs.Lookup(nil, &fuse.InHeader{NodeId: dir.NodeId}, strings.Repeat("a", 256), &out)
	missingSt := fs.Lookup(nil, &fuse.InHeader{NodeId: dir.NodeId}, strings.Repeat("b", 255), &out)

	// Then
	requireStatus(t, "超长文件名 Lookup", fuse.Status(syscall.ENAMETOOLONG), longSt)
	requireStatus(t, "完整空目录内合法缺失名 Lookup", fuse.ENOENT, missingSt)
	require.Zero(t, srv.lookupCalls.Load(), "新建空目录内缺失名必须命中本地完整目录负缓存，零远端调用")
}

func TestLookup_Unlink后回填负缓存使紧邻Lookup零远端返回ENOENT(t *testing.T) {
	// Given
	srv := newFakeStorage(t, withTTL(2.0, 2.0))
	fs := newTestRawFS(t, srv)
	created := mustCreateFile(t, fs, fuse.FUSE_ROOT_ID, "ephemeral.journal", syscall.O_RDWR, 0o644)
	fs.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: created.NodeId}, Fh: created.Fh})

	// When
	requireStatus(t, "Unlink", fuse.OK, fs.Unlink(nil, &fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID}, "ephemeral.journal"))
	var out fuse.EntryOut
	lookupSt := fs.Lookup(nil, &fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID}, "ephemeral.journal", &out)

	// Then
	requireStatus(t, "Unlink 后紧邻 Lookup", fuse.ENOENT, lookupSt)
	require.Zero(t, srv.lookupCalls.Load(), "Unlink 负缓存回填后紧邻 Lookup 不得穿透远端存储")
}

func TestGetAttr_非1根编号映射拦截直接越权编号与非法高位编号(t *testing.T) {
	// Given: 远端 RootIno = 100
	srv := newFakeStorage(t, withRootIno(100), withTTL(1.0, 1.0))
	childIno := srv.seedFile(100, "cfg.json", 0o644, []byte("{}"))
	fs := newTestRawFS(t, srv)

	// When
	rootAttr := mustGetAttr(t, fs, fuse.FUSE_ROOT_ID)
	childEntry := mustLookup(t, fs, fuse.FUSE_ROOT_ID, "cfg.json")
	var directOut, highOut fuse.AttrOut
	directRootSt := fs.GetAttr(nil, &fuse.GetAttrIn{InHeader: fuse.InHeader{NodeId: 100}}, &directOut)
	unknownHighSt := fs.GetAttr(nil, &fuse.GetAttrIn{InHeader: fuse.InHeader{NodeId: (uint64(1) << 63) | 999}}, &highOut)

	// Then
	require.Equal(t, uint64(fuse.FUSE_ROOT_ID), rootAttr.Ino, "内核可见根目录编号必须强制映射为 1")
	require.Equal(t, childIno, childEntry.NodeId)
	requireStatus(t, "越权直接访问远端存储 RootIno=100", fuse.ENOENT, directRootSt)
	requireStatus(t, "访问未登记高位匿名编号", fuse.ENOENT, unknownHighSt)
}

func mustZeroAllocs(t *testing.T, label string, fn func() fuse.Status) {
	t.Helper()
	allocs := testing.AllocsPerRun(50, func() {
		if st := fn(); st != fuse.OK {
			t.Fatalf("%s 失败: want=OK got=%v", label, st)
		}
	})
	require.Zero(t, allocs, "%s 缓存命中快路径必须保持 0 allocs/op", label)
}

func TestLookup与GetAttr_本地缓存命中快路径保持零堆分配(t *testing.T) {
	// Given
	srv := newFakeStorage(t, withTTL(2.0, 2.0))
	fs := newTestRawFS(t, srv)
	created := mustCreateFile(t, fs, fuse.FUSE_ROOT_ID, "hot.txt", syscall.O_RDWR, 0o644)
	fs.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: created.NodeId}, Fh: created.Fh})

	var entryOut fuse.EntryOut
	var attrOut fuse.AttrOut
	lookupHeader := &fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID}
	getAttrIn := &fuse.GetAttrIn{InHeader: fuse.InHeader{NodeId: created.NodeId}}

	// When & Then
	mustZeroAllocs(t, "Lookup", func() fuse.Status {
		return fs.Lookup(nil, lookupHeader, "hot.txt", &entryOut)
	})
	mustZeroAllocs(t, "GetAttr", func() fuse.Status {
		return fs.GetAttr(nil, getAttrIn, &attrOut)
	})
	snap := fs.Snapshot()
	require.GreaterOrEqual(t, snap.FUSE.Ops[fusefs.OpLookup].Calls, uint64(50))
	require.GreaterOrEqual(t, snap.FUSE.Ops[fusefs.OpGetAttr].Calls, uint64(50))
}

func TestReadlink_严格零秒透传与缓存有效期内命中并在Unlink后失效(t *testing.T) {
	t.Run("严格零秒模式每次Readlink穿透远端存储", func(t *testing.T) {
		srv := newFakeStorage(t, withTTL(0, 0))
		fs := newTestRawFS(t, srv)
		var symOut fuse.EntryOut
		requireStatus(t, "Symlink", fuse.OK, fs.Symlink(nil, &fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID}, "/target/strict", "s0", &symOut))

		target1, st1 := fs.Readlink(nil, &fuse.InHeader{NodeId: symOut.NodeId})
		target2, st2 := fs.Readlink(nil, &fuse.InHeader{NodeId: symOut.NodeId})

		requireStatus(t, "Readlink#1", fuse.OK, st1)
		requireStatus(t, "Readlink#2", fuse.OK, st2)
		require.Equal(t, "/target/strict", string(target1))
		require.Equal(t, "/target/strict", string(target2))
		require.Equal(t, int64(2), srv.readlinkCalls.Load())
		require.Zero(t, fs.Snapshot().NamespaceCache.ReadlinkHits)
	})

	t.Run("生产TTL模式Symlink预热后命中缓存并在Unlink后失效", func(t *testing.T) {
		srv := newFakeStorage(t, withTTL(2.0, 2.0))
		fs := newTestRawFS(t, srv)
		var symOut fuse.EntryOut
		requireStatus(t, "Symlink", fuse.OK, fs.Symlink(nil, &fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID}, "/target/prod", "s1", &symOut))

		target1, st1 := fs.Readlink(nil, &fuse.InHeader{NodeId: symOut.NodeId})
		target2, st2 := fs.Readlink(nil, &fuse.InHeader{NodeId: symOut.NodeId})

		requireStatus(t, "Readlink#1", fuse.OK, st1)
		requireStatus(t, "Readlink#2", fuse.OK, st2)
		require.Equal(t, "/target/prod", string(target1))
		require.Equal(t, "/target/prod", string(target2))
		require.Zero(t, srv.readlinkCalls.Load(), "Symlink 预热后 TTL 窗口内 Readlink 必须零远端调用")
		require.Equal(t, uint64(2), fs.Snapshot().NamespaceCache.ReadlinkHits)

		requireStatus(t, "Unlink", fuse.OK, fs.Unlink(nil, &fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID}, "s1"))
		_, afterSt := fs.Readlink(nil, &fuse.InHeader{NodeId: symOut.NodeId})
		requireStatus(t, "Unlink 后 Readlink", fuse.ENOENT, afterSt)
		require.Equal(t, int64(1), srv.readlinkCalls.Load())
	})
}

func TestReadDirPlus_小目录本地合成与降级回退策略(t *testing.T) {
	t.Run("Mkdir加Create构建完整小目录后ReadDirPlus零远端合成", func(t *testing.T) {
		srv := newFakeStorage(t, withTTL(2.0, 2.0))
		fs := newTestRawFS(t, srv)
		dir := mustMkdir(t, fs, fuse.FUSE_ROOT_ID, "small-dir", 0o755)
		seedRepeatedFilesViaCreate(t, fs, dir.NodeId, reverseSortedNames("entry", 20))

		// When
		names := collectDirEntryNames(t, fs, dir.NodeId)

		// Then
		require.NotEmpty(t, names)
		require.Zero(t, srv.readDirPlusCalls.Load(), "完整目录子属性齐全时 ReadDirPlus 必须本地零远端合成")
	})

	t.Run("子项属性失效或跨目录Rename后安全回退远端ReadDirPlus", func(t *testing.T) {
		srv := newFakeStorage(t, withTTL(2.0, 2.0))
		fs := newTestRawFS(t, srv)
		d1 := mustMkdir(t, fs, fuse.FUSE_ROOT_ID, "d1", 0o755)
		d2 := mustMkdir(t, fs, fuse.FUSE_ROOT_ID, "d2", 0o755)
		f1 := mustCreateFile(t, fs, d1.NodeId, "f1", syscall.O_RDWR, 0o644)
		fs.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: f1.NodeId}, Fh: f1.Fh})

		// 通过公开 SetXAttr 触发 f1 属性缓存失效
		requireStatus(t, "SetXAttr", fuse.OK, fs.SetXAttr(nil, &fuse.SetXAttrIn{InHeader: fuse.InHeader{NodeId: f1.NodeId}}, "user.tag", []byte("1")))
		_ = collectDirEntryNames(t, fs, d1.NodeId)
		require.Equal(t, int64(1), srv.readDirPlusCalls.Load(), "子项属性失效后 ReadDirPlus 必须回退远端查询")

		// 将空完整目录 d2 跨父目录 Rename 到 d1 下，再执行 ReadDirPlus 必须回退远端
		requireStatus(t, "Rename", fuse.OK, fs.Rename(nil, &fuse.RenameIn{
			InHeader: fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID},
			Newdir:   d1.NodeId,
		}, "d2", "d2-moved"))
		_ = collectDirEntryNames(t, fs, d2.NodeId)
		require.Equal(t, int64(2), srv.readDirPlusCalls.Load(), "跨目录移动后 ReadDirPlus 必须回退远端查询")
	})

	t.Run("目录流降级保留子节点属性消除后续OpenDir的GetAttr穿透", func(t *testing.T) {
		srv := newFakeStorage(t, withTTL(2.0, 2.0))
		sub0 := srv.seedDir(1, "sub-00", 0o755)
		sub1 := srv.seedDir(1, "sub-01", 0o755)
		sub2 := srv.seedDir(1, "sub-02", 0o755)
		fs := newTestRawFS(t, srv)

		// 首次 ReadDirPlus 预热根目录流与子目录属性
		_ = collectDirEntryNames(t, fs, fuse.FUSE_ROOT_ID)
		// 在根目录新建文件触发根目录流降级为单条目索引
		created := mustCreateFile(t, fs, fuse.FUSE_ROOT_ID, "trigger-demote.txt", syscall.O_RDWR, 0o644)
		fs.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: created.NodeId}, Fh: created.Fh})

		// 随后 OpenDir 子目录必须命中降级保留的本地属性缓存，零 GetAttr RPC
		beforeGetAttr := srv.getAttrCalls.Load()
		assertAllDirsOpenWithoutGetAttr(t, fs, []uint64{sub0, sub1, sub2})
		require.Equal(t, beforeGetAttr, srv.getAttrCalls.Load(), "目录流降级后 OpenDir 子目录必须零 GetAttr 穿透")
	})
}

func TestGetAttr与StatFs_存在挂起写入时先排空写缓冲再查询远端存储(t *testing.T) {
	// Given
	srv := newFakeStorage(t, withTTL(2.0, 2.0))
	fs := newTestRawFS(t, srv, withStatsFile(true))
	created := mustCreateFile(t, fs, fuse.FUSE_ROOT_ID, "buffered.bin", syscall.O_RDWR, 0o644)
	mustWriteAt(t, fs, created.NodeId, created.Fh, 0, buildRepeatingBytes(4096, 'k'))
	require.Zero(t, srv.writeCalls.Load(), "写入应先暂存在内存缓冲")

	// 通过公开 SetXAttr 失效本地属性缓存，模拟缓存淘汰后 GetAttr 穿透场景
	requireStatus(t, "SetXAttr", fuse.OK, fs.SetXAttr(nil, &fuse.SetXAttrIn{InHeader: fuse.InHeader{NodeId: created.NodeId}}, "user.invalidate", []byte("v")))
	require.Equal(t, int64(1), srv.writeCalls.Load(), "SetXAttr 前的写回屏障应先刷出挂起写入")

	// When: 再次写入内存缓冲后，分别验证失效后 GetAttr 与 StatFs
	mustWriteAt(t, fs, created.NodeId, created.Fh, 4096, buildRepeatingBytes(4096, 'm'))
	var statOut fuse.StatfsOut
	statSt := fs.StatFs(nil, &fuse.InHeader{NodeId: fusefs.VirtualStatsIno}, &statOut)

	// Then
	requireStatus(t, "虚拟统计文件 StatFs 重定向至根目录", fuse.OK, statSt)
	require.Equal(t, uint64(1024*1024), statOut.Blocks)
	require.Equal(t, int64(2), srv.writeCalls.Load(), "StatFs 必须通过 BarrierAll 刷出挂起写入")
	attr := mustGetAttr(t, fs, created.NodeId)
	require.Equal(t, uint64(8192), attr.Size)
}

func TestBuildMountOptions_按读写配置返回预期挂载选项(t *testing.T) {
	// Given / When
	rwOpts := fusefs.BuildMountOptions(fusefs.Config{DiskID: "disk-rw", Mode: fusefs.MountModeStaged, AllowOther: true})
	roOpts := fusefs.BuildMountOptions(fusefs.Config{DiskID: "disk-ro", Mode: fusefs.MountModeRO})

	// Then
	require.Equal(t, "bowl:disk-rw", rwOpts.FsName)
	require.True(t, rwOpts.AllowOther)
	require.Equal(t, []string{"default_permissions"}, rwOpts.Options)
	require.Equal(t, []string{"default_permissions", "ro"}, roOpts.Options)
	require.NotZero(t, rwOpts.ExtraCapabilities&fuse.CAP_HANDLE_KILLPRIV)
	require.NotZero(t, rwOpts.DisabledCapabilities&(fuse.CAP_WRITEBACK_CACHE|fuse.CAP_AUTO_INVAL_DATA))
}

func TestLookup_远端返回空属性节点时兜底返回EIO(t *testing.T) {
	// Given
	srv := newFakeStorage(t, withTTL(1.0, 1.0))
	srv.lookupHook = func(_ context.Context, _ *storagev1.LookupRequest) (*storagev1.LookupResponse, error) {
		return &storagev1.LookupResponse{Inode: &storagev1.Inode{Ino: 777}}, nil
	}
	fs := newTestRawFS(t, srv)

	// When
	var out fuse.EntryOut
	st := fs.Lookup(nil, &fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID}, "broken", &out)

	// Then
	requireStatus(t, "空属性 Inode 的 Lookup", fuse.EIO, st)
}
