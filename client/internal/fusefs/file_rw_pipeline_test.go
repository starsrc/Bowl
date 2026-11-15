package fusefs_test

import (
	"context"
	"fmt"
	"sync/atomic"
	"syscall"
	"testing"

	"connectrpc.com/connect"
	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/stretchr/testify/require"

	"github.com/starsrc/bowl/client/internal/fusefs"
	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
)

const (
	linuxTmpfileFlag  uint32 = 0o20000000
	linuxDirectFlag   uint32 = 0o40000
	linuxCloexecFlag  uint32 = 0o2000000
	fallocFlKeepSize  uint32 = 0x01
	fallocFlPunchHole uint32 = 0x02
	fallocFlZeroRange uint32 = 0x10
	smallWriteSize           = 1024
	coalesceWrites           = 96
	chunk4MiB                = fusefs.MaxRPCChunkSize
)

func performSequentialWrites(t *testing.T, fs *fusefs.RawFS, ino, fh uint64, count, chunkSize int, seed byte) []byte {
	t.Helper()
	expected := make([]byte, 0, count*chunkSize)
	for i := range count {
		block := buildRepeatingBytes(chunkSize, seed+byte(i))
		mustWriteAt(t, fs, ino, fh, uint64(i*chunkSize), block)
		expected = append(expected, block...)
	}
	return expected
}

func performSequentialReads(t *testing.T, fs *fusefs.RawFS, ino, fh uint64, totalSize, stepSize int) []byte {
	t.Helper()
	out := make([]byte, 0, totalSize)
	for off := 0; off < totalSize; off += stepSize {
		want := min(stepSize, totalSize-off)
		out = append(out, mustReadAt(t, fs, ino, fh, uint64(off), want)...)
	}
	return out
}

func performRandomReads(t *testing.T, fs *fusefs.RawFS, ino, fh uint64, offsets []uint64, size int, truth []byte) {
	t.Helper()
	for _, off := range offsets {
		got := mustReadAt(t, fs, ino, fh, off, size)
		requireBytesMatch(t, fmt.Sprintf("随机读 offset=%d", off), truth[off:off+uint64(size)], got)
	}
}

func makeSecondChunkEnospcHook(failFlag *atomic.Bool, minOffset uint64) func(context.Context, *storagev1.WriteRequest) (uint32, error) {
	return func(_ context.Context, req *storagev1.WriteRequest) (uint32, error) {
		if failFlag.Load() && req.GetOffset() >= minOffset {
			return 0, errnoError(connect.CodeResourceExhausted, "ENOSPC")
		}
		return 0, nil
	}
}

func TestToProtoOpenFlags_剥离宿主机高位防碰撞远端存储临时文件标志(t *testing.T) {
	// Given
	rawFlags := uint32(syscall.O_RDWR|syscall.O_CREAT|syscall.O_EXCL|syscall.O_TRUNC|syscall.O_APPEND) |
		linuxTmpfileFlag | linuxDirectFlag | linuxCloexecFlag

	// When
	protoFlags := fusefs.ToProtoOpenFlags(rawFlags)

	// Then
	require.Equal(t, uint32(0o2|0o100|0o200|0o1000|0o2000), protoFlags)
	require.Zero(t, protoFlags&linuxTmpfileFlag, "转换结果不得残留 Linux O_TMPFILE 禁区位")
}

func TestCreate与Open_校验标志位截断行为与缓存策略(t *testing.T) {
	t.Run("非法访问模式只读挂载与目录误用按契约返回错误码", func(t *testing.T) {
		// Given
		srv := newFakeStorage(t, withTTL(1.0, 1.0))
		dirIno := srv.seedDir(1, "sub", 0o755)
		fileIno := srv.seedFile(1, "exist.txt", 0o644, []byte("old"))
		rwFS := newTestRawFS(t, srv)
		roFS := newTestRawFS(t, srv, withReadOnly(true))

		// When & Then
		var cOut fuse.CreateOut
		requireStatus(t, "Create 非法 O_ACCMODE", fuse.Status(syscall.EINVAL), rwFS.Create(nil, &fuse.CreateIn{
			InHeader: fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID},
			Flags:    uint32(syscall.O_ACCMODE),
			Mode:     0o644,
		}, "bad-acc", &cOut))
		requireStatus(t, "只读 Create", fuse.Status(syscall.EROFS), roFS.Create(nil, &fuse.CreateIn{
			InHeader: fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID},
			Flags:    uint32(syscall.O_RDWR),
			Mode:     0o644,
		}, "ro-file", &cOut))

		var oOut fuse.OpenOut
		requireStatus(t, "Open 非法 O_ACCMODE", fuse.Status(syscall.EINVAL), rwFS.Open(nil, &fuse.OpenIn{
			InHeader: fuse.InHeader{NodeId: fileIno},
			Flags:    uint32(syscall.O_ACCMODE),
		}, &oOut))
		requireStatus(t, "只读 Open(O_TRUNC)", fuse.Status(syscall.EROFS), roFS.Open(nil, &fuse.OpenIn{
			InHeader: fuse.InHeader{NodeId: fileIno},
			Flags:    uint32(syscall.O_RDONLY | syscall.O_TRUNC),
		}, &oOut))
		requireStatus(t, "只读 Open(O_WRONLY)", fuse.Status(syscall.EROFS), roFS.Open(nil, &fuse.OpenIn{
			InHeader: fuse.InHeader{NodeId: fileIno},
			Flags:    uint32(syscall.O_WRONLY),
		}, &oOut))
		requireStatus(t, "目录 Open(O_WRONLY)", fuse.Status(syscall.EISDIR), rwFS.Open(nil, &fuse.OpenIn{
			InHeader: fuse.InHeader{NodeId: dirIno},
			Flags:    uint32(syscall.O_WRONLY),
		}, &oOut))
	})

	t.Run("DirectIO开关控制OpenFlags且O_TRUNC清空挂起写并刷新节点尺寸", func(t *testing.T) {
		// Given
		srv := newFakeStorage(t, withTTL(2.0, 2.0))
		fsKeep := newTestRawFS(t, srv, withDirectIO(false))
		fsDirect := newTestRawFS(t, srv, withDirectIO(true))

		// When
		cKeep := mustCreateFile(t, fsKeep, fuse.FUSE_ROOT_ID, "keep.dat", syscall.O_RDWR, 0o644)
		mustWriteAt(t, fsKeep, cKeep.NodeId, cKeep.Fh, 0, []byte("before-truncate"))
		truncOut := mustOpenFile(t, fsKeep, cKeep.NodeId, syscall.O_RDWR|syscall.O_TRUNC)
		keepOpenOut := mustOpenFile(t, fsKeep, cKeep.NodeId, syscall.O_RDONLY)
		directOut := mustOpenFile(t, fsDirect, cKeep.NodeId, syscall.O_RDONLY)

		// Then
		require.NotZero(t, cKeep.OpenFlags&fuse.FOPEN_KEEP_CACHE)
		require.Zero(t, cKeep.OpenFlags&fuse.FOPEN_DIRECT_IO)
		require.Zero(t, truncOut.OpenFlags&fuse.FOPEN_KEEP_CACHE, "O_TRUNC 打开时不得设置 FOPEN_KEEP_CACHE")
		require.NotZero(t, keepOpenOut.OpenFlags&fuse.FOPEN_KEEP_CACHE, "常规非截断非 DirectIO 打开必须设置 FOPEN_KEEP_CACHE")
		require.NotZero(t, directOut.OpenFlags&fuse.FOPEN_DIRECT_IO)
		require.Zero(t, directOut.OpenFlags&fuse.FOPEN_KEEP_CACHE)
		require.Zero(t, mustGetAttr(t, fsKeep, cKeep.NodeId).Size, "O_TRUNC 必须重置写缓冲并刷新长度为 0")

		fsKeep.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: cKeep.NodeId}, Fh: cKeep.Fh})
		fsKeep.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: cKeep.NodeId}, Fh: truncOut.Fh})
		fsKeep.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: cKeep.NodeId}, Fh: keepOpenOut.Fh})
		fsDirect.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: cKeep.NodeId}, Fh: directOut.Fh})
	})
}

func TestWrite与WriteBehind_小块聚合直通探测与特权位绕过(t *testing.T) {
	t.Run("Create播撒WAL聚合96次小块写并在Fsync单次落盘且GetAttr不拆分缓冲", func(t *testing.T) {
		// Given
		srv := newFakeStorage(t, withTTL(2.0, 2.0))
		fs := newTestRawFS(t, srv, withDirectIO(false))
		created := mustCreateFile(t, fs, fuse.FUSE_ROOT_ID, "wal.log", syscall.O_RDWR, 0o644)

		// When: 连续 96 次 1 KiB 顺序写 -> 期间不得触发 Write RPC；GetAttr 读取暂存尺寸也不得提前落盘
		expected := performSequentialWrites(t, fs, created.NodeId, created.Fh, coalesceWrites, smallWriteSize, 0x10)
		writesBeforeFsync := srv.writeCalls.Load()
		attrDuringWrite := mustGetAttr(t, fs, created.NodeId)
		writesAfterGetAttr := srv.writeCalls.Load()

		requireStatus(t, "Fsync", fuse.OK, fs.Fsync(nil, &fuse.FsyncIn{InHeader: fuse.InHeader{NodeId: created.NodeId}, Fh: created.Fh}))
		writesAfterFsync := srv.writeCalls.Load()
		fs.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: created.NodeId}, Fh: created.Fh})

		// Then
		require.Zero(t, writesBeforeFsync, "Create 播撒写流未满 4 MiB 前必须保留在内存中")
		require.Equal(t, uint64(coalesceWrites*smallWriteSize), attrDuringWrite.Size, "GetAttr 必须读取到暂存后的最新尺寸")
		require.Zero(t, writesAfterGetAttr, "TTL>0 时 GetAttr 不得拆散 stageBuf 提前触发 Write RPC")
		require.Equal(t, int64(1), writesAfterFsync, "96 次小块写必须在 Fsync 时合并为单次 Write RPC")
		stored, ok := srv.fileBytes(created.NodeId)
		require.True(t, ok)
		requireBytesMatch(t, "Fsync 落盘数据", expected, stored)
	})

	t.Run("Open既有文件首写直通探测且后续连续与回退聚合并在跳跃间隙排空", func(t *testing.T) {
		// Given
		srv := newFakeStorage(t, withTTL(2.0, 2.0))
		ino := srv.seedFile(1, "existing.bin", 0o644, buildRepeatingBytes(8192, 'A'))
		fs := newTestRawFS(t, srv, withDirectIO(false))
		opened := mustOpenFile(t, fs, ino, syscall.O_RDWR)

		// When: 首写直通探测（1 次 RPC），随后连续追加 [100..200) 与向后邻接 [50..100) 合并暂存，
		// 跳转到存在间隙的 [1000..1100) 时触发前序聚合刷新。
		mustWriteAt(t, fs, ino, opened.Fh, 0, []byte("probe-0123"))
		afterProbe := srv.writeCalls.Load()
		mustWriteAt(t, fs, ino, opened.Fh, 100, []byte("second"))
		mustWriteAt(t, fs, ino, opened.Fh, 94, []byte("first-"))
		afterCoalesce := srv.writeCalls.Load()
		mustWriteAt(t, fs, ino, opened.Fh, 1000, []byte("gap"))
		afterGap := srv.writeCalls.Load()
		requireStatus(t, "Flush", fuse.OK, fs.Flush(nil, &fuse.FlushIn{InHeader: fuse.InHeader{NodeId: ino}, Fh: opened.Fh}))
		fs.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: ino}, Fh: opened.Fh})

		// Then
		require.Equal(t, int64(1), afterProbe, "Open 既有文件首写必须直通远端探测权属")
		require.Equal(t, int64(1), afterCoalesce, "连续与向左邻接小块写必须合并暂存")
		require.Equal(t, int64(2), afterGap, "出现偏移间隙时必须先刷出前序暂存缓冲")
		stored, _ := srv.fileBytes(ino)
		requireBytesMatch(t, "邻接聚合结果", []byte("first-second"), stored[94:106])
		requireBytesMatch(t, "间隙写入结果", []byte("gap"), stored[1000:1003])
	})

	t.Run("SetUID文件与零秒AttrTimeout强制每次写入直通远端", func(t *testing.T) {
		// Given
		srvZero := newFakeStorage(t, withTTL(0, 0))
		fsZero := newTestRawFS(t, srvZero)
		cZero := mustCreateFile(t, fsZero, fuse.FUSE_ROOT_ID, "zero-ttl.bin", syscall.O_RDWR, 0o644)

		srvSuid := newFakeStorage(t, withTTL(2.0, 2.0))
		suidIno := srvSuid.seedFile(1, "suid.bin", syscall.S_ISUID|0o755, nil)
		fsSuid := newTestRawFS(t, srvSuid)
		oSuid := mustOpenFile(t, fsSuid, suidIno, syscall.O_RDWR)

		// When
		mustWriteAt(t, fsZero, cZero.NodeId, cZero.Fh, 0, []byte("a"))
		mustWriteAt(t, fsZero, cZero.NodeId, cZero.Fh, 1, []byte("b"))
		mustWriteAt(t, fsSuid, suidIno, oSuid.Fh, 0, []byte("x"))
		mustWriteAt(t, fsSuid, suidIno, oSuid.Fh, 1, []byte("y"))

		// Then
		require.Equal(t, int64(2), srvZero.writeCalls.Load(), "AttrTimeout=0 模式下每次写入必须直通存储")
		require.Equal(t, int64(2), srvSuid.writeCalls.Load(), "含 S_ISUID 特权位文件不得进入写聚合缓存")

		fsZero.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: cZero.NodeId}, Fh: cZero.Fh})
		fsSuid.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: suidIno}, Fh: oSuid.Fh})
	})
}

func TestWrite_追加模式分片长写POSIX短写与Writev多段写入(t *testing.T) {
	t.Run("O_APPEND忽略在途中断信号在既有尾部定序追加", func(t *testing.T) {
		// Given
		srv := newFakeStorage(t, withTTL(1.0, 1.0))
		ino := srv.seedFile(1, "append.log", 0o644, []byte("head-"))
		fs := newTestRawFS(t, srv)
		_ = mustLookup(t, fs, fuse.FUSE_ROOT_ID, "append.log")
		opened := mustOpenFile(t, fs, ino, syscall.O_WRONLY|syscall.O_APPEND)
		cancelled := make(chan struct{})
		srv.writeHook = func(ctx context.Context, _ *storagev1.WriteRequest) (uint32, error) {
			close(cancelled)
			return 0, ctx.Err()
		}

		// When: 传入低偏移 0 且 Write RPC 在途期间触发 FUSE_INTERRUPT（关闭 cancel），
		// 校验 O_APPEND 仍基于最新尾部追加且 Write RPC 不受中途取消影响。
		n, st := fs.Write(cancelled, &fuse.WriteIn{
			InHeader: fuse.InHeader{NodeId: ino},
			Fh:       opened.Fh,
			Offset:   0,
			Size:     4,
		}, []byte("tail"))
		fs.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: ino}, Fh: opened.Fh})

		// Then
		requireStatus(t, "O_APPEND Write", fuse.OK, st)
		require.Equal(t, uint32(4), n)
		stored, _ := srv.fileBytes(ino)
		requireBytesMatch(t, "O_APPEND 尾部追加内容", []byte("head-tail"), stored)
	})

	t.Run("超过4MiB按分片落盘且中途片ENOSPC按POSIX返回短写成功", func(t *testing.T) {
		// Given
		srv := newFakeStorage(t, withTTL(1.0, 1.0))
		var failSecondChunk atomic.Bool
		srv.writeHook = makeSecondChunkEnospcHook(&failSecondChunk, chunk4MiB)
		fs := newTestRawFS(t, srv)
		created := mustCreateFile(t, fs, fuse.FUSE_ROOT_ID, "large.bin", syscall.O_RDWR, 0o644)

		// When: 5 MiB 成功写入后，开启二片故障注入再写 5 MiB
		okBuf := buildSeededBytes(chunk4MiB+512*1024, 0x5A)
		nOK, stOK := fs.Write(nil, &fuse.WriteIn{
			InHeader: fuse.InHeader{NodeId: created.NodeId},
			Fh:       created.Fh,
			Offset:   0,
			Size:     uint32(len(okBuf)),
		}, okBuf)
		requireStatus(t, "超 4 MiB 正常写入", fuse.OK, stOK)
		require.Equal(t, uint32(len(okBuf)), nOK)

		failSecondChunk.Store(true)
		shortBuf := buildRepeatingBytes(chunk4MiB+1024, 'Z')
		nShort, stShort := fs.Write(nil, &fuse.WriteIn{
			InHeader: fuse.InHeader{NodeId: created.NodeId},
			Fh:       created.Fh,
			Offset:   0,
			Size:     uint32(len(shortBuf)),
		}, shortBuf)

		// 首片直接失败时必须返回对应错误码
		nErr, stErr := fs.Write(nil, &fuse.WriteIn{
			InHeader: fuse.InHeader{NodeId: created.NodeId},
			Fh:       created.Fh,
			Offset:   chunk4MiB,
			Size:     4096,
		}, shortBuf[:4096])

		// Then
		requireStatus(t, "二片 ENOSPC 按 POSIX 短写返回 OK", fuse.OK, stShort)
		require.Equal(t, uint32(chunk4MiB), nShort)
		requireStatus(t, "首片 ENOSPC 返回 ENOSPC", fuse.Status(syscall.ENOSPC), stErr)
		require.Zero(t, nErr)
		fs.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: created.NodeId}, Fh: created.Fh})
	})

	t.Run("Writev覆盖空段单段与多段聚集写入且读出一致", func(t *testing.T) {
		// Given
		srv := newFakeStorage(t, withTTL(1.0, 1.0))
		fs := newTestRawFS(t, srv)
		created := mustCreateFile(t, fs, fuse.FUSE_ROOT_ID, "vec.bin", syscall.O_RDWR, 0o644)

		// When
		n0, st0 := fs.Writev(nil, &fuse.WriteIn{InHeader: fuse.InHeader{NodeId: created.NodeId}, Fh: created.Fh, Offset: 0}, nil)
		n1, st1 := fs.Writev(nil, &fuse.WriteIn{InHeader: fuse.InHeader{NodeId: created.NodeId}, Fh: created.Fh, Offset: 0, Size: 3}, [][]byte{[]byte("one")})
		n3, st3 := fs.Writev(nil, &fuse.WriteIn{InHeader: fuse.InHeader{NodeId: created.NodeId}, Fh: created.Fh, Offset: 3, Size: 10}, [][]byte{[]byte("-two-"), []byte("three")})

		// Then
		requireStatus(t, "Writev 零段", fuse.OK, st0)
		require.Zero(t, n0)
		requireStatus(t, "Writev 单段", fuse.OK, st1)
		require.Equal(t, uint32(3), n1)
		requireStatus(t, "Writev 多段", fuse.OK, st3)
		require.Equal(t, uint32(10), n3)
		requireBytesMatch(t, "Writev 组合内容", []byte("one-two-three"), mustReadAt(t, fs, created.NodeId, created.Fh, 0, 13))
		fs.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: created.NodeId}, Fh: created.Fh})
	})
}

func TestRead与ReadAhead_校验读写权限屏障保护顺序预读命中及随机读零放大(t *testing.T) {
	t.Run("只写句柄读返回EBADF且零长读直接成功且ReadAhead前排空WriteBehind", func(t *testing.T) {
		// Given
		srv := newFakeStorage(t, withTTL(1.0, 1.0))
		fs := newTestRawFS(t, srv, withDirectIO(false))
		wrOnly := mustCreateFile(t, fs, fuse.FUSE_ROOT_ID, "barrier.bin", syscall.O_WRONLY, 0o644)
		mustWriteAt(t, fs, wrOnly.NodeId, wrOnly.Fh, 0, []byte("staged-payload"))
		rdOnly := mustOpenFile(t, fs, wrOnly.NodeId, syscall.O_RDONLY)

		// When
		_, ebadfSt := fs.Read(nil, &fuse.ReadIn{InHeader: fuse.InHeader{NodeId: wrOnly.NodeId}, Fh: wrOnly.Fh, Size: 16}, make([]byte, 16))
		zeroRes, zeroSt := fs.Read(nil, &fuse.ReadIn{InHeader: fuse.InHeader{NodeId: wrOnly.NodeId}, Fh: rdOnly.Fh, Size: 0}, nil)
		zeroBytes, _ := zeroRes.Bytes(nil)
		readBack := mustReadAt(t, fs, wrOnly.NodeId, rdOnly.Fh, 0, 14)

		// Then
		requireStatus(t, "O_WRONLY Read", fuse.EBADF, ebadfSt)
		requireStatus(t, "零字节 Read", fuse.OK, zeroSt)
		require.Empty(t, zeroBytes)
		requireBytesMatch(t, "Read 触发写缓冲屏障后应读见未手动Fsync的新写内容", []byte("staged-payload"), readBack)

		fs.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: wrOnly.NodeId}, Fh: wrOnly.Fh})
		fs.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: wrOnly.NodeId}, Fh: rdOnly.Fh})
	})

	t.Run("顺序流触发预读且写后失效并在随机大跨度读下保持零预读放大", func(t *testing.T) {
		// Given: 构造 16 MiB 文件分别验证顺序流预读命中、写入失效及随机 1 MiB 跳跃零放大
		const fileBytes = 16 << 20
		truth := buildSeededBytes(fileBytes, 0x71)
		srv := newFakeStorage(t, withTTL(1.0, 1.0))
		seqIno := srv.seedFile(1, "seq.bin", 0o644, truth)
		randIno := srv.seedFile(1, "rand.bin", 0o644, truth)
		fs := newTestRawFS(t, srv, withDirectIO(false))

		// When: 1. 顺序按 512 KiB 步长读完 16 MiB（含 EOF），总 Read RPC 远少于 32 次，随后覆写末尾触发预读失效
		seqFH := mustOpenFile(t, fs, seqIno, syscall.O_RDWR)
		gotSeq := performSequentialReads(t, fs, seqIno, seqFH.Fh, fileBytes, 512<<10)
		seqRPCCalls := srv.readCalls.Load()
		requireBytesMatch(t, "顺序预读完整内容", truth, gotSeq)
		require.Less(t, seqRPCCalls, int64(12), "16 MiB 按 512 KiB 顺序读经 4 MiB 窗口与预读合并后 RPC 次数必须显著收敛")

		// 覆写首部使 ReadAhead 缓存失效，再次读取必须观察到新内容
		mustWriteAt(t, fs, seqIno, seqFH.Fh, 0, []byte("PATCHED!"))
		requireBytesMatch(t, "Write 使 ReadAhead 失效后应读见最新数据", []byte("PATCHED!"), mustReadAt(t, fs, seqIno, seqFH.Fh, 0, 8))
		fs.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: seqIno}, Fh: seqFH.Fh})

		// 2. 随机跳跃读取 6 个离散 1 MiB 切片，不得产生投机预读放大
		randFH := mustOpenFile(t, fs, randIno, syscall.O_RDONLY)
		beforeRandCalls := srv.readCalls.Load()
		jumps := []uint64{13 << 20, 2 << 20, 10 << 20, 5 << 20, 15 << 20, 0}
		performRandomReads(t, fs, randIno, randFH.Fh, jumps, 1<<20, truth)
		randCalls := srv.readCalls.Load() - beforeRandCalls
		fs.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: randIno}, Fh: randFH.Fh})

		// Then
		require.Equal(t, int64(len(jumps)), randCalls, "非顺序随机跳跃读取不得触发投机预读放大")
	})
}

func TestTmpfile与OnUnmount_匿名文件具名化孤儿持久化与乱序双清零回收(t *testing.T) {
	t.Run("Tmpfile具名化可读取且禁止O_RDONLY创建与二次Link", func(t *testing.T) {
		// Given
		srv := newFakeStorage(t, withTTL(1.0, 1.0))
		fs := newTestRawFS(t, srv)

		var badOut fuse.CreateOut
		requireStatus(t, "O_RDONLY Tmpfile", fuse.EINVAL, fs.Tmpfile(nil, &fuse.CreateIn{
			InHeader: fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID},
			Flags:    uint32(syscall.O_RDONLY),
			Mode:     0o644,
		}, &badOut))

		var tmpOut fuse.CreateOut
		requireStatus(t, "Tmpfile O_RDWR", fuse.OK, fs.Tmpfile(nil, &fuse.CreateIn{
			InHeader: fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID},
			Flags:    uint32(syscall.O_RDWR),
			Mode:     0o640,
		}, &tmpOut))

		// When: 写入数据并执行 Fallocate 打洞、零范围清零与越界扩展，随后具名化为正常文件并拒绝二次 Link
		mustWriteAt(t, fs, tmpOut.NodeId, tmpOut.Fh, 0, []byte("tmp-header-tail"))
		requireStatus(t, "Tmpfile Fallocate 打洞", fuse.OK, fs.Fallocate(nil, &fuse.FallocateIn{
			InHeader: fuse.InHeader{NodeId: tmpOut.NodeId},
			Fh:       tmpOut.Fh,
			Offset:   4,
			Length:   7,
			Mode:     fallocFlPunchHole | fallocFlKeepSize,
		}))
		requireStatus(t, "Tmpfile 单独 PUNCH_HOLE 返回 EINVAL", fuse.EINVAL, fs.Fallocate(nil, &fuse.FallocateIn{
			InHeader: fuse.InHeader{NodeId: tmpOut.NodeId},
			Fh:       tmpOut.Fh,
			Offset:   0,
			Length:   4,
			Mode:     fallocFlPunchHole,
		}))
		requireStatus(t, "Tmpfile ZERO_RANGE KEEP_SIZE", fuse.OK, fs.Fallocate(nil, &fuse.FallocateIn{
			InHeader: fuse.InHeader{NodeId: tmpOut.NodeId},
			Fh:       tmpOut.Fh,
			Offset:   13,
			Length:   6,
			Mode:     fallocFlZeroRange | fallocFlKeepSize,
		}))
		require.Equal(t, uint64(15), mustGetAttr(t, fs, tmpOut.NodeId).Size)
		requireStatus(t, "Tmpfile ZERO_RANGE 越界扩展", fuse.OK, fs.Fallocate(nil, &fuse.FallocateIn{
			InHeader: fuse.InHeader{NodeId: tmpOut.NodeId},
			Fh:       tmpOut.Fh,
			Offset:   12,
			Length:   8,
			Mode:     fallocFlZeroRange,
		}))
		require.Equal(t, uint64(20), mustGetAttr(t, fs, tmpOut.NodeId).Size)
		requireStatus(t, "Tmpfile 非法 Fallocate 偏移", fuse.EINVAL, fs.Fallocate(nil, &fuse.FallocateIn{
			InHeader: fuse.InHeader{NodeId: tmpOut.NodeId},
			Fh:       tmpOut.Fh,
			Offset:   uint64(1) << 63,
			Length:   4096,
			Mode:     0,
		}))
		requireStatus(t, "Tmpfile 非法 SetAttr 大小返回 EFBIG", fuse.Status(syscall.EFBIG), fs.SetAttr(nil, &fuse.SetAttrIn{
			SetAttrInCommon: fuse.SetAttrInCommon{
				InHeader: fuse.InHeader{NodeId: tmpOut.NodeId},
				Valid:    fuse.FATTR_SIZE,
				Size:     uint64(1) << 63,
			},
		}, &fuse.AttrOut{}))

		var promoted fuse.EntryOut
		requireStatus(t, "Tmpfile Link 具名化", fuse.OK, fs.Link(nil, &fuse.LinkIn{
			InHeader:  fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID},
			Oldnodeid: tmpOut.NodeId,
		}, "materialized.txt", &promoted))
		var doubleLink fuse.EntryOut
		requireStatus(t, "禁止对已具名 Tmpfile 二次 Link", fuse.ENOENT, fs.Link(nil, &fuse.LinkIn{
			InHeader:  fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID},
			Oldnodeid: tmpOut.NodeId,
		}, "second-link.txt", &doubleLink))

		fs.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: tmpOut.NodeId}, Fh: tmpOut.Fh})
		fs.Forget(tmpOut.NodeId, 2)

		// Then: 通过具名文件读取确认打洞、零范围清零与越界扩展完整持久化
		looked := mustLookup(t, fs, fuse.FUSE_ROOT_ID, "materialized.txt")
		require.Equal(t, uint64(20), looked.Size)
		opened := mustOpenFile(t, fs, looked.NodeId, syscall.O_RDONLY)
		got := mustReadAt(t, fs, looked.NodeId, opened.Fh, 0, 20)
		requireBytesMatch(t, "具名化后的打洞与零范围文件内容", []byte("tmp-\x00\x00\x00\x00\x00\x00\x00t\x00\x00\x00\x00\x00\x00\x00\x00"), got)
		fs.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: looked.NodeId}, Fh: opened.Fh})
	})

	t.Run("匿名文件Flush上云孤儿并在Forget与Release乱序双清零时触发回收", func(t *testing.T) {
		// Given
		srv := newFakeStorage(t, withTTL(1.0, 1.0))
		fs := newTestRawFS(t, srv)
		var tmpA, tmpB fuse.CreateOut
		requireStatus(t, "Tmpfile A", fuse.OK, fs.Tmpfile(nil, &fuse.CreateIn{
			InHeader: fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID},
			Flags:    uint32(syscall.O_RDWR),
			Mode:     0o600,
		}, &tmpA))
		requireStatus(t, "Tmpfile B", fuse.OK, fs.Tmpfile(nil, &fuse.CreateIn{
			InHeader: fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID},
			Flags:    uint32(syscall.O_RDWR),
			Mode:     0o600,
		}, &tmpB))

		// When: tmpA Flush 上云成为孤儿后，先 Forget 再 Release；tmpB 保持纯内存匿名后先 Release 再 Forget
		mustWriteAt(t, fs, tmpA.NodeId, tmpA.Fh, 0, []byte("orphan-flush"))
		requireStatus(t, "Tmpfile A Flush 上云", fuse.OK, fs.Flush(nil, &fuse.FlushIn{
			InHeader: fuse.InHeader{NodeId: tmpA.NodeId},
			Fh:       tmpA.Fh,
		}))
		fs.Forget(tmpA.NodeId, 1)
		require.Zero(t, srv.reclaimOrphanCalls.Load(), "仅有 Forget 但句柄未 Release 时不得提前回收孤儿")
		fs.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: tmpA.NodeId}, Fh: tmpA.Fh})
		require.Equal(t, int64(1), srv.reclaimOrphanCalls.Load(), "Forget 后最终 Release 必须回收已上云孤儿")

		mustWriteAt(t, fs, tmpB.NodeId, tmpB.Fh, 0, []byte("local-only"))
		fs.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: tmpB.NodeId}, Fh: tmpB.Fh})
		require.Equal(t, int64(1), srv.reclaimOrphanCalls.Load())
		fs.Forget(tmpB.NodeId, 1)

		// Then: 双清零后原本地节点编号不可再访问
		var attrAfter fuse.AttrOut
		requireStatus(t, "tmpA 双清零后 GetAttr", fuse.ENOENT, fs.GetAttr(nil, &fuse.GetAttrIn{InHeader: fuse.InHeader{NodeId: tmpA.NodeId}}, &attrAfter))
		requireStatus(t, "tmpB 双清零后 GetAttr", fuse.ENOENT, fs.GetAttr(nil, &fuse.GetAttrIn{InHeader: fuse.InHeader{NodeId: tmpB.NodeId}}, &attrAfter))
	})

	t.Run("OnUnmount对所有仍存活的脏节点各执行一次Fsync同步", func(t *testing.T) {
		// Given
		srv := newFakeStorage(t, withTTL(1.0, 1.0))
		fs := newTestRawFS(t, srv, withDirectIO(false))
		f1 := mustCreateFile(t, fs, fuse.FUSE_ROOT_ID, "dirty1.txt", syscall.O_RDWR, 0o644)
		f2 := mustCreateFile(t, fs, fuse.FUSE_ROOT_ID, "dirty2.txt", syscall.O_RDWR, 0o644)
		mustWriteAt(t, fs, f1.NodeId, f1.Fh, 0, []byte("d1"))
		mustWriteAt(t, fs, f2.NodeId, f2.Fh, 0, []byte("d2"))
		fs.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: f1.NodeId}, Fh: f1.Fh})
		fs.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: f2.NodeId}, Fh: f2.Fh})
		fsyncBefore := srv.fsyncCalls.Load()

		// When: 首次 OnUnmount 应同步 2 个脏节点，第二次重复 OnUnmount 幂等不再重复 Fsync
		fs.OnUnmount()
		fsyncAfterFirst := srv.fsyncCalls.Load()
		fs.OnUnmount()
		fsyncAfterSecond := srv.fsyncCalls.Load()

		// Then
		require.Equal(t, fsyncBefore+2, fsyncAfterFirst, "OnUnmount 必须为每个存活脏节点精确触发 1 次 Fsync")
		require.Equal(t, fsyncAfterFirst, fsyncAfterSecond, "重复调用 OnUnmount 不得重复 Fsync 已同步节点")
	})
}
