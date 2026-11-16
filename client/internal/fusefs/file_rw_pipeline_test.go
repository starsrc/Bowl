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
	t.Run("Create传入非法O_ACCMODE返回EINVAL", func(t *testing.T) {
		// Given
		srv := newFakeStorage(t, withTTL(1.0, 1.0))
		rwFS := newTestRawFS(t, srv)
		var out fuse.CreateOut

		// When
		st := rwFS.Create(nil, &fuse.CreateIn{
			InHeader: fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID},
			Flags:    uint32(syscall.O_ACCMODE),
			Mode:     0o644,
		}, "bad-acc", &out)

		// Then
		requireStatus(t, "Create 非法 O_ACCMODE", fuse.Status(syscall.EINVAL), st)
	})

	t.Run("只读挂载调用Create拦截并返回EROFS", func(t *testing.T) {
		// Given
		srv := newFakeStorage(t, withTTL(1.0, 1.0))
		roFS := newTestRawFS(t, srv, withReadOnly(true))
		var out fuse.CreateOut

		// When
		st := roFS.Create(nil, &fuse.CreateIn{
			InHeader: fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID},
			Flags:    uint32(syscall.O_RDWR),
			Mode:     0o644,
		}, "ro-file", &out)

		// Then
		requireStatus(t, "只读 Create", fuse.Status(syscall.EROFS), st)
	})

	t.Run("Open传入非法O_ACCMODE返回EINVAL", func(t *testing.T) {
		// Given
		srv := newFakeStorage(t, withTTL(1.0, 1.0))
		fileIno := srv.seedFile(1, "exist.txt", 0o644, []byte("old"))
		rwFS := newTestRawFS(t, srv)
		var out fuse.OpenOut

		// When
		st := rwFS.Open(nil, &fuse.OpenIn{
			InHeader: fuse.InHeader{NodeId: fileIno},
			Flags:    uint32(syscall.O_ACCMODE),
		}, &out)

		// Then
		requireStatus(t, "Open 非法 O_ACCMODE", fuse.Status(syscall.EINVAL), st)
	})

	t.Run("只读挂载Open携带O_TRUNC返回EROFS", func(t *testing.T) {
		// Given
		srv := newFakeStorage(t, withTTL(1.0, 1.0))
		fileIno := srv.seedFile(1, "exist.txt", 0o644, []byte("old"))
		roFS := newTestRawFS(t, srv, withReadOnly(true))
		var out fuse.OpenOut

		// When
		st := roFS.Open(nil, &fuse.OpenIn{
			InHeader: fuse.InHeader{NodeId: fileIno},
			Flags:    uint32(syscall.O_RDONLY | syscall.O_TRUNC),
		}, &out)

		// Then
		requireStatus(t, "只读 Open(O_TRUNC)", fuse.Status(syscall.EROFS), st)
	})

	t.Run("只读挂载Open携带O_WRONLY返回EROFS", func(t *testing.T) {
		// Given
		srv := newFakeStorage(t, withTTL(1.0, 1.0))
		fileIno := srv.seedFile(1, "exist.txt", 0o644, []byte("old"))
		roFS := newTestRawFS(t, srv, withReadOnly(true))
		var out fuse.OpenOut

		// When
		st := roFS.Open(nil, &fuse.OpenIn{
			InHeader: fuse.InHeader{NodeId: fileIno},
			Flags:    uint32(syscall.O_WRONLY),
		}, &out)

		// Then
		requireStatus(t, "只读 Open(O_WRONLY)", fuse.Status(syscall.EROFS), st)
	})

	t.Run("目录节点以O_WRONLY打开返回EISDIR", func(t *testing.T) {
		// Given
		srv := newFakeStorage(t, withTTL(1.0, 1.0))
		dirIno := srv.seedDir(1, "sub", 0o755)
		rwFS := newTestRawFS(t, srv)
		var out fuse.OpenOut

		// When
		st := rwFS.Open(nil, &fuse.OpenIn{
			InHeader: fuse.InHeader{NodeId: dirIno},
			Flags:    uint32(syscall.O_WRONLY),
		}, &out)

		// Then
		requireStatus(t, "目录 Open(O_WRONLY)", fuse.Status(syscall.EISDIR), st)
	})

	t.Run("DirectIO关闭时普通Create与Open设置FOPEN_KEEP_CACHE", func(t *testing.T) {
		// Given
		srv := newFakeStorage(t, withTTL(2.0, 2.0))
		fsKeep := newTestRawFS(t, srv, withDirectIO(false))

		// When
		created := mustCreateFile(t, fsKeep, fuse.FUSE_ROOT_ID, "keep.dat", syscall.O_RDWR, 0o644)
		opened := mustOpenFile(t, fsKeep, created.NodeId, syscall.O_RDONLY)

		// Then
		require.NotZero(t, created.OpenFlags&fuse.FOPEN_KEEP_CACHE, "Create 应设置 FOPEN_KEEP_CACHE")
		require.Zero(t, created.OpenFlags&fuse.FOPEN_DIRECT_IO, "DirectIO=false 时 Create 不应设 FOPEN_DIRECT_IO")
		require.NotZero(t, opened.OpenFlags&fuse.FOPEN_KEEP_CACHE, "非截断常规 Open 应设置 FOPEN_KEEP_CACHE")

		fsKeep.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: created.NodeId}, Fh: created.Fh})
		fsKeep.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: created.NodeId}, Fh: opened.Fh})
	})

	t.Run("DirectIO开启时Open强制设置FOPEN_DIRECT_IO并清除KEEP_CACHE", func(t *testing.T) {
		// Given
		srv := newFakeStorage(t, withTTL(2.0, 2.0))
		ino := srv.seedFile(1, "direct.dat", 0o644, []byte("payload"))
		fsDirect := newTestRawFS(t, srv, withDirectIO(true))

		// When
		opened := mustOpenFile(t, fsDirect, ino, syscall.O_RDONLY)

		// Then
		require.NotZero(t, opened.OpenFlags&fuse.FOPEN_DIRECT_IO, "DirectIO=true 必须设置 FOPEN_DIRECT_IO")
		require.Zero(t, opened.OpenFlags&fuse.FOPEN_KEEP_CACHE, "DirectIO=true 不得设置 FOPEN_KEEP_CACHE")
		fsDirect.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: ino}, Fh: opened.Fh})
	})

	t.Run("Open携带O_TRUNC清除KEEP_CACHE并截零挂起写入与文件尺寸", func(t *testing.T) {
		// Given
		srv := newFakeStorage(t, withTTL(2.0, 2.0))
		fs := newTestRawFS(t, srv, withDirectIO(false))
		created := mustCreateFile(t, fs, fuse.FUSE_ROOT_ID, "trunc.dat", syscall.O_RDWR, 0o644)
		mustWriteAt(t, fs, created.NodeId, created.Fh, 0, []byte("before-truncate"))

		// When
		truncOut := mustOpenFile(t, fs, created.NodeId, syscall.O_RDWR|syscall.O_TRUNC)

		// Then
		require.Zero(t, truncOut.OpenFlags&fuse.FOPEN_KEEP_CACHE, "O_TRUNC 打开时不得设置 FOPEN_KEEP_CACHE")
		require.Zero(t, mustGetAttr(t, fs, created.NodeId).Size, "O_TRUNC 后文件尺寸: want=0")
		fs.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: created.NodeId}, Fh: created.Fh})
		fs.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: created.NodeId}, Fh: truncOut.Fh})
	})
}

func TestWrite与Stage_小块聚合直通探测与特权位绕过(t *testing.T) {
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
		require.Zero(t, writesBeforeFsync, "Create 播撒写流未满 4 MiB 前必须保留在本地暂存分块")
		require.Equal(t, uint64(98304), attrDuringWrite.Size, "GetAttr 暂存尺寸: want=98304, got=%d", attrDuringWrite.Size)
		require.Zero(t, writesAfterGetAttr, "GetAttr 不得拆散本地暂存分块提前触发 Write RPC")
		require.Equal(t, int64(1), writesAfterFsync, "96 次小块写必须在 Fsync 时合并为单次 Write RPC")
		stored, ok := srv.fileBytes(created.NodeId)
		require.True(t, ok)
		requireBytesMatch(t, "Fsync 落盘数据", expected, stored)
	})

	t.Run("Open既有文件首写直通远端完成能力探测", func(t *testing.T) {
		// Given
		srv := newFakeStorage(t, withTTL(2.0, 2.0))
		ino := srv.seedFile(1, "existing-probe.bin", 0o644, buildRepeatingBytes(8192, 'A'))
		fs := newTestRawFS(t, srv, withDirectIO(false))
		opened := mustOpenFile(t, fs, ino, syscall.O_RDWR)

		// When
		mustWriteAt(t, fs, ino, opened.Fh, 0, []byte("probe-0123"))

		// Then
		require.Equal(t, int64(1), srv.writeCalls.Load(), "Open 既有文件首写 Write RPC 次数: want=1, got=%d", srv.writeCalls.Load())
		fs.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: ino}, Fh: opened.Fh})
	})

	t.Run("Open既有多段连续与回退及跳跃写经Flush非阻塞并在Fsync落盘一致", func(t *testing.T) {
		// Given
		srv := newFakeStorage(t, withTTL(2.0, 2.0))
		ino := srv.seedFile(1, "existing-stage.bin", 0o644, buildRepeatingBytes(8192, 'A'))
		fs := newTestRawFS(t, srv, withDirectIO(false))
		opened := mustOpenFile(t, fs, ino, syscall.O_RDWR)
		mustWriteAt(t, fs, ino, opened.Fh, 0, []byte("probe-0123"))

		// When
		mustWriteAt(t, fs, ino, opened.Fh, 100, []byte("second"))
		mustWriteAt(t, fs, ino, opened.Fh, 94, []byte("first-"))
		writesBeforeJump := srv.writeCalls.Load()
		mustWriteAt(t, fs, ino, opened.Fh, 1000, []byte("gap"))
		flushSt := fs.Flush(nil, &fuse.FlushIn{InHeader: fuse.InHeader{NodeId: ino}, Fh: opened.Fh})
		fsyncSt := fs.Fsync(nil, &fuse.FsyncIn{InHeader: fuse.InHeader{NodeId: ino}, Fh: opened.Fh})
		afterFsync := srv.writeCalls.Load()
		fs.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: ino}, Fh: opened.Fh})

		// Then
		require.Equal(t, int64(1), writesBeforeJump, "连续与向左回退写期间 Write RPC 次数: want=1, got=%d", writesBeforeJump)
		requireStatus(t, "Flush", fuse.OK, flushSt)
		requireStatus(t, "Fsync", fuse.OK, fsyncSt)
		require.Equal(t, int64(3), afterFsync, "Fsync 后总 Write RPC 次数: want=3, got=%d", afterFsync)
		stored, _ := srv.fileBytes(ino)
		requireBytesMatch(t, "邻接聚合结果", []byte("first-second"), stored[94:106])
		requireBytesMatch(t, "间隙写入结果", []byte("gap"), stored[1000:1003])
	})

	t.Run("零秒AttrTimeout下普通文件写入仍进入Stage暂存并支持本地直读", func(t *testing.T) {
		// Given
		srvZero := newFakeStorage(t, withTTL(0, 0))
		fsZero := newTestRawFS(t, srvZero)
		cZero := mustCreateFile(t, fsZero, fuse.FUSE_ROOT_ID, "zero-ttl.bin", syscall.O_RDWR, 0o644)

		// When
		mustWriteAt(t, fsZero, cZero.NodeId, cZero.Fh, 0, []byte("a"))
		mustWriteAt(t, fsZero, cZero.NodeId, cZero.Fh, 1, []byte("b"))
		zeroWritesBeforeFsync := srvZero.writeCalls.Load()
		zeroAttr := mustGetAttr(t, fsZero, cZero.NodeId)
		zeroRead := mustReadAt(t, fsZero, cZero.NodeId, cZero.Fh, 0, 2)
		requireStatus(t, "Fsync zero-ttl", fuse.OK, fsZero.Fsync(nil, &fuse.FsyncIn{InHeader: fuse.InHeader{NodeId: cZero.NodeId}, Fh: cZero.Fh}))

		// Then
		require.Zero(t, zeroWritesBeforeFsync, "AttrTimeout=0 模式下普通文件写入仍必须进入本地 Stage 暂存")
		require.Equal(t, uint64(2), zeroAttr.Size, "AttrTimeout=0 下 GetAttr 暂存尺寸: want=2, got=%d", zeroAttr.Size)
		requireBytesMatch(t, "AttrTimeout=0 下本地读己所写", []byte("ab"), zeroRead)
		require.Zero(t, srvZero.readCalls.Load(), "完全由本地 Stage 覆盖的读取不得触发远端 Read RPC")
		require.Equal(t, int64(1), srvZero.writeCalls.Load(), "Fsync 后 Write RPC 次数: want=1, got=%d", srvZero.writeCalls.Load())
		fsZero.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: cZero.NodeId}, Fh: cZero.Fh})
	})

	t.Run("含SetUID特权位文件绕过Stage直接同步落盘", func(t *testing.T) {
		// Given
		srvSuid := newFakeStorage(t, withTTL(2.0, 2.0))
		suidIno := srvSuid.seedFile(1, "suid.bin", syscall.S_ISUID|0o755, nil)
		fsSuid := newTestRawFS(t, srvSuid)
		oSuid := mustOpenFile(t, fsSuid, suidIno, syscall.O_RDWR)

		// When
		mustWriteAt(t, fsSuid, suidIno, oSuid.Fh, 0, []byte("x"))
		mustWriteAt(t, fsSuid, suidIno, oSuid.Fh, 1, []byte("y"))

		// Then
		require.Equal(t, int64(2), srvSuid.writeCalls.Load(), "SetUID 同步直通 Write RPC 次数: want=2, got=%d", srvSuid.writeCalls.Load())
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

	t.Run("超过4MiB单次写入按4MiB分片完整直写落盘", func(t *testing.T) {
		// Given
		srv := newFakeStorage(t, withTTL(1.0, 1.0))
		fs := newTestRawFS(t, srv)
		created := mustCreateFile(t, fs, fuse.FUSE_ROOT_ID, "large-ok.bin", syscall.O_RDWR, 0o644)
		okBuf := buildSeededBytes(4718592, 0x5A)

		// When
		nOK, stOK := fs.Write(nil, &fuse.WriteIn{
			InHeader: fuse.InHeader{NodeId: created.NodeId},
			Fh:       created.Fh,
			Offset:   0,
			Size:     4718592,
		}, okBuf)

		// Then
		requireStatus(t, "超 4 MiB 正常写入", fuse.OK, stOK)
		require.Equal(t, uint32(4718592), nOK, "超 4 MiB 写入长度: want=4718592, got=%d", nOK)
		stored, ok := srv.fileBytes(created.NodeId)
		require.True(t, ok)
		requireBytesMatch(t, "超 4 MiB 分片落盘数据", okBuf, stored)
		fs.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: created.NodeId}, Fh: created.Fh})
	})

	t.Run("超过4MiB多片写入第二片遇ENOSPC按POSIX返回首片短写成功", func(t *testing.T) {
		// Given
		srv := newFakeStorage(t, withTTL(1.0, 1.0))
		var failSecondChunk atomic.Bool
		failSecondChunk.Store(true)
		srv.writeHook = makeSecondChunkEnospcHook(&failSecondChunk, chunk4MiB)
		fs := newTestRawFS(t, srv)
		created := mustCreateFile(t, fs, fuse.FUSE_ROOT_ID, "large-short.bin", syscall.O_RDWR, 0o644)
		shortBuf := buildRepeatingBytes(4195328, 'Z')

		// When
		nShort, stShort := fs.Write(nil, &fuse.WriteIn{
			InHeader: fuse.InHeader{NodeId: created.NodeId},
			Fh:       created.Fh,
			Offset:   0,
			Size:     4195328,
		}, shortBuf)

		// Then
		requireStatus(t, "二片 ENOSPC 按 POSIX 短写返回 OK", fuse.OK, stShort)
		require.Equal(t, uint32(4194304), nShort, "短写返回字节: want=4194304, got=%d", nShort)
		fs.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: created.NodeId}, Fh: created.Fh})
	})

	t.Run("超过4MiB多片写入首片遇ENOSPC直接返回ENOSPC", func(t *testing.T) {
		// Given
		srv := newFakeStorage(t, withTTL(1.0, 1.0))
		var failAtOffset atomic.Bool
		failAtOffset.Store(true)
		srv.writeHook = makeSecondChunkEnospcHook(&failAtOffset, chunk4MiB)
		fs := newTestRawFS(t, srv)
		created := mustCreateFile(t, fs, fuse.FUSE_ROOT_ID, "large-first-fail.bin", syscall.O_RDWR, 0o644)
		buf := buildRepeatingBytes(4195328, 'Z')

		// When
		nErr, stErr := fs.Write(nil, &fuse.WriteIn{
			InHeader: fuse.InHeader{NodeId: created.NodeId},
			Fh:       created.Fh,
			Offset:   4194304,
			Size:     4195328,
		}, buf)

		// Then
		requireStatus(t, "首片 ENOSPC 返回 ENOSPC", fuse.Status(syscall.ENOSPC), stErr)
		require.Zero(t, nErr, "首片即失败返回字节数: want=0, got=%d", nErr)
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

func TestRead与ReadAhead_校验读写权限本地Stage直读顺序预读命中及随机读零放大(t *testing.T) {
	t.Run("O_WRONLY只写句柄调用Read返回EBADF", func(t *testing.T) {
		// Given
		srv := newFakeStorage(t, withTTL(1.0, 1.0))
		fs := newTestRawFS(t, srv, withDirectIO(false))
		wrOnly := mustCreateFile(t, fs, fuse.FUSE_ROOT_ID, "wronly.bin", syscall.O_WRONLY, 0o644)

		// When
		_, ebadfSt := fs.Read(nil, &fuse.ReadIn{InHeader: fuse.InHeader{NodeId: wrOnly.NodeId}, Fh: wrOnly.Fh, Size: 16}, make([]byte, 16))

		// Then
		requireStatus(t, "O_WRONLY Read", fuse.EBADF, ebadfSt)
		fs.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: wrOnly.NodeId}, Fh: wrOnly.Fh})
	})

	t.Run("零长度Read请求直接返回OK空切片且不发RPC", func(t *testing.T) {
		// Given
		srv := newFakeStorage(t, withTTL(1.0, 1.0))
		fs := newTestRawFS(t, srv, withDirectIO(false))
		created := mustCreateFile(t, fs, fuse.FUSE_ROOT_ID, "zero-read.bin", syscall.O_RDWR, 0o644)

		// When
		zeroRes, zeroSt := fs.Read(nil, &fuse.ReadIn{InHeader: fuse.InHeader{NodeId: created.NodeId}, Fh: created.Fh, Size: 0}, nil)
		zeroBytes, _ := zeroRes.Bytes(nil)

		// Then
		requireStatus(t, "零字节 Read", fuse.OK, zeroSt)
		require.Empty(t, zeroBytes, "零长度 Read 返回长度: want=0, got=%d", len(zeroBytes))
		require.Zero(t, srv.readCalls.Load(), "零长度 Read RPC 次数: want=0, got=%d", srv.readCalls.Load())
		fs.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: created.NodeId}, Fh: created.Fh})
	})

	t.Run("未Fsync的Stage暂存写入被只读句柄立即从本地分片读见", func(t *testing.T) {
		// Given
		srv := newFakeStorage(t, withTTL(1.0, 1.0))
		fs := newTestRawFS(t, srv, withDirectIO(false))
		wrOnly := mustCreateFile(t, fs, fuse.FUSE_ROOT_ID, "stage-read.bin", syscall.O_WRONLY, 0o644)
		mustWriteAt(t, fs, wrOnly.NodeId, wrOnly.Fh, 0, []byte("staged-payload"))
		rdOnly := mustOpenFile(t, fs, wrOnly.NodeId, syscall.O_RDONLY)

		// When
		readBack := mustReadAt(t, fs, wrOnly.NodeId, rdOnly.Fh, 0, 14)

		// Then
		requireBytesMatch(t, "Read 直接从本地 Stage 读见未Fsync写入", []byte("staged-payload"), readBack)
		require.Zero(t, srv.writeCalls.Load(), "Read 不得阻塞等待或主动触发后台 Write RPC")
		require.Zero(t, srv.readCalls.Load(), "本地 Stage 已全覆盖的读取不得产生远端 Read RPC")

		fs.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: wrOnly.NodeId}, Fh: wrOnly.Fh})
		fs.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: wrOnly.NodeId}, Fh: rdOnly.Fh})
	})

	t.Run("顺序大步长读取通过ReadAhead预读合并将RPC次数收敛至12次以内", func(t *testing.T) {
		// Given
		truth := buildSeededBytes(16777216, 0x71)
		srv := newFakeStorage(t, withTTL(1.0, 1.0))
		seqIno := srv.seedFile(1, "seq.bin", 0o644, truth)
		fs := newTestRawFS(t, srv, withDirectIO(false))
		seqFH := mustOpenFile(t, fs, seqIno, syscall.O_RDWR)

		// When
		gotSeq := performSequentialReads(t, fs, seqIno, seqFH.Fh, 16777216, 524288)
		seqRPCCalls := srv.readCalls.Load()
		fs.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: seqIno}, Fh: seqFH.Fh})

		// Then
		requireBytesMatch(t, "顺序预读完整内容", truth, gotSeq)
		require.Less(t, seqRPCCalls, int64(12), "16 MiB 顺序预读 RPC 次数: want<12, got=%d", seqRPCCalls)
	})

	t.Run("Write覆写首部使ReadAhead失效后立即读见最新写入数据", func(t *testing.T) {
		// Given
		srv := newFakeStorage(t, withTTL(1.0, 1.0))
		ino := srv.seedFile(1, "inval.bin", 0o644, buildSeededBytes(1048576, 0x71))
		fs := newTestRawFS(t, srv, withDirectIO(false))
		fh := mustOpenFile(t, fs, ino, syscall.O_RDWR)
		_ = mustReadAt(t, fs, ino, fh.Fh, 0, 524288)

		// When
		mustWriteAt(t, fs, ino, fh.Fh, 0, []byte("PATCHED!"))
		got := mustReadAt(t, fs, ino, fh.Fh, 0, 8)
		fs.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: ino}, Fh: fh.Fh})

		// Then
		requireBytesMatch(t, "Write 使 ReadAhead 失效后应读见最新数据", []byte("PATCHED!"), got)
	})

	t.Run("随机大跨度跳跃读取零预读放大", func(t *testing.T) {
		// Given
		truth := buildSeededBytes(16777216, 0x71)
		srv := newFakeStorage(t, withTTL(1.0, 1.0))
		randIno := srv.seedFile(1, "rand.bin", 0o644, truth)
		fs := newTestRawFS(t, srv, withDirectIO(false))
		randFH := mustOpenFile(t, fs, randIno, syscall.O_RDONLY)
		jumps := []uint64{13631488, 2097152, 10485760, 5242880, 15728640, 0}

		// When
		performRandomReads(t, fs, randIno, randFH.Fh, jumps, 1048576, truth)
		randCalls := srv.readCalls.Load()
		fs.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: randIno}, Fh: randFH.Fh})

		// Then
		require.Equal(t, int64(6), randCalls, "6 次随机跳跃读取 Read RPC 次数: want=6, got=%d", randCalls)
	})
}

func TestTmpfile与OnUnmount_匿名文件具名化本地零RPC与关闭同步(t *testing.T) {
	t.Run("O_RDONLY创建Tmpfile被拒绝返回EINVAL", func(t *testing.T) {
		// Given
		srv := newFakeStorage(t, withTTL(1.0, 1.0))
		fs := newTestRawFS(t, srv)
		var badOut fuse.CreateOut

		// When
		st := fs.Tmpfile(nil, &fuse.CreateIn{
			InHeader: fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID},
			Flags:    uint32(syscall.O_RDONLY),
			Mode:     0o644,
		}, &badOut)

		// Then
		requireStatus(t, "O_RDONLY Tmpfile", fuse.EINVAL, st)
	})

	t.Run("Tmpfile非法Fallocate偏移返回EINVAL且非法SetAttr尺寸返回EFBIG", func(t *testing.T) {
		// Given
		srv := newFakeStorage(t, withTTL(1.0, 1.0))
		fs := newTestRawFS(t, srv)
		var tmpOut fuse.CreateOut
		requireStatus(t, "Tmpfile O_RDWR", fuse.OK, fs.Tmpfile(nil, &fuse.CreateIn{
			InHeader: fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID},
			Flags:    uint32(syscall.O_RDWR),
			Mode:     0o640,
		}, &tmpOut))

		// When
		fallocSt := fs.Fallocate(nil, &fuse.FallocateIn{
			InHeader: fuse.InHeader{NodeId: tmpOut.NodeId},
			Fh:       tmpOut.Fh,
			Offset:   uint64(1) << 63,
			Length:   4096,
			Mode:     0,
		})
		setattrSt := fs.SetAttr(nil, &fuse.SetAttrIn{
			SetAttrInCommon: fuse.SetAttrInCommon{
				InHeader: fuse.InHeader{NodeId: tmpOut.NodeId},
				Valid:    fuse.FATTR_SIZE,
				Size:     uint64(1) << 63,
			},
		}, &fuse.AttrOut{})

		// Then
		requireStatus(t, "Tmpfile 非法 Fallocate 偏移", fuse.EINVAL, fallocSt)
		requireStatus(t, "Tmpfile 非法 SetAttr 大小返回 EFBIG", fuse.Status(syscall.EFBIG), setattrSt)
		fs.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: tmpOut.NodeId}, Fh: tmpOut.Fh})
		fs.Forget(tmpOut.NodeId, 1)
	})

	t.Run("Tmpfile写入并局部打洞后Link具名化可读取完整打洞数据", func(t *testing.T) {
		// Given
		srv := newFakeStorage(t, withTTL(1.0, 1.0))
		fs := newTestRawFS(t, srv)
		var tmpOut fuse.CreateOut
		requireStatus(t, "Tmpfile O_RDWR", fuse.OK, fs.Tmpfile(nil, &fuse.CreateIn{
			InHeader: fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID},
			Flags:    uint32(syscall.O_RDWR),
			Mode:     0o640,
		}, &tmpOut))
		mustWriteAt(t, fs, tmpOut.NodeId, tmpOut.Fh, 0, []byte("tmp-header-tail"))
		requireStatus(t, "Tmpfile Fallocate 打洞", fuse.OK, fs.Fallocate(nil, &fuse.FallocateIn{
			InHeader: fuse.InHeader{NodeId: tmpOut.NodeId},
			Fh:       tmpOut.Fh,
			Offset:   4,
			Length:   7,
			Mode:     fallocFlPunchHole | fallocFlKeepSize,
		}))

		// When
		var promoted fuse.EntryOut
		linkSt := fs.Link(nil, &fuse.LinkIn{
			InHeader:  fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID},
			Oldnodeid: tmpOut.NodeId,
		}, "materialized.txt", &promoted)
		fs.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: tmpOut.NodeId}, Fh: tmpOut.Fh})
		fs.Forget(tmpOut.NodeId, 2)

		looked := mustLookup(t, fs, fuse.FUSE_ROOT_ID, "materialized.txt")
		opened := mustOpenFile(t, fs, looked.NodeId, syscall.O_RDONLY)
		got := mustReadAt(t, fs, looked.NodeId, opened.Fh, 0, 15)
		fs.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: looked.NodeId}, Fh: opened.Fh})

		// Then
		requireStatus(t, "Tmpfile Link 具名化", fuse.OK, linkSt)
		requireBytesMatch(t, "具名化后的打洞文件内容", []byte("tmp-\x00\x00\x00\x00\x00\x00\x00tail"), got)
	})

	t.Run("已具名化的Tmpfile禁止二次Link并返回ENOENT", func(t *testing.T) {
		// Given
		srv := newFakeStorage(t, withTTL(1.0, 1.0))
		fs := newTestRawFS(t, srv)
		var tmpOut fuse.CreateOut
		requireStatus(t, "Tmpfile O_RDWR", fuse.OK, fs.Tmpfile(nil, &fuse.CreateIn{
			InHeader: fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID},
			Flags:    uint32(syscall.O_RDWR),
			Mode:     0o640,
		}, &tmpOut))
		var first fuse.EntryOut
		requireStatus(t, "首次 Link", fuse.OK, fs.Link(nil, &fuse.LinkIn{
			InHeader:  fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID},
			Oldnodeid: tmpOut.NodeId,
		}, "first-link.txt", &first))

		// When
		var second fuse.EntryOut
		secondSt := fs.Link(nil, &fuse.LinkIn{
			InHeader:  fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID},
			Oldnodeid: tmpOut.NodeId,
		}, "second-link.txt", &second)

		// Then
		requireStatus(t, "禁止对已具名 Tmpfile 二次 Link", fuse.ENOENT, secondSt)
		fs.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: tmpOut.NodeId}, Fh: tmpOut.Fh})
		fs.Forget(tmpOut.NodeId, 2)
	})

	t.Run("未具名匿名文件Flush与SetAttr与Fsync纯本地执行零服务端RPC", func(t *testing.T) {
		// Given
		srv := newFakeStorage(t, withTTL(1.0, 1.0))
		fs := newTestRawFS(t, srv)
		var tmpOut fuse.CreateOut
		requireStatus(t, "Tmpfile", fuse.OK, fs.Tmpfile(nil, &fuse.CreateIn{
			InHeader: fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID},
			Flags:    uint32(syscall.O_RDWR),
			Mode:     0o600,
		}, &tmpOut))

		// When
		mustWriteAt(t, fs, tmpOut.NodeId, tmpOut.Fh, 0, []byte("local-host-file"))
		var truncOut fuse.AttrOut
		truncSt := fs.SetAttr(nil, &fuse.SetAttrIn{
			SetAttrInCommon: fuse.SetAttrInCommon{
				InHeader: fuse.InHeader{NodeId: tmpOut.NodeId},
				Valid:    fuse.FATTR_SIZE,
				Size:     10,
			},
		}, &truncOut)
		fsyncSt := fs.Fsync(nil, &fuse.FsyncIn{InHeader: fuse.InHeader{NodeId: tmpOut.NodeId}, Fh: tmpOut.Fh})
		flushSt := fs.Flush(nil, &fuse.FlushIn{InHeader: fuse.InHeader{NodeId: tmpOut.NodeId}, Fh: tmpOut.Fh})

		// Then
		requireStatus(t, "Tmpfile Truncate", fuse.OK, truncSt)
		require.Equal(t, uint64(10), truncOut.Size, "Truncate 后尺寸: want=10, got=%d", truncOut.Size)
		requireStatus(t, "Tmpfile Fsync", fuse.OK, fsyncSt)
		requireStatus(t, "Tmpfile Flush", fuse.OK, flushSt)
		require.Zero(t, srv.mknodCalls.Load()+srv.writeCalls.Load()+srv.unlinkCalls.Load()+srv.reclaimOrphanCalls.Load(),
			"未具名 O_TMPFILE 服务端 RPC 总数: want=0")

		fs.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: tmpOut.NodeId}, Fh: tmpOut.Fh})
		fs.Forget(tmpOut.NodeId, 1)
	})

	t.Run("未具名匿名文件先Forget后Release双清零本地销毁返回ENOENT", func(t *testing.T) {
		// Given
		srv := newFakeStorage(t, withTTL(1.0, 1.0))
		fs := newTestRawFS(t, srv)
		var tmpA fuse.CreateOut
		requireStatus(t, "Tmpfile A", fuse.OK, fs.Tmpfile(nil, &fuse.CreateIn{
			InHeader: fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID},
			Flags:    uint32(syscall.O_RDWR),
			Mode:     0o600,
		}, &tmpA))
		mustWriteAt(t, fs, tmpA.NodeId, tmpA.Fh, 0, []byte("forget-first"))

		// When
		fs.Forget(tmpA.NodeId, 1)
		fs.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: tmpA.NodeId}, Fh: tmpA.Fh})

		// Then
		require.Zero(t, srv.reclaimOrphanCalls.Load(), "本地回收 ReclaimOrphan 次数: want=0, got=%d", srv.reclaimOrphanCalls.Load())
		var attrAfter fuse.AttrOut
		requireStatus(t, "tmpA 双清零后 GetAttr", fuse.ENOENT, fs.GetAttr(nil, &fuse.GetAttrIn{InHeader: fuse.InHeader{NodeId: tmpA.NodeId}}, &attrAfter))
	})

	t.Run("未具名匿名文件先Release后Forget双清零本地销毁返回ENOENT", func(t *testing.T) {
		// Given
		srv := newFakeStorage(t, withTTL(1.0, 1.0))
		fs := newTestRawFS(t, srv)
		var tmpB fuse.CreateOut
		requireStatus(t, "Tmpfile B", fuse.OK, fs.Tmpfile(nil, &fuse.CreateIn{
			InHeader: fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID},
			Flags:    uint32(syscall.O_RDWR),
			Mode:     0o600,
		}, &tmpB))
		mustWriteAt(t, fs, tmpB.NodeId, tmpB.Fh, 0, []byte("release-first"))

		// When
		fs.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: tmpB.NodeId}, Fh: tmpB.Fh})
		fs.Forget(tmpB.NodeId, 1)

		// Then
		require.Zero(t, srv.reclaimOrphanCalls.Load(), "本地回收 ReclaimOrphan 次数: want=0, got=%d", srv.reclaimOrphanCalls.Load())
		var attrAfter fuse.AttrOut
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
