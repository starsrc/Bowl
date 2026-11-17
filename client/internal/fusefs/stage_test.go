package fusefs_test

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
	"unsafe"

	"connectrpc.com/connect"
	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
	"google.golang.org/genproto/googleapis/rpc/errdetails"

	"github.com/starsrc/bowl/client/internal/fusefs"
	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
)

func newTestGate() (gate <-chan struct{}, release func()) {
	ch := make(chan struct{})
	return ch, sync.OnceFunc(func() { close(ch) })
}

func listStageChunkFiles(t *testing.T, instanceDir string) []string {
	t.Helper()
	matches, err := filepath.Glob(filepath.Join(instanceDir, "chunk-*.stage"))
	require.NoError(t, err, "列举暂存分片文件失败: dir=%s", instanceDir)
	return matches
}

func assertNoOpenChunkFDs(t *testing.T, instanceDir string) {
	t.Helper()
	if runtime.GOOS != "linux" {
		return
	}
	entries, err := os.ReadDir("/proc/self/fd")
	require.NoError(t, err, "读取 /proc/self/fd 失败")
	prefix := filepath.Clean(instanceDir) + string(filepath.Separator)
	for _, entry := range entries {
		target, readErr := os.Readlink("/proc/self/fd/" + entry.Name())
		if readErr != nil {
			continue
		}
		if strings.HasPrefix(target, prefix) && strings.HasSuffix(target, ".stage") {
			t.Fatalf("空闲暂存分片描述符未关闭: want=0, got=%s->%s", entry.Name(), target)
		}
	}
}

func assertPlatformTmpfsResult(t *testing.T, nonTmpfsPath string) {
	t.Helper()
	isTmpfs, err := fusefs.CheckTmpfs(nonTmpfsPath)
	require.NoError(t, err, "CheckTmpfs(%s) 返回错误", nonTmpfsPath)
	if runtime.GOOS != "linux" {
		require.False(t, isTmpfs, "非 Linux 平台 CheckTmpfs: want=false, got=%v", isTmpfs)
		return
	}
	if _, statErr := os.Stat("/dev/shm"); statErr == nil {
		shmTmpfs, shmErr := fusefs.CheckTmpfs("/dev/shm")
		require.NoError(t, shmErr, "CheckTmpfs(/dev/shm) 失败")
		require.True(t, shmTmpfs, "Linux /dev/shm CheckTmpfs: want=true, got=%v", shmTmpfs)
	}
}

func createHeldLockDir(t *testing.T, dir string) {
	t.Helper()
	require.NoError(t, os.MkdirAll(dir, 0o700))
	lockFile, err := os.OpenFile(filepath.Join(dir, ".lock"), os.O_CREATE|os.O_RDWR|unix.O_CLOEXEC, 0o600)
	require.NoError(t, err)
	t.Cleanup(func() { _ = lockFile.Close() })
	require.NoError(t, unix.Flock(int(lockFile.Fd()), unix.LOCK_EX|unix.LOCK_NB))
}

func verifyConcurrentReadsDuringUpload(t *testing.T, fs *fusefs.RawFS, nodeID, fh uint64, want []byte) {
	t.Helper()
	var wg sync.WaitGroup
	wg.Go(func() {
		for range 12 {
			got := mustReadAt(t, fs, nodeID, fh, 0, len(want))
			requireBytesMatch(t, "上传解除删除并发读取", want, got)
		}
	})
	wg.Wait()
}

func assertOnlyLockEntryInDir(t *testing.T, dir string) {
	t.Helper()
	entries, err := os.ReadDir(dir)
	require.NoError(t, err, "读取暂存实例目录失败: dir=%s", dir)
	names := make([]string, 0, len(entries))
	for _, entry := range entries {
		names = append(names, entry.Name())
	}
	require.Equal(t, []string{".lock"}, names, "匿名宿主文件目录项检查: want=[.lock], got=%v", names)
}

func sumUploadedRanges(extents [][2]uint64, mu *sync.Mutex) uint64 {
	mu.Lock()
	defer mu.Unlock()
	var total uint64
	for _, ext := range extents {
		total += ext[1]
	}
	return total
}

func gatherMetricNameSet(t *testing.T, reg *prometheus.Registry) map[string]bool {
	t.Helper()
	mfs, err := reg.Gather()
	require.NoError(t, err, "采集 Prometheus 指标失败")
	names := make(map[string]bool, len(mfs))
	for _, mf := range mfs {
		names[mf.GetName()] = true
	}
	return names
}

func occupyAllStageWorkers(t *testing.T, fs *fusefs.RawFS, gate <-chan struct{}, srv *fakeStorage) (allBusy <-chan struct{}) {
	t.Helper()
	busyCh := make(chan struct{})
	var count atomic.Int32
	var once sync.Once
	srv.writeHook = func(_ context.Context, req *storagev1.WriteRequest) (uint32, error) {
		if len(req.GetData()) == 7 {
			if count.Add(1) == 8 {
				once.Do(func() { close(busyCh) })
			}
			<-gate
		}
		return 0, nil
	}
	for i := range 8 {
		b := mustCreateFile(t, fs, fuse.FUSE_ROOT_ID, fmt.Sprintf("blocker-%d.bin", i), syscall.O_RDWR, 0o644)
		mustWriteAt(t, fs, b.NodeId, b.Fh, 0, []byte("blocker"))
		fs.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: b.NodeId}, Fh: b.Fh})
	}
	return busyCh
}

func verifyChunkOrderTrack(cur int32, maxConcurrent *atomic.Int32) {
	for {
		prev := maxConcurrent.Load()
		if cur <= prev || maxConcurrent.CompareAndSwap(prev, cur) {
			break
		}
	}
}

func readDirPlusEntrySize(t *testing.T, fs *fusefs.RawFS, parent uint64, wantName string) uint64 {
	t.Helper()
	var dh fuse.OpenOut
	requireStatus(t, "OpenDir", fuse.OK, fs.OpenDir(nil, &fuse.OpenIn{InHeader: fuse.InHeader{NodeId: parent}}, &dh))
	defer fs.ReleaseDir(&fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: parent}, Fh: dh.Fh})

	buf := make([]byte, 32*1024)
	dl := fuse.NewDirEntryList(buf, 0)
	requireStatus(t, "ReadDirPlus", fuse.OK, fs.ReadDirPlus(nil, &fuse.ReadIn{
		InHeader: fuse.InHeader{NodeId: parent}, Fh: dh.Fh, Offset: 0, Size: uint32(len(buf)),
	}, dl))

	raw := dl.Bytes()
	sizeFieldOff := int(unsafe.Offsetof(fuse.EntryOut{}.Attr) + unsafe.Offsetof(fuse.Attr{}.Size))
	pos := 0
	for pos < len(raw) {
		head := pos + entryOutHeaderLen
		require.LessOrEqual(t, head+direntHeaderLen, len(raw), "ReadDirPlus 目录项头部截断")
		nameLen := int(binary.LittleEndian.Uint32(raw[head+16 : head+20]))
		nameStart := head + direntHeaderLen
		require.LessOrEqual(t, nameStart+nameLen, len(raw), "ReadDirPlus 目录项名字越界")
		if string(raw[nameStart:nameStart+nameLen]) == wantName {
			return binary.LittleEndian.Uint64(raw[pos+sizeFieldOff : pos+sizeFieldOff+8])
		}
		padding := (8 - (nameLen & 7)) & 7
		pos = nameStart + nameLen + padding
	}
	t.Fatalf("ReadDirPlus 未找到目标目录项: want=%q", wantName)
	return 0
}

func errorWithReason(code connect.Code, msg, reason string) error {
	cErr := connect.NewError(code, errors.New(msg))
	if detail, err := connect.NewErrorDetail(&errdetails.ErrorInfo{Reason: reason}); err == nil {
		cErr.AddDetail(detail)
	}
	return cErr
}

func makeGateOffsetWriteHook(targetOffset uint64, entered chan<- struct{}, gate <-chan struct{}) func(context.Context, *storagev1.WriteRequest) (uint32, error) {
	var once sync.Once
	return func(_ context.Context, req *storagev1.WriteRequest) (uint32, error) {
		if req.GetOffset() == targetOffset {
			once.Do(func() { close(entered) })
			<-gate
		}
		return 0, nil
	}
}

func makeGateMinOffsetWriteHook(minOffset uint64, gate <-chan struct{}) func(context.Context, *storagev1.WriteRequest) (uint32, error) {
	return func(_ context.Context, req *storagev1.WriteRequest) (uint32, error) {
		if req.GetOffset() >= minOffset {
			<-gate
		}
		return 0, nil
	}
}

func makeConcurrentFullChunkGateHook(chunkLen int, targetCount int32, entered chan<- struct{}, gate <-chan struct{}) func(context.Context, *storagev1.WriteRequest) (uint32, error) {
	var inflight atomic.Int32
	var once sync.Once
	return func(_ context.Context, req *storagev1.WriteRequest) (uint32, error) {
		if len(req.GetData()) == chunkLen {
			if inflight.Add(1) == targetCount {
				once.Do(func() { close(entered) })
			}
			<-gate
			inflight.Add(-1)
		}
		return 0, nil
	}
}

func makeTargetInoOrderedHook(
	targetIno, holdOffset uint64,
	active, maxConcurrent *atomic.Int32,
	offMu *sync.Mutex,
	observedOffsets *[]uint64,
	started chan<- struct{},
	gate <-chan struct{},
) func(context.Context, *storagev1.WriteRequest) (uint32, error) {
	var once sync.Once
	return func(_ context.Context, req *storagev1.WriteRequest) (uint32, error) {
		if req.GetIno() == targetIno {
			verifyChunkOrderTrack(active.Add(1), maxConcurrent)
			offMu.Lock()
			*observedOffsets = append(*observedOffsets, req.GetOffset())
			offMu.Unlock()
			if req.GetOffset() == holdOffset {
				once.Do(func() { close(started) })
				<-gate
			}
			active.Add(-1)
		}
		return 0, nil
	}
}

func makeFailFirstAttemptsWriteHook(failCount int32, injectErr error, attempts *atomic.Int32) func(context.Context, *storagev1.WriteRequest) (uint32, error) {
	return func(_ context.Context, _ *storagev1.WriteRequest) (uint32, error) {
		if attempts.Add(1) <= failCount {
			return 0, injectErr
		}
		return 0, nil
	}
}

func makeToggleErrorWriteHook(flag *atomic.Bool, injectErr error) func(context.Context, *storagev1.WriteRequest) (uint32, error) {
	return func(_ context.Context, _ *storagev1.WriteRequest) (uint32, error) {
		if flag.Load() {
			return 0, injectErr
		}
		return 0, nil
	}
}

func TestOpenStageDirSession_空路径或纯空白路径_拒绝并返回错误(t *testing.T) {
	cases := []struct {
		name string
		dir  string
	}{
		{name: "空字符串", dir: ""},
		{name: "纯空白字符", dir: "   "},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// Given / When
			_, err := fusefs.OpenStageDirSession(tc.dir)

			// Then
			require.Error(t, err, "无效 stageDir=%q 必须返回错误: want=error, got=nil", tc.dir)
		})
	}
}

func TestOpenStageDirSession_空白根目录关闭后重开_CleanedCount为0且重建实例目录(t *testing.T) {
	// Given
	stageRoot := filepath.Join(t.TempDir(), "empty-stage")

	// When
	sess1, err1 := fusefs.OpenStageDirSession(stageRoot)
	require.NoError(t, err1)
	cleaned1 := sess1.CleanedCount()
	require.NoError(t, sess1.Close())

	sess2, err2 := fusefs.OpenStageDirSession(stageRoot)
	require.NoError(t, err2)
	t.Cleanup(func() { _ = sess2.Close() })

	// Then
	require.Zero(t, cleaned1, "首次空白目录 CleanedCount: want=0, got=%d", cleaned1)
	require.Zero(t, sess2.CleanedCount(), "Close 后重开 CleanedCount: want=0, got=%d", sess2.CleanedCount())
	require.DirExists(t, sess2.InstanceDir(), "重开后实例目录必须存在: dir=%s", sess2.InstanceDir())
}

func TestOpenStageDirSession_存在失效残留与持锁存活目录_清理失效项并保留活跃目录(t *testing.T) {
	// Given
	stageRoot := filepath.Join(t.TempDir(), "stage-root")
	require.NoError(t, os.MkdirAll(stageRoot, 0o755))

	defunctUnlocked := filepath.Join(stageRoot, "inst-999998-aaaa")
	require.NoError(t, os.MkdirAll(defunctUnlocked, 0o700))
	require.NoError(t, os.WriteFile(filepath.Join(defunctUnlocked, ".lock"), nil, 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(defunctUnlocked, "chunk-1.stage"), []byte("stale"), 0o600))

	defunctDeadPID := filepath.Join(stageRoot, "inst-999997-bbbb")
	require.NoError(t, os.MkdirAll(defunctDeadPID, 0o700))

	staleLooseFile := filepath.Join(stageRoot, "loose.tmp")
	require.NoError(t, os.WriteFile(staleLooseFile, []byte("orphan"), 0o600))

	liveLocked := filepath.Join(stageRoot, "inst-111111-live")
	createHeldLockDir(t, liveLocked)

	livePIDDir := filepath.Join(stageRoot, fmt.Sprintf("inst-%d-nolock", os.Getpid()))
	require.NoError(t, os.MkdirAll(livePIDDir, 0o700))

	// When
	sess, err := fusefs.OpenStageDirSession(stageRoot)
	require.NoError(t, err)
	t.Cleanup(func() { _ = sess.Close() })

	// Then
	require.Equal(t, 3, sess.CleanedCount(), "清理计数校验: want=3, got=%d", sess.CleanedCount())
	require.NoDirExists(t, defunctUnlocked, "未持锁失效目录必须被删除")
	require.NoDirExists(t, defunctDeadPID, "死亡 PID 无锁目录必须被删除")
	require.NoFileExists(t, staleLooseFile, "根目录散落过期文件必须被删除")
	require.DirExists(t, liveLocked, "外部持 flock 目录必须保留")
	require.DirExists(t, livePIDDir, "无锁但 PID 存活目录必须保留")
	require.DirExists(t, sess.InstanceDir(), "当前实例目录必须创建成功")
}

func TestOpenStageDirSession_并发双实例与重复Close_互不清理彼此目录且Close严格幂等(t *testing.T) {
	// Given
	stageRoot := filepath.Join(t.TempDir(), "concurrent-stage")
	sess1, err := fusefs.OpenStageDirSession(stageRoot)
	require.NoError(t, err)
	t.Cleanup(func() { _ = sess1.Close() })

	// When
	sess2, err2 := fusefs.OpenStageDirSession(stageRoot)
	require.NoError(t, err2)
	cleaned2 := sess2.CleanedCount()

	// Then
	require.Zero(t, cleaned2, "并发开启第二实例 CleanedCount: want=0, got=%d", cleaned2)
	require.DirExists(t, sess1.InstanceDir())
	require.DirExists(t, sess2.InstanceDir())
	require.NoError(t, sess2.Close(), "首次 Close 必须成功")
	require.NoError(t, sess2.Close(), "二次重复 Close 必须幂等无错")
	require.NoDirExists(t, sess2.InstanceDir(), "Close 后本实例目录必须删除")
	require.DirExists(t, sess1.InstanceDir(), "关闭 sess2 不得影响存活的 sess1")
}

func TestCheckTmpfs_按宿主机平台判定文件系类型_非Linux恒False且Linux识别Tmpfs(t *testing.T) {
	// Given
	stageRoot := filepath.Join(t.TempDir(), "nested", "bowl-stage")
	sess, err := fusefs.OpenStageDirSession(stageRoot)
	require.NoError(t, err)
	t.Cleanup(func() { _ = sess.Close() })

	// When / Then
	assertPlatformTmpfsResult(t, stageRoot)
}

func TestNewRawFS_未传StageSession且非只读_自动创建实例目录并在OnUnmount销毁(t *testing.T) {
	// Given
	srv := newFakeStorage(t, withTTL(1.0, 1.0))
	autoDir := filepath.Join(t.TempDir(), "auto-stage")

	// When
	rwFS := newTestRawFS(t, srv, withStageDir(autoDir))
	entriesBefore, errBefore := os.ReadDir(autoDir)
	require.NoError(t, errBefore)
	rwFS.OnUnmount()
	entriesAfter, errAfter := os.ReadDir(autoDir)
	require.NoError(t, errAfter)

	// Then
	require.Len(t, entriesBefore, 1, "隐式读写挂载应创建 1 个实例目录: want=1, got=%d", len(entriesBefore))
	require.Empty(t, entriesAfter, "OnUnmount 后必须销毁隐式 StageDirSession: want=0, got=%d", len(entriesAfter))
}

func TestNewRawFS_ReadOnly模式_零目录零工作协程且写操作返回EROFS(t *testing.T) {
	// Given
	srv := newFakeStorage(t, withTTL(1.0, 1.0))
	roDir := filepath.Join(t.TempDir(), "ro-stage")
	roFS := newTestRawFS(t, srv, withMode(fusefs.MountModeRO), withStageDir(roDir))
	t.Cleanup(roFS.OnUnmount)

	// When
	_, statErr := os.Stat(roDir)
	snap := roFS.Snapshot()
	var out fuse.CreateOut
	createSt := roFS.Create(nil, &fuse.CreateIn{
		InHeader: fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID}, Flags: uint32(syscall.O_RDWR), Mode: 0o644,
	}, "ro.txt", &out)
	tmpSt := roFS.Tmpfile(nil, &fuse.CreateIn{
		InHeader: fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID}, Flags: uint32(syscall.O_RDWR), Mode: 0o600,
	}, &out)

	// Then
	require.True(t, os.IsNotExist(statErr), "只读挂载严禁创建暂存目录: want=NotExist, got=%v", statErr)
	require.Zero(t, snap.Stage.CapacityBytes, "只读挂载 Stage CapacityBytes: want=0, got=%d", snap.Stage.CapacityBytes)
	requireStatus(t, "ReadOnly Create", fuse.Status(syscall.EROFS), createSt)
	requireStatus(t, "ReadOnly Tmpfile", fuse.Status(syscall.EROFS), tmpSt)
}

func TestStageCoordinator_连续与向左回退小块写_合并进0600标准分片文件且闲置零OpenFD(t *testing.T) {
	// Given
	srv := newFakeStorage(t, withTTL(1.0, 1.0))
	stageRoot := filepath.Join(t.TempDir(), "coalesce-stage")
	sess, err := fusefs.OpenStageDirSession(stageRoot)
	require.NoError(t, err)
	t.Cleanup(func() { _ = sess.Close() })

	fs := newTestRawFS(t, srv, withDirectIO(false), withStageSession(sess))
	created := mustCreateFile(t, fs, fuse.FUSE_ROOT_ID, "coalesce.bin", syscall.O_RDWR, 0o644)

	// When: 先写 [3..8)="lo-go"，再向左邻接写 [0..3)="hel"，保持单一活跃分片文件
	mustWriteAt(t, fs, created.NodeId, created.Fh, 3, []byte("lo-go"))
	mustWriteAt(t, fs, created.NodeId, created.Fh, 0, []byte("hel"))

	// Then
	chunkFiles := listStageChunkFiles(t, sess.InstanceDir())
	require.Len(t, chunkFiles, 1, "连续与向左回退写入分片数: want=1, got=%d", len(chunkFiles))
	info, statErr := os.Stat(chunkFiles[0])
	require.NoError(t, statErr)
	require.Equal(t, os.FileMode(0o600), info.Mode().Perm(), "分片文件权限: want=0600, got=%o", info.Mode().Perm())
	assertNoOpenChunkFDs(t, sess.InstanceDir())
	require.Zero(t, srv.writeCalls.Load(), "未封口小块写不得触发服务端 Write RPC")

	fs.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: created.NodeId}, Fh: created.Fh})
}

func TestStageCoordinator_后台上传挂起期间_Flush零阻塞且ReadGetAttrLookupReadDirPlus直读含空洞本地分片(t *testing.T) {
	// Given
	srv := newFakeStorage(t, withTTL(1.0, 1.0))
	uploadEntered := make(chan struct{})
	uploadGate, releaseUploadGate := newTestGate()
	defer releaseUploadGate()
	srv.writeHook = makeGateOffsetWriteHook(0, uploadEntered, uploadGate)

	fs := newTestRawFS(t, srv, withDirectIO(false))
	created := mustCreateFile(t, fs, fuse.FUSE_ROOT_ID, "overlay.bin", syscall.O_RDWR, 0o644)

	// When: 写 [0..8)="hello-go"，跳跃写 [12..17)="world" 触发第 1 块入队挂起在 writeHook
	mustWriteAt(t, fs, created.NodeId, created.Fh, 0, []byte("hello-go"))
	mustWriteAt(t, fs, created.NodeId, created.Fh, 12, []byte("world"))
	<-uploadEntered

	flushSt := fs.Flush(nil, &fuse.FlushIn{InHeader: fuse.InHeader{NodeId: created.NodeId}, Fh: created.Fh})
	attr := mustGetAttr(t, fs, created.NodeId)
	looked := mustLookup(t, fs, fuse.FUSE_ROOT_ID, "overlay.bin")
	rdpSize := readDirPlusEntrySize(t, fs, fuse.FUSE_ROOT_ID, "overlay.bin")
	gotLocal := mustReadAt(t, fs, created.NodeId, created.Fh, 0, 17)

	// Then
	requireStatus(t, "Flush 必须立即非阻塞返回", fuse.OK, flushSt)
	require.Equal(t, uint64(17), attr.Size, "GetAttr 叠加尺寸: want=17, got=%d", attr.Size)
	require.Equal(t, uint64(17), looked.Size, "Lookup 叠加尺寸: want=17, got=%d", looked.Size)
	require.Equal(t, uint64(17), rdpSize, "ReadDirPlus 目录项尺寸: want=17, got=%d", rdpSize)
	requireBytesMatch(t, "本地空洞叠加读", []byte("hello-go\x00\x00\x00\x00world"), gotLocal)
	require.Zero(t, srv.readCalls.Load(), "本地全覆盖读服务端 Read RPC 次数: want=0, got=%d", srv.readCalls.Load())

	releaseUploadGate()
	fs.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: created.NodeId}, Fh: created.Fh})
}

func TestStageCoordinator_后台上传提交并并发读取_立即Unlink分片文件且读者零撕裂(t *testing.T) {
	// Given
	srv := newFakeStorage(t, withTTL(1.0, 1.0))
	uploadEntered := make(chan struct{})
	uploadGate, releaseUploadGate := newTestGate()
	defer releaseUploadGate()
	srv.writeHook = makeGateOffsetWriteHook(0, uploadEntered, uploadGate)

	stageRoot := filepath.Join(t.TempDir(), "unlink-stage")
	sess, err := fusefs.OpenStageDirSession(stageRoot)
	require.NoError(t, err)
	t.Cleanup(func() { _ = sess.Close() })

	fs := newTestRawFS(t, srv, withDirectIO(false), withStageSession(sess))
	created := mustCreateFile(t, fs, fuse.FUSE_ROOT_ID, "concurrent-unlink.bin", syscall.O_RDWR, 0o644)
	mustWriteAt(t, fs, created.NodeId, created.Fh, 0, []byte("hello-go"))
	mustWriteAt(t, fs, created.NodeId, created.Fh, 12, []byte("world"))
	<-uploadEntered

	// When: 放行上传的同时高并发读取相同区间，随后 Fsync 收口
	releaseUploadGate()
	verifyConcurrentReadsDuringUpload(t, fs, created.NodeId, created.Fh, []byte("hello-go\x00\x00\x00\x00world"))
	requireStatus(t, "Fsync", fuse.OK, fs.Fsync(nil, &fuse.FsyncIn{InHeader: fuse.InHeader{NodeId: created.NodeId}, Fh: created.Fh}))

	// Then
	require.Empty(t, listStageChunkFiles(t, sess.InstanceDir()), "上传提交后 chunk-*.stage 必须立即被 unlink 清除")
	assertNoOpenChunkFDs(t, sess.InstanceDir())
	fs.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: created.NodeId}, Fh: created.Fh})
}

func TestStageCoordinator_ZeroAttrTTL既有多段写入_本地分片融合服务端基底读见最新尺寸与内容(t *testing.T) {
	// Given
	srv := newFakeStorage(t, withTTL(0, 0))
	ino := srv.seedFile(1, "base.bin", 0o644, []byte("0123456789abcdefghij"))

	uploadBlocked, releaseUploadBlocked := newTestGate()
	defer releaseUploadBlocked()
	uploadStarted := make(chan struct{})
	srv.writeHook = makeGateOffsetWriteHook(5, uploadStarted, uploadBlocked)

	fs := newTestRawFS(t, srv, withDirectIO(false))
	fh := mustOpenFile(t, fs, ino, syscall.O_RDWR)

	// When: 首写探测后覆写中间段 [5..10)="HELLO" 并跳跃写入尾部扩展 [20..25)="KLMNO"
	mustWriteAt(t, fs, ino, fh.Fh, 0, []byte("0"))
	mustWriteAt(t, fs, ino, fh.Fh, 5, []byte("HELLO"))
	mustWriteAt(t, fs, ino, fh.Fh, 20, []byte("KLMNO"))
	<-uploadStarted

	attr := mustGetAttr(t, fs, ino)
	looked := mustLookup(t, fs, fuse.FUSE_ROOT_ID, "base.bin")
	got := mustReadAt(t, fs, ino, fh.Fh, 0, 25)

	// Then
	require.Equal(t, uint64(25), attr.Size, "ZeroTTL GetAttr 尺寸: want=25, got=%d", attr.Size)
	require.Equal(t, uint64(25), looked.Size, "ZeroTTL Lookup 尺寸: want=25, got=%d", looked.Size)
	requireBytesMatch(t, "服务端基底与多段本地分片融合读取", []byte("01234HELLOabcdefghijKLMNO"), got)

	releaseUploadBlocked()
	requireStatus(t, "Fsync", fuse.OK, fs.Fsync(nil, &fuse.FsyncIn{InHeader: fuse.InHeader{NodeId: ino}, Fh: fh.Fh}))
	fs.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: ino}, Fh: fh.Fh})
}

func TestStageCoordinator_首段上传清除后再写尾段_跨远端回源与本地分片无缝拼接读出(t *testing.T) {
	// Given
	srv := newFakeStorage(t, withTTL(1.0, 1.0))
	fs := newTestRawFS(t, srv, withDirectIO(false))
	f := mustCreateFile(t, fs, fuse.FUSE_ROOT_ID, "split-partial.bin", syscall.O_RDWR, 0o644)

	firstChunk := buildRepeatingBytes(4194304, 'Q')
	mustWriteAt(t, fs, f.NodeId, f.Fh, 0, firstChunk)
	require.Eventually(t, func() bool {
		return fs.Snapshot().Stage.UsedBytes == 0 && srv.writeCalls.Load() == 1
	}, 5*time.Second, 5*time.Millisecond, "首块 4 MiB 未在超时前完成异步上传")

	// When: 阻塞第 2 段上传，再写尾段并通过单次 Read 跨过首尾边界
	secondGate, releaseSecondGate := newTestGate()
	defer releaseSecondGate()
	srv.writeHook = makeGateMinOffsetWriteHook(4194304, secondGate)
	mustWriteAt(t, fs, f.NodeId, f.Fh, 4194304, []byte("TAIL-888"))
	got := mustReadAt(t, fs, f.NodeId, f.Fh, 4194288, 24)

	// Then: [4194288..4194304) 来自服务端回源 16 个 'Q'，[4194304..4194312) 来自本地分片 "TAIL-888"
	requireBytesMatch(t, "跨已落盘首段与本地在途尾段拼接读", []byte("QQQQQQQQQQQQQQQQTAIL-888"), got)

	releaseSecondGate()
	fs.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: f.NodeId}, Fh: f.Fh})
}

func TestStageCoordinator_同节点256KiB对齐整块_允许并发在途WriteRPC(t *testing.T) {
	// Given
	srv := newFakeStorage(t, withTTL(1.0, 1.0))
	bothInflight := make(chan struct{})
	releaseBoth, unblockBoth := newTestGate()
	defer unblockBoth()
	srv.writeHook = makeConcurrentFullChunkGateHook(4194304, 2, bothInflight, releaseBoth)
	fs := newTestRawFS(t, srv, withDirectIO(false))
	fAligned := mustCreateFile(t, fs, fuse.FUSE_ROOT_ID, "aligned.bin", syscall.O_RDWR, 0o644)

	// When: 同一文件连续写入两个 256 KiB 对齐的 4 MiB 整块
	mustWriteAt(t, fs, fAligned.NodeId, fAligned.Fh, 0, buildRepeatingBytes(4194304, 'A'))
	mustWriteAt(t, fs, fAligned.NodeId, fAligned.Fh, 4194304, buildRepeatingBytes(4194304, 'B'))
	<-bothInflight

	// Then: 两个 Write RPC 并发同时停留在服务端
	unblockBoth()
	requireStatus(t, "Fsync aligned", fuse.OK, fs.Fsync(nil, &fuse.FsyncIn{InHeader: fuse.InHeader{NodeId: fAligned.NodeId}, Fh: fAligned.Fh}))
	fs.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: fAligned.NodeId}, Fh: fAligned.Fh})
}

func TestStageCoordinator_同节点非对齐分片_严格按Seq串行提交WriteRPC(t *testing.T) {
	// Given
	srv := newFakeStorage(t, withTTL(1.0, 1.0))
	var activeOnTarget atomic.Int32
	var maxConcurrent atomic.Int32
	var observedOffsets []uint64
	var offMu sync.Mutex
	firstStarted := make(chan struct{})
	firstGate, releaseFirstGate := newTestGate()
	defer releaseFirstGate()

	fs := newTestRawFS(t, srv, withDirectIO(false))
	fUnaligned := mustCreateFile(t, fs, fuse.FUSE_ROOT_ID, "unaligned.bin", syscall.O_RDWR, 0o644)
	probeFile := mustCreateFile(t, fs, fuse.FUSE_ROOT_ID, "probe-fifo.bin", syscall.O_RDWR, 0o644)
	srv.writeHook = makeTargetInoOrderedHook(fUnaligned.NodeId, 100, &activeOnTarget, &maxConcurrent, &offMu, &observedOffsets, firstStarted, firstGate)

	// When: 向目标文件写 3 段非对齐分片（触发前 2 段入队），在首段挂起后提交并等待另一独立文件走完同队列
	mustWriteAt(t, fs, fUnaligned.NodeId, fUnaligned.Fh, 100, []byte("first-seq"))
	mustWriteAt(t, fs, fUnaligned.NodeId, fUnaligned.Fh, 1000, []byte("second-seq"))
	mustWriteAt(t, fs, fUnaligned.NodeId, fUnaligned.Fh, 2000, []byte("third-seq"))
	<-firstStarted

	mustWriteAt(t, fs, probeFile.NodeId, probeFile.Fh, 0, []byte("wake-and-scan"))
	requireStatus(t, "Fsync probeFile 确定 FIFO 队列已扫描跳过第二段", fuse.OK, fs.Fsync(nil, &fuse.FsyncIn{
		InHeader: fuse.InHeader{NodeId: probeFile.NodeId}, Fh: probeFile.Fh,
	}))
	concurrentDuringHold := maxConcurrent.Load()

	releaseFirstGate()
	requireStatus(t, "Fsync fUnaligned", fuse.OK, fs.Fsync(nil, &fuse.FsyncIn{
		InHeader: fuse.InHeader{NodeId: fUnaligned.NodeId}, Fh: fUnaligned.Fh,
	}))

	// Then
	require.Equal(t, int32(1), concurrentDuringHold, "同节点非对齐分片最大在途并发数: want=1, got=%d", concurrentDuringHold)
	offMu.Lock()
	require.Equal(t, []uint64{100, 1000, 2000}, observedOffsets, "同节点非对齐提交顺序: want=[100 1000 2000], got=%v", observedOffsets)
	offMu.Unlock()

	fs.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: probeFile.NodeId}, Fh: probeFile.Fh})
	fs.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: fUnaligned.NodeId}, Fh: fUnaligned.Fh})
}

func TestStageCoordinator_多未关闭文件触发配额背压_主动封口活跃块防死锁(t *testing.T) {
	// Given: StageSize=4 MiB；f1 与 f2 各持有 1.5 MiB 未封口活跃分片，均未调用 Flush/Release
	srv := newFakeStorage(t, withTTL(1.0, 1.0))
	fs := newTestRawFS(t, srv, withStageSize(4194304))
	f1 := mustCreateFile(t, fs, fuse.FUSE_ROOT_ID, "a.bin", syscall.O_RDWR, 0o644)
	f2 := mustCreateFile(t, fs, fuse.FUSE_ROOT_ID, "b.bin", syscall.O_RDWR, 0o644)
	f3 := mustCreateFile(t, fs, fuse.FUSE_ROOT_ID, "c.bin", syscall.O_RDWR, 0o644)

	mustWriteAt(t, fs, f1.NodeId, f1.Fh, 0, buildRepeatingBytes(1572864, '1'))
	mustWriteAt(t, fs, f2.NodeId, f2.Fh, 0, buildRepeatingBytes(1572864, '2'))

	// When: f3 写入 2 MiB 超出 4 MiB 上限，必须自动封口 f1/f2 并腾出配额完成写入
	mustWriteAt(t, fs, f3.NodeId, f3.Fh, 0, buildRepeatingBytes(2097152, '3'))
	requireStatus(t, "Fsync c.bin", fuse.OK, fs.Fsync(nil, &fuse.FsyncIn{InHeader: fuse.InHeader{NodeId: f3.NodeId}, Fh: f3.Fh}))

	// Then
	require.GreaterOrEqual(t, fs.Snapshot().Stage.BackpressureWaits, uint64(1),
		"多文件活跃块占满配额必须进入并解除背压等待: want>=1, got=%d", fs.Snapshot().Stage.BackpressureWaits)
	storedC, okC := srv.fileBytes(f3.NodeId)
	require.True(t, okC)
	require.Len(t, storedC, 2097152, "c.bin 落盘长度: want=2097152, got=%d", len(storedC))

	fs.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: f1.NodeId}, Fh: f1.Fh})
	fs.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: f2.NodeId}, Fh: f2.Fh})
	fs.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: f3.NodeId}, Fh: f3.Fh})
}

func TestStageCoordinator_背压等待期间Cancel关闭_返回EINTR且不泄漏配额(t *testing.T) {
	// Given
	notifier := newSpanStartNotifier()
	bpEntered := notifier.watch("stage.BackpressureWait")
	tp, _ := newTestTracerProvider(t, notifier)

	srv := newFakeStorage(t, withTTL(1.0, 1.0))
	bpGate, releaseBPGate := newTestGate()
	defer releaseBPGate()
	fullStarted := make(chan struct{})
	var gateOnce sync.Once
	srv.writeHook = func(_ context.Context, _ *storagev1.WriteRequest) (uint32, error) {
		gateOnce.Do(func() { close(fullStarted) })
		<-bpGate
		return 0, nil
	}

	fs := newTestRawFS(t, srv, withStageSize(4194304), withTracerProvider(tp))
	fb := mustCreateFile(t, fs, fuse.FUSE_ROOT_ID, "bp.bin", syscall.O_RDWR, 0o644)
	mustWriteAt(t, fs, fb.NodeId, fb.Fh, 0, buildRepeatingBytes(4194304, 'X'))
	<-fullStarted

	// When
	cancelCh := make(chan struct{})
	errCh := make(chan fuse.Status, 1)
	go func() {
		_, st := fs.Write(cancelCh, &fuse.WriteIn{
			InHeader: fuse.InHeader{NodeId: fb.NodeId},
			Fh:       fb.Fh,
			Offset:   4194304,
			Size:     4096,
		}, make([]byte, 4096))
		errCh <- st
	}()
	<-bpEntered
	close(cancelCh)

	// Then
	requireStatus(t, "BackpressureWait cancel 返回 EINTR", fuse.EINTR, waitUntilStatusChan(t, errCh, "Backpressure EINTR"))
	require.Equal(t, uint64(4194304), fs.Snapshot().Stage.UsedBytes, "取消等待后配额: want=4194304, got=%d", fs.Snapshot().Stage.UsedBytes)

	releaseBPGate()
	requireStatus(t, "Fsync bp.bin", fuse.OK, fs.Fsync(nil, &fuse.FsyncIn{InHeader: fuse.InHeader{NodeId: fb.NodeId}, Fh: fb.Fh}))
	require.Zero(t, fs.Snapshot().Stage.UsedBytes, "排空后配额: want=0, got=%d", fs.Snapshot().Stage.UsedBytes)
	fs.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: fb.NodeId}, Fh: fb.Fh})
}

func TestStageCoordinator_Unlink丢弃排队中分片_不产生WriteRPC浪费(t *testing.T) {
	// Given: 占满 8 个上传工人使新入队文件留在队列中
	srv := newFakeStorage(t, withTTL(1.0, 1.0))
	fs := newTestRawFS(t, srv, withDirectIO(false))
	gateBlockers, releaseBlockers := newTestGate()
	defer releaseBlockers()
	allWorkersBusy := occupyAllStageWorkers(t, fs, gateBlockers, srv)
	<-allWorkersBusy
	blockerWrites := srv.writeCalls.Load()

	// When
	unlinked := mustCreateFile(t, fs, fuse.FUSE_ROOT_ID, "discard-unlink.bin", syscall.O_RDWR, 0o644)
	mustWriteAt(t, fs, unlinked.NodeId, unlinked.Fh, 0, []byte("never-upload-unlink"))
	fs.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: unlinked.NodeId}, Fh: unlinked.Fh})
	unlinkSt := fs.Unlink(nil, &fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID}, "discard-unlink.bin")

	releaseBlockers()
	fs.OnUnmount()

	// Then
	requireStatus(t, "Unlink", fuse.OK, unlinkSt)
	require.Equal(t, blockerWrites, srv.writeCalls.Load(),
		"Unlink 丢弃排队分片后 Write RPC 次数: want=%d, got=%d", blockerWrites, srv.writeCalls.Load())
}

func TestStageCoordinator_OpenTrunc丢弃排队中分片_不产生WriteRPC浪费(t *testing.T) {
	// Given: 占满 8 个上传工人使新入队文件留在队列中
	srv := newFakeStorage(t, withTTL(1.0, 1.0))
	fs := newTestRawFS(t, srv, withDirectIO(false))
	gateBlockers, releaseBlockers := newTestGate()
	defer releaseBlockers()
	allWorkersBusy := occupyAllStageWorkers(t, fs, gateBlockers, srv)
	<-allWorkersBusy
	blockerWrites := srv.writeCalls.Load()

	// When
	truncFile := mustCreateFile(t, fs, fuse.FUSE_ROOT_ID, "discard-trunc.bin", syscall.O_RDWR, 0o644)
	mustWriteAt(t, fs, truncFile.NodeId, truncFile.Fh, 0, []byte("never-upload-trunc"))
	fs.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: truncFile.NodeId}, Fh: truncFile.Fh})
	var truncOpen fuse.OpenOut
	openSt := fs.Open(nil, &fuse.OpenIn{
		InHeader: fuse.InHeader{NodeId: truncFile.NodeId}, Flags: uint32(syscall.O_RDWR | syscall.O_TRUNC),
	}, &truncOpen)
	truncAttr := mustGetAttr(t, fs, truncFile.NodeId)
	fs.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: truncFile.NodeId}, Fh: truncOpen.Fh})

	releaseBlockers()
	fs.OnUnmount()

	// Then
	requireStatus(t, "Open O_TRUNC", fuse.OK, openSt)
	require.Zero(t, truncAttr.Size, "Open(O_TRUNC) 后尺寸: want=0, got=%d", truncAttr.Size)
	require.Equal(t, blockerWrites, srv.writeCalls.Load(),
		"Open(O_TRUNC) 丢弃排队分片后 Write RPC 次数: want=%d, got=%d", blockerWrites, srv.writeCalls.Load())
}

func TestStageCoordinator_OpenTrunc遇到未关闭活跃分片_立即本地丢弃未传数据(t *testing.T) {
	// Given
	srv := newFakeStorage(t, withTTL(1.0, 1.0))
	fs := newTestRawFS(t, srv, withDirectIO(false))
	f1 := mustCreateFile(t, fs, fuse.FUSE_ROOT_ID, "active-trunc.bin", syscall.O_RDWR, 0o644)
	mustWriteAt(t, fs, f1.NodeId, f1.Fh, 0, []byte("dirty-unsealed"))

	// When
	var f2 fuse.OpenOut
	requireStatus(t, "Open O_TRUNC", fuse.OK, fs.Open(nil, &fuse.OpenIn{
		InHeader: fuse.InHeader{NodeId: f1.NodeId}, Flags: uint32(syscall.O_RDWR | syscall.O_TRUNC),
	}, &f2))

	// Then
	require.Zero(t, srv.writeCalls.Load(), "O_TRUNC 不得上传已作废的活跃分片: want=0, got=%d", srv.writeCalls.Load())
	require.Zero(t, mustGetAttr(t, fs, f1.NodeId).Size, "O_TRUNC 后 GetAttr Size: want=0")
	require.Empty(t, mustReadAt(t, fs, f1.NodeId, f2.Fh, 0, 16), "O_TRUNC 后读取内容必须为空")

	fs.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: f1.NodeId}, Fh: f1.Fh})
	fs.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: f1.NodeId}, Fh: f2.Fh})
}

func TestRawFS_Link_等待源节点暂存分片落盘后再发LinkRPC(t *testing.T) {
	// Given
	srv := newFakeStorage(t, withTTL(1.0, 1.0))
	fs := newTestRawFS(t, srv, withDirectIO(false))
	src := mustCreateFile(t, fs, fuse.FUSE_ROOT_ID, "src.bin", syscall.O_RDWR, 0o644)
	mustWriteAt(t, fs, src.NodeId, src.Fh, 0, []byte("src-payload"))

	var linkVerified bool
	srv.linkHook = func(_ context.Context, req *storagev1.LinkRequest) error {
		data, ok := srv.fileBytes(req.GetIno())
		linkVerified = ok && bytes.Equal(data, []byte("src-payload"))
		return nil
	}

	// When
	var linkOut fuse.EntryOut
	st := fs.Link(nil, &fuse.LinkIn{
		InHeader: fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID}, Oldnodeid: src.NodeId,
	}, "src-hard.bin", &linkOut)

	// Then
	requireStatus(t, "Link", fuse.OK, st)
	require.True(t, linkVerified, "Link RPC 到达服务端前源节点暂存分片必须已完成上传: want=true, got=false")
	fs.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: src.NodeId}, Fh: src.Fh})
}

func TestRawFS_Rename_等待源与目标节点暂存落盘后再发RenameRPC(t *testing.T) {
	// Given
	srv := newFakeStorage(t, withTTL(1.0, 1.0))
	fs := newTestRawFS(t, srv, withDirectIO(false))
	src := mustCreateFile(t, fs, fuse.FUSE_ROOT_ID, "src.bin", syscall.O_RDWR, 0o644)
	dst := mustCreateFile(t, fs, fuse.FUSE_ROOT_ID, "dst.bin", syscall.O_RDWR, 0o644)
	mustWriteAt(t, fs, src.NodeId, src.Fh, 0, []byte("src-v2"))
	mustWriteAt(t, fs, dst.NodeId, dst.Fh, 0, []byte("dst-payload"))

	var renameVerified bool
	srv.renameHook = func(_ context.Context, _ *storagev1.RenameRequest) error {
		sData, sOK := srv.fileBytes(src.NodeId)
		dData, dOK := srv.fileBytes(dst.NodeId)
		renameVerified = sOK && dOK && bytes.Equal(sData, []byte("src-v2")) && bytes.Equal(dData, []byte("dst-payload"))
		return nil
	}

	// When
	st := fs.Rename(nil, &fuse.RenameIn{
		InHeader: fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID},
		Newdir:   fuse.FUSE_ROOT_ID,
	}, "src.bin", "dst.bin")

	// Then
	requireStatus(t, "Rename", fuse.OK, st)
	require.True(t, renameVerified, "Rename RPC 到达服务端前源与目标双节点暂存内容必须均已落盘: want=true, got=false")
	fs.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: src.NodeId}, Fh: src.Fh})
	fs.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: dst.NodeId}, Fh: dst.Fh})
}

func TestRawFS_StatFs_通过BarrierAll等待全部暂存落盘后再发StatFSRPC(t *testing.T) {
	// Given
	srv := newFakeStorage(t, withTTL(1.0, 1.0))
	fs := newTestRawFS(t, srv, withDirectIO(false))
	target := mustCreateFile(t, fs, fuse.FUSE_ROOT_ID, "statfs-bar.bin", syscall.O_RDWR, 0o644)
	mustWriteAt(t, fs, target.NodeId, target.Fh, 0, []byte("barrier-all"))

	var statfsVerified bool
	srv.statFSHook = func(_ context.Context, _ *storagev1.StatFSRequest) error {
		data, ok := srv.fileBytes(target.NodeId)
		statfsVerified = ok && bytes.Equal(data, []byte("barrier-all"))
		return nil
	}

	// When
	var stOut fuse.StatfsOut
	st := fs.StatFs(nil, &fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID}, &stOut)

	// Then
	requireStatus(t, "StatFs", fuse.OK, st)
	require.True(t, statfsVerified, "StatFS RPC 到达服务端前全部暂存分片必须已完成上传: want=true, got=false")
	fs.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: target.NodeId}, Fh: target.Fh})
}

func TestStageCoordinator_后台上传瞬态错误_保留分片退避重试并最终落盘一致(t *testing.T) {
	cases := []struct {
		name     string
		err      error
		failOnce int32
	}{
		{
			name:     "Unavailable两次失败后第三次退避成功",
			err:      connect.NewError(connect.CodeUnavailable, errors.New("transient")),
			failOnce: 2,
		},
		{
			name:     "DeadlineExceeded单次失败后退避成功",
			err:      connect.NewError(connect.CodeDeadlineExceeded, errors.New("timeout")),
			failOnce: 1,
		},
		{
			name:     "TransportUnexpectedEOF单次失败后退避成功",
			err:      io.ErrUnexpectedEOF,
			failOnce: 1,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// Given
			srv := newFakeStorage(t, withTTL(1.0, 1.0))
			var attempts atomic.Int32
			srv.writeHook = makeFailFirstAttemptsWriteHook(tc.failOnce, tc.err, &attempts)
			fs := newTestRawFS(t, srv, withDirectIO(false))
			f := mustCreateFile(t, fs, fuse.FUSE_ROOT_ID, "retry.bin", syscall.O_RDWR, 0o644)

			// When
			mustWriteAt(t, fs, f.NodeId, f.Fh, 0, []byte("retry-payload"))
			fsyncSt := fs.Fsync(nil, &fuse.FsyncIn{InHeader: fuse.InHeader{NodeId: f.NodeId}, Fh: f.Fh})

			// Then
			requireStatus(t, "Fsync retry", fuse.OK, fsyncSt)
			require.Equal(t, tc.failOnce+1, attempts.Load(), "重试总次数: want=%d, got=%d", tc.failOnce+1, attempts.Load())
			stored, ok := srv.fileBytes(f.NodeId)
			require.True(t, ok)
			requireBytesMatch(t, "瞬态重试落盘一致", []byte("retry-payload"), stored)
			fs.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: f.NodeId}, Fh: f.Fh})
		})
	}
}

func TestStageCoordinator_后台上传NotFound_丢弃分片并立即释放暂存配额(t *testing.T) {
	// Given
	srv := newFakeStorage(t, withTTL(1.0, 1.0))
	srv.writeHook = func(_ context.Context, _ *storagev1.WriteRequest) (uint32, error) {
		return 0, errnoError(connect.CodeNotFound, "ENOENT")
	}
	fs := newTestRawFS(t, srv)
	fnf := mustCreateFile(t, fs, fuse.FUSE_ROOT_ID, "gone.bin", syscall.O_RDWR, 0o644)

	// When
	mustWriteAt(t, fs, fnf.NodeId, fnf.Fh, 0, []byte("dropped-on-enoent"))
	fsyncSt := fs.Fsync(nil, &fuse.FsyncIn{InHeader: fuse.InHeader{NodeId: fnf.NodeId}, Fh: fnf.Fh})

	// Then
	requireStatus(t, "Fsync NotFound 忽略并收口", fuse.OK, fsyncSt)
	require.Zero(t, fs.Snapshot().Stage.UsedBytes, "NotFound 丢弃后 Stage 配额: want=0, got=%d", fs.Snapshot().Stage.UsedBytes)
	fs.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: fnf.NodeId}, Fh: fnf.Fh})
}

func TestStageCoordinator_后台上传不可恢复错误_清理分片释放配额并按句柄隔离消费Errseq(t *testing.T) {
	cases := []struct {
		name       string
		injectErr  error
		wantStatus fuse.Status
	}{
		{
			name:       "ENOSPC",
			injectErr:  errnoError(connect.CodeResourceExhausted, "ENOSPC"),
			wantStatus: fuse.Status(syscall.ENOSPC),
		},
		{
			name:       "Unavailable携带EBUSY业务ErrorInfo终止重试",
			injectErr:  errorWithReason(connect.CodeUnavailable, "busy", "EBUSY"),
			wantStatus: fuse.Status(syscall.EBUSY),
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// Given
			stageRoot := filepath.Join(t.TempDir(), "errseq-stage")
			sess, err := fusefs.OpenStageDirSession(stageRoot)
			require.NoError(t, err)
			t.Cleanup(func() { _ = sess.Close() })

			srv := newFakeStorage(t, withTTL(2.0, 2.0))
			var injectErr atomic.Bool
			injectErr.Store(true)
			srv.writeHook = makeToggleErrorWriteHook(&injectErr, tc.injectErr)
			fs := newTestRawFS(t, srv, withStageSession(sess))
			fhErr := mustCreateFile(t, fs, fuse.FUSE_ROOT_ID, "errseq.bin", syscall.O_RDWR, 0o644)
			secondFH := mustOpenFile(t, fs, fhErr.NodeId, syscall.O_RDWR)

			// When: 写满 4 MiB 整块立刻入队触发不可恢复失败
			mustWriteAt(t, fs, fhErr.NodeId, fhErr.Fh, 0, buildRepeatingBytes(4194304, 'E'))
			require.Eventually(t, func() bool {
				return fs.Snapshot().Stage.UploadErrors >= 1 && fs.Snapshot().Stage.UsedBytes == 0
			}, 5*time.Second, 5*time.Millisecond, "不可恢复错误未及时释放配额与计数")
			injectErr.Store(false)

			cleanFH := mustOpenFile(t, fs, fhErr.NodeId, syscall.O_RDWR)

			// Then
			require.Empty(t, listStageChunkFiles(t, sess.InstanceDir()), "不可恢复错误后必须删除 chunk-*.stage")
			requireStatus(t, "新开 cleanFH 不得继承历史错误", fuse.OK, fs.Flush(nil, &fuse.FlushIn{
				InHeader: fuse.InHeader{NodeId: fhErr.NodeId}, Fh: cleanFH.Fh,
			}))
			requireStatus(t, "fhErr 首次 Flush 必须消费错码", tc.wantStatus, fs.Flush(nil, &fuse.FlushIn{
				InHeader: fuse.InHeader{NodeId: fhErr.NodeId}, Fh: fhErr.Fh,
			}))
			requireStatus(t, "fhErr 二次 Fsync 错误游标已清零", fuse.OK, fs.Fsync(nil, &fuse.FsyncIn{
				InHeader: fuse.InHeader{NodeId: fhErr.NodeId}, Fh: fhErr.Fh,
			}))
			_, writeSt := fs.Write(nil, &fuse.WriteIn{
				InHeader: fuse.InHeader{NodeId: fhErr.NodeId}, Fh: secondFH.Fh, Offset: 0, Size: 2,
			}, []byte("ok"))
			requireStatus(t, "secondFH 独立消费同一历史错码", tc.wantStatus, writeSt)

			fs.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: fhErr.NodeId}, Fh: fhErr.Fh})
			fs.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: fhErr.NodeId}, Fh: secondFH.Fh})
			fs.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: fhErr.NodeId}, Fh: cleanFH.Fh})
		})
	}
}

func TestHostTmpfile_超过StageSize写入与打洞_本地独立落盘零RPC且不自锁(t *testing.T) {
	// Given
	stageRoot := filepath.Join(t.TempDir(), "tmp-local-stage")
	sess, err := fusefs.OpenStageDirSession(stageRoot)
	require.NoError(t, err)
	t.Cleanup(func() { _ = sess.Close() })

	srv := newFakeStorage(t, withTTL(1.0, 1.0))
	fs := newTestRawFS(t, srv, withStageSize(4194304), withStageSession(sess))

	var tmpOut fuse.CreateOut
	requireStatus(t, "Tmpfile", fuse.OK, fs.Tmpfile(nil, &fuse.CreateIn{
		InHeader: fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID}, Flags: uint32(syscall.O_RDWR), Mode: 0o600,
	}, &tmpOut))

	// When: 写 6 MiB（超出 4 MiB StageSize）并对中间 [1 MiB..5 MiB) 打洞，不 Link 仅 Flush 后销毁
	payload := buildRepeatingBytes(3145728, 'T')
	mustWriteAt(t, fs, tmpOut.NodeId, tmpOut.Fh, 0, payload)
	mustWriteAt(t, fs, tmpOut.NodeId, tmpOut.Fh, 3145728, payload)
	punchSt := fs.Fallocate(nil, &fuse.FallocateIn{
		InHeader: fuse.InHeader{NodeId: tmpOut.NodeId},
		Fh:       tmpOut.Fh,
		Offset:   1048576,
		Length:   4194304,
		Mode:     0x03, // FALLOC_FL_PUNCH_HOLE | FALLOC_FL_KEEP_SIZE
	})
	flushSt := fs.Flush(nil, &fuse.FlushIn{InHeader: fuse.InHeader{NodeId: tmpOut.NodeId}, Fh: tmpOut.Fh})
	activeDuringHold := fs.Snapshot().TmpFile.ActiveFiles
	assertOnlyLockEntryInDir(t, sess.InstanceDir())

	fs.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: tmpOut.NodeId}, Fh: tmpOut.Fh})
	fs.Forget(tmpOut.NodeId, 1)

	// Then
	requireStatus(t, "Tmpfile 局部打洞", fuse.OK, punchSt)
	requireStatus(t, "未具名 Tmpfile Flush 零RPC", fuse.OK, flushSt)
	require.Equal(t, 1, activeDuringHold, "未具名 TmpFile.ActiveFiles: want=1, got=%d", activeDuringHold)
	require.Zero(t, srv.mknodCalls.Load()+srv.writeCalls.Load()+srv.unlinkCalls.Load()+srv.reclaimOrphanCalls.Load(),
		"未 Link 匿名文件全生命周期服务端 RPC 总数: want=0")
	require.Zero(t, fs.Snapshot().TmpFile.ActiveFiles, "双清零后 TmpFile.ActiveFiles: want=0")
}

func TestHostTmpfile_Link具名化触发跳过空洞后台上传_在线读写不阻塞且Promotes计数准确(t *testing.T) {
	// Given
	stageRoot := filepath.Join(t.TempDir(), "tmp-promote-stage")
	sess, err := fusefs.OpenStageDirSession(stageRoot)
	require.NoError(t, err)
	t.Cleanup(func() { _ = sess.Close() })

	srv := newFakeStorage(t, withTTL(1.0, 1.0))
	uploadStarted := make(chan struct{})
	uploadGate, releaseUploadGate := newTestGate()
	defer releaseUploadGate()
	var gateOnce sync.Once
	var uploadedExtents [][2]uint64
	var extMu sync.Mutex
	srv.writeHook = func(_ context.Context, req *storagev1.WriteRequest) (uint32, error) {
		extMu.Lock()
		uploadedExtents = append(uploadedExtents, [2]uint64{req.GetOffset(), uint64(len(req.GetData()))})
		extMu.Unlock()
		gateOnce.Do(func() { close(uploadStarted) })
		<-uploadGate
		return 0, nil
	}

	fs := newTestRawFS(t, srv, withStageSize(4194304), withStageSession(sess))
	var tmpOut fuse.CreateOut
	requireStatus(t, "Tmpfile 创建", fuse.OK, fs.Tmpfile(nil, &fuse.CreateIn{
		InHeader: fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID}, Flags: uint32(syscall.O_RDWR), Mode: 0o600,
	}, &tmpOut))

	part := buildRepeatingBytes(3145728, 'T')
	mustWriteAt(t, fs, tmpOut.NodeId, tmpOut.Fh, 0, part)
	mustWriteAt(t, fs, tmpOut.NodeId, tmpOut.Fh, 3145728, part)
	requireStatus(t, "Tmpfile 局部打洞", fuse.OK, fs.Fallocate(nil, &fuse.FallocateIn{
		InHeader: fuse.InHeader{NodeId: tmpOut.NodeId},
		Fh:       tmpOut.Fh,
		Offset:   1048576,
		Length:   4194304,
		Mode:     0x03, // FALLOC_FL_PUNCH_HOLE | FALLOC_FL_KEEP_SIZE
	}))

	// When
	var entry fuse.EntryOut
	requireStatus(t, "Link 具名化", fuse.OK, fs.Link(nil, &fuse.LinkIn{
		InHeader: fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID}, Oldnodeid: tmpOut.NodeId,
	}, "promoted-large.bin", &entry))
	<-uploadStarted

	attrDuringUpload := mustGetAttr(t, fs, tmpOut.NodeId)
	boundaryRead := mustReadAt(t, fs, tmpOut.NodeId, tmpOut.Fh, 1048568, 16)

	releaseUploadGate()
	requireStatus(t, "Fsync 等待转正上传完毕", fuse.OK, fs.Fsync(nil, &fuse.FsyncIn{
		InHeader: fuse.InHeader{NodeId: tmpOut.NodeId}, Fh: tmpOut.Fh,
	}))

	// Then
	require.Equal(t, int64(1), srv.mknodCalls.Load(), "Link 具名化 Mknod 调用数: want=1, got=%d", srv.mknodCalls.Load())
	require.Equal(t, uint64(6291456), entry.Size, "EntryOut Size: want=6291456, got=%d", entry.Size)
	require.Equal(t, uint32(1), entry.Nlink, "EntryOut Nlink: want=1, got=%d", entry.Nlink)
	require.Equal(t, uint64(6291456), attrDuringUpload.Size, "上传期间 GetAttr Size: want=6291456, got=%d", attrDuringUpload.Size)
	requireBytesMatch(t, "上传挂起期间跨洞边界读取", []byte("TTTTTTTT\x00\x00\x00\x00\x00\x00\x00\x00"), boundaryRead)

	uploadedTotal := sumUploadedRanges(uploadedExtents, &extMu)
	require.Equal(t, uint64(2097152), uploadedTotal, "跳过 4 MiB 空洞后实际上传字节: want=2097152, got=%d", uploadedTotal)

	snap := fs.Snapshot()
	require.Zero(t, snap.TmpFile.ActiveFiles, "转正完毕后 ActiveFiles: want=0, got=%d", snap.TmpFile.ActiveFiles)
	require.Zero(t, snap.TmpFile.ResidentBytes, "转正完毕后 ResidentBytes: want=0, got=%d", snap.TmpFile.ResidentBytes)
	require.Equal(t, uint64(1), snap.TmpFile.Promotes, "TmpFile.Promotes: want=1, got=%d", snap.TmpFile.Promotes)
	require.Zero(t, snap.Stage.UsedBytes, "转正完毕后 Stage.UsedBytes: want=0, got=%d", snap.Stage.UsedBytes)

	fs.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: tmpOut.NodeId}, Fh: tmpOut.Fh})
	fs.Forget(tmpOut.NodeId, 2)
}

func TestMetrics_Prometheus导出Stage与Tmpfile新指标且移除WriteBehind与Tmpfile回滚指标(t *testing.T) {
	// Given
	srv := newFakeStorage(t, withTTL(1.0, 1.0))
	reg := prometheus.NewRegistry()
	metrics := fusefs.NewMetrics()
	reg.MustRegister(metrics)
	fs := newTestRawFS(t, srv, withStageSize(4194304), withMetrics(metrics))
	t.Cleanup(fs.OnUnmount)

	// When
	f := mustCreateFile(t, fs, fuse.FUSE_ROOT_ID, "metric.bin", syscall.O_RDWR, 0o644)
	mustWriteAt(t, fs, f.NodeId, f.Fh, 0, []byte("m"))
	requireStatus(t, "Fsync", fuse.OK, fs.Fsync(nil, &fuse.FsyncIn{InHeader: fuse.InHeader{NodeId: f.NodeId}, Fh: f.Fh}))
	fs.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: f.NodeId}, Fh: f.Fh})

	seenNames := gatherMetricNameSet(t, reg)

	// Then
	require.Equal(t, 8, fusefs.StageWorkers, "StageWorkers: want=8, got=%d", fusefs.StageWorkers)
	require.True(t, seenNames["bowl_client_stage_writes_total"])
	require.True(t, seenNames["bowl_client_stage_bytes_total"])
	require.True(t, seenNames["bowl_client_stage_upload_rpcs_total"])
	require.True(t, seenNames["bowl_client_stage_used_bytes"])
	require.True(t, seenNames["bowl_client_stage_capacity_bytes"])
	require.True(t, seenNames["bowl_client_tmpfile_active_files"])
	require.True(t, seenNames["bowl_client_tmpfile_resident_bytes"])
	require.True(t, seenNames["bowl_client_tmpfile_promotes_total"])
	require.False(t, seenNames["bowl_client_write_behind_writes_total"])
	require.False(t, seenNames["bowl_client_tmpfile_rollbacks_total"])
}
