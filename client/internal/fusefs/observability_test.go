package fusefs_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"

	"connectrpc.com/connect"
	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"

	"github.com/starsrc/bowl/client/internal/fusefs"
	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
	"github.com/starsrc/bowl/pkg/rpcerr"
)

type spanStartNotifier struct {
	mu      sync.Mutex
	signals map[string]chan struct{}
}

func newSpanStartNotifier() *spanStartNotifier {
	return &spanStartNotifier{signals: make(map[string]chan struct{})}
}

func (n *spanStartNotifier) watch(spanName string) <-chan struct{} {
	n.mu.Lock()
	defer n.mu.Unlock()
	ch := make(chan struct{})
	n.signals[spanName] = ch
	return ch
}

func (n *spanStartNotifier) OnStart(_ context.Context, s sdktrace.ReadWriteSpan) {
	n.mu.Lock()
	ch, ok := n.signals[s.Name()]
	if ok {
		delete(n.signals, s.Name())
	}
	n.mu.Unlock()
	if ok {
		close(ch)
	}
}

func (n *spanStartNotifier) OnEnd(sdktrace.ReadOnlySpan)      {}
func (n *spanStartNotifier) Shutdown(context.Context) error   { return nil }
func (n *spanStartNotifier) ForceFlush(context.Context) error { return nil }

func newTestTracerProvider(t *testing.T, extra ...sdktrace.SpanProcessor) (*sdktrace.TracerProvider, *tracetest.InMemoryExporter) {
	t.Helper()
	exp := tracetest.NewInMemoryExporter()
	opts := []sdktrace.TracerProviderOption{
		sdktrace.WithSyncer(exp),
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
	}
	for _, p := range extra {
		opts = append(opts, sdktrace.WithSpanProcessor(p))
	}
	tp := sdktrace.NewTracerProvider(opts...)
	t.Cleanup(func() { _ = tp.Shutdown(context.Background()) })
	return tp, exp
}

func findSpansByName(spans tracetest.SpanStubs, name string) []tracetest.SpanStub {
	var out []tracetest.SpanStub
	for i := range spans {
		if spans[i].Name == name {
			out = append(out, spans[i])
		}
	}
	return out
}

func findMetricFamily(t *testing.T, reg *prometheus.Registry, name string) *dto.MetricFamily {
	t.Helper()
	families, err := reg.Gather()
	require.NoError(t, err)
	for _, mf := range families {
		if mf.GetName() == name {
			return mf
		}
	}
	t.Fatalf("未采集到 Prometheus 指标族: want=%s", name)
	return nil
}

func findMetricWithoutLabels(t *testing.T, mf *dto.MetricFamily) *dto.Metric {
	t.Helper()
	for _, m := range mf.GetMetric() {
		if len(m.GetLabel()) == 0 {
			return m
		}
	}
	t.Fatalf("指标族 %s 缺少无标签样本", mf.GetName())
	return nil
}

func parseJSONLogMessages(t *testing.T, raw string) []map[string]any {
	t.Helper()
	var records []map[string]any
	for line := range strings.SplitSeq(strings.TrimSpace(raw), "\n") {
		if strings.TrimSpace(line) == "" {
			continue
		}
		var rec map[string]any
		require.NoError(t, json.Unmarshal([]byte(line), &rec))
		records = append(records, rec)
	}
	return records
}

func requireLogMsgPresent(t *testing.T, raw, wantMsg string) {
	t.Helper()
	for _, rec := range parseJSONLogMessages(t, raw) {
		if rec["msg"] == wantMsg {
			return
		}
	}
	t.Fatalf("JSON 日志中缺少预期事件: want=%s raw=%s", wantMsg, raw)
}

func mustAllSpansShareSameTraceID(t *testing.T, spans []trace.SpanContext, label string) trace.TraceID {
	t.Helper()
	require.NotEmpty(t, spans, "%s 必须至少捕获 1 条追踪上下文", label)
	first := spans[0].TraceID()
	require.True(t, first.IsValid(), "%s 首个 TraceID 必须有效", label)
	for i, sc := range spans {
		require.Equal(t, first, sc.TraceID(), "%s 第 %d 次 RPC TraceID 断裂", label, i)
	}
	return first
}

func TestMetrics与Prometheus与Snapshot_验证41个FUSE操作与26桶直方图及空安全性(t *testing.T) {
	t.Run("空接收者Collector安全性与Prometheus全量采集及文本和JSON序列化", func(t *testing.T) {
		// Given: nil Metrics 的 Collector 不崩溃，真实 RawFS 采集覆盖 Prometheus 与 Snapshot 全格式输出
		var nilMetrics *fusefs.Metrics
		emptyReg := prometheus.NewRegistry()
		require.NoError(t, emptyReg.Register(nilMetrics))
		emptyFamilies, gatherErr := emptyReg.Gather()
		require.NoError(t, gatherErr)
		require.Empty(t, emptyFamilies)

		srv := newFakeStorage(t, withTTL(1.0, 1.0))
		m := fusefs.NewMetrics()
		fs := newTestRawFS(t, srv, withMetrics(m), withStatsFile(true))
		fs.BindMountpoint("")
		fs.BindMountpoint("/mnt/nonexistent-test-fusefs")

		// When: 触发覆盖 Writev/Access/Lseek/CopyFileRange/Statx 等 41 个入口中的多种操作
		created := mustCreateFile(t, fs, fuse.FUSE_ROOT_ID, "obs.bin", syscall.O_RDWR, 0o644)
		_, writevSt := fs.Writev(nil, &fuse.WriteIn{InHeader: fuse.InHeader{NodeId: created.NodeId}, Fh: created.Fh, Size: 5}, [][]byte{[]byte("ab"), []byte("cde")})
		accSt := fs.Access(nil, &fuse.AccessIn{InHeader: fuse.InHeader{NodeId: created.NodeId}, Mask: fuse.R_OK})
		var lseekOut fuse.LseekOut
		lseekSt := fs.Lseek(nil, &fuse.LseekIn{InHeader: fuse.InHeader{NodeId: created.NodeId}, Fh: created.Fh}, &lseekOut)
		_, copySt := fs.CopyFileRange(nil, &fuse.CopyFileRangeIn{InHeader: fuse.InHeader{NodeId: created.NodeId}, FhIn: created.Fh})
		var statxOut fuse.StatxOut
		statxSt := fs.Statx(nil, &fuse.StatxIn{InHeader: fuse.InHeader{NodeId: created.NodeId}}, &statxOut)
		fs.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: created.NodeId}, Fh: created.Fh})

		// Then
		requireStatus(t, "Writev", fuse.OK, writevSt)
		requireStatus(t, "Access", fuse.Status(syscall.ENOSYS), accSt)
		requireStatus(t, "Lseek", fuse.Status(syscall.ENOSYS), lseekSt)
		requireStatus(t, "CopyFileRange", fuse.Status(syscall.ENOSYS), copySt)
		requireStatus(t, "Statx", fuse.Status(syscall.ENOSYS), statxSt)

		snap := fs.Snapshot()
		require.Len(t, snap.FUSE.Ops, fusefs.OpCount)
		require.Equal(t, uint64(1), snap.FUSE.Ops[fusefs.OpWritev].Calls)
		require.Equal(t, uint64(5), snap.FUSE.Ops[fusefs.OpWritev].Bytes)
		require.Zero(t, snap.FUSE.Ops[fusefs.OpWrite].Calls, "Writev 不得重复累加 OpWrite 计数")
		require.Equal(t, uint64(1), snap.FUSE.Ops[fusefs.OpAccess].Calls)
		require.Equal(t, uint64(1), snap.FUSE.Ops[fusefs.OpLseek].Calls)
		require.Equal(t, uint64(1), snap.FUSE.Ops[fusefs.OpCopyFileRange].Calls)
		require.Equal(t, uint64(1), snap.FUSE.Ops[fusefs.OpStatx].Calls)

		jsonBytes, jsonErr := snap.FormatJSON()
		require.NoError(t, jsonErr)
		require.Contains(t, string(jsonBytes), `"fuse"`)
		require.Contains(t, snap.FormatText(), "FUSE")
		require.Contains(t, snap.FormatStatsFile(), "bowl_client_fuse_ops_total")

		reg := prometheus.NewRegistry()
		require.NoError(t, reg.Register(fs.Collector()))
		durMF := findMetricFamily(t, reg, "bowl_client_fuse_op_duration_seconds")
		require.NotEmpty(t, durMF.GetMetric())
		require.Len(t, durMF.GetMetric()[0].GetHistogram().GetBucket(), 26, "FUSE 操作直方图必须精确为 26 桶")
	})

	t.Run("SetLkw真实阻塞等待更新锁等待计数与Prometheus直方图", func(t *testing.T) {
		// Given
		srv := newFakeStorage(t, withTTL(1.0, 1.0))
		fs := newTestRawFS(t, srv)
		f := mustCreateFile(t, fs, fuse.FUSE_ROOT_ID, "lock-hist.bin", syscall.O_RDWR, 0o644)
		h2 := mustOpenFile(t, fs, f.NodeId, syscall.O_RDWR)
		mustSetLock(t, fs, &fuse.LkIn{
			InHeader: fuse.InHeader{NodeId: f.NodeId}, Fh: f.Fh, Owner: 1,
			Lk: fuse.FileLock{Start: 0, End: 63, Typ: syscall.F_WRLCK, Pid: 1},
		})

		// When: 协程进入 SetLkw 等待队列后，由主协程 Flush 释放并等待返回
		done := make(chan fuse.Status, 1)
		go func() {
			done <- fs.SetLkw(nil, &fuse.LkIn{
				InHeader: fuse.InHeader{NodeId: f.NodeId}, Fh: h2.Fh, Owner: 2,
				Lk: fuse.FileLock{Start: 0, End: 63, Typ: syscall.F_WRLCK, Pid: 2},
			})
		}()
		waitUntilLockConflictsAtLeast(t, fs, 1)
		requireStatus(t, "Flush 释放锁", fuse.OK, fs.Flush(nil, &fuse.FlushIn{
			InHeader: fuse.InHeader{NodeId: f.NodeId}, Fh: f.Fh, LockOwner: 1,
		}))
		requireStatus(t, "SetLkw 阻塞获取成功", fuse.OK, waitUntilStatusChan(t, done, "SetLkw 统计唤醒"))

		// Then
		snap := fs.Snapshot()
		require.Equal(t, uint64(1), snap.Locks.Waits)
		require.Equal(t, uint64(1), snap.Locks.Conflicts)

		reg := prometheus.NewRegistry()
		require.NoError(t, reg.Register(fs.Collector()))
		lockHistMF := findMetricFamily(t, reg, "bowl_client_lock_wait_duration_seconds")
		m := findMetricWithoutLabels(t, lockHistMF)
		require.Equal(t, uint64(1), m.GetHistogram().GetSampleCount())

		fs.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: f.NodeId}, Fh: f.Fh, LockOwner: 1})
		fs.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: f.NodeId}, Fh: h2.Fh, LockOwner: 2})
	})
}

func TestVirtualStatsFile_校验stats与jfsStats只读读取越界EOF及全量写与元操作错误码(t *testing.T) {
	// Given
	srv := newFakeStorage(t, withTTL(1.0, 1.0))
	fs := newTestRawFS(t, srv, withStatsFile(true))

	// When: 1. Lookup 与 GetAttr .stats 与 .jfs.stats
	var eStats, eJFS fuse.EntryOut
	requireStatus(t, "Lookup .stats", fuse.OK, fs.Lookup(nil, &fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID}, ".stats", &eStats))
	requireStatus(t, "Lookup .jfs.stats", fuse.OK, fs.Lookup(nil, &fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID}, ".jfs.stats", &eJFS))
	require.Equal(t, fusefs.VirtualStatsIno, eStats.NodeId)
	require.Equal(t, fusefs.VirtualStatsIno, eJFS.NodeId)

	var attrOut fuse.AttrOut
	requireStatus(t, "GetAttr VirtualStatsIno", fuse.OK, fs.GetAttr(nil, &fuse.GetAttrIn{InHeader: fuse.InHeader{NodeId: fusefs.VirtualStatsIno}}, &attrOut))

	// 2. Open / Read / EOF Read / Release
	var openOut fuse.OpenOut
	requireStatus(t, "Open .stats", fuse.OK, fs.Open(nil, &fuse.OpenIn{InHeader: fuse.InHeader{NodeId: fusefs.VirtualStatsIno}, Flags: uint32(syscall.O_RDONLY)}, &openOut))
	require.NotZero(t, openOut.OpenFlags&fuse.FOPEN_DIRECT_IO)

	statsData := mustReadAt(t, fs, fusefs.VirtualStatsIno, openOut.Fh, 0, 64<<10)
	require.Contains(t, string(statsData), "bowl_client_fuse_ops_total")

	eofRes, eofSt := fs.Read(nil, &fuse.ReadIn{
		InHeader: fuse.InHeader{NodeId: fusefs.VirtualStatsIno},
		Fh:       openOut.Fh,
		Offset:   uint64(len(statsData)) + 4096,
		Size:     256,
	}, make([]byte, 256))
	requireStatus(t, "越界 EOF Read .stats", fuse.OK, eofSt)
	eofBytes, _ := eofRes.Bytes(nil)
	require.Empty(t, eofBytes)

	// Then: 3. 拦截所有非法写/目录/元操作，不得触达远端存储
	var dummyOpen fuse.OpenOut
	var dummyEntry fuse.EntryOut
	var dummyAttr fuse.AttrOut
	var dummyIoctl fuse.IoctlOut
	requireStatus(t, "OpenDir .stats", fuse.Status(syscall.ENOTDIR), fs.OpenDir(nil, &fuse.OpenIn{InHeader: fuse.InHeader{NodeId: fusefs.VirtualStatsIno}}, &dummyOpen))
	_, writeSt := fs.Write(nil, &fuse.WriteIn{InHeader: fuse.InHeader{NodeId: fusefs.VirtualStatsIno}, Fh: openOut.Fh, Size: 1}, []byte("x"))
	requireStatus(t, "Write .stats", fuse.EACCES, writeSt)
	requireStatus(t, "SetAttr .stats", fuse.EACCES, fs.SetAttr(nil, &fuse.SetAttrIn{SetAttrInCommon: fuse.SetAttrInCommon{InHeader: fuse.InHeader{NodeId: fusefs.VirtualStatsIno}, Valid: fuse.FATTR_MODE, Mode: 0o600}}, &dummyAttr))
	requireStatus(t, "Unlink .stats", fuse.EACCES, fs.Unlink(nil, &fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID}, ".stats"))
	requireStatus(t, "Mkdir .stats", fuse.EACCES, fs.Mkdir(nil, &fuse.MkdirIn{InHeader: fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID}, Mode: 0o755}, ".stats", &dummyEntry))
	requireStatus(t, "Symlink .stats", fuse.EACCES, fs.Symlink(nil, &fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID}, "/x", ".stats", &dummyEntry))
	requireStatus(t, "Link .stats", fuse.EACCES, fs.Link(nil, &fuse.LinkIn{InHeader: fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID}, Oldnodeid: fusefs.VirtualStatsIno}, "hard-stats", &dummyEntry))
	requireStatus(t, "Rename .stats", fuse.EACCES, fs.Rename(nil, &fuse.RenameIn{InHeader: fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID}, Newdir: fuse.FUSE_ROOT_ID}, ".stats", "other"))
	requireStatus(t, "Fallocate .stats", fuse.EACCES, fs.Fallocate(nil, &fuse.FallocateIn{InHeader: fuse.InHeader{NodeId: fusefs.VirtualStatsIno}, Fh: openOut.Fh, Offset: 0, Length: 4}))
	requireStatus(t, "SetXAttr .stats", fuse.EACCES, fs.SetXAttr(nil, &fuse.SetXAttrIn{InHeader: fuse.InHeader{NodeId: fusefs.VirtualStatsIno}}, "user.k", []byte("v")))
	requireStatus(t, "RemoveXAttr .stats", fuse.EACCES, fs.RemoveXAttr(nil, &fuse.InHeader{NodeId: fusefs.VirtualStatsIno}, "user.k"))
	_, gxSt := fs.GetXAttr(nil, &fuse.InHeader{NodeId: fusefs.VirtualStatsIno}, "user.k", make([]byte, 8))
	requireStatus(t, "GetXAttr .stats", fuse.Status(syscall.ENODATA), gxSt)
	lxSz, lxSt := fs.ListXAttr(nil, &fuse.InHeader{NodeId: fusefs.VirtualStatsIno}, make([]byte, 16))
	requireStatus(t, "ListXAttr .stats", fuse.OK, lxSt)
	require.Zero(t, lxSz)
	requireStatus(t, "Ioctl .stats", fuse.Status(syscall.ENOTTY), fs.Ioctl(nil, &fuse.IoctlIn{InHeader: fuse.InHeader{NodeId: fusefs.VirtualStatsIno}, Cmd: fusefs.FS_IOC_GETFLAGS}, nil, &dummyIoctl, make([]byte, 8)))

	fs.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: fusefs.VirtualStatsIno}, Fh: openOut.Fh})
	require.Zero(t, srv.lookupCalls.Load()+srv.getAttrCalls.Load()+srv.readCalls.Load(), "虚拟统计文件不得产生任何远端存储 RPC")
}

func makeWriteBehindSpanGateHook(barrierOffset uint64, allInflightEntered, barrierFlushEntered chan struct{}, inflightGate, barrierGate <-chan struct{}) func(context.Context, *storagev1.WriteRequest) (uint32, error) {
	var gateOnce, barOnce sync.Once
	var enteredCount atomic.Int32
	return func(_ context.Context, req *storagev1.WriteRequest) (uint32, error) {
		if req.GetOffset() >= barrierOffset {
			barOnce.Do(func() { close(barrierFlushEntered) })
			<-barrierGate
			return 0, nil
		}
		if enteredCount.Add(1) == fusefs.WriteBehindMaxInFlight {
			gateOnce.Do(func() { close(allInflightEntered) })
		}
		<-inflightGate
		return 0, nil
	}
}

func makePrefetchGateReadHook(targetOffset uint64, prefetchReached chan struct{}, prefetchGate <-chan struct{}) func(context.Context, *storagev1.ReadRequest) ([]byte, error) {
	var pfOnce sync.Once
	return func(_ context.Context, req *storagev1.ReadRequest) ([]byte, error) {
		if req.GetOffset() == targetOffset {
			pfOnce.Do(func() { close(prefetchReached) })
			<-prefetchGate
		}
		return nil, nil
	}
}

func makeTmpPrefixUnlinkFailHook() func(context.Context, *storagev1.UnlinkRequest) error {
	return func(_ context.Context, req *storagev1.UnlinkRequest) error {
		if strings.HasPrefix(string(req.GetName()), ".bowl-tmp-") {
			return errors.New("injected unlink failure")
		}
		return nil
	}
}

func TestTracing_验证OTel根Span状态语义与分页同Trace及异步流水线与孤儿回收链路(t *testing.T) {
	t.Run("fuse根Span覆盖EACCES报错与ENOENT非报错及ReadDirPlus分页同TraceID", func(t *testing.T) {
		// Given
		tp, exp := newTestTracerProvider(t)
		srv := newFakeStorage(t, withTTL(0, 0))
		seedLargeDirectory(srv, 1, 600)
		fs := newTestRawFS(t, srv, withTracerProvider(tp))

		// When: 1. ENOENT Lookup 不得标记 Span Status=Error；EACCES（只读挂载 Mkdir）必须标记 Error
		var missOut fuse.EntryOut
		requireStatus(t, "Lookup 缺失文件", fuse.ENOENT, fs.Lookup(nil, &fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID}, "absent.txt", &missOut))
		roFS := newTestRawFS(t, srv, withReadOnly(true), withTracerProvider(tp))
		requireStatus(t, "只读 Mkdir 返回 EROFS", fuse.Status(syscall.EROFS), roFS.Mkdir(nil, &fuse.MkdirIn{InHeader: fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID}, Mode: 0o755}, "ro", &missOut))

		// 2. 600 条目目录单次 ReadDirPlus 内部多页（512 + 88）共享唯一 TraceID
		var dh fuse.OpenOut
		requireStatus(t, "OpenDir", fuse.OK, fs.OpenDir(nil, &fuse.OpenIn{InHeader: fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID}}, &dh))
		buf := make([]byte, 128<<10)
		requireStatus(t, "ReadDirPlus 首页加载全目录", fuse.OK, fs.ReadDirPlus(nil, &fuse.ReadIn{
			InHeader: fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID}, Fh: dh.Fh, Offset: 0, Size: uint32(len(buf)),
		}, fuse.NewDirEntryList(buf, 0)))
		fs.ReleaseDir(&fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID}, Fh: dh.Fh})

		// Then
		spans := exp.GetSpans()
		lookupSpans := findSpansByName(spans, "fuse.Lookup")
		require.NotEmpty(t, lookupSpans)
		require.NotEqual(t, codes.Error, lookupSpans[0].Status.Code, "预期业务 ENOENT 不得把 Span 标红为 Error")

		mkdirSpans := findSpansByName(spans, "fuse.Mkdir")
		require.NotEmpty(t, mkdirSpans)
		require.Equal(t, codes.Error, mkdirSpans[0].Status.Code, "写入拒绝等系统错误必须将 Span 标记为 Error")

		rdpRPCs := srv.spansFor("ReadDirPlus")
		require.Len(t, rdpRPCs, 2, "600 条目应分 512+88 两页拉取")
		rdpTraceID := mustAllSpansShareSameTraceID(t, rdpRPCs, "ReadDirPlus 分页")
		rdpRootSpans := findSpansByName(spans, "fuse.ReadDirPlus")
		require.NotEmpty(t, rdpRootSpans)
		require.Equal(t, rdpTraceID, rdpRootSpans[0].SpanContext.TraceID())
	})

	t.Run("WriteBehind异步刷盘SpanLink与BackpressureWait及BarrierWait全链路观测", func(t *testing.T) {
		// Given: 使用 SpanProcessor.OnStart 精准观测阻塞入口，零 sleep 完成 BackpressureWait 与 BarrierWait
		notifier := newSpanStartNotifier()
		bpEntered := notifier.watch("write_behind.BackpressureWait")
		barrierEntered := notifier.watch("write_behind.BarrierWait")
		tp, exp := newTestTracerProvider(t, notifier)

		srv := newFakeStorage(t, withTTL(2.0, 2.0))
		inflightGate := make(chan struct{})
		allInflightEntered := make(chan struct{})
		barrierGate := make(chan struct{})
		barrierFlushEntered := make(chan struct{})
		srv.writeHook = makeWriteBehindSpanGateHook(uint64(6)*uint64(fusefs.WriteBehindChunkSize), allInflightEntered, barrierFlushEntered, inflightGate, barrierGate)

		fs := newTestRawFS(t, srv, withDirectIO(false), withTracerProvider(tp))
		f := mustCreateFile(t, fs, fuse.FUSE_ROOT_ID, "wb-trace.bin", syscall.O_RDWR, 0o644)
		chunkPayload := buildRepeatingBytes(fusefs.WriteBehindChunkSize, 'W')

		// When: 1. 通过 256 KiB 对齐小块写填满 4 个 4 MiB 槽位占满全部在途并发，
		// 再写入第 5 块触发 BackpressureWait，确认进入后放行。
		writeFullChunksViaAlignedFrames(t, fs, f.NodeId, f.Fh, 0, chunkPayload, fusefs.WriteBehindMaxInFlight)
		<-allInflightEntered

		fifthDone := make(chan fuse.Status, 1)
		go func() {
			fifthDone <- writeFullChunkViaAlignedFramesStatus(
				fs, f.NodeId, f.Fh,
				uint64(fusefs.WriteBehindMaxInFlight)*uint64(fusefs.WriteBehindChunkSize),
				chunkPayload,
			)
		}()
		<-bpEntered
		close(inflightGate)
		requireStatus(t, "第5块写入完成", fuse.OK, waitUntilStatusChan(t, fifthDone, "第5块 Write"))

		// 2. 再提交第 6、7 块使第 7 块在后台挂起（不持有 opMu），并发起 Read 触发 BarrierWait
		writeFullChunksViaAlignedFrames(t, fs, f.NodeId, f.Fh, uint64(5)*uint64(fusefs.WriteBehindChunkSize), chunkPayload, 2)
		<-barrierFlushEntered

		barrierDone := make(chan fuse.Status, 1)
		go func() {
			_, st := fs.Read(nil, &fuse.ReadIn{
				InHeader: fuse.InHeader{NodeId: f.NodeId},
				Fh:       f.Fh,
				Offset:   0,
				Size:     64,
			}, make([]byte, 64))
			barrierDone <- st
		}()
		<-barrierEntered

		close(barrierGate)
		requireStatus(t, "屏障读完成", fuse.OK, waitUntilStatusChan(t, barrierDone, "BarrierWait Read"))
		fs.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: f.NodeId}, Fh: f.Fh})

		// Then
		spans := exp.GetSpans()
		flushSpans := findSpansByName(spans, "write_behind.FlushChunk")
		bpSpans := findSpansByName(spans, "write_behind.BackpressureWait")
		barSpans := findSpansByName(spans, "write_behind.BarrierWait")
		require.NotEmpty(t, flushSpans, "必须记录 write_behind.FlushChunk 独立根 Span")
		require.NotEmpty(t, flushSpans[0].Links, "write_behind.FlushChunk 必须携带关联触发写入请求的 SpanLink")
		require.NotEmpty(t, bpSpans, "必须记录 write_behind.BackpressureWait")
		require.NotEmpty(t, barSpans, "必须记录 write_behind.BarrierWait")
	})

	t.Run("ReadAhead异步预取Prefetch与WaitSpan及孤儿Unlink到ReclaimOrphanSpan传播", func(t *testing.T) {
		// Given
		notifier := newSpanStartNotifier()
		waitEntered := notifier.watch("read_ahead.Wait")
		tp, exp := newTestTracerProvider(t, notifier)

		const size = 16 << 20
		srv := newFakeStorage(t, withTTL(1.0, 1.0))
		ino := srv.seedFile(1, "ra-trace.bin", 0o644, buildSeededBytes(size, 0x29))

		prefetchReached := make(chan struct{})
		prefetchGate := make(chan struct{})
		srv.readHook = makePrefetchGateReadHook(fusefs.MaxRPCChunkSize, prefetchReached, prefetchGate)

		fs := newTestRawFS(t, srv, withDirectIO(false), withTracerProvider(tp))
		opened := mustOpenFile(t, fs, ino, syscall.O_RDWR)

		// When: 1. 在首窗口（0..4MiB）连续步进读以稳定触发 offset=4MiB 的异步预取，
		// 待 Prefetch 抵达 offset=4MiB 并挂起后，读 offset=4MiB 精准进入 read_ahead.Wait 阻塞等待。
		_ = mustReadAt(t, fs, ino, opened.Fh, 0, 512<<10)
		_ = mustReadAt(t, fs, ino, opened.Fh, 512<<10, 512<<10)
		_ = mustReadAt(t, fs, ino, opened.Fh, 1024<<10, 512<<10)
		<-prefetchReached

		waitReadDone := make(chan []byte, 1)
		go func() {
			waitReadDone <- mustReadAt(t, fs, ino, opened.Fh, fusefs.MaxRPCChunkSize, 4096)
		}()
		<-waitEntered
		close(prefetchGate)
		require.Len(t, <-waitReadDone, 4096)

		// 2. Unlink 后 Release 孤儿回收传播有效的 TraceID
		requireStatus(t, "Unlink 打开文件", fuse.OK, fs.Unlink(nil, &fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID}, "ra-trace.bin"))
		fs.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: ino}, Fh: opened.Fh})

		// Then
		spans := exp.GetSpans()
		pfSpans := findSpansByName(spans, "read_ahead.Prefetch")
		waitSpans := findSpansByName(spans, "read_ahead.Wait")
		require.NotEmpty(t, pfSpans, "必须记录 read_ahead.Prefetch 根 Span")
		require.NotEmpty(t, pfSpans[0].Links, "read_ahead.Prefetch 必须保留 SpanLink")
		require.NotEmpty(t, waitSpans, "必须记录 read_ahead.Wait 子 Span")

		reclaimCtxs := srv.spansFor("ReclaimOrphan")
		mustAllSpansShareSameTraceID(t, reclaimCtxs, "孤儿回收 ReclaimOrphan")
	})
}

func writeFullChunkViaAlignedFramesStatus(fs *fusefs.RawFS, ino, fh, startOffset uint64, chunk []byte) fuse.Status {
	const frameSize = 256 << 10
	for off := 0; off < len(chunk); off += frameSize {
		end := min(off+frameSize, len(chunk))
		n, st := fs.Write(nil, &fuse.WriteIn{
			InHeader: fuse.InHeader{NodeId: ino},
			Fh:       fh,
			Offset:   startOffset + uint64(off),
			Size:     uint32(end - off),
		}, chunk[off:end])
		if st != fuse.OK {
			return st
		}
		if int(n) != end-off {
			return fuse.EIO
		}
	}
	return fuse.OK
}

func writeFullChunksViaAlignedFrames(t *testing.T, fs *fusefs.RawFS, ino, fh, startOffset uint64, chunk []byte, count int) {
	t.Helper()
	for i := range count {
		st := writeFullChunkViaAlignedFramesStatus(fs, ino, fh, startOffset+uint64(i)*uint64(len(chunk)), chunk)
		requireStatus(t, "按 256 KiB 对齐帧写入整块", fuse.OK, st)
	}
}

type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

func TestStructuredLogging_验证unmountFsyncFailed与orphanBacklogged与tmpfileRollbackFailed(t *testing.T) {
	t.Run("真实slogJSON记录unmount_fsync_failed与orphan_reclaim_backlogged与tmpfile_rollback_failed", func(t *testing.T) {
		// Given
		var logBuf syncBuffer
		logger := slog.New(slog.NewJSONHandler(&logBuf, &slog.HandlerOptions{Level: slog.LevelWarn}))
		srv := newFakeStorage(t, withTTL(1.0, 1.0))

		// 1. 构造 OnUnmount 时 Fsync 失败 -> 记录 unmount_fsync_failed
		srv.fsyncHook = func(context.Context, *storagev1.FsyncRequest) error {
			return errnoError(connect.CodeInternal, rpcerr.ReasonIO)
		}
		fs1 := newTestRawFS(t, srv, withDirectIO(false), withLogger(logger))
		dirty := mustCreateFile(t, fs1, fuse.FUSE_ROOT_ID, "dirty-fail.txt", syscall.O_RDWR, 0o644)
		mustWriteAt(t, fs1, dirty.NodeId, dirty.Fh, 0, []byte("fail-on-unmount"))
		fs1.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: dirty.NodeId}, Fh: dirty.Fh})
		fs1.OnUnmount()

		// 2. 构造孤儿回收失败且阈值=1 -> 记录 orphan_reclaim_backlogged
		srv.fsyncHook = nil
		srv.reclaimOrphanHook = func(context.Context, *storagev1.ReclaimOrphanRequest) error {
			return errnoError(connect.CodeInternal, rpcerr.ReasonIO)
		}
		fs2 := newTestRawFS(t, srv, withOrphanThreshold(1), withLogger(logger))
		orp := mustCreateFile(t, fs2, fuse.FUSE_ROOT_ID, "orp-fail.txt", syscall.O_RDWR, 0o644)
		requireStatus(t, "Unlink", fuse.OK, fs2.Unlink(nil, &fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID}, "orp-fail.txt"))
		fs2.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: orp.NodeId}, Fh: orp.Fh})

		// 3. 构造匿名文件 Flush 上云时临时 Unlink 与回滚 Unlink 双失败 -> 记录 tmpfile_rollback_failed
		srv.reclaimOrphanHook = nil
		srv.unlinkHook = makeTmpPrefixUnlinkFailHook()
		fs3 := newTestRawFS(t, srv, withLogger(logger))
		var tmpOut fuse.CreateOut
		requireStatus(t, "Tmpfile", fuse.OK, fs3.Tmpfile(nil, &fuse.CreateIn{
			InHeader: fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID}, Flags: uint32(syscall.O_RDWR), Mode: 0o600,
		}, &tmpOut))
		mustWriteAt(t, fs3, tmpOut.NodeId, tmpOut.Fh, 0, []byte("rollback-me"))
		flushSt := fs3.Flush(nil, &fuse.FlushIn{InHeader: fuse.InHeader{NodeId: tmpOut.NodeId}, Fh: tmpOut.Fh})
		require.NotEqual(t, fuse.OK, flushSt)
		fs3.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: tmpOut.NodeId}, Fh: tmpOut.Fh})
		fs3.Forget(tmpOut.NodeId, 1)

		// Then
		out := logBuf.String()
		requireLogMsgPresent(t, out, "unmount_fsync_failed")
		requireLogMsgPresent(t, out, "orphan_reclaim_backlogged")
		requireLogMsgPresent(t, out, "tmpfile_rollback_failed")
	})
}
