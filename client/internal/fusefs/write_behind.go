package fusefs

import (
	"context"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"
	"google.golang.org/protobuf/types/known/timestamppb"

	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
)

const (
	// WriteBehindChunkSize 定义客户端顺序写聚合槽位上限 (B* = 4 MiB = MaxRPCChunkSize)。
	WriteBehindChunkSize = MaxRPCChunkSize

	// WriteBehindMaxInFlight 定义单文件最大在途并发 Write RPC 数 (W* = 4，峰值 16 MiB 在途)。
	WriteBehindMaxInFlight = 4

	// WriteBehindBlockAlign 与服务端 store.BlobSize (256 KiB) 严格一致：
	// 仅当聚合起始偏移按 256 KiB 对齐且异步分片为 4 MiB 整倍数时才允许并发在途，
	// 从数学上保证任意两个在途 Write RPC 覆盖完全不相交的 256 KiB 物理块区间。
	WriteBehindBlockAlign = 256 * 1024

	// WriteBehindActivateBytes 为大块流式写入（> 256 KiB，如 1 MiB FUSE 分片）激活异步并发流水线前的同步探针字节阈值 (2 MiB)：
	// 首段同步写入直接透传验证 Epoch 栅栏、Immutable/Append、KILLPRIV 与小盘配额；
	// 小块写入（<= 256 KiB）在完成首笔同步探针或经 Create 预热（SeedCreatedInode）后直接合并进内存 stageBuf。
	WriteBehindActivateBytes = 2 * 1024 * 1024

	// writeBehindRetainedBufs 为常驻有界 noscan 4 MiB 缓冲槽数（8 x 4 MiB = 32 MiB，免受 GC 周期驱逐）。
	writeBehindRetainedBufs = 8

	// maxStageSpanLinks 限制单个 4 MiB 聚合槽最多挂载的采样写入者 SpanLink 数。
	maxStageSpanLinks = 32
)

var (
	writeBehindFreeCh  = make(chan *[WriteBehindChunkSize]byte, writeBehindRetainedBufs)
	writeBehindBufPool = sync.Pool{
		New: func() any {
			return new([WriteBehindChunkSize]byte)
		},
	}
)

func acquireWriteBehindBuf() *[WriteBehindChunkSize]byte {
	select {
	case buf := <-writeBehindFreeCh:
		return buf
	default:
	}
	buf, _ := writeBehindBufPool.Get().(*[WriteBehindChunkSize]byte)
	if buf == nil {
		return new([WriteBehindChunkSize]byte)
	}
	return buf
}

func releaseWriteBehindBuf(buf *[WriteBehindChunkSize]byte) {
	if buf == nil {
		return
	}
	select {
	case writeBehindFreeCh <- buf:
	default:
		writeBehindBufPool.Put(buf)
	}
}

// ChunkWriter 定义向底层存储服务提交单块写入的端口函数。
type ChunkWriter func(ctx context.Context, ino, offset uint64, data []byte) (uint32, *storagev1.Inode, error)

// WriteBehindHooks 定义内存缓冲与异步/同步刷盘完成后的本地状态同步钩子。
type WriteBehindHooks struct {
	OnStage  func(nodeID, cacheEpoch, endOffset uint64, ts *timestamppb.Timestamp)
	OnCommit func(nodeID, ino, cacheEpoch uint64, inode *storagev1.Inode, minSize uint64, refreshAttr bool)
	OnError  func(nodeID uint64)
}

type inodeWriteBehind struct {
	// opMu 串行化同一 Inode 的前台 Write / Flush / Barrier / Reset / Remove 操作，
	// 杜绝 st.cond.Wait() 释放 st.mu 期间被并发前台请求穿插篡改聚合槽或流偏移；
	// 后台在途提交协程绝不获取 opMu，严格维持 opMu -> {mu, c.mu} -> c.seqMu 单向无环锁序。
	opMu           sync.Mutex
	mu             sync.Mutex
	cond           *sync.Cond
	ino            uint64
	removed        bool
	nodeID         uint64
	cacheEpoch     uint64
	probed         bool
	probedBytes    uint64
	expectedOffset uint64
	maxStagedEnd   uint64
	hasStream      bool
	streamGen      uint64
	stageBuf       *[WriteBehindChunkSize]byte
	stageOffset    uint64
	stageLen       int
	stageGen       uint64
	stageLinks     []trace.Link
	inflight       int
	lastCommitted  *storagev1.Inode
	errGen         uint64
	errSeq         uint64
	lastErr        error
	defaultSeenSeq uint64
}

func newInodeWriteBehind(ino uint64) *inodeWriteBehind {
	st := &inodeWriteBehind{
		ino:       ino,
		streamGen: 1,
	}
	st.cond = sync.NewCond(&st.mu)
	return st
}

func compareProtoTimestamp(a, b *timestamppb.Timestamp) int {
	var aSec, bSec int64
	var aNano, bNano int32
	if a != nil {
		aSec, aNano = a.GetSeconds(), a.GetNanos()
	}
	if b != nil {
		bSec, bNano = b.GetSeconds(), b.GetNanos()
	}
	if aSec != bSec {
		if aSec > bSec {
			return 1
		}
		return -1
	}
	if aNano != bNano {
		if aNano > bNano {
			return 1
		}
		return -1
	}
	return 0
}

func isNotOlderCommittedInode(prev, cand *storagev1.Inode) bool {
	if cand == nil || cand.GetAttr() == nil {
		return false
	}
	if prev == nil || prev.GetAttr() == nil {
		return true
	}
	candAttr := cand.GetAttr()
	prevAttr := prev.GetAttr()
	// 在同一连续顺序写世代（streamGen）内无截断介入，服务端文件尺寸单调不减：
	// 更大 Size 必定晚于更小 Size 在服务端提交；Size 相同时（乱序先写高偏移再补低偏移）取时间戳更新者。
	if candAttr.GetSize() != prevAttr.GetSize() {
		return candAttr.GetSize() > prevAttr.GetSize()
	}
	if cmp := compareProtoTimestamp(candAttr.GetCtime(), prevAttr.GetCtime()); cmp != 0 {
		return cmp > 0
	}
	return compareProtoTimestamp(candAttr.GetMtime(), prevAttr.GetMtime()) >= 0
}

func (st *inodeWriteBehind) resetStreamLocked() {
	st.hasStream = false
	st.probed = false
	st.probedBytes = 0
	st.maxStagedEnd = 0
	st.streamGen++
	st.lastCommitted = nil
}

func (st *inodeWriteBehind) recordStageLinkLocked(ctx context.Context) {
	if ctx == nil || len(st.stageLinks) >= maxStageSpanLinks {
		return
	}
	sc := trace.SpanContextFromContext(ctx)
	if !sc.IsValid() || !sc.IsSampled() {
		return
	}
	for i := range st.stageLinks {
		if st.stageLinks[i].SpanContext.SpanID() == sc.SpanID() {
			return
		}
	}
	st.stageLinks = append(st.stageLinks, trace.Link{SpanContext: sc})
}

func (st *inodeWriteBehind) discardStageLocked(pendingCount *atomic.Int64) {
	if st.stageBuf != nil {
		releaseWriteBehindBuf(st.stageBuf)
		st.stageBuf = nil
		pendingCount.Add(-1)
	}
	st.stageOffset = 0
	st.stageLen = 0
	st.stageGen = 0
	st.stageLinks = nil
}

func (st *inodeWriteBehind) waitInflightObservedLocked(ctx context.Context, c *WriteBehindCoordinator) {
	if st.inflight <= 0 {
		return
	}
	if ctx == nil {
		ctx = context.Background()
	}
	waitStart := time.Now()
	var waitSpan trace.Span = noopSpanInstance
	if tracer, ok := c.activeTracer(); ok {
		_, waitSpan = tracer.Start(ctx, "write_behind.BarrierWait",
			trace.WithAttributes(
				attribute.Int64("write_behind.ino", int64(st.ino)),
				attribute.Int("write_behind.inflight", st.inflight),
			),
		)
	}
	for st.inflight > 0 {
		st.cond.Wait()
	}
	waitSpan.End()
	c.metrics.Load().observeWriteBehindBarrierWait(time.Since(waitStart))
}

func (st *inodeWriteBehind) hasUnseenErrLocked(c *WriteBehindCoordinator, fh uint64) bool {
	if st.errSeq == 0 {
		return false
	}
	if fh == 0 {
		return st.defaultSeenSeq < st.errSeq
	}
	return c.handleSeenSeq(fh) < st.errSeq
}

func (st *inodeWriteBehind) consumeErrLocked(c *WriteBehindCoordinator, fh uint64) error {
	if st.errSeq == 0 {
		return nil
	}
	if fh == 0 {
		if st.defaultSeenSeq >= st.errSeq {
			return nil
		}
		st.defaultSeenSeq = st.errSeq
		st.resetStreamLocked()
		return st.lastErr
	}
	if !c.advanceHandleSeenSeq(fh, st.errSeq) {
		return nil
	}
	st.resetStreamLocked()
	return st.lastErr
}

func (st *inodeWriteBehind) dispatchLocked(ctx context.Context, c *WriteBehindCoordinator) bool {
	var bpStart time.Time
	var bpSpan trace.Span = noopSpanInstance
	bpWaiting := false
	finishBP := func() {
		if bpWaiting {
			bpSpan.End()
			c.metrics.Load().observeWriteBehindBackpressureWait(time.Since(bpStart))
			bpWaiting = false
		}
	}

	for {
		if st.stageBuf == nil || st.stageLen == 0 {
			finishBP()
			st.discardStageLocked(&c.pendingCount)
			return true
		}
		if st.stageGen != st.streamGen {
			finishBP()
			st.discardStageLocked(&c.pendingCount)
			return false
		}
		if st.inflight < WriteBehindMaxInFlight {
			finishBP()
			break
		}
		if !bpWaiting {
			bpWaiting = true
			bpStart = time.Now()
			if tracer, ok := c.activeTracer(); ok {
				parent := ctx
				if parent == nil {
					parent = context.Background()
				}
				_, bpSpan = tracer.Start(parent, "write_behind.BackpressureWait",
					trace.WithAttributes(
						attribute.Int64("write_behind.ino", int64(st.ino)),
						attribute.Int("write_behind.inflight", st.inflight),
					),
				)
			}
		}
		st.cond.Wait()
	}

	buf := st.stageBuf
	chunkOff := st.stageOffset
	chunkLen := st.stageLen
	links := st.stageLinks
	ino := st.ino
	nodeID := st.nodeID
	cacheEpoch := st.cacheEpoch
	gen := st.stageGen
	writer := c.writer
	hooks := c.hooks

	// 所有权从 stageBuf 转移至在途协程，总挂起计数保持不变，在协程收口递减 inflight 时再减 1。
	st.stageBuf = nil
	st.stageOffset = 0
	st.stageLen = 0
	st.stageGen = 0
	st.stageLinks = nil
	st.inflight++
	c.metrics.Load().addWriteBehindInflight(1)

	go func() {
		flushBaseCtx := context.Background()
		var flushSpan trace.Span = noopSpanInstance
		if tracer, ok := c.activeTracer(); ok {
			opts := make([]trace.SpanStartOption, 0, 3)
			opts = append(opts,
				trace.WithNewRoot(),
				trace.WithAttributes(
					attribute.Int64("write_behind.ino", int64(ino)),
					attribute.Int64("write_behind.offset", int64(chunkOff)),
					attribute.Int("write_behind.length", chunkLen),
				),
			)
			if len(links) > 0 {
				opts = append(opts, trace.WithLinks(links...))
			}
			flushBaseCtx, flushSpan = tracer.Start(flushBaseCtx, "write_behind.FlushChunk", opts...)
		}

		rpcCtx, cancel := context.WithTimeout(flushBaseCtx, DefaultRPCTimeout)
		written, respInode, err := writer(rpcCtx, ino, chunkOff, buf[:chunkLen:chunkLen])
		cancel()
		releaseWriteBehindBuf(buf)

		if err == nil && written < uint32(chunkLen) {
			err = syscall.EIO
		}
		if err != nil {
			flushSpan.RecordError(err)
			flushSpan.SetStatus(codes.Error, err.Error())
		} else {
			flushSpan.SetAttributes(attribute.Int("write_behind.written_bytes", int(written)))
		}
		flushSpan.End()

		m := c.metrics.Load()
		m.incWriteBehindFlushRPC(err != nil)
		m.addWriteBehindInflight(-1)

		// 在同一临界区内串行完成水位比较、本地缓存/脏集回填与 inflight 递减唤醒，
		// 彻底杜绝锁外并发 OnCommit 超车导致文件尺寸或时间戳倒退。
		st.mu.Lock()
		if err != nil {
			if st.errGen != gen {
				st.errGen = gen
				st.lastErr = err
				st.errSeq = c.errSeqGen.Add(1)
			}
			if st.streamGen == gen {
				st.resetStreamLocked()
			}
			if hooks.OnError != nil {
				hooks.OnError(nodeID)
			}
		} else {
			refreshAttr := false
			var minSize uint64
			if gen == st.streamGen {
				minSize = st.maxStagedEnd
				if isNotOlderCommittedInode(st.lastCommitted, respInode) {
					st.lastCommitted = cloneInode(respInode)
					refreshAttr = true
				}
			}
			if hooks.OnCommit != nil {
				hooks.OnCommit(nodeID, ino, cacheEpoch, respInode, minSize, refreshAttr)
			}
		}
		st.inflight--
		c.pendingCount.Add(-1)
		st.cond.Broadcast()
		st.mu.Unlock()
	}()

	return true
}

// flushStageSyncLocked 在调用方当前协程内将未满 4 MiB 或未 256 KiB 对齐的聚合槽合并为单次同步 Write RPC 刷出，
// 零后台协程派生、零条件变量唤醒开销；刷出期间维持 inflight 与 pendingCount，保证并发 HasPending/BarrierInode 严格等待。
// 调用前提：调用方同时持有 st.opMu 与 st.mu。
func (st *inodeWriteBehind) flushStageSyncLocked(ctx context.Context, c *WriteBehindCoordinator) bool {
	if st.stageBuf == nil || st.stageLen == 0 {
		st.discardStageLocked(&c.pendingCount)
		return true
	}
	if st.stageGen != st.streamGen {
		st.discardStageLocked(&c.pendingCount)
		return false
	}
	if st.stageOffset%WriteBehindBlockAlign != 0 && st.inflight > 0 {
		st.waitInflightObservedLocked(ctx, c)
		if st.stageBuf == nil || st.stageLen == 0 {
			st.discardStageLocked(&c.pendingCount)
			return true
		}
		if st.stageGen != st.streamGen {
			st.discardStageLocked(&c.pendingCount)
			return false
		}
	}

	buf := st.stageBuf
	chunkOff := st.stageOffset
	chunkLen := st.stageLen
	ino := st.ino
	nodeID := st.nodeID
	cacheEpoch := st.cacheEpoch
	gen := st.stageGen
	writer := c.writer
	hooks := c.hooks

	st.stageBuf = nil
	st.stageOffset = 0
	st.stageLen = 0
	st.stageGen = 0
	st.stageLinks = nil
	st.inflight++
	c.metrics.Load().addWriteBehindInflight(1)
	st.mu.Unlock()

	baseCtx := context.Background()
	var dlCancel context.CancelFunc
	if ctx != nil {
		baseCtx = context.WithoutCancel(ctx)
		// 保留外层传入的超时截止时间（如 OnUnmount 总刷盘预算），避免单次阻塞撑满 30 秒。
		if deadline, ok := ctx.Deadline(); ok {
			baseCtx, dlCancel = context.WithDeadline(baseCtx, deadline)
		}
	}
	rpcCtx, cancel := context.WithTimeout(baseCtx, DefaultRPCTimeout)
	written, respInode, err := writer(rpcCtx, ino, chunkOff, buf[:chunkLen:chunkLen])
	cancel()
	if dlCancel != nil {
		dlCancel()
	}
	releaseWriteBehindBuf(buf)

	if err == nil && written < uint32(chunkLen) {
		err = syscall.EIO
	}
	m := c.metrics.Load()
	m.incWriteBehindFlushRPC(err != nil)
	m.addWriteBehindInflight(-1)

	st.mu.Lock()
	if err != nil {
		if st.errGen != gen {
			st.errGen = gen
			st.lastErr = err
			st.errSeq = c.errSeqGen.Add(1)
		}
		if st.streamGen == gen {
			st.resetStreamLocked()
		}
		if hooks.OnError != nil {
			hooks.OnError(nodeID)
		}
	} else {
		refreshAttr := false
		var minSize uint64
		if gen == st.streamGen {
			minSize = st.maxStagedEnd
			if isNotOlderCommittedInode(st.lastCommitted, respInode) {
				st.lastCommitted = cloneInode(respInode)
				refreshAttr = true
			}
		}
		if hooks.OnCommit != nil {
			hooks.OnCommit(nodeID, ino, cacheEpoch, respInode, minSize, refreshAttr)
		}
	}
	st.inflight--
	c.pendingCount.Add(-1)
	st.cond.Broadcast()
	return err == nil
}

func (st *inodeWriteBehind) flushFullStageLocked(ctx context.Context, c *WriteBehindCoordinator) bool {
	if st.stageLen == WriteBehindChunkSize && st.stageOffset%WriteBehindBlockAlign == 0 {
		return st.dispatchLocked(ctx, c)
	}
	return st.flushStageSyncLocked(ctx, c)
}

func (st *inodeWriteBehind) barrierLocked(ctx context.Context, c *WriteBehindCoordinator) {
	if st.stageBuf != nil || st.stageLen > 0 {
		if st.stageLen == WriteBehindChunkSize && st.stageOffset%WriteBehindBlockAlign == 0 {
			st.dispatchLocked(ctx, c)
		} else {
			st.flushStageSyncLocked(ctx, c)
		}
	}
	st.waitInflightObservedLocked(ctx, c)
}

// WriteBehindCoordinator 与 ReadAheadCoordinator 对称，负责将 Linux FUSE 拆碎的连续小块写（<= 1 MiB）
// 聚合成 B*=4 MiB 分片并以有界并发度 W*=4 流水线提交至服务端，同时在 Flush/Fsync/Read/GetAttr 等屏障处排空。
type WriteBehindCoordinator struct {
	mu             sync.RWMutex
	inodes         map[uint64]*inodeWriteBehind
	pendingCount   atomic.Int64
	errSeqGen      atomic.Uint64
	seqMu          sync.Mutex
	handleSeq      map[uint64]uint64
	writer         ChunkWriter
	hooks          WriteBehindHooks
	metrics        atomic.Pointer[Metrics]
	tracerProvider atomic.Pointer[trace.TracerProvider]
}

// NewWriteBehindCoordinator 创建并初始化 WriteBehindCoordinator。
func NewWriteBehindCoordinator(writer ChunkWriter, hooks WriteBehindHooks) *WriteBehindCoordinator {
	return &WriteBehindCoordinator{
		inodes:    make(map[uint64]*inodeWriteBehind),
		handleSeq: make(map[uint64]uint64),
		writer:    writer,
		hooks:     hooks,
	}
}

func (c *WriteBehindCoordinator) bindMetrics(m *Metrics) {
	c.metrics.Store(m)
}

func (c *WriteBehindCoordinator) bindTracerProvider(tp trace.TracerProvider) {
	if tp == nil {
		c.tracerProvider.Store(nil)
		return
	}
	c.tracerProvider.Store(&tp)
}

func (c *WriteBehindCoordinator) activeTracer() (trace.Tracer, bool) {
	ptr := c.tracerProvider.Load()
	if ptr == nil || *ptr == nil {
		return nil, false
	}
	tp := *ptr
	if _, isNoop := tp.(noop.TracerProvider); isNoop {
		return nil, false
	}
	return tp.Tracer("bowl/client/fusefs"), true
}

// PendingSlots 返回当前挂起（含聚合槽与在途提交）的 4 MiB 槽位总数。
func (c *WriteBehindCoordinator) PendingSlots() int64 {
	return max(int64(0), c.pendingCount.Load())
}

// RegisterHandle 在打开文件句柄时按 Linux errseq_t 语义采样当前全局错误水位，
// 确保新打开句柄（如 O_TRUNC 空间恢复）不会被打开前的历史异步错误污染，而旧句柄仍能精确收到自身写入期间的错误。
func (c *WriteBehindCoordinator) RegisterHandle(fh uint64) {
	if fh == 0 {
		return
	}
	seq := c.errSeqGen.Load()
	if seq == 0 {
		return
	}
	c.seqMu.Lock()
	if c.handleSeq == nil {
		c.handleSeq = make(map[uint64]uint64)
	}
	c.handleSeq[fh] = seq
	c.seqMu.Unlock()
}

// ForgetHandle 在 Release 关闭本地文件句柄时清理其错误游标。
func (c *WriteBehindCoordinator) ForgetHandle(fh uint64) {
	if fh == 0 || c.errSeqGen.Load() == 0 {
		return
	}
	c.seqMu.Lock()
	delete(c.handleSeq, fh)
	c.seqMu.Unlock()
}

func (c *WriteBehindCoordinator) handleSeenSeq(fh uint64) uint64 {
	c.seqMu.Lock()
	seen := c.handleSeq[fh]
	c.seqMu.Unlock()
	return seen
}

func (c *WriteBehindCoordinator) advanceHandleSeenSeq(fh, target uint64) bool {
	c.seqMu.Lock()
	if c.handleSeq == nil {
		c.handleSeq = make(map[uint64]uint64)
	}
	if c.handleSeq[fh] >= target {
		c.seqMu.Unlock()
		return false
	}
	c.handleSeq[fh] = target
	c.seqMu.Unlock()
	return true
}

func (c *WriteBehindCoordinator) getInode(ino uint64) *inodeWriteBehind {
	c.mu.RLock()
	st := c.inodes[ino]
	c.mu.RUnlock()
	return st
}

func (c *WriteBehindCoordinator) getOrCreateInode(ino uint64) *inodeWriteBehind {
	if st := c.getInode(ino); st != nil {
		return st
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.inodes == nil {
		c.inodes = make(map[uint64]*inodeWriteBehind)
	}
	st := c.inodes[ino]
	if st == nil {
		st = newInodeWriteBehind(ino)
		c.inodes[ino] = st
	}
	return st
}

func (c *WriteBehindCoordinator) acquireInode(ino uint64) *inodeWriteBehind {
	for {
		st := c.getOrCreateInode(ino)
		st.opMu.Lock()
		if !st.removed {
			return st
		}
		st.opMu.Unlock()
	}
}

func (c *WriteBehindCoordinator) lockExistingInode(ino uint64) *inodeWriteBehind {
	st := c.getInode(ino)
	if st == nil {
		return nil
	}
	st.opMu.Lock()
	if st.removed {
		st.opMu.Unlock()
		return nil
	}
	return st
}

// HasAnyPending 无锁报告当前是否存在任何尚未落地的聚合缓冲或在途异步 Write RPC。
func (c *WriteBehindCoordinator) HasAnyPending() bool {
	return c.pendingCount.Load() > 0
}

// HasPending 报告指定服务端 Inode 是否存在尚未落地的聚合缓冲或在途异步 Write RPC。
func (c *WriteBehindCoordinator) HasPending(ino uint64) bool {
	if c.pendingCount.Load() == 0 {
		return false
	}
	st := c.getInode(ino)
	if st == nil {
		return false
	}
	st.mu.Lock()
	pending := st.stageBuf != nil || st.stageLen > 0 || st.inflight > 0
	st.mu.Unlock()
	return pending
}

// SeedCreatedInode 为客户端刚通过 Create 新建的常规文件预热顺序流探针标记：
// 新建普通文件已知无 SUID/SGID、无 Immutable/Append、无 security.capability，
// 允许首笔小块写入（<= 256 KiB）直接进入内存聚合槽直至 Flush/Fsync 批量提交。
func (c *WriteBehindCoordinator) SeedCreatedInode(ino uint64) {
	st := c.acquireInode(ino)
	st.mu.Lock()
	if st.stageLen == 0 && st.inflight == 0 && st.errSeq == 0 {
		st.hasStream = true
		st.probed = true
		st.expectedOffset = 0
	}
	st.mu.Unlock()
	st.opMu.Unlock()
}

// Write 尝试将顺序写入纳入 4 MiB 聚合槽与 W*=4 异步流水线（默认无句柄游标模式）。
func (c *WriteBehindCoordinator) Write(ctx context.Context, ino, nodeID, offset uint64, data []byte, cacheEpoch uint64) (written uint32, handled bool, err error) {
	return c.WriteForHandle(ctx, 0, ino, nodeID, offset, data, cacheEpoch)
}

// WriteForHandle 尝试将指定句柄 fh 的写入纳入 4 MiB 内存聚合槽与 W*=4 异步流水线：
//   - 若 handled == true，说明本次请求已由聚合槽/流水线接管（或捕获到该句柄尚未观测的异步写错误 err）；
//   - 若 handled == false，说明当前处于首笔探针期、大块激活探针期或大块非连续跳转（已自动排空旧缓冲），调用方应走同步 RPC。
func (c *WriteBehindCoordinator) WriteForHandle(ctx context.Context, fh, ino, nodeID, offset uint64, data []byte, cacheEpoch uint64) (written uint32, handled bool, err error) {
	if len(data) == 0 || len(data) > WriteBehindChunkSize || c.writer == nil {
		if flushErr := c.FlushAndResetInodeForHandle(ctx, fh, ino); flushErr != nil {
			return 0, true, flushErr
		}
		return 0, false, nil
	}

	if c.getInode(ino) == nil && c.errSeqGen.Load() == 0 {
		return 0, false, nil
	}
	st := c.acquireInode(ino)
	defer st.opMu.Unlock()

	st.mu.Lock()
	if st.hasUnseenErrLocked(c, fh) {
		st.barrierLocked(ctx, c)
		err = st.consumeErrLocked(c, fh)
		st.mu.Unlock()
		return 0, true, err
	}

	if !st.probed {
		st.barrierLocked(ctx, c)
		if err = st.consumeErrLocked(c, fh); err != nil {
			st.mu.Unlock()
			return 0, true, err
		}
		st.resetStreamLocked()
		st.mu.Unlock()
		return 0, false, nil
	}

	isSmallWrite := len(data) <= WriteBehindBlockAlign
	if !isSmallWrite {
		// 大块流式写入（> 256 KiB，如 1 MiB FUSE 分片）：维持严格连续性与 2 MiB 同步探针阈值。
		if !st.hasStream || offset != st.expectedOffset {
			st.barrierLocked(ctx, c)
			if err = st.consumeErrLocked(c, fh); err != nil {
				st.mu.Unlock()
				return 0, true, err
			}
			st.resetStreamLocked()
			st.mu.Unlock()
			return 0, false, nil
		}
		if st.stageBuf == nil && (st.probedBytes < WriteBehindActivateBytes || offset%WriteBehindBlockAlign != 0) {
			st.barrierLocked(ctx, c)
			if err = st.consumeErrLocked(c, fh); err != nil {
				st.mu.Unlock()
				return 0, true, err
			}
			st.mu.Unlock()
			return 0, false, nil
		}
	}

	st.nodeID = nodeID
	st.cacheEpoch = cacheEpoch
	activeGen := st.streamGen
	writeEnd := offset + uint64(len(data))

	// 已探测的小块/页写（<= 256 KiB）优先尝试在当前活跃 stageBuf 窗口内原地合并（覆盖连续帧追加、局部覆写与相邻页写）。
	if isSmallWrite && st.stageBuf != nil && st.stageLen > 0 {
		stageStart := st.stageOffset
		stageEnd := stageStart + uint64(st.stageLen)
		switch {
		case offset >= stageStart && offset <= stageEnd && writeEnd-stageStart <= WriteBehindChunkSize:
			st.recordStageLinkLocked(ctx)
			relOff := int(offset - stageStart)
			copy(st.stageBuf[relOff:relOff+len(data)], data)
			if relOff+len(data) > st.stageLen {
				st.stageLen = relOff + len(data)
			}
			st.hasStream = true
			st.expectedOffset = st.stageOffset + uint64(st.stageLen)
			if writeEnd > st.maxStagedEnd {
				st.maxStagedEnd = writeEnd
			}
			if st.stageLen == WriteBehindChunkSize {
				if !st.flushFullStageLocked(ctx, c) {
					st.waitInflightObservedLocked(ctx, c)
					err = st.consumeErrLocked(c, fh)
					if err == nil {
						err = st.lastErr
					}
					st.resetStreamLocked()
					st.mu.Unlock()
					return 0, true, err
				}
			}
			if c.hooks.OnStage != nil {
				c.hooks.OnStage(nodeID, cacheEpoch, st.maxStagedEnd, timestamppb.Now())
			}
			st.mu.Unlock()
			c.metrics.Load().addWriteBehindStaged(uint64(len(data)))
			return uint32(len(data)), true, nil

		case offset < stageStart && writeEnd >= stageStart &&
			max(stageEnd, writeEnd)-offset <= WriteBehindChunkSize && st.stageLen <= WriteBehindBlockAlign:
			st.recordStageLinkLocked(ctx)
			newEnd := max(stageEnd, writeEnd)
			shift := int(stageStart - offset)
			copy(st.stageBuf[shift:shift+st.stageLen], st.stageBuf[:st.stageLen])
			copy(st.stageBuf[:len(data)], data)
			st.stageOffset = offset
			st.stageLen = int(newEnd - offset)
			st.hasStream = true
			st.expectedOffset = newEnd
			if newEnd > st.maxStagedEnd {
				st.maxStagedEnd = newEnd
			}
			if st.stageLen == WriteBehindChunkSize {
				if !st.flushFullStageLocked(ctx, c) {
					st.waitInflightObservedLocked(ctx, c)
					err = st.consumeErrLocked(c, fh)
					if err == nil {
						err = st.lastErr
					}
					st.resetStreamLocked()
					st.mu.Unlock()
					return 0, true, err
				}
			}
			if c.hooks.OnStage != nil {
				c.hooks.OnStage(nodeID, cacheEpoch, st.maxStagedEnd, timestamppb.Now())
			}
			st.mu.Unlock()
			c.metrics.Load().addWriteBehindStaged(uint64(len(data)))
			return uint32(len(data)), true, nil

		case offset == stageEnd:
			// 连续追加跨越 4 MiB 槽位上限：落入下方分片循环先填满当前 4 MiB 槽刷出，再开启下一槽位。

		default:
			// 非相邻偏移跳转（例如 wal_checkpoint 分段页写）：先将旧聚合槽合并为单次同步 Write RPC 刷出，再开启新聚合槽。
			if !st.flushStageSyncLocked(ctx, c) {
				st.waitInflightObservedLocked(ctx, c)
				err = st.consumeErrLocked(c, fh)
				if err == nil {
					err = st.lastErr
				}
				st.resetStreamLocked()
				st.mu.Unlock()
				return 0, true, err
			}
			activeGen = st.streamGen
		}
	}

	rem := data
	curOff := offset
	for len(rem) > 0 {
		if st.streamGen != activeGen || st.hasUnseenErrLocked(c, fh) {
			st.barrierLocked(ctx, c)
			err = st.consumeErrLocked(c, fh)
			if err == nil {
				err = st.lastErr
			}
			st.resetStreamLocked()
			st.mu.Unlock()
			return 0, true, err
		}
		if st.stageBuf == nil {
			st.stageBuf = acquireWriteBehindBuf()
			c.pendingCount.Add(1)
			st.stageOffset = curOff
			st.stageLen = 0
			st.stageGen = activeGen
		}
		st.recordStageLinkLocked(ctx)
		space := WriteBehindChunkSize - st.stageLen
		n := min(len(rem), space)
		copy(st.stageBuf[st.stageLen:st.stageLen+n], rem[:n])
		st.stageLen += n
		rem = rem[n:]
		curOff += uint64(n)
		st.hasStream = true
		st.expectedOffset = curOff
		if curOff > st.maxStagedEnd {
			st.maxStagedEnd = curOff
		}

		if st.stageLen == WriteBehindChunkSize {
			if !st.flushFullStageLocked(ctx, c) {
				st.waitInflightObservedLocked(ctx, c)
				err = st.consumeErrLocked(c, fh)
				if err == nil {
					err = st.lastErr
				}
				st.resetStreamLocked()
				st.mu.Unlock()
				return 0, true, err
			}
		}
	}

	if c.hooks.OnStage != nil {
		c.hooks.OnStage(nodeID, cacheEpoch, st.maxStagedEnd, timestamppb.Now())
	}
	st.mu.Unlock()
	c.metrics.Load().addWriteBehindStaged(uint64(len(data)))
	return uint32(len(data)), true, nil
}

// ObserveSyncWrite 在同步直通写入成功后记录首写探针已完成并推进顺序流偏移。
func (c *WriteBehindCoordinator) ObserveSyncWrite(ino, offset uint64, written uint32) {
	if written == 0 {
		return
	}
	st := c.acquireInode(ino)
	st.mu.Lock()
	if st.stageLen == 0 && st.inflight == 0 {
		end := offset + uint64(written)
		if st.hasStream && offset == st.expectedOffset {
			st.probed = true
			st.probedBytes += uint64(written)
			st.expectedOffset = end
		} else {
			st.streamGen++
			st.lastCommitted = nil
			st.hasStream = true
			st.probed = true
			st.probedBytes = uint64(written)
			st.expectedOffset = end
		}
		if end > st.maxStagedEnd {
			st.maxStagedEnd = end
		}
	}
	st.mu.Unlock()
	st.opMu.Unlock()
}

// ResetInodeStream 原子排空目标 Inode 的挂起写入并重置顺序流探测进度与属性提交世代（不消费句柄级写错误游标）。
// 供 Open(O_TRUNC)、SetAttr、Fallocate 与 Ioctl 调用。
func (c *WriteBehindCoordinator) ResetInodeStream(ctx context.Context, ino uint64) {
	st := c.lockExistingInode(ino)
	if st == nil {
		return
	}
	st.mu.Lock()
	st.barrierLocked(ctx, c)
	st.resetStreamLocked()
	st.mu.Unlock()
	st.opMu.Unlock()
}

// FlushInode 刷出目标 Inode 的尾部分片缓冲，等待全部在途异步 Write RPC 完成，并按默认游标消费粘性错误。
func (c *WriteBehindCoordinator) FlushInode(ctx context.Context, ino uint64) error {
	return c.FlushInodeForHandle(ctx, 0, ino)
}

// FlushInodeForHandle 刷出目标 Inode 的尾部分片缓冲，等待全部在途异步 Write RPC 完成，并按句柄 fh 的 errseq 游标消费未观测错误。
// 供可写句柄 Flush 与 Fsync 调用。
func (c *WriteBehindCoordinator) FlushInodeForHandle(ctx context.Context, fh, ino uint64) error {
	st := c.lockExistingInode(ino)
	if st == nil {
		return nil
	}
	st.mu.Lock()
	st.barrierLocked(ctx, c)
	err := st.consumeErrLocked(c, fh)
	st.mu.Unlock()
	st.opMu.Unlock()
	return err
}

// FlushAndResetInode 在同一原子临界区内刷出尾部分片、等待全部在途 RPC 收口、按默认游标消费错误并重置顺序流状态。
func (c *WriteBehindCoordinator) FlushAndResetInode(ctx context.Context, ino uint64) error {
	return c.FlushAndResetInodeForHandle(ctx, 0, ino)
}

// FlushAndResetInodeForHandle 在同一原子临界区内刷出尾部分片、等待全部在途 RPC 收口、按句柄 fh 的 errseq 游标消费未观测错误并重置顺序流状态。
// 供同步旁路 Write（如 O_APPEND、O_SYNC、特殊标志校验）调用。
func (c *WriteBehindCoordinator) FlushAndResetInodeForHandle(ctx context.Context, fh, ino uint64) error {
	st := c.lockExistingInode(ino)
	if st == nil {
		return nil
	}
	st.mu.Lock()
	st.barrierLocked(ctx, c)
	err := st.consumeErrLocked(c, fh)
	st.resetStreamLocked()
	st.mu.Unlock()
	st.opMu.Unlock()
	return err
}

// BarrierInode 刷出目标 Inode 的尾部分片缓冲并等待全部在途异步 Write RPC 完成，
// 但保留未消费的 stickyErr 留待后续可写句柄 Write / Flush / Fsync 报告。
// 供 Read、GetAttr、Lookup、只读句柄 Flush、Link、Unlink、Rename、Release 等只读观测或拓扑屏障调用。
func (c *WriteBehindCoordinator) BarrierInode(ctx context.Context, ino uint64) {
	if c.pendingCount.Load() == 0 {
		return
	}
	st := c.lockExistingInode(ino)
	if st == nil {
		return
	}
	st.mu.Lock()
	st.barrierLocked(ctx, c)
	st.mu.Unlock()
	st.opMu.Unlock()
}

// BarrierAll 刷出所有活跃 Inode 的未落盘聚合缓冲并等待在途 RPC 全部收口。
func (c *WriteBehindCoordinator) BarrierAll(ctx context.Context) {
	if c.pendingCount.Load() == 0 {
		return
	}
	c.mu.RLock()
	targets := make([]*inodeWriteBehind, 0, len(c.inodes))
	for _, st := range c.inodes {
		targets = append(targets, st)
	}
	c.mu.RUnlock()

	for _, st := range targets {
		st.opMu.Lock()
		st.mu.Lock()
		st.barrierLocked(ctx, c)
		st.mu.Unlock()
		st.opMu.Unlock()
	}
}

// RemoveInode 在文件的最后一个本地句柄关闭或文件被彻底解链后排空在途写入并回收其内存状态。
func (c *WriteBehindCoordinator) RemoveInode(ctx context.Context, ino uint64) {
	st := c.lockExistingInode(ino)
	if st == nil {
		return
	}
	defer st.opMu.Unlock()

	st.mu.Lock()
	st.barrierLocked(ctx, c)
	canDelete := st.stageBuf == nil && st.stageLen == 0 && st.inflight == 0
	if canDelete {
		st.removed = true
	}
	st.mu.Unlock()

	if canDelete {
		c.mu.Lock()
		if c.inodes[ino] == st {
			delete(c.inodes, ino)
		}
		c.mu.Unlock()
	}
}

// Clear 排空所有在途写入并清空全部状态（供卸载时调用）。
func (c *WriteBehindCoordinator) Clear(ctx context.Context) {
	c.mu.Lock()
	targets := make([]*inodeWriteBehind, 0, len(c.inodes))
	for _, st := range c.inodes {
		targets = append(targets, st)
	}
	clear(c.inodes)
	c.mu.Unlock()

	for _, st := range targets {
		st.opMu.Lock()
		st.mu.Lock()
		if ctx == nil || ctx.Err() == nil {
			st.barrierLocked(ctx, c)
		}
		st.discardStageLocked(&c.pendingCount)
		st.removed = true
		st.mu.Unlock()
		st.opMu.Unlock()
	}

	c.seqMu.Lock()
	clear(c.handleSeq)
	c.seqMu.Unlock()
}
