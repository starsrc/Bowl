package fusefs

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"

	"github.com/starsrc/bowl/client/internal/rpc"
)

// MaxRPCChunkSize 定义客户端单次读写 RPC 的最大载荷切片大小 (4 MiB = 4,194,304 字节)。
const MaxRPCChunkSize = 4 * 1024 * 1024

// ReadAheadMaxPrefetch 定义确认连续顺序流后单句柄最多并发维持的 4 MiB 异步预读窗口数（W_read = 4）。
const ReadAheadMaxPrefetch = 4

// ReadAheadMaxGlobalInflight 定义单个客户端实例跨所有活跃句柄允许并发在途的最大异步预读 RPC 数（32 * 4 MiB = 128 MiB）。
// 1~8 路并发顺序流均可享满 W_read = 4 流水线深度；16 路高并发顺序读（如 fio seq_read_16jobs）每流稳定获得 perStreamCap = 2 个专属双缓冲预读槽位（共 32 路 = 128 MiB），
// 配合客户端 8 条独立 H2C 数据条带连接（总接收窗口 512 MiB），彻底消除预读流水线断流与单连接流控阻塞。
const ReadAheadMaxGlobalInflight = 32

const defaultPrefetchTimeout = 30 * time.Second

// readAheadMinWant 是触发对齐窗口拉取的最小请求尺寸。
// 内核 FUSE 的 readahead 请求按 128 KiB 起步（MaxReadAhead=1 MiB），
// 随机小读通常为 4~64 KiB；用尺寸阈值把“预读流量”和“随机小读”分开，
// 保持既有契约：非连续随机读严格按请求尺寸直通，绝不放大成 4 MiB。
const readAheadMinWant = 128 * 1024

// sharedReadBuf 为池化 rpc.ReadPayload 提供原子引用计数包装，
// 确保活跃窗口、上一窗口、单飞等待者与前台拷贝均释放引用后才归还 4 MiB 缓冲池。
type sharedReadBuf struct {
	payload rpc.ReadPayload
	refs    atomic.Int32
}

func newSharedReadBuf(p rpc.ReadPayload) *sharedReadBuf {
	b := &sharedReadBuf{payload: p}
	b.refs.Store(1)
	return b
}

func (b *sharedReadBuf) bytes() []byte {
	if b == nil {
		return nil
	}
	return b.payload.Data
}

func (b *sharedReadBuf) retain() *sharedReadBuf {
	if b == nil {
		return nil
	}
	for {
		cur := b.refs.Load()
		if cur <= 0 {
			return nil
		}
		if b.refs.CompareAndSwap(cur, cur+1) {
			return b
		}
	}
}

func (b *sharedReadBuf) release() {
	if b == nil {
		return
	}
	if b.refs.Add(-1) == 0 {
		b.payload.Release()
	}
}

// windowSlot 统一表示单个 4 MiB 对齐窗口 [offset, offset+MaxRPCChunkSize) 的在途拉取或预取就绪槽位。
// 无论是后台异步预取（isPrefetch=true）还是前台按需拉取（isPrefetch=false），槽位在完成并原子晋升至活跃窗口（或被废弃）前
// 始终保留在 hra.slots 中，彻底消除“弹出预取等待期间并发读看不到在途请求而重复发 RPC”的单飞盲区。
type windowSlot struct {
	offset     uint64
	gen        uint64
	isPrefetch bool
	done       chan struct{}
	cancel     context.CancelFunc
	spanCtx    trace.SpanContext

	// 以下字段全部受所属 handleReadAhead.mu 保护：
	inSlots        bool
	completed      bool
	installed      bool
	discarded      bool
	discardCounted bool
	waiters        int
	waiterConsumed uint64
	fetchedBytes   uint64
	buf            *sharedReadBuf
	err            error
}

type handleReadAhead struct {
	mu          sync.Mutex
	ino         uint64
	gen         uint64
	lastReadEnd uint64
	hasLastRead bool
	seqCount    int
	pipelined   bool
	// streamEnd 是该句柄已观测到的顺序读高水位（单调递增）。
	// 内核 readahead 线程与用户线程会并发下发读请求，到达顺序与偏移顺序无关，
	// 单靠 lastReadEnd 无法判定“是否同一股顺序流”，必须用单调高水位。
	streamEnd uint64
	// reads 是该句柄已收到的读请求数；首个请求一律直通，避免一次性大读被放大。
	reads         int
	firstWinStart uint64
	hasFirstWin   bool
	winOffset     uint64
	winBuf        *sharedReadBuf
	winValid      bool
	winEOF        bool
	winConsumed   int

	// prevWinBuf 保留顺序推进刚滑出的上一 4 MiB 窗口，供乱序晚到的内核并发 readahead 请求直接命中内存，
	// 避免跨 4 MiB 边界乱序请求误判为随机跳读而清空整条流水线。
	prevWinOffset   uint64
	prevWinBuf      *sharedReadBuf
	prevWinValid    bool
	prevWinConsumed int

	// slots 按 offset 升序维护所有在途或已就绪待晋升的 4 MiB 对齐窗口槽位（统一承载前台单飞与后台预取）。
	slots []*windowSlot
}

func (hra *handleReadAhead) winData() []byte {
	return hra.winBuf.bytes()
}

func (hra *handleReadAhead) prevWinData() []byte {
	return hra.prevWinBuf.bytes()
}

// requestInWindowLocked 报告请求区间是否与当前活跃窗口、上一窗口或在途/就绪窗口槽位相交。
// 调用方必须持有 hra.mu。
func (hra *handleReadAhead) requestInWindowLocked(offset uint64, want int) bool {
	end := offset + uint64(want)
	if hra.winValid && offset < hra.winOffset+uint64(len(hra.winData())) && end > hra.winOffset {
		return true
	}
	if hra.prevWinValid && offset < hra.prevWinOffset+uint64(len(hra.prevWinData())) && end > hra.prevWinOffset {
		return true
	}
	for _, s := range hra.slots {
		if offset < s.offset+MaxRPCChunkSize && end > s.offset {
			return true
		}
	}
	return false
}

// ChunkFetcher 定义在指定字节偏移与期望长度下向底层存储拉取数据块的窄端口适配器。
type ChunkFetcher func(ctx context.Context, offset uint64, length uint32) (rpc.ReadPayload, error)

// ReadAheadCoordinator 协调单个客户端实例中所有打开句柄的 4 MiB 对齐预读窗口、
// W_read=4 并发流水线预取、跨窗口边界拼接、单飞（Singleflight）窗口合并及并发写代数（gen）失效。
type ReadAheadCoordinator struct {
	mu                 sync.Mutex
	handles            map[uint64]*handleReadAhead
	byInode            map[uint64]map[uint64]*handleReadAhead
	inflightPrefetches atomic.Int32
	activePipelined    atomic.Int32
	metrics            atomic.Pointer[Metrics]
	tracerProvider     atomic.Pointer[trace.TracerProvider]
}

// NewReadAheadCoordinator 创建并初始化 ReadAheadCoordinator。
func NewReadAheadCoordinator() *ReadAheadCoordinator {
	return &ReadAheadCoordinator{
		handles: make(map[uint64]*handleReadAhead),
		byInode: make(map[uint64]map[uint64]*handleReadAhead),
	}
}

func (c *ReadAheadCoordinator) bindInodeLocked(fh, ino uint64, hra *handleReadAhead) {
	if c.byInode == nil {
		c.byInode = make(map[uint64]map[uint64]*handleReadAhead)
	}
	set := c.byInode[ino]
	if set == nil {
		set = make(map[uint64]*handleReadAhead)
		c.byInode[ino] = set
	}
	set[fh] = hra
}

func (c *ReadAheadCoordinator) unbindInodeLocked(fh, ino uint64) {
	if set := c.byInode[ino]; set != nil {
		delete(set, fh)
		if len(set) == 0 {
			delete(c.byInode, ino)
		}
	}
}

func (c *ReadAheadCoordinator) bindMetrics(m *Metrics) {
	c.metrics.Store(m)
}

func (c *ReadAheadCoordinator) bindTracerProvider(tp trace.TracerProvider) {
	if tp == nil {
		c.tracerProvider.Store(nil)
		return
	}
	c.tracerProvider.Store(&tp)
}

func (c *ReadAheadCoordinator) activeTracer() (trace.Tracer, bool) {
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

func (c *ReadAheadCoordinator) waitDone(ctx context.Context, done <-chan struct{}, linkSC trace.SpanContext) error {
	select {
	case <-done:
		return nil
	default:
	}
	waitStart := time.Now()
	var waitSpan trace.Span = noopSpanInstance
	if tracer, ok := c.activeTracer(); ok {
		var opts []trace.SpanStartOption
		if linkSC.IsValid() {
			opts = append(opts, trace.WithLinks(trace.Link{SpanContext: linkSC}))
		}
		_, waitSpan = tracer.Start(ctx, "read_ahead.Wait", opts...)
	}
	var waitErr error
	select {
	case <-done:
	case <-ctx.Done():
		waitErr = ctx.Err()
		waitSpan.RecordError(waitErr)
		waitSpan.SetStatus(codes.Error, waitErr.Error())
	}
	waitSpan.End()
	c.metrics.Load().observeReadAheadWait(time.Since(waitStart))
	return waitErr
}

func (c *ReadAheadCoordinator) findSlotLocked(hra *handleReadAhead, alignedOffset uint64) *windowSlot {
	for _, s := range hra.slots {
		if s.offset == alignedOffset {
			return s
		}
		if s.offset > alignedOffset {
			break
		}
	}
	return nil
}

func (c *ReadAheadCoordinator) insertSlotLocked(hra *handleReadAhead, slot *windowSlot) {
	slot.inSlots = true
	pos := len(hra.slots)
	for i, existing := range hra.slots {
		if existing.offset > slot.offset {
			pos = i
			break
		}
	}
	hra.slots = append(hra.slots, nil)
	copy(hra.slots[pos+1:], hra.slots[pos:])
	hra.slots[pos] = slot
}

func (c *ReadAheadCoordinator) removeSlotLocked(hra *handleReadAhead, target *windowSlot) {
	if !target.inSlots {
		return
	}
	target.inSlots = false
	for i, s := range hra.slots {
		if s == target {
			rem := copy(hra.slots[i:], hra.slots[i+1:])
			hra.slots[i+rem] = nil
			hra.slots = hra.slots[:i+rem]
			return
		}
	}
}

// maybeReleaseSlotBufLocked 在槽位已脱离 hra.slots 且无任何活跃等待者时释放其持有的缓冲引用，
// 并对未被安装到窗口的废弃预取字节进行精确入账。
func (c *ReadAheadCoordinator) maybeReleaseSlotBufLocked(s *windowSlot) {
	if !s.completed || s.inSlots || s.waiters > 0 {
		return
	}
	if !s.installed && s.discarded && !s.discardCounted && s.err == nil {
		s.discardCounted = true
		if s.fetchedBytes > s.waiterConsumed {
			c.metrics.Load().addReadAheadDiscardedBytes(s.fetchedBytes - s.waiterConsumed)
		}
	}
	if s.buf != nil {
		s.buf.release()
		s.buf = nil
	}
}

func (c *ReadAheadCoordinator) discardAllSlotsLocked(hra *handleReadAhead) {
	for i, s := range hra.slots {
		hra.slots[i] = nil
		s.inSlots = false
		s.discarded = true
		if s.cancel != nil {
			s.cancel()
		}
		c.maybeReleaseSlotBufLocked(s)
	}
	hra.slots = hra.slots[:0]
}

// discardUnawaitedSlotsLocked 清理无前台等待者的冗余预取槽位（如命中 EOF 后更高偏移的预取），
// 同时保留已有前台等待者的在途槽位正常完成，避免误杀并发读请求。
func (c *ReadAheadCoordinator) discardUnawaitedSlotsLocked(hra *handleReadAhead) {
	keep := 0
	for _, s := range hra.slots {
		if s.waiters > 0 {
			hra.slots[keep] = s
			keep++
			continue
		}
		s.inSlots = false
		s.discarded = true
		if s.cancel != nil {
			s.cancel()
		}
		c.maybeReleaseSlotBufLocked(s)
	}
	clear(hra.slots[keep:])
	hra.slots = hra.slots[:keep]
}

func (c *ReadAheadCoordinator) discardPrevWindowLocked(hra *handleReadAhead) {
	if hra.prevWinValid {
		dataLen := len(hra.prevWinData())
		if dataLen > hra.prevWinConsumed {
			c.metrics.Load().addReadAheadDiscardedBytes(uint64(dataLen - hra.prevWinConsumed))
		}
	}
	if hra.prevWinBuf != nil {
		hra.prevWinBuf.release()
		hra.prevWinBuf = nil
	}
	hra.prevWinOffset = 0
	hra.prevWinValid = false
	hra.prevWinConsumed = 0
}

func (c *ReadAheadCoordinator) discardAllWindowsLocked(hra *handleReadAhead) {
	c.discardPrevWindowLocked(hra)
	if hra.winValid {
		dataLen := len(hra.winData())
		if dataLen > hra.winConsumed {
			c.metrics.Load().addReadAheadDiscardedBytes(uint64(dataLen - hra.winConsumed))
		}
	}
	if hra.winBuf != nil {
		hra.winBuf.release()
		hra.winBuf = nil
	}
	hra.winOffset = 0
	hra.winValid = false
	hra.winEOF = false
	hra.winConsumed = 0
}

// installWindowMonotonicLocked 按单调递增偏移语义将新完成的对齐窗口安装到句柄上：
//   - 若 offset 严格推进当前活跃窗口，则将紧邻的旧活跃窗口平滑降级为 prevWinBuf 并推进 winOffset；
//   - 若句柄已推进到更高偏移（并发等待者乱序晚唤醒），绝不倒退覆盖新窗口，仅在紧邻高窗口左侧且 prevWin 空闲时补入 prevWin。
//
// 返回 installed 指示 buf 是否被纳入 currWin 或 prevWin 管理。
func (c *ReadAheadCoordinator) installWindowMonotonicLocked(
	hra *handleReadAhead,
	offset uint64,
	buf *sharedReadBuf,
	eof bool,
	consumed int,
) (installed bool) {
	dataLen := len(buf.bytes())
	clampedConsumed := min(dataLen, max(0, consumed))

	if hra.winValid {
		if offset == hra.winOffset {
			hra.winConsumed = min(len(hra.winData()), hra.winConsumed+clampedConsumed)
			return true
		}
		if offset < hra.winOffset {
			if hra.prevWinValid && offset == hra.prevWinOffset {
				hra.prevWinConsumed = min(len(hra.prevWinData()), hra.prevWinConsumed+clampedConsumed)
				return true
			}
			if !hra.prevWinValid && dataLen > 0 && offset+uint64(dataLen) == hra.winOffset {
				hra.prevWinOffset = offset
				hra.prevWinBuf = buf.retain()
				hra.prevWinValid = hra.prevWinBuf != nil
				hra.prevWinConsumed = clampedConsumed
				return hra.prevWinValid
			}
			return false
		}
	}

	if hra.winValid && hra.winBuf != nil && dataLen > 0 && hra.winOffset+uint64(len(hra.winData())) == offset {
		c.discardPrevWindowLocked(hra)
		hra.prevWinOffset = hra.winOffset
		hra.prevWinBuf = hra.winBuf
		hra.prevWinValid = true
		hra.prevWinConsumed = hra.winConsumed
		hra.winBuf = nil
		hra.winValid = false
		hra.winConsumed = 0
	} else {
		c.discardAllWindowsLocked(hra)
	}

	hra.winOffset = offset
	if dataLen > 0 {
		hra.winBuf = buf.retain()
		hra.winValid = hra.winBuf != nil
	} else {
		hra.winBuf = nil
		hra.winValid = false
	}
	hra.winEOF = eof
	hra.winConsumed = clampedConsumed
	return true
}

func (c *ReadAheadCoordinator) recordWindowConsumedLocked(hra *handleReadAhead, alignedOffset uint64, consumed int) {
	if consumed <= 0 {
		return
	}
	if hra.winValid && hra.winOffset == alignedOffset {
		hra.winConsumed = min(len(hra.winData()), hra.winConsumed+consumed)
		return
	}
	if hra.prevWinValid && hra.prevWinOffset == alignedOffset {
		hra.prevWinConsumed = min(len(hra.prevWinData()), hra.prevWinConsumed+consumed)
	}
}

func (c *ReadAheadCoordinator) setPipelinedLocked(hra *handleReadAhead, v bool) {
	if hra.pipelined == v {
		return
	}
	hra.pipelined = v
	if v {
		c.activePipelined.Add(1)
	} else {
		c.activePipelined.Add(-1)
	}
}

func (c *ReadAheadCoordinator) tryAcquirePrefetchSlot() bool {
	for {
		cur := c.inflightPrefetches.Load()
		if cur >= ReadAheadMaxGlobalInflight {
			return false
		}
		if c.inflightPrefetches.CompareAndSwap(cur, cur+1) {
			return true
		}
	}
}

func (c *ReadAheadCoordinator) resetHandleLocked(hra *handleReadAhead) {
	hra.gen++
	hra.reads = 0
	hra.firstWinStart = 0
	hra.hasFirstWin = false
	hra.lastReadEnd = 0
	hra.hasLastRead = false
	hra.seqCount = 0
	c.setPipelinedLocked(hra, false)
	hra.streamEnd = 0
	c.discardAllWindowsLocked(hra)
	c.discardAllSlotsLocked(hra)
}

func (c *ReadAheadCoordinator) clearWindowAndPrefetchLocked(hra *handleReadAhead) {
	hra.gen++
	hra.lastReadEnd = 0
	hra.hasLastRead = false
	hra.seqCount = 0
	hra.streamEnd = 0
	c.setPipelinedLocked(hra, false)
	c.discardAllWindowsLocked(hra)
	// 对无前台等待者的预取槽位立即取消，对已有前台等待者的槽位仅摘除缓存资格而不断流，
	// 严禁预读淘汰启发式误杀正在等待结果的正常前台 Read 请求。
	for i, s := range hra.slots {
		hra.slots[i] = nil
		s.inSlots = false
		s.discarded = true
		if s.waiters == 0 && s.cancel != nil {
			s.cancel()
		}
		c.maybeReleaseSlotBufLocked(s)
	}
	hra.slots = hra.slots[:0]
}

func (c *ReadAheadCoordinator) getOrCreateHandle(fh, ino uint64) *handleReadAhead {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.handles == nil {
		c.handles = make(map[uint64]*handleReadAhead)
	}
	hra, ok := c.handles[fh]
	if !ok {
		hra = &handleReadAhead{
			ino: ino,
		}
		c.handles[fh] = hra
		c.bindInodeLocked(fh, ino, hra)
		return hra
	}
	hra.mu.Lock()
	if hra.ino != ino {
		oldIno := hra.ino
		c.resetHandleLocked(hra)
		hra.ino = ino
		c.unbindInodeLocked(fh, oldIno)
		c.bindInodeLocked(fh, ino, hra)
	}
	hra.mu.Unlock()
	return hra
}

func (c *ReadAheadCoordinator) launchPrefetchSlotLocked(
	triggerCtx context.Context,
	hra *handleReadAhead,
	nextOffset uint64,
	fetch ChunkFetcher,
) *windowSlot {
	triggerSC := trace.SpanContextFromContext(triggerCtx)
	pfBaseCtx := context.Background()
	var pfSpan trace.Span = noopSpanInstance
	if tracer, ok := c.activeTracer(); ok {
		opts := make([]trace.SpanStartOption, 0, 3)
		opts = append(opts,
			trace.WithNewRoot(),
			trace.WithAttributes(
				attribute.Int64("read_ahead.ino", int64(hra.ino)),
				attribute.Int64("read_ahead.offset", int64(nextOffset)),
				attribute.Int("read_ahead.length", MaxRPCChunkSize),
			),
		)
		if triggerSC.IsValid() {
			opts = append(opts, trace.WithLinks(trace.Link{SpanContext: triggerSC}))
		}
		pfBaseCtx, pfSpan = tracer.Start(pfBaseCtx, "read_ahead.Prefetch", opts...)
	}

	pfCtx, pfCancel := context.WithTimeout(pfBaseCtx, defaultPrefetchTimeout)
	s := &windowSlot{
		offset:     nextOffset,
		gen:        hra.gen,
		isPrefetch: true,
		done:       make(chan struct{}),
		cancel:     pfCancel,
		spanCtx:    pfSpan.SpanContext(),
	}
	m := c.metrics.Load()
	m.incReadAheadTrigger()
	m.addReadAheadInflight(1)

	go func() {
		slotReleased := false
		releaseBudget := func() {
			if slotReleased {
				return
			}
			slotReleased = true
			pfCancel()
			pfSpan.End()
			c.inflightPrefetches.Add(-1)
			c.metrics.Load().addReadAheadInflight(-1)
		}
		defer releaseBudget()

		payload, err := fetch(pfCtx, nextOffset, MaxRPCChunkSize)
		var fetchedBuf *sharedReadBuf
		var fetchedLen uint64
		if err != nil {
			pfSpan.RecordError(err)
			pfSpan.SetStatus(codes.Error, err.Error())
		} else {
			fetchedBuf = newSharedReadBuf(payload)
			fetchedLen = uint64(len(fetchedBuf.bytes()))
			pfSpan.SetAttributes(attribute.Int64("read_ahead.fetched_bytes", int64(fetchedLen)))
			c.metrics.Load().addReadAheadPrefetchedBytes(fetchedLen)
		}

		// 在唤醒等待者前先释放全局预读配额并结束 Prefetch Span，
		// 确保被唤醒的前台消费者能够立即申请下一窗口槽位，且链路导出无竞争。
		releaseBudget()

		hra.mu.Lock()
		s.completed = true
		s.err = err
		s.buf = fetchedBuf
		s.fetchedBytes = fetchedLen
		if err != nil || s.discarded || hra.gen != s.gen {
			if err == nil {
				s.discarded = true
			}
			c.removeSlotLocked(hra, s)
		}
		c.maybeReleaseSlotBufLocked(s)
		close(s.done)
		hra.mu.Unlock()
	}()
	return s
}

// ensurePrefetchPipelineLocked 根据当前顺序流成熟度补齐异步预取流水线：
//   - 首窗口刚建立时预取 1 个 4 MiB 窗口；
//   - 一旦确认连续流（累计 >= 3 次顺序读、窗口内已消费 >= 1 MiB 或已有预取晋升），按活跃流数公平收敛在 [1, W_read=4] 深度补齐后续窗口。
func (c *ReadAheadCoordinator) ensurePrefetchPipelineLocked(triggerCtx context.Context, hra *handleReadAhead, fetch ChunkFetcher) {
	if fetch == nil || !hra.winValid || hra.winEOF {
		c.discardUnawaitedSlotsLocked(hra)
		return
	}
	if len(hra.winData()) < MaxRPCChunkSize {
		hra.winEOF = true
		c.discardUnawaitedSlotsLocked(hra)
		return
	}

	targetDepth := 1
	if hra.pipelined || hra.seqCount >= 3 || hra.winConsumed >= 1<<20 {
		c.setPipelinedLocked(hra, true)
		streams := max(1, int(c.activePipelined.Load()))
		perStreamCap := max(1, ReadAheadMaxGlobalInflight/streams)
		targetDepth = min(ReadAheadMaxPrefetch, perStreamCap)
	}

	maxAllowedOffset := hra.winOffset + uint64(ReadAheadMaxPrefetch)*MaxRPCChunkSize
	var eofCutoff uint64
	hasEOFCutoff := false

	keep := 0
	for _, s := range hra.slots {
		outOfRange := s.offset <= hra.winOffset || s.offset > maxAllowedOffset || (hasEOFCutoff && s.offset > eofCutoff)
		if (s.discarded || outOfRange) && s.waiters == 0 {
			s.inSlots = false
			s.discarded = true
			if s.cancel != nil {
				s.cancel()
			}
			c.maybeReleaseSlotBufLocked(s)
			continue
		}
		hra.slots[keep] = s
		keep++
		if !hasEOFCutoff && s.offset > hra.winOffset && s.completed && s.err == nil && s.fetchedBytes < MaxRPCChunkSize {
			hasEOFCutoff = true
			eofCutoff = s.offset
		}
	}
	clear(hra.slots[keep:])
	hra.slots = hra.slots[:keep]

	for step := 1; step <= targetDepth; step++ {
		nextOff := hra.winOffset + uint64(step)*MaxRPCChunkSize
		if hasEOFCutoff && nextOff > eofCutoff {
			break
		}
		if c.findSlotLocked(hra, nextOff) != nil {
			continue
		}
		if !c.tryAcquirePrefetchSlot() {
			break
		}
		s := c.launchPrefetchSlotLocked(triggerCtx, hra, nextOff, fetch)
		c.insertSlotLocked(hra, s)
	}
}

func (c *ReadAheadCoordinator) consumeSlotResultLocked(
	hra *handleReadAhead,
	s *windowSlot,
	buf *sharedReadBuf,
	callerStartGen uint64,
	relOffset, maxConsume int,
) {
	data := buf.bytes()
	var consumed int
	if relOffset >= 0 && relOffset < len(data) && maxConsume > 0 {
		consumed = min(maxConsume, len(data)-relOffset)
	}

	if s.installed {
		if hra.gen == callerStartGen {
			c.recordWindowConsumedLocked(hra, s.offset, consumed)
		}
		return
	}

	c.removeSlotLocked(hra, s)
	if s.discarded || hra.gen != s.gen || hra.gen != callerStartGen {
		s.discarded = true
		s.waiterConsumed += uint64(consumed)
		return
	}

	if s.isPrefetch {
		c.setPipelinedLocked(hra, true)
	}
	if len(data) == 0 {
		s.installed = true
		if !hra.winValid || s.offset >= hra.winOffset {
			hra.winEOF = true
			c.discardUnawaitedSlotsLocked(hra)
		}
		return
	}

	winEOF := len(data) < MaxRPCChunkSize
	if c.installWindowMonotonicLocked(hra, s.offset, buf, winEOF, consumed) {
		s.installed = true
		if hra.winEOF {
			c.discardUnawaitedSlotsLocked(hra)
		}
	} else {
		s.discarded = true
		s.waiterConsumed += uint64(consumed)
	}
}

// acquireWindowLocked 单飞获取覆盖 alignedOffset 的 4 MiB 对齐窗口缓冲（已为当前调用方独立 retain 1 次引用，调用方负责 release）。
// 调用前必须持有 hra.mu，返回时仍持有 hra.mu（等待 IO 期间会临时解锁 hra.mu，但槽位始终驻留 hra.slots 防止并发重复拉取）。
func (c *ReadAheadCoordinator) acquireWindowLocked(
	ctx context.Context,
	hra *handleReadAhead,
	startGen, alignedOffset uint64,
	relOffset, maxConsume int,
	fetch ChunkFetcher,
) (buf *sharedReadBuf, fromCacheOrPrefetch, genMatched bool, err error) {
	for attempt := range 2 {
		if hra.winValid && hra.winOffset == alignedOffset {
			if b := hra.winBuf.retain(); b != nil {
				data := b.bytes()
				if hra.gen == startGen && relOffset >= 0 && relOffset < len(data) && maxConsume > 0 {
					hra.winConsumed = min(len(data), hra.winConsumed+min(maxConsume, len(data)-relOffset))
				}
				return b, true, hra.gen == startGen, nil
			}
		}

		if hra.prevWinValid && hra.prevWinOffset == alignedOffset {
			if b := hra.prevWinBuf.retain(); b != nil {
				data := b.bytes()
				if hra.gen == startGen && relOffset >= 0 && relOffset < len(data) && maxConsume > 0 {
					hra.prevWinConsumed = min(len(data), hra.prevWinConsumed+min(maxConsume, len(data)-relOffset))
				}
				return b, true, hra.gen == startGen, nil
			}
		}

		if s := c.findSlotLocked(hra, alignedOffset); s != nil {
			wasPrefetch := s.isPrefetch
			s.waiters++
			if !s.completed {
				hra.mu.Unlock()
				waitErr := c.waitDone(ctx, s.done, s.spanCtx)
				hra.mu.Lock()
				if waitErr != nil {
					s.waiters--
					c.maybeReleaseSlotBufLocked(s)
					return nil, false, false, waitErr
				}
			}

			if s.err != nil {
				slotErr := s.err
				s.waiters--
				c.removeSlotLocked(hra, s)
				c.maybeReleaseSlotBufLocked(s)
				if attempt == 0 && ctx.Err() == nil && (s.discarded || !wasPrefetch) {
					continue
				}
				return nil, false, false, slotErr
			}

			callerBuf := s.buf.retain()
			c.consumeSlotResultLocked(hra, s, callerBuf, startGen, relOffset, maxConsume)
			s.waiters--
			c.maybeReleaseSlotBufLocked(s)
			return callerBuf, wasPrefetch, hra.gen == startGen, nil
		}

		s := &windowSlot{
			offset:  alignedOffset,
			gen:     hra.gen,
			done:    make(chan struct{}),
			spanCtx: trace.SpanContextFromContext(ctx),
			waiters: 1,
		}
		c.insertSlotLocked(hra, s)
		hra.mu.Unlock()

		payload, fetchErr := fetch(ctx, alignedOffset, MaxRPCChunkSize)
		var fetchedBuf *sharedReadBuf
		var fetchedLen uint64
		if fetchErr == nil {
			fetchedBuf = newSharedReadBuf(payload)
			fetchedLen = uint64(len(fetchedBuf.bytes()))
			c.metrics.Load().addReadAheadPrefetchedBytes(fetchedLen)
		}

		hra.mu.Lock()
		s.completed = true
		s.err = fetchErr
		s.buf = fetchedBuf
		s.fetchedBytes = fetchedLen
		if fetchErr != nil {
			c.removeSlotLocked(hra, s)
			s.waiters--
			c.maybeReleaseSlotBufLocked(s)
			close(s.done)
			return nil, false, false, fetchErr
		}

		callerBuf := s.buf.retain()
		c.consumeSlotResultLocked(hra, s, callerBuf, startGen, relOffset, maxConsume)
		s.waiters--
		c.maybeReleaseSlotBufLocked(s)
		close(s.done)
		return callerBuf, false, hra.gen == startGen, nil
	}

	return nil, false, false, ctx.Err()
}

// Read 在指定文件句柄上执行带预读缓存与单飞窗口协调的读取，将结果写入 dest 并返回实际读取字节数。
func (c *ReadAheadCoordinator) Read(ctx context.Context, fh, ino, offset uint64, dest []byte, fetch ChunkFetcher) (int, error) {
	want := len(dest)
	if want == 0 {
		return 0, nil
	}

	hra := c.getOrCreateHandle(fh, ino)

	if want <= MaxRPCChunkSize {
		hra.mu.Lock()
		hra.reads++
		if hra.reads == 1 {
			hra.firstWinStart = offset &^ uint64(MaxRPCChunkSize-1)
			hra.hasFirstWin = true
		}
		startGen := hra.gen

		// 1. 完整落在刚滑出的上一 4 MiB 窗口内（内核并发 readahead 跨边界乱序到达），直接从内存返回。
		if hra.prevWinValid && offset >= hra.prevWinOffset && offset+uint64(want) <= hra.prevWinOffset+uint64(len(hra.prevWinData())) {
			prevData := hra.prevWinData()
			relOff := int(offset - hra.prevWinOffset)
			copy(dest[:want], prevData[relOff:relOff+want])
			hra.prevWinConsumed = min(len(prevData), hra.prevWinConsumed+want)
			hra.seqCount++
			hra.streamEnd = max(hra.streamEnd, offset+uint64(want))
			hra.mu.Unlock()
			c.metrics.Load().addReadAheadHit(uint64(want))
			return want, nil
		}

		// 2. 命中当前活跃预读窗口（含跨窗口边界拼接）。
		if hra.winValid && offset >= hra.winOffset && offset < hra.winOffset+uint64(len(hra.winData())) {
			winData := hra.winData()
			relOff := int(offset - hra.winOffset)
			avail := len(winData) - relOff
			if avail >= want || hra.winEOF {
				n := min(want, avail)
				copy(dest[:n], winData[relOff:relOff+n])
				hra.winConsumed = min(len(winData), hra.winConsumed+n)
				hra.lastReadEnd = offset + uint64(n)
				hra.hasLastRead = true
				hra.seqCount++
				hra.streamEnd = max(hra.streamEnd, hra.lastReadEnd)
				if !hra.winEOF {
					c.ensurePrefetchPipelineLocked(ctx, hra, fetch)
				}
				hra.mu.Unlock()
				c.metrics.Load().addReadAheadHit(uint64(n))
				return n, nil
			}

			// 跨窗口边界读取：前缀直接写入 dest[:avail]，后缀单飞衔接下一 4 MiB 窗口
			copy(dest[:avail], winData[relOff:relOff+avail])
			hra.winConsumed = len(winData)
			nextWinOffset := hra.winOffset + uint64(len(winData))

			nextBuf, fromCacheOrPrefetch, genMatched, err := c.acquireWindowLocked(ctx, hra, startGen, nextWinOffset, 0, want-avail, fetch)
			if err != nil {
				hra.mu.Unlock()
				return 0, err
			}
			nextData := nextBuf.bytes()
			rem := min(want-avail, len(nextData))
			copy(dest[avail:avail+rem], nextData[:rem])
			nextBuf.release()

			total := avail + rem
			if genMatched {
				hra.lastReadEnd = offset + uint64(total)
				hra.hasLastRead = true
				hra.seqCount++
				hra.streamEnd = max(hra.streamEnd, hra.lastReadEnd)
				if !hra.winEOF {
					c.ensurePrefetchPipelineLocked(ctx, hra, fetch)
				}
			}
			hra.mu.Unlock()
			hitBytes := uint64(avail)
			if fromCacheOrPrefetch {
				hitBytes += uint64(rem)
			}
			c.metrics.Load().addReadAheadHit(hitBytes)
			return total, nil
		}

		// 3. 命中在途预取/单飞槽位、跨 prevWin 尾部拼接、连续顺序读或已确认顺序流的内核 readahead 流量，统一走 4 MiB 对齐窗口单飞协调。
		aligned := offset &^ uint64(MaxRPCChunkSize-1)
		inPrevTail := hra.prevWinValid && offset >= hra.prevWinOffset && offset < hra.prevWinOffset+uint64(len(hra.prevWinData()))
		inSlot := c.findSlotLocked(hra, aligned) != nil
		sequential := hra.hasLastRead && offset == hra.lastReadEnd
		// 首窗口并发启动容错：仅当第 2 次读取仍落在句柄首读所在的同一 4 MiB 对齐窗口内时，
		// 视为 Linux FUSE 多后台线程并发下发的同首窗 readahead；跨窗口随机大块跳读（如 fio direct_randread bs=1M）不误触发预读。
		sameInitWindow := hra.hasFirstWin && !hra.winValid && hra.reads == 2 && aligned == hra.firstWinStart
		confirmedStreamReadahead := hra.seqCount >= 2 &&
			offset+uint64(want) > hra.streamEnd && offset < hra.streamEnd+MaxRPCChunkSize
		readahead := want >= readAheadMinWant && hra.reads > 1 && !hra.winEOF &&
			(confirmedStreamReadahead || sameInitWindow)
		if inPrevTail || inSlot || sequential || readahead {
			rel := int(offset - aligned)
			winBuf, fromCacheOrPrefetch, genMatched, err := c.acquireWindowLocked(ctx, hra, startGen, aligned, rel, want, fetch)
			if err != nil {
				hra.mu.Unlock()
				return 0, err
			}
			data := winBuf.bytes()
			winEOF := len(data) < MaxRPCChunkSize

			if rel >= len(data) {
				winBuf.release()
				if genMatched && winEOF {
					hra.mu.Unlock()
					if fromCacheOrPrefetch {
						c.metrics.Load().addReadAheadHit(0)
					}
					return 0, nil
				}
			} else {
				avail := len(data) - rel
				if avail >= want || winEOF {
					n := min(want, avail)
					copy(dest[:n], data[rel:rel+n])
					winBuf.release()
					if genMatched {
						hra.lastReadEnd = offset + uint64(n)
						hra.hasLastRead = true
						hra.seqCount++
						hra.streamEnd = max(hra.streamEnd, hra.lastReadEnd)
						if !hra.winEOF {
							c.ensurePrefetchPipelineLocked(ctx, hra, fetch)
						}
					}
					hra.mu.Unlock()
					if fromCacheOrPrefetch {
						c.metrics.Load().addReadAheadHit(uint64(n))
					}
					return n, nil
				}

				copy(dest[:avail], data[rel:rel+avail])
				nextWinOffset := aligned + uint64(len(data))
				winBuf.release()

				nextBuf, nextFromCacheOrPrefetch, nextGenMatched, err := c.acquireWindowLocked(ctx, hra, startGen, nextWinOffset, 0, want-avail, fetch)
				if err != nil {
					hra.mu.Unlock()
					return 0, err
				}
				nextData := nextBuf.bytes()
				rem := min(want-avail, len(nextData))
				copy(dest[avail:avail+rem], nextData[:rem])
				nextBuf.release()

				total := avail + rem
				if genMatched && nextGenMatched {
					hra.lastReadEnd = offset + uint64(total)
					hra.hasLastRead = true
					hra.seqCount++
					hra.streamEnd = max(hra.streamEnd, hra.lastReadEnd)
					if !hra.winEOF {
						c.ensurePrefetchPipelineLocked(ctx, hra, fetch)
					}
				}
				hra.mu.Unlock()
				var hitBytes uint64
				if fromCacheOrPrefetch {
					hitBytes += uint64(avail)
				}
				if nextFromCacheOrPrefetch {
					hitBytes += uint64(rem)
				}
				if fromCacheOrPrefetch || nextFromCacheOrPrefetch {
					c.metrics.Load().addReadAheadHit(hitBytes)
				}
				return total, nil
			}
		}

		// 4. 首次读取或非连续随机读取：按请求尺寸直通，不产生多余读放大。
		// 若是内核并发 readahead 偶发晚到 1 个窗口的大块尾包（紧邻 prevWin 左侧），仅直通本请求而不清空已推进的前向流水线。
		lateReadaheadStraggler := want >= readAheadMinWant && hra.prevWinValid &&
			offset < hra.prevWinOffset && offset+MaxRPCChunkSize >= hra.prevWinOffset
		if !hra.requestInWindowLocked(offset, want) && !lateReadaheadStraggler {
			c.clearWindowAndPrefetchLocked(hra)
		}
		passGen := hra.gen
		hra.mu.Unlock()

		payload, err := fetch(ctx, offset, uint32(want))
		if err != nil {
			return 0, err
		}
		data := payload.Data
		n := min(want, len(data))
		copy(dest[:n], data[:n])
		payload.Release()

		hra.mu.Lock()
		if hra.gen == passGen {
			end := offset + uint64(n)
			if !hra.winValid || end >= hra.streamEnd {
				hra.lastReadEnd = end
				hra.hasLastRead = true
				if !hra.winValid {
					hra.seqCount = 1
				}
			}
			hra.streamEnd = max(hra.streamEnd, end)
		}
		hra.mu.Unlock()

		return n, nil
	}

	hra.mu.Lock()
	c.clearWindowAndPrefetchLocked(hra)
	startGen := hra.gen
	hra.mu.Unlock()

	var totalRead int
	for totalRead < want {
		chunkReqSize := min(want-totalRead, MaxRPCChunkSize)
		curOffset := offset + uint64(totalRead)

		payload, err := fetch(ctx, curOffset, uint32(chunkReqSize))
		if err != nil {
			if totalRead > 0 {
				break
			}
			return 0, err
		}
		chunkData := payload.Data
		if len(chunkData) == 0 {
			payload.Release()
			break
		}
		n := min(chunkReqSize, len(chunkData))
		copy(dest[totalRead:totalRead+n], chunkData[:n])
		payload.Release()
		totalRead += n

		if n < chunkReqSize {
			break
		}
	}

	hra.mu.Lock()
	if hra.gen == startGen {
		hra.lastReadEnd = offset + uint64(totalRead)
		hra.hasLastRead = true
		hra.seqCount = 1
		hra.streamEnd = max(hra.streamEnd, hra.lastReadEnd)
	}
	hra.mu.Unlock()

	return totalRead, nil
}

// InvalidateInode 递增目标 Inode 下所有打开句柄的代数（gen），取消在途预取并清空缓存窗口。
func (c *ReadAheadCoordinator) InvalidateInode(ino uint64) {
	c.mu.Lock()
	set := c.byInode[ino]
	if len(set) == 0 {
		c.mu.Unlock()
		return
	}
	targets := make([]*handleReadAhead, 0, len(set))
	for _, hra := range set {
		targets = append(targets, hra)
	}
	c.mu.Unlock()

	for _, hra := range targets {
		hra.mu.Lock()
		if hra.ino == ino {
			c.resetHandleLocked(hra)
		}
		hra.mu.Unlock()
	}
}

// RemoveHandle 在本地句柄释放时取消在途预取并移除其预读状态。
func (c *ReadAheadCoordinator) RemoveHandle(fh uint64) {
	c.mu.Lock()
	hra, ok := c.handles[fh]
	if ok {
		delete(c.handles, fh)
		if hra != nil {
			c.unbindInodeLocked(fh, hra.ino)
		}
	}
	c.mu.Unlock()

	if ok && hra != nil {
		hra.mu.Lock()
		c.resetHandleLocked(hra)
		hra.mu.Unlock()
	}
}

// Clear 取消所有句柄的在途预取并清空全部预读状态。
func (c *ReadAheadCoordinator) Clear() {
	c.mu.Lock()
	handles := c.handles
	c.handles = make(map[uint64]*handleReadAhead)
	c.byInode = make(map[uint64]map[uint64]*handleReadAhead)
	c.mu.Unlock()

	for _, hra := range handles {
		hra.mu.Lock()
		c.resetHandleLocked(hra)
		hra.mu.Unlock()
	}
}
