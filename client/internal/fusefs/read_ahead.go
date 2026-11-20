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
	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
)

const MaxRPCChunkSize = 4 * 1024 * 1024

const ReadAheadMaxPrefetch = 4

const ReadAheadMaxGlobalInflight = 32

const defaultPrefetchTimeout = 30 * time.Second

const readAheadMinWant = 128 * 1024

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

type windowSlot struct {
	offset     uint64
	gen        uint64
	isPrefetch bool
	done       chan struct{}
	cancel     context.CancelFunc
	spanCtx    trace.SpanContext

	inSlots        bool
	completed      bool
	installed      bool
	discarded      bool
	discardCounted bool
	waiters        int
	waiterConsumed uint64
	fetchedBytes   uint64
	expiresAtNano  int64
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

	streamEnd uint64

	reads            int
	firstWinStart    uint64
	hasFirstWin      bool
	winOffset        uint64
	winBuf           *sharedReadBuf
	winValid         bool
	winEOF           bool
	winConsumed      int
	winExpiresAtNano int64

	prevWinOffset        uint64
	prevWinBuf           *sharedReadBuf
	prevWinValid         bool
	prevWinConsumed      int
	prevWinExpiresAtNano int64

	slots []*windowSlot
}

func (hra *handleReadAhead) winData() []byte {
	return hra.winBuf.bytes()
}

func (hra *handleReadAhead) prevWinData() []byte {
	return hra.prevWinBuf.bytes()
}

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

type ChunkFetcher func(ctx context.Context, offset uint64, length uint32) (rpc.ReadPayload, error)

type inodeStamp struct {
	generation uint64
	size       uint64
	mtimeSec   int64
	mtimeNsec  int32
	ctimeSec   int64
	ctimeNsec  int32
}

type inodeTrack struct {
	openCount int
	hasStamp  bool
	stamp     inodeStamp
}

func stampFromInode(inode *storagev1.Inode) (inodeStamp, bool) {
	if inode == nil || inode.GetAttr() == nil {
		return inodeStamp{}, false
	}
	attr := inode.GetAttr()
	mtime := attr.GetMtime()
	ctime := attr.GetCtime()
	return inodeStamp{
		generation: inode.GetGeneration(),
		size:       attr.GetSize(),
		mtimeSec:   mtime.GetSeconds(),
		mtimeNsec:  mtime.GetNanos(),
		ctimeSec:   ctime.GetSeconds(),
		ctimeNsec:  ctime.GetNanos(),
	}, true
}

type ReadAheadCoordinator struct {
	mu                 sync.Mutex
	readOnly           bool
	windowTTL          float64
	nowFn              func() time.Time
	handles            map[uint64]*handleReadAhead
	byInode            map[uint64]map[uint64]*handleReadAhead
	inodeTracks        map[uint64]*inodeTrack
	inflightPrefetches atomic.Int32
	activePipelined    atomic.Int32
	metrics            atomic.Pointer[Metrics]
	tracerProvider     atomic.Pointer[trace.TracerProvider]
}

func NewReadAheadCoordinator() *ReadAheadCoordinator {
	return &ReadAheadCoordinator{
		handles:     make(map[uint64]*handleReadAhead),
		byInode:     make(map[uint64]map[uint64]*handleReadAhead),
		inodeTracks: make(map[uint64]*inodeTrack),
	}
}

func (c *ReadAheadCoordinator) configureReadOnly(windowTTL float64, nowFn func() time.Time) {
	c.mu.Lock()
	c.readOnly = true
	c.windowTTL = windowTTL
	c.nowFn = nowFn
	c.mu.Unlock()
}

func (c *ReadAheadCoordinator) currentTime() time.Time {
	if c.nowFn != nil {
		return c.nowFn()
	}
	return time.Now()
}

func (c *ReadAheadCoordinator) ttlDisabled() bool {
	return c.readOnly && ttlDuration(c.windowTTL) <= 0
}

func (c *ReadAheadCoordinator) stampExpiry() int64 {
	dur := ttlDuration(c.windowTTL)
	if dur <= 0 {
		return 0
	}
	return c.currentTime().Add(dur).UnixNano()
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
	hra.prevWinExpiresAtNano = 0
}

func (c *ReadAheadCoordinator) discardCurrentWindowLocked(hra *handleReadAhead) {
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
	hra.winExpiresAtNano = 0
}

func (c *ReadAheadCoordinator) discardAllWindowsLocked(hra *handleReadAhead) {
	c.discardPrevWindowLocked(hra)
	c.discardCurrentWindowLocked(hra)
}

func (c *ReadAheadCoordinator) discardExpiredSlotsLocked(hra *handleReadAhead, nowNano int64) {
	keep := 0
	for _, s := range hra.slots {
		if s.completed && s.err == nil && nowNano >= s.expiresAtNano {
			s.inSlots = false
			s.discarded = true
			c.maybeReleaseSlotBufLocked(s)
			continue
		}
		hra.slots[keep] = s
		keep++
	}
	clear(hra.slots[keep:])
	hra.slots = hra.slots[:keep]
}

func (c *ReadAheadCoordinator) discardExpiredLocked(hra *handleReadAhead, nowNano int64) {
	if !c.readOnly {
		return
	}
	if hra.prevWinValid && nowNano >= hra.prevWinExpiresAtNano {
		c.discardPrevWindowLocked(hra)
	}
	if (hra.winValid || hra.winEOF) && nowNano >= hra.winExpiresAtNano {
		c.discardCurrentWindowLocked(hra)
	}
	c.discardExpiredSlotsLocked(hra, nowNano)
}

func (c *ReadAheadCoordinator) installWindowMonotonicLocked(
	hra *handleReadAhead,
	offset uint64,
	buf *sharedReadBuf,
	eof bool,
	consumed int,
	expiresAtNano int64,
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
				hra.prevWinExpiresAtNano = expiresAtNano
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
		hra.prevWinExpiresAtNano = hra.winExpiresAtNano
		hra.winBuf = nil
		hra.winValid = false
		hra.winConsumed = 0
		hra.winExpiresAtNano = 0
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
	hra.winExpiresAtNano = expiresAtNano
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
		var expiresAtNano int64
		if err != nil {
			pfSpan.RecordError(err)
			pfSpan.SetStatus(codes.Error, err.Error())
		} else {
			fetchedBuf = newSharedReadBuf(payload)
			fetchedLen = uint64(len(fetchedBuf.bytes()))
			pfSpan.SetAttributes(attribute.Int64("read_ahead.fetched_bytes", int64(fetchedLen)))
			c.metrics.Load().addReadAheadPrefetchedBytes(fetchedLen)
			if c.readOnly {
				expiresAtNano = c.stampExpiry()
			}
		}

		releaseBudget()

		hra.mu.Lock()
		s.completed = true
		s.err = err
		s.buf = fetchedBuf
		s.fetchedBytes = fetchedLen
		s.expiresAtNano = expiresAtNano
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

func (c *ReadAheadCoordinator) ensurePrefetchPipelineLocked(triggerCtx context.Context, hra *handleReadAhead, fetch ChunkFetcher) {
	if c.ttlDisabled() || fetch == nil || !hra.winValid || hra.winEOF {
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
	var nowNano int64
	if c.readOnly {
		nowNano = c.currentTime().UnixNano()
	}

	keep := 0
	for _, s := range hra.slots {
		outOfRange := s.offset <= hra.winOffset || s.offset > maxAllowedOffset || (hasEOFCutoff && s.offset > eofCutoff)
		expired := c.readOnly && s.completed && s.err == nil && nowNano >= s.expiresAtNano
		if (s.discarded || outOfRange || expired) && s.waiters == 0 {
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
			if !hra.winValid {
				hra.winExpiresAtNano = s.expiresAtNano
			}
			c.discardUnawaitedSlotsLocked(hra)
		}
		return
	}

	winEOF := len(data) < MaxRPCChunkSize
	if c.installWindowMonotonicLocked(hra, s.offset, buf, winEOF, consumed, s.expiresAtNano) {
		s.installed = true
		if hra.winEOF {
			c.discardUnawaitedSlotsLocked(hra)
		}
	} else {
		s.discarded = true
		s.waiterConsumed += uint64(consumed)
	}
}

func (c *ReadAheadCoordinator) acquireWindowLocked(
	ctx context.Context,
	hra *handleReadAhead,
	startGen, alignedOffset uint64,
	relOffset, maxConsume int,
	fetch ChunkFetcher,
) (buf *sharedReadBuf, fromCacheOrPrefetch, genMatched bool, err error) {
	for attempt := range 2 {
		if c.readOnly {
			c.discardExpiredLocked(hra, c.currentTime().UnixNano())
		}
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

			if c.readOnly && attempt == 0 && ctx.Err() == nil && (s.discarded || c.currentTime().UnixNano() >= s.expiresAtNano) {
				s.waiters--
				s.discarded = true
				c.removeSlotLocked(hra, s)
				c.maybeReleaseSlotBufLocked(s)
				continue
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
		var expiresAtNano int64
		if fetchErr == nil {
			fetchedBuf = newSharedReadBuf(payload)
			fetchedLen = uint64(len(fetchedBuf.bytes()))
			c.metrics.Load().addReadAheadPrefetchedBytes(fetchedLen)
			if c.readOnly {
				expiresAtNano = c.stampExpiry()
			}
		}

		hra.mu.Lock()
		s.completed = true
		s.err = fetchErr
		s.buf = fetchedBuf
		s.fetchedBytes = fetchedLen
		s.expiresAtNano = expiresAtNano
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

func (c *ReadAheadCoordinator) Read(ctx context.Context, fh, ino, offset uint64, dest []byte, fetch ChunkFetcher) (int, error) {
	want := len(dest)
	if want == 0 {
		return 0, nil
	}

	if c.ttlDisabled() && want <= MaxRPCChunkSize {
		payload, err := fetch(ctx, offset, uint32(want))
		if err != nil {
			return 0, err
		}
		n := min(want, len(payload.Data))
		copy(dest[:n], payload.Data[:n])
		payload.Release()
		return n, nil
	}

	hra := c.getOrCreateHandle(fh, ino)

	if want <= MaxRPCChunkSize {
		hra.mu.Lock()
		if c.readOnly {
			c.discardExpiredLocked(hra, c.currentTime().UnixNano())
		}
		hra.reads++
		if hra.reads == 1 {
			hra.firstWinStart = offset &^ uint64(MaxRPCChunkSize-1)
			hra.hasFirstWin = true
		}
		startGen := hra.gen

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

		aligned := offset &^ uint64(MaxRPCChunkSize-1)
		inPrevTail := hra.prevWinValid && offset >= hra.prevWinOffset && offset < hra.prevWinOffset+uint64(len(hra.prevWinData()))
		inSlot := c.findSlotLocked(hra, aligned) != nil
		sequential := hra.hasLastRead && offset == hra.lastReadEnd

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

func (c *ReadAheadCoordinator) collectTargetsLocked(ino uint64) []*handleReadAhead {
	set := c.byInode[ino]
	if len(set) == 0 {
		return nil
	}
	targets := make([]*handleReadAhead, 0, len(set))
	for _, hra := range set {
		targets = append(targets, hra)
	}
	return targets
}

func (c *ReadAheadCoordinator) invalidateTargets(ino uint64, targets []*handleReadAhead) {
	for _, hra := range targets {
		hra.mu.Lock()
		if hra.ino == ino {
			c.resetHandleLocked(hra)
		}
		hra.mu.Unlock()
	}
}

func (c *ReadAheadCoordinator) updateTrackStampLocked(tr *inodeTrack, ino uint64, stamp inodeStamp) []*handleReadAhead {
	var targets []*handleReadAhead
	if tr.hasStamp && tr.stamp != stamp && len(c.byInode[ino]) > 0 {
		targets = c.collectTargetsLocked(ino)
	}
	tr.stamp = stamp
	tr.hasStamp = true
	return targets
}

func (c *ReadAheadCoordinator) TrackOpenInode(ino uint64, inode *storagev1.Inode) {
	if !c.readOnly {
		return
	}
	stamp, hasStamp := stampFromInode(inode)
	c.mu.Lock()
	if c.inodeTracks == nil {
		c.inodeTracks = make(map[uint64]*inodeTrack)
	}
	tr := c.inodeTracks[ino]
	if tr == nil {
		tr = &inodeTrack{}
		c.inodeTracks[ino] = tr
	}
	tr.openCount++
	if hasStamp && !tr.hasStamp {
		tr.stamp = stamp
		tr.hasStamp = true
	}
	c.mu.Unlock()
}

func (c *ReadAheadCoordinator) UntrackOpenInode(ino uint64) {
	if !c.readOnly {
		return
	}
	c.mu.Lock()
	if tr := c.inodeTracks[ino]; tr != nil {
		if tr.openCount > 0 {
			tr.openCount--
		}
		if tr.openCount == 0 && len(c.byInode[ino]) == 0 {
			delete(c.inodeTracks, ino)
		}
	}
	c.mu.Unlock()
}

func (c *ReadAheadCoordinator) ObserveInode(ino uint64, inode *storagev1.Inode) {
	if !c.readOnly {
		return
	}
	stamp, ok := stampFromInode(inode)
	if !ok {
		return
	}
	c.mu.Lock()
	if c.inodeTracks == nil {
		c.inodeTracks = make(map[uint64]*inodeTrack)
	}
	tr := c.inodeTracks[ino]
	if tr == nil {
		if len(c.byInode[ino]) == 0 {
			c.mu.Unlock()
			return
		}
		c.inodeTracks[ino] = &inodeTrack{hasStamp: true, stamp: stamp}
		c.mu.Unlock()
		return
	}
	if tr.hasStamp && tr.stamp == stamp {
		c.mu.Unlock()
		return
	}
	targets := c.updateTrackStampLocked(tr, ino, stamp)
	c.mu.Unlock()
	c.invalidateTargets(ino, targets)
}

func (c *ReadAheadCoordinator) InvalidateInode(ino uint64) {
	c.mu.Lock()
	targets := c.collectTargetsLocked(ino)
	c.mu.Unlock()

	c.invalidateTargets(ino, targets)
}

func (c *ReadAheadCoordinator) RemoveHandle(fh uint64) {
	c.mu.Lock()
	hra, ok := c.handles[fh]
	if ok {
		delete(c.handles, fh)
		if hra != nil {
			c.unbindInodeLocked(fh, hra.ino)
			if c.readOnly {
				if tr := c.inodeTracks[hra.ino]; tr != nil && tr.openCount == 0 && len(c.byInode[hra.ino]) == 0 {
					delete(c.inodeTracks, hra.ino)
				}
			}
		}
	}
	c.mu.Unlock()

	if ok && hra != nil {
		hra.mu.Lock()
		c.resetHandleLocked(hra)
		hra.mu.Unlock()
	}
}

func (c *ReadAheadCoordinator) InvalidateAll() {
	if c == nil {
		return
	}
	c.Clear()
}

func (c *ReadAheadCoordinator) Clear() {
	c.mu.Lock()
	handles := c.handles
	c.handles = make(map[uint64]*handleReadAhead)
	c.byInode = make(map[uint64]map[uint64]*handleReadAhead)
	c.inodeTracks = make(map[uint64]*inodeTrack)
	c.mu.Unlock()

	for _, hra := range handles {
		hra.mu.Lock()
		c.resetHandleLocked(hra)
		hra.mu.Unlock()
	}
}
