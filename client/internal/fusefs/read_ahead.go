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

const (
	MaxRPCChunkSize            = 4 * 1024 * 1024
	ReadAheadMaxGlobalInflight = 32
	defaultPrefetchTimeout     = 30 * time.Second
)

type ChunkFetcher func(ctx context.Context, offset uint64, length uint32) (rpc.ReadPayload, error)

type sharedReadBuf struct {
	payload rpc.ReadPayload
	refs    atomic.Int32
}

func newSharedReadBuf(p rpc.ReadPayload) *sharedReadBuf {
	b := &sharedReadBuf{payload: p}
	b.refs.Store(1)
	return b
}

func (b *sharedReadBuf) retain() *sharedReadBuf {
	for b != nil {
		if cur := b.refs.Load(); cur <= 0 {
			return nil
		} else if b.refs.CompareAndSwap(cur, cur+1) {
			return b
		}
	}
	return nil
}

func (b *sharedReadBuf) release() {
	if b != nil && b.refs.Add(-1) == 0 {
		b.payload.Release()
	}
}

type prefetchReq struct {
	offset, gen   uint64
	isPrefetch    bool
	done          chan struct{}
	cancel        context.CancelFunc
	spanCtx       trace.SpanContext
	buf           *sharedReadBuf
	err           error
	expiresAtNano int64
	waiters       int
	completed     bool
	attached      bool
	promoted      bool
	discarded     bool
}

type handleReadAhead struct {
	mu                               sync.Mutex
	ino, gen, lastReadEnd, winOffset uint64
	hasLastRead, winValid, winEOF    bool
	winConsumed                      int
	winExpiresAtNano                 int64
	winBuf                           *sharedReadBuf
	pf                               *prefetchReq
}

type ReadAheadCoordinator struct {
	mu             sync.Mutex
	readOnly       bool
	windowTTL      float64
	nowFn          func() time.Time
	handles        map[uint64]*handleReadAhead
	inflight       atomic.Int32
	metrics        atomic.Pointer[Metrics]
	tracerProvider atomic.Pointer[trace.TracerProvider]
}

func NewReadAheadCoordinator() *ReadAheadCoordinator {
	return &ReadAheadCoordinator{handles: make(map[uint64]*handleReadAhead)}
}

func (c *ReadAheadCoordinator) configureReadOnly(windowTTL float64, nowFn func() time.Time) {
	c.mu.Lock()
	c.readOnly, c.windowTTL, c.nowFn = true, windowTTL, nowFn
	c.mu.Unlock()
}

func (c *ReadAheadCoordinator) bindMetrics(m *Metrics) { c.metrics.Store(m) }

func (c *ReadAheadCoordinator) bindTracerProvider(tp trace.TracerProvider) {
	if tp == nil {
		c.tracerProvider.Store(nil)
		return
	}
	c.tracerProvider.Store(&tp)
}

func (c *ReadAheadCoordinator) activeTracer() (trace.Tracer, bool) {
	if ptr := c.tracerProvider.Load(); ptr != nil && *ptr != nil {
		if _, isNoop := (*ptr).(noop.TracerProvider); !isNoop {
			return (*ptr).Tracer("bowl/client/fusefs"), true
		}
	}
	return nil, false
}

func (c *ReadAheadCoordinator) nowNano() int64 {
	if c.nowFn != nil {
		return c.nowFn().UnixNano()
	}
	return time.Now().UnixNano()
}

func (c *ReadAheadCoordinator) ttlDur() time.Duration {
	if c.windowTTL <= 0 {
		return 0
	}
	return time.Duration(c.windowTTL * float64(time.Second))
}

func (c *ReadAheadCoordinator) ttlDisabled() bool { return c.readOnly && c.ttlDur() <= 0 }

func (c *ReadAheadCoordinator) stampExpiry() int64 {
	if d := c.ttlDur(); d > 0 {
		return c.nowNano() + int64(d)
	}
	return 0
}

func (c *ReadAheadCoordinator) expireLocked(hra *handleReadAhead) {
	if !c.readOnly {
		return
	}
	now := c.nowNano()
	if (hra.winValid || hra.winEOF) && now >= hra.winExpiresAtNano {
		c.discardWinLocked(hra)
	}
	if hra.pf != nil && hra.pf.completed && hra.pf.err == nil && now >= hra.pf.expiresAtNano {
		c.cancelPFLocked(hra, false)
	}
}

func (c *ReadAheadCoordinator) releasePFLocked(pf *prefetchReq) {
	if pf == nil || !pf.completed || pf.attached || pf.waiters > 0 {
		return
	}
	if !pf.promoted && pf.discarded && pf.err == nil && pf.buf != nil {
		c.metrics.Load().addReadAheadDiscardedBytes(uint64(len(pf.buf.payload.Data)))
	}
	pf.buf.release()
	pf.buf = nil
}

func (c *ReadAheadCoordinator) discardWinLocked(hra *handleReadAhead) {
	if hra.winValid && hra.winBuf != nil {
		if rem := len(hra.winBuf.payload.Data) - hra.winConsumed; rem > 0 {
			c.metrics.Load().addReadAheadDiscardedBytes(uint64(rem))
		}
	}
	hra.winBuf.release()
	hra.winBuf, hra.winOffset, hra.winValid, hra.winEOF, hra.winConsumed, hra.winExpiresAtNano = nil, 0, false, false, 0, 0
}

func (c *ReadAheadCoordinator) cancelPFLocked(hra *handleReadAhead, force bool) {
	if pf := hra.pf; pf != nil {
		hra.pf, pf.attached, pf.discarded = nil, false, true
		if (force || pf.waiters == 0) && pf.cancel != nil {
			pf.cancel()
		}
		c.releasePFLocked(pf)
	}
}

func (c *ReadAheadCoordinator) resetLocked(hra *handleReadAhead, force bool) {
	hra.gen++
	hra.hasLastRead, hra.lastReadEnd = false, 0
	c.discardWinLocked(hra)
	c.cancelPFLocked(hra, force)
}

func (c *ReadAheadCoordinator) waitDone(ctx context.Context, done <-chan struct{}, linkSC trace.SpanContext) error {
	select {
	case <-done:
		return nil
	default:
	}
	start := time.Now()
	var span trace.Span = noopSpanInstance
	if tracer, ok := c.activeTracer(); ok {
		var opts []trace.SpanStartOption
		if linkSC.IsValid() {
			opts = append(opts, trace.WithLinks(trace.Link{SpanContext: linkSC}))
		}
		_, span = tracer.Start(ctx, "read_ahead.Wait", opts...)
	}
	var err error
	select {
	case <-done:
	case <-ctx.Done():
		err = ctx.Err()
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	}
	span.End()
	c.metrics.Load().observeReadAheadWait(time.Since(start))
	return err
}

func (c *ReadAheadCoordinator) launchPrefetchLocked(triggerCtx context.Context, hra *handleReadAhead, fetch ChunkFetcher) {
	if c.ttlDisabled() || fetch == nil || !hra.winValid || hra.winEOF || hra.pf != nil || len(hra.winBuf.payload.Data) < MaxRPCChunkSize {
		return
	}
	for {
		cur := c.inflight.Load()
		if cur >= ReadAheadMaxGlobalInflight {
			return
		}
		if c.inflight.CompareAndSwap(cur, cur+1) {
			break
		}
	}
	nextOff := hra.winOffset + MaxRPCChunkSize
	baseCtx := context.Background()
	var span trace.Span = noopSpanInstance
	if tracer, ok := c.activeTracer(); ok {
		opts := []trace.SpanStartOption{
			trace.WithNewRoot(),
			trace.WithAttributes(attribute.Int64("read_ahead.ino", int64(hra.ino)), attribute.Int64("read_ahead.offset", int64(nextOff)), attribute.Int("read_ahead.length", MaxRPCChunkSize)),
		}
		if sc := trace.SpanContextFromContext(triggerCtx); sc.IsValid() {
			opts = append(opts, trace.WithLinks(trace.Link{SpanContext: sc}))
		}
		baseCtx, span = tracer.Start(baseCtx, "read_ahead.Prefetch", opts...)
	}
	pfCtx, pfCancel := context.WithTimeout(baseCtx, defaultPrefetchTimeout)
	pf := &prefetchReq{offset: nextOff, gen: hra.gen, isPrefetch: true, attached: true, done: make(chan struct{}), cancel: pfCancel, spanCtx: span.SpanContext()}
	hra.pf = pf
	c.metrics.Load().incReadAheadTrigger()
	c.metrics.Load().addReadAheadInflight(1)
	go func() {
		released := false
		finishBudget := func() {
			if !released {
				released = true
				pfCancel()
				span.End()
				c.inflight.Add(-1)
				c.metrics.Load().addReadAheadInflight(-1)
			}
		}
		defer finishBudget()
		payload, err := fetch(pfCtx, nextOff, MaxRPCChunkSize)
		var buf *sharedReadBuf
		var exp int64
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
		} else {
			buf = newSharedReadBuf(payload)
			span.SetAttributes(attribute.Int64("read_ahead.fetched_bytes", int64(len(buf.payload.Data))))
			c.metrics.Load().addReadAheadPrefetchedBytes(uint64(len(buf.payload.Data)))
			if c.readOnly {
				exp = c.stampExpiry()
			}
		}
		finishBudget()
		hra.mu.Lock()
		pf.completed, pf.err, pf.buf, pf.expiresAtNano = true, err, buf, exp
		if pf.discarded || hra.gen != pf.gen {
			pf.discarded = true
		}
		if err != nil || pf.discarded {
			if hra.pf == pf {
				hra.pf, pf.attached = nil, false
			}
		}
		c.releasePFLocked(pf)
		close(pf.done)
		hra.mu.Unlock()
	}()
}

func (c *ReadAheadCoordinator) promoteLocked(hra *handleReadAhead, pf *prefetchReq, buf *sharedReadBuf, startGen uint64, rel, want int) {
	data := buf.payload.Data
	consumed := 0
	if rel >= 0 && rel < len(data) {
		consumed = min(want, len(data)-rel)
	}
	if pf.promoted {
		if hra.gen == startGen && hra.winValid && hra.winOffset == pf.offset {
			hra.winConsumed = min(len(hra.winBuf.payload.Data), hra.winConsumed+consumed)
		}
		return
	}
	if hra.pf == pf {
		hra.pf, pf.attached = nil, false
	}
	if pf.discarded || hra.gen != pf.gen || hra.gen != startGen {
		pf.discarded = true
		return
	}
	pf.promoted = true
	c.discardWinLocked(hra)
	hra.winOffset, hra.winEOF, hra.winExpiresAtNano = pf.offset, len(data) < MaxRPCChunkSize, pf.expiresAtNano
	if len(data) > 0 {
		hra.winBuf = buf.retain()
		hra.winValid = hra.winBuf != nil
	}
	hra.winConsumed = min(len(data), consumed)
}

func (c *ReadAheadCoordinator) acquireLocked(ctx context.Context, hra *handleReadAhead, startGen, aligned uint64, rel, want int, fetch ChunkFetcher) (*sharedReadBuf, bool, error) {
	for attempt := range 2 {
		c.expireLocked(hra)
		if hra.winValid && hra.winOffset == aligned {
			if b := hra.winBuf.retain(); b != nil {
				if hra.gen == startGen && rel >= 0 && rel < len(b.payload.Data) {
					hra.winConsumed = min(len(b.payload.Data), hra.winConsumed+min(want, len(b.payload.Data)-rel))
				}
				return b, true, nil
			}
		}
		if pf := hra.pf; pf != nil && pf.offset == aligned {
			pf.waiters++
			if !pf.completed {
				hra.mu.Unlock()
				wErr := c.waitDone(ctx, pf.done, pf.spanCtx)
				hra.mu.Lock()
				if wErr != nil {
					pf.waiters--
					c.releasePFLocked(pf)
					return nil, false, wErr
				}
			}
			expired := c.readOnly && pf.err == nil && (pf.discarded || c.nowNano() >= pf.expiresAtNano)
			if pf.err != nil || expired {
				slotErr, retry := pf.err, attempt == 0 && ctx.Err() == nil && (expired || pf.discarded || !pf.isPrefetch)
				pf.waiters--
				if expired {
					pf.discarded = true
				}
				if hra.pf == pf {
					hra.pf, pf.attached = nil, false
				}
				c.releasePFLocked(pf)
				if retry {
					continue
				}
				return nil, false, slotErr
			}
			buf := pf.buf.retain()
			c.promoteLocked(hra, pf, buf, startGen, rel, want)
			pf.waiters--
			c.releasePFLocked(pf)
			return buf, pf.isPrefetch, nil
		}
		c.cancelPFLocked(hra, false)
		pf := &prefetchReq{offset: aligned, gen: hra.gen, attached: true, done: make(chan struct{}), spanCtx: trace.SpanContextFromContext(ctx), waiters: 1}
		hra.pf = pf
		hra.mu.Unlock()
		payload, fErr := fetch(ctx, aligned, MaxRPCChunkSize)
		var buf *sharedReadBuf
		var exp int64
		if fErr == nil {
			buf = newSharedReadBuf(payload)
			c.metrics.Load().addReadAheadPrefetchedBytes(uint64(len(buf.payload.Data)))
			if c.readOnly {
				exp = c.stampExpiry()
			}
		}
		hra.mu.Lock()
		pf.completed, pf.err, pf.buf, pf.expiresAtNano = true, fErr, buf, exp
		if fErr != nil {
			if hra.pf == pf {
				hra.pf, pf.attached = nil, false
			}
			pf.waiters--
			c.releasePFLocked(pf)
			close(pf.done)
			return nil, false, fErr
		}
		callerBuf := buf.retain()
		c.promoteLocked(hra, pf, callerBuf, startGen, rel, want)
		pf.waiters--
		c.releasePFLocked(pf)
		close(pf.done)
		return callerBuf, false, nil
	}
	return nil, false, ctx.Err()
}

func (c *ReadAheadCoordinator) Read(ctx context.Context, fh, ino, offset uint64, dest []byte, fetch ChunkFetcher) (int, error) {
	want := len(dest)
	if want == 0 {
		return 0, nil
	}
	if c.ttlDisabled() {
		return readDirectChunks(ctx, offset, dest, fetch)
	}
	hra := c.getOrCreateHandle(fh, ino)
	hra.mu.Lock()
	if want <= MaxRPCChunkSize {
		c.expireLocked(hra)
		startGen, aligned := hra.gen, offset&^uint64(MaxRPCChunkSize-1)
		inWin := hra.winValid && offset >= hra.winOffset && offset < hra.winOffset+uint64(len(hra.winBuf.payload.Data))
		if inWin {
			winData, rel := hra.winBuf.payload.Data, int(offset-hra.winOffset)
			if avail := len(winData) - rel; avail >= want || hra.winEOF {
				n := min(want, avail)
				copy(dest[:n], winData[rel:rel+n])
				hra.winConsumed = min(len(winData), hra.winConsumed+n)
				hra.lastReadEnd, hra.hasLastRead = offset+uint64(n), true
				c.launchPrefetchLocked(ctx, hra, fetch)
				hra.mu.Unlock()
				c.metrics.Load().addReadAheadHit(uint64(n))
				return n, nil
			}
		}
		if inWin || (hra.pf != nil && hra.pf.offset == aligned) || (hra.hasLastRead && offset == hra.lastReadEnd) {
			rel := int(offset - aligned)
			buf, fromCache, err := c.acquireLocked(ctx, hra, startGen, aligned, rel, want, fetch)
			if err != nil {
				hra.mu.Unlock()
				return 0, err
			}
			data := buf.payload.Data
			if rel >= len(data) {
				buf.release()
				hra.mu.Unlock()
				if fromCache {
					c.metrics.Load().addReadAheadHit(0)
				}
				return 0, nil
			}
			avail := len(data) - rel
			total, hitBytes := min(want, avail), uint64(0)
			copy(dest[:total], data[rel:rel+total])
			if fromCache {
				hitBytes += uint64(total)
			}
			buf.release()
			if avail < want && len(data) == MaxRPCChunkSize {
				nextBuf, nextCache, nextErr := c.acquireLocked(ctx, hra, startGen, aligned+MaxRPCChunkSize, 0, want-avail, fetch)
				if nextErr != nil {
					hra.mu.Unlock()
					return 0, nextErr
				}
				rem := min(want-avail, len(nextBuf.payload.Data))
				copy(dest[avail:avail+rem], nextBuf.payload.Data[:rem])
				nextBuf.release()
				total += rem
				if nextCache {
					hitBytes += uint64(rem)
					fromCache = true
				}
			}
			if hra.gen == startGen {
				hra.lastReadEnd, hra.hasLastRead = offset+uint64(total), true
				c.launchPrefetchLocked(ctx, hra, fetch)
			}
			hra.mu.Unlock()
			if fromCache {
				c.metrics.Load().addReadAheadHit(hitBytes)
			}
			return total, nil
		}
	}
	c.resetLocked(hra, false)
	passGen := hra.gen
	hra.mu.Unlock()
	n, err := readDirectChunks(ctx, offset, dest, fetch)
	if err == nil {
		hra.mu.Lock()
		if hra.gen == passGen {
			hra.lastReadEnd, hra.hasLastRead = offset+uint64(n), true
		}
		hra.mu.Unlock()
	}
	return n, err
}

func (c *ReadAheadCoordinator) getOrCreateHandle(fh, ino uint64) *handleReadAhead {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.handles == nil {
		c.handles = make(map[uint64]*handleReadAhead)
	}
	hra, ok := c.handles[fh]
	if !ok {
		hra = &handleReadAhead{ino: ino}
		c.handles[fh] = hra
		return hra
	}
	hra.mu.Lock()
	if hra.ino != ino {
		c.resetLocked(hra, true)
		hra.ino = ino
	}
	hra.mu.Unlock()
	return hra
}

func (c *ReadAheadCoordinator) InvalidateInode(ino uint64) {
	c.mu.Lock()
	var targets []*handleReadAhead
	for _, hra := range c.handles {
		targets = append(targets, hra)
	}
	c.mu.Unlock()
	for _, hra := range targets {
		hra.mu.Lock()
		if hra.ino == ino {
			c.resetLocked(hra, true)
		}
		hra.mu.Unlock()
	}
}

func (c *ReadAheadCoordinator) RemoveHandle(fh uint64) {
	c.mu.Lock()
	hra, ok := c.handles[fh]
	delete(c.handles, fh)
	c.mu.Unlock()
	if ok && hra != nil {
		hra.mu.Lock()
		c.resetLocked(hra, true)
		hra.mu.Unlock()
	}
}

func (c *ReadAheadCoordinator) InvalidateAll() {
	if c != nil {
		c.Clear()
	}
}

func (c *ReadAheadCoordinator) Clear() {
	c.mu.Lock()
	handles := c.handles
	c.handles = make(map[uint64]*handleReadAhead)
	c.mu.Unlock()
	for _, hra := range handles {
		hra.mu.Lock()
		c.resetLocked(hra, true)
		hra.mu.Unlock()
	}
}
