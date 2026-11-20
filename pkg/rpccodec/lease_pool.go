package rpccodec

import (
	"runtime"
	"sync"
	"sync/atomic"
	"weak"
)

const (
	MaxPayloadSize = 4 << 20
)

type OrphanPolicy uint8

const (
	OrphanDropEntryOnly OrphanPolicy = iota

	OrphanRecycleBuffer
)

type leaseReleaser interface {
	release(stopGuardCleanup bool)
}

type Guard struct {
	lease leaseReleaser
}

func (g *Guard) Release() {
	if g == nil || g.lease == nil {
		return
	}
	g.lease.release(true)
}

type payloadLease[T any] struct {
	pool         *PayloadLeasePool[T]
	key          weak.Pointer[T]
	buf          *[MaxPayloadSize]byte
	msgCleanup   runtime.Cleanup
	guardCleanup runtime.Cleanup
	claimed      atomic.Bool
	released     atomic.Bool
}

func (l *payloadLease[T]) release(stopGuardCleanup bool) {
	if l == nil || !l.released.CompareAndSwap(false, true) {
		return
	}
	if stopGuardCleanup {
		l.guardCleanup.Stop()
	}
	buf := l.buf
	l.buf = nil
	l.pool.RecycleBuf(buf)
}

func cleanupUnclaimedLease[T any](l *payloadLease[T]) {
	p := l.pool
	p.mu.Lock()
	if p.leases[l.key] == l {
		delete(p.leases, l.key)
		p.active.Add(-1)
	}
	p.mu.Unlock()

	if p.policy == OrphanRecycleBuffer && l.claimed.CompareAndSwap(false, true) {
		l.release(false)
	}
}

func cleanupOrphanedGuard[T any](l *payloadLease[T]) {
	l.release(false)
}

type PayloadLeasePool[T any] struct {
	freeCh chan *[MaxPayloadSize]byte
	pool   sync.Pool
	policy OrphanPolicy

	mu     sync.Mutex
	leases map[weak.Pointer[T]]*payloadLease[T]
	active atomic.Int64
}

func NewPayloadLeasePool[T any](retainedCap int, policy OrphanPolicy) *PayloadLeasePool[T] {
	if retainedCap < 0 {
		retainedCap = 0
	}
	return &PayloadLeasePool[T]{
		freeCh: make(chan *[MaxPayloadSize]byte, retainedCap),
		pool: sync.Pool{
			New: func() any {
				return new([MaxPayloadSize]byte)
			},
		},
		policy: policy,
		leases: make(map[weak.Pointer[T]]*payloadLease[T]),
	}
}

func (p *PayloadLeasePool[T]) AcquireBuf() *[MaxPayloadSize]byte {
	select {
	case buf := <-p.freeCh:
		return buf
	default:
	}
	buf, _ := p.pool.Get().(*[MaxPayloadSize]byte)
	if buf == nil {
		return new([MaxPayloadSize]byte)
	}
	return buf
}

func (p *PayloadLeasePool[T]) RecycleBuf(buf *[MaxPayloadSize]byte) {
	if buf == nil {
		return
	}
	select {
	case p.freeCh <- buf:
	default:
		p.pool.Put(buf)
	}
}

func (p *PayloadLeasePool[T]) Register(msg *T, buf *[MaxPayloadSize]byte) {
	if p == nil {
		return
	}
	if msg == nil {
		p.RecycleBuf(buf)
		return
	}
	if buf == nil {
		return
	}

	key := weak.Make(msg)
	lease := &payloadLease[T]{
		pool: p,
		key:  key,
		buf:  buf,
	}
	lease.msgCleanup = runtime.AddCleanup(msg, cleanupUnclaimedLease[T], lease)

	var prev *payloadLease[T]
	p.mu.Lock()
	old, existed := p.leases[key]
	if existed && old != nil && old != lease {
		prev = old
	}
	p.leases[key] = lease
	if !existed {
		p.active.Add(1)
	}
	p.mu.Unlock()

	if prev != nil && prev.claimed.CompareAndSwap(false, true) {
		prev.msgCleanup.Stop()
		prev.release(false)
	}
}

func (p *PayloadLeasePool[T]) Release(msg *T, clearData func(*T)) bool {
	if p == nil || msg == nil || p.active.Load() == 0 {
		return false
	}

	key := weak.Make(msg)
	p.mu.Lock()
	lease, existed := p.leases[key]
	if existed {
		delete(p.leases, key)
		p.active.Add(-1)
	}
	p.mu.Unlock()

	if lease == nil || !lease.claimed.CompareAndSwap(false, true) {
		return false
	}
	lease.msgCleanup.Stop()
	if clearData != nil {
		clearData(msg)
	}
	lease.release(false)
	return true
}

func (p *PayloadLeasePool[T]) Claim(msg *T) *Guard {
	if p == nil || msg == nil || p.active.Load() == 0 {
		return nil
	}

	key := weak.Make(msg)
	p.mu.Lock()
	lease, existed := p.leases[key]
	if existed {
		delete(p.leases, key)
		p.active.Add(-1)
	}
	p.mu.Unlock()

	if lease == nil || !lease.claimed.CompareAndSwap(false, true) {
		return nil
	}
	lease.msgCleanup.Stop()

	guard := &Guard{lease: lease}
	lease.guardCleanup = runtime.AddCleanup(guard, cleanupOrphanedGuard[T], lease)
	return guard
}

func (p *PayloadLeasePool[T]) LeasedCount() int {
	if p == nil {
		return 0
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.leases)
}
