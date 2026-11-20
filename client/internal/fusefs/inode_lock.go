package fusefs

import (
	"sync"
	"sync/atomic"
	"time"
)

type inodeLocks struct {
	mu      sync.Mutex
	entries map[uint64]*inodeLockEntry
	metrics atomic.Pointer[Metrics]
}

type inodeLockEntry struct {
	mu   sync.Mutex
	refs int
}

func (l *inodeLocks) bindMetrics(m *Metrics) {
	l.metrics.Store(m)
}

func (l *inodeLocks) activeEntries() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.entries)
}

func (l *inodeLocks) lock(ino uint64) func() {
	l.mu.Lock()
	if l.entries == nil {
		l.entries = make(map[uint64]*inodeLockEntry)
	}
	entry := l.entries[ino]
	if entry == nil {
		entry = &inodeLockEntry{}
		l.entries[ino] = entry
	}
	entry.refs++
	l.mu.Unlock()

	if entry.mu.TryLock() {
		l.metrics.Load().incInodeLockFast()
	} else {
		waitStart := time.Now()
		entry.mu.Lock()
		l.metrics.Load().observeInodeLockContended(time.Since(waitStart))
	}
	return func() {
		entry.mu.Unlock()
		l.mu.Lock()
		entry.refs--
		if entry.refs == 0 {
			delete(l.entries, ino)
		}
		l.mu.Unlock()
	}
}
