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
	mu   sync.RWMutex
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

func (l *inodeLocks) acquireEntry(ino uint64) *inodeLockEntry {
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
	return entry
}

func (l *inodeLocks) releaseEntry(ino uint64, entry *inodeLockEntry) {
	l.mu.Lock()
	entry.refs--
	if entry.refs == 0 {
		delete(l.entries, ino)
	}
	l.mu.Unlock()
}

func (l *inodeLocks) lock(ino uint64) func() {
	entry := l.acquireEntry(ino)
	if entry.mu.TryLock() {
		l.metrics.Load().incInodeLockFast()
	} else {
		waitStart := time.Now()
		entry.mu.Lock()
		l.metrics.Load().observeInodeLockContended(time.Since(waitStart))
	}
	return func() {
		entry.mu.Unlock()
		l.releaseEntry(ino, entry)
	}
}

func (l *inodeLocks) rlock(ino uint64) func() {
	entry := l.acquireEntry(ino)
	if entry.mu.TryRLock() {
		l.metrics.Load().incInodeLockFast()
	} else {
		waitStart := time.Now()
		entry.mu.RLock()
		l.metrics.Load().observeInodeLockContended(time.Since(waitStart))
	}
	return func() {
		entry.mu.RUnlock()
		l.releaseEntry(ino, entry)
	}
}
