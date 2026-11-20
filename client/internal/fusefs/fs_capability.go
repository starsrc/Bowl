package fusefs

import (
	"sync"
	"sync/atomic"
)

const securityCapabilityXattr = "security.capability"

type capabilityCache struct {
	mu      sync.Mutex
	entries map[uint64]bool
	metrics atomic.Pointer[Metrics]
}

func newCapabilityCache() *capabilityCache {
	return &capabilityCache{entries: make(map[uint64]bool)}
}

func (c *capabilityCache) bindMetrics(m *Metrics) {
	c.metrics.Store(m)
}

func (c *capabilityCache) count() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.entries)
}

func (c *capabilityCache) knownAbsent(ino uint64) bool {
	c.mu.Lock()
	present, ok := c.entries[ino]
	c.mu.Unlock()
	if ok && !present {
		c.metrics.Load().incCapHit()
		return true
	}
	c.metrics.Load().incCapMiss()
	return false
}

func (c *capabilityCache) markAbsent(ino uint64) {
	c.mu.Lock()
	c.entries[ino] = false
	c.mu.Unlock()
	c.metrics.Load().incCapGrant()
}

func (c *capabilityCache) markPresent(ino uint64) {
	c.mu.Lock()
	c.entries[ino] = true
	c.mu.Unlock()
	c.metrics.Load().incCapGrant()
}

func (c *capabilityCache) forget(ino uint64) {
	c.mu.Lock()
	delete(c.entries, ino)
	c.mu.Unlock()
}

func (c *capabilityCache) reset() {
	c.mu.Lock()
	c.entries = make(map[uint64]bool)
	c.mu.Unlock()
	c.metrics.Load().incCapEpochInvalidation()
}
