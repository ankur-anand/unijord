package isledb

import "sync"

// runRangeCache is a bounded immutable-generation range cache. Entries are
// keyed by the run ID, region, object generation token, offset and length by
// the caller; eviction is deliberately simple and deterministic.
type runRangeCache struct {
	mu      sync.Mutex
	max     uint64
	bytes   uint64
	entries map[string][]byte
}

func newRunRangeCache(max uint64) *runRangeCache {
	return &runRangeCache{max: max, entries: make(map[string][]byte)}
}

func (c *runRangeCache) get(key string) ([]byte, bool) {
	if c == nil {
		return nil, false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	b, ok := c.entries[key]
	if !ok {
		return nil, false
	}
	return b, true
}

func (c *runRangeCache) set(key string, value []byte) {
	if c == nil || c.max == 0 || uint64(len(value)) > c.max {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if old, ok := c.entries[key]; ok {
		c.bytes -= uint64(len(old))
	}
	for c.bytes+uint64(len(value)) > c.max {
		for k, old := range c.entries {
			delete(c.entries, k)
			c.bytes -= uint64(len(old))
			break
		}
	}
	c.entries[key] = value
	c.bytes += uint64(len(value))
}

func (c *runRangeCache) stats() (uint64, int) {
	if c == nil {
		return 0, 0
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.bytes, len(c.entries)
}
