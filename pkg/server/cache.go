package server

import (
	"sync"
	"time"
)

type labelMatchCacheKey struct {
	backend  string
	selector string
}

type labelMatchCacheEntry struct {
	matched   bool
	expiresAt time.Time
}

type labelMatchCache struct {
	mtx     sync.RWMutex
	ttl     time.Duration
	entries map[labelMatchCacheKey]labelMatchCacheEntry
}

func newLabelMatchCache(ttl time.Duration) *labelMatchCache {
	if ttl <= 0 {
		return nil
	}
	return &labelMatchCache{
		ttl:     ttl,
		entries: make(map[labelMatchCacheKey]labelMatchCacheEntry),
	}
}

func (c *labelMatchCache) Get(backend, selector string) (bool, bool) {
	if c == nil {
		return false, false
	}
	c.mtx.RLock()
	entry, ok := c.entries[labelMatchCacheKey{backend: backend, selector: selector}]
	c.mtx.RUnlock()
	if !ok {
		return false, false
	}
	if time.Now().After(entry.expiresAt) {
		return false, false
	}
	return entry.matched, true
}

func (c *labelMatchCache) Put(backend, selector string, matched bool) {
	if c == nil {
		return
	}
	c.mtx.Lock()
	c.entries[labelMatchCacheKey{backend: backend, selector: selector}] = labelMatchCacheEntry{
		matched:   matched,
		expiresAt: time.Now().Add(c.ttl),
	}
	c.mtx.Unlock()
}