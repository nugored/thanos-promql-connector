package server

import (
	"sort"
	"strings"
	"sync"
	"time"

	v1 "github.com/prometheus/client_golang/api/prometheus/v1"
	"github.com/prometheus/common/model"
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
	mtx        sync.RWMutex
	ttl        time.Duration
	maxEntries int
	entries    map[labelMatchCacheKey]labelMatchCacheEntry
}

func newLabelMatchCache(ttl time.Duration, maxEntries int) *labelMatchCache {
	if ttl <= 0 {
		return nil
	}
	return &labelMatchCache{
		ttl:        ttl,
		maxEntries: maxEntries,
		entries:    make(map[labelMatchCacheKey]labelMatchCacheEntry),
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

func (c *labelMatchCache) PurgeExpired() {
	if c == nil {
		return
	}
	c.mtx.Lock()
	defer c.mtx.Unlock()
	c.purgeExpiredLocked()
}

func (c *labelMatchCache) purgeExpiredLocked() {
	now := time.Now()
	for k, v := range c.entries {
		if now.After(v.expiresAt) {
			delete(c.entries, k)
		}
	}
}

func (c *labelMatchCache) evictOldestLocked() {
	if len(c.entries) == 0 {
		return
	}
	var oldestKey labelMatchCacheKey
	var oldestTime time.Time
	first := true
	for k, v := range c.entries {
		if first || v.expiresAt.Before(oldestTime) {
			oldestKey = k
			oldestTime = v.expiresAt
			first = false
		}
	}
	delete(c.entries, oldestKey)
}

func (c *labelMatchCache) Put(backend, selector string, matched bool) {
	if c == nil {
		return
	}
	c.mtx.Lock()
	defer c.mtx.Unlock()

	key := labelMatchCacheKey{backend: backend, selector: selector}
	if _, exists := c.entries[key]; !exists && c.maxEntries > 0 && len(c.entries) >= c.maxEntries {
		c.purgeExpiredLocked()
		if len(c.entries) >= c.maxEntries {
			c.evictOldestLocked()
		}
	}

	c.entries[key] = labelMatchCacheEntry{
		matched:   matched,
		expiresAt: time.Now().Add(c.ttl),
	}
}

type labelNamesCacheKey struct {
	backend     string
	matches     string
	startBucket int64
	endBucket   int64
}

type labelNamesCacheEntry struct {
	names     []string
	warnings  v1.Warnings
	expiresAt time.Time
}

type labelNamesCache struct {
	mtx        sync.RWMutex
	ttl        time.Duration
	maxEntries int
	entries    map[labelNamesCacheKey]labelNamesCacheEntry
}

func newLabelNamesCache(ttl time.Duration, maxEntries int) *labelNamesCache {
	if ttl <= 0 {
		return nil
	}
	return &labelNamesCache{
		ttl:        ttl,
		maxEntries: maxEntries,
		entries:    make(map[labelNamesCacheKey]labelNamesCacheEntry),
	}
}

func (c *labelNamesCache) Get(backend string, matches []string, startTime, endTime time.Time) ([]string, v1.Warnings, bool) {
	if c == nil {
		return nil, nil, false
	}
	key := labelNamesCacheKey{
		backend:     backend,
		matches:     formatMatchesKey(matches),
		startBucket: bucketTimeMs(startTime),
		endBucket:   bucketTimeMs(endTime),
	}
	c.mtx.RLock()
	entry, ok := c.entries[key]
	c.mtx.RUnlock()
	if !ok {
		return nil, nil, false
	}
	if time.Now().After(entry.expiresAt) {
		return nil, nil, false
	}
	return cloneStringSlice(entry.names), cloneWarningsSlice(entry.warnings), true
}

func (c *labelNamesCache) PurgeExpired() {
	if c == nil {
		return
	}
	c.mtx.Lock()
	defer c.mtx.Unlock()
	c.purgeExpiredLocked()
}

func (c *labelNamesCache) purgeExpiredLocked() {
	now := time.Now()
	for k, v := range c.entries {
		if now.After(v.expiresAt) {
			delete(c.entries, k)
		}
	}
}

func (c *labelNamesCache) evictOldestLocked() {
	if len(c.entries) == 0 {
		return
	}
	var oldestKey labelNamesCacheKey
	var oldestTime time.Time
	first := true
	for k, v := range c.entries {
		if first || v.expiresAt.Before(oldestTime) {
			oldestKey = k
			oldestTime = v.expiresAt
			first = false
		}
	}
	delete(c.entries, oldestKey)
}

func (c *labelNamesCache) Put(backend string, matches []string, startTime, endTime time.Time, names []string, warnings v1.Warnings) {
	if c == nil {
		return
	}
	key := labelNamesCacheKey{
		backend:     backend,
		matches:     formatMatchesKey(matches),
		startBucket: bucketTimeMs(startTime),
		endBucket:   bucketTimeMs(endTime),
	}
	c.mtx.Lock()
	defer c.mtx.Unlock()

	if _, exists := c.entries[key]; !exists && c.maxEntries > 0 && len(c.entries) >= c.maxEntries {
		c.purgeExpiredLocked()
		if len(c.entries) >= c.maxEntries {
			c.evictOldestLocked()
		}
	}

	c.entries[key] = labelNamesCacheEntry{
		names:     cloneStringSlice(names),
		warnings:  cloneWarningsSlice(warnings),
		expiresAt: time.Now().Add(c.ttl),
	}
}

type labelValuesCacheKey struct {
	backend     string
	label       string
	matches     string
	startBucket int64
	endBucket   int64
}

type labelValuesCacheEntry struct {
	values    model.LabelValues
	warnings  v1.Warnings
	expiresAt time.Time
}

type labelValuesCache struct {
	mtx        sync.RWMutex
	ttl        time.Duration
	maxEntries int
	entries    map[labelValuesCacheKey]labelValuesCacheEntry
}

func newLabelValuesCache(ttl time.Duration, maxEntries int) *labelValuesCache {
	if ttl <= 0 {
		return nil
	}
	return &labelValuesCache{
		ttl:        ttl,
		maxEntries: maxEntries,
		entries:    make(map[labelValuesCacheKey]labelValuesCacheEntry),
	}
}

func (c *labelValuesCache) Get(backend, label string, matches []string, startTime, endTime time.Time) (model.LabelValues, v1.Warnings, bool) {
	if c == nil {
		return nil, nil, false
	}
	key := labelValuesCacheKey{
		backend:     backend,
		label:       label,
		matches:     formatMatchesKey(matches),
		startBucket: bucketTimeMs(startTime),
		endBucket:   bucketTimeMs(endTime),
	}
	c.mtx.RLock()
	entry, ok := c.entries[key]
	c.mtx.RUnlock()
	if !ok {
		return nil, nil, false
	}
	if time.Now().After(entry.expiresAt) {
		return nil, nil, false
	}
	return cloneLabelValuesSlice(entry.values), cloneWarningsSlice(entry.warnings), true
}

func (c *labelValuesCache) PurgeExpired() {
	if c == nil {
		return
	}
	c.mtx.Lock()
	defer c.mtx.Unlock()
	c.purgeExpiredLocked()
}

func (c *labelValuesCache) purgeExpiredLocked() {
	now := time.Now()
	for k, v := range c.entries {
		if now.After(v.expiresAt) {
			delete(c.entries, k)
		}
	}
}

func (c *labelValuesCache) evictOldestLocked() {
	if len(c.entries) == 0 {
		return
	}
	var oldestKey labelValuesCacheKey
	var oldestTime time.Time
	first := true
	for k, v := range c.entries {
		if first || v.expiresAt.Before(oldestTime) {
			oldestKey = k
			oldestTime = v.expiresAt
			first = false
		}
	}
	delete(c.entries, oldestKey)
}

func (c *labelValuesCache) Put(backend, label string, matches []string, startTime, endTime time.Time, values model.LabelValues, warnings v1.Warnings) {
	if c == nil {
		return
	}
	key := labelValuesCacheKey{
		backend:     backend,
		label:       label,
		matches:     formatMatchesKey(matches),
		startBucket: bucketTimeMs(startTime),
		endBucket:   bucketTimeMs(endTime),
	}
	c.mtx.Lock()
	defer c.mtx.Unlock()

	if _, exists := c.entries[key]; !exists && c.maxEntries > 0 && len(c.entries) >= c.maxEntries {
		c.purgeExpiredLocked()
		if len(c.entries) >= c.maxEntries {
			c.evictOldestLocked()
		}
	}

	c.entries[key] = labelValuesCacheEntry{
		values:    cloneLabelValuesSlice(values),
		warnings:  cloneWarningsSlice(warnings),
		expiresAt: time.Now().Add(c.ttl),
	}
}

func bucketTimeMs(t time.Time) int64 {
	if t.IsZero() {
		return 0
	}
	return t.Truncate(time.Minute).UnixMilli()
}

func formatMatchesKey(matches []string) string {
	if len(matches) == 0 {
		return ""
	}
	sorted := append([]string(nil), matches...)
	sort.Strings(sorted)
	return strings.Join(sorted, "\x00")
}

func cloneStringSlice(s []string) []string {
	if s == nil {
		return nil
	}
	res := make([]string, len(s))
	copy(res, s)
	return res
}

func cloneWarningsSlice(w v1.Warnings) v1.Warnings {
	if w == nil {
		return nil
	}
	res := make(v1.Warnings, len(w))
	copy(res, w)
	return res
}

func cloneLabelValuesSlice(v model.LabelValues) model.LabelValues {
	if v == nil {
		return nil
	}
	res := make(model.LabelValues, len(v))
	copy(res, v)
	return res
}
