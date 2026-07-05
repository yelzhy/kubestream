/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package controller

import "sync"

// hashCache is a mutex-protected map from objectKey to CacheEntry, with a
// version-gated commit primitive so an async write's outcome can be applied
// to the cache only if nothing newer has landed for that key since the write
// was issued. Reconcile calls for a given key are serialized by
// controller-runtime, but the *async* ClickHouse write that a Reconcile call
// kicks off is not — a later-issued write can finish before an
// earlier-issued one. Without version gating, whichever commit callback
// happens to run last would win regardless of which one was actually
// issued last, silently reverting the cache to stale data. A plain
// mutex-protected map (rather than sync.Map) is used because every mutation
// here is a read-decide-write sequence that must be atomic as a whole, which
// sync.Map's individual Load/Store/Delete operations cannot provide.
type hashCache struct {
	mu   sync.Mutex
	data map[string]CacheEntry
}

// Load returns the current entry for key, if any.
func (c *hashCache) Load(key string) (CacheEntry, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	entry, ok := c.data[key]
	return entry, ok
}

// Reserve atomically assigns the next version for key and stores the entry
// built from it, returning that version. The caller threads the returned
// version into the write job it's about to issue, so the eventual commit can
// later prove (via CommitIfCurrent/DeleteIfCurrent) that it's still settling
// the latest write for this key before mutating the cache.
func (c *hashCache) Reserve(key string, entry CacheEntry) uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.data == nil {
		c.data = make(map[string]CacheEntry)
	}
	entry.Version = c.data[key].Version + 1
	c.data[key] = entry
	return entry.Version
}

// CommitIfCurrent stores entry for key only if the entry currently present
// (if any) still has exactly expectedVersion — i.e. no newer Reserve has
// happened for this key since the caller's write was issued. Returns
// whether it applied. A newer entry present means a later write has already
// superseded this one; leaving it alone (rather than overwriting) is what
// prevents a stale, out-of-order commit from clobbering fresher state.
func (c *hashCache) CommitIfCurrent(key string, expectedVersion uint64, entry CacheEntry) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	cur, present := c.data[key]
	if present && cur.Version != expectedVersion {
		return false
	}
	if !present && expectedVersion != 0 {
		return false
	}
	if c.data == nil {
		c.data = make(map[string]CacheEntry)
	}
	entry.Version = expectedVersion
	c.data[key] = entry
	return true
}

// DeleteIfCurrent removes key only if its entry still has exactly
// expectedVersion. Returns whether it applied.
func (c *hashCache) DeleteIfCurrent(key string, expectedVersion uint64) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	cur, present := c.data[key]
	if !present || cur.Version != expectedVersion {
		return false
	}
	delete(c.data, key)
	return true
}

// StoreIfAbsent sets entry for key only if key has no entry yet. Used by
// restoreAndWarm to seed historical baselines without clobbering a live
// entry that a concurrent Reconcile may have already reserved for this key
// while the restore was still in flight.
func (c *hashCache) StoreIfAbsent(key string, entry CacheEntry) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.data == nil {
		c.data = make(map[string]CacheEntry)
	}
	if _, exists := c.data[key]; exists {
		return
	}
	entry.Version = 1
	c.data[key] = entry
}
