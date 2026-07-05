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

import (
	"sync"
	"testing"
)

func TestHashCacheReserveThenCommit(t *testing.T) {
	var c hashCache

	version := c.Reserve("k", CacheEntry{Hash: "h1"})
	if version != 1 {
		t.Fatalf("expected first Reserve to assign version 1, got %d", version)
	}

	if ok := c.CommitIfCurrent("k", version, CacheEntry{Hash: "h1-confirmed"}); !ok {
		t.Fatalf("CommitIfCurrent should apply when the version still matches")
	}

	entry, exists := c.Load("k")
	if !exists || entry.Hash != "h1-confirmed" {
		t.Fatalf("expected confirmed entry, got %+v (exists=%v)", entry, exists)
	}
}

// TestHashCacheStaleCommitDoesNotClobberNewer reproduces the exact race
// behind confirmed findings #1/#2 of the code review: job A is issued
// first but its commit fires after job B's (a later write for the same
// key). Without version gating, A's commit would blindly overwrite B's
// already-confirmed result. With it, A's stale commit must be a no-op.
func TestHashCacheStaleCommitDoesNotClobberNewer(t *testing.T) {
	var c hashCache

	versionA := c.Reserve("k", CacheEntry{Hash: "hA"})
	versionB := c.Reserve("k", CacheEntry{Hash: "hB"}) // a second write supersedes the first

	if versionB != versionA+1 {
		t.Fatalf("expected monotonically increasing versions, got A=%d B=%d", versionA, versionB)
	}

	// B's commit fires first (its write finished faster).
	if ok := c.CommitIfCurrent("k", versionB, CacheEntry{Hash: "hB-confirmed"}); !ok {
		t.Fatalf("CommitIfCurrent for the current version should apply")
	}

	// A's commit fires later, even though it was issued first — it must be
	// rejected rather than clobbering B's confirmed state.
	if ok := c.CommitIfCurrent("k", versionA, CacheEntry{Hash: "hA-confirmed"}); ok {
		t.Fatalf("stale CommitIfCurrent must not apply")
	}

	entry, exists := c.Load("k")
	if !exists || entry.Hash != "hB-confirmed" {
		t.Fatalf("expected B's confirmed entry to survive A's stale commit, got %+v (exists=%v)", entry, exists)
	}
}

// TestHashCacheStaleDeleteDoesNotClobberNewer reproduces confirmed finding
// #2: an object is deleted, then quickly recreated (new UID) before the
// delete job's commit fires. The stale DeleteIfCurrent must not remove the
// newer incarnation's entry.
func TestHashCacheStaleDeleteDoesNotClobberNewer(t *testing.T) {
	var c hashCache

	deleteVersion := c.Reserve("k", CacheEntry{UID: "old-uid"})
	// Object recreated under a new UID before the delete's commit fires.
	c.Reserve("k", CacheEntry{UID: "new-uid"})

	if ok := c.DeleteIfCurrent("k", deleteVersion); ok {
		t.Fatalf("stale DeleteIfCurrent must not apply once a newer entry exists")
	}

	entry, exists := c.Load("k")
	if !exists || entry.UID != "new-uid" {
		t.Fatalf("expected the newer incarnation's entry to survive, got %+v (exists=%v)", entry, exists)
	}
}

func TestHashCacheStoreIfAbsentDoesNotClobberLiveEntry(t *testing.T) {
	var c hashCache

	version := c.Reserve("k", CacheEntry{Hash: "live"})
	c.StoreIfAbsent("k", CacheEntry{Hash: "historical-baseline"})

	entry, exists := c.Load("k")
	if !exists || entry.Hash != "live" || entry.Version != version {
		t.Fatalf("StoreIfAbsent must not overwrite an existing entry, got %+v (exists=%v)", entry, exists)
	}

	c.StoreIfAbsent("other-key", CacheEntry{Hash: "seeded"})
	entry, exists = c.Load("other-key")
	if !exists || entry.Hash != "seeded" {
		t.Fatalf("StoreIfAbsent should seed a genuinely absent key, got %+v (exists=%v)", entry, exists)
	}
}

// TestHashCacheConcurrentReserveCommit exercises Reserve/CommitIfCurrent
// under concurrent access (run with -race) to catch any lock-ordering bug
// in hashCache itself, independent of Reconcile's usage of it.
func TestHashCacheConcurrentReserveCommit(t *testing.T) {
	var c hashCache
	const n = 200

	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			version := c.Reserve("k", CacheEntry{Hash: "h"})
			c.CommitIfCurrent("k", version, CacheEntry{Hash: "h-confirmed"})
		}(i)
	}
	wg.Wait()

	if _, exists := c.Load("k"); !exists {
		t.Fatalf("expected an entry to exist after concurrent Reserve/CommitIfCurrent calls")
	}
}
