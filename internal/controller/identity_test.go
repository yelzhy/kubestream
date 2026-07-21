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
	"os"
	"path/filepath"
	"strings"
	"testing"

	"k8s.io/apimachinery/pkg/runtime/schema"
)

// TestCacheKeyDistinguishesGVKCollision is the core Task 0.4 regression: two
// objects that share a Kind but live in different API groups — batch/v1 Job
// and a CRD example.com/v1 Job — must produce distinct identity keys, and thus
// wholly independent hashCache entries. Before the fix cacheKey keyed on Kind
// alone, so both collapsed onto one entry and cross-contaminated each other's
// dedup/warm-up history (a latent audit-corruption bug, Invariant 7).
func TestCacheKeyDistinguishesGVKCollision(t *testing.T) {
	batchJob := &ResourceStreamReconciler{GVK: schema.GroupVersionKind{Group: "batch", Version: "v1", Kind: "Job"}}
	crdJob := &ResourceStreamReconciler{GVK: schema.GroupVersionKind{Group: "example.com", Version: "v1", Kind: "Job"}}

	batchKey := batchJob.cacheKey("foo", "bar")
	crdKey := crdJob.cacheKey("foo", "bar")

	if batchKey == crdKey {
		t.Fatalf("cacheKey collided across groups: batch/v1 Job and example.com/v1 Job both produced %q", batchKey)
	}

	// Prove the distinct keys drive independent hashCache entries: a write
	// under one key must never be observable under the other.
	var cache hashCache
	cache.Reserve(batchKey, CacheEntry{Hash: "batch-hash", UID: "batch-uid"})
	cache.Reserve(crdKey, CacheEntry{Hash: "crd-hash", UID: "crd-uid"})

	got, ok := cache.Load(batchKey)
	if !ok || got.Hash != "batch-hash" || got.UID != "batch-uid" {
		t.Fatalf("batch/v1 Job entry corrupted by example.com/v1 Job write: got %+v (ok=%v)", got, ok)
	}
	got, ok = cache.Load(crdKey)
	if !ok || got.Hash != "crd-hash" || got.UID != "crd-uid" {
		t.Fatalf("example.com/v1 Job entry corrupted by batch/v1 Job write: got %+v (ok=%v)", got, ok)
	}
	if cache.Len() != 2 {
		t.Fatalf("expected 2 independent cache entries, got %d", cache.Len())
	}
}

// TestCacheKeyIsVersionAgnostic asserts the other half of Invariant 7: apps/v1
// and a hypothetical apps/v2 Deployment are the SAME object, so they must key
// identically. The fix adds the group discriminator without adding version.
func TestCacheKeyIsVersionAgnostic(t *testing.T) {
	v1 := &ResourceStreamReconciler{GVK: schema.GroupVersionKind{Group: "apps", Version: "v1", Kind: "Deployment"}}
	v2 := &ResourceStreamReconciler{GVK: schema.GroupVersionKind{Group: "apps", Version: "v2", Kind: "Deployment"}}

	if got, want := v2.cacheKey("ns", "name"), v1.cacheKey("ns", "name"); got != want {
		t.Fatalf("cacheKey must be version-agnostic: apps/v2 gave %q, apps/v1 gave %q", got, want)
	}
}

// TestCacheKeyCoreGroupAndClusterScoped documents the shape for core-group and
// cluster-scoped (empty-namespace) objects, both of which key unambiguously.
func TestCacheKeyCoreGroupAndClusterScoped(t *testing.T) {
	pod := &ResourceStreamReconciler{GVK: schema.GroupVersionKind{Group: "", Version: "v1", Kind: "Pod"}}
	if got, want := pod.cacheKey("default", "p"), "|Pod|default/p"; got != want {
		t.Fatalf("core-group key = %q, want %q", got, want)
	}
	// ObjectKey.String() renders an empty namespace as a leading "/", so a
	// cluster-scoped object keys as "|Node|/n1" — still unambiguous.
	node := &ResourceStreamReconciler{GVK: schema.GroupVersionKind{Group: "", Version: "v1", Kind: "Node"}}
	if got, want := node.cacheKey("", "n1"), "|Node|/n1"; got != want {
		t.Fatalf("cluster-scoped key = %q, want %q", got, want)
	}
}

// TestNoRogueIdentityKeyConcatenation enforces the "exactly one function
// constructs identity keys" acceptance criterion: no non-test source file in
// this package may hand-build a key by concatenating `Kind + "/"`. cacheKey is
// the sole canonical builder and uses "|" delimiters, so a match here means a
// call site has drifted back to the pre-fix, collision-prone pattern.
func TestNoRogueIdentityKeyConcatenation(t *testing.T) {
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatalf("reading package dir: %v", err)
	}
	const forbidden = `Kind + "/"`
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		data, err := os.ReadFile(filepath.Clean(name))
		if err != nil {
			t.Fatalf("reading %s: %v", name, err)
		}
		if strings.Contains(string(data), forbidden) {
			t.Fatalf("%s contains a rogue identity-key concatenation %q; all keys must go through cacheKey", name, forbidden)
		}
	}
}
