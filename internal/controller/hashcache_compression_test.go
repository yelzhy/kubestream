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
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/wI2L/jsondiff"
)

// corpusObject is one realistic normalized-JSON baseline loaded from
// testdata/, plus a slightly mutated variant used to exercise the diff path.
type corpusObject struct {
	name     string
	raw      []byte // compact json.Marshal output, exactly as the reconciler stores it
	modified []byte // raw with one field changed, so a diff is non-empty
}

// loadCorpus reads every *.json file under testdata/, normalizing each the
// same way Reconcile does (unmarshal → compact re-marshal) so the corpus
// bytes match what actually lands in a CacheEntry. It also derives a mutated
// variant of each object so round-trip fidelity is tested through a real,
// non-empty diff rather than a trivial equal-to-itself comparison.
func loadCorpus(t testing.TB) []corpusObject {
	t.Helper()

	entries, err := os.ReadDir("testdata")
	if err != nil {
		t.Fatalf("reading testdata: %v", err)
	}

	var corpus []corpusObject
	for _, e := range entries {
		if e.IsDir() || filepath.Ext(e.Name()) != ".json" {
			continue
		}
		fileBytes, err := os.ReadFile(filepath.Join("testdata", e.Name()))
		if err != nil {
			t.Fatalf("reading %s: %v", e.Name(), err)
		}

		var obj map[string]any
		if err := json.Unmarshal(fileBytes, &obj); err != nil {
			t.Fatalf("unmarshalling %s: %v", e.Name(), err)
		}
		raw, err := json.Marshal(obj)
		if err != nil {
			t.Fatalf("marshalling %s: %v", e.Name(), err)
		}

		// Mutate metadata.name so a diff against raw is guaranteed non-empty
		// without depending on any type-specific field being present.
		meta, ok := obj["metadata"].(map[string]any)
		if !ok {
			t.Fatalf("%s: missing metadata object", e.Name())
		}
		meta["name"] = "mutated-for-diff"
		modified, err := json.Marshal(obj)
		if err != nil {
			t.Fatalf("marshalling mutated %s: %v", e.Name(), err)
		}

		corpus = append(corpus, corpusObject{name: e.Name(), raw: raw, modified: modified})
	}

	if len(corpus) == 0 {
		t.Fatal("testdata corpus is empty")
	}
	return corpus
}

// diffBytes reproduces the reconciler's exact diff production: CompareJSON on
// the baseline, then json.Marshal of the resulting patch.
func diffBytes(t testing.TB, baseline, current []byte) []byte {
	t.Helper()
	patch, err := jsondiff.CompareJSON(baseline, current)
	if err != nil {
		t.Fatalf("CompareJSON: %v", err)
	}
	out, err := json.Marshal(patch)
	if err != nil {
		t.Fatalf("marshalling patch: %v", err)
	}
	return out
}

// TestCacheEntryRoundTripDiffFidelity is the primary Task 0.7 acceptance
// criterion: for every corpus object, the diff computed over a
// compressed-then-decompressed baseline must be byte-identical to the diff
// computed over the raw baseline. It also asserts the decompressed bytes
// equal the original, which is the underlying guarantee that makes the diffs
// match.
func TestCacheEntryRoundTripDiffFidelity(t *testing.T) {
	for _, obj := range loadCorpus(t) {
		t.Run(obj.name, func(t *testing.T) {
			data, enc, compressed := compressBaseline(obj.raw)
			if !compressed || enc != encodingZstd {
				t.Fatalf("expected corpus object to compress, got compressed=%v enc=%d", compressed, enc)
			}

			entry := CacheEntry{JSON: data, Encoding: enc}
			decoded, err := entry.decodeBaseline()
			if err != nil {
				t.Fatalf("decodeBaseline: %v", err)
			}
			if !bytes.Equal(decoded, obj.raw) {
				t.Fatalf("decompressed baseline differs from raw (%d vs %d bytes)", len(decoded), len(obj.raw))
			}

			rawDiff := diffBytes(t, obj.raw, obj.modified)
			roundTripDiff := diffBytes(t, decoded, obj.modified)
			if !bytes.Equal(rawDiff, roundTripDiff) {
				t.Fatalf("diff over compressed baseline not byte-identical to diff over raw baseline\nraw:        %s\nroundtrip:  %s", rawDiff, roundTripDiff)
			}
		})
	}
}

// TestDecodeBaselineCorruption verifies that a truncated compressed entry is
// rejected with an error (never silently decoded to garbage), which is what
// drives Reconcile's full-state fallback. It covers both a body truncation
// (valid magic, corrupt payload) and a magic-byte truncation (caught before
// the decoder is even invoked).
func TestDecodeBaselineCorruption(t *testing.T) {
	corpus := loadCorpus(t)
	data, _, compressed := compressBaseline(corpus[0].raw)
	if !compressed {
		t.Fatal("expected corpus object to compress")
	}

	t.Run("truncated body", func(t *testing.T) {
		truncated := append([]byte(nil), data[:len(data)/2]...)
		entry := CacheEntry{JSON: truncated, Encoding: encodingZstd}
		if _, err := entry.decodeBaseline(); err == nil {
			t.Fatal("expected an error decoding a truncated compressed baseline")
		}
	})

	t.Run("missing magic bytes", func(t *testing.T) {
		entry := CacheEntry{JSON: []byte("not a zstd frame"), Encoding: encodingZstd}
		if _, err := entry.decodeBaseline(); err == nil {
			t.Fatal("expected an error when the zstd magic bytes are absent")
		}
	})

	t.Run("nil baseline is not an error", func(t *testing.T) {
		got, err := (CacheEntry{}).decodeBaseline()
		if err != nil || got != nil {
			t.Fatalf("nil baseline must decode to (nil, nil), got (%v, %v)", got, err)
		}
	})
}

// corpusReductionPercent compresses the whole corpus and returns the
// percentage reduction in aggregate CacheEntry payload bytes (raw vs
// compressed), the figure Task 0.7 requires to be ≥60%.
func corpusReductionPercent(t testing.TB, corpus []corpusObject) (rawTotal, compressedTotal int, pct float64) {
	t.Helper()
	for _, obj := range corpus {
		data, _, compressed := compressBaseline(obj.raw)
		if !compressed {
			t.Fatalf("%s did not compress", obj.name)
		}
		rawTotal += len(obj.raw)
		compressedTotal += len(data)
	}
	pct = 100 * (1 - float64(compressedTotal)/float64(rawTotal))
	return rawTotal, compressedTotal, pct
}

// TestCacheEntryCompressionReducesMemory enforces the ≥60% aggregate
// reduction acceptance criterion on the realistic corpus.
func TestCacheEntryCompressionReducesMemory(t *testing.T) {
	corpus := loadCorpus(t)
	rawTotal, compressedTotal, pct := corpusReductionPercent(t, corpus)
	t.Logf("aggregate CacheEntry payload: raw=%d compressed=%d reduction=%.1f%%", rawTotal, compressedTotal, pct)
	if pct < 60 {
		t.Fatalf("expected ≥60%% reduction on the corpus, got %.1f%% (raw=%d compressed=%d)", pct, rawTotal, compressedTotal)
	}
}

// BenchmarkHashCacheMemory reports the aggregate CacheEntry payload reduction
// achieved by compression on the corpus as the custom metric reduction_pct.
// It is the Task 0.7 benchmark whose number is recorded in
// docs/PERFORMANCE.md; the ≥60% threshold itself is asserted by
// TestCacheEntryCompressionReducesMemory.
func BenchmarkHashCacheMemory(b *testing.B) {
	corpus := loadCorpus(b)

	var rawTotal, compressedTotal int
	for b.Loop() {
		rawTotal, compressedTotal = 0, 0
		for _, obj := range corpus {
			data, _, _ := compressBaseline(obj.raw)
			rawTotal += len(obj.raw)
			compressedTotal += len(data)
		}
	}

	pct := 100 * (1 - float64(compressedTotal)/float64(rawTotal))
	b.ReportMetric(pct, "reduction_pct")
	b.ReportMetric(float64(rawTotal), "raw_bytes")
	b.ReportMetric(float64(compressedTotal), "compressed_bytes")
}

// BenchmarkHashCacheShortCircuit measures the allocation cost of the dedup
// hot path — the unchanged-hash short-circuit — proving it decompresses
// nothing. Reconcile short-circuits on Load + a string hash comparison
// alone; this benchmark exercises exactly that and reports allocs/op so a
// regression that sneaks a decompress into the hot path would show up as a
// jump in allocations. A compressed baseline is stored on the entry precisely
// to demonstrate it is never touched here.
func BenchmarkHashCacheShortCircuit(b *testing.B) {
	corpus := loadCorpus(b)
	data, enc, _ := compressBaseline(corpus[0].raw)

	var c hashCache
	const key = "g|Kind|ns/name"
	c.Reserve(key, CacheEntry{Hash: "deadbeef", JSON: data, Encoding: enc, UID: "uid-1"})

	const incomingHash = "deadbeef" // identical → short-circuit

	b.ReportAllocs()
	var hits int
	for b.Loop() {
		entry, ok := c.Load(key)
		if ok && entry.Hash == incomingHash {
			hits++ // dedup skip: no decode, no diff, no write
		}
	}
	if hits != b.N {
		b.Fatalf("expected every iteration to short-circuit, got %d/%d", hits, b.N)
	}
}
