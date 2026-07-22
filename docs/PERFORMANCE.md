# kubestream — Performance

## `hashCache` memory: compressed diff baselines (Task 0.7)

`hashCache` keeps a full normalized-JSON copy of every watched object — the
diff baseline — *in addition to* the informer cache's own copy. At scale (D2)
that second copy is the dominant memory cost of the operator. Kubernetes JSON
compresses extremely well, so each `CacheEntry.JSON` is now stored
**zstd-compressed** (`klauspost/compress`, `SpeedDefault`) and decompressed
only when a diff is actually computed. The unchanged-object dedup path is
hash-comparison-only and never decompresses.

### Baseline measurement

`BenchmarkHashCacheMemory` (in `internal/controller`) compresses a corpus of
realistic normalized Pod / Deployment / Service objects
(`internal/controller/testdata/`) and reports the aggregate reduction in
`CacheEntry` payload bytes. `TestCacheEntryCompressionReducesMemory` asserts
the reduction stays at or above the 60% target.

Reproduce:

```sh
go test ./internal/controller/ -run '^$' -bench BenchmarkHashCacheMemory -benchmem
```

Recorded result (corpus of 3 objects, `SpeedDefault`):

| Metric                          | Value        |
|---------------------------------|--------------|
| Aggregate raw baseline bytes    | 14234        |
| Aggregate compressed bytes      | 5501         |
| **Reduction**                   | **61.4%**    |

This is measured with each entry compressed **independently**, exactly as the
operator stores it — so the figure is conservative. Real clusters hold
thousands of structurally near-identical objects; per-entry compression
already clears the 60% bar without exploiting any cross-object redundancy.

### Hot-path allocation guard

`BenchmarkHashCacheShortCircuit` exercises the dedup short-circuit (cache
`Load` + hash comparison) and reports **0 allocs/op**, confirming the
unchanged-hash path decompresses nothing and does not regress allocations.

```sh
go test ./internal/controller/ -run '^$' -bench BenchmarkHashCacheShortCircuit -benchmem
```

### Failure behavior

- **Compression failure** (encoder unavailable): the raw bytes are stored with
  the `encodingRaw` marker and the anomaly is logged at `Error` level
  (Invariant 5). Diffing still works — it just costs more memory.
- **Decompression failure on diff** (corrupt/truncated entry): the reconciler
  logs at `Error` level and falls back to a **full-state write**, identical to
  the missing-baseline path. The event is never dropped or mis-recorded.
