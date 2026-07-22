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

## Load harness + write-path baseline (Task 0.8)

`test/loadgen` is a synthetic-churn harness that drives realistic object churn
through the **real** pipeline — an in-process envtest apiserver →
`ResourceStreamReconciler` → `CHWriter` → a dockerized ClickHouse — and reports
the figures Phase 0's throughput claims rest on. It watches `v1/ConfigMap`
(a built-in kind whose arbitrary string `Data` lets the harness dial payload
size precisely) and reports, to stdout:

- **sustained records/sec** — settled writes over the churn window;
- **p50 / p99 enqueue-block** — how long `Enqueue` blocked for queue room (the
  hot-path backpressure a reconcile actually feels);
- **peak `write_queue_depth`** — how close the hand-off queue came to saturation;
- **process RSS** — peak resident set (`getrusage`, unit-normalized per OS).

### Running it

```sh
make bench-load                                   # small default profile
make bench-load LOADGEN_RATE=4000 LOADGEN_DURATION=20s LOADGEN_CONCURRENCY=32
```

`make bench-load` stands up a throwaway ClickHouse container (as
`make test-integration` does) and provides `KUBEBUILDER_ASSETS` for envtest, then
runs the harness under the `integration` build tag. Harness parameters are flags
on the harness itself (with `LOADGEN_*` env twins the Makefile forwards):
`-objects`, `-rate` (mutations/sec), `-payload-bytes`, `-duration`,
`-delete-ratio`, `-concurrency`.

### Recorded dev-hardware baseline

Measured on an Apple-silicon dev laptop (darwin/arm64), ClickHouse
`clickhouse-server:24.8` in Docker, default writer tuning (queue 5000, 4 workers,
batch max 1000 rows / 1s), small profile (`objects=50, rate=200/s,
payload=2KiB, duration=10s`):

| Metric                   | Value      |
|--------------------------|------------|
| sustained records/sec    | ~201       |
| enqueue-block p50        | ~0.003 ms  |
| enqueue-block p99        | ~0.007 ms  |
| peak `write_queue_depth` | 3          |
| process RSS              | ~71 MiB    |

Pushed harder (`objects=300–400, rate=4000–6000/s, concurrency=16–64`), achieved
throughput plateaus at **~550–565 records/sec** while the write path stays
essentially idle — p99 enqueue-block <0.01 ms and peak `write_queue_depth` ≤11.
The bottleneck at these rates is the **envtest apiserver's own write throughput**,
not `CHWriter`: the batched sink absorbs everything envtest can source without
the hand-off queue ever backing up.

### Initial SLO (starting target — re-validated in Task 2.3)

> **Sustain ≥2,000 records/sec single-replica with p99 enqueue-block <10 ms while
> ClickHouse is healthy.**

This is a starting target, not a marketing claim. The dev-hardware baseline above
already shows the write path meets the *latency* half of the SLO with enormous
margin (p99 <0.01 ms vs. the 10 ms budget) and never saturates its queue. The
*throughput* half cannot be validated here because envtest tops out well below
2,000 events/sec; **Task 2.3** re-runs this same harness with a "massive" profile
against a real apiserver to confirm ≥2,000 records/sec end-to-end.
