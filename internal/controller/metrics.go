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

	"github.com/prometheus/client_golang/prometheus"
	ctrlmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"
)

// metricsNamespace prefixes every metric name with "kubestream_", giving the
// operator a single, greppable namespace on the /metrics endpoint that
// controller-runtime already serves via --metrics-bind-address.
const metricsNamespace = "kubestream"

// PipelineMetrics is the full set of Prometheus collectors describing the
// write pipeline's health: queue saturation, write outcomes and latency,
// retry storms, dedup short-circuits, cache size, per-kind Snapshot (safe)
// mode, and dropped requeue triggers. Before this, all of those were only
// visible in logs; every later performance task (0.6, 0.8, 2.3) needs them to
// prove it actually helped.
//
// It is exported because the ClickHouse writer (internal/sink/clickhouse)
// records the write-path metrics through the narrow clickhouse.Metrics
// interface, which *PipelineMetrics satisfies (see the setter methods below).
// The collector fields stay unexported: callers mutate them only through those
// methods or through this package's own reconciler code.
//
// Collectors are grouped in a struct (rather than package-level vars) so tests
// can build an isolated instance against a fresh registry — Prometheus panics
// on duplicate registration, so a package-level singleton alone would make
// repeated test setups fatal. Production code uses exactly one instance,
// registered once on controller-runtime's global registry (see
// PipelineMetricsInstance).
type PipelineMetrics struct {
	// writeQueueDepth / writeQueueCapacity together show how close the
	// CHWriter's bounded hand-off queue is to saturation — the earliest
	// warning that ClickHouse can't keep up with the reconcile rate.
	writeQueueDepth    prometheus.Gauge
	writeQueueCapacity prometheus.Gauge

	// writesTotal counts settled write outcomes, labelled success|failed, so a
	// rising failed rate is distinguishable from a healthy throughput dip.
	writesTotal *prometheus.CounterVec
	// writeLatency measures a single job's time from first attempt to final
	// settle (including retries), the direct signal of sink responsiveness.
	writeLatency prometheus.Histogram
	// writeRetryAttempts counts every attempt beyond the first, surfacing
	// retry storms that writesTotal alone hides (a write can succeed after
	// many retries and still count only once as a success).
	writeRetryAttempts prometheus.Counter
	// writeBatchRows records the number of rows in each flushed ClickHouse
	// batch, observed once per flush. It is the direct signal of how well the
	// batcher is coalescing: a distribution clustered near batchMaxRows means
	// full, efficient batches, while a mass near 1 means trickle traffic is
	// flushing on the batchMaxWait timer instead of filling batches.
	writeBatchRows prometheus.Histogram

	// enqueueBlock measures how long Enqueue blocks waiting for queue room —
	// the hot-path backpressure the reconciler actually feels. enqueueTimeouts
	// counts the cases where that wait gave up, i.e. the queue stayed full.
	enqueueBlock    prometheus.Histogram
	enqueueTimeouts prometheus.Counter

	// dedupSkips counts Reconciles that short-circuited because the object's
	// hash was unchanged — the proportion of work the hashCache saves.
	dedupSkips prometheus.Counter

	// hashcacheEntries reports the live entry count per kind, the in-memory
	// baseline footprint that Task 0.7 later works to shrink.
	hashcacheEntries *prometheus.GaugeVec
	// safeMode is 1 while a kind's cache is still warming from ClickHouse
	// history (cache-misses tagged Snapshot, not Added) and 0 once warm.
	safeMode *prometheus.GaugeVec

	// requeueDrops counts re-reconcile triggers dropped because the requeue
	// channel was full — a dropped trigger only delays a retry (the cache was
	// already reverted), but a nonzero rate means the channel is undersized.
	requeueDrops prometheus.Counter
}

// NewPipelineMetrics constructs every collector and registers it on reg.
// Registration uses MustRegister, so passing a registry that already holds
// these names panics — that is intentional: production passes the global
// registry exactly once (guarded by sync.Once in PipelineMetricsInstance),
// and each test passes its own fresh registry.
func NewPipelineMetrics(reg prometheus.Registerer) *PipelineMetrics {
	m := &PipelineMetrics{
		writeQueueDepth: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: metricsNamespace,
			Name:      "write_queue_depth",
			Help:      "Current number of jobs buffered in the CHWriter hand-off queue.",
		}),
		writeQueueCapacity: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: metricsNamespace,
			Name:      "write_queue_capacity",
			Help:      "Maximum number of jobs the CHWriter hand-off queue can buffer.",
		}),
		writesTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: metricsNamespace,
			Name:      "writes_total",
			Help:      "Count of settled ClickHouse write jobs by outcome.",
		}, []string{"outcome"}),
		writeLatency: prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: metricsNamespace,
			Name:      "write_latency_seconds",
			Help:      "Time from a write job's first attempt to its final settle, including retries.",
			Buckets:   prometheus.DefBuckets,
		}),
		writeRetryAttempts: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: metricsNamespace,
			Name:      "write_retry_attempts_total",
			Help:      "Count of write attempts beyond the first (i.e. retries) across all jobs.",
		}),
		writeBatchRows: prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: metricsNamespace,
			Name:      "write_batch_rows",
			Help:      "Number of rows in each flushed ClickHouse insert batch.",
			// Exponential buckets 1..2048 span a single trickle row through a
			// full batch at any realistic batchMaxRows setting.
			Buckets: prometheus.ExponentialBuckets(1, 2, 12),
		}),
		enqueueBlock: prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: metricsNamespace,
			Name:      "enqueue_block_seconds",
			Help:      "Time Enqueue spent blocked waiting for room in the write queue.",
			Buckets:   prometheus.DefBuckets,
		}),
		enqueueTimeouts: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: metricsNamespace,
			Name:      "enqueue_timeouts_total",
			Help:      "Count of Enqueue calls that gave up because the queue stayed full past the timeout.",
		}),
		dedupSkips: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: metricsNamespace,
			Name:      "dedup_skips_total",
			Help:      "Count of Reconciles short-circuited because the object's hash was unchanged.",
		}),
		hashcacheEntries: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: metricsNamespace,
			Name:      "hashcache_entries",
			Help:      "Current number of live hashCache entries, by kind.",
		}, []string{"kind"}),
		safeMode: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: metricsNamespace,
			Name:      "safe_mode",
			Help:      "1 while a kind's cache is still warming (Snapshot mode), 0 once warm.",
		}, []string{"kind"}),
		requeueDrops: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: metricsNamespace,
			Name:      "requeue_drops_total",
			Help:      "Count of re-reconcile triggers dropped because the requeue channel was full.",
		}),
	}

	// Materialize both outcome series at 0 up front: the label set is a fixed
	// enum, so exposing success and failed from the start makes dashboards and
	// rate() queries well-defined before the first write ever settles.
	m.writesTotal.WithLabelValues("success")
	m.writesTotal.WithLabelValues("failed")

	reg.MustRegister(
		m.writeQueueDepth,
		m.writeQueueCapacity,
		m.writesTotal,
		m.writeLatency,
		m.writeRetryAttempts,
		m.writeBatchRows,
		m.enqueueBlock,
		m.enqueueTimeouts,
		m.dedupSkips,
		m.hashcacheEntries,
		m.safeMode,
		m.requeueDrops,
	)
	return m
}

var (
	pipelineMetricsOnce      sync.Once
	pipelineMetricsSingleton *PipelineMetrics
)

// PipelineMetricsInstance returns the process-wide PipelineMetrics, registered
// exactly once on controller-runtime's global registry so the existing
// --metrics-bind-address server exposes them. The sync.Once guard makes
// repeated calls (e.g. the ClickHouse writer plus several reconcilers all
// fetching it) safe and non-duplicating.
func PipelineMetricsInstance() *PipelineMetrics {
	pipelineMetricsOnce.Do(func() {
		pipelineMetricsSingleton = NewPipelineMetrics(ctrlmetrics.Registry)
	})
	return pipelineMetricsSingleton
}

// The methods below implement the clickhouse.Metrics interface: the write-path
// slice of these collectors, exposed as behavior rather than raw fields so the
// clickhouse package depends on a narrow contract and never imports this one.

// SetWriteQueueDepth publishes the current CHWriter hand-off queue depth.
func (m *PipelineMetrics) SetWriteQueueDepth(n float64) { m.writeQueueDepth.Set(n) }

// SetWriteQueueCapacity publishes the fixed CHWriter hand-off queue capacity.
func (m *PipelineMetrics) SetWriteQueueCapacity(n float64) { m.writeQueueCapacity.Set(n) }

// ObserveEnqueueBlock records how long an Enqueue blocked waiting for room.
func (m *PipelineMetrics) ObserveEnqueueBlock(seconds float64) { m.enqueueBlock.Observe(seconds) }

// IncEnqueueTimeout counts an Enqueue that gave up because the queue stayed full.
func (m *PipelineMetrics) IncEnqueueTimeout() { m.enqueueTimeouts.Inc() }

// ObserveWriteLatency records a job's first-attempt-to-final-settle latency.
func (m *PipelineMetrics) ObserveWriteLatency(seconds float64) { m.writeLatency.Observe(seconds) }

// IncWriteRetryAttempt counts one write attempt beyond the first.
func (m *PipelineMetrics) IncWriteRetryAttempt() { m.writeRetryAttempts.Inc() }

// ObserveWriteBatchRows records the row count of one flushed insert batch.
func (m *PipelineMetrics) ObserveWriteBatchRows(rows float64) { m.writeBatchRows.Observe(rows) }

// IncWrite counts one settled write by outcome ("success" | "failed").
func (m *PipelineMetrics) IncWrite(outcome string) { m.writesTotal.WithLabelValues(outcome).Inc() }
