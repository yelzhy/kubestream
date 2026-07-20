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
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/cenkalti/backoff/v4"
	"github.com/go-logr/logr"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
)

const (
	defaultWriteQueueSize       = 5000
	defaultWriteWorkers         = 4
	defaultInsertTimeout        = 5 * time.Second
	defaultEnqueueTimeout       = 2 * time.Second
	defaultMaxRetryBackoff      = 60 * time.Second
	defaultShutdownDrainTimeout = 15 * time.Second

	chTimeFormat = "2006-01-02 15:04:05.999999999"

	insertResourceStateQuery = `
        INSERT INTO resource_states (
            ts, cluster_id, event_type, api_group, api_version, kind, namespace, name, uid, resource_version, labels, data, diff_data, sha256
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
)

// writeJob is a single ClickHouse insert submitted by a Reconcile call.
// commit is invoked by a CHWriter worker exactly once, only after the write
// has been durably confirmed or definitively abandoned after retries — it is
// the sole place cache mutation for that job's object is allowed to happen,
// so a failed write can never be mistaken for a persisted one.
type writeJob struct {
	query  string
	args   []any
	commit func(ok bool)
}

// CHWriter decouples ClickHouse inserts from the Reconcile hot path. Writes
// are handed off to a bounded queue and drained by a small worker pool, so a
// slow or unavailable ClickHouse connection never blocks a Reconcile call.
// A single CHWriter is shared across all watched GVKs.
type CHWriter struct {
	conn    driver.Conn
	jobs    chan writeJob
	workers int

	insertTimeout        time.Duration
	maxRetryBackoff      time.Duration
	shutdownDrainTimeout time.Duration

	// mu guards closing and drainCtx; see Enqueue/attemptContext.
	mu      sync.Mutex
	closing bool
	// inflight tracks Enqueue calls that observed closing==false and are
	// therefore permitted to send on jobs; Start waits for it to drain to
	// zero before closing jobs, so a send can never race a close.
	inflight sync.WaitGroup
	// drainCtx is swapped from a plain context.Background() to a
	// shutdownDrainTimeout-bounded context the moment Start detects shutdown
	// (ctx.Done() fires) — see attemptContext. Starts non-nil so a worker
	// that reads it before Start ever swaps it (a narrow, harmless race) still
	// gets a safe, non-nil context rather than risking a nil-context panic.
	drainCtx context.Context

	// otherUsers, if set, is waited on after this CHWriter's own workers
	// finish draining and before conn is closed — for goroutines outside
	// CHWriter (namely restoreAndWarm) that also use the shared connection.
	// See WaitForOthers.
	otherUsers *sync.WaitGroup

	// metrics records queue depth/capacity, enqueue blocking and timeouts, and
	// per-job write latency, retries, and outcomes. Never nil once built via
	// NewCHWriter; tests may swap in an isolated instance before Start.
	metrics *pipelineMetrics
}

// NewCHWriter builds a CHWriter around an existing ClickHouse connection.
// Zero-valued queueSize/workers/timeouts fall back to sane defaults.
func NewCHWriter(conn driver.Conn, queueSize, workers int, insertTimeout, maxRetryBackoff, shutdownDrainTimeout time.Duration) *CHWriter {
	if queueSize <= 0 {
		queueSize = defaultWriteQueueSize
	}
	if workers <= 0 {
		workers = defaultWriteWorkers
	}
	if insertTimeout <= 0 {
		insertTimeout = defaultInsertTimeout
	}
	if maxRetryBackoff <= 0 {
		maxRetryBackoff = defaultMaxRetryBackoff
	}
	if shutdownDrainTimeout <= 0 {
		shutdownDrainTimeout = defaultShutdownDrainTimeout
	}
	return &CHWriter{
		conn:                 conn,
		jobs:                 make(chan writeJob, queueSize),
		workers:              workers,
		insertTimeout:        insertTimeout,
		maxRetryBackoff:      maxRetryBackoff,
		shutdownDrainTimeout: shutdownDrainTimeout,
		drainCtx:             context.Background(),
		metrics:              pipelineMetricsInstance(),
	}
}

// WaitForOthers registers a WaitGroup that Start waits on, after its own
// workers finish draining and before it closes conn, for goroutines outside
// CHWriter that also use the shared connection (namely restoreAndWarm). The
// caller is responsible for Add/Done-ing it around any such goroutine's
// lifetime. Must be called before Start.
func (w *CHWriter) WaitForOthers(wg *sync.WaitGroup) {
	w.otherUsers = wg
}

// Enqueue submits a write job without blocking the caller on the actual
// ClickHouse round-trip. If the queue is full, it waits up to enqueueTimeout
// (falls back to a default if <= 0) for room before giving up — a job is
// never dropped silently. A returned error should be propagated by the
// caller (e.g. as Reconcile's own error) so controller-runtime's requeue and
// backoff take over as backpressure; nothing is lost in the meantime because
// the informer cache still holds the object's latest state for the retry.
func (w *CHWriter) Enqueue(ctx context.Context, enqueueTimeout time.Duration, job writeJob) error {
	w.mu.Lock()
	if w.closing {
		w.mu.Unlock()
		return fmt.Errorf("chwriter: shutting down, refusing new write")
	}
	w.inflight.Add(1)
	w.mu.Unlock()
	defer w.inflight.Done()

	if enqueueTimeout <= 0 {
		enqueueTimeout = defaultEnqueueTimeout
	}
	timer := time.NewTimer(enqueueTimeout)
	defer timer.Stop()

	// enqueue_block_seconds measures how long the hot path actually waited for
	// room, whether the send eventually succeeded or timed out — the direct
	// backpressure signal a reconciler feels.
	start := time.Now()
	select {
	case w.jobs <- job:
		w.metrics.enqueueBlock.Observe(time.Since(start).Seconds())
		w.metrics.writeQueueDepth.Set(float64(len(w.jobs)))
		return nil
	case <-ctx.Done():
		w.metrics.enqueueBlock.Observe(time.Since(start).Seconds())
		return ctx.Err()
	case <-timer.C:
		w.metrics.enqueueBlock.Observe(time.Since(start).Seconds())
		w.metrics.enqueueTimeouts.Inc()
		return fmt.Errorf("chwriter: write queue still full after waiting %s", enqueueTimeout)
	}
}

// Start implements manager.Runnable. It runs the worker pool until ctx is
// cancelled, then shuts down in a strict order so no write is ever stranded
// or raced against connection closure:
//  1. Stop accepting new Enqueue calls (under mu, so this can't race a send).
//  2. Swap in a fresh, shutdownDrainTimeout-bounded drainCtx for any job
//     processed from here on — see attemptContext for why the original ctx
//     (already cancelled by this point) can't be reused for these attempts.
//  3. Wait for any Enqueue call already past the closing check to finish
//     sending (or bail via its own ctx/timeout) — after this, jobs can
//     receive no further sends from anyone.
//  4. Close jobs. Workers range over it, so they drain every already-queued
//     job and then exit cleanly once it's both empty and closed — no worker
//     can exit "too early" and leave a job stranded.
//  5. Wait for otherUsers (if set) — other goroutines sharing conn.
//  6. Close conn — guaranteed safe now, since nothing can still be using it.
func (w *CHWriter) Start(ctx context.Context) error {
	log := logf.Log.WithName("chwriter")

	// Capacity is fixed for this CHWriter's lifetime; publishing it here (once
	// the queue exists) lets dashboards express depth as a fraction of it.
	w.metrics.writeQueueCapacity.Set(float64(cap(w.jobs)))

	var wg sync.WaitGroup
	for i := 0; i < w.workers; i++ {
		wg.Go(func() {
			for job := range w.jobs {
				w.process(w.attemptContext(ctx), log, job)
			}
		})
	}

	<-ctx.Done()

	drainCtx, cancel := context.WithTimeout(context.Background(), w.shutdownDrainTimeout)
	defer cancel()

	w.mu.Lock()
	w.closing = true
	w.drainCtx = drainCtx
	w.mu.Unlock()
	w.inflight.Wait()
	close(w.jobs)
	wg.Wait()

	if w.otherUsers != nil {
		w.otherUsers.Wait()
	}

	if err := w.conn.Close(); err != nil {
		log.Error(err, "chwriter: failed to close ClickHouse connection")
		return err
	}
	log.Info("chwriter: ClickHouse connection closed")
	return nil
}

// attemptContext returns ctx unchanged while it's still live, so a write that
// is genuinely in flight when shutdown begins is still cancelled promptly
// rather than allowed to run past the manager's own shutdown deadline. Once
// ctx has already fired — meaning this job is being pulled from the queue
// during Start's post-shutdown drain, not interrupted mid-attempt — deriving
// its write's timeout from ctx would guarantee an instant, no-chance failure
// on the very first attempt regardless of ClickHouse's actual health,
// defeating the drain phase's whole purpose. Such jobs get drainCtx instead:
// one context.WithTimeout(shutdownDrainTimeout) shared by every job drained
// during shutdown, so the whole drain phase (not each job individually) is
// what's bounded, giving queued writes a genuine chance to flush to a
// healthy ClickHouse before the process exits.
func (w *CHWriter) attemptContext(ctx context.Context) context.Context {
	if ctx.Err() == nil {
		return ctx
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.drainCtx
}

// process writes a single job to ClickHouse, retrying with exponential
// backoff. Each attempt's deadline is derived from ctx (the same context
// Start received), so a manager shutdown cancels an in-flight attempt
// immediately instead of letting it run for a full insertTimeout. It always
// reports the outcome via job.commit exactly once, only after the write is
// truly settled.
//
//nolint:logcheck
func (w *CHWriter) process(ctx context.Context, log logr.Logger, job writeJob) {
	// A job leaves the queue the moment we pull it, so refresh depth here too —
	// not only on enqueue — so a draining queue is reflected, not just a
	// filling one.
	w.metrics.writeQueueDepth.Set(float64(len(w.jobs)))

	eb := backoff.NewExponentialBackOff()
	eb.MaxElapsedTime = w.maxRetryBackoff

	start := time.Now()
	attempts := 0
	err := backoff.Retry(func() error {
		// Every attempt after the first is a retry; counting them here (rather
		// than the successes) is what makes a retry storm visible even when the
		// writes ultimately succeed.
		if attempts > 0 {
			w.metrics.writeRetryAttempts.Inc()
		}
		attempts++
		writeCtx, cancel := context.WithTimeout(ctx, w.insertTimeout)
		defer cancel()
		return w.conn.Exec(writeCtx, job.query, job.args...)
	}, backoff.WithContext(eb, ctx))

	w.metrics.writeLatency.Observe(time.Since(start).Seconds())
	if err != nil {
		w.metrics.writesTotal.WithLabelValues("failed").Inc()
		log.Error(err, "chwriter: giving up on write after exhausting retries")
	} else {
		w.metrics.writesTotal.WithLabelValues("success").Inc()
	}
	if job.commit != nil {
		job.commit(err == nil)
	}
}
