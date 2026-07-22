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

// Package clickhouse is the ClickHouse implementation of the sink contract
// (internal/sink). CHWriter implements both sink.Writer (batched, asynchronous
// inserts off the caller's hot path) and sink.StateReader (cache warm-up
// history), and owns the single shared ClickHouse connection for both, plus
// connect-time schema validation.
package clickhouse

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/cenkalti/backoff/v4"
	"github.com/go-logr/logr"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/manager"

	"github.com/yelzhy/kubestream/internal/sink"
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
            ts, cluster_id, event_type, api_group, api_version, kind, namespace, name, uid, resource_version, labels, actors, data, diff, sha256
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
)

// Config holds the externally configurable ClickHouse connection settings. It
// carries no defaults of its own — cmd/main.go is responsible for sourcing
// every field from flags/environment variables, so no ClickHouse host,
// credential, or timeout is ever hardcoded in this package.
type Config struct {
	Addr        string
	Database    string
	Username    string
	Password    string
	DialTimeout time.Duration
	ReadTimeout time.Duration
	// AutoCreateSchema, when true, makes the operator execute the shipped DDL
	// (deploy/clickhouse/schema) idempotently at connect time before validating
	// the live schema. Sourced from --ch-auto-create-schema / CH_AUTO_CREATE_SCHEMA;
	// defaults to false so the operator never mutates ClickHouse DDL unless asked.
	AutoCreateSchema bool
}

// Metrics is the narrow slice of pipeline metrics CHWriter records. It is an
// interface (rather than a concrete dependency on the pipeline metrics struct)
// so this package never imports internal/controller: the caller injects an
// implementation — see internal/controller.PipelineMetrics.
type Metrics interface {
	// SetWriteQueueDepth publishes the current hand-off queue depth.
	SetWriteQueueDepth(n float64)
	// SetWriteQueueCapacity publishes the fixed hand-off queue capacity.
	SetWriteQueueCapacity(n float64)
	// ObserveEnqueueBlock records how long an Enqueue blocked waiting for room.
	ObserveEnqueueBlock(seconds float64)
	// IncEnqueueTimeout counts an Enqueue that gave up because the queue stayed full.
	IncEnqueueTimeout()
	// ObserveWriteLatency records a job's first-attempt-to-final-settle latency.
	ObserveWriteLatency(seconds float64)
	// IncWriteRetryAttempt counts one write attempt beyond the first.
	IncWriteRetryAttempt()
	// IncWrite counts one settled write by outcome ("success" | "failed").
	IncWrite(outcome string)
}

// writeJob is a single ClickHouse insert drained by a CHWriter worker. It is the
// backend-specific rendering of a sink.Job: the query/args are computed once, at
// Enqueue time, from the job's Record, so a worker never touches sink types. The
// commit callback carries the exactly-once contract documented on sink.Job.
type writeJob struct {
	query  string
	args   []any
	commit func(ok bool)
}

// CHWriter decouples ClickHouse inserts from the caller's hot path. Writes
// are handed off to a bounded queue and drained by a small worker pool, so a
// slow or unavailable ClickHouse connection never blocks a reconcile. A single
// CHWriter is shared across all watched GVKs. It implements sink.Writer and
// sink.StateReader (see statereader.go), sharing one connection for both.
type CHWriter struct {
	conn driver.Conn
	// database is the schema-introspection target for connect-time validation;
	// set by Open, empty for test-constructed writers that never validate.
	database string
	// autoCreate mirrors Config.AutoCreateSchema; consulted by RegisterWithManager.
	autoCreate bool

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

	// otherUsers tracks in-flight uses of conn that are NOT the worker pool —
	// namely LastKnownStates calls (cache warm-up) and the schema-validation
	// runnable, both of which share this CHWriter's connection. Start waits for
	// it to drain to zero, after its own workers finish, before closing conn, so
	// shutdown can never race a use of the shared connection against its
	// closure. Registration mirrors inflight: a user Adds only after observing
	// closing==false under mu, so an Add can never race Start's Wait.
	otherUsers sync.WaitGroup

	// metrics records queue depth/capacity, enqueue blocking and timeouts, and
	// per-job write latency, retries, and outcomes. Never nil once built via
	// NewCHWriter.
	metrics Metrics
}

// NewCHWriter builds a CHWriter around an existing ClickHouse connection and
// the metrics sink it reports to. Zero-valued queueSize/workers/timeouts fall
// back to sane defaults. Exposed (rather than only Open) so tests can drive the
// writer with a fake driver.Conn and an isolated metrics instance.
func NewCHWriter(conn driver.Conn, queueSize, workers int, insertTimeout, maxRetryBackoff, shutdownDrainTimeout time.Duration, metrics Metrics) *CHWriter {
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
		metrics:              metrics,
	}
}

// Open dials ClickHouse per cfg and returns a CHWriter over that connection,
// reporting to metrics. insertTimeout is cfg.ReadTimeout, so the
// operator-configured value governs the async write path (not just the
// connection's own driver-level timeout). The returned CHWriter owns conn and
// closes it on shutdown (see Start); register it with RegisterWithManager.
func Open(cfg Config, metrics Metrics) (*CHWriter, error) {
	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{cfg.Addr},
		Auth: clickhouse.Auth{
			Database: cfg.Database,
			Username: cfg.Username,
			Password: cfg.Password,
		},
		Protocol:    clickhouse.Native,
		DialTimeout: cfg.DialTimeout,
		ReadTimeout: cfg.ReadTimeout,
	})
	if err != nil {
		return nil, err
	}
	w := NewCHWriter(conn, 0, 0, cfg.ReadTimeout, 0, 0, metrics)
	w.database = cfg.Database
	w.autoCreate = cfg.AutoCreateSchema
	return w, nil
}

// RegisterWithManager wires this CHWriter's lifecycle into mgr: the writer's
// own Start runnable, the connect-time schema-validation runnable (which shares
// conn and is therefore tracked in otherUsers so Start never closes conn while
// it is mid-introspection), and the "clickhouse-schema" readyz check that
// degrades readiness on a confirmed schema mismatch. The schema work runs as a
// background runnable so mgr.Start is never gated on ClickHouse being reachable
// at boot.
func (w *CHWriter) RegisterWithManager(mgr manager.Manager) error {
	if err := mgr.Add(w); err != nil {
		return err
	}

	gate := newSchemaGate()
	if err := mgr.AddReadyzCheck("clickhouse-schema", gate.readyCheck); err != nil {
		return err
	}

	return mgr.Add(manager.RunnableFunc(func(ctx context.Context) error {
		// Track this conn user exactly like a LastKnownStates call: register
		// only while not closing (under mu) so the Add can never race Start's
		// otherUsers.Wait, and release when validation returns.
		w.mu.Lock()
		if w.closing {
			w.mu.Unlock()
			return nil
		}
		w.otherUsers.Add(1)
		w.mu.Unlock()
		defer w.otherUsers.Done()

		validateSchemaWithRetry(ctx, w.conn, w.database, w.autoCreate, gate, logf.Log.WithName("schema"))
		return nil
	}))
}

// Enqueue submits a write job without blocking the caller on the actual
// ClickHouse round-trip. The job's Record is rendered to the insert query and
// positional args here, once, so workers never touch sink types. If the queue
// is full, it waits up to the default enqueue timeout for room before giving up
// — a job is never dropped silently. A returned error should be propagated by
// the caller (e.g. as Reconcile's own error) so controller-runtime's requeue
// and backoff take over as backpressure; nothing is lost in the meantime
// because the informer cache still holds the object's latest state for the
// retry.
func (w *CHWriter) Enqueue(ctx context.Context, job sink.Job) error {
	internal := writeJob{
		query:  insertResourceStateQuery,
		args:   insertArgs(job.Record),
		commit: job.Commit,
	}

	w.mu.Lock()
	if w.closing {
		w.mu.Unlock()
		return fmt.Errorf("chwriter: shutting down, refusing new write")
	}
	w.inflight.Add(1)
	w.mu.Unlock()
	defer w.inflight.Done()

	timer := time.NewTimer(defaultEnqueueTimeout)
	defer timer.Stop()

	// enqueue_block_seconds measures how long the hot path actually waited for
	// room, whether the send eventually succeeded or timed out — the direct
	// backpressure signal a reconciler feels.
	start := time.Now()
	select {
	case w.jobs <- internal:
		w.metrics.ObserveEnqueueBlock(time.Since(start).Seconds())
		w.metrics.SetWriteQueueDepth(float64(len(w.jobs)))
		return nil
	case <-ctx.Done():
		w.metrics.ObserveEnqueueBlock(time.Since(start).Seconds())
		return ctx.Err()
	case <-timer.C:
		w.metrics.ObserveEnqueueBlock(time.Since(start).Seconds())
		w.metrics.IncEnqueueTimeout()
		return fmt.Errorf("chwriter: write queue still full after waiting %s", defaultEnqueueTimeout)
	}
}

// Start implements manager.Runnable and sink.Writer. It runs the worker pool
// until ctx is cancelled, then shuts down in a strict order so no write is ever
// stranded or raced against connection closure:
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
//  5. Wait for otherUsers — the LastKnownStates / schema-validation goroutines
//     that share conn.
//  6. Close conn — guaranteed safe now, since nothing can still be using it.
func (w *CHWriter) Start(ctx context.Context) error {
	log := logf.Log.WithName("chwriter")

	// Capacity is fixed for this CHWriter's lifetime; publishing it here (once
	// the queue exists) lets dashboards express depth as a fraction of it.
	w.metrics.SetWriteQueueCapacity(float64(cap(w.jobs)))

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

	w.otherUsers.Wait()

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
	w.metrics.SetWriteQueueDepth(float64(len(w.jobs)))

	eb := backoff.NewExponentialBackOff()
	eb.MaxElapsedTime = w.maxRetryBackoff

	start := time.Now()
	attempts := 0
	err := backoff.Retry(func() error {
		// Every attempt after the first is a retry; counting them here (rather
		// than the successes) is what makes a retry storm visible even when the
		// writes ultimately succeed.
		if attempts > 0 {
			w.metrics.IncWriteRetryAttempt()
		}
		attempts++
		writeCtx, cancel := context.WithTimeout(ctx, w.insertTimeout)
		defer cancel()
		return w.conn.Exec(writeCtx, job.query, job.args...)
	}, backoff.WithContext(eb, ctx))

	w.metrics.ObserveWriteLatency(time.Since(start).Seconds())
	if err != nil {
		w.metrics.IncWrite("failed")
		log.Error(err, "chwriter: giving up on write after exhausting retries")
	} else {
		w.metrics.IncWrite("success")
	}
	if job.commit != nil {
		job.commit(err == nil)
	}
}

// insertArgs returns the positional arguments for the resource_states INSERT,
// in exactly the column order expected by insertResourceStateQuery. nil
// Labels/Actors are coerced to empty containers so the Map/Array columns always
// bind a concrete value rather than a NULL the non-nullable schema would reject.
func insertArgs(rec sink.Record) []any {
	labels := rec.Labels
	if labels == nil {
		labels = map[string]string{}
	}
	actors := rec.Actors
	if actors == nil {
		actors = []string{}
	}
	return []any{
		rec.Timestamp.UTC().Format(chTimeFormat), rec.ClusterID, rec.EventType, rec.APIGroup, rec.APIVersion,
		rec.Kind, rec.Namespace, rec.Name, rec.UID, rec.ResourceVersion, labels, actors, rec.Data, rec.Diff, rec.SHA256,
	}
}
