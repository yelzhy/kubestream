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

package clickhouse

import (
	"context"
	"errors"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/yelzhy/kubestream/internal/controller"
	"github.com/yelzhy/kubestream/internal/sink"
)

// nameArgIndex is the position of Record.Name in the positional args produced by
// insertArgs; the fakes use it to identify a specific "poison" row.
const nameArgIndex = 7

// erroringConn is a driver.Conn whose batch and single-row writes always fail,
// used to prove the failure-outcome accounting. Embedding the interface
// satisfies the full method set; only PrepareBatch, Exec, and Close are ever
// exercised here. PrepareBatch failing forces the batch path to exhaust retries
// and fall through to individual isolation, whose Exec then also fails.
type erroringConn struct {
	driver.Conn
}

func (erroringConn) PrepareBatch(context.Context, string, ...driver.PrepareBatchOption) (driver.Batch, error) {
	return nil, errors.New("clickhouse unavailable")
}

func (erroringConn) Exec(context.Context, string, ...any) error {
	return errors.New("clickhouse unavailable")
}

func (erroringConn) Close() error { return nil }

// fakeConn is a controllable driver.Conn for the batching tests. Its batch Send
// and single-row Exec behaviors are injected via sendErr/execErr (nil hook =
// success), and it records Send/Exec counts plus a monotonic sequence so tests
// can assert ordering (e.g. the drain flush's Send happening before Close).
type fakeConn struct {
	driver.Conn

	sendCount atomic.Int64
	execCount atomic.Int64

	seq      atomic.Int64 // monotonic tick incremented on Send and Close
	lastSend atomic.Int64 // seq of the most recent Send
	closeSeq atomic.Int64 // seq at which Close ran

	// sendErr, if set, decides the outcome of a batch Send given its context
	// and the appended rows. execErr does the same for a single-row Exec.
	sendErr func(ctx context.Context, rows [][]any) error
	execErr func(ctx context.Context, args []any) error
}

func (c *fakeConn) PrepareBatch(ctx context.Context, _ string, _ ...driver.PrepareBatchOption) (driver.Batch, error) {
	return &fakeBatch{conn: c, ctx: ctx}, nil
}

func (c *fakeConn) Exec(ctx context.Context, _ string, args ...any) error {
	c.execCount.Add(1)
	if c.execErr != nil {
		return c.execErr(ctx, args)
	}
	return nil
}

func (c *fakeConn) Close() error {
	c.closeSeq.Store(c.seq.Add(1))
	return nil
}

// fakeBatch is the driver.Batch returned by fakeConn.PrepareBatch. It buffers
// appended rows so a sendErr hook can inspect them, and captures the context so
// a test can make Send block until the context is cancelled (the "cancel
// mid-batch" scenario).
type fakeBatch struct {
	driver.Batch

	conn *fakeConn
	ctx  context.Context
	rows [][]any
}

func (b *fakeBatch) Append(v ...any) error {
	b.rows = append(b.rows, v)
	return nil
}

func (b *fakeBatch) Send() error {
	b.conn.sendCount.Add(1)
	b.conn.lastSend.Store(b.conn.seq.Add(1))
	if b.conn.sendErr != nil {
		return b.conn.sendErr(b.ctx, b.rows)
	}
	return nil
}

func (b *fakeBatch) Abort() error { return nil }
func (b *fakeBatch) Close() error { return nil }

// commitLog records every commit callback invocation, keyed by record name, so
// tests can assert both the outcome (true/false counts) and — crucially for the
// exactly-once contract — that no job's callback fired more than once.
type commitLog struct {
	mu     sync.Mutex
	byName map[string][]bool
}

func newCommitLog() *commitLog { return &commitLog{byName: map[string][]bool{}} }

func (c *commitLog) record(name string, ok bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.byName[name] = append(c.byName[name], ok)
}

// counts returns the total number of commits, and how many were true / false.
func (c *commitLog) counts() (total, trues, falses int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, oks := range c.byName {
		for _, ok := range oks {
			total++
			if ok {
				trues++
			} else {
				falses++
			}
		}
	}
	return total, trues, falses
}

// maxPerName returns the highest number of commits any single job received; a
// value >1 means some job's callback fired more than once (contract violation).
func (c *commitLog) maxPerName() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	maxN := 0
	for _, oks := range c.byName {
		if len(oks) > maxN {
			maxN = len(oks)
		}
	}
	return maxN
}

// enqueueNamed submits a single record whose Name is name, routing its commit
// outcome into log.
func enqueueNamed(t *testing.T, w *CHWriter, ctx context.Context, log *commitLog, name string) {
	t.Helper()
	if err := w.Enqueue(ctx, sink.Job{
		Record: sink.Record{Name: name},
		Commit: func(ok bool) { log.record(name, ok) },
	}); err != nil {
		t.Fatalf("Enqueue(%s): %v", name, err)
	}
}

// waitForCommits blocks until log has recorded at least n commits or the
// timeout elapses, failing the test on timeout.
func waitForCommits(t *testing.T, log *commitLog, n int, timeout time.Duration) {
	t.Helper()
	deadline := time.After(timeout)
	tick := time.NewTicker(time.Millisecond)
	defer tick.Stop()
	for {
		if total, _, _ := log.counts(); total >= n {
			return
		}
		select {
		case <-deadline:
			total, _, _ := log.counts()
			t.Fatalf("timed out waiting for %d commits; got %d", n, total)
		case <-tick.C:
		}
	}
}

// TestBatchFlushBoundsSendCalls covers AC (a): 100 jobs with batchMaxRows=10
// produce at most ⌈100/10⌉ + workers Send calls — the full batches plus, at
// worst, one trailing partial batch per worker flushed during drain.
func TestBatchFlushBoundsSendCalls(t *testing.T) {
	const jobs, batchMaxRows, workers = 100, 10, 4
	conn := &fakeConn{} // healthy: nil hooks

	reg := prometheus.NewRegistry()
	m := controller.NewPipelineMetrics(reg)
	// Large batchMaxWait so only row-count and drain drive flushes, never the timer.
	w := NewCHWriter(conn, jobs, workers, batchMaxRows, 5*time.Millisecond, time.Second, time.Second, 30*time.Second, time.Second, m)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- w.Start(ctx) }()

	log := newCommitLog()
	for i := range jobs {
		enqueueNamed(t, w, ctx, log, "r"+strconv.Itoa(i))
	}

	// Stop accepting, letting the drain flush every partial batch.
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("Start returned error: %v", err)
	}

	if total, trues, _ := log.counts(); total != jobs || trues != jobs {
		t.Fatalf("commits: total=%d trues=%d, want %d/%d", total, trues, jobs, jobs)
	}
	if n := log.maxPerName(); n != 1 {
		t.Fatalf("a job committed %d times, want exactly 1", n)
	}
	bound := int64((jobs+batchMaxRows-1)/batchMaxRows + workers)
	if got := conn.sendCount.Load(); got > bound {
		t.Fatalf("Send calls = %d, want <= %d", got, bound)
	}
}

// TestPoisonRowIsolation covers AC (b): a single poison row in a 10-row batch,
// whose batch Send always fails, yields exactly one commit(false) (the poison
// row) and nine commit(true) after individual isolation.
func TestPoisonRowIsolation(t *testing.T) {
	const batchMaxRows = 10
	conn := &fakeConn{
		// The batch never succeeds, forcing the isolation path.
		sendErr: func(context.Context, [][]any) error { return errors.New("batch rejected") },
		// Only the poison row fails its individual attempt.
		execErr: func(_ context.Context, args []any) error {
			if len(args) > nameArgIndex && args[nameArgIndex] == "poison" {
				return errors.New("bad row")
			}
			return nil
		},
	}

	reg := prometheus.NewRegistry()
	m := controller.NewPipelineMetrics(reg)
	// One worker so all ten jobs land in one batch; tiny retry cap so the batch
	// exhausts quickly; large batchMaxWait so row-count (not the timer) flushes.
	w := NewCHWriter(conn, 100, 1, batchMaxRows, 5*time.Millisecond, 20*time.Millisecond, time.Second, 30*time.Second, time.Second, m)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- w.Start(ctx) }()

	log := newCommitLog()
	for i := range batchMaxRows - 1 {
		enqueueNamed(t, w, ctx, log, "ok"+strconv.Itoa(i))
	}
	enqueueNamed(t, w, ctx, log, "poison")

	waitForCommits(t, log, batchMaxRows, 10*time.Second)

	total, trues, falses := log.counts()
	if total != batchMaxRows || trues != 9 || falses != 1 {
		t.Fatalf("commits: total=%d trues=%d falses=%d, want 10/9/1", total, trues, falses)
	}
	if n := log.maxPerName(); n != 1 {
		t.Fatalf("a job committed %d times, want exactly 1", n)
	}
	if v := writesTotalValue(t, reg, "failed"); v != 1 {
		t.Fatalf("writes_total{failed} = %v, want 1", v)
	}
	if v := writesTotalValue(t, reg, "success"); v != 9 {
		t.Fatalf("writes_total{success} = %v, want 9", v)
	}

	cancel()
	if err := <-done; err != nil {
		t.Fatalf("Start returned error: %v", err)
	}
}

// TestLoneJobFlushesOnWait covers AC (c): a single job flushes after
// batchMaxWait elapses even though no further traffic ever arrives to fill the
// batch — and it does not flush instantly, proving the timer (not the drain)
// drove it.
func TestLoneJobFlushesOnWait(t *testing.T) {
	const batchMaxWait = 80 * time.Millisecond
	conn := &fakeConn{} // healthy

	reg := prometheus.NewRegistry()
	m := controller.NewPipelineMetrics(reg)
	// batchMaxRows well above 1 so only the wait timer can flush the lone job.
	w := NewCHWriter(conn, 100, 1, 100, 5*time.Millisecond, time.Second, time.Second, batchMaxWait, time.Second, m)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- w.Start(ctx) }()

	log := newCommitLog()
	start := time.Now()
	enqueueNamed(t, w, ctx, log, "lonely")

	waitForCommits(t, log, 1, 5*time.Second)
	elapsed := time.Since(start)

	if elapsed < batchMaxWait/2 {
		t.Fatalf("lone job settled in %s, too fast to have waited for batchMaxWait (%s)", elapsed, batchMaxWait)
	}
	if total, trues, _ := log.counts(); total != 1 || trues != 1 {
		t.Fatalf("commits: total=%d trues=%d, want 1/1", total, trues)
	}
	if got := conn.sendCount.Load(); got != 1 {
		t.Fatalf("Send calls = %d, want 1", got)
	}

	cancel()
	if err := <-done; err != nil {
		t.Fatalf("Start returned error: %v", err)
	}
}

// TestShutdownFlushesPartialBatch covers AC (d): a half-full batch left in a
// worker at shutdown is flushed during the drain window, and that flush's Send
// happens strictly before conn.Close.
func TestShutdownFlushesPartialBatch(t *testing.T) {
	conn := &fakeConn{} // healthy

	reg := prometheus.NewRegistry()
	m := controller.NewPipelineMetrics(reg)
	// One worker, batchMaxRows=10, large batchMaxWait: the 5 jobs never reach a
	// row-count or timer flush, so only the drain can flush them.
	w := NewCHWriter(conn, 100, 1, 10, 5*time.Millisecond, time.Second, time.Second, 30*time.Second, time.Second, m)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- w.Start(ctx) }()

	log := newCommitLog()
	const partial = 5
	for i := range partial {
		enqueueNamed(t, w, ctx, log, "p"+strconv.Itoa(i))
	}

	// Nothing has flushed yet (batch is half full, timer far off).
	if got := conn.sendCount.Load(); got != 0 {
		t.Fatalf("Send calls before shutdown = %d, want 0", got)
	}

	cancel()
	if err := <-done; err != nil {
		t.Fatalf("Start returned error: %v", err)
	}

	if total, trues, _ := log.counts(); total != partial || trues != partial {
		t.Fatalf("commits: total=%d trues=%d, want %d/%d", total, trues, partial, partial)
	}
	if got := conn.sendCount.Load(); got != 1 {
		t.Fatalf("Send calls = %d, want 1 (the drained partial batch)", got)
	}
	if send, close := conn.lastSend.Load(), conn.closeSeq.Load(); send == 0 || send >= close {
		t.Fatalf("drain flush ordering: lastSend=%d closeSeq=%d, want 0 < send < close", send, close)
	}
}

// TestConcurrentEnqueueStorm covers AC (e): many goroutines enqueuing at once
// settle cleanly with every job committed exactly once. Run under -race to
// exercise the batching worker's concurrency.
func TestConcurrentEnqueueStorm(t *testing.T) {
	const producers, perProducer = 20, 50
	const jobs = producers * perProducer
	conn := &fakeConn{} // healthy

	reg := prometheus.NewRegistry()
	m := controller.NewPipelineMetrics(reg)
	w := NewCHWriter(conn, jobs, 4, 10, 5*time.Millisecond, time.Second, time.Second, 20*time.Millisecond, time.Second, m)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- w.Start(ctx) }()

	log := newCommitLog()
	var wg sync.WaitGroup
	for p := range producers {
		wg.Go(func() {
			for i := range perProducer {
				enqueueNamed(t, w, ctx, log, "p"+strconv.Itoa(p)+"-"+strconv.Itoa(i))
			}
		})
	}
	wg.Wait()

	cancel()
	if err := <-done; err != nil {
		t.Fatalf("Start returned error: %v", err)
	}

	if total, trues, _ := log.counts(); total != jobs || trues != jobs {
		t.Fatalf("commits: total=%d trues=%d, want %d/%d", total, trues, jobs, jobs)
	}
	if n := log.maxPerName(); n != 1 {
		t.Fatalf("a job committed %d times, want exactly 1", n)
	}
}

// TestCancelMidBatchCommitsOnce covers AC (f): cancelling the context while a
// batch's Send is in flight settles every job exactly once (never twice),
// proven by an atomic per-job commit counter.
func TestCancelMidBatchCommitsOnce(t *testing.T) {
	const batchMaxRows = 5
	conn := &fakeConn{
		// Send blocks until the batch context is cancelled, then fails — this is
		// the "in flight when shutdown begins" moment.
		sendErr: func(ctx context.Context, _ [][]any) error {
			<-ctx.Done()
			return ctx.Err()
		},
		// The isolation Exec that follows also fails under the cancelled context.
		execErr: func(ctx context.Context, _ []any) error { return ctx.Err() },
	}

	reg := prometheus.NewRegistry()
	m := controller.NewPipelineMetrics(reg)
	w := NewCHWriter(conn, 100, 1, batchMaxRows, time.Second, time.Second, time.Second, 30*time.Second, time.Second, m)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- w.Start(ctx) }()

	// Instrument exactly-once with an atomic counter per job, independent of the
	// commitLog, so a double-fire is caught even under -race.
	var commits [batchMaxRows]atomic.Int64
	for i := range batchMaxRows {
		idx := i
		if err := w.Enqueue(ctx, sink.Job{
			Record: sink.Record{Name: "c" + strconv.Itoa(idx)},
			Commit: func(bool) { commits[idx].Add(1) },
		}); err != nil {
			t.Fatalf("Enqueue: %v", err)
		}
	}

	// Let the batch form and its Send block, then cancel mid-flight.
	time.Sleep(50 * time.Millisecond)
	cancel()

	if err := <-done; err != nil {
		t.Fatalf("Start returned error: %v", err)
	}

	for i := range batchMaxRows {
		if got := commits[i].Load(); got != 1 {
			t.Fatalf("job %d commit callback fired %d times, want exactly 1", i, got)
		}
	}
}

// TestWritesTotalFailedIncrements asserts that a job whose write can never
// succeed (a permanently-erroring conn) settles as exactly one
// writes_total{outcome="failed"}.
func TestWritesTotalFailedIncrements(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := controller.NewPipelineMetrics(reg)

	// Tiny per-attempt timeout and retry cap so the job exhausts retries and
	// settles quickly; small batchMaxWait so the lone job flushes on the timer.
	w := NewCHWriter(erroringConn{}, 10, 1, 10, 5*time.Millisecond, 20*time.Millisecond, time.Second, 20*time.Millisecond, time.Second, m)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- w.Start(ctx) }()

	committed := make(chan bool, 1)
	if err := w.Enqueue(ctx, sink.Job{
		Record: sink.Record{},
		Commit: func(ok bool) { committed <- ok },
	}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	select {
	case ok := <-committed:
		if ok {
			t.Fatalf("expected the write to be reported as failed, got ok=true")
		}
	case <-time.After(10 * time.Second):
		t.Fatalf("timed out waiting for the write to settle")
	}

	// The failed counter is incremented before commit fires, so it is already
	// settled by the time we read it above.
	if v := writesTotalValue(t, reg, "failed"); v != 1 {
		t.Fatalf("writes_total{outcome=\"failed\"} = %v, want 1", v)
	}
	if v := writesTotalValue(t, reg, "success"); v != 0 {
		t.Fatalf("writes_total{outcome=\"success\"} = %v, want 0", v)
	}

	cancel()
	if err := <-done; err != nil {
		t.Fatalf("Start returned error: %v", err)
	}
}

// TestInsertArgsTimestampFrozen supports Fix 2 (Task 0.9): it proves that
// rendering the same sink.Record to insert args twice produces byte-identical
// positional args, including the timestamp. Record.Timestamp is stamped once at
// reconcile time and never re-stamped on a retry / re-Exec, which is exactly the
// property ReplacingMergeTree relies on — a re-inserted row is byte-identical
// and therefore collapses on merge.
func TestInsertArgsTimestampFrozen(t *testing.T) {
	rec := sink.Record{
		Timestamp:       time.Date(2026, 7, 23, 10, 30, 45, 123456789, time.UTC),
		ClusterID:       "c1",
		EventType:       "Modified",
		APIGroup:        "apps",
		APIVersion:      "v1",
		Kind:            "Deployment",
		Namespace:       "default",
		Name:            "frozen",
		UID:             "uid-frozen",
		ResourceVersion: "42",
		Labels:          map[string]string{"app": "demo"},
		Actors:          []string{"kubectl"},
		Data:            "",
		Diff:            `[{"op":"replace","path":"/x","value":1}]`,
		SHA256:          "deadbeef",
	}

	first := insertArgs(rec)
	second := insertArgs(rec)

	if !reflect.DeepEqual(first, second) {
		t.Fatalf("insertArgs is not deterministic:\n first  = %v\n second = %v", first, second)
	}

	// The timestamp is positional arg 0; assert it round-trips to the exact
	// frozen value both times (a re-stamp would change it between calls).
	wantTS := rec.Timestamp.UTC().Format(chTimeFormat)
	if first[0] != wantTS || second[0] != wantTS {
		t.Fatalf("timestamp arg = (%v, %v), want %q both times", first[0], second[0], wantTS)
	}
}

// TestIsolationPhaseBoundedOnHungBackend is the Fix 3 (M3) guard against a
// wedged worker (Task 0.9). The backend is "hung": each row's Exec blocks until
// its own context is cancelled (it never succeeds or errors on its own), and the
// batch Send always fails so the whole batch falls through to per-row isolation.
// With maxIsolationPhase set well below len(batch)×insertTimeout, the isolation
// phase must return at roughly the phase budget — NOT insertTimeout×batchMaxRows,
// which is the unbounded behavior this fix removes. Every job must still commit
// exactly once, with outcome false. Run under -race.
func TestIsolationPhaseBoundedOnHungBackend(t *testing.T) {
	const batchMaxRows = 10
	const insertTimeout = 2 * time.Second
	const maxIsolationPhase = 500 * time.Millisecond

	conn := &fakeConn{
		// Batch always fails → forces the isolation path.
		sendErr: func(context.Context, [][]any) error { return errors.New("batch rejected") },
		// Hung backend: block until the row context is cancelled, then surface it.
		execErr: func(ctx context.Context, _ []any) error {
			<-ctx.Done()
			return ctx.Err()
		},
	}

	reg := prometheus.NewRegistry()
	m := controller.NewPipelineMetrics(reg)
	// One worker so all rows land in one batch; tiny retry cap so the batch phase
	// exhausts fast and the isolation phase dominates; large batchMaxWait so only
	// the row-count flush fires.
	w := NewCHWriter(conn, 100, 1, batchMaxRows, insertTimeout, 20*time.Millisecond, time.Second, 30*time.Second, time.Second, m)
	w.maxIsolationPhase = maxIsolationPhase // same-package override; not a NewCHWriter param.

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- w.Start(ctx) }()

	log := newCommitLog()
	start := time.Now()
	for i := range batchMaxRows {
		enqueueNamed(t, w, ctx, log, "h"+strconv.Itoa(i))
	}

	waitForCommits(t, log, batchMaxRows, 10*time.Second)
	elapsed := time.Since(start)

	// The unbounded behavior would be insertTimeout×batchMaxRows = 20s. A bound
	// far below that (and far above the phase budget to tolerate CI jitter)
	// proves the phase cap fired rather than every hung row burning insertTimeout.
	const unbounded = insertTimeout * batchMaxRows
	if elapsed >= unbounded/4 {
		t.Fatalf("isolation phase took %s; expected roughly the phase budget (%s), not near %s",
			elapsed, maxIsolationPhase, unbounded)
	}

	total, trues, falses := log.counts()
	if total != batchMaxRows || trues != 0 || falses != batchMaxRows {
		t.Fatalf("commits: total=%d trues=%d falses=%d, want %d/0/%d", total, trues, falses, batchMaxRows, batchMaxRows)
	}
	if n := log.maxPerName(); n != 1 {
		t.Fatalf("a job committed %d times, want exactly 1", n)
	}
	if v := writesTotalValue(t, reg, "failed"); v != batchMaxRows {
		t.Fatalf("writes_total{failed} = %v, want %d", v, batchMaxRows)
	}

	cancel()
	if err := <-done; err != nil {
		t.Fatalf("Start returned error: %v", err)
	}
}

// TestIsolationPhaseDoesNotTruncateSlowBackend is the Fix 3 non-starvation guard
// (Task 0.9). Each isolation Exec succeeds after a delay comfortably under
// insertTimeout but non-trivial, so the cumulative loop time is a meaningful
// fraction of the phase budget; the batch Send always fails to force isolation.
// Every row must settle commit(true) — none cancelled by the phase bound — which
// proves the bound is sized never to convert mere slowness into a manufactured
// failure. That non-truncation property is what distinguishes a correct Fix 3
// from a naive whole-loop timeout. Run under -race.
func TestIsolationPhaseDoesNotTruncateSlowBackend(t *testing.T) {
	const batchMaxRows = 20
	const insertTimeout = 1 * time.Second
	const rowDelay = 50 * time.Millisecond // comfortably under insertTimeout
	const maxIsolationPhase = 4 * time.Second

	conn := &fakeConn{
		sendErr: func(context.Context, [][]any) error { return errors.New("batch rejected") },
		// Slow but alive: each row returns success after rowDelay.
		execErr: func(context.Context, []any) error {
			time.Sleep(rowDelay)
			return nil
		},
	}

	reg := prometheus.NewRegistry()
	m := controller.NewPipelineMetrics(reg)
	w := NewCHWriter(conn, 100, 1, batchMaxRows, insertTimeout, 20*time.Millisecond, time.Second, 30*time.Second, time.Second, m)
	w.maxIsolationPhase = maxIsolationPhase

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- w.Start(ctx) }()

	log := newCommitLog()
	for i := range batchMaxRows {
		enqueueNamed(t, w, ctx, log, "s"+strconv.Itoa(i))
	}

	waitForCommits(t, log, batchMaxRows, 30*time.Second)

	total, trues, falses := log.counts()
	if total != batchMaxRows || trues != batchMaxRows || falses != 0 {
		t.Fatalf("commits: total=%d trues=%d falses=%d, want %d/%d/0", total, trues, falses, batchMaxRows, batchMaxRows)
	}
	if n := log.maxPerName(); n != 1 {
		t.Fatalf("a job committed %d times, want exactly 1", n)
	}
	if v := writesTotalValue(t, reg, "success"); v != batchMaxRows {
		t.Fatalf("writes_total{success} = %v, want %d", v, batchMaxRows)
	}

	cancel()
	if err := <-done; err != nil {
		t.Fatalf("Start returned error: %v", err)
	}
}

// writesTotalValue gathers reg and returns the value of
// kubestream_writes_total{outcome=outcome}, or fails the test if absent.
func writesTotalValue(t *testing.T, reg prometheus.Gatherer, outcome string) float64 {
	t.Helper()
	const metric, label = "kubestream_writes_total", "outcome"
	families, err := reg.Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}
	for _, mf := range families {
		if mf.GetName() != metric {
			continue
		}
		for _, mtc := range mf.GetMetric() {
			for _, lp := range mtc.GetLabel() {
				if lp.GetName() == label && lp.GetValue() == outcome {
					return mtc.GetCounter().GetValue()
				}
			}
		}
	}
	t.Fatalf("counter %s{%s=%q} not found", metric, label, outcome)
	return 0
}

// TestLastKnownStatesQueryScoping proves the warm-up query is GVK-scoped by
// default and namespace-scoped only when ScopeFilter.Namespace is set — the
// behavior-preserving extraction of restoreAndWarm's original inline query.
func TestLastKnownStatesQueryScoping(t *testing.T) {
	t.Run("no namespace matches every namespace", func(t *testing.T) {
		q, args := lastKnownStatesQuery(sink.ScopeFilter{
			ClusterID: "c1", APIGroup: "apps", Kind: "Deployment",
		})
		if strings.Contains(q, "namespace = ?") {
			t.Errorf("expected no namespace predicate, got query:\n%s", q)
		}
		if len(args) != 3 {
			t.Fatalf("expected 3 args (api_group, kind, cluster_id), got %d: %v", len(args), args)
		}
		if args[0] != "apps" || args[1] != "Deployment" || args[2] != "c1" {
			t.Errorf("args = %v, want [apps Deployment c1]", args)
		}
	})

	t.Run("namespace narrows the scope", func(t *testing.T) {
		q, args := lastKnownStatesQuery(sink.ScopeFilter{
			ClusterID: "c1", APIGroup: "apps", Kind: "Deployment", Namespace: "team-a",
		})
		if !strings.Contains(q, "namespace = ?") {
			t.Errorf("expected a namespace predicate, got query:\n%s", q)
		}
		if len(args) != 4 || args[3] != "team-a" {
			t.Fatalf("expected 4 args ending in the namespace, got %v", args)
		}
	})
}
