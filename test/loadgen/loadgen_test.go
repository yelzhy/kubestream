//go:build integration

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

// Package loadgen is kubestream's synthetic-churn load harness (Task 0.8). It
// drives realistic object churn (create N objects, then sustain M updates/sec
// with a configurable payload size and delete ratio) through the *real*
// pipeline — envtest apiserver -> ResourceStreamReconciler -> CHWriter -> a
// dockerized ClickHouse — and reports the four figures Phase 0's throughput
// claims rest on: sustained records/sec, p50/p99 enqueue-block, peak
// write_queue_depth, and process RSS.
//
// It is guarded by the `integration` build tag and run via `make bench-load`,
// which stands up the ClickHouse container and points CH_TEST_ADDR at it,
// exactly like `make test-integration`. Phase 2 (Task 2.3) reuses this harness
// with a larger "massive" profile to re-validate the SLO.
package loadgen

import (
	"context"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"runtime"
	"sort"
	"sync"
	"syscall"
	"testing"
	"time"

	chdriver "github.com/ClickHouse/clickhouse-go/v2"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	"github.com/yelzhy/kubestream/internal/controller"
	"github.com/yelzhy/kubestream/internal/sink/clickhouse"
)

// Harness parameters are flags on the harness itself (Task 0.8 AC). The
// defaults describe the small profile `make bench-load` runs; a larger profile
// (e.g. Task 2.3's "massive" run) overrides them on the command line, and each
// has an env twin so the Makefile can set the profile without positional args.
var (
	flagObjects     = flag.Int("objects", envIntOr("LOADGEN_OBJECTS", 50), "number of objects to create before the sustain phase")
	flagRate        = flag.Int("rate", envIntOr("LOADGEN_RATE", 200), "sustained mutations per second during the churn phase")
	flagPayload     = flag.Int("payload-bytes", envIntOr("LOADGEN_PAYLOAD_BYTES", 2048), "approximate payload size per object, in bytes")
	flagDuration    = flag.Duration("duration", envDurationOr("LOADGEN_DURATION", 10*time.Second), "duration of the sustained churn phase")
	flagDeleteRatio = flag.Float64("delete-ratio", envFloatOr("LOADGEN_DELETE_RATIO", 0), "fraction [0,1] of mutations that delete-and-recreate an object")
	flagConcurrency = flag.Int("concurrency", envIntOr("LOADGEN_CONCURRENCY", 16), "number of concurrent churn workers driving mutations")
)

// harnessMetrics implements clickhouse.Metrics. It captures the exact figures
// the report needs — every enqueue-block sample (for true percentiles), the
// peak queue depth, and the settled-write count — directly at the source,
// rather than scraping and interpolating a Prometheus histogram. All methods
// are called concurrently by the CHWriter workers and the reconcile hot path,
// so every field is mutex-guarded.
type harnessMetrics struct {
	mu             sync.Mutex
	enqueueBlocks  []float64
	peakQueueDepth float64
	settledOK      int64
	settledFailed  int64
}

func (m *harnessMetrics) ObserveEnqueueBlock(seconds float64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.enqueueBlocks = append(m.enqueueBlocks, seconds)
}

func (m *harnessMetrics) SetWriteQueueDepth(n float64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if n > m.peakQueueDepth {
		m.peakQueueDepth = n
	}
}

func (m *harnessMetrics) IncWrite(outcome string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if outcome == "success" {
		m.settledOK++
	} else {
		m.settledFailed++
	}
}

// The remaining clickhouse.Metrics methods are not part of the report; they are
// satisfied as no-ops so the harness need not depend on a Prometheus registry.
func (m *harnessMetrics) SetWriteQueueCapacity(float64) {}
func (m *harnessMetrics) IncEnqueueTimeout()            {}
func (m *harnessMetrics) ObserveWriteLatency(float64)   {}
func (m *harnessMetrics) IncWriteRetryAttempt()         {}
func (m *harnessMetrics) ObserveWriteBatchRows(float64) {}

// settled returns the current success and failed write counts.
func (m *harnessMetrics) settled() (ok, failed int64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.settledOK, m.settledFailed
}

// percentiles returns the p50 and p99 of the collected enqueue-block samples.
func (m *harnessMetrics) percentiles() (p50, p99 float64) {
	m.mu.Lock()
	samples := append([]float64(nil), m.enqueueBlocks...)
	m.mu.Unlock()
	if len(samples) == 0 {
		return 0, 0
	}
	sort.Float64s(samples)
	return quantile(samples, 0.50), quantile(samples, 0.99)
}

func (m *harnessMetrics) peakDepth() float64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.peakQueueDepth
}

// quantile returns the q-quantile of an already-sorted slice using the
// nearest-rank method — exact for the modest sample counts this harness
// collects, with no interpolation to reason about.
func quantile(sorted []float64, q float64) float64 {
	if len(sorted) == 0 {
		return 0
	}
	idx := int(q * float64(len(sorted)))
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return sorted[idx]
}

// TestLoadGenChurn is the harness entry point. It is a test (not a standalone
// main) so it can bootstrap envtest exactly as the controller suite does and be
// invoked through the same `go test -tags=integration` path the Makefile
// already uses for integration coverage.
func TestLoadGenChurn(t *testing.T) {
	if *flagRate <= 0 {
		t.Fatalf("-rate must be positive, got %d", *flagRate)
	}
	if *flagDeleteRatio < 0 || *flagDeleteRatio > 1 {
		t.Fatalf("-delete-ratio must be in [0,1], got %v", *flagDeleteRatio)
	}

	logf.SetLogger(zap.New(zap.WriteTo(os.Stderr), zap.UseDevMode(true)))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// --- envtest apiserver ---
	testEnv := &envtest.Environment{}
	cfg, err := testEnv.Start()
	if err != nil {
		t.Fatalf("start envtest (is KUBEBUILDER_ASSETS set? run via `make bench-load`): %v", err)
	}
	defer func() {
		if stopErr := testEnv.Stop(); stopErr != nil {
			t.Logf("envtest stop: %v", stopErr)
		}
	}()

	// --- dockerized ClickHouse (auto-create the schema-v1 DDL on connect) ---
	chCfg := clickhouse.Config{
		Addr:             envOr("CH_TEST_ADDR", "127.0.0.1:9000"),
		Database:         envOr("CH_TEST_DB", "default"),
		Username:         envOr("CH_TEST_USER", "default"),
		Password:         os.Getenv("CH_TEST_PASSWORD"),
		DialTimeout:      5 * time.Second,
		ReadTimeout:      10 * time.Second,
		AutoCreateSchema: true,
	}
	metrics := &harnessMetrics{}
	chWriter, err := clickhouse.Open(chCfg, metrics)
	if err != nil {
		t.Fatalf("open ClickHouse: %v", err)
	}
	// Clean the throwaway tables on the way out so a re-targeted persistent
	// ClickHouse starts fresh; the dockerized default target is discarded anyway.
	defer cleanupClickHouse(chCfg)

	// --- manager + real reconciler over v1/ConfigMap ---
	// ConfigMaps are the churn vehicle: a built-in kind present in envtest whose
	// arbitrary string Data lets the harness dial payload size precisely.
	mgr, err := ctrl.NewManager(cfg, ctrl.Options{
		Scheme:                 scheme.Scheme,
		Metrics:                metricsserver.Options{BindAddress: "0"},
		HealthProbeBindAddress: "0",
	})
	if err != nil {
		t.Fatalf("create manager: %v", err)
	}
	if err := chWriter.RegisterWithManager(mgr); err != nil {
		t.Fatalf("register ClickHouse writer: %v", err)
	}
	if err := (&controller.ResourceStreamReconciler{
		Client: mgr.GetClient(),
		Scheme: mgr.GetScheme(),
	}).SetupWithManager(mgr, chWriter, chWriter, controller.ReconcilerConfig{
		ClusterID:               "loadgen",
		MaxConcurrentReconciles: 8,
		WatchedGVKs:             []schema.GroupVersionKind{{Version: "v1", Kind: "ConfigMap"}},
	}); err != nil {
		t.Fatalf("set up reconciler: %v", err)
	}

	mgrDone := make(chan error, 1)
	go func() { mgrDone <- mgr.Start(ctx) }()
	if !mgr.GetCache().WaitForCacheSync(ctx) {
		t.Fatal("cache did not sync")
	}

	// A direct (non-cached) client for the churn writes, so mutations hit the
	// apiserver immediately rather than being served from the manager's cache.
	writeClient, err := client.New(cfg, client.Options{Scheme: scheme.Scheme})
	if err != nil {
		t.Fatalf("create write client: %v", err)
	}

	const namespace = "default"
	t.Logf("load profile: objects=%d rate=%d/s payload=%dB duration=%s delete-ratio=%.2f concurrency=%d",
		*flagObjects, *flagRate, *flagPayload, *flagDuration, *flagDeleteRatio, *flagConcurrency)

	// --- create phase ---
	names := make([]string, *flagObjects)
	for i := range names {
		names[i] = fmt.Sprintf("loadgen-%04d", i)
		if err := writeClient.Create(ctx, newConfigMap(namespace, names[i], 0, *flagPayload)); err != nil {
			t.Fatalf("create ConfigMap %s: %v", names[i], err)
		}
	}

	// --- sustained churn phase ---
	okStart, _ := metrics.settled()
	start := time.Now()
	runChurn(ctx, t, writeClient, namespace, names, churnParams{
		rate:        *flagRate,
		duration:    *flagDuration,
		deleteRatio: *flagDeleteRatio,
		payload:     *flagPayload,
		concurrency: *flagConcurrency,
	})
	churnElapsed := time.Since(start)

	// --- drain: let the async pipeline settle the writes it already accepted so
	// the throughput figure reflects records actually persisted, not just
	// enqueued. Poll until the settled count stops moving (or a safety cap).
	drainUntilQuiescent(metrics, 30*time.Second)

	okEnd, failedEnd := metrics.settled()
	settled := okEnd - okStart
	p50, p99 := metrics.percentiles()
	rss := peakRSSBytes()

	recordsPerSec := float64(settled) / churnElapsed.Seconds()

	// Report to stdout so `make bench-load` surfaces it directly.
	fmt.Printf("\n===== kubestream load harness result =====\n")
	fmt.Printf("sustained records/sec : %.0f  (%d records settled over %.1fs churn)\n", recordsPerSec, settled, churnElapsed.Seconds())
	fmt.Printf("enqueue-block p50     : %.3f ms\n", p50*1000)
	fmt.Printf("enqueue-block p99     : %.3f ms\n", p99*1000)
	fmt.Printf("peak write_queue_depth: %.0f\n", metrics.peakDepth())
	fmt.Printf("process RSS           : %.1f MiB\n", float64(rss)/(1024*1024))
	if failedEnd > 0 {
		fmt.Printf("WARNING: %d writes settled as failed\n", failedEnd)
	}
	fmt.Printf("==========================================\n\n")

	// Shut the manager down cleanly (drains the writer, closes the connection).
	cancel()
	select {
	case err := <-mgrDone:
		if err != nil {
			t.Logf("manager stopped with: %v", err)
		}
	case <-time.After(30 * time.Second):
		t.Log("manager did not stop within 30s")
	}

	if settled == 0 {
		t.Fatal("no records settled — the pipeline produced nothing; check ClickHouse connectivity")
	}
}

// churnParams bundles the sustained-churn knobs so runChurn's signature stays
// readable as the harness grows.
type churnParams struct {
	rate        int
	duration    time.Duration
	deleteRatio float64
	payload     int
	concurrency int
}

// runChurn sustains p.rate mutations per second for p.duration. A single ticker
// paces the target rate and dispatches each mutation to a pool of p.concurrency
// workers, so the many small apiserver round-trips overlap rather than
// serializing — without that, a single-threaded generator caps out well below
// the write path's real ceiling (the write path is what this harness exists to
// measure). Object names are partitioned across workers so no two workers ever
// mutate the same object concurrently, avoiding spurious 409 conflicts that
// would otherwise depress the achieved rate for reasons unrelated to the sink.
//
// If every worker is busy when a tick fires, that tick is dropped: the achieved
// records/sec then honestly reflects the ceiling of this environment (here,
// envtest's apiserver), which is exactly the signal Task 2.3 re-validates
// against a real apiserver.
func runChurn(ctx context.Context, t *testing.T, c client.Client, namespace string, names []string, p churnParams) {
	t.Helper()
	workers := p.concurrency
	if workers < 1 {
		workers = 1
	}

	// jobs carries a monotonically-increasing revision so every mutation changes
	// its object's content hash (a Modified event, never a dedup skip).
	jobs := make(chan int64, workers)
	var wg sync.WaitGroup
	for w := range workers {
		// Partition the object pool: worker w owns names[w], names[w+workers], …
		var owned []string
		for i := w; i < len(names); i += workers {
			owned = append(owned, names[i])
		}
		if len(owned) == 0 {
			continue
		}
		wg.Go(func() {
			churnWorker(ctx, t, c, namespace, owned, p.deleteRatio, p.payload, int64(w), jobs)
		})
	}

	interval := time.Second / time.Duration(p.rate)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	deadline := time.After(p.duration)

	var rev int64 = 1
dispatch:
	for {
		select {
		case <-ctx.Done():
			break dispatch
		case <-deadline:
			break dispatch
		case <-ticker.C:
			select {
			case jobs <- rev:
				rev++
			default:
				// All workers busy — this tick is dropped (see doc comment).
			}
		}
	}
	close(jobs)
	wg.Wait()
}

// churnWorker performs one mutation per revision received on jobs, against its
// own partition of the object pool (so concurrent workers never collide on a
// name). A deleteRatio fraction of mutations delete-and-recreate the object
// (exercising the deletion + reincarnation paths); the rest are updates. The
// rng is seeded per worker so a run is reproducible.
func churnWorker(ctx context.Context, t *testing.T, c client.Client, namespace string, owned []string,
	deleteRatio float64, payload int, seed int64, jobs <-chan int64) {
	rng := rand.New(rand.NewSource(seed + 1))
	for rev := range jobs {
		name := owned[rng.Intn(len(owned))]
		if deleteRatio > 0 && rng.Float64() < deleteRatio {
			// Delete then recreate under the same name (fresh UID): a Deleted
			// record plus a subsequent Added for the reincarnation.
			cm := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: name}}
			if err := c.Delete(ctx, cm); err != nil {
				t.Logf("churn delete %s: %v", name, err)
				continue
			}
			if err := c.Create(ctx, newConfigMap(namespace, name, int(rev), payload)); err != nil {
				t.Logf("churn recreate %s: %v", name, err)
			}
			continue
		}
		// Ordinary update: fetch, bump the revision, persist.
		cm := &corev1.ConfigMap{}
		if err := c.Get(ctx, client.ObjectKey{Namespace: namespace, Name: name}, cm); err != nil {
			t.Logf("churn get %s: %v", name, err)
			continue
		}
		cm.Data["rev"] = fmt.Sprintf("%d", rev)
		if err := c.Update(ctx, cm); err != nil {
			t.Logf("churn update %s: %v", name, err)
		}
	}
}

// drainUntilQuiescent waits until the settled-write count has been stable for a
// short window, or until timeout elapses, so throughput accounting includes the
// writes the async pipeline had already accepted when churn stopped.
func drainUntilQuiescent(m *harnessMetrics, timeout time.Duration) {
	deadline := time.Now().Add(timeout)
	prevOK, _ := m.settled()
	stableFor := time.Duration(0)
	const stableTarget = 2 * time.Second
	for time.Now().Before(deadline) {
		time.Sleep(250 * time.Millisecond)
		ok, _ := m.settled()
		if ok == prevOK {
			stableFor += 250 * time.Millisecond
			if stableFor >= stableTarget {
				return
			}
			continue
		}
		prevOK = ok
		stableFor = 0
	}
}

// newConfigMap builds a ConfigMap whose Data holds roughly payload bytes of
// filler plus a revision marker, so successive updates change the content hash.
func newConfigMap(namespace, name string, rev, payload int) *corev1.ConfigMap {
	filler := make([]byte, payload)
	for i := range filler {
		filler[i] = byte('a' + (i % 26))
	}
	return &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: name},
		Data: map[string]string{
			"payload": string(filler),
			"rev":     fmt.Sprintf("%d", rev),
		},
	}
}

// cleanupClickHouse drops the throwaway schema-v1 tables. It opens its own
// short-lived connection because the harness's CHWriter connection is closed by
// the manager's own shutdown sequence.
func cleanupClickHouse(cfg clickhouse.Config) {
	conn, err := chdriver.Open(&chdriver.Options{
		Addr:        []string{cfg.Addr},
		Auth:        chdriver.Auth{Database: cfg.Database, Username: cfg.Username, Password: cfg.Password},
		Protocol:    chdriver.Native,
		DialTimeout: 5 * time.Second,
		ReadTimeout: 10 * time.Second,
	})
	if err != nil {
		return
	}
	defer func() { _ = conn.Close() }()
	dropCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = conn.Exec(dropCtx, "DROP TABLE IF EXISTS resource_states")
	_ = conn.Exec(dropCtx, "DROP TABLE IF EXISTS watch_scopes")
}

// peakRSSBytes returns the process's peak resident set size in bytes via
// getrusage(RUSAGE_SELF). ru_maxrss is reported in bytes on darwin and in
// kilobytes on linux, so the unit is normalized by GOOS.
func peakRSSBytes() int64 {
	var ru syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &ru); err != nil {
		return 0
	}
	maxrss := int64(ru.Maxrss)
	if runtime.GOOS == "linux" {
		return maxrss * 1024
	}
	return maxrss
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func envIntOr(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		var n int
		if _, err := fmt.Sscanf(v, "%d", &n); err == nil {
			return n
		}
	}
	return def
}

func envFloatOr(key string, def float64) float64 {
	if v := os.Getenv(key); v != "" {
		var f float64
		if _, err := fmt.Sscanf(v, "%g", &f); err == nil {
			return f
		}
	}
	return def
}

func envDurationOr(key string, def time.Duration) time.Duration {
	if v := os.Getenv(key); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return def
}
