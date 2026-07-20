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
	"errors"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

// TestPipelineMetricsRegistration builds an isolated pipelineMetrics on a fresh
// registry (proving repeated setups never collide on the global one) and
// asserts every metric the acceptance criteria name is present and typed as
// specified.
func TestPipelineMetricsRegistration(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := newPipelineMetrics(reg)

	// The kind-labelled gauges only materialize a series once a label value is
	// used; touch one apiece so they appear in Gather like the others. The
	// writes_total series are already seeded by the constructor.
	m.hashcacheEntries.WithLabelValues("Pod")
	m.safeMode.WithLabelValues("Pod")

	families, err := reg.Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}

	got := make(map[string]dto.MetricType, len(families))
	for _, mf := range families {
		got[mf.GetName()] = mf.GetType()
	}

	want := map[string]dto.MetricType{
		"kubestream_write_queue_depth":          dto.MetricType_GAUGE,
		"kubestream_write_queue_capacity":       dto.MetricType_GAUGE,
		"kubestream_writes_total":               dto.MetricType_COUNTER,
		"kubestream_write_latency_seconds":      dto.MetricType_HISTOGRAM,
		"kubestream_write_retry_attempts_total": dto.MetricType_COUNTER,
		"kubestream_enqueue_block_seconds":      dto.MetricType_HISTOGRAM,
		"kubestream_enqueue_timeouts_total":     dto.MetricType_COUNTER,
		"kubestream_dedup_skips_total":          dto.MetricType_COUNTER,
		"kubestream_hashcache_entries":          dto.MetricType_GAUGE,
		"kubestream_safe_mode":                  dto.MetricType_GAUGE,
		"kubestream_requeue_drops_total":        dto.MetricType_COUNTER,
	}

	for name, wantType := range want {
		gotType, ok := got[name]
		if !ok {
			t.Errorf("metric %q not registered", name)
			continue
		}
		if gotType != wantType {
			t.Errorf("metric %q has type %s, want %s", name, gotType, wantType)
		}
	}
}

// erroringConn is a driver.Conn whose Exec always fails, used to prove the
// failure-outcome accounting. Embedding the interface satisfies the full
// method set; only Exec and Close are ever exercised here.
type erroringConn struct {
	driver.Conn
}

func (erroringConn) Exec(context.Context, string, ...any) error {
	return errors.New("clickhouse unavailable")
}

func (erroringConn) Close() error { return nil }

// TestWritesTotalFailedIncrements asserts that a job whose write can never
// succeed (a permanently-erroring conn) settles as exactly one
// writes_total{outcome="failed"}.
func TestWritesTotalFailedIncrements(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := newPipelineMetrics(reg)

	// Tiny per-attempt timeout and retry cap so the job exhausts retries and
	// settles quickly rather than blocking the test on real backoff.
	w := NewCHWriter(erroringConn{}, 10, 1, 5*time.Millisecond, 20*time.Millisecond, time.Second)
	w.metrics = m

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- w.Start(ctx) }()

	committed := make(chan bool, 1)
	if err := w.Enqueue(ctx, time.Second, writeJob{
		query:  "INSERT",
		commit: func(ok bool) { committed <- ok },
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
	if v := counterValue(t, reg, "kubestream_writes_total", "outcome", "failed"); v != 1 {
		t.Fatalf("writes_total{outcome=\"failed\"} = %v, want 1", v)
	}
	if v := counterValue(t, reg, "kubestream_writes_total", "outcome", "success"); v != 0 {
		t.Fatalf("writes_total{outcome=\"success\"} = %v, want 0", v)
	}

	cancel()
	if err := <-done; err != nil {
		t.Fatalf("Start returned error: %v", err)
	}
}

// counterValue gathers reg and returns the value of the counter named metric
// whose label labelName equals labelValue, or fails the test if absent.
func counterValue(t *testing.T, reg prometheus.Gatherer, metric, labelName, labelValue string) float64 {
	t.Helper()
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
				if lp.GetName() == labelName && lp.GetValue() == labelValue {
					return mtc.GetCounter().GetValue()
				}
			}
		}
	}
	t.Fatalf("counter %s{%s=%q} not found", metric, labelName, labelValue)
	return 0
}
