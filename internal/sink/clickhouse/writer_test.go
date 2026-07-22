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
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/yelzhy/kubestream/internal/controller"
	"github.com/yelzhy/kubestream/internal/sink"
)

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
	m := controller.NewPipelineMetrics(reg)

	// Tiny per-attempt timeout and retry cap so the job exhausts retries and
	// settles quickly rather than blocking the test on real backoff.
	w := NewCHWriter(erroringConn{}, 10, 1, 5*time.Millisecond, 20*time.Millisecond, time.Second, m)

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
