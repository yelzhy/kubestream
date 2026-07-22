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
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

// TestPipelineMetricsRegistration builds an isolated PipelineMetrics on a fresh
// registry (proving repeated setups never collide on the global one) and
// asserts every metric the acceptance criteria name is present and typed as
// specified.
func TestPipelineMetricsRegistration(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := NewPipelineMetrics(reg)

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
