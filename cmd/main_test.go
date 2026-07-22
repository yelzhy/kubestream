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

package main

import (
	"flag"
	"io"
	"strings"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/runtime/schema"

	"github.com/yelzhy/kubestream/internal/sink/clickhouse"
)

func TestParseGVKListDefaultSet(t *testing.T) {
	gvks, err := parseGVKList(defaultWatchedGVKs)
	if err != nil {
		t.Fatalf("unexpected error parsing the default GVK list: %v", err)
	}
	want := []schema.GroupVersionKind{
		{Group: "", Version: "v1", Kind: "Pod"},
		{Group: "apps", Version: "v1", Kind: "Deployment"},
		{Group: "", Version: "v1", Kind: "Service"},
	}
	if len(gvks) != len(want) {
		t.Fatalf("got %d GVKs, want %d: %+v", len(gvks), len(want), gvks)
	}
	for i, g := range gvks {
		if g != want[i] {
			t.Fatalf("gvks[%d] = %+v, want %+v", i, g, want[i])
		}
	}
}

func TestParseGVKListCoreAndNonCoreGroups(t *testing.T) {
	gvks, err := parseGVKList("v1/Pod, networking.k8s.io/v1/Ingress ,apps/v1/Deployment")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	want := []schema.GroupVersionKind{
		{Group: "", Version: "v1", Kind: "Pod"},
		{Group: "networking.k8s.io", Version: "v1", Kind: "Ingress"},
		{Group: "apps", Version: "v1", Kind: "Deployment"},
	}
	if len(gvks) != len(want) {
		t.Fatalf("got %d GVKs, want %d: %+v", len(gvks), len(want), gvks)
	}
	for i, g := range gvks {
		if g != want[i] {
			t.Fatalf("gvks[%d] = %+v, want %+v", i, g, want[i])
		}
	}
}

func TestParseGVKListSkipsBlankEntries(t *testing.T) {
	gvks, err := parseGVKList(" , v1/Pod ,, ")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(gvks) != 1 || gvks[0] != (schema.GroupVersionKind{Version: "v1", Kind: "Pod"}) {
		t.Fatalf("expected exactly one Pod GVK with blanks skipped, got %+v", gvks)
	}
}

func TestParseGVKListEmptyInputYieldsEmptyList(t *testing.T) {
	gvks, err := parseGVKList("")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(gvks) != 0 {
		t.Fatalf("expected an empty list for empty input, got %+v", gvks)
	}
}

func TestParseGVKListRejectsWrongSegmentCount(t *testing.T) {
	for _, raw := range []string{"Pod", "a/b/c/d", "/"} {
		if _, err := parseGVKList(raw); err == nil {
			t.Fatalf("expected an error for malformed entry %q, got none", raw)
		}
	}
}

func TestParseGVKListRejectsEmptyVersionOrKind(t *testing.T) {
	for _, raw := range []string{"/Pod", "v1/", "apps//Deployment"} {
		if _, err := parseGVKList(raw); err == nil {
			t.Fatalf("expected an error for entry with empty version/kind %q, got none", raw)
		}
	}
}

func TestParseGVKListRejectsDuplicateGroupKind(t *testing.T) {
	// Same (group, kind), differing versions: the identity key is
	// version-agnostic (Invariant 7), so this would watch one resource twice.
	cases := []struct {
		name         string
		raw          string
		wantMentions []string
	}{
		{
			name:         "non-core group",
			raw:          "apps/v1/Deployment,apps/v2/Deployment",
			wantMentions: []string{"apps/v1/Deployment", "apps/v2/Deployment"},
		},
		{
			// A core-group entry ("v1/Job", group "") and a group-qualified
			// entry for the same core kind still collide on (group, kind).
			name:         "core group two-segment vs three-segment",
			raw:          "v1/Endpoints,/v2/Endpoints",
			wantMentions: []string{"v1/Endpoints", "/v2/Endpoints"},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := parseGVKList(tc.raw)
			if err == nil {
				t.Fatalf("expected a duplicate (group, kind) error for %q, got none", tc.raw)
			}
			for _, m := range tc.wantMentions {
				if !strings.Contains(err.Error(), m) {
					t.Fatalf("error %q does not name offending entry %q", err.Error(), m)
				}
			}
		})
	}
}

func TestParseGVKListAllowsSameGroupDifferentKinds(t *testing.T) {
	// Same group, different kinds must still parse: the duplicate check keys on
	// (group, kind), not group alone.
	gvks, err := parseGVKList("apps/v1/Deployment,apps/v1/StatefulSet")
	if err != nil {
		t.Fatalf("unexpected error for distinct kinds in one group: %v", err)
	}
	if len(gvks) != 2 {
		t.Fatalf("expected 2 GVKs, got %+v", gvks)
	}
}

// parseWriterFlags registers the --writer-* flags on a throwaway FlagSet and
// parses args against them, so a test drives the exact same registration path
// main() uses without touching the global flag.CommandLine. Parse output is
// discarded; any parse error is surfaced as a return value.
func parseWriterFlags(t *testing.T, args []string) (*writerTuning, error) {
	t.Helper()
	fs := flag.NewFlagSet("test", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	tuning := registerWriterFlags(fs)
	return tuning, fs.Parse(args)
}

// TestRegisterWriterFlagsDefaults asserts that, with no flags and no env vars,
// every knob resolves to the exported clickhouse.Default* constant — the
// single source of truth the README config table also documents.
func TestRegisterWriterFlagsDefaults(t *testing.T) {
	tuning, err := parseWriterFlags(t, nil)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if tuning.queueSize != clickhouse.DefaultWriteQueueSize {
		t.Errorf("queueSize = %d, want %d", tuning.queueSize, clickhouse.DefaultWriteQueueSize)
	}
	if tuning.workers != clickhouse.DefaultWriteWorkers {
		t.Errorf("workers = %d, want %d", tuning.workers, clickhouse.DefaultWriteWorkers)
	}
	if tuning.batchMaxRows != clickhouse.DefaultBatchMaxRows {
		t.Errorf("batchMaxRows = %d, want %d", tuning.batchMaxRows, clickhouse.DefaultBatchMaxRows)
	}
	if tuning.batchMaxWait != clickhouse.DefaultBatchMaxWait {
		t.Errorf("batchMaxWait = %s, want %s", tuning.batchMaxWait, clickhouse.DefaultBatchMaxWait)
	}
	if tuning.enqueueTimeout != clickhouse.DefaultEnqueueTimeout {
		t.Errorf("enqueueTimeout = %s, want %s", tuning.enqueueTimeout, clickhouse.DefaultEnqueueTimeout)
	}
	if tuning.drainTimeout != clickhouse.DefaultShutdownDrainTimeout {
		t.Errorf("drainTimeout = %s, want %s", tuning.drainTimeout, clickhouse.DefaultShutdownDrainTimeout)
	}
}

// TestRegisterWriterFlagsParseOverrides covers the flag path for all six knobs:
// an explicit --writer-* flag must win over the default.
func TestRegisterWriterFlagsParseOverrides(t *testing.T) {
	tuning, err := parseWriterFlags(t, []string{
		"--writer-queue-size=1234",
		"--writer-workers=9",
		"--writer-batch-max-rows=250",
		"--writer-batch-max-wait=500ms",
		"--writer-enqueue-timeout=3s",
		"--writer-drain-timeout=45s",
	})
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if tuning.queueSize != 1234 {
		t.Errorf("queueSize = %d, want 1234", tuning.queueSize)
	}
	if tuning.workers != 9 {
		t.Errorf("workers = %d, want 9", tuning.workers)
	}
	if tuning.batchMaxRows != 250 {
		t.Errorf("batchMaxRows = %d, want 250", tuning.batchMaxRows)
	}
	if tuning.batchMaxWait != 500*time.Millisecond {
		t.Errorf("batchMaxWait = %s, want 500ms", tuning.batchMaxWait)
	}
	if tuning.enqueueTimeout != 3*time.Second {
		t.Errorf("enqueueTimeout = %s, want 3s", tuning.enqueueTimeout)
	}
	if tuning.drainTimeout != 45*time.Second {
		t.Errorf("drainTimeout = %s, want 45s", tuning.drainTimeout)
	}
}

// TestRegisterWriterFlagsEnvFallback covers the env-twin path: with no flag
// given, each knob picks up its WRITER_* env var. t.Setenv restores the
// environment on cleanup, so these cases don't leak into other tests.
func TestRegisterWriterFlagsEnvFallback(t *testing.T) {
	t.Setenv("WRITER_QUEUE_SIZE", "7000")
	t.Setenv("WRITER_WORKERS", "12")
	t.Setenv("WRITER_BATCH_MAX_ROWS", "2000")
	t.Setenv("WRITER_BATCH_MAX_WAIT", "2s")
	t.Setenv("WRITER_ENQUEUE_TIMEOUT", "750ms")
	t.Setenv("WRITER_DRAIN_TIMEOUT", "30s")

	tuning, err := parseWriterFlags(t, nil)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if tuning.queueSize != 7000 {
		t.Errorf("queueSize = %d, want 7000", tuning.queueSize)
	}
	if tuning.workers != 12 {
		t.Errorf("workers = %d, want 12", tuning.workers)
	}
	if tuning.batchMaxRows != 2000 {
		t.Errorf("batchMaxRows = %d, want 2000", tuning.batchMaxRows)
	}
	if tuning.batchMaxWait != 2*time.Second {
		t.Errorf("batchMaxWait = %s, want 2s", tuning.batchMaxWait)
	}
	if tuning.enqueueTimeout != 750*time.Millisecond {
		t.Errorf("enqueueTimeout = %s, want 750ms", tuning.enqueueTimeout)
	}
	if tuning.drainTimeout != 30*time.Second {
		t.Errorf("drainTimeout = %s, want 30s", tuning.drainTimeout)
	}
}

// TestRegisterWriterFlagsFlagBeatsEnv asserts the documented precedence: when
// both an env var and a flag are set, the flag wins.
func TestRegisterWriterFlagsFlagBeatsEnv(t *testing.T) {
	t.Setenv("WRITER_WORKERS", "12")
	t.Setenv("WRITER_ENQUEUE_TIMEOUT", "750ms")

	tuning, err := parseWriterFlags(t, []string{"--writer-workers=3", "--writer-enqueue-timeout=1s"})
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if tuning.workers != 3 {
		t.Errorf("workers = %d, want 3 (flag must beat env)", tuning.workers)
	}
	if tuning.enqueueTimeout != time.Second {
		t.Errorf("enqueueTimeout = %s, want 1s (flag must beat env)", tuning.enqueueTimeout)
	}
}

// TestRegisterWriterFlagsInvalidEnvFallsBack covers Invariant-5 graceful
// degradation: an unparsable WRITER_* env value must not fail startup — it
// falls back to the shipped default (via getEnvIntOrDefault /
// getEnvDurationOrDefault, which also emit the stderr warning).
func TestRegisterWriterFlagsInvalidEnvFallsBack(t *testing.T) {
	t.Setenv("WRITER_QUEUE_SIZE", "not-a-number")
	t.Setenv("WRITER_WORKERS", "12.5")
	t.Setenv("WRITER_BATCH_MAX_WAIT", "not-a-duration")
	t.Setenv("WRITER_ENQUEUE_TIMEOUT", "10")  // missing unit — invalid duration
	t.Setenv("WRITER_DRAIN_TIMEOUT", "later") // invalid duration

	tuning, err := parseWriterFlags(t, nil)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if tuning.queueSize != clickhouse.DefaultWriteQueueSize {
		t.Errorf("queueSize = %d, want default %d on invalid env", tuning.queueSize, clickhouse.DefaultWriteQueueSize)
	}
	if tuning.workers != clickhouse.DefaultWriteWorkers {
		t.Errorf("workers = %d, want default %d on invalid env", tuning.workers, clickhouse.DefaultWriteWorkers)
	}
	if tuning.batchMaxWait != clickhouse.DefaultBatchMaxWait {
		t.Errorf("batchMaxWait = %s, want default %s on invalid env", tuning.batchMaxWait, clickhouse.DefaultBatchMaxWait)
	}
	if tuning.enqueueTimeout != clickhouse.DefaultEnqueueTimeout {
		t.Errorf("enqueueTimeout = %s, want default %s on invalid env",
			tuning.enqueueTimeout, clickhouse.DefaultEnqueueTimeout)
	}
	if tuning.drainTimeout != clickhouse.DefaultShutdownDrainTimeout {
		t.Errorf("drainTimeout = %s, want default %s on invalid env",
			tuning.drainTimeout, clickhouse.DefaultShutdownDrainTimeout)
	}
}
