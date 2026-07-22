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

// Package sink defines the backend-agnostic contract every kubestream storage
// backend implements. The pipeline (reconcilers, cache warm-up) depends only on
// these interfaces and value types, never on a concrete driver, so a future
// backend (Postgres, Elasticsearch, Kafka) is a new implementation of Writer /
// StateReader rather than a change to the hot path. ClickHouse is the only
// implementation today (see internal/sink/clickhouse).
package sink

import "time"

// Record is the universal structure used to send a row to a sink. It is a pure
// data type: how a Record maps onto a backend's schema (column order, encoding,
// query) is entirely the backend implementation's concern, so this struct
// carries no query or driver detail.
type Record struct {
	Timestamp       time.Time         `json:"timestamp"`
	ClusterID       string            `json:"cluster_id"`
	EventType       string            `json:"event_type"` // Added, Modified, Deleted, Snapshot
	APIGroup        string            `json:"group"`
	APIVersion      string            `json:"version"`
	Kind            string            `json:"kind"`
	Namespace       string            `json:"namespace"`
	Name            string            `json:"name"`
	UID             string            `json:"uid"`
	ResourceVersion string            `json:"resource_version"`
	Labels          map[string]string `json:"labels"`
	// Actors are the distinct, sorted field-manager names harvested from the
	// object's managedFields (see extractActors and the resource_states.actors
	// column) — the cheapest "who probably changed this" signal. Deleted rows
	// carry no actors: there is no live object left to inspect, so a deletion's
	// authorship is intentionally not attributed here.
	Actors []string `json:"actors"`
	Data   string   `json:"data"` // Full JSON (for Added)
	// Diff is an RFC 6902 JSON Patch (wI2L/jsondiff) describing the change on a
	// Modified event; empty on every other event type. Named to match the
	// schema-v1 "diff" column (renamed from the old "diff_data").
	Diff   string `json:"diff"`
	SHA256 string `json:"sha256"`
}
