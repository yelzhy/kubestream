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

package sink

import "context"

// Job is a single record submitted to a Writer, together with the callback that
// settles its outcome.
//
// commit is invoked by a CHWriter worker exactly once, only after the write
// has been durably confirmed or definitively abandoned after retries — it is
// the sole place cache mutation for that job's object is allowed to happen,
// so a failed write can never be mistaken for a persisted one.
//
// (The wording above is preserved verbatim from the original writeJob contract:
// every Writer implementation must uphold it, whatever its backend.)
type Job struct {
	// Record is the row to persist.
	Record Record
	// Commit reports this job's settled outcome (true = durably written,
	// false = abandoned after retries). See the exactly-once contract above.
	Commit func(ok bool)
}

// Writer is the write half of a sink: a bounded, asynchronous hand-off that
// decouples record persistence from the caller's hot path. ClickHouse is the
// only implementation today (see internal/sink/clickhouse).
type Writer interface {
	// Start runs the Writer's worker pool until ctx is cancelled, then shuts
	// down in a strict order so no write is ever stranded or raced against
	// connection closure:
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
	//
	// It is manager.Runnable-compatible so the manager owns its lifecycle.
	Start(ctx context.Context) error

	// Enqueue submits a write job without blocking the caller on the actual
	// sink round-trip. It is a bounded, metered hand-off: if the queue is full
	// it waits a bounded time for room and then returns an error rather than
	// dropping the job silently or blocking the hot path indefinitely. The
	// returned error should be propagated so the caller's own backpressure
	// (e.g. controller-runtime's requeue/backoff) takes over.
	Enqueue(ctx context.Context, job Job) error
}

// ScopeFilter narrows a StateReader query to a single watch scope. Namespace is
// optional: an empty Namespace matches every namespace (a GVK-wide scope),
// while a non-empty Namespace restricts the result to that namespace. ClusterID
// is explicit here because the sink is a multi-cluster store even though a
// single operator process only ever serves one cluster (Invariant 7).
type ScopeFilter struct {
	ClusterID string
	APIGroup  string
	Kind      string
	Namespace string
}

// KnownState is one object's last-known persisted identity/content, as returned
// by StateReader. It is the minimum a cache warm-up needs to reconstruct its
// in-memory baseline: the identity (Namespace, Name, UID) and the content hash
// (SHA256) used for dedup.
type KnownState struct {
	Namespace string
	Name      string
	UID       string
	SHA256    string
}

// StateReader is the read half of a sink: it reports, per scope, the last-known
// state of every object not currently tombstoned, so a restarting operator can
// warm its dedup cache from durable history rather than re-emitting every live
// object as a duplicate.
//
// StateReader is optional for future sinks: a Writer that cannot read its own
// history back can omit it. Such a Writer-only sink runs with cache warm-up and
// zombie garbage-collection disabled and tags every record as a permanent
// Snapshot (it can never prove an object is genuinely new versus merely
// un-warmed). This is a design note only — no code path exercises a Writer-only
// sink yet.
type StateReader interface {
	// LastKnownStates returns the last-known state of every object matching
	// filter whose most recent event is not a deletion. A transient backend
	// error is returned as-is so the caller can retry; a partial read must be
	// reported as an error, never as a short-but-successful result.
	LastKnownStates(ctx context.Context, filter ScopeFilter) ([]KnownState, error)
}
