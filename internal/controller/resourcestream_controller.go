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
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	ctrlcontroller "sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/source"

	"github.com/cenkalti/backoff/v4"
	"github.com/go-logr/logr"
	"github.com/wI2L/jsondiff"

	"github.com/yelzhy/kubestream/internal/sink"
)

// errAsyncWriteFailed is logged when a sink write exhausts its retries.
// The actual driver error is already logged inside the sink implementation;
// this sentinel just gives Reconcile's log.Error calls a non-nil error value.
var errAsyncWriteFailed = errors.New("clickhouse write did not succeed after retries")

type ResourceStreamReconciler struct {
	client.Client
	Scheme *runtime.Scheme
	// Writer is the sink this reconciler hands records off to. It is the
	// backend-agnostic contract (internal/sink), so the reconciler never
	// depends on ClickHouse directly.
	Writer    sink.Writer
	HashCache hashCache
	GVK       schema.GroupVersionKind
	// ClusterID identifies this operator's cluster in every ClickHouse row
	// it writes; it comes from ReconcilerConfig via SetupWithManager.
	ClusterID string
	// SafeMode is true until this GVK's HashCache has been successfully
	// warmed from ClickHouse history (see restoreAndWarm). While true, a
	// cache-miss can't be trusted to mean "genuinely new" — it might just
	// mean "not loaded yet" — so Reconcile tags such events "Snapshot"
	// instead of "Added" to avoid a mass-duplicate-write storm if
	// ClickHouse is slow or unavailable at startup.
	SafeMode atomic.Bool
	// requeueCh lets a terminally-failed async write (see requeue) trigger a
	// fresh Reconcile for the object it belongs to, instead of the write
	// simply vanishing until something else happens to touch that object.
	// Wired into the controller via WatchesRawSource(source.Channel(...)) in
	// SetupWithManager.
	requeueCh chan event.GenericEvent
	// closeOuts tracks reincarnation close-out writes (see the UID-mismatch
	// branch in Reconcile) that failed and are awaiting retry. It's separate
	// from HashCache because HashCache's entry for a key is immediately
	// overwritten with the new incarnation's live state by Reserve — there is
	// nowhere in HashCache to durably remember "the old UID's close-out still
	// needs to happen" without a newer write clobbering it.
	closeOuts closeOutRetryQueue

	// metrics records dedup short-circuits, hashCache size, per-kind SafeMode,
	// and dropped requeue triggers. Never nil once set in SetupWithManager.
	metrics *PipelineMetrics
}

// closeOutRetryQueue is a mutex-protected map from cacheKey to the
// sink.Records still awaiting a successful close-out write for that key.
// A slice, not a single record, because a second reincarnation could in
// principle occur for the same name before the first close-out resolves;
// this way that (rare) case queues up rather than one write silently
// replacing tracking of the other.
type closeOutRetryQueue struct {
	mu   sync.Mutex
	data map[string][]sink.Record
}

// Add appends record to key's pending list, to be retried on a later call to
// TakeAll for the same key.
func (q *closeOutRetryQueue) Add(key string, record sink.Record) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.data == nil {
		q.data = make(map[string][]sink.Record)
	}
	q.data[key] = append(q.data[key], record)
}

// TakeAll returns and clears key's pending records, if any.
func (q *closeOutRetryQueue) TakeAll(key string) []sink.Record {
	q.mu.Lock()
	defer q.mu.Unlock()
	records := q.data[key]
	delete(q.data, key)
	return records
}

// requeue asks controller-runtime to re-Reconcile namespace/name for this
// GVK. Used when an async write is abandoned after exhausting CHWriter's
// retries, so the object gets a fresh attempt instead of waiting on an
// unrelated future change or the informer's periodic resync to notice the
// cache was reverted. Non-blocking: if the channel is unexpectedly full, the
// trigger is dropped and loudly logged rather than blocking a CHWriter
// worker — the revert this follows has already made the cache consistent,
// so a dropped trigger only delays the retry, it doesn't lose data.
func (r *ResourceStreamReconciler) requeue(namespace, name string) {
	obj := &unstructured.Unstructured{}
	obj.SetGroupVersionKind(r.GVK)
	obj.SetNamespace(namespace)
	obj.SetName(name)

	select {
	case r.requeueCh <- event.GenericEvent{Object: obj}:
	default:
		r.metrics.requeueDrops.Inc()
		logf.Log.WithName("chwriter").Info("requeue channel full, dropping re-reconcile trigger", "kind", r.GVK.Kind, "namespace", namespace, "name", name)
	}
}

// recordHashCacheEntries publishes the current per-kind hashCache size to the
// hashcache_entries gauge. Len() takes and releases the cache's own lock, and
// the gauge Set happens here, strictly outside it — no metric call ever runs
// while a hashCache lock is held (a Task 0.1 acceptance criterion).
func (r *ResourceStreamReconciler) recordHashCacheEntries() {
	r.metrics.hashcacheEntries.WithLabelValues(r.GVK.Kind).Set(float64(r.HashCache.Len()))
}

// cacheKey builds the hashCache key for a namespace/name pair under this
// reconciler's GVK. It is the single canonical identity-key builder in the
// codebase (Invariant 7); every consumer — Reconcile, emitDelete,
// restoreAndWarm, and the closeOuts reincarnation branch — routes through it,
// so a cache entry written under one code path is always found by the others,
// and no other call site concatenates a key by hand.
//
// Invariant 7 (verbatim): An object's identity is
// (cluster_id, api_group, kind, namespace, name) — version-agnostic (apps/v1
// and a hypothetical apps/v2 Deployment are the same object). Exactly one
// function in the codebase constructs this key. cluster_id is explicit in the
// schema but implicit in-process (one operator instance serves one cluster):
// in-memory cache/queue keys are (api_group, kind, namespace, name) — do not
// thread cluster_id through them.
//
// Hence the key embeds Group and Kind but never Version: batch/v1 Job and a
// CRD example.com/v1 Job are distinct resources and must not share a cache
// entry (the GVK-collision bug this builder fixes), while apps/v1 and apps/v2
// of the same resource must share one. The "|" delimiters keep Group, Kind,
// and the ObjectKey unambiguous; ObjectKey.String() always renders as
// "namespace/name" even when namespace is empty, so namespaced and
// cluster-scoped names key identically.
func (r *ResourceStreamReconciler) cacheKey(namespace, name string) string {
	return r.GVK.Group + "|" + r.GVK.Kind + "|" + (client.ObjectKey{Namespace: namespace, Name: name}).String()
}

// emitDelete is the single place a "Deleted" row is ever enqueued. Both the
// live delete path (Reconcile, on IsNotFound) and the startup GC pass
// (restoreAndWarm's zombie cleanup) detect the same condition — this object
// is gone but the cache still holds its last-known state — and must claim
// through the same hashCache.ReserveDelete so they can never both emit a
// duplicate "Deleted" row for the same disappearance. It also protects
// against plain redelivery: controller-runtime's workqueue guarantees at
// least one more Reconcile for a key that was touched again while the
// current one was processing, and since a Reconcile returns as soon as the
// write is enqueued (long before ClickHouse confirms it), a redelivered
// NotFound-Reconcile can easily run before the first one's commit fires. A
// second call for the same key returns claimed=false and does nothing.
//
// expectedUID lets a caller whose belief that the object is gone comes from a
// stale, point-in-time snapshot (the startup GC pass) assert it still matches
// the cache's current UID before claiming — otherwise a live reincarnation
// that happened after the snapshot was taken would let this delete claim and
// remove a currently-existing object's entry by name alone. Pass "" for the
// live IsNotFound path, which has no independent belief to check and simply
// trusts whatever the cache currently holds.
//
//nolint:logcheck
func (r *ResourceStreamReconciler) emitDelete(ctx context.Context, log logr.Logger, namespace, name, expectedUID string) (claimed bool, err error) {
	objectKey := r.cacheKey(namespace, name)

	entry, version, claimed := r.HashCache.ReserveDelete(objectKey, expectedUID)
	if !claimed {
		return false, nil
	}

	log.Info("🗑️ Object gone, queuing Deleted event for ClickHouse", "kind", r.GVK.Kind, "namespace", namespace, "name", name, "uid", entry.UID)

	// A Deleted row carries empty data, diff, and sha256: event_type alone
	// carries deletion semantics in schema v1 (see docs/SCHEMA.md), replacing
	// the pre-v1 data/sha256 deletion sentinels.
	record := sink.Record{
		Timestamp:  time.Now().UTC(),
		ClusterID:  r.ClusterID,
		EventType:  "Deleted",
		APIGroup:   r.GVK.Group,
		APIVersion: r.GVK.Version,
		Kind:       r.GVK.Kind,
		Namespace:  namespace,
		Name:       name,
		UID:        entry.UID,
	}

	// The cache entry is only removed once the deletion record is durably
	// written (see commit) — never before — so a crash or write failure
	// can't silently drop this object from history. On failure, the claim is
	// released (not the whole entry) so a later attempt can retry; a stale
	// release from a superseded claim (e.g. the object was recreated under a
	// new UID while this write was in flight) is a safe no-op — see
	// UnclaimDelete.
	enqueueErr := r.Writer.Enqueue(ctx, sink.Job{
		Record: record,
		Commit: func(ok bool) {
			if ok {
				r.HashCache.DeleteIfCurrent(objectKey, version)
				r.recordHashCacheEntries()
				return
			}
			log.Error(errAsyncWriteFailed, "🗑️ Deletion write failed, releasing claim so it is retried", "kind", r.GVK.Kind, "namespace", namespace, "name", name)
			r.HashCache.UnclaimDelete(objectKey, version)
			r.requeue(namespace, name)
		},
	})
	if enqueueErr != nil {
		log.Error(enqueueErr, "🗑️ Failed to queue deletion event, releasing claim", "kind", r.GVK.Kind, "namespace", namespace, "name", name)
		r.HashCache.UnclaimDelete(objectKey, version)
		r.requeue(namespace, name)
		return true, enqueueErr
	}
	return true, nil
}

// enqueueCloseOut submits a reincarnation close-out write (a "Deleted" row
// for a UID that's been superseded by a same-named recreate). Unlike
// emitDelete, it has no cache entry of its own to gate or settle — by the
// time this runs, HashCache's entry for objectKey is about to be (or already
// has been) overwritten with the new incarnation's live state. So instead of
// a version-gated commit, a failure (whether the enqueue itself or the write
// after CHWriter's own retries) is remembered in closeOuts and a fresh
// Reconcile is requested; retryPendingCloseOuts re-attempts it on that (or
// any later) Reconcile for this key, so a permanently-failed attempt keeps
// getting retried instead of the historical record silently vanishing.
//
//nolint:logcheck
func (r *ResourceStreamReconciler) enqueueCloseOut(ctx context.Context, log logr.Logger, objectKey string, record sink.Record) {
	enqueueErr := r.Writer.Enqueue(ctx, sink.Job{
		Record: record,
		Commit: func(ok bool) {
			if ok {
				return
			}
			log.Error(errAsyncWriteFailed, "🧟 Failed to close out reincarnated object's history, will retry on next event for this name", "kind", r.GVK.Kind, "namespace", record.Namespace, "name", record.Name, "old_uid", record.UID)
			r.closeOuts.Add(objectKey, record)
			r.requeue(record.Namespace, record.Name)
		},
	})
	if enqueueErr != nil {
		log.Error(enqueueErr, "🧟 Failed to queue reincarnation close-out event, will retry on next event for this name", "kind", r.GVK.Kind, "namespace", record.Namespace, "name", record.Name, "old_uid", record.UID)
		r.closeOuts.Add(objectKey, record)
		r.requeue(record.Namespace, record.Name)
	}
}

// retryPendingCloseOuts re-attempts any reincarnation close-out writes still
// pending for objectKey (see closeOuts). Called unconditionally at the top
// of Reconcile so a previously-failed close-out gets retried on the very
// next event for this name — including the fresh Reconcile that
// enqueueCloseOut's failure path explicitly triggers via requeue — rather
// than only ever being attempted once.
//
//nolint:logcheck
func (r *ResourceStreamReconciler) retryPendingCloseOuts(ctx context.Context, log logr.Logger, objectKey string) {
	for _, record := range r.closeOuts.TakeAll(objectKey) {
		r.enqueueCloseOut(ctx, log, objectKey, record)
	}
}

// The markers below grant exactly what the default WATCHED_GVKS list
// (v1/Pod, apps/v1/Deployment, v1/Service — see cmd/main.go) needs.
// Kubernetes RBAC is a static, server-side resource; it cannot be made
// dynamic from this Go code. If WATCHED_GVKS is overridden to add a
// resource type these markers don't cover (including a CRD), the
// operator's ClusterRole (config/rbac/role.yaml, regenerated from markers
// like these via `make manifests`) must be extended to match, or that
// GVK's watch will fail at startup with a Forbidden error.
// +kubebuilder:rbac:groups="",resources=pods,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=services,verbs=get;list;watch
// +kubebuilder:rbac:groups=apps,resources=deployments,verbs=get;list;watch

func (r *ResourceStreamReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	obj := &unstructured.Unstructured{}
	obj.SetGroupVersionKind(r.GVK)

	objectKey := r.cacheKey(req.Namespace, req.Name)
	r.retryPendingCloseOuts(ctx, log, objectKey)

	err := r.Get(ctx, req.NamespacedName, obj)

	// --- DELETE HANDLING BLOCK (IsNotFound) ---
	if err != nil {
		if apierrors.IsNotFound(err) {
			if _, enqueueErr := r.emitDelete(ctx, log, req.Namespace, req.Name, ""); enqueueErr != nil {
				return ctrl.Result{}, enqueueErr
			}
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	// --- NORMALIZATION BLOCK ---
	originalRV := obj.GetResourceVersion()
	currentUID := string(obj.GetUID())

	labels := obj.GetLabels()
	if labels == nil {
		labels = make(map[string]string)
	}

	// Harvest field-manager names before managedFields is stripped below —
	// this is the only chance to read them, and extractActors only reads, so
	// the normalization + hashing that follows is byte-for-byte unaffected.
	actors := extractActors(obj)

	unstructured.RemoveNestedField(obj.Object, "metadata", "managedFields")
	unstructured.RemoveNestedField(obj.Object, "metadata", "resourceVersion")
	unstructured.RemoveNestedField(obj.Object, "metadata", "generation")

	objJson, err := json.Marshal(obj.Object)
	if err != nil {
		return ctrl.Result{}, err
	}

	hashBytes := sha256.Sum256(objJson)
	hashString := hex.EncodeToString(hashBytes[:])

	var eventType = "Added"
	var diffString = ""
	var dataString = string(objJson)

	// revertEntry is what the cache should fall back to if the write below
	// ultimately fails, so a lost write can never be mistaken for a
	// confirmed one on a subsequent Reconcile. nil means "no prior confirmed
	// state" (delete the key entirely on failure).
	var revertEntry *CacheEntry

	// cacheMiss records whether this Reconcile found no cache entry at all
	// for objectKey — the one case the SafeMode Snapshot fallback below
	// exists to guard, since it's genuinely ambiguous whether "Added" means
	// "truly new" or "not yet warmed from ClickHouse history." A
	// reincarnation (cache hit, UID mismatch) is never ambiguous this way —
	// Reconcile has direct proof (the stored old UID vs. the live new UID)
	// that this is a real, current state transition, so it must always be
	// recorded as "Added," never downgraded to "Snapshot," regardless of
	// SafeMode.
	var cacheMiss bool

	// --- DEDUPLICATION AND REINCARNATION BLOCK ---
	if cachedEntry, exists := r.HashCache.Load(objectKey); exists {
		// 🚨 ANTI-ZOMBIE MAGIC: check the UID!
		if cachedEntry.UID != "" && cachedEntry.UID != currentUID {
			log.Info("🧟 Reincarnation! Old object died during downtime — closing its history and treating the current one as Added", "name", req.Name)

			// Claim the old incarnation's delete atomically via
			// ReserveDelete instead of trusting the cachedEntry.PendingDelete
			// value snapshotted by the Load above — that read and this
			// branch are not atomic with respect to a concurrent claim by
			// the live IsNotFound path or the startup GC pass, both of
			// which claim through this exact same primitive (via
			// emitDelete) for the very same key. Without this, both this
			// branch and a concurrent emitDelete call could each observe
			// "not yet claimed" and independently enqueue their own
			// "Deleted" row for the same old UID.
			if claimedEntry, _, claimed := r.HashCache.ReserveDelete(objectKey, cachedEntry.UID); claimed {
				// Deleted rows carry empty data/diff/sha256 in schema v1 —
				// event_type alone marks the deletion (see docs/SCHEMA.md).
				closeRecord := sink.Record{
					Timestamp:  time.Now().UTC(),
					ClusterID:  r.ClusterID,
					EventType:  "Deleted",
					APIGroup:   r.GVK.Group,
					APIVersion: r.GVK.Version,
					Kind:       r.GVK.Kind,
					Namespace:  req.Namespace,
					Name:       req.Name,
					UID:        claimedEntry.UID,
				}

				// 1. Close out the old object's history — failures are
				// remembered and retried (see enqueueCloseOut), not just
				// logged, so this historical record can't be silently lost.
				// The claim above is immediately superseded by this
				// Reconcile's own Reserve for the new incarnation a few
				// lines below, so ReserveDelete's version-gated commit
				// (DeleteIfCurrent/UnclaimDelete) becomes a safe no-op once
				// that happens — it's enqueueCloseOut's own closeOuts-based
				// retry, not the claim, that carries this write forward on
				// failure.
				r.enqueueCloseOut(ctx, log, objectKey, closeRecord)
			} else {
				log.Info("🧟 Old incarnation's deletion already claimed elsewhere, skipping close-out write", "kind", r.GVK.Kind, "name", req.Name, "old_uid", cachedEntry.UID)
			}

			// 2. The current object is treated as a plain Added (leave eventType = "Added").
			// There is no confirmed prior state for THIS UID, so revertEntry stays nil.
		} else {
			// Ordinary logic (same object)
			if cachedEntry.Hash == hashString {
				r.metrics.dedupSkips.Inc()
				return ctrl.Result{}, nil // Duplicate
			}

			// switchToFullState is the shared fallback for every case where a
			// diff can't be produced (no prior JSON to diff against, or the
			// diff/marshal itself fails) — writing the full current state is
			// always correct on its own, just larger than a diff would be.
			switchToFullState := func() {
				dataString = string(objJson)
				diffString = ""
			}

			eventType = "Modified"
			if cachedEntry.JSON == nil {
				log.Info("🔄 Restored after restart (Full State)", "kind", r.GVK.Kind, "name", req.Name)
				switchToFullState()
			} else if patch, err := jsondiff.CompareJSON(cachedEntry.JSON, objJson); err != nil {
				// Not expected to be reachable today — cachedEntry.JSON and
				// objJson are always the product of a prior successful
				// json.Marshal — but a silently-discarded error here would
				// otherwise write neither a diff nor the full state,
				// corrupting this row's audit value with no log signal.
				log.Error(err, "⚠️ Failed to compute JSON diff, falling back to full state", "kind", r.GVK.Kind, "name", req.Name)
				switchToFullState()
			} else if patchBytes, err := json.Marshal(patch); err != nil {
				log.Error(err, "⚠️ Failed to marshal JSON diff, falling back to full state", "kind", r.GVK.Kind, "name", req.Name)
				switchToFullState()
			} else {
				diffString = string(patchBytes)
				dataString = ""
				log.Info("📝 Change detected (Diff)", "kind", r.GVK.Kind, "name", req.Name)
			}
			entryCopy := cachedEntry
			revertEntry = &entryCopy
		}
	} else {
		log.Info("🌟 New object observed", "kind", r.GVK.Kind, "name", req.Name)
		cacheMiss = true
	}

	// A genuine cache-miss during SafeMode can't be trusted to mean
	// "genuinely new" — the cache may simply not be warmed yet. Tag it
	// Snapshot so a slow/unavailable ClickHouse at startup never
	// masquerades as a mass "Added" duplicate-write storm. This
	// intentionally does not cover the reincarnation branch above, which
	// also reaches this point with eventType == "Added" but is never
	// ambiguous — see cacheMiss's doc comment.
	if cacheMiss && r.SafeMode.Load() {
		eventType = "Snapshot"
		log.Info("🌱 Cache not yet warmed, tagging as Snapshot", "kind", r.GVK.Kind, "name", req.Name)
	}

	// Reserve atomically assigns the next version for this key and stores
	// the pending entry, so a duplicate Reconcile firing before the write is
	// confirmed short-circuits as a no-op instead of enqueuing a second
	// write for identical content. The returned version is threaded into
	// the job below: the eventual commit (running in a CHWriter worker,
	// possibly out of order relative to some other job for this same key)
	// only applies its result via CommitIfCurrent/revertVersion if this is
	// still the latest write issued for the key — otherwise a newer write
	// has already superseded it and is left alone.
	version := r.HashCache.Reserve(objectKey, CacheEntry{
		Hash: hashString,
		JSON: objJson,
		UID:  currentUID,
	})
	// Reserve may have added a brand-new key; refresh the size gauge outside
	// any cache lock (recordHashCacheEntries takes/releases it internally).
	r.recordHashCacheEntries()

	revertVersion := func() {
		if revertEntry != nil {
			r.HashCache.CommitIfCurrent(objectKey, version, *revertEntry)
		} else {
			r.HashCache.DeleteIfCurrent(objectKey, version)
		}
	}

	record := sink.Record{
		Timestamp:       time.Now().UTC(),
		ClusterID:       r.ClusterID,
		EventType:       eventType,
		APIGroup:        r.GVK.Group,
		APIVersion:      r.GVK.Version,
		Kind:            r.GVK.Kind,
		Namespace:       req.Namespace,
		Name:            req.Name,
		UID:             currentUID,
		ResourceVersion: originalRV,
		Labels:          labels,
		Actors:          actors,
		Data:            dataString,
		Diff:            diffString,
		SHA256:          hashString,
	}

	// --- EXPORT BLOCK ---
	enqueueErr := r.Writer.Enqueue(ctx, sink.Job{
		Record: record,
		Commit: func(ok bool) {
			if ok {
				// Only now is the write durably confirmed — settle the
				// pending marker into a confirmed cache entry, unless a
				// newer write has already superseded this one.
				r.HashCache.CommitIfCurrent(objectKey, version, CacheEntry{
					Hash: hashString,
					JSON: objJson,
					UID:  currentUID,
				})
				return
			}
			log.Error(errAsyncWriteFailed, "Write failed after retries, reverting cache and requesting a fresh Reconcile", "kind", r.GVK.Kind, "name", req.Name)
			revertVersion()
			// Unlike a synchronous write failure, returning an error here
			// wouldn't reach controller-runtime — this callback runs well
			// after Reconcile already returned. Trigger a fresh Reconcile
			// explicitly so the write is retried instead of only self-healing
			// on the next unrelated change or informer resync.
			r.requeue(req.Namespace, req.Name)
		},
	})
	if enqueueErr != nil {
		// The job never entered the write pipeline, so no commit will ever
		// fire for it — undo the optimistic marker ourselves.
		revertVersion()
		log.Error(enqueueErr, "Failed to queue write, requeuing", "kind", r.GVK.Kind, "name", req.Name)
		return ctrl.Result{}, enqueueErr
	}

	return ctrl.Result{}, nil
}

func (r *ResourceStreamReconciler) SetupWithManager(mgr ctrl.Manager, writer sink.Writer, reader sink.StateReader, reconcilerConfig ReconcilerConfig) error {
	// gvksToWatch comes from ReconcilerConfig (sourced from WATCHED_GVKS /
	// --watched-gvks in cmd/main.go) rather than a hardcoded slice, so
	// watching a new resource type — including a CRD — is a config change,
	// not a code change + rebuild. See the RBAC caveat on the kubebuilder
	// markers above Reconcile: this does not, and cannot, dynamically grant
	// the RBAC permissions a newly-added GVK needs — that's still a
	// separate, static Kubernetes resource.
	gvksToWatch := reconcilerConfig.WatchedGVKs
	if len(gvksToWatch) == 0 {
		return errors.New("no watched GVKs configured: WatchedGVKs is empty")
	}

	log := logf.Log.WithName("setup")

	for _, gvk := range gvksToWatch {
		// requeueCh lets a terminally-failed write for this GVK trigger a
		// fresh Reconcile (see ResourceStreamReconciler.requeue) without
		// going through the normal informer watch.
		requeueCh := make(chan event.GenericEvent, 100)

		reconciler := &ResourceStreamReconciler{
			Client:    mgr.GetClient(),
			Scheme:    mgr.GetScheme(),
			Writer:    writer,
			GVK:       gvk,
			ClusterID: reconcilerConfig.ClusterID,
			requeueCh: requeueCh,
			metrics:   PipelineMetricsInstance(),
		}
		// The cache starts empty and isn't trustworthy until restoreAndWarm
		// (below) finishes — see the SafeMode field doc and Reconcile.
		reconciler.SafeMode.Store(true)
		reconciler.metrics.safeMode.WithLabelValues(gvk.Kind).Set(1)

		if err := mgr.Add(manager.RunnableFunc(func(ctx context.Context) error {
			restoreAndWarm(ctx, mgr, reader, reconciler, gvk, log)
			return nil
		})); err != nil {
			return err
		}

		obj := &unstructured.Unstructured{}
		obj.SetGroupVersionKind(gvk)

		if err := ctrl.NewControllerManagedBy(mgr).
			For(obj).
			WatchesRawSource(source.Channel(requeueCh, &handler.EnqueueRequestForObject{})).
			Named("stream-" + gvk.Kind).
			WithOptions(ctrlcontroller.Options{MaxConcurrentReconciles: reconcilerConfig.MaxConcurrentReconciles}).
			Complete(reconciler); err != nil {
			return err
		}
	}

	return nil
}

// restoreAndWarm rebuilds a single GVK's HashCache from the sink's history in
// the background, then runs the zombie-object GC pass. Running this as a
// manager.Runnable (rather than inline in SetupWithManager) means mgr.Start()
// is never gated on the sink being reachable at boot.
//
// The restore is retried indefinitely (bounded backoff, no elapsed-time
// cutoff) until it succeeds or ctx is cancelled by manager shutdown. While it
// hasn't yet succeeded, reconciler.SafeMode stays true, so Reconcile tags
// cache-miss events "Snapshot" instead of "Added" — a sink outage at startup
// degrades to that instead of re-emitting every live object in the cluster as
// a duplicate "Added" row.
//
//nolint:logcheck
func restoreAndWarm(ctx context.Context, mgr ctrl.Manager, reader sink.StateReader, reconciler *ResourceStreamReconciler, gvk schema.GroupVersionKind, log logr.Logger) {
	log.Info("🔄 Warming cache from sink history...", "kind", gvk.Kind)

	type gcTarget struct {
		Namespace string
		Name      string
		UID       string
	}
	var targetsToCheck []gcTarget

	warm := func() error {
		targetsToCheck = nil

		// The scope is GVK-wide (empty Namespace): (cluster_id, api_group, kind)
		// is the version-agnostic identity the in-process cacheKey builder keys
		// on, and LastKnownStates mirrors it on the sink side so two resources
		// that share a Kind (e.g. batch/v1 Job vs. a CRD example.com/v1 Job)
		// never cross-contaminate each other's warm-up history. A transient
		// backend error (or a partial read) is returned so backoff.Retry tries
		// again from scratch rather than clearing SafeMode with an under-
		// restored cache.
		states, err := reader.LastKnownStates(ctx, sink.ScopeFilter{
			ClusterID: reconciler.ClusterID,
			APIGroup:  gvk.Group,
			Kind:      gvk.Kind,
		})
		if err != nil {
			return err
		}

		for _, st := range states {
			key := reconciler.cacheKey(st.Namespace, st.Name)

			// StoreIfAbsent, not Store: a live Reconcile may already have
			// reserved a newer entry for this key while SafeMode was still
			// true (tagged "Snapshot") — that live state is authoritative
			// and must not be clobbered by this historical baseline.
			reconciler.HashCache.StoreIfAbsent(key, CacheEntry{
				Hash: st.SHA256,
				JSON: nil,
				UID:  st.UID,
			})
			targetsToCheck = append(targetsToCheck, gcTarget{Namespace: st.Namespace, Name: st.Name, UID: st.UID})
		}
		log.Info("✅ Cache warmed", "kind", gvk.Kind, "objects_loaded", len(states))
		return nil
	}

	eb := backoff.NewExponentialBackOff()
	eb.MaxInterval = 30 * time.Second
	eb.MaxElapsedTime = 0 // retry forever — only ctx cancellation (shutdown) gives up

	err := backoff.Retry(func() error {
		if err := warm(); err != nil {
			log.Error(err, "⚠️ Failed to warm cache from ClickHouse, staying in Snapshot mode and retrying", "kind", gvk.Kind)
			return err
		}
		return nil
	}, backoff.WithContext(eb, ctx))
	if err != nil {
		// Only reachable if ctx was cancelled (manager shutting down) before
		// the restore ever succeeded — SafeMode simply stays on; there is no
		// GC to run since we never got a trustworthy targetsToCheck.
		return
	}

	reconciler.SafeMode.Store(false)
	reconciler.metrics.safeMode.WithLabelValues(gvk.Kind).Set(0)
	log.Info("🔓 Cache warm-up complete, leaving Snapshot mode", "kind", gvk.Kind)

	if len(targetsToCheck) == 0 {
		return
	}

	// ==========================================
	// 🕵️‍♂️ Garbage Collector (with UID verification)
	// ==========================================
	if !mgr.GetCache().WaitForCacheSync(ctx) {
		return
	}

	zombieCount := 0
	for _, target := range targetsToCheck {
		obj := &unstructured.Unstructured{}
		obj.SetGroupVersionKind(gvk)
		reqKey := client.ObjectKey{Namespace: target.Namespace, Name: target.Name}

		getErr := mgr.GetClient().Get(ctx, reqKey, obj)

		// A zombie is either an object that's gone entirely, or one whose
		// current UID no longer matches the UID this GC pass believes it
		// should have (i.e. it was deleted and recreated since the ClickHouse
		// snapshot was taken).
		isZombie := false
		if getErr != nil {
			if apierrors.IsNotFound(getErr) {
				isZombie = true
			} else {
				log.Error(getErr, "🧟 Failed to check whether object still exists, skipping this target", "kind", gvk.Kind, "namespace", target.Namespace, "name", target.Name)
				continue
			}
		} else if string(obj.GetUID()) != target.UID {
			isZombie = true
		}

		if !isZombie {
			continue
		}

		// Routed through the same claim as the live delete path
		// (hashCache.ReserveDelete via emitDelete): if a NotFound-Reconcile
		// already claimed this exact deletion between building
		// targetsToCheck and getting here, claimed comes back false and
		// there is nothing left for the GC pass to do — without this shared
		// claim, both would independently enqueue their own "Deleted" row
		// for the same object. expectedUID=target.UID additionally guards
		// against a live reincarnation that happened in that same window:
		// if a Reconcile already recreated this object under a new UID and
		// Reserve'd the cache accordingly, target.UID no longer matches the
		// live entry, so the claim is refused instead of deleting a
		// currently-existing object by name alone.
		claimed, enqueueErr := reconciler.emitDelete(ctx, log, reqKey.Namespace, reqKey.Name, target.UID)
		if enqueueErr != nil {
			log.Error(enqueueErr, "🧟 Failed to queue zombie cleanup event", "kind", gvk.Kind, "name", reqKey.Name)
			continue
		}
		if !claimed {
			continue
		}

		zombieCount++
		log.Info("🧟 Zombie object detected and cleaned up by GC", "kind", gvk.Kind, "name", reqKey.Name)
	}
	if zombieCount > 0 {
		log.Info("🧹 Garbage collector finished", "kind", gvk.Kind, "zombies_cleared", zombieCount)
	}
}
