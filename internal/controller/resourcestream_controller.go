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

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/cenkalti/backoff/v4"
	"github.com/go-logr/logr"
	"github.com/wI2L/jsondiff"
)

// errAsyncWriteFailed is logged when a CHWriter job exhausts its retries.
// The actual driver error is already logged inside CHWriter.process; this
// sentinel just gives Reconcile's log.Error calls a non-nil error value.
var errAsyncWriteFailed = errors.New("clickhouse write did not succeed after retries")

type ResourceStreamReconciler struct {
	client.Client
	Scheme    *runtime.Scheme
	CHConn    driver.Conn
	CHWriter  *CHWriter
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
}

// closeOutRetryQueue is a mutex-protected map from cacheKey to the
// ResourceRecords still awaiting a successful close-out write for that key.
// A slice, not a single record, because a second reincarnation could in
// principle occur for the same name before the first close-out resolves;
// this way that (rare) case queues up rather than one write silently
// replacing tracking of the other.
type closeOutRetryQueue struct {
	mu   sync.Mutex
	data map[string][]ResourceRecord
}

// Add appends record to key's pending list, to be retried on a later call to
// TakeAll for the same key.
func (q *closeOutRetryQueue) Add(key string, record ResourceRecord) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.data == nil {
		q.data = make(map[string][]ResourceRecord)
	}
	q.data[key] = append(q.data[key], record)
}

// TakeAll returns and clears key's pending records, if any.
func (q *closeOutRetryQueue) TakeAll(key string) []ResourceRecord {
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
		logf.Log.WithName("chwriter").Info("requeue channel full, dropping re-reconcile trigger", "kind", r.GVK.Kind, "namespace", namespace, "name", name)
	}
}

// cacheKey builds the hashCache key for a namespace/name pair under this
// reconciler's GVK. The single canonical builder, used by Reconcile,
// emitDelete, and restoreAndWarm, so a cache entry written under one code
// path is always found by the others — namespaced and cluster-scoped names
// alike (client.ObjectKey.String() always renders as "namespace/name", even
// when namespace is empty, which req.NamespacedName.String() already relies
// on; hand-rolling this per call site previously let it drift, harmlessly
// today since every watched GVK is namespaced, but silently, had a
// cluster-scoped GVK ever been added).
func (r *ResourceStreamReconciler) cacheKey(namespace, name string) string {
	return r.GVK.Kind + "/" + (client.ObjectKey{Namespace: namespace, Name: name}).String()
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
func (r *ResourceStreamReconciler) emitDelete(ctx context.Context, log logr.Logger, namespace, name, expectedUID string) (claimed bool, err error) {
	objectKey := r.cacheKey(namespace, name)

	entry, version, claimed := r.HashCache.ReserveDelete(objectKey, expectedUID)
	if !claimed {
		return false, nil
	}

	log.Info("🗑️ Object gone, queuing Deleted event for ClickHouse", "kind", r.GVK.Kind, "namespace", namespace, "name", name, "uid", entry.UID)

	record := ResourceRecord{
		Timestamp:  time.Now().UTC(),
		ClusterID:  r.ClusterID,
		EventType:  "Deleted",
		APIGroup:   r.GVK.Group,
		APIVersion: r.GVK.Version,
		Kind:       r.GVK.Kind,
		Namespace:  namespace,
		Name:       name,
		UID:        entry.UID,
		Data:       `{"status": "deleted"}`,
		SHA256:     "DELETED",
	}

	// The cache entry is only removed once the deletion record is durably
	// written (see commit) — never before — so a crash or write failure
	// can't silently drop this object from history. On failure, the claim is
	// released (not the whole entry) so a later attempt can retry; a stale
	// release from a superseded claim (e.g. the object was recreated under a
	// new UID while this write was in flight) is a safe no-op — see
	// UnclaimDelete.
	enqueueErr := r.CHWriter.Enqueue(ctx, 0, writeJob{
		query: insertResourceStateQuery,
		args:  record.insertArgs(),
		commit: func(ok bool) {
			if ok {
				r.HashCache.DeleteIfCurrent(objectKey, version)
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
func (r *ResourceStreamReconciler) enqueueCloseOut(ctx context.Context, log logr.Logger, objectKey string, record ResourceRecord) {
	enqueueErr := r.CHWriter.Enqueue(ctx, 0, writeJob{
		query: insertResourceStateQuery,
		args:  record.insertArgs(),
		commit: func(ok bool) {
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
func (r *ResourceStreamReconciler) retryPendingCloseOuts(ctx context.Context, log logr.Logger, objectKey string) {
	for _, record := range r.closeOuts.TakeAll(objectKey) {
		r.enqueueCloseOut(ctx, log, objectKey, record)
	}
}

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

	// --- DEDUPLICATION AND REINCARNATION BLOCK ---
	if cachedEntry, exists := r.HashCache.Load(objectKey); exists {
		// 🚨 ANTI-ZOMBIE MAGIC: check the UID!
		if cachedEntry.UID != "" && cachedEntry.UID != currentUID {
			log.Info("🧟 Reincarnation! Old object died during downtime — closing its history and treating the current one as Added", "name", req.Name)

			// If a delete for the old incarnation is already claimed (see
			// hashCache.ReserveDelete) — e.g. a NotFound-Reconcile for it
			// already ran and is mid-flight — that write will land in
			// ClickHouse on its own. Enqueuing a second close-out row here
			// would duplicate it, so skip straight to treating the current
			// object as Added.
			if cachedEntry.PendingDelete {
				log.Info("🧟 Old incarnation's deletion already claimed elsewhere, skipping close-out write", "kind", r.GVK.Kind, "name", req.Name, "old_uid", cachedEntry.UID)
			} else {
				closeRecord := ResourceRecord{
					Timestamp:  time.Now().UTC(),
					ClusterID:  r.ClusterID,
					EventType:  "Deleted",
					APIGroup:   r.GVK.Group,
					APIVersion: r.GVK.Version,
					Kind:       r.GVK.Kind,
					Namespace:  req.Namespace,
					Name:       req.Name,
					UID:        cachedEntry.UID,
					Data:       `{"status": "deleted"}`,
					SHA256:     "DELETED",
				}

				// 1. Close out the old object's history — failures are
				// remembered and retried (see enqueueCloseOut), not just
				// logged, so this historical record can't be silently lost.
				r.enqueueCloseOut(ctx, log, objectKey, closeRecord)
			}

			// 2. The current object is treated as a plain Added (leave eventType = "Added").
			// There is no confirmed prior state for THIS UID, so revertEntry stays nil.
		} else {
			// Ordinary logic (same object)
			if cachedEntry.Hash == hashString {
				return ctrl.Result{}, nil // Duplicate
			}

			eventType = "Modified"
			if cachedEntry.JSON == nil {
				log.Info("🔄 Restored after restart (Full State)", "kind", r.GVK.Kind, "name", req.Name)
				dataString = string(objJson)
				diffString = ""
			} else {
				patch, _ := jsondiff.CompareJSON(cachedEntry.JSON, objJson)
				patchBytes, _ := json.Marshal(patch)
				diffString = string(patchBytes)
				dataString = ""
				log.Info("📝 Change detected (Diff)", "kind", r.GVK.Kind, "name", req.Name)
			}
			entryCopy := cachedEntry
			revertEntry = &entryCopy
		}
	} else {
		log.Info("🌟 New object observed", "kind", r.GVK.Kind, "name", req.Name)
	}

	// A cache-miss (eventType still "Added") during SafeMode can't be
	// trusted to mean "genuinely new" — the cache may simply not be warmed
	// yet. Tag it Snapshot so a slow/unavailable ClickHouse at startup
	// never masquerades as a mass "Added" duplicate-write storm.
	if eventType == "Added" && r.SafeMode.Load() {
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

	revertVersion := func() {
		if revertEntry != nil {
			r.HashCache.CommitIfCurrent(objectKey, version, *revertEntry)
		} else {
			r.HashCache.DeleteIfCurrent(objectKey, version)
		}
	}

	record := ResourceRecord{
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
		Data:            dataString,
		DiffData:        diffString,
		SHA256:          hashString,
	}

	// --- EXPORT BLOCK ---
	enqueueErr := r.CHWriter.Enqueue(ctx, 0, writeJob{
		query: insertResourceStateQuery,
		args:  record.insertArgs(),
		commit: func(ok bool) {
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

func (r *ResourceStreamReconciler) SetupWithManager(mgr ctrl.Manager, chConfig ClickHouseConfig, reconcilerConfig ReconcilerConfig) error {
	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{chConfig.Addr},
		Auth: clickhouse.Auth{
			Database: chConfig.Database,
			Username: chConfig.Username,
			Password: chConfig.Password,
		},
		Protocol:    clickhouse.Native,
		DialTimeout: chConfig.DialTimeout,
		ReadTimeout: chConfig.ReadTimeout,
	})
	if err != nil {
		return err
	}
	r.CHConn = conn

	// CHWriter decouples every ClickHouse insert from the Reconcile hot path
	// (see chwriter.go). One instance is shared across all watched GVKs; its
	// lifecycle — including closing conn on shutdown — is tied to the
	// manager's via mgr.Add. insertTimeout is chConfig.ReadTimeout, so the
	// operator-configured value actually governs the async write path (not
	// just the connection's own driver-level timeout).
	chWriter := NewCHWriter(conn, 0, 0, chConfig.ReadTimeout, 0, 0)

	// connUsers tracks goroutines besides CHWriter's own workers that still
	// need conn (namely restoreAndWarm, below) — CHWriter waits for this to
	// drain before closing conn, so shutdown can never race a use of the
	// shared connection against its closure.
	var connUsers sync.WaitGroup
	chWriter.WaitForOthers(&connUsers)

	if err := mgr.Add(chWriter); err != nil {
		return err
	}

	gvksToWatch := []schema.GroupVersionKind{
		{Group: "", Version: "v1", Kind: "Pod"},
		{Group: "", Version: "v1", Kind: "Service"},
		{Group: "apps", Version: "v1", Kind: "Deployment"},
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
			CHConn:    conn,
			CHWriter:  chWriter,
			GVK:       gvk,
			ClusterID: reconcilerConfig.ClusterID,
			requeueCh: requeueCh,
		}
		// The cache starts empty and isn't trustworthy until restoreAndWarm
		// (below) finishes — see the SafeMode field doc and Reconcile.
		reconciler.SafeMode.Store(true)

		connUsers.Add(1)
		if err := mgr.Add(manager.RunnableFunc(func(ctx context.Context) error {
			defer connUsers.Done()
			return restoreAndWarm(ctx, mgr, conn, reconciler, gvk, log)
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

// restoreAndWarm rebuilds a single GVK's HashCache from ClickHouse history in
// the background, then runs the zombie-object GC pass. Running this as a
// manager.Runnable (rather than inline in SetupWithManager) means mgr.Start()
// is never gated on ClickHouse being reachable at boot.
//
// The restore query is retried indefinitely (bounded backoff, no elapsed-time
// cutoff) until it succeeds or ctx is cancelled by manager shutdown. While it
// hasn't yet succeeded, reconciler.SafeMode stays true, so Reconcile tags
// cache-miss events "Snapshot" instead of "Added" — a ClickHouse outage at
// startup degrades to that instead of re-emitting every live object in the
// cluster as a duplicate "Added" row.
func restoreAndWarm(ctx context.Context, mgr ctrl.Manager, conn driver.Conn, reconciler *ResourceStreamReconciler, gvk schema.GroupVersionKind, log logr.Logger) error {
	log.Info("🔄 Warming cache from ClickHouse history...", "kind", gvk.Kind)

	type gcTarget struct {
		Namespace string
		Name      string
		UID       string
	}
	var targetsToCheck []gcTarget

	warm := func() error {
		targetsToCheck = nil

		rows, err := conn.Query(ctx, `
            SELECT namespace, name, argMax(uid, ts), argMax(sha256, ts)
            FROM resource_states
            WHERE kind = ?
            GROUP BY namespace, name
            HAVING argMax(event_type, ts) != 'Deleted'
        `, gvk.Kind)
		if err != nil {
			return err
		}
		defer rows.Close()

		restoredCount := 0
		for rows.Next() {
			var namespace, name, uid, hash string
			if err := rows.Scan(&namespace, &name, &uid, &hash); err != nil {
				continue
			}
			key := reconciler.cacheKey(namespace, name)

			// StoreIfAbsent, not Store: a live Reconcile may already have
			// reserved a newer entry for this key while SafeMode was still
			// true (tagged "Snapshot") — that live state is authoritative
			// and must not be clobbered by this historical baseline.
			reconciler.HashCache.StoreIfAbsent(key, CacheEntry{
				Hash: hash,
				JSON: nil,
				UID:  uid,
			})
			restoredCount++
			targetsToCheck = append(targetsToCheck, gcTarget{Namespace: namespace, Name: name, UID: uid})
		}
		log.Info("✅ Cache warmed", "kind", gvk.Kind, "objects_loaded", restoredCount)
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
		return nil
	}

	reconciler.SafeMode.Store(false)
	log.Info("🔓 Cache warm-up complete, leaving Snapshot mode", "kind", gvk.Kind)

	if len(targetsToCheck) == 0 {
		return nil
	}

	// ==========================================
	// 🕵️‍♂️ Garbage Collector (with UID verification)
	// ==========================================
	if !mgr.GetCache().WaitForCacheSync(ctx) {
		return nil
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
	return nil
}
