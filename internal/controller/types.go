package controller

import (
	"time"

	"k8s.io/apimachinery/pkg/runtime/schema"
)

// CacheEntry holds the in-memory cached state for one object (see hashCache).
type CacheEntry struct {
	Hash string
	JSON []byte
	UID  string
	// Version is assigned by hashCache.Reserve/StoreIfAbsent and is the basis
	// for CommitIfCurrent/DeleteIfCurrent's staleness check: an async write's
	// outcome is only applied if the entry's Version hasn't moved on since
	// the write was issued, so an out-of-order (but stale) commit can never
	// clobber a newer write's result. See hashcache.go.
	Version uint64
	// PendingDelete is set by hashCache.ReserveDelete while a "Deleted" write
	// for this key is in flight. It's what lets the live delete path and the
	// startup GC pass share one claim: whichever of them notices the object
	// is gone first claims it and flips this on; anyone else who notices the
	// same disappearance before the claim resolves sees it already set and
	// does not enqueue a second write. See hashcache.go.
	PendingDelete bool
}

// ResourceRecord is the universal structure used to send a row to ClickHouse.
type ResourceRecord struct {
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
	// Actors are the field-manager names harvested from the object's
	// managedFields (see the resource_states.actors column). The field exists
	// from Task 0.2 so the new column binds and this package compiles; it is
	// always empty here (every row writes []) and is populated by Task 0.3's
	// managedFields extraction. Deleted rows carry no actors — there is no live
	// object to inspect.
	Actors []string `json:"actors"`
	Data   string   `json:"data"` // Full JSON (for Added)
	// Diff is an RFC 6902 JSON Patch (wI2L/jsondiff) describing the change on a
	// Modified event; empty on every other event type. Named to match the
	// schema-v1 "diff" column (renamed from the old "diff_data").
	Diff   string `json:"diff"`
	SHA256 string `json:"sha256"`
}

// ClickHouseConfig holds the externally configurable ClickHouse connection
// settings. It carries no defaults of its own — cmd/main.go is responsible
// for sourcing every field from flags/environment variables, so no
// ClickHouse host, credential, or timeout is ever hardcoded in this package.
type ClickHouseConfig struct {
	Addr        string
	Database    string
	Username    string
	Password    string
	DialTimeout time.Duration
	ReadTimeout time.Duration
	// AutoCreateSchema, when true, makes the operator execute the shipped DDL
	// (deploy/clickhouse/schema) idempotently at connect time before validating
	// the live schema. Sourced from --ch-auto-create-schema / CH_AUTO_CREATE_SCHEMA;
	// defaults to false so the operator never mutates ClickHouse DDL unless asked.
	AutoCreateSchema bool
}

// ReconcilerConfig holds operator-level settings that aren't specific to the
// ClickHouse connection itself. Like ClickHouseConfig, cmd/main.go is
// responsible for sourcing every field from flags/environment variables.
type ReconcilerConfig struct {
	// ClusterID identifies this operator's cluster in every row it writes
	// to ClickHouse (replaces the old hardcoded "local-kind-cluster").
	ClusterID string
	// MaxConcurrentReconciles bounds how many Reconciles run concurrently
	// per watched GVK. Safe to raise above controller-runtime's default of
	// 1 now that ClickHouse writes are off the Reconcile hot path (see
	// CHWriter) — but it's still a bound, not unlimited, so throughput
	// can't grow without limit under event floods.
	MaxConcurrentReconciles int
	// WatchedGVKs is the set of resource types (including any CRD) this
	// operator watches and streams to ClickHouse. cmd/main.go sources it
	// from the WATCHED_GVKS env var / --watched-gvks flag (see its
	// parseGVKList) rather than a hardcoded Go slice, so adding a new type
	// is a config change, not a code change. Kubernetes RBAC is still a
	// static, server-side resource, though — watching a GVK outside the
	// operator's default ClusterRole (config/rbac/role.yaml) additionally
	// requires that role to be extended to grant it; see the kubebuilder
	// markers on ResourceStreamReconciler.Reconcile.
	WatchedGVKs []schema.GroupVersionKind
}

// insertArgs returns the positional arguments for the resource_states INSERT
// used by CHWriter, in exactly the column order expected by
// insertResourceStateQuery. nil Labels/Actors are coerced to empty containers
// so the Map/Array columns always bind a concrete value rather than a NULL the
// non-nullable schema would reject.
func (rec ResourceRecord) insertArgs() []any {
	labels := rec.Labels
	if labels == nil {
		labels = map[string]string{}
	}
	actors := rec.Actors
	if actors == nil {
		actors = []string{}
	}
	return []any{
		rec.Timestamp.UTC().Format(chTimeFormat), rec.ClusterID, rec.EventType, rec.APIGroup, rec.APIVersion,
		rec.Kind, rec.Namespace, rec.Name, rec.UID, rec.ResourceVersion, labels, actors, rec.Data, rec.Diff, rec.SHA256,
	}
}
