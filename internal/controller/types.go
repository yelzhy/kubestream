package controller

import (
	"time"
)

// CacheEntry хранит данные в оперативной памяти (hashCache)
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
}

// ResourceRecord - это универсальная структура для отправки в ClickHouse
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
	Data            string            `json:"data"`      // Полный JSON (для Added)
	DiffData        string            `json:"diff_data"` // Дельта изменений (для Modified)
	SHA256          string            `json:"sha256"`
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
}

// insertArgs returns the positional arguments for the resource_states INSERT
// used by CHWriter, in exactly the column order expected by
// insertResourceStateQuery.
func (rec ResourceRecord) insertArgs() []any {
	labels := rec.Labels
	if labels == nil {
		labels = map[string]string{}
	}
	return []any{
		rec.Timestamp.UTC().Format(chTimeFormat), rec.ClusterID, rec.EventType, rec.APIGroup, rec.APIVersion,
		rec.Kind, rec.Namespace, rec.Name, rec.UID, rec.ResourceVersion, labels, rec.Data, rec.DiffData, rec.SHA256,
	}
}
