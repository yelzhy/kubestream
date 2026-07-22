package controller

import (
	"bytes"
	"fmt"

	"github.com/klauspost/compress/zstd"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// entryEncoding marks how CacheEntry.JSON is stored so the diff path knows
// whether it must decompress before comparing. It exists because the diff
// baseline — a full normalized-JSON copy of every watched object, held in
// addition to the informer cache's own copy — is the dominant hashCache
// memory cost at scale (D2). Kubernetes JSON compresses extremely well, so
// baselines are stored zstd-compressed and decompressed only when a diff is
// actually computed. The marker (rather than always assuming compression)
// lets a compression failure degrade gracefully to storing raw bytes
// (Invariant 5) while a later diff still knows not to decompress them.
type entryEncoding uint8

const (
	// encodingRaw means CacheEntry.JSON holds uncompressed bytes. It is the
	// zero value, so a CacheEntry built with a nil JSON (e.g. the warm-up
	// baseline in restoreAndWarm) is correctly classified without any
	// explicit assignment.
	encodingRaw entryEncoding = iota
	// encodingZstd means CacheEntry.JSON holds zstd-compressed bytes that
	// must be run through decodeBaseline before diffing.
	encodingZstd
)

// zstdMagic is the 4-byte magic number that opens every zstd frame
// (RFC 8878). decodeBaseline checks for it before attempting a decompress so
// a corrupted or mislabelled entry fails fast with a clear error instead of
// feeding garbage into the decoder — the corruption path then falls back to
// a full-state write exactly like a missing baseline.
var zstdMagic = []byte{0x28, 0xB5, 0x2F, 0xFD}

// zstdEncoder and zstdDecoder are process-wide singletons: zstd's EncodeAll
// and DecodeAll are explicitly safe for concurrent use (each call runs on its
// own goroutine from an internal pool), so every pipeline worker shares one
// pair rather than allocating a codec per write. SpeedDefault is the level
// the task pins — a good size/CPU trade-off for the hot write path. Both are
// created with valid options, so the errors are not expected; a nil codec
// (should construction ever fail) is handled defensively by compressBaseline
// and decodeBaseline rather than panicking.
var (
	zstdEncoder, _ = zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedDefault))
	zstdDecoder, _ = zstd.NewReader(nil)
)

// compressBaseline compresses a normalized-JSON diff baseline for storage in
// CacheEntry.JSON, returning the bytes to store, their encoding marker, and
// whether compression actually happened. It never fails the caller: if the
// shared encoder is unavailable (construction failed) it degrades to storing
// the raw bytes and reports compressed=false so the caller can log the
// anomaly at Error level (Invariant 5). An empty input is passed through
// untouched — there is nothing to compress and nothing to diff against.
func compressBaseline(raw []byte) (data []byte, enc entryEncoding, compressed bool) {
	if len(raw) == 0 || zstdEncoder == nil {
		return raw, encodingRaw, false
	}
	return zstdEncoder.EncodeAll(raw, nil), encodingZstd, true
}

// decodeBaseline returns the raw normalized JSON for this entry's stored diff
// baseline, decompressing if it was zstd-encoded. A nil JSON yields (nil, nil)
// — the "no prior baseline" case the caller already handles as a full-state
// write. A corrupt or truncated compressed entry returns an error so the
// caller falls back to a full-state write (and logs) rather than diffing
// against garbage; the magic-byte check catches a mislabelled entry before
// the decoder is even invoked.
func (e CacheEntry) decodeBaseline() ([]byte, error) {
	if e.JSON == nil {
		return nil, nil
	}
	switch e.Encoding {
	case encodingRaw:
		return e.JSON, nil
	case encodingZstd:
		if !bytes.HasPrefix(e.JSON, zstdMagic) {
			return nil, fmt.Errorf("compressed baseline missing zstd magic bytes (corrupt entry, %d bytes)", len(e.JSON))
		}
		if zstdDecoder == nil {
			return nil, fmt.Errorf("zstd decoder unavailable")
		}
		return zstdDecoder.DecodeAll(e.JSON, nil)
	default:
		return nil, fmt.Errorf("unknown cache entry encoding %d", e.Encoding)
	}
}

// CacheEntry holds the in-memory cached state for one object (see hashCache).
type CacheEntry struct {
	Hash string
	// JSON is the normalized-JSON diff baseline for this object, stored in the
	// form indicated by Encoding (zstd-compressed in the common case). It is
	// never read on the dedup short-circuit — only Hash is — so an unchanged
	// object is deduplicated without ever decompressing. Decode it via
	// decodeBaseline() before diffing. A nil JSON means "no confirmed
	// baseline yet" (e.g. a history-warmed entry) and diffs to full state.
	JSON []byte
	// Encoding records how JSON is stored (raw vs zstd) so decodeBaseline
	// knows whether to decompress. See entryEncoding.
	Encoding entryEncoding
	UID      string
	// Version is assigned by hashCache.Reserve/StoreIfAbsent and is the basis
	// for CommitIfCurrent/DeleteIfCurrent's staleness check: an async write's
	// outcome is only applied if the entry's Version hasn't moved on since
	// the write was issued, so an out-of-order (but stale) commit can never
	// clobber a newer entry. See hashcache.go.
	Version uint64
	// PendingDelete is set by hashCache.ReserveDelete while a "Deleted" write
	// for this key is in flight. It's what lets the live delete path and the
	// startup GC pass share one claim: whichever of them notices the object
	// is gone first claims it and flips this on; anyone else who notices the
	// same disappearance before the claim resolves sees it already set and
	// does not enqueue a second write. See hashcache.go.
	PendingDelete bool
}

// ReconcilerConfig holds operator-level settings that aren't specific to the
// ClickHouse connection itself. cmd/main.go is responsible for sourcing every
// field from flags/environment variables.
type ReconcilerConfig struct {
	// ClusterID identifies this operator's cluster in every row it writes
	// to ClickHouse (replaces the old hardcoded "local-kind-cluster").
	ClusterID string
	// MaxConcurrentReconciles bounds how many Reconciles run concurrently
	// per watched GVK. Safe to raise above controller-runtime's default of
	// 1 now that ClickHouse writes are off the Reconcile hot path (see
	// sink.Writer) — but it's still a bound, not unlimited, so throughput
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
