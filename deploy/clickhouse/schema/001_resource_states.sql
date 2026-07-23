CREATE TABLE IF NOT EXISTS resource_states
(
    ts               DateTime64(9, 'UTC') CODEC(Delta, ZSTD(1)),
    cluster_id       LowCardinality(String),
    event_type       LowCardinality(String), -- Added|Modified|Deleted|Snapshot|Checkpoint (Checkpoint reserved until Task 2.2)
    api_group        LowCardinality(String),
    api_version      LowCardinality(String),
    kind             LowCardinality(String),
    namespace        String,
    name             String,
    uid              String,
    resource_version String,
    labels           Map(LowCardinality(String), String),
    actors           Array(LowCardinality(String)),   -- field-manager names from managedFields
    data             String CODEC(ZSTD(3)),           -- full normalized JSON (Added/Snapshot/Checkpoint), empty otherwise
    diff             String CODEC(ZSTD(3)),           -- RFC 6902 JSON Patch (Modified/Checkpoint), empty otherwise
    sha256           String                           -- hex of normalized JSON; empty on Deleted
)
-- ReplacingMergeTree (not plain MergeTree): the operator's write path is
-- at-least-once. A lost acknowledgement after a successful server-side insert
-- makes the poison-isolation path re-insert a byte-identical row (same ts —
-- frozen once at reconcile time — plus identical sha256, uid, event_type, data,
-- and diff). Such a re-insert collides on the full ORDER BY key, so
-- ReplacingMergeTree collapses it to a single row on merge. A genuinely-distinct
-- event never collides: ts is DateTime64(9) (nanosecond) and frozen per event,
-- so the ORDER BY tuple alone distinguishes real re-inserts from real events.
-- Readers needing exact counts before a merge must use FINAL (or an equivalent
-- argMax / LIMIT 1 BY dedup) — see docs/SCHEMA.md "Delivery semantics".
ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMM(ts)
ORDER BY (cluster_id, api_group, kind, namespace, name, ts);
