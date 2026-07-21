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
ENGINE = MergeTree
PARTITION BY toYYYYMM(ts)
ORDER BY (cluster_id, api_group, kind, namespace, name, ts);
