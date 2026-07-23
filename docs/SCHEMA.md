# kubestream ClickHouse Schema (v1)

This document is the authoritative reference for the kubestream ClickHouse
schema. The DDL is shipped in-repo under
[`deploy/clickhouse/schema/`](../deploy/clickhouse/schema/):

- [`001_resource_states.sql`](../deploy/clickhouse/schema/001_resource_states.sql) — the per-object change stream.
- [`002_watch_scopes.sql`](../deploy/clickhouse/schema/002_watch_scopes.sql) — the watch-scope epoch log.

> **Schema stability.** Under [D5] the schema gets one free redesign now (this
> is it) and is then **frozen as a public API** in Task 2.6. Every column below
> is deliberate; treat additions as append-only and breaking changes as
> off-limits after the freeze.

## Applying the schema

Either apply the two `.sql` files yourself (e.g. `clickhouse-client --multiquery
< 001_resource_states.sql`), or let the operator create them idempotently at
connect time by starting it with `--ch-auto-create-schema=true` (env twin
`CH_AUTO_CREATE_SCHEMA`). Auto-create is **off by default** — the operator never
mutates ClickHouse DDL unless explicitly asked.

Regardless of how the tables are created, on connect the operator introspects
`system.columns` for both tables and verifies every required column name and
type. A mismatch is logged at `Error` level naming each offending
table/column, and the dedicated `clickhouse-schema` readiness probe degrades to
not-ready — the process does not crash-loop.

---

## `resource_states`

One row per observed state transition of a watched object.

| Column | Type | Semantics |
|---|---|---|
| `ts` | `DateTime64(9, 'UTC')` | Event timestamp (nanosecond precision, UTC). `Delta, ZSTD(1)` codec — monotonic-ish timestamps compress extremely well under delta coding. |
| `cluster_id` | `LowCardinality(String)` | Identifies the cluster this operator instance serves. Explicit in the schema (a future multi-cluster reader distinguishes rows by it); implicit in-process (one operator serves one cluster). |
| `event_type` | `LowCardinality(String)` | The state-machine label — see [Event-type state machine](#event-type-state-machine). |
| `api_group` | `LowCardinality(String)` | API group (e.g. `apps`; empty `""` for the core group). Part of the canonical identity. |
| `api_version` | `LowCardinality(String)` | API version observed (e.g. `v1`). Recorded for provenance but **not** part of identity — see [Identity is version-agnostic](#identity-is-version-agnostic). |
| `kind` | `LowCardinality(String)` | Object kind (e.g. `Deployment`). Part of the canonical identity. |
| `namespace` | `String` | Object namespace; `""` for cluster-scoped objects. Part of the canonical identity. |
| `name` | `String` | Object name. Part of the canonical identity. |
| `uid` | `String` | Kubernetes UID of the object incarnation. Distinguishes a delete-and-recreate of the same name (a "reincarnation") from a plain update. |
| `resource_version` | `String` | The object's `metadata.resourceVersion` at observation time. |
| `labels` | `Map(LowCardinality(String), String)` | The object's labels at observation time. |
| `actors` | `Array(LowCardinality(String))` | Field-manager names harvested from `metadata.managedFields` — the cheapest "who probably changed this" signal. De-duplicated and sorted; empty manager names are recorded as `unknown`. **Always empty (`[]`) on `Deleted` rows** — there is no live object left to inspect, so a deletion's authorship is intentionally not attributed. |
| `data` | `String` (`ZSTD(3)`) | Full normalized JSON of the object. Populated on `Added`, `Snapshot`, and `Checkpoint`; **empty** otherwise. |
| `diff` | `String` (`ZSTD(3)`) | RFC 6902 JSON Patch describing the change. Populated on `Modified` (and `Checkpoint`); **empty** otherwise. See [Diff format](#diff-format). |
| `sha256` | `String` | Hex SHA-256 of the normalized JSON, used for dedup/version-gating. **Empty on `Deleted`.** |

**Engine & layout:**

```sql
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
ORDER BY (cluster_id, api_group, kind, namespace, name, ts)
```

The sort key leads with the identity tuple and ends with `ts`, so all history
for a single object is physically contiguous and time-ordered — the exact
access pattern of both the warm-up query and typical audit lookups. Monthly
partitioning keeps merges and TTL drops cheap at scale.

### Delivery semantics

The operator's write path is **at-least-once**, not exactly-once, at the row
level. `driver.Batch.Send()` (and a single-row fallback `Exec`) is a network
operation with three outcomes: nothing inserted; everything inserted but the
acknowledgement lost (timeout or connection reset mid-response); or a partial
insert. In the lost-ack and partial cases the call returns an error while the
rows are already durable, so the writer's poison-isolation path re-inserts them.

A re-inserted row is **byte-identical** to the original: `ts` is stamped once at
reconcile time and frozen into the insert's positional args, so the re-insert
carries the same `ts`, `sha256`, `uid`, `event_type`, `data`, and `diff`. Every
column of the `ORDER BY` tuple is therefore identical, and `resource_states`
(`ReplacingMergeTree`) **collapses the duplicate to a single row on merge**. The
writer's per-job commit callback remains **exactly-once** regardless — a lost ack
never causes a job to be counted or committed twice, only a physical row to be
re-sent.

Because `ReplacingMergeTree` de-duplicates only on background merge, a naive
`SELECT *` can transiently observe a duplicate before the merge runs. **Any read
that must not double-count must use `FINAL`** (or an explicit `argMax` / `LIMIT 1
BY` dedup). The operator's own warm-up read (`statereader.go`) is already
dedup-safe without `FINAL`: it `GROUP BY (namespace, name)` with `argMax(…, ts)`,
so it emits exactly one row per identity regardless of unmerged duplicates, and
argMax over byte-identical duplicates returns the same value either way.

### Deletion semantics — no sentinels

Schema v1 removes the pre-v1 magic-string sentinels that the `sha256` and
`data` columns used to carry for deletions. **`event_type` alone carries
deletion semantics now.** A `Deleted` row has empty `data`, empty `diff`, empty
`sha256`, and empty `actors`. Consumers must key off `event_type = 'Deleted'`,
never off sentinel values in the data columns.

---

## `watch_scopes`

The watch-scope epoch log. **Created and frozen now, but not written to until
Task 1.6** — with dynamic rules, "we stopped watching X" and "X was deleted"
are different truths and must be recorded differently, or the audit trail lies.
A `Stopped` row means the operator stopped observing a scope; it is *not* a
statement that the objects in that scope were deleted.

| Column | Type | Semantics |
|---|---|---|
| `ts` | `DateTime64(9, 'UTC')` | Transition timestamp (`Delta, ZSTD(1)` codec). |
| `cluster_id` | `LowCardinality(String)` | Cluster this operator serves. |
| `api_group` | `LowCardinality(String)` | API group of the watched scope. |
| `api_version` | `LowCardinality(String)` | API version of the watched scope. |
| `kind` | `LowCardinality(String)` | Kind of the watched scope. |
| `namespace` | `String` | Watched namespace; `""` means cluster-scoped or all-namespaces. |
| `action` | `LowCardinality(String)` | `Started` when a `(sink, scope)` gains its first interested rule; `Stopped` when it loses its last. |
| `rule_ref` | `String` | The rule that triggered the transition: `"<namespace>/<name>"` for a `StreamRule`, `"<name>"` for a `ClusterStreamRule`. |

```sql
ENGINE = MergeTree
ORDER BY (cluster_id, api_group, kind, namespace, ts)
```

---

## Event-type state machine

`event_type` is one of `Added | Modified | Deleted | Snapshot | Checkpoint`.

- **`Added`** — the object was observed for the first time (a genuine cache
  miss while the cache is trusted), or a reincarnation (same name, new UID)
  supersedes a prior incarnation. Carries full `data`.
- **`Modified`** — a subsequent observation whose content hash differs from the
  last recorded state. Carries a `diff` against the prior state; falls back to
  full `data` when a diff cannot be produced (see below).
- **`Deleted`** — the object is gone (live delete, reincarnation close-out of
  the old UID, or startup GC of a "zombie"). Empty `data`/`diff`/`sha256`.
- **`Snapshot`** — a cache miss observed *while the cache has not yet been
  warmed from ClickHouse history* (startup "SafeMode"). Tagged `Snapshot`
  rather than `Added` so a slow/unavailable ClickHouse at startup can't
  masquerade as a mass duplicate-`Added` storm. Carries full `data`.
- **`Checkpoint`** — **reserved until Task 2.2**; no code emits it yet. It will
  carry either full `data` or a `diff`. The column set and validation accept it
  now so the schema need not change when it lands.

Typical lifecycle for one object:

```
(first seen) --> Added --> Modified --> Modified --> ... --> Deleted
                   ^                                            |
                   +------------ (reincarnation, new UID) ------+
```

At startup before warm-up completes, first sightings are `Snapshot` instead of
`Added`; once warm, normal `Added`/`Modified` resumes.

## Diff format

`Modified` rows store an **RFC 6902 JSON Patch** in the `diff` column, produced
by [`wI2L/jsondiff`](https://github.com/wI2L/jsondiff) comparing the previous
normalized JSON against the current one. Normalization strips volatile fields
(`metadata.managedFields`, `metadata.resourceVersion`, `metadata.generation`)
before hashing and diffing, so cosmetic churn does not generate rows.

**Graceful degradation:** if no prior JSON baseline exists, or the diff/marshal
fails, the operator writes the full current state to `data` and leaves `diff`
empty — a full-state row is always correct, merely larger than a diff.

## Identity is version-agnostic

> An object's identity is `(cluster_id, api_group, kind, namespace, name)` —
> **version-agnostic** (`apps/v1` and a hypothetical `apps/v2` Deployment are
> the same object).

`api_version` is recorded for provenance but is **not** part of identity. This
is why the warm-up/restore query filters on `api_group`, `kind`, and
`cluster_id` (not `api_version`), and why the sort key omits `api_version`.
Filtering on `api_group` — not `kind` alone — is what keeps two resources that
share a Kind (e.g. `batch/v1` `Job` vs. a CRD `example.com/v1` `Job`) from
cross-contaminating each other's history.

## Suggested TTL (optional, non-mandatory)

kubestream does **not** impose a retention policy — audit data is often kept
indefinitely, and retention is a deployment decision. If you do want automatic
expiry, a TTL clause can be added to `resource_states` (and/or `watch_scopes`),
for example a 1-year retention:

```sql
ALTER TABLE resource_states MODIFY TTL toDateTime(ts) + INTERVAL 1 YEAR;
```

Or bake it into the table at creation time by appending to the DDL:

```sql
ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMM(ts)
ORDER BY (cluster_id, api_group, kind, namespace, name, ts)
TTL toDateTime(ts) + INTERVAL 1 YEAR;
```

This is a suggestion only; the operator neither sets nor requires a TTL, and
connect-time validation ignores TTL clauses (it checks column names/types
only).

[D5]: ../kubestream-backlog.md
