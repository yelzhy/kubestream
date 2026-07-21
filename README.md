# kubestream

**Git blame for your Kubernetes cluster.** A lightweight operator that streams every Pod, Service, and Deployment state change into ClickHouse — so "what did this look like before it broke?" has an answer.

## Overview & Motivation

Kubernetes' own audit trail is short-lived and symptom-focused. `kubectl get events` shows you the last hour or so of what happened, native audit logs capture *API calls* rather than *resulting object state*, and once a Pod is evicted or a Deployment is rolled back, the exact spec/status that existed five minutes before the incident is gone. Incident post-mortems end up reconstructing history from a mix of memory, dashboards, and luck.

`kubestream` is a Kubernetes operator (built on `controller-runtime`/`client-go`) that watches a configurable set of resource types and writes every observed state transition — `Added`, `Modified`, `Deleted` — into a ClickHouse table as an immutable, append-only row. Because it hashes and diffs each object's normalized JSON, it only writes when something actually changed, giving you a compact, queryable, retrospective timeline of cluster state instead of a live-only snapshot.

It is currently scoped to core workload resources (Pods, Deployments, Services) but the watched set is a runtime configuration value, not a compiled-in list — extending it to other built-in types or CRDs is a config change (plus a matching RBAC grant), not a code change.

## Use Cases

- **Incident post-mortems** — "what was this Deployment's spec/status at 03:14 UTC, right before the outage started?" Query ClickHouse instead of hoping someone took a screenshot.
- **Platform engineering** — track configuration drift across a fleet of clusters: who/what changed a resource, and what changed exactly (via the stored diff).
- **SecOps** — an independent, operator-owned record of resource state that doesn't depend on the API server's own audit log retention window.
- **Compliance / change auditing** — a durable, timestamped history of every workload state change, per cluster, for retrospective review.

## Key Features

- **Dynamic resource watching** — the set of watched GVKs is driven entirely by the `WATCHED_GVKS` env var / `--watched-gvks` flag (`"version/kind"` or `"group/version/kind"`, comma-separated). Defaults to `v1/Pod,apps/v1/Deployment,v1/Service`. Adding a new type (including a CRD) is a config change; it still requires a matching RBAC grant in `config/rbac/role.yaml`.
- **Asynchronous, non-blocking ClickHouse writes** — every insert is handed off to `CHWriter`, a bounded-queue worker pool with exponential-backoff retries (`backoff/v4`). `Reconcile` never calls ClickHouse synchronously and returns as soon as a job is enqueued, so a slow or unavailable database never stalls the reconcile loop or the informer.
- **Hash-based deduplication** — every object's normalized JSON (with `managedFields`, `resourceVersion`, and `generation` stripped) is SHA-256 hashed; an unchanged hash short-circuits as a no-op, so only genuine state changes reach ClickHouse.
- **Graceful JSON diffing** — `Modified` events store a computed JSON patch (`wI2L/jsondiff`) instead of the full object where possible, falling back to the full state whenever a prior baseline is missing or the diff/marshal step itself fails, rather than dropping the row.
- **Version-gated cache, not a naive map** — the in-memory dedup cache (`hashCache`) assigns a monotonic version per key on every write attempt; a commit or delete only applies if its version is still current, so an out-of-order async write can never clobber a newer one.
- **Duplicate-write-safe delete handling** — the live delete path, the reincarnation (delete-then-recreate) close-out path, and the startup zombie-GC pass all claim through the same `ReserveDelete` primitive (with UID verification), so the same disappearance can never produce two `Deleted` rows.
- **Robust zombie-resource garbage collection** — on startup, a background pass compares ClickHouse's last-known state per object against the live (cache-backed) cluster and emits a `Deleted` row for anything that vanished (or was recreated under a new UID) while the operator was down.
- **Crash/restart resilient, non-blocking startup** — cache warm-up and GC run as a `manager.Runnable` in the background; `mgr.Start()` is never gated on ClickHouse being reachable. While the cache is still warming, cache-miss events are tagged `Snapshot` instead of `Added`, so a slow ClickHouse at boot degrades gracefully instead of re-emitting the whole cluster as a flood of duplicate `Added` rows.
- **Self-healing on write failure** — a terminally-failed async write reverts its optimistic cache entry and explicitly triggers a fresh `Reconcile` (via an internal event channel), rather than silently vanishing until an unrelated future change happens to touch the same object.
- **No hardcoded configuration** — ClickHouse address/credentials/timeouts, the cluster identifier, concurrency, and the watched GVK list are all sourced from flags/environment variables; `CH_PASSWORD` is environment-only (never a flag) so it never shows up in a process listing.

## Architecture Overview

```
Informer cache (per watched GVK)
        │  (Added/Modified/Deleted events)
        ▼
  Reconcile()
    ├─ normalize object JSON (strip managedFields/resourceVersion/generation)
    ├─ SHA-256 hash + compare against hashCache (versioned, in-memory)
    ├─ unchanged?  → no-op, return
    ├─ changed?    → compute JSON diff (or full state on cache-miss/diff failure)
    └─ Reserve() a pending cache version, then CHWriter.Enqueue(job)
                       │ (non-blocking, bounded channel)
                       ▼
              CHWriter worker pool
                (backoff/v4 retries per job)
                       │
                       ▼
              ClickHouse INSERT
                       │
          success ─────┴───── failure
            │                    │
   CommitIfCurrent()      revert cache + re-trigger Reconcile
   (settle the version)   (UnclaimDelete/requeue)
```

- **Informer caches, not live API calls**: all reads (`Get`, and the GC pass's existence check) go through the manager's shared, cache-backed client — nothing in the hot path issues a direct, uncached API request.
- **One `CHWriter` per manager, shared across every watched GVK**: registered as a `manager.Runnable`, its own lifecycle (start workers → drain the queue on shutdown → close the ClickHouse connection) is tied to the manager's.
- **One `hashCache` per GVK**: a mutex-protected map from `"<Kind>/<namespace>/<name>"` to a versioned `CacheEntry` (hash, normalized JSON, UID, version, pending-delete flag). All commits/deletes are gated on the version issued when the corresponding write was reserved.
- **Startup warm-up + GC**: for each watched GVK, a `restoreAndWarm` goroutine queries ClickHouse for the latest known state of every object, seeds the cache (without clobbering anything a live `Reconcile` already wrote), then diffs that snapshot against the live cluster to detect and close out "zombie" objects that disappeared while the operator was offline.
- **ClickHouse schema v1 is shipped in this repository** — the DDL lives under [`deploy/clickhouse/schema/`](deploy/clickhouse/schema/) and every column is documented in [`docs/SCHEMA.md`](docs/SCHEMA.md).

### ClickHouse schema

`kubestream` writes the per-object change stream to `resource_states` and the
watch-scope epoch log to `watch_scopes`. The full, authoritative DDL is shipped
in-repo and documented column-by-column:

- **DDL:** [`deploy/clickhouse/schema/001_resource_states.sql`](deploy/clickhouse/schema/001_resource_states.sql), [`deploy/clickhouse/schema/002_watch_scopes.sql`](deploy/clickhouse/schema/002_watch_scopes.sql)
- **Reference:** [`docs/SCHEMA.md`](docs/SCHEMA.md) — column semantics, the `event_type` state machine, the RFC 6902 diff format, the version-agnostic identity rule, and a suggested (optional) `TTL` clause.

Apply the two `.sql` files yourself, or start the operator with
`--ch-auto-create-schema=true` to have it execute the shipped DDL idempotently
at connect time. Either way, on connect the operator introspects
`system.columns` and validates the live tables against schema v1; a mismatch is
logged and degrades the `clickhouse-schema` readiness probe (it does not
crash-loop).

## Configuration

Every setting is available as both a CLI flag and an environment variable (flag wins if both are set), except `CH_PASSWORD`, which is environment-only.

| Flag | Env Var | Default | Description |
|---|---|---|---|
| `--ch-addr` | `CH_ADDR` | `127.0.0.1:9000` | ClickHouse server address (`host:port`, native protocol). |
| `--ch-database` | `CH_DATABASE` | `kubestream` | ClickHouse database name. |
| `--ch-username` | `CH_USERNAME` | `default` | ClickHouse username. |
| — | `CH_PASSWORD` | *(empty)* | ClickHouse password. Env-only by design — flag values are visible in `ps`; connects passwordless if unset (logged as a warning). |
| `--ch-dial-timeout` | `CH_DIAL_TIMEOUT` | `5s` | Timeout for establishing the ClickHouse connection. |
| `--ch-read-timeout` | `CH_READ_TIMEOUT` | `10s` | Timeout for a single ClickHouse query/insert round-trip; also governs the async writer's per-attempt insert timeout. |
| `--ch-auto-create-schema` | `CH_AUTO_CREATE_SCHEMA` | `false` | Execute the shipped DDL (`deploy/clickhouse/schema`) idempotently at connect time. Off by default — the operator never mutates ClickHouse DDL unless asked. |
| `--cluster-id` | `CLUSTER_ID` | `local-kind-cluster` | Identifier for this cluster, recorded on every row. |
| `--reconciler-max-concurrent` | `RECONCILER_MAX_CONCURRENT` | `5` | Max concurrent `Reconcile` calls per watched resource type. |
| `--watched-gvks` | `WATCHED_GVKS` | `v1/Pod,apps/v1/Deployment,v1/Service` | Comma-separated list of resource types to watch, as `version/kind` or `group/version/kind`. Adding a type outside the default RBAC grant requires extending `config/rbac/role.yaml`. |
| `--metrics-bind-address` | — | `0` (disabled) | Metrics endpoint bind address; `:8443` for HTTPS, `:8080` for HTTP. |
| `--metrics-secure` | — | `true` | Serve the metrics endpoint over HTTPS. |
| `--health-probe-bind-address` | — | `:8081` | Health/readiness probe bind address. |
| `--leader-elect` | — | `false` | Enable leader election (for multi-replica deployments). |
| `--webhook-cert-path` / `--webhook-cert-name` / `--webhook-cert-key` | — | *(empty)* / `tls.crt` / `tls.key` | Webhook server TLS certificate (unused today — no webhooks are registered — reserved for future use). |
| `--metrics-cert-path` / `--metrics-cert-name` / `--metrics-cert-key` | — | *(empty)* / `tls.crt` / `tls.key` | Metrics server TLS certificate. |
| `--enable-http2` | — | `false` | Enable HTTP/2 on the metrics/webhook servers (disabled by default due to known CVEs). |

`CHWriter`'s queue size, worker count, per-attempt retry backoff cap, and shutdown drain timeout currently default internally (queue: 5000 jobs, workers: 4, max retry backoff: 60s, shutdown drain: 15s) and are not yet exposed as flags/env vars.

Standard `controller-runtime`/Zap logging flags (`--zap-devel`, `--zap-encoder`, `--zap-log-level`, `--zap-stacktrace-level`, `--zap-time-encoding`) are also available; run the binary with `--help` for the full, exact list.

## Metrics

kubestream registers the following pipeline metrics on controller-runtime's global Prometheus registry, so they are served by the existing metrics endpoint (`--metrics-bind-address`). All names are prefixed `kubestream_`.

| Metric | Type | Labels | Description |
|---|---|---|---|
| `kubestream_write_queue_depth` | Gauge | — | Jobs currently buffered in the `CHWriter` hand-off queue. |
| `kubestream_write_queue_capacity` | Gauge | — | Maximum jobs the hand-off queue can buffer. |
| `kubestream_writes_total` | Counter | `outcome="success"\|"failed"` | Settled ClickHouse write jobs, by outcome. |
| `kubestream_write_latency_seconds` | Histogram | — | Time from a job's first write attempt to its final settle (incl. retries). |
| `kubestream_write_retry_attempts_total` | Counter | — | Write attempts beyond the first (i.e. retries), across all jobs. |
| `kubestream_enqueue_block_seconds` | Histogram | — | Time `Enqueue` spent blocked waiting for queue room. |
| `kubestream_enqueue_timeouts_total` | Counter | — | `Enqueue` calls that gave up because the queue stayed full past the timeout. |
| `kubestream_dedup_skips_total` | Counter | — | Reconciles short-circuited because the object's hash was unchanged. |
| `kubestream_hashcache_entries` | Gauge | `kind` | Live `hashCache` entries, per kind. |
| `kubestream_safe_mode` | Gauge (0/1) | `kind` | `1` while a kind's cache is still warming (Snapshot mode), `0` once warm. |
| `kubestream_requeue_drops_total` | Counter | — | Re-reconcile triggers dropped because the requeue channel was full. |

## Getting Started

### Prerequisites

- Go v1.25+ (see `go.mod`)
- Docker (or another `CONTAINER_TOOL`) for building the operator image
- `kubectl`, and access to a Kubernetes cluster
- A reachable ClickHouse instance, with the schema v1 tables created — either apply [`deploy/clickhouse/schema/*.sql`](deploy/clickhouse/schema/) yourself or start the operator with `--ch-auto-create-schema=true` (see [ClickHouse schema](#clickhouse-schema))

This project does not define a CRD — there is nothing to `make install` beyond the operator itself; it only watches existing built-in resource types (or others you configure via `WATCHED_GVKS`).

### Deploying to a cluster

1. **Build and push the operator image:**

   ```sh
   make docker-build docker-push IMG=<some-registry>/kubestream:tag
   ```

2. **Set the real ClickHouse password.** `config/manager/clickhouse-secret.yaml` ships with a `changeme` placeholder — replace it before applying, e.g.:

   ```sh
   kubectl create secret generic clickhouse-credentials \
     --namespace kubestream-system \
     --from-literal=password='<your-password>' \
     --dry-run=client -o yaml | kubectl apply -f -
   ```

   (For a real deployment, prefer a Kustomize `SecretGenerator` overlay or a secret-management tool — Sealed Secrets, External Secrets, SOPS — over committing a plaintext value.)

3. **Point the deployment at your ClickHouse instance and cluster identifier.** Edit the `env` block in `config/manager/manager.yaml` (`CH_ADDR`, `CH_DATABASE`, `CH_USERNAME`, `CLUSTER_ID`, etc.) — the checked-in values are local/dev placeholders.

4. **Deploy:**

   ```sh
   make deploy IMG=<some-registry>/kubestream:tag
   ```

### Uninstalling

```sh
make undeploy
```

## Local Development

This project is scaffolded with [Kubebuilder](https://book.kubebuilder.io/) and uses its standard Makefile targets:

```sh
make build          # go build the manager binary
make run            # run the controller locally against your current kubeconfig context
make test           # run the unit/envtest suite (requires the envtest/etcd binaries; make setup-envtest fetches them)
make lint           # run golangci-lint (see .golangci.yml for the enabled linters)
make lint-fix       # run golangci-lint with --fix
make fmt vet        # gofmt + go vet
```

`make test` runs both the pure-Go unit tests (e.g. `internal/controller/hashcache_test.go`, `cmd/main_test.go`) and the Ginkgo/envtest-based suite in `internal/controller/suite_test.go`, which spins up a real (test-only) API server via `envtest` — no live cluster is required for it, but the envtest binaries must be present locally (`make setup-envtest`).

Run `make help` for the full list of available targets (image building, Kustomize install/deploy, dependency downloads, etc.).

## License

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
