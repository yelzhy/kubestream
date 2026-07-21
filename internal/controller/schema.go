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
	"errors"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"sync/atomic"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/cenkalti/backoff/v4"
	"github.com/go-logr/logr"

	schemaddl "github.com/yelzhy/kubestream/deploy/clickhouse/schema"
)

const (
	tableResourceStates = "resource_states"
	tableWatchScopes    = "watch_scopes"
)

// errSchemaMismatch gives the schema-mismatch log lines a non-nil error value
// to log against (mirroring errAsyncWriteFailed); the specific discrepancies
// are carried as structured key/value context, not in this sentinel.
var errSchemaMismatch = errors.New("clickhouse schema does not match the expected schema v1")

// schemaColumn is one required column and the exact type system.columns must
// report for it. The type strings deliberately omit the CODEC clauses present
// in the DDL — codecs are a storage-level detail ClickHouse does not surface in
// system.columns.type, so validating against them would always spuriously fail.
type schemaColumn struct {
	name   string
	chType string
}

// requiredColumns is the connect-time contract: every column the operator
// depends on, per table, with the type system.columns is expected to report.
// It is kept in lockstep with the shipped DDL (deploy/clickhouse/schema); a
// live table that drifts from this degrades readiness rather than letting the
// operator write rows that would silently mismatch the frozen public schema.
var requiredColumns = map[string][]schemaColumn{
	tableResourceStates: {
		{"ts", "DateTime64(9, 'UTC')"},
		{"cluster_id", "LowCardinality(String)"},
		{"event_type", "LowCardinality(String)"},
		{"api_group", "LowCardinality(String)"},
		{"api_version", "LowCardinality(String)"},
		{"kind", "LowCardinality(String)"},
		{"namespace", "String"},
		{"name", "String"},
		{"uid", "String"},
		{"resource_version", "String"},
		{"labels", "Map(LowCardinality(String), String)"},
		{"actors", "Array(LowCardinality(String))"},
		{"data", "String"},
		{"diff", "String"},
		{"sha256", "String"},
	},
	tableWatchScopes: {
		{"ts", "DateTime64(9, 'UTC')"},
		{"cluster_id", "LowCardinality(String)"},
		{"api_group", "LowCardinality(String)"},
		{"api_version", "LowCardinality(String)"},
		{"kind", "LowCardinality(String)"},
		{"namespace", "String"},
		{"action", "LowCardinality(String)"},
		{"rule_ref", "String"},
	},
}

// schemaMismatchError is returned by validateSchema when the live schema is
// readable but does not match requiredColumns. It is distinct from a transient
// query error so the retry loop can stop (a mismatch will not fix itself) while
// still retrying a ClickHouse that is merely unreachable. Each discrepancy
// names the offending table/column so operators can pinpoint the drift.
type schemaMismatchError struct {
	discrepancies []string
}

func (e *schemaMismatchError) Error() string {
	return fmt.Sprintf("clickhouse schema mismatch: %s", strings.Join(e.discrepancies, "; "))
}

// introspectColumns reads system.columns for both operator tables in one
// round-trip and returns table -> (column -> reported type). A table that does
// not exist simply yields no rows and therefore no entry in the outer map.
func introspectColumns(ctx context.Context, conn driver.Conn, database string) (map[string]map[string]string, error) {
	rows, err := conn.Query(ctx, `
        SELECT table, name, type
        FROM system.columns
        WHERE database = ? AND table IN (?, ?)
    `, database, tableResourceStates, tableWatchScopes)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	observed := make(map[string]map[string]string)
	for rows.Next() {
		var table, name, chType string
		if err := rows.Scan(&table, &name, &chType); err != nil {
			return nil, err
		}
		if observed[table] == nil {
			observed[table] = make(map[string]string)
		}
		observed[table][name] = chType
	}
	// A mid-stream error surfaces here, not from Next(); treating a partial read
	// as a valid (short) schema would be exactly the silent corruption this
	// validation exists to prevent.
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return observed, nil
}

// validateSchema verifies the live ClickHouse schema against requiredColumns.
// A nil return means every required column of both tables is present with the
// expected type. A *schemaMismatchError names each discrepancy. Any other
// error is transient (ClickHouse unreachable, query failed) and the caller
// should retry rather than degrade readiness.
func validateSchema(ctx context.Context, conn driver.Conn, database string) error {
	observed, err := introspectColumns(ctx, conn, database)
	if err != nil {
		return err
	}

	var discrepancies []string
	// Deterministic table order so log output and error messages are stable.
	for _, table := range []string{tableResourceStates, tableWatchScopes} {
		cols := observed[table]
		if cols == nil {
			discrepancies = append(discrepancies, fmt.Sprintf("table %q is missing", table))
			continue
		}
		for _, req := range requiredColumns[table] {
			got, ok := cols[req.name]
			if !ok {
				discrepancies = append(discrepancies,
					fmt.Sprintf("table %q is missing column %q (expected type %s)", table, req.name, req.chType))
				continue
			}
			if got != req.chType {
				discrepancies = append(discrepancies,
					fmt.Sprintf("table %q column %q has type %s, expected %s", table, req.name, got, req.chType))
			}
		}
	}

	if len(discrepancies) > 0 {
		return &schemaMismatchError{discrepancies: discrepancies}
	}
	return nil
}

// autoCreateSchema executes the shipped DDL files in filename order. Every
// statement is CREATE TABLE IF NOT EXISTS, so this is idempotent and safe to
// run on every start. Only invoked when --ch-auto-create-schema is set; the
// default remains "the operator never mutates ClickHouse DDL on its own".
func autoCreateSchema(ctx context.Context, conn driver.Conn) error {
	entries, err := schemaddl.FS.ReadDir(".")
	if err != nil {
		return err
	}
	names := make([]string, 0, len(entries))
	for _, e := range entries {
		if !e.IsDir() && strings.HasSuffix(e.Name(), ".sql") {
			names = append(names, e.Name())
		}
	}
	sort.Strings(names)

	for _, name := range names {
		ddl, err := schemaddl.FS.ReadFile(name)
		if err != nil {
			return err
		}
		// The native protocol executes a single statement per Exec; trimming a
		// trailing ';' avoids an empty second statement being parsed.
		stmt := strings.TrimRight(strings.TrimSpace(string(ddl)), ";")
		if err := conn.Exec(ctx, stmt); err != nil {
			return fmt.Errorf("executing %s: %w", name, err)
		}
	}
	return nil
}

// Schema-gate states. Readiness degrades only on a confirmed mismatch; while
// the schema is still being probed (or ClickHouse is briefly unreachable at
// boot) the gate stays "unknown" and does not hold readiness down — mirroring
// restoreAndWarm's tolerance of a slow ClickHouse at startup.
const (
	schemaUnknown int32 = iota
	schemaValid
	schemaInvalid
)

// schemaGate is the connect-time schema-validation result consulted by the
// "clickhouse-schema" readyz check. It is written once by the validation
// runnable and read by the probe endpoint, so an atomic is sufficient — no
// lock, no blocking on the hot path.
type schemaGate struct {
	state atomic.Int32
}

func newSchemaGate() *schemaGate { return &schemaGate{} }

func (g *schemaGate) setValid()   { g.state.Store(schemaValid) }
func (g *schemaGate) setInvalid() { g.state.Store(schemaInvalid) }

// readyCheck degrades readiness when, and only when, the live schema has been
// confirmed to mismatch schema v1. Returning an error here flips the
// "clickhouse-schema" readyz probe to not-ready so the mismatch is visible to
// Kubernetes and operators, without crash-looping the process.
func (g *schemaGate) readyCheck(*http.Request) error {
	if g.state.Load() == schemaInvalid {
		return errors.New("clickhouse schema validation failed; see operator logs for the offending columns")
	}
	return nil
}

// validateSchemaWithRetry optionally auto-creates the schema, then validates it,
// updating gate with the outcome. It runs as a background manager.Runnable so
// mgr.Start is never gated on ClickHouse being reachable at boot (like
// restoreAndWarm). Transient errors (ClickHouse unreachable) are retried with
// bounded backoff until ctx is cancelled; a definitive result — valid schema or
// a confirmed mismatch — ends the loop, since neither will change without
// operator action.
//
//nolint:logcheck
func validateSchemaWithRetry(ctx context.Context, conn driver.Conn, database string, autoCreate bool, gate *schemaGate, log logr.Logger) {
	newBackoff := func() *backoff.ExponentialBackOff {
		eb := backoff.NewExponentialBackOff()
		eb.MaxInterval = 30 * time.Second
		eb.MaxElapsedTime = 0 // retry forever — only ctx cancellation gives up
		return eb
	}

	if autoCreate {
		err := backoff.Retry(func() error {
			if err := autoCreateSchema(ctx, conn); err != nil {
				log.Error(err, "⚠️ Failed to auto-create ClickHouse schema, retrying")
				return err
			}
			return nil
		}, backoff.WithContext(newBackoff(), ctx))
		if err != nil {
			return // ctx cancelled before the DDL could be applied
		}
		log.Info("🗄️ ClickHouse schema auto-create applied (idempotent)")
	}

	var mismatch *schemaMismatchError
	err := backoff.Retry(func() error {
		verr := validateSchema(ctx, conn, database)
		if verr == nil {
			return nil
		}
		// A mismatch will not resolve on its own — stop retrying and report it.
		if errors.As(verr, &mismatch) {
			return backoff.Permanent(verr)
		}
		log.Error(verr, "⚠️ Failed to introspect ClickHouse schema, retrying")
		return verr
	}, backoff.WithContext(newBackoff(), ctx))

	if err != nil {
		if errors.As(err, &mismatch) {
			for _, d := range mismatch.discrepancies {
				log.Error(errSchemaMismatch, "❌ ClickHouse schema mismatch", "detail", d)
			}
			gate.setInvalid()
			return
		}
		return // ctx cancelled before validation completed
	}

	gate.setValid()
	log.Info("✅ ClickHouse schema validated against schema v1")
}
