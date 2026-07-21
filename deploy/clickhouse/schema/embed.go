// Package schema embeds the shipped ClickHouse DDL so the operator can create
// the tables idempotently when --ch-auto-create-schema is set. Embedding the
// reviewed .sql files — rather than duplicating the DDL as Go string literals —
// guarantees the statements the operator executes are byte-identical to the
// files that are documented in docs/SCHEMA.md and frozen as a public API in
// Task 2.6, so the two can never silently drift apart.
package schema

import "embed"

// FS holds the versioned ClickHouse DDL files (NNN_*.sql). They are applied in
// filename order, which is why each is prefixed with a zero-padded sequence
// number; every statement is CREATE TABLE IF NOT EXISTS, so applying them is
// idempotent and safe to repeat on every operator start.
//
//go:embed *.sql
var FS embed.FS
