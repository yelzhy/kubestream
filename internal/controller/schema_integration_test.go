//go:build integration

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
	"os"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
)

// TestSchemaRoundTripIntegration proves a real ClickHouse round-trip on the
// schema-v1 tables: auto-create the shipped DDL, validate it, insert a row via
// the exact writer query/args, then read it back through the warm-up query.
// Runs only under `make test-integration` (build tag `integration`), which
// stands up a dockerized ClickHouse and points CH_TEST_ADDR at it.
func TestSchemaRoundTripIntegration(t *testing.T) {
	addr := envOrDefault("CH_TEST_ADDR", "127.0.0.1:9000")
	username := envOrDefault("CH_TEST_USER", "default")
	password := os.Getenv("CH_TEST_PASSWORD")
	database := envOrDefault("CH_TEST_DB", "default")

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr:        []string{addr},
		Auth:        clickhouse.Auth{Database: database, Username: username, Password: password},
		Protocol:    clickhouse.Native,
		DialTimeout: 5 * time.Second,
		ReadTimeout: 10 * time.Second,
	})
	if err != nil {
		t.Fatalf("open connection: %v", err)
	}
	// Drop the throwaway tables on the way out so a persistent ClickHouse can be
	// re-targeted cleanly; the dockerized default target is discarded anyway.
	defer func() {
		_ = conn.Exec(context.Background(), "DROP TABLE IF EXISTS "+tableResourceStates)
		_ = conn.Exec(context.Background(), "DROP TABLE IF EXISTS "+tableWatchScopes)
		_ = conn.Close()
	}()

	// 1. Auto-create the shipped DDL (idempotent).
	if err := autoCreateSchema(ctx, conn); err != nil {
		t.Fatalf("autoCreateSchema: %v", err)
	}
	// Running it twice must be a no-op, proving idempotency.
	if err := autoCreateSchema(ctx, conn); err != nil {
		t.Fatalf("autoCreateSchema (second run): %v", err)
	}

	// 2. Validate the live schema matches schema v1 exactly.
	if err := validateSchema(ctx, conn, database); err != nil {
		t.Fatalf("validateSchema: %v", err)
	}

	// 3. Insert one row via the exact writer query and positional args.
	rec := ResourceRecord{
		Timestamp:       time.Now().UTC(),
		ClusterID:       "it-cluster",
		EventType:       "Added",
		APIGroup:        "apps",
		APIVersion:      "v1",
		Kind:            "Deployment",
		Namespace:       "default",
		Name:            "roundtrip",
		UID:             "uid-roundtrip",
		ResourceVersion: "123",
		Labels:          map[string]string{"app": "demo"},
		Data:            `{"kind":"Deployment"}`,
		SHA256:          "abc123",
	}
	if err := conn.Exec(ctx, insertResourceStateQuery, rec.insertArgs()...); err != nil {
		t.Fatalf("insert row: %v", err)
	}

	// 4. Read it back via the warm-up query shape (api_group + kind + cluster_id).
	rows, err := conn.Query(ctx, `
        SELECT namespace, name, argMax(uid, ts), argMax(sha256, ts)
        FROM resource_states
        WHERE api_group = ? AND kind = ? AND cluster_id = ?
        GROUP BY namespace, name
        HAVING argMax(event_type, ts) != 'Deleted'
    `, rec.APIGroup, rec.Kind, rec.ClusterID)
	if err != nil {
		t.Fatalf("warm query: %v", err)
	}
	defer func() { _ = rows.Close() }()

	var got int
	for rows.Next() {
		var namespace, name, uid, sha string
		if err := rows.Scan(&namespace, &name, &uid, &sha); err != nil {
			t.Fatalf("scan: %v", err)
		}
		got++
		if namespace != rec.Namespace || name != rec.Name || uid != rec.UID || sha != rec.SHA256 {
			t.Errorf("row mismatch: got (%s/%s uid=%s sha=%s), want (%s/%s uid=%s sha=%s)",
				namespace, name, uid, sha, rec.Namespace, rec.Name, rec.UID, rec.SHA256)
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows error: %v", err)
	}
	if got != 1 {
		t.Fatalf("expected exactly 1 warm-query row, got %d", got)
	}
}

// envOrDefault returns the named environment variable's value, or def if unset.
func envOrDefault(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
