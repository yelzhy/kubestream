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

package clickhouse

import (
	"context"
	"fmt"

	"github.com/yelzhy/kubestream/internal/sink"
)

// lastKnownStatesQuery renders the resource_states warm-up query for filter.
//
// Filtering on api_group as well as kind keeps two different resources that
// share a Kind (e.g. batch/v1 Job vs. a CRD example.com/v1 Job) from
// cross-contaminating each other's warm-up history — the schema-v1 identity is
// (cluster_id, api_group, kind, namespace, name). This is the ClickHouse-side
// mirror of the in-process cacheKey builder, which keys on
// (api_group, kind, namespace, name) for the same reason.
//
// A non-empty filter.Namespace narrows to that namespace; an empty one matches
// every namespace (the GVK-wide scope today's warm-up uses), so the emitted SQL
// is identical to the original inline query when Namespace is unset.
func lastKnownStatesQuery(filter sink.ScopeFilter) (string, []any) {
	query := `
        SELECT namespace, name, argMax(uid, ts), argMax(sha256, ts)
        FROM resource_states
        WHERE api_group = ? AND kind = ? AND cluster_id = ?`
	args := []any{filter.APIGroup, filter.Kind, filter.ClusterID}
	if filter.Namespace != "" {
		query += `
        AND namespace = ?`
		args = append(args, filter.Namespace)
	}
	query += `
        GROUP BY namespace, name
        HAVING argMax(event_type, ts) != 'Deleted'`
	return query, args
}

// LastKnownStates implements sink.StateReader against the shared ClickHouse
// connection. It reports, per scope, the last-known (uid, sha256) of every
// object whose most recent event is not a deletion — exactly what a cache
// warm-up needs to reconstruct its dedup baseline without re-emitting live
// objects.
//
// The call is registered in otherUsers under the closing check so Start never
// closes conn while a query is in flight (see CHWriter.otherUsers). A
// mid-stream read failure (the connection dropping after some rows) surfaces
// via rows.Err(), not Next(); it is returned as an error rather than silently
// treated as a short-but-complete result, so a caller relying on completeness
// (the warm-up) retries the whole scan instead of trusting a partial one.
func (w *CHWriter) LastKnownStates(ctx context.Context, filter sink.ScopeFilter) ([]sink.KnownState, error) {
	w.mu.Lock()
	if w.closing {
		w.mu.Unlock()
		return nil, fmt.Errorf("chwriter: shutting down, refusing state read")
	}
	w.otherUsers.Add(1)
	w.mu.Unlock()
	defer w.otherUsers.Done()

	query, args := lastKnownStatesQuery(filter)
	rows, err := w.conn.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	var states []sink.KnownState
	for rows.Next() {
		var st sink.KnownState
		if err := rows.Scan(&st.Namespace, &st.Name, &st.UID, &st.SHA256); err != nil {
			return nil, err
		}
		states = append(states, st)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return states, nil
}
