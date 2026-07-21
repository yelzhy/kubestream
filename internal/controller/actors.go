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
	"maps"
	"slices"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
)

// extractActors harvests the distinct field-manager names from an object's
// metadata.managedFields — the cheapest available "who probably changed this"
// signal and the backbone of the GitOps-drift story (kubectl-client-side-apply,
// argocd-controller, kube-controller-manager, …). It must be called before
// Reconcile strips managedFields, and it only reads the object: it never
// mutates obj.Object, so the subsequent normalization + hashing is unaffected.
//
// The returned slice is de-duplicated and sorted for determinism (so an
// unchanged actor set never produces a spurious diff downstream), with empty
// manager names mapped to "unknown". A non-map entry in managedFields is
// skipped rather than failing the whole extraction; if any are seen they are
// logged once per call (never once per entry) so a malformed object degrades to
// a partial actor set instead of a silent error or a log storm. The result is
// always non-nil (empty slice when there is nothing to harvest).
//
//nolint:logcheck
func extractActors(obj *unstructured.Unstructured) []string {
	// NestedFieldNoCopy, not NestedSlice: the latter deep-copies the whole
	// slice (needless work on the hot path) and panics on any non-JSON value
	// it encounters, which would turn a single malformed managedFields entry
	// into a crash rather than the graceful skip this function guarantees. We
	// only read, so a no-copy view is both cheaper and safer.
	raw, found, err := unstructured.NestedFieldNoCopy(obj.Object, "metadata", "managedFields")
	if err != nil || !found {
		return []string{}
	}
	managedFields, ok := raw.([]any)
	if !ok || len(managedFields) == 0 {
		return []string{}
	}

	seen := make(map[string]struct{}, len(managedFields))
	malformed := 0
	for _, entry := range managedFields {
		m, ok := entry.(map[string]any)
		if !ok {
			malformed++
			continue
		}
		// A missing, non-string, or empty manager all collapse to "unknown":
		// the row still records that *something* touched the object even when
		// the field manager can't be named.
		manager, _ := m["manager"].(string)
		if manager == "" {
			manager = "unknown"
		}
		seen[manager] = struct{}{}
	}

	if malformed > 0 {
		logf.Log.WithName("actors").Error(nil, "skipped malformed managedFields entries while extracting actors",
			"kind", obj.GetKind(), "namespace", obj.GetNamespace(), "name", obj.GetName(), "skipped", malformed)
	}

	if len(seen) == 0 {
		return []string{}
	}
	return slices.Sorted(maps.Keys(seen))
}
