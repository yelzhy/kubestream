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
	"crypto/sha256"
	"encoding/json"
	"reflect"
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

// managedField builds a single managedFields entry with the given manager name,
// mirroring the shape the API server produces (only the manager key matters to
// extractActors, but the surrounding keys keep the fixtures realistic).
func managedField(manager string) map[string]any {
	return map[string]any{
		"manager":    manager,
		"operation":  "Update",
		"apiVersion": "v1",
	}
}

func TestExtractActors(t *testing.T) {
	tests := []struct {
		name          string
		managedFields []any // nil means the key is absent entirely
		absent        bool
		want          []string
	}{
		{
			name:   "absent managedFields yields empty slice",
			absent: true,
			want:   []string{},
		},
		{
			name:          "nil managedFields yields empty slice",
			managedFields: nil,
			want:          []string{},
		},
		{
			name:          "empty managedFields yields empty slice",
			managedFields: []any{},
			want:          []string{},
		},
		{
			name: "single manager",
			managedFields: []any{
				managedField("argocd-controller"),
			},
			want: []string{"argocd-controller"},
		},
		{
			name: "duplicate managers are deduped",
			managedFields: []any{
				managedField("kubectl-client-side-apply"),
				managedField("kubectl-client-side-apply"),
				managedField("kubectl-client-side-apply"),
			},
			want: []string{"kubectl-client-side-apply"},
		},
		{
			name: "mixed managers are sorted",
			managedFields: []any{
				managedField("kube-controller-manager"),
				managedField("argocd-controller"),
				managedField("kubectl-client-side-apply"),
			},
			want: []string{"argocd-controller", "kube-controller-manager", "kubectl-client-side-apply"},
		},
		{
			name: "empty manager maps to unknown",
			managedFields: []any{
				managedField(""),
			},
			want: []string{"unknown"},
		},
		{
			name: "missing manager key maps to unknown",
			managedFields: []any{
				map[string]any{"operation": "Update"},
			},
			want: []string{"unknown"},
		},
		{
			name: "non-string manager maps to unknown",
			managedFields: []any{
				map[string]any{"manager": 42},
			},
			want: []string{"unknown"},
		},
		{
			name: "empty and named managers coexist",
			managedFields: []any{
				managedField("argocd-controller"),
				managedField(""),
			},
			want: []string{"argocd-controller", "unknown"},
		},
		{
			name: "malformed non-map entry is skipped, others harvested",
			managedFields: []any{
				managedField("argocd-controller"),
				"i am not a map",
				managedField("kube-controller-manager"),
			},
			want: []string{"argocd-controller", "kube-controller-manager"},
		},
		{
			name: "only malformed entries yield empty slice",
			managedFields: []any{
				"not a map",
				12345,
			},
			want: []string{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			obj := &unstructured.Unstructured{Object: map[string]any{
				"apiVersion": "v1",
				"kind":       "Pod",
				"metadata":   map[string]any{"name": "example"},
			}}
			if !tt.absent {
				meta := obj.Object["metadata"].(map[string]any)
				meta["managedFields"] = tt.managedFields
			}

			got := extractActors(obj)
			if got == nil {
				t.Fatalf("extractActors returned nil, want non-nil slice")
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("extractActors() = %v, want %v", got, tt.want)
			}
		})
	}
}

// TestExtractActorsDoesNotMutate proves extractActors only reads the object:
// managedFields must still be present afterwards, so Reconcile — which relies
// on stripping it itself, immediately after — sees an unperturbed object.
func TestExtractActorsDoesNotMutate(t *testing.T) {
	obj := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "v1",
		"kind":       "Pod",
		"metadata": map[string]any{
			"name": "example",
			"managedFields": []any{
				managedField("argocd-controller"),
			},
		},
	}}

	_ = extractActors(obj)

	if _, found, _ := unstructured.NestedSlice(obj.Object, "metadata", "managedFields"); !found {
		t.Fatalf("extractActors removed managedFields; it must only read the object")
	}
}

// normalizedHash reproduces Reconcile's normalization + hashing so the
// regression test below asserts on exactly the bytes Reconcile would hash.
func normalizedHash(t *testing.T, obj *unstructured.Unstructured) string {
	t.Helper()
	unstructured.RemoveNestedField(obj.Object, "metadata", "managedFields")
	unstructured.RemoveNestedField(obj.Object, "metadata", "resourceVersion")
	unstructured.RemoveNestedField(obj.Object, "metadata", "generation")
	objJSON, err := json.Marshal(obj.Object)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	sum := sha256.Sum256(objJSON)
	return string(sum[:])
}

// TestManagedFieldsDoNotPerturbHash is the regression guard for the core
// invariant of this task: harvesting actors must not change what gets hashed.
// An object with managedFields (after extraction + stripping) must hash
// identically to the same object that never carried them.
func TestManagedFieldsDoNotPerturbHash(t *testing.T) {
	withManagedFields := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "v1",
		"kind":       "Pod",
		"metadata": map[string]any{
			"name":      "example",
			"namespace": "default",
			"managedFields": []any{
				managedField("kubectl-client-side-apply"),
				managedField("kube-controller-manager"),
			},
		},
		"spec": map[string]any{"nodeName": "node-1"},
	}}
	without := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "v1",
		"kind":       "Pod",
		"metadata": map[string]any{
			"name":      "example",
			"namespace": "default",
		},
		"spec": map[string]any{"nodeName": "node-1"},
	}}

	// Extraction runs first on the managedFields variant, exactly as Reconcile
	// orders it — proving the extraction step itself introduces no perturbation.
	if actors := extractActors(withManagedFields); len(actors) != 2 {
		t.Fatalf("expected 2 actors from fixture, got %v", actors)
	}

	hashWith := normalizedHash(t, withManagedFields)
	hashWithout := normalizedHash(t, without)
	if hashWith != hashWithout {
		t.Errorf("hash differs with vs. without managedFields: extraction perturbed normalization")
	}
}
