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

package main

import (
	"testing"

	"k8s.io/apimachinery/pkg/runtime/schema"
)

func TestParseGVKListDefaultSet(t *testing.T) {
	gvks, err := parseGVKList(defaultWatchedGVKs)
	if err != nil {
		t.Fatalf("unexpected error parsing the default GVK list: %v", err)
	}
	want := []schema.GroupVersionKind{
		{Group: "", Version: "v1", Kind: "Pod"},
		{Group: "apps", Version: "v1", Kind: "Deployment"},
		{Group: "", Version: "v1", Kind: "Service"},
	}
	if len(gvks) != len(want) {
		t.Fatalf("got %d GVKs, want %d: %+v", len(gvks), len(want), gvks)
	}
	for i, g := range gvks {
		if g != want[i] {
			t.Fatalf("gvks[%d] = %+v, want %+v", i, g, want[i])
		}
	}
}

func TestParseGVKListCoreAndNonCoreGroups(t *testing.T) {
	gvks, err := parseGVKList("v1/Pod, networking.k8s.io/v1/Ingress ,apps/v1/Deployment")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	want := []schema.GroupVersionKind{
		{Group: "", Version: "v1", Kind: "Pod"},
		{Group: "networking.k8s.io", Version: "v1", Kind: "Ingress"},
		{Group: "apps", Version: "v1", Kind: "Deployment"},
	}
	if len(gvks) != len(want) {
		t.Fatalf("got %d GVKs, want %d: %+v", len(gvks), len(want), gvks)
	}
	for i, g := range gvks {
		if g != want[i] {
			t.Fatalf("gvks[%d] = %+v, want %+v", i, g, want[i])
		}
	}
}

func TestParseGVKListSkipsBlankEntries(t *testing.T) {
	gvks, err := parseGVKList(" , v1/Pod ,, ")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(gvks) != 1 || gvks[0] != (schema.GroupVersionKind{Version: "v1", Kind: "Pod"}) {
		t.Fatalf("expected exactly one Pod GVK with blanks skipped, got %+v", gvks)
	}
}

func TestParseGVKListEmptyInputYieldsEmptyList(t *testing.T) {
	gvks, err := parseGVKList("")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(gvks) != 0 {
		t.Fatalf("expected an empty list for empty input, got %+v", gvks)
	}
}

func TestParseGVKListRejectsWrongSegmentCount(t *testing.T) {
	for _, raw := range []string{"Pod", "a/b/c/d", "/"} {
		if _, err := parseGVKList(raw); err == nil {
			t.Fatalf("expected an error for malformed entry %q, got none", raw)
		}
	}
}

func TestParseGVKListRejectsEmptyVersionOrKind(t *testing.T) {
	for _, raw := range []string{"/Pod", "v1/", "apps//Deployment"} {
		if _, err := parseGVKList(raw); err == nil {
			t.Fatalf("expected an error for entry with empty version/kind %q, got none", raw)
		}
	}
}
