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
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	ctrl "sigs.k8s.io/controller-runtime/pkg/reconcile"

	"github.com/yelzhy/kubestream/internal/sink"
)

// This spec is the Invariant 2 (per-key serialization) regression guard for
// Fix 1 (Task 0.9). It MUST NOT be deleted or weakened. When the Phase 1
// workqueue pipeline introduces genuine per-key concurrency, this test should
// be *ported* to assert the pipeline's per-key serialization contract — not
// removed — because the property it protects (each emitted Modified diff is
// computed against the immediately-preceding emitted state, never a stale
// shared baseline) is exactly what per-key serialization exists to guarantee.
//
// The current Reconcile has no per-key lock; controller-runtime provides the
// serialization, which is why RECONCILER_MAX_CONCURRENT must stay 1. This test
// drives the reconciles in the serialized order that guarantee provides and
// asserts the emitted timeline is consistent. If two same-key Reconciles were
// allowed to run their read-decide-write sections concurrently (i.e.
// MaxConcurrentReconciles > 1 against the non-locked Reconcile), both could
// Load the same committed baseline A and emit diff(A→B) and diff(A→C): the
// second emitted diff would be an "add" of the label against A rather than the
// "replace" against B this test asserts, breaking the timeline.
var _ = Describe("ResourceStream Controller per-key serialization (Invariant 2 guard)", func() {
	podGVK := schema.GroupVersionKind{Group: "", Version: "v1", Kind: "Pod"}

	It("emits each Modified diff against the immediately-preceding state, never a shared baseline", func() {
		writer := &capturingWriter{captured: make(chan sink.Record, 8)}

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		reconciler := &ResourceStreamReconciler{
			Client:    k8sClient,
			Scheme:    k8sClient.Scheme(),
			Writer:    writer,
			GVK:       podGVK,
			ClusterID: "test-cluster",
			requeueCh: make(chan event.GenericEvent, 1),
			metrics:   PipelineMetricsInstance(),
		}

		// State A: a pod with no "step" label.
		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: "serialization-pod", Namespace: "default"},
			Spec:       corev1.PodSpec{Containers: []corev1.Container{{Name: "c", Image: "busybox"}}},
		}
		Expect(k8sClient.Create(ctx, pod)).To(Succeed())
		DeferCleanup(func() {
			Expect(client.IgnoreNotFound(k8sClient.Delete(context.Background(), pod))).To(Succeed())
		})

		req := ctrl.Request{NamespacedName: client.ObjectKey{Namespace: pod.Namespace, Name: pod.Name}}

		// Reconcile A → seeds the diff baseline (event_type Added).
		_, err := reconciler.Reconcile(ctx, req)
		Expect(err).NotTo(HaveOccurred())
		var added sink.Record
		Eventually(writer.captured, 5*time.Second).Should(Receive(&added))
		Expect(added.EventType).To(Equal("Added"))

		// State B: add the "step" label (value "b").
		setStepLabel(ctx, req.NamespacedName, "b")
		_, err = reconciler.Reconcile(ctx, req)
		Expect(err).NotTo(HaveOccurred())
		var modB sink.Record
		Eventually(writer.captured, 5*time.Second).Should(Receive(&modB))
		Expect(modB.EventType).To(Equal("Modified"))
		// State A had no labels at all, so A→B "adds" the whole labels map.
		Expect(modB.Diff).To(ContainSubstring(`"op":"add"`))
		Expect(modB.Diff).To(ContainSubstring(`{"step":"b"}`))

		// State C: change the existing "step" label to "c".
		setStepLabel(ctx, req.NamespacedName, "c")
		_, err = reconciler.Reconcile(ctx, req)
		Expect(err).NotTo(HaveOccurred())
		var modC sink.Record
		Eventually(writer.captured, 5*time.Second).Should(Receive(&modC))
		Expect(modC.EventType).To(Equal("Modified"))

		// The timeline-consistency assertion: because B→C was computed against
		// the immediately-preceding emitted state B (which already had the label),
		// the patch REPLACES the existing key. Had it been computed against the
		// shared baseline A (which lacked the label — the concurrency hazard this
		// guards against), the patch would instead ADD the key. So a "replace"
		// proves the predecessor was B, not A.
		Expect(modC.Diff).To(ContainSubstring(`"op":"replace"`))
		Expect(modC.Diff).To(ContainSubstring(`"value":"c"`))
		Expect(modC.Diff).NotTo(ContainSubstring(`"op":"add"`))
	})
})

// setStepLabel fetches the named pod and sets metadata.labels["step"] to value,
// so each reconcile observes a distinct, controlled state transition.
func setStepLabel(ctx context.Context, key client.ObjectKey, value string) {
	fetched := &corev1.Pod{}
	Expect(k8sClient.Get(ctx, key, fetched)).To(Succeed())
	if fetched.Labels == nil {
		fetched.Labels = map[string]string{}
	}
	fetched.Labels["step"] = value
	Expect(k8sClient.Update(ctx, fetched)).To(Succeed())
}
