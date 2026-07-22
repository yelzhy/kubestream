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
	"slices"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	ctrl "sigs.k8s.io/controller-runtime/pkg/reconcile"

	"github.com/yelzhy/kubestream/internal/sink"
)

// capturingWriter is a sink.Writer that records the Record of every enqueued
// job, so the integration test can inspect exactly what Reconcile produced
// without a real sink. It settles each job immediately as a success so the
// reconciler's version-gated cache commit fires just as it would in production.
type capturingWriter struct {
	captured chan sink.Record
}

func (w *capturingWriter) Start(ctx context.Context) error {
	<-ctx.Done()
	return nil
}

func (w *capturingWriter) Enqueue(_ context.Context, job sink.Job) error {
	w.captured <- job.Record
	if job.Commit != nil {
		job.Commit(true)
	}
	return nil
}

var _ = Describe("ResourceStream Controller", func() {
	Context("When reconciling a resource with managedFields", func() {
		It("enqueues a record whose actors match the applied object's field managers", func() {
			podGVK := schema.GroupVersionKind{Group: "", Version: "v1", Kind: "Pod"}

			// A capturing writer stands in for the sink so we can read back the
			// exact record Reconcile produced.
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

			pod := &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{Name: "actors-pod", Namespace: "default"},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{{Name: "c", Image: "busybox"}},
				},
			}
			Expect(k8sClient.Create(ctx, pod)).To(Succeed())
			DeferCleanup(func() {
				Expect(client.IgnoreNotFound(k8sClient.Delete(context.Background(), pod))).To(Succeed())
			})

			// The manager set the API server actually attributed to this create
			// is the ground truth the enqueued record's actors must equal.
			expected := appliedObjectManagers(ctx, podGVK, pod.Namespace, pod.Name)
			Expect(expected).NotTo(BeEmpty())

			_, err := reconciler.Reconcile(ctx, ctrl.Request{
				NamespacedName: client.ObjectKey{Namespace: pod.Namespace, Name: pod.Name},
			})
			Expect(err).NotTo(HaveOccurred())

			var record sink.Record
			Eventually(writer.captured, 5*time.Second).Should(Receive(&record))
			Expect(record.Actors).To(Equal(expected))
		})
	})
})

// appliedObjectManagers fetches the live object and returns its distinct,
// sorted managedFields manager names, computed independently of extractActors
// so the assertion is against ground truth rather than the code under test.
func appliedObjectManagers(ctx context.Context, gvk schema.GroupVersionKind, namespace, name string) []string {
	obj := &unstructured.Unstructured{}
	obj.SetGroupVersionKind(gvk)
	Expect(k8sClient.Get(ctx, client.ObjectKey{Namespace: namespace, Name: name}, obj)).To(Succeed())

	managedFields, found, err := unstructured.NestedSlice(obj.Object, "metadata", "managedFields")
	Expect(err).NotTo(HaveOccurred())
	Expect(found).To(BeTrue())

	seen := map[string]struct{}{}
	for _, entry := range managedFields {
		m := entry.(map[string]any)
		manager, _ := m["manager"].(string)
		if manager == "" {
			manager = "unknown"
		}
		seen[manager] = struct{}{}
	}
	managers := make([]string, 0, len(seen))
	for m := range seen {
		managers = append(managers, m)
	}
	slices.Sort(managers)
	return managers
}
