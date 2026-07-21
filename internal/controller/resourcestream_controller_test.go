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

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	ctrl "sigs.k8s.io/controller-runtime/pkg/reconcile"
)

// capturingConn is a driver.Conn that records the positional args of every
// Exec, so the integration test can inspect exactly what Reconcile enqueued for
// ClickHouse without a real database. Embedding the interface satisfies the
// full method set; only Exec and Close are exercised.
type capturingConn struct {
	driver.Conn
	captured chan []any
}

func (c *capturingConn) Exec(_ context.Context, _ string, args ...any) error {
	c.captured <- args
	return nil
}

func (c *capturingConn) Close() error { return nil }

var _ = Describe("ResourceStream Controller", func() {
	Context("When reconciling a resource with managedFields", func() {
		It("enqueues a record whose actors match the applied object's field managers", func() {
			podGVK := schema.GroupVersionKind{Group: "", Version: "v1", Kind: "Pod"}

			// A capturing conn stands in for ClickHouse so we can read back the
			// exact insert args Reconcile produced. The CHWriter runs under a
			// local context cancelled at the end of this spec.
			conn := &capturingConn{captured: make(chan []any, 8)}
			chWriter := NewCHWriter(conn, 8, 1, time.Second, time.Second, time.Second)

			writerCtx, cancelWriter := context.WithCancel(context.Background())
			defer cancelWriter()
			writerDone := make(chan error, 1)
			go func() { writerDone <- chWriter.Start(writerCtx) }()

			reconciler := &ResourceStreamReconciler{
				Client:    k8sClient,
				Scheme:    k8sClient.Scheme(),
				CHWriter:  chWriter,
				GVK:       podGVK,
				ClusterID: "test-cluster",
				requeueCh: make(chan event.GenericEvent, 1),
				metrics:   pipelineMetricsInstance(),
			}

			pod := &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{Name: "actors-pod", Namespace: "default"},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{{Name: "c", Image: "busybox"}},
				},
			}
			Expect(k8sClient.Create(writerCtx, pod)).To(Succeed())
			DeferCleanup(func() {
				Expect(client.IgnoreNotFound(k8sClient.Delete(context.Background(), pod))).To(Succeed())
			})

			// The manager set the API server actually attributed to this create
			// is the ground truth the enqueued record's actors must equal.
			expected := appliedObjectManagers(writerCtx, podGVK, pod.Namespace, pod.Name)
			Expect(expected).NotTo(BeEmpty())

			_, err := reconciler.Reconcile(writerCtx, ctrl.Request{
				NamespacedName: client.ObjectKey{Namespace: pod.Namespace, Name: pod.Name},
			})
			Expect(err).NotTo(HaveOccurred())

			var args []any
			Eventually(conn.captured, 5*time.Second).Should(Receive(&args))

			// insertArgs column order puts actors at index 11 (ts, cluster_id,
			// event_type, api_group, api_version, kind, namespace, name, uid,
			// resource_version, labels, actors, data, diff, sha256).
			Expect(args).To(HaveLen(15))
			actors, ok := args[11].([]string)
			Expect(ok).To(BeTrue(), "actors arg should be a []string")
			Expect(actors).To(Equal(expected))
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
