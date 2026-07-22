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
	"sync"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	ctrl "sigs.k8s.io/controller-runtime/pkg/reconcile"

	"github.com/yelzhy/kubestream/internal/sink"
)

// recordingSink is a logr.LogSink that captures the errors passed to
// log.Error, so a test can assert the reconciler logged a specific anomaly
// (Invariant 4: zero silent errors). It is concurrency-safe because a commit
// callback can run on a different goroutine than the Reconcile call.
type recordingSink struct {
	mu     sync.Mutex
	errors []error
}

func (s *recordingSink) Init(logr.RuntimeInfo)          {}
func (s *recordingSink) Enabled(int) bool               { return true }
func (s *recordingSink) Info(int, string, ...any)       {}
func (s *recordingSink) WithValues(...any) logr.LogSink { return s }
func (s *recordingSink) WithName(string) logr.LogSink   { return s }
func (s *recordingSink) Error(err error, _ string, _ ...any) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.errors = append(s.errors, err)
}

func (s *recordingSink) loggedErrors() []error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return slices.Clone(s.errors)
}

// corruptBaseline truncates the compressed diff baseline stored for key so a
// later decodeBaseline fails, simulating an on-disk/in-memory corruption. It
// leaves Hash untouched so callers can independently control whether the
// dedup short-circuit or the diff-decode path is exercised.
func corruptBaseline(hc *hashCache, key string) {
	hc.mu.Lock()
	defer hc.mu.Unlock()
	entry := hc.data[key]
	if len(entry.JSON) > 1 {
		entry.JSON = entry.JSON[:len(entry.JSON)/2]
	}
	hc.data[key] = entry
}

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

var _ = Describe("ResourceStream Controller compressed baselines", func() {
	podGVK := schema.GroupVersionKind{Group: "", Version: "v1", Kind: "Pod"}

	// newReconciler wires a reconciler around a capturing writer and a
	// recording logger so each spec can inspect both the enqueued records and
	// any errors the reconciler logged.
	newReconciler := func() (*ResourceStreamReconciler, *capturingWriter, *recordingSink) {
		writer := &capturingWriter{captured: make(chan sink.Record, 8)}
		return &ResourceStreamReconciler{
			Client:    k8sClient,
			Scheme:    k8sClient.Scheme(),
			Writer:    writer,
			GVK:       podGVK,
			ClusterID: "test-cluster",
			requeueCh: make(chan event.GenericEvent, 1),
			metrics:   PipelineMetricsInstance(),
		}, writer, &recordingSink{}
	}

	Context("when a cached baseline is corrupt", func() {
		It("short-circuits an unchanged object on hash alone, never decompressing", func() {
			reconciler, writer, rec := newReconciler()
			ctx := logf.IntoContext(context.Background(), logr.New(rec))

			pod := &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{Name: "shortcircuit-pod", Namespace: "default"},
				Spec:       corev1.PodSpec{Containers: []corev1.Container{{Name: "c", Image: "busybox"}}},
			}
			Expect(k8sClient.Create(ctx, pod)).To(Succeed())
			DeferCleanup(func() {
				Expect(client.IgnoreNotFound(k8sClient.Delete(context.Background(), pod))).To(Succeed())
			})

			req := ctrl.Request{NamespacedName: client.ObjectKey{Namespace: pod.Namespace, Name: pod.Name}}
			_, err := reconciler.Reconcile(ctx, req)
			Expect(err).NotTo(HaveOccurred())

			var first sink.Record
			Eventually(writer.captured, 5*time.Second).Should(Receive(&first))
			Expect(first.EventType).To(Equal("Added"))

			// Corrupt the stored baseline but leave its hash intact, so the
			// next Reconcile of the unchanged object matches on hash and must
			// short-circuit before ever touching (and failing to decode) the
			// baseline.
			corruptBaseline(&reconciler.HashCache, reconciler.cacheKey(pod.Namespace, pod.Name))

			_, err = reconciler.Reconcile(ctx, req)
			Expect(err).NotTo(HaveOccurred())

			// No record and no logged error: the corrupt baseline was never
			// decoded, proving the dedup hot path is hash-comparison-only.
			Consistently(writer.captured, 500*time.Millisecond).ShouldNot(Receive())
			Expect(rec.loggedErrors()).To(BeEmpty())
		})

		It("falls back to a full-state write (event not dropped) and logs the decode error when the object changed", func() {
			reconciler, writer, rec := newReconciler()
			ctx := logf.IntoContext(context.Background(), logr.New(rec))

			pod := &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{Name: "corrupt-baseline-pod", Namespace: "default"},
				Spec:       corev1.PodSpec{Containers: []corev1.Container{{Name: "c", Image: "busybox"}}},
			}
			Expect(k8sClient.Create(ctx, pod)).To(Succeed())
			DeferCleanup(func() {
				Expect(client.IgnoreNotFound(k8sClient.Delete(context.Background(), pod))).To(Succeed())
			})

			req := ctrl.Request{NamespacedName: client.ObjectKey{Namespace: pod.Namespace, Name: pod.Name}}
			_, err := reconciler.Reconcile(ctx, req)
			Expect(err).NotTo(HaveOccurred())

			var first sink.Record
			Eventually(writer.captured, 5*time.Second).Should(Receive(&first))
			Expect(first.EventType).To(Equal("Added"))

			// Corrupt the baseline, then change the object so the next
			// Reconcile takes the diff path and must decode the (now corrupt)
			// baseline.
			corruptBaseline(&reconciler.HashCache, reconciler.cacheKey(pod.Namespace, pod.Name))

			fetched := &corev1.Pod{}
			Expect(k8sClient.Get(ctx, req.NamespacedName, fetched)).To(Succeed())
			fetched.Labels = map[string]string{"changed": "true"}
			Expect(k8sClient.Update(ctx, fetched)).To(Succeed())

			_, err = reconciler.Reconcile(ctx, req)
			Expect(err).NotTo(HaveOccurred())

			// The event is preserved as a full-state Modified write, not
			// dropped and not mis-recorded as a diff.
			var second sink.Record
			Eventually(writer.captured, 5*time.Second).Should(Receive(&second))
			Expect(second.EventType).To(Equal("Modified"))
			Expect(second.Data).NotTo(BeEmpty())
			Expect(second.Diff).To(BeEmpty())

			// The decode failure was logged at Error level, never swallowed.
			Expect(rec.loggedErrors()).NotTo(BeEmpty())
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
