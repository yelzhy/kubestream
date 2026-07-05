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
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"sync"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/manager"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/wI2L/jsondiff"
)

type ResourceStreamReconciler struct {
	client.Client
	Scheme    *runtime.Scheme
	CHConn    driver.Conn
	HashCache sync.Map
	GVK       schema.GroupVersionKind
}

// +kubebuilder:rbac:groups="",resources=pods,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=services,verbs=get;list;watch
// +kubebuilder:rbac:groups=apps,resources=deployments,verbs=get;list;watch

func (r *ResourceStreamReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	obj := &unstructured.Unstructured{}
	obj.SetGroupVersionKind(r.GVK)

	err := r.Get(ctx, req.NamespacedName, obj)
	objectKey := r.GVK.Kind + "/" + req.NamespacedName.String()

	// --- БЛОК ОБРАБОТКИ УДАЛЕНИЯ (IsNotFound) ---
	if err != nil {
		if apierrors.IsNotFound(err) {
			if cachedVal, exists := r.HashCache.Load(objectKey); exists {
				cachedEntry := cachedVal.(CacheEntry)
				log.Info("🗑️ Объект удален, записываем событие в БД", "kind", r.GVK.Kind, "name", req.Name, "uid", cachedEntry.UID)
				r.HashCache.Delete(objectKey)

				emptyLabels := make(map[string]string)
				_ = r.CHConn.Exec(ctx, `
                    INSERT INTO resource_states (
                        ts, cluster_id, event_type, api_group, api_version, kind, namespace, name, uid, resource_version, labels, data, diff_data, sha256
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
					time.Now().UTC().Format("2006-01-02 15:04:05.999999999"), "local-kind-cluster", "Deleted", r.GVK.Group, r.GVK.Version, r.GVK.Kind,
					req.Namespace, req.Name, cachedEntry.UID, "", emptyLabels, `{"status": "deleted"}`, "", "DELETED",
				)
			}
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	// --- БЛОК НОРМАЛИЗАЦИИ ---
	originalRV := obj.GetResourceVersion()
	currentUID := string(obj.GetUID())

	labels := obj.GetLabels()
	if labels == nil {
		labels = make(map[string]string)
	}

	unstructured.RemoveNestedField(obj.Object, "metadata", "managedFields")
	unstructured.RemoveNestedField(obj.Object, "metadata", "resourceVersion")
	unstructured.RemoveNestedField(obj.Object, "metadata", "generation")

	objJson, err := json.Marshal(obj.Object)
	if err != nil {
		return ctrl.Result{}, err
	}

	hashBytes := sha256.Sum256(objJson)
	hashString := hex.EncodeToString(hashBytes[:])

	var eventType = "Added"
	var diffString = ""
	var dataString = string(objJson)

	// --- БЛОК ДЕДУПЛИКАЦИИ И РЕИНКАРНАЦИИ ---
	if cachedVal, exists := r.HashCache.Load(objectKey); exists {
		cachedEntry := cachedVal.(CacheEntry)

		// 🚨 МАГИЯ ПРОТИВ ЗОМБИ: Проверяем UID!
		if cachedEntry.UID != "" && cachedEntry.UID != currentUID {
			log.Info("🧟 Реинкарнация! Старый объект умер во время даунтайма. Пишем Deleted для старого и Added для нового", "name", req.Name)

			// 1. Закрываем историю старого объекта
			emptyLabels := make(map[string]string)
			_ = r.CHConn.Exec(ctx, `
                INSERT INTO resource_states (
                    ts, cluster_id, event_type, api_group, api_version, kind, namespace, name, uid, resource_version, labels, data, diff_data, sha256
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
				time.Now().UTC().Format("2006-01-02 15:04:05.999999999"), "local-kind-cluster", "Deleted", r.GVK.Group, r.GVK.Version, r.GVK.Kind,
				req.Namespace, req.Name, cachedEntry.UID, "", emptyLabels, `{"status": "deleted"}`, "", "DELETED",
			)

			// 2. Текущий объект обрабатывается как чистый Added (оставляем eventType = "Added")
		} else {
			// Обычная логика (объект тот же самый)
			if cachedEntry.Hash == hashString {
				return ctrl.Result{}, nil // Дубликат
			}

			eventType = "Modified"
			if cachedEntry.JSON == nil {
				log.Info("🔄 Відновлено після рестарту (Full State)", "kind", r.GVK.Kind, "name", req.Name)
				dataString = string(objJson)
				diffString = ""
			} else {
				patch, _ := jsondiff.CompareJSON(cachedEntry.JSON, objJson)
				patchBytes, _ := json.Marshal(patch)
				diffString = string(patchBytes)
				dataString = ""
				log.Info("📝 Обнаружено изменение (Diff)", "kind", r.GVK.Kind, "name", req.Name)
			}
		}
	} else {
		log.Info("🌟 Новый объект (Added)", "kind", r.GVK.Kind, "name", req.Name)
	}

	// Сохраняем в кэш
	r.HashCache.Store(objectKey, CacheEntry{
		Hash: hashString,
		JSON: objJson,
		UID:  currentUID,
	})

	// --- БЛОК ЭКСПОРТА ---
	err = r.CHConn.Exec(ctx, `
        INSERT INTO resource_states (
            ts, cluster_id, event_type, api_group, api_version, kind, namespace, name, uid, resource_version, labels, data, diff_data, sha256
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		time.Now().UTC().Format("2006-01-02 15:04:05.999999999"), "local-kind-cluster", eventType, r.GVK.Group, r.GVK.Version,
		r.GVK.Kind, req.Namespace, req.Name, currentUID, originalRV,
		labels, dataString, diffString, hashString,
	)

	if err != nil {
		log.Error(err, "Ошибка при записи в ClickHouse")
		return ctrl.Result{}, err
	}

	return ctrl.Result{}, nil
}

func (r *ResourceStreamReconciler) SetupWithManager(mgr ctrl.Manager) error {
	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{"127.0.0.1:9000"},
		Auth: clickhouse.Auth{
			Database: "kubestream",
			Username: "default",
			Password: "kpi123",
		},
		Protocol: clickhouse.Native,
	})
	if err != nil {
		return err
	}
	r.CHConn = conn

	gvksToWatch := []schema.GroupVersionKind{
		{Group: "", Version: "v1", Kind: "Pod"},
		{Group: "", Version: "v1", Kind: "Service"},
		{Group: "apps", Version: "v1", Kind: "Deployment"},
	}

	log := logf.Log.WithName("setup")

	for _, gvk := range gvksToWatch {
		reconciler := &ResourceStreamReconciler{
			Client: mgr.GetClient(),
			Scheme: mgr.GetScheme(),
			CHConn: conn,
			GVK:    gvk,
		}

		log.Info("🔄 Відновлення пам'яті з ClickHouse...", "kind", gvk.Kind)

		// Витягуємо UID з бази!
		rows, err := conn.Query(context.Background(), `
            SELECT namespace, name, argMax(uid, ts), argMax(sha256, ts)
            FROM resource_states
            WHERE kind = ?
            GROUP BY namespace, name
            HAVING argMax(event_type, ts) != 'Deleted'
        `, gvk.Kind)

		// Структура для GC
		type gcTarget struct {
			Namespace string
			Name      string
			UID       string
		}
		var targetsToCheck []gcTarget

		if err == nil {
			restoredCount := 0
			for rows.Next() {
				var namespace, name, uid, hash string
				if err := rows.Scan(&namespace, &name, &uid, &hash); err == nil {
					key := gvk.Kind + "/"
					if namespace != "" {
						key += namespace + "/" + name
					} else {
						key += name
					}

					reconciler.HashCache.Store(key, CacheEntry{
						Hash: hash,
						JSON: nil,
						UID:  uid,
					})
					restoredCount++
					targetsToCheck = append(targetsToCheck, gcTarget{Namespace: namespace, Name: name, UID: uid})
				}
			}
			rows.Close()
			log.Info("✅ Кеш відновлено", "kind", gvk.Kind, "objects_loaded", restoredCount)

			// ==========================================
			// 🕵️‍♂️ Garbage Collector (з перевіркою UID)
			// ==========================================
			if len(targetsToCheck) > 0 {
				gcGVK := gvk
				gcReconciler := reconciler

				mgr.Add(manager.RunnableFunc(func(ctx context.Context) error {
					if !mgr.GetCache().WaitForCacheSync(ctx) {
						return nil
					}

					zombieCount := 0
					for _, target := range targetsToCheck {
						obj := &unstructured.Unstructured{}
						obj.SetGroupVersionKind(gcGVK)
						reqKey := client.ObjectKey{Namespace: target.Namespace, Name: target.Name}

						err := mgr.GetClient().Get(ctx, reqKey, obj)

						// Зомбі виявлено, якщо об'єкта немає АБО якщо його UID змінився
						isZombie := false
						if err != nil && apierrors.IsNotFound(err) {
							isZombie = true
						} else if err == nil && string(obj.GetUID()) != target.UID {
							isZombie = true
						}

						if isZombie {
							cacheKey := gcGVK.Kind + "/"
							if reqKey.Namespace != "" {
								cacheKey += reqKey.Namespace + "/" + reqKey.Name
							} else {
								cacheKey += reqKey.Name
							}

							gcReconciler.HashCache.Delete(cacheKey)
							emptyLabels := make(map[string]string)
							_ = conn.Exec(ctx, `
                                INSERT INTO resource_states (
                                    ts, cluster_id, event_type, api_group, api_version, kind, namespace, name, uid, resource_version, labels, data, diff_data, sha256
                                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
								time.Now().UTC().Format("2006-01-02 15:04:05.999999999"), "local-kind-cluster", "Deleted", gcGVK.Group, gcGVK.Version, gcGVK.Kind,
								reqKey.Namespace, reqKey.Name, target.UID, "", emptyLabels, `{"status": "deleted"}`, "", "DELETED",
							)
							zombieCount++
							log.Info("🧟 Зомбі-об'єкт виявлено та видалено GC", "kind", gcGVK.Kind, "name", reqKey.Name)
						}
					}
					if zombieCount > 0 {
						log.Info("🧹 Garbage Collector завершив роботу", "kind", gcGVK.Kind, "zombies_cleared", zombieCount)
					}
					return nil
				}))
			}
			// ==========================================

		} else {
			log.Error(err, "Помилка відновлення кешу (можливо таблиця порожня)")
		}

		obj := &unstructured.Unstructured{}
		obj.SetGroupVersionKind(gvk)

		err = ctrl.NewControllerManagedBy(mgr).
			For(obj).
			Named("stream-" + gvk.Kind).
			Complete(reconciler)
		if err != nil {
			return err
		}
	}

	return nil
}
