package controller

import (
	"time"
)

// CacheEntry хранит данные в оперативной памяти (sync.Map)
type CacheEntry struct {
	Hash string
	JSON []byte
	UID  string
}

// ResourceRecord - это универсальная структура для отправки в ClickHouse
type ResourceRecord struct {
	Timestamp       time.Time         `json:"timestamp"`
	ClusterID       string            `json:"cluster_id"`
	EventType       string            `json:"event_type"` // Added, Modified, Deleted, Snapshot
	APIGroup        string            `json:"group"`
	APIVersion      string            `json:"version"`
	Kind            string            `json:"kind"`
	Namespace       string            `json:"namespace"`
	Name            string            `json:"name"`
	UID             string            `json:"uid"`
	ResourceVersion string            `json:"resource_version"`
	Labels          map[string]string `json:"labels"`
	Data            string            `json:"data"`      // Полный JSON (для Added)
	DiffData        string            `json:"diff_data"` // Дельта изменений (для Modified)
	SHA256          string            `json:"sha256"`
}
