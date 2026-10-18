package handler

import (
	"context"
	"file-storage-linhe/internal/cache/redis"
	"file-storage-linhe/internal/db"
	"file-storage-linhe/internal/monitor"
	"file-storage-linhe/internal/mq"
	"file-storage-linhe/internal/store"
	"net/http"
	"time"
)

func ReadyHandler(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()
	checks := map[string]string{}
	ready := true
	if db.DB == nil || db.DB.PingContext(ctx) != nil {
		checks["mysql"] = "down"
		ready = false
	} else {
		checks["mysql"] = "up"
	}
	if redis.Rdb == nil || redis.Rdb.Ping(ctx).Err() != nil {
		checks["redis"] = "down"
		ready = false
	} else {
		checks["redis"] = "up"
	}
	if !mq.Ready() {
		checks["rabbitmq"] = "down"
		ready = false
	} else {
		checks["rabbitmq"] = "up"
	}
	if store.MinioClient == nil {
		checks["minio"] = "down"
		ready = false
	} else {
		checks["minio"] = "up"
	}
	status := http.StatusOK
	if !ready {
		status = http.StatusServiceUnavailable
	}
	writeJSON(w, status, map[string]any{"ready": ready, "checks": checks})
}

func MetricsHandler(w http.ResponseWriter, r *http.Request) { monitor.MetricsHandler(w, r) }
