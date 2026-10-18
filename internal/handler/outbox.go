package handler

import (
	"database/sql"
	"errors"
	"file-storage-linhe/config"
	"file-storage-linhe/internal/db"
	"file-storage-linhe/internal/mq"
	"net/http"
	"strconv"
)

func adminAuthorized(r *http.Request) bool {
	return config.OutboxAdminToken != "" && r.Header.Get("X-Admin-Token") == config.OutboxAdminToken
}

func DeadLetterAdminHandler(w http.ResponseWriter, r *http.Request) {
	if !adminAuthorized(r) {
		writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "admin authentication required"})
		return
	}
	queue := r.URL.Query().Get("queue")
	if queue == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "queue is required"})
		return
	}
	switch r.Method {
	case http.MethodGet:
		count, err := mq.DeadLetterCount(queue)
		if err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"queue": queue, "dead_letters": count})
	case http.MethodPost:
		limit := 100
		if raw := r.URL.Query().Get("limit"); raw != "" {
			parsed, err := strconv.Atoi(raw)
			if err != nil {
				writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid limit"})
				return
			}
			limit = parsed
		}
		replayed, err := mq.ReplayDeadLetters(r.Context(), queue, limit)
		if err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"queue": queue, "replayed": replayed})
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

func OutboxAdminHandler(w http.ResponseWriter, r *http.Request) {
	if !adminAuthorized(r) {
		writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "admin authentication required"})
		return
	}
	switch r.Method {
	case http.MethodGet:
		limit := 100
		if raw := r.URL.Query().Get("limit"); raw != "" {
			parsed, err := strconv.Atoi(raw)
			if err != nil {
				writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid limit"})
				return
			}
			limit = parsed
		}
		records, err := db.ListOutbox(r.Context(), r.URL.Query().Get("status"), limit)
		if err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "failed to list outbox"})
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"items": records})
	case http.MethodPost:
		id, err := strconv.ParseInt(r.URL.Query().Get("id"), 10, 64)
		if err != nil || id <= 0 {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid outbox id"})
			return
		}
		if err := db.RetryOutbox(r.Context(), id); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				writeJSON(w, http.StatusNotFound, map[string]string{"error": "failed outbox message not found"})
				return
			}
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "failed to retry outbox"})
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"id": id, "status": db.OutboxPending})
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}
