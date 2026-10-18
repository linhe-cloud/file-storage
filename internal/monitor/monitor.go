package monitor

import (
	"bytes"
	"context"
	"encoding/json"
	"file-storage-linhe/config"
	"log"
	"net/http"
	"strconv"
	"sync/atomic"
	"time"
)

var (
	outboxPublished atomic.Uint64
	outboxRetries   atomic.Uint64
	outboxFailed    atomic.Uint64
	mqDeadLetters   atomic.Uint64
	alertsSent      atomic.Uint64
)

func IncOutboxPublished() { outboxPublished.Add(1) }
func IncOutboxRetry()     { outboxRetries.Add(1) }
func IncOutboxFailed()    { outboxFailed.Add(1) }
func IncMQDeadLetter()    { mqDeadLetters.Add(1) }

func MetricsHandler(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/plain; version=0.0.4")
	_, _ = w.Write([]byte("# TYPE filestore_outbox_published_total counter\nfilestore_outbox_published_total " + strconv.FormatUint(outboxPublished.Load(), 10) + "\n"))
	_, _ = w.Write([]byte("# TYPE filestore_outbox_retries_total counter\nfilestore_outbox_retries_total " + strconv.FormatUint(outboxRetries.Load(), 10) + "\n"))
	_, _ = w.Write([]byte("# TYPE filestore_outbox_failed_total counter\nfilestore_outbox_failed_total " + strconv.FormatUint(outboxFailed.Load(), 10) + "\n"))
	_, _ = w.Write([]byte("# TYPE filestore_mq_dead_letters_total counter\nfilestore_mq_dead_letters_total " + strconv.FormatUint(mqDeadLetters.Load(), 10) + "\n"))
	_, _ = w.Write([]byte("# TYPE filestore_alerts_sent_total counter\nfilestore_alerts_sent_total " + strconv.FormatUint(alertsSent.Load(), 10) + "\n"))
}

func Alert(ctx context.Context, event string, details map[string]string) {
	if config.AlertWebhookURL == "" {
		log.Printf("alert event=%s details=%v", event, details)
		return
	}
	payload, err := json.Marshal(map[string]any{"event": event, "details": details, "time": time.Now().UTC()})
	if err != nil {
		return
	}
	requestCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(requestCtx, http.MethodPost, config.AlertWebhookURL, bytes.NewReader(payload))
	if err != nil {
		return
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		log.Printf("alert webhook failed: %v", err)
		return
	}
	_ = resp.Body.Close()
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		alertsSent.Add(1)
	}
}
