package consumer

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"file-storage-linhe/internal/db"
	"file-storage-linhe/internal/monitor"
	"file-storage-linhe/internal/mq"
)

const outboxPollInterval = 2 * time.Second

func StartOutboxPublisher(ctx context.Context) {
	go func() {
		ticker := time.NewTicker(outboxPollInterval)
		defer ticker.Stop()
		for {
			publishOutboxBatch(ctx)
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}
		}
	}()
}

func publishOutboxBatch(ctx context.Context) {
	messages, err := db.ClaimPendingOutbox(ctx, 50)
	if err != nil {
		log.Printf("claim outbox messages failed: %v", err)
		return
	}
	for _, message := range messages {
		if err := publishOutboxMessage(ctx, message); err != nil {
			log.Printf("publish outbox id=%d type=%s failed: %v", message.ID, message.Type, err)
			if markErr := db.MarkOutboxRetry(ctx, message.ID, message.Attempts, err); markErr != nil {
				log.Printf("mark outbox retry id=%d failed: %v", message.ID, markErr)
			}
			monitor.IncOutboxRetry()
			if message.Attempts+1 >= db.OutboxMaxAttempts {
				monitor.IncOutboxFailed()
				monitor.Alert(ctx, "outbox_failed", map[string]string{"id": fmt.Sprint(message.ID), "type": message.Type})
			}
			continue
		}
		if err := db.MarkOutboxPublished(ctx, message.ID); err != nil {
			log.Printf("mark outbox published id=%d failed: %v", message.ID, err)
		}
		monitor.IncOutboxPublished()
	}
}

func publishOutboxMessage(ctx context.Context, message db.OutboxMessage) error {
	switch message.Type {
	case "file_delete":
		var payload mq.FileDeleteMessage
		if err := json.Unmarshal(message.Payload, &payload); err != nil {
			return err
		}
		return mq.PublishFileDeleteMessage(ctx, &payload)
	case "file_cleanup":
		var payload mq.FileCleanupMessage
		if err := json.Unmarshal(message.Payload, &payload); err != nil {
			return err
		}
		return mq.PublishFileCleanup(ctx, &payload)
	case "file_transform":
		var payload mq.FileTransformMessage
		if err := json.Unmarshal(message.Payload, &payload); err != nil {
			return err
		}
		return mq.PublishFileTransform(ctx, &payload)
	case "operation_log":
		var payload mq.OperationLogMessage
		if err := json.Unmarshal(message.Payload, &payload); err != nil {
			return err
		}
		return mq.PublishOperationLog(ctx, &payload)
	default:
		return fmt.Errorf("unknown outbox message type %q", message.Type)
	}
}
