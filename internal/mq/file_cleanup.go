package mq

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
)

const FileCleanupQueue = "file_cleanup_queue"

type FileCleanupMessage struct {
	FileHash string `json:"file_hash"`
	Location string `json:"location"`
}

func InitFileCleanupQueue() error {
	_, err := channel.QueueDeclare(FileCleanupQueue, true, false, false, false, nil)
	if err != nil {
		return fmt.Errorf("declare file cleanup queue: %w", err)
	}
	return nil
}

func NewFileCleanupMessage(fileHash, location string) *FileCleanupMessage {
	return &FileCleanupMessage{FileHash: fileHash, Location: location}
}

func PublishFileCleanup(ctx context.Context, msg *FileCleanupMessage) error {
	body, err := json.Marshal(msg)
	if err != nil {
		return err
	}
	if err := publishWithConfirm(ctx, "", FileCleanupQueue, body, nil); err != nil {
		return fmt.Errorf("publish file cleanup: %w", err)
	}
	log.Printf("published file cleanup: file_hash=%s", msg.FileHash)
	return nil
}

func ConsumeFileCleanup(handler func(*FileCleanupMessage) error) error {
	return ConsumeJSON(FileCleanupQueue, func(body []byte) error {
		var msg FileCleanupMessage
		if err := json.Unmarshal(body, &msg); err != nil {
			return err
		}
		return handler(&msg)
	})
}
