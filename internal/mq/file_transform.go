package mq

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
)

type FileTransformMessage struct {
	FileHash string `json:"file_hash"`
	FileName string `json:"file_name"`
	Username string `json:"username"`
	FileType string `json:"file_type"`
}

const FileTransformQueue = "file_transform_queue"

func InitFileTransformQueue() error {
	_, err := channel.QueueDeclare(
		FileTransformQueue,
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return fmt.Errorf("声明文件转换队列失败: %w", err)
	}
	log.Printf("文件转换队列初始化成功: %s", FileTransformQueue)
	return nil
}

func PublishFileTransform(ctx context.Context, msg *FileTransformMessage) error {
	body, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("序列化文件转换消息失败: %w", err)
	}

	err = publishWithConfirm(ctx, "", FileTransformQueue, body, nil)
	if err != nil {
		return fmt.Errorf("发布文件转换消息失败: %w", err)
	}

	log.Printf("发布文件转换消息: file_hash=%s, file_name=%s, user=%s",
		msg.FileHash, msg.FileName, msg.Username)
	return nil
}

func ConsumeFileTransform(handler func(*FileTransformMessage) error) error {
	return ConsumeJSON(FileTransformQueue, func(body []byte) error {
		var msg FileTransformMessage
		if err := json.Unmarshal(body, &msg); err != nil {
			return err
		}
		log.Printf("处理文件转换: file_hash=%s, file_name=%s", msg.FileHash, msg.FileName)
		return handler(&msg)
	})
}
