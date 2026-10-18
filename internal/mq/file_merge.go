package mq

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"
)

// ==================== 消息结构 ====================

// FileMergeMessage 分片合并消息
type FileMergeMessage struct {
	UploadID    string    `json:"upload_id"`    // 上传任务ID
	Username    string    `json:"username"`     // 用户名
	FileName    string    `json:"file_name"`    // 文件名
	ChunkCount  int       `json:"chunk_count"`  // 分片总数
	FileSize    int64     `json:"file_size"`    // 文件总大小
	FileSha1    string    `json:"file_sha1"`    // 文件SHA1（如果前端传了）
	SubmittedAt time.Time `json:"submitted_at"` // 提交时间
}

// ==================== 队列配置 ====================

const FileMergeQueue = "file_merge_queue"

// ==================== 初始化队列 ====================

// InitFileMergeQueue 初始化分片合并队列
func InitFileMergeQueue() error {
	_, err := channel.QueueDeclare(
		FileMergeQueue,
		true,  // 持久化
		false, // 不自动删除
		false, // 非独占
		false, // 不等待
		nil,
	)
	if err != nil {
		return fmt.Errorf("声明分片合并队列失败: %w", err)
	}
	log.Printf("分片合并队列初始化成功: %s", FileMergeQueue)
	return nil
}

// ==================== 生产者 ====================

// PublishFileMerge 发布分片合并消息
func PublishFileMerge(ctx context.Context, msg *FileMergeMessage) error {
	body, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("序列化分片合并消息失败: %w", err)
	}

	err = publishWithConfirm(ctx, "", FileMergeQueue, body, nil)
	if err != nil {
		return fmt.Errorf("发布分片合并消息失败: %w", err)
	}

	log.Printf("发布分片合并消息: upload_id=%s, username=%s, file=%s",
		msg.UploadID, msg.Username, msg.FileName)
	return nil
}

// ==================== 消费者 ====================

// ConsumeFileMerge 消费分片合并消息
func ConsumeFileMerge(handler func(*FileMergeMessage) error) error {
	return ConsumeJSON(FileMergeQueue, func(body []byte) error {
		var msg FileMergeMessage
		if err := json.Unmarshal(body, &msg); err != nil {
			return err
		}
		log.Printf("处理分片合并: upload_id=%s, file=%s", msg.UploadID, msg.FileName)
		return handler(&msg)
	})
}

// ==================== 辅助函数 ====================

// NewFileMergeMessage 创建分片合并消息的便捷函数
func NewFileMergeMessage(uploadID, username, fileName string, chunkCount int, fileSize int64) *FileMergeMessage {
	return &FileMergeMessage{
		UploadID:    uploadID,
		Username:    username,
		FileName:    fileName,
		ChunkCount:  chunkCount,
		FileSize:    fileSize,
		SubmittedAt: time.Now(),
	}
}
