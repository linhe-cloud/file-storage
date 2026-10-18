package mq

import (
	"context"
	"encoding/json"
	"file-storage-linhe/config"
	"fmt"
	"log"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// ==================== 消息结构 ====================

// FileDeleteMessage 文件删除消息
type FileDeleteMessage struct {
	Username  string    `json:"username"`
	FileHash  string    `json:"file_hash"`
	FileName  string    `json:"file_name"`
	DeletedAt time.Time `json:"deleted_at"`
}

// ==================== 队列配置 ====================

const (
	FileDeleteDLX        = "file_delete_dlx"        // 死信交换机
	FileDeleteWorkQueue  = "file_delete_work_queue" // 工作队列
	FileDeleteRoutingKey = "file_delete_work"       // 路由键
)

// ==================== 初始化队列 ====================

// InitFileDeleteQueue 初始化文件删除延迟队列（死信队列机制）
func InitFileDeleteQueue() error {
	delayQueue := config.RabbitMQDelayQueue // 延迟队列名

	// 1. 声明死信交换机
	if err := channel.ExchangeDeclare(
		FileDeleteDLX,
		"direct", // 直连交换机
		true,     // 持久化
		false,
		false,
		false,
		nil,
	); err != nil {
		return fmt.Errorf("声明死信交换机失败: %w", err)
	}

	// 2. 声明延迟队列（带死信配置）
	args := amqp.Table{
		"x-dead-letter-exchange":    FileDeleteDLX,           // 过期后自动转到死信交换机
		"x-dead-letter-routing-key": FileDeleteRoutingKey,    // 死信路由键
		"x-message-ttl":             config.RabbitMQDelayTTL, // 延迟时间（3天）
	}
	if _, err := channel.QueueDeclare(
		delayQueue,
		true, // 持久化
		false,
		false,
		false,
		args,
	); err != nil {
		return fmt.Errorf("声明延迟队列失败: %w", err)
	}

	// 3. 声明工作队列（消费者监听的队列）
	if _, err := channel.QueueDeclare(
		FileDeleteWorkQueue,
		true, // 持久化
		false,
		false,
		false,
		nil,
	); err != nil {
		return fmt.Errorf("声明工作队列失败: %w", err)
	}

	// 4. 绑定工作队列到死信交换机
	if err := channel.QueueBind(
		FileDeleteWorkQueue,
		FileDeleteRoutingKey,
		FileDeleteDLX,
		false,
		nil,
	); err != nil {
		return fmt.Errorf("绑定工作队列失败: %w", err)
	}

	log.Printf("文件删除队列初始化成功 (延迟: %dms)", config.RabbitMQDelayTTL)
	return nil
}

// ==================== 生产者 ====================

// PublishFileDeleteMessage 发布文件删除消息到延迟队列
func PublishFileDeleteMessage(ctx context.Context, msg *FileDeleteMessage) error {
	body, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("序列化消息失败: %w", err)
	}

	// 发布消息到延迟队列
	err = publishWithConfirm(ctx, "", config.RabbitMQDelayQueue, body, nil)
	if err != nil {
		return fmt.Errorf("发布消息失败: %w", err)
	}

	log.Printf("发布文件删除消息: username=%s, filehash=%s", msg.Username, msg.FileHash)
	return nil
}

// ==================== 消费者 ====================

// ConsumeFileDeleteMessages 消费文件删除消息（从工作队列）
func ConsumeFileDeleteMessages(handler func(*FileDeleteMessage) error) error {
	return ConsumeJSON(FileDeleteWorkQueue, func(body []byte) error {
		var msg FileDeleteMessage
		if err := json.Unmarshal(body, &msg); err != nil {
			return err
		}
		log.Printf("处理文件删除: username=%s, filehash=%s", msg.Username, msg.FileHash)
		return handler(&msg)
	})
}

// ==================== 辅助函数 ====================

// NewFileDeleteMessage 创建文件删除消息的便捷函数
func NewFileDeleteMessage(username, fileHash, fileName string) *FileDeleteMessage {
	return &FileDeleteMessage{
		Username:  username,
		FileHash:  fileHash,
		FileName:  fileName,
		DeletedAt: time.Now(),
	}
}
