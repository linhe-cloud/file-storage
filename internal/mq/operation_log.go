package mq

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// ==================== 消息结构 ====================

// OperationLogMessage 操作日志消息
type OperationLogMessage struct {
	UserName     string            `json:"user_name"`
	Operation    string            `json:"operation"`      // login/upload/download/delete/restore等
	ResourceType string            `json:"resource_type"`  // file/user等
	ResourceID   string            `json:"resource_id"`    // 文件hash/用户名等
	IPAddress    string            `json:"ip_address"`
	UserAgent    string            `json:"user_agent"`
	Status       string            `json:"status"`         // success/failed
	ErrorMsg     string            `json:"error_msg,omitempty"`
	ExtraInfo    map[string]string `json:"extra_info,omitempty"`	// 额外信息
	CreatedAt    time.Time         `json:"created_at"`
}

// ==================== 常量定义 ====================

// 操作类型常量
const (
	OpLogin    = "login"
	OpLogout   = "logout"
	OpSignup   = "signup"
	OpUpload   = "upload"
	OpDownload = "download"
	OpDelete   = "delete"
	OpRestore  = "restore"
)

// 资源类型常量
const (
	ResourceTypeFile = "file"
	ResourceTypeUser = "user"
)

// 状态常量
const (
	StatusSuccess = "success"
	StatusFailed  = "failed"
)

// 队列名称
const OperationLogQueue = "operation_log_queue"

// ==================== 初始化队列 ====================

// InitOperationLogQueue 初始化操作日志队列
func InitOperationLogQueue() error {
	_, err := channel.QueueDeclare(
		OperationLogQueue,
		true,  // 持久化
		false, // 不自动删除
		false, // 非独占
		false, // 不等待
		nil,
	)
	if err != nil {
		return fmt.Errorf("声明操作日志队列失败: %w", err)
	}
	log.Printf("操作日志队列初始化成功: %s", OperationLogQueue)
	return nil
}

// ==================== 生产者 ====================

// PublishOperationLog 发布操作日志消息
func PublishOperationLog(ctx context.Context, msg *OperationLogMessage) error {
	body, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("序列化操作日志失败: %w", err)
	}

	err = channel.PublishWithContext(
		ctx,
		"",                // 默认交换机
		OperationLogQueue, // 路由到操作日志队列
		false,
		false,
		amqp.Publishing{
			DeliveryMode: amqp.Persistent,
			ContentType:  "application/json",
			Body:         body,
		},
	)
	if err != nil {
		return fmt.Errorf("发布操作日志失败: %w", err)
	}

	return nil
}

// ==================== 消费者 ====================

// ConsumeOperationLogs 消费操作日志消息
func ConsumeOperationLogs(handler func(*OperationLogMessage) error) error {
	msgs, err := channel.Consume(
		OperationLogQueue,
		"",
		false, // 手动确认
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return fmt.Errorf("消费操作日志队列失败: %w", err)
	}

	log.Println("开始消费操作日志消息...")

	go func() {
		for msg := range msgs {
			var logMsg OperationLogMessage
			if err := json.Unmarshal(msg.Body, &logMsg); err != nil {
				log.Printf("解析操作日志消息失败: %v", err)
				msg.Nack(false, false) // 拒绝消息，不重新入队
				continue
			}

			log.Printf("处理操作日志: user=%s, op=%s, resource=%s",
				logMsg.UserName, logMsg.Operation, logMsg.ResourceID)

			if err := handler(&logMsg); err != nil {
				log.Printf("处理操作日志失败: %v", err)
				msg.Nack(false, true) // 拒绝消息，重新入队
			} else {
				msg.Ack(false) // 确认消息
			}
		}
	}()

	return nil
}

// ==================== 辅助函数 ====================

// NewOperationLogMessage 创建操作日志消息的便捷函数
func NewOperationLogMessage(username, operation, resourceType, resourceID string) *OperationLogMessage {
	return &OperationLogMessage{
		UserName:     username,
		Operation:    operation,
		ResourceType: resourceType,
		ResourceID:   resourceID,
		Status:       StatusSuccess,
		CreatedAt:    time.Now(),
	}
}

// WithStatus 设置状态
func (msg *OperationLogMessage) WithStatus(status string) *OperationLogMessage {
	msg.Status = status
	return msg
}

// WithError 设置错误信息
func (msg *OperationLogMessage) WithError(errMsg string) *OperationLogMessage {
	msg.Status = StatusFailed
	msg.ErrorMsg = errMsg
	return msg
}

// WithExtraInfo 设置额外信息
func (msg *OperationLogMessage) WithExtraInfo(info map[string]string) *OperationLogMessage {
	msg.ExtraInfo = info
	return msg
}

// WithIPAndUA 设置IP和UserAgent
func (msg *OperationLogMessage) WithIPAndUA(ip, ua string) *OperationLogMessage {
	msg.IPAddress = ip
	msg.UserAgent = ua
	return msg
}
