package mq

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
)

// ==================== 消息结构 ====================

// CacheInvalidationMessage 缓存失效消息
type CacheInvalidationMessage struct {
	CacheType string `json:"cache_type"` // "file_meta" / "user_info"
	Key       string `json:"key"`        // SHA1 或 username
	Reason    string `json:"reason"`     // 触发原因
}

// ==================== 队列配置 ====================

const CacheInvalidationQueue = "cache_invalidation_queue"

// ==================== 初始化队列 ====================

// InitCacheInvalidationQueue 初始化缓存失效队列
func InitCacheInvalidationQueue() error {
	_, err := channel.QueueDeclare(
		CacheInvalidationQueue,
		true,  // 持久化
		false, // 不自动删除
		false, // 非独占
		false, // 不等待
		nil,
	)
	if err != nil {
		return fmt.Errorf("声明缓存失效队列失败: %w", err)
	}
	log.Printf("缓存失效队列初始化成功: %s", CacheInvalidationQueue)
	return nil
}

// ==================== 生产者 ====================

// PublishCacheInvalidation 发布缓存失效消息
func PublishCacheInvalidation(ctx context.Context, msg *CacheInvalidationMessage) error {
	body, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("序列化缓存失效消息失败: %w", err)
	}

	err = publishWithConfirm(ctx, "", CacheInvalidationQueue, body, nil)
	if err != nil {
		return fmt.Errorf("发布缓存失效消息失败: %w", err)
	}

	log.Printf("发布缓存失效消息: cache_type=%s, key=%s, reason=%s",
		msg.CacheType, msg.Key, msg.Reason)
	return nil
}

// ==================== 消费者 ====================

// ConsumeCacheInvalidation 消费缓存失效消息
func ConsumeCacheInvalidation(handler func(*CacheInvalidationMessage) error) error {
	return ConsumeJSON(CacheInvalidationQueue, func(body []byte) error {
		var msg CacheInvalidationMessage
		if err := json.Unmarshal(body, &msg); err != nil {
			return err
		}
		log.Printf("处理缓存失效: cache_type=%s, key=%s, reason=%s", msg.CacheType, msg.Key, msg.Reason)
		return handler(&msg)
	})
}
