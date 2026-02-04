package consumer

import (
	"context"
	"log"

	"file-storage-linhe/internal/db"
	"file-storage-linhe/internal/mq"
)

// StartOperationLogConsumer 启动操作日志消费者
func StartOperationLogConsumer() error {
	return mq.ConsumeOperationLogs(func(msg *mq.OperationLogMessage) error {
		ctx := context.Background()

		// 写入数据库
		if err := db.InsertOperationLog(ctx, msg); err != nil {
			log.Printf("写入操作日志失败: %v", err)
			return err // 返回错误，消息会重试
		}

		log.Printf("操作日志写入成功: user=%s, op=%s, resource=%s",
			msg.UserName, msg.Operation, msg.ResourceID)
		return nil
	})
}
