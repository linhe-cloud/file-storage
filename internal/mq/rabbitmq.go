package mq

import (
	"file-storage-linhe/config"
	"fmt"
	"log"

	amqp "github.com/rabbitmq/amqp091-go"
)

var (
	conn    *amqp.Connection
	channel *amqp.Channel
)

// InitRabbitMQ 初始化 RabbitMQ 连接和所有队列
func InitRabbitMQ() error {
	var err error

	// 1. 建立连接
	conn, err = amqp.Dial(config.RabbitMQURL)
	if err != nil {
		return fmt.Errorf("连接 RabbitMQ 失败: %w", err)
	}

	// 2. 创建通道
	channel, err = conn.Channel()
	if err != nil {
		return fmt.Errorf("创建 Channel 失败: %w", err)
	}

	// 3. 初始化文件删除队列（延迟队列 + 死信队列）
	if err := InitFileDeleteQueue(); err != nil {
		return err
	}

	// 4. 初始化操作日志队列
	if err := InitOperationLogQueue(); err != nil {
		return err
	}

	log.Println("RabbitMQ 初始化成功")
	return nil
}

func Close() {
	if channel != nil {
		channel.Close()
	}
	if conn != nil {
		conn.Close()
	}
}
