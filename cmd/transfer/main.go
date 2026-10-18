package main

import (
	"context"
	"file-storage-linhe/internal/cache/redis"
	"file-storage-linhe/internal/consumer"
	"file-storage-linhe/internal/db"
	"file-storage-linhe/internal/mq"
	"file-storage-linhe/internal/store"
	"log"
	"os"
	"os/signal"
	"syscall"
)

// 主函数，程序的入口点
func main() {
	// 初始化数据库连接，如果失败则记录错误并退出程序
	if err := db.InitDB(); err != nil {
		log.Fatalf("init db failed: %v", err)
	}

	// 初始化Minio对象存储服务，如果失败则记录错误并退出程序
	if err := store.InitMinio(); err != nil {
		log.Fatalf("init minio failed: %v", err)
	}

	// 初始化Redis缓存服务，如果失败则记录错误并退出程序
	if err := redis.InitRedis(context.Background()); err != nil {
		log.Fatalf("init redis failed: %v", err)
	}

	// 初始化RabbitMQ消息队列服务，如果失败则记录错误并退出程序
	// 使用defer确保在程序退出前关闭MQ连接
	if err := mq.InitRabbitMQ(); err != nil {
		log.Fatalf("init rabbitmq failed: %v", err)
	}
	defer mq.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 启动文件删除消息消费者，如果失败则记录错误并退出程序
	if err := consumer.StartFileDeleteConsumer(); err != nil {
		log.Fatalf("start file delete consumer failed: %v", err)
	}

	// 启动操作日志消息消费者，如果失败则记录错误并退出程序
	if err := consumer.StartOperationLogConsumer(); err != nil {
		log.Fatalf("start operation log consumer failed: %v", err)
	}

	// 启动文件合并消息消费者，如果失败则记录错误并退出程序
	if err := consumer.StartFileMergeConsumer(); err != nil {
		log.Fatalf("start file merge consumer failed: %v", err)
	}

	// 启动缓存失效消息消费者，如果失败则记录错误并退出程序
	if err := consumer.StartCacheInvalidationConsumer(); err != nil {
		log.Fatalf("start cache invalidation consumer failed: %v", err)
	}

	// 启动文件转换消息消费者，如果失败则记录错误并退出程序
	if err := consumer.StartFileTransformConsumer(); err != nil {
		log.Fatalf("start file transform consumer failed: %v", err)
	}
	if err := consumer.StartFileCleanupConsumer(); err != nil {
		log.Fatalf("start file cleanup consumer failed: %v", err)
	}
	consumer.StartOutboxPublisher(ctx)

	// 打印服务启动成功信息
	log.Printf("transfer service started, waiting for messages...")

	// 创建信号通道，用于接收系统信号
	sigCh := make(chan os.Signal, 1)
	// 注册要监听的信号：SIGINT (Ctrl+C) 和 SIGTERM (终止信号)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	// 阻塞等待信号
	sig := <-sigCh
	// 收到信号后，打印关闭信息并退出程序
	log.Printf("received signal %v, shutting down...", sig)
	cancel()
}
