package mq

import (
	"context"
	"file-storage-linhe/config"
	"file-storage-linhe/internal/monitor"
	"fmt"
	"log"
	"strconv"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

var (
	conn    *amqp.Connection
	channel *amqp.Channel
	mqMu    sync.Mutex
	stopCh  chan struct{}
)

const (
	maxPublishAttempts = 3
	maxMessageRetries  = 5
	retryHeader        = "x-retry-count"
)

// InitRabbitMQ 初始化 RabbitMQ 连接和所有队列
func InitRabbitMQ() error {
	mqMu.Lock()
	defer mqMu.Unlock()
	return initRabbitMQLocked()
}

// initRabbitMQLocked 初始化 RabbitMQ 连接和相关队列
// 该函数负责建立与 RabbitMQ 的连接，创建通道，并初始化各种队列
// 返回 error 类型的错误信息，如果初始化过程中出现任何错误，将返回相应的错误信息
func initRabbitMQLocked() error {
	var err error
	if stopCh == nil {
		stopCh = make(chan struct{})
	}

	// 1. 建立连接
	// 使用 amqp.Dial 函数根据配置的 RabbitMQURL 建立连接
	conn, err = amqp.Dial(config.RabbitMQURL)
	if err != nil {
		return fmt.Errorf("连接 RabbitMQ 失败: %w", err)
	}

	// 2. 创建通道
	// 通过连接创建一个通道，这是后续操作的基础
	channel, err = conn.Channel()
	if err != nil {
		return fmt.Errorf("创建 Channel 失败: %w", err)
	}

	// 3. 初始化文件删除队列（延迟队列 + 死信队列）
	// 初始化用于处理文件删除的队列，包含延迟队列和死信队列功能
	if err := InitFileDeleteQueue(); err != nil {
		return err
	}

	// 4. 初始化操作日志队列
	// 初始化用于记录系统操作日志的队列
	if err := InitOperationLogQueue(); err != nil {
		return err
	}

	// 5. 初始化分片合并队列
	// 初始化用于处理文件分片合并的队列
	if err := InitFileMergeQueue(); err != nil {
		return err
	}

	// 6. 初始化缓存失效队列
	// 初始化用于处理缓存失效的队列
	if err := InitCacheInvalidationQueue(); err != nil {
		return err
	}

	// 7. 初始化文件转换队列
	// 初始化用于处理文件转换的队列
	if err := InitFileTransformQueue(); err != nil {
		return err
	}
	// 初始化文件清理队列
	// 初始化用于处理文件清理的队列
	if err := InitFileCleanupQueue(); err != nil {
		return err
	}
	// 为所有队列创建对应的死信队列
	// 遍历所有队列名称，为每个队列创建一个对应的死信队列（.dead 后缀）
	for _, queue := range []string{FileDeleteWorkQueue, OperationLogQueue, FileMergeQueue, CacheInvalidationQueue, FileTransformQueue, FileCleanupQueue} {
		if _, err := channel.QueueDeclare(queue+".dead", true, false, false, false, nil); err != nil {
			return fmt.Errorf("declare dead-letter queue %s: %w", queue, err)
		}
	}

	// 打印初始化成功的日志信息
	log.Println("RabbitMQ 初始化成功")
	return nil
}

func reconnect() error {
	mqMu.Lock()
	defer mqMu.Unlock()
	if conn != nil && !conn.IsClosed() && channel != nil {
		return nil
	}
	if conn != nil {
		_ = conn.Close()
	}
	return initRabbitMQLocked()
}

// publishWithConfirm 是一个带有确认机制的发布消息函数
// 它会在 RabbitMQ 中发布消息并等待确认，如果失败会进行重试
// 参数:
//
//	ctx: 上下文，用于控制请求的超时和取消
//	exchange: 交换机名称
//	routingKey: 路由键
//	body: 消息体
//	headers: 消息头信息
//
// 返回值:
//
//	error: 错误信息，如果发布失败则返回错误
func publishWithConfirm(ctx context.Context, exchange, routingKey string, body []byte, headers amqp.Table) error {
	var lastErr error // 记录最后一次的错误信息
	// 循环尝试发布消息，最多尝试 maxPublishAttempts 次
	for attempt := 0; attempt < maxPublishAttempts; attempt++ {
		// 尝试重新连接 RabbitMQ
		if err := reconnect(); err != nil {
			lastErr = err
			time.Sleep(time.Duration(attempt+1) * 200 * time.Millisecond) // 指数退避等待
			continue
		}

		// 获取当前连接
		mqMu.Lock()
		currentConn := conn
		mqMu.Unlock()
		// 创建一个新的通道
		pubChannel, err := currentConn.Channel()
		if err != nil {
			lastErr = err
			_ = reconnect() // 如果创建通道失败，尝试重新连接
			continue
		}
		// 启用发布确认模式
		if err = pubChannel.Confirm(false); err != nil {
			_ = pubChannel.Close()
			lastErr = err
			continue
		}
		// 创建一个确认通道用于接收确认信息
		confirms := pubChannel.NotifyPublish(make(chan amqp.Confirmation, 1))
		// 发布消息
		err = pubChannel.PublishWithContext(ctx, exchange, routingKey, false, false, amqp.Publishing{
			DeliveryMode: amqp.Persistent,    // 持久化消息
			ContentType:  "application/json", // 内容类型为 JSON
			Headers:      headers,            // 消息头
			Body:         body,               // 消息体
		})
		// 如果发布成功，等待确认
		if err == nil {
			select {
			case confirmation := <-confirms: // 接收确认信息
				if !confirmation.Ack { // 如果消息被拒绝
					err = fmt.Errorf("rabbitmq publisher rejected message")
				}
			case <-ctx.Done(): // 上下文取消
				err = ctx.Err()
			case <-time.After(5 * time.Second): // 超时等待
				err = fmt.Errorf("rabbitmq publisher confirmation timeout")
			}
		}
		_ = pubChannel.Close() // 关闭通道
		// 如果发布成功且确认通过，返回 nil
		if err == nil {
			return nil
		}
		lastErr = err   // 记录错误
		_ = reconnect() // 尝试重新连接
	}
	// 如果所有尝试都失败，返回错误信息
	return fmt.Errorf("publish failed after %d attempts: %w", maxPublishAttempts, lastErr)
}

// ConsumeJSON 是一个持续消费队列中JSON格式消息的函数
// 它会在后台启动一个goroutine不断尝试从指定队列中获取消息并处理
//
// @param queue: 要消费的队列名称
// @param handler: 消息处理函数，接收JSON字节数组作为参数，返回处理错误
// @return error: 如果初始化连接失败则返回错误，否则返回nil
func ConsumeJSON(queue string, handler func([]byte) error) error {
	// 首先尝试重新建立连接，如果连接失败则直接返回错误
	if err := reconnect(); err != nil {
		return err
	}
	// 启动一个goroutine在后台持续消费消息
	go func() {
		// 无限循环，持续尝试消费消息
		for {
			if isStopping() {
				return
			}
			// 尝试消费一条消息，如果出错则打印日志并等待一秒后重试
			if err := consumeJSONOnce(queue, handler); err != nil {
				if isStopping() {
					return
				}
				log.Printf("consumer queue=%s stopped: %v; retrying", queue, err)
				select {
				case <-stopCh:
					return
				case <-time.After(time.Second):
				}
			}
		}
	}()
	return nil
}

func isStopping() bool {
	mqMu.Lock()
	defer mqMu.Unlock()
	if stopCh == nil {
		return false
	}
	select {
	case <-stopCh:
		return true
	default:
		return false
	}
}

// consumeJSONOnce 从指定队列消费JSON消息一次
// 参数:
//   - queue: 要消费的队列名称
//   - handler: 处理消息的回调函数，接收消息体([]byte)并返回错误
//
// 返回值:
//   - error: 如果操作过程中出现错误则返回错误信息
func consumeJSONOnce(queue string, handler func([]byte) error) error {
	// 尝试重新连接，如果连接失败则返回错误
	if err := reconnect(); err != nil {
		return err
	}
	// 获取当前连接，使用互斥锁确保线程安全
	mqMu.Lock()
	currentConn := conn
	mqMu.Unlock()
	// 创建一个新的通道
	consumerChannel, err := currentConn.Channel()
	if err != nil {
		return err
	}
	defer consumerChannel.Close() // 确保通道在使用后被关闭
	// 设置服务质量，确保一次只处理一条消息
	if err := consumerChannel.Qos(1, 0, false); err != nil {
		return err
	}
	// 开始消费队列中的消息
	msgs, err := consumerChannel.Consume(queue, "", false, false, false, false, nil)
	if err != nil {
		return err
	}
	// 遍历接收到的消息
	for msg := range msgs {
		// 调用处理函数处理消息
		if err := handler(msg.Body); err != nil {
			// 处理消息失败，重试计数逻辑
			retryCount := 0
			if raw, ok := msg.Headers[retryHeader]; ok {
				switch value := raw.(type) {
				case int:
					retryCount = value
				case int32:
					retryCount = int(value)
				case string:
					retryCount, _ = strconv.Atoi(value)
				}
			}
			// 如果达到最大重试次数，将消息移至死信队列
			if retryCount >= maxMessageRetries {
				if deadErr := publishWithConfirm(context.Background(), "", queue+".dead", msg.Body, nil); deadErr != nil {
					_ = msg.Nack(false, true) // 拒绝消息并重新入队
					continue
				}
				_ = msg.Ack(false) // 确认消息
				monitor.IncMQDeadLetter()
				monitor.Alert(context.Background(), "rabbitmq_dead_letter", map[string]string{"queue": queue, "retries": strconv.Itoa(retryCount)})
				log.Printf("message moved to dead-letter queue=%s after %d retries: %v", queue, retryCount, err)
				continue
			}
			// 否则增加重试次数并重新发布消息
			headers := amqp.Table{retryHeader: retryCount + 1}
			if retryErr := publishWithConfirm(context.Background(), "", queue, msg.Body, headers); retryErr != nil {
				_ = msg.Nack(false, true) // 拒绝消息并重新入队
				continue
			}
			_ = msg.Ack(false) // 确认消息
			continue
		}
		// 消息处理成功，确认消息
		_ = msg.Ack(false)
	}
	// 如果消费通道关闭，返回错误
	return fmt.Errorf("delivery channel closed")
}

func Ready() bool {
	mqMu.Lock()
	defer mqMu.Unlock()
	return conn != nil && !conn.IsClosed() && channel != nil
}

func validDeadLetterQueue(queue string) bool {
	for _, name := range []string{FileDeleteWorkQueue, OperationLogQueue, FileMergeQueue, CacheInvalidationQueue, FileTransformQueue, FileCleanupQueue} {
		if name == queue {
			return true
		}
	}
	return false
}

func DeadLetterCount(queue string) (int, error) {
	if !validDeadLetterQueue(queue) {
		return 0, fmt.Errorf("invalid dead-letter queue")
	}
	if err := reconnect(); err != nil {
		return 0, err
	}
	mqMu.Lock()
	currentConn := conn
	mqMu.Unlock()
	ch, err := currentConn.Channel()
	if err != nil {
		return 0, err
	}
	defer ch.Close()
	state, err := ch.QueueInspect(queue + ".dead")
	if err != nil {
		return 0, err
	}
	return state.Messages, nil
}

func ReplayDeadLetters(ctx context.Context, queue string, limit int) (int, error) {
	if !validDeadLetterQueue(queue) {
		return 0, fmt.Errorf("invalid dead-letter queue")
	}
	if limit <= 0 || limit > 500 {
		return 0, fmt.Errorf("invalid replay limit")
	}
	if err := reconnect(); err != nil {
		return 0, err
	}
	mqMu.Lock()
	currentConn := conn
	mqMu.Unlock()
	ch, err := currentConn.Channel()
	if err != nil {
		return 0, err
	}
	defer ch.Close()
	replayed := 0
	for replayed < limit {
		message, ok, err := ch.Get(queue+".dead", false)
		if err != nil {
			return replayed, err
		}
		if !ok {
			break
		}
		if err := publishWithConfirm(ctx, "", queue, message.Body, nil); err != nil {
			_ = message.Nack(false, true)
			return replayed, err
		}
		if err := message.Ack(false); err != nil {
			return replayed, err
		}
		replayed++
	}
	return replayed, nil
}

func Close() {
	mqMu.Lock()
	defer mqMu.Unlock()
	if stopCh != nil {
		select {
		case <-stopCh:
		default:
			close(stopCh)
		}
	}
	if channel != nil {
		channel.Close()
	}
	if conn != nil {
		conn.Close()
	}
}
