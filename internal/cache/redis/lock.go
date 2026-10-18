package redis

/**
 * @Description: 解决两个请求同时上传相同的文件，只保留一份底层MinIO对象
 */

import (
	"context"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

type Lock struct {
	Rdb      *redis.Client
	Context  context.Context
	Key      string
	Value    string
	TTL      time.Duration
	stopCh   chan struct{}
	stopOnce sync.Once
}

// NewLock 创建一个新的分布式锁实例
// 参数:
//   - ctx: 上下文，用于控制锁的生命周期和取消操作
//   - key: 锁的键名，用于标识唯一的锁
//   - ttl: 锁的生存时间，超过此时间自动释放
//
// 返回值:
//   - *Lock: 返回一个新创建的分布式锁实例
func NewLock(ctx context.Context, key string, ttl time.Duration) *Lock {
	return &Lock{
		Rdb:     Rdb,                 // Redis客户端实例，用于操作Redis
		Context: ctx,                 // 传入的上下文
		Key:     key,                 // 锁的键名
		Value:   uuid.NewString(),    // 锁的唯一值，用于防止误删其他客户端的锁
		TTL:     ttl,                 // 锁的生存时间
		stopCh:  make(chan struct{}), // 用于停止后台goroutine的通道
	}
}

// 尝试获取锁
// TryLock 尝试获取锁，如果获取成功则启动看门狗机制
// 参数：无
// 返回值：
//   - bool: 是否成功获取锁
//   - error: 错误信息，如果获取锁过程中出现错误则返回
func (l *Lock) TryLock() (bool, error) {
	// 使用Redis的SetNX命令尝试设置键值对，如果键不存在则设置成功并返回true
	// 同时设置TTL（生存时间），确保锁在未主动释放的情况下会自动过期
	ok, err := l.Rdb.SetNX(l.Context, l.Key, l.Value, l.TTL).Result()
	if err != nil {
		// 如果Redis操作出错，返回错误信息
		return false, err
	}
	if !ok {
		// 如果SetNX返回false，表示锁已被其他客户端持有，获取锁失败
		return false, nil
	}
	// 成功获取锁后，启动看门狗机制，定期续约锁的TTL
	go l.watchdog()
	return true, nil
}

// Watchdog 自动续期 goroutine
// watchdog 是一个锁的监控协程方法，用于定期续约锁的过期时间
func (l *Lock) watchdog() {
	// 创建一个定时器，每隔 TTL/3 的时间触发一次
	ticker := time.NewTicker(l.TTL / 3)
	defer ticker.Stop()

	// 定义续约锁的 Redis Lua 脚本
	// 脚本逻辑：如果锁的值与当前值匹配，则更新锁的过期时间；否则返回0
	const renewScript = `
		if redis.call("GET", KEYS[1]) == ARGV[1] then
			return redis.call("PEXPIRE", KEYS[1], ARGV[2])
		else
			return 0
		end
	`

	// 无限循环，监控锁的状态
	for {
		select {
		// 接收到停止信号时，退出监控协程
		case <-l.stopCh:
			return
		// 定时器触发时，执行续约操作
		case <-ticker.C:
			// 执行 Redis 续约脚本，传入锁的键、值和过期时间（毫秒）
			result, err := l.Rdb.Eval(l.Context, renewScript, []string{l.Key}, l.Value, l.TTL.Milliseconds()).Int64()
			// 如果执行出错或续约失败（返回0），则退出监控协程
			if err != nil || result == 0 {
				return
			}
		}
	}
}

// 释放锁
// Unlock 方法用于释放锁
func (l *Lock) Unlock() error {
	// 使用 stopOnce 确保停止通道只关闭一次
	l.stopOnce.Do(func() {
		close(l.stopCh)
	})

	// 定义 Lua 脚本，用于在 Redis 中安全地释放锁
	// 脚本逻辑：如果键的值等于传入的值，则删除该键，否则返回0
	const luaScript = `
		if redis.call("GET", KEYS[1]) == ARGV[1] then
			return redis.call("DEL", KEYS[1])
		else
			return 0
		end
	`

	// 在 Redis 中执行 Lua 脚本，释放锁
	// 参数：上下文、键名、键值
	_, err := l.Rdb.Eval(l.Context, luaScript, []string{l.Key}, l.Value).Result()
	return err
}
