package redis

/**
 * @Description: 解决两个请求同时上传相同的文件，只保留一份底层MinIO对象
 */

import (
	"context"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

type Lock struct {
	Rdb     *redis.Client
	Context context.Context
	Key     string
	Value   string
	TTL     time.Duration
}

func NewLock(ctx context.Context, key string, ttl time.Duration) *Lock {
	return &Lock{
		Rdb:     Rdb,
		Context: ctx,
		Key:     key,
		Value:   uuid.NewString(),
		TTL:     ttl,
	}
}

// 尝试获取锁
func (l *Lock) TryLock() (bool, error) {
	ok, err := l.Rdb.SetNX(l.Context, l.Key, l.Value, l.TTL).Result()
	if err != nil {
		return false, err
	}
	if !ok {
		return false, nil
	}
	return true, nil
}

// 释放锁
func (l *Lock) Unlock() error {
	const luaScript = `
		if redis.call("GET", KEYS[1]) == ARGV[1] then
			return redis.call("DEL", KEYS[1])
		else
			return 0
		end
	`

	_, err := l.Rdb.Eval(l.Context, luaScript, []string{l.Key}, l.Value).Result()
	return err
}
