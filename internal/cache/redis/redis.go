package redis

import (
	"context"
	"file-storage-linhe/config"
	"log"

	"github.com/redis/go-redis/v9"
)

var Rdb *redis.Client

func InitRedis(ctx context.Context) error {
	Rdb = redis.NewClient(&redis.Options{
		Addr:     config.RedisAddr,
		Password: config.RedisPassword,
		DB:       config.RedisDB,
	})
	log.Println("Redis连接成功！")
	return Rdb.Ping(ctx).Err()
}
