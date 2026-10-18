package redis

import (
	"context"
	"file-storage-linhe/config"
	"log"

	"github.com/redis/go-redis/v9"
)

var Rdb *redis.Client

// InitRedis 初始化Redis客户端连接
// 参数:
//   - ctx: context.Context 用于控制请求的超时和取消
//
// 返回值:
//   - error: 如果连接失败则返回错误信息
func InitRedis(ctx context.Context) error {
	// 创建一个新的Redis客户端
	// 配置包括地址、密码和数据库编号
	Rdb = redis.NewClient(&redis.Options{
		Addr:     config.RedisAddr,     // Redis服务器地址
		Password: config.RedisPassword, // Redis服务器密码
		DB:       config.RedisDB,       // 使用的数据库编号
	})
	// 打印连接成功的日志信息
	log.Println("Redis连接成功！")
	// 通过Ping命令测试连接是否正常
	// 如果连接失败则返回错误
	return Rdb.Ping(ctx).Err()
}
