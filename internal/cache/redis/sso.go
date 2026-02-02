package redis

/**
 * @Description: 单点登录+鉴权 相关操作
 * 实现“后登录踢前一个登录”
 */

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"
)

// SetUserToken 设置用户token (单点登录)
// 同一用户只有一个有效token，新登录回覆盖旧token
func SetUserToken(ctx context.Context, username string, token string, ttl time.Duration) error {
	key := "user:" + username + ":token"
	return Rdb.Set(ctx, key, token, ttl).Err()
}

// GetUserToken 获取用户当前有效token
func GetUserToken(ctx context.Context, username string) (string, error) {
	key := "user:" + username + ":token"
	return Rdb.Get(ctx, key).Result()
}

// DeleteUserToken 删除用户token (用户登出)
func DeleteUserToken(ctx context.Context, username string) error {
	key := "user:" + username + ":token"
	return Rdb.Del(ctx, key).Err()
}

// GetOnlineDeviceCount 获取用户在线设备数
// 用于单点登录策略，只要token存在且未过期，设备数就是1，否则就是0
func GetOnlineDeviceCount(ctx context.Context, username string) (int, error) {
	key := "user:" + username + ":token"
	exists, err := Rdb.Exists(ctx, key).Result()
	if err != nil {
		return 0, err
	}
	return int(exists), nil
}

// IsTokenValid 校验token是否为用户当前有效token（用于鉴权中间件）
func IsTokenValid(ctx context.Context, username string, token string) (bool, error) {
	storedToken, err := GetUserToken(ctx, username)
	if err != nil {
		if err == redis.Nil {
			return false, nil
		}
		return false, err
	}
	return storedToken == token, nil
}
