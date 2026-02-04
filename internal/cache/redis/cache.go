package redis

/**
 * @Description: 文件元信息和用户信息的Redis缓存封装
 * 实现"先查缓存，miss则查DB并回写"的缓存模式
 */

import (
	"context"
	"encoding/json"
	"file-storage-linhe/internal/meta"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	FileMetaCacheTTL = 10 * time.Minute // 文件元信息缓存10分钟
	UserInfoCacheTTL = 5 * time.Minute  // 用户信息缓存5分钟
)

// ======================= 文件元信息缓存 =======================

// GetFileMetaCache 从缓存获取文件元信息
// 如果缓存未命中，调用 dbFetcher 查询 DB 并自动回写缓存
func GetFileMetaCache(ctx context.Context, sha1 string, dbFetcher func(context.Context, string) (*meta.FileMeta, error)) (*meta.FileMeta, error) {
	key := "cache:file:meta:" + sha1

	// 1. 先查 Redis
	data, err := Rdb.Get(ctx, key).Result()
	if err == nil {
		// 缓存命中，反序列化
		var fm meta.FileMeta
		if json.Unmarshal([]byte(data), &fm) == nil {
			return &fm, nil
		}
		// 反序列化失败，继续查 DB
	} else if err != redis.Nil {
		// Redis 出错（不是 key 不存在），记录日志但继续查 DB
		// log.Printf("redis get error: %v", err)
	}

	// 2. 缓存未命中或反序列化失败，查 DB
	fm, err := dbFetcher(ctx, sha1)
	if err != nil {
		return nil, err
	}

	// 3. 写回 Redis
	if data, err := json.Marshal(fm); err == nil {
		_ = Rdb.Set(ctx, key, data, FileMetaCacheTTL).Err()
	}

	return fm, nil
}

// SetFileMetaCache 主动更新文件元信息缓存
// 在 InsertFileMeta 成功后调用
func SetFileMetaCache(ctx context.Context, fm *meta.FileMeta) error {
	key := "cache:file:meta:" + fm.FileSha1
	data, err := json.Marshal(fm)
	if err != nil {
		return err
	}
	return Rdb.Set(ctx, key, data, FileMetaCacheTTL).Err()
}

// DeleteFileMetaCache 删除文件元信息缓存
// 在文件被删除时调用
func DeleteFileMetaCache(ctx context.Context, sha1 string) error {
	key := "cache:file:meta:" + sha1
	return Rdb.Del(ctx, key).Err()
}

// ======================= 用户信息缓存 =======================

// User 简化的用户信息结构（用于缓存）
type User struct {
	UserName string    `json:"user_name"`
	SignupAt time.Time `json:"signup_at"`
}

// GetUserInfoCache 从缓存获取用户基本信息
// 如果缓存未命中，调用 dbFetcher 查询 DB 并自动回写缓存
func GetUserInfoCache(ctx context.Context, username string, dbFetcher func(context.Context, string) (*User, error)) (*User, error) {
	key := "cache:user:info:" + username

	// 1. 先查 Redis
	data, err := Rdb.Get(ctx, key).Result()
	if err == nil {
		// 缓存命中
		var user User
		if json.Unmarshal([]byte(data), &user) == nil {
			return &user, nil
		}
	} else if err != redis.Nil {
		// Redis 出错但不是 key 不存在
		// log.Printf("redis get error: %v", err)
	}

	// 2. 缓存未命中，查 DB
	user, err := dbFetcher(ctx, username)
	if err != nil {
		return nil, err
	}

	// 3. 写回 Redis
	if data, err := json.Marshal(user); err == nil {
		_ = Rdb.Set(ctx, key, data, UserInfoCacheTTL).Err()
	}

	return user, nil
}

// SetUserInfoCache 主动更新用户信息缓存
func SetUserInfoCache(ctx context.Context, user *User) error {
	key := "cache:user:info:" + user.UserName
	data, err := json.Marshal(user)
	if err != nil {
		return err
	}
	return Rdb.Set(ctx, key, data, UserInfoCacheTTL).Err()
}

// DeleteUserInfoCache 删除用户信息缓存
func DeleteUserInfoCache(ctx context.Context, username string) error {
	key := "cache:user:info:" + username
	return Rdb.Del(ctx, key).Err()
}
