package redis

/**
 * @Description: 文件元信息和用户信息的Redis缓存封装
 * 实现"先查缓存，miss则查DB并回写"的缓存模式
 */

import (
	"context"
	"encoding/json"
	"file-storage-linhe/internal/meta"
	"fmt"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	FileMetaCacheTTL  = 10 * time.Minute // 文件元信息缓存10分钟
	UserInfoCacheTTL  = 5 * time.Minute  // 用户信息缓存5分钟
	ChunkInfoCacheTTL = 24 * time.Hour   // 分片信息缓存24小时
)

// ======================= 文件元信息缓存 =======================

// GetFileMetaCache 从缓存获取文件元信息
// 如果缓存未命中，调用 dbFetcher 查询 DB 并自动回写缓存
// GetFileMetaCache 获取文件元数据的缓存函数，实现了缓存优先的策略
// @param ctx 上下文信息
// @param sha1 文件的SHA1值，作为缓存的key
// @param dbFetcher 数据库查询函数，用于从数据库中获取文件元数据
// @return *meta.FileMeta 文件元数据指针
// @return error 错误信息
func GetFileMetaCache(ctx context.Context, sha1 string, dbFetcher func(context.Context, string) (*meta.FileMeta, error)) (*meta.FileMeta, error) {
	// 构建Redis缓存的key
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
// SetFileMetaCache 设置文件元数据到缓存中
// 参数:
//   - ctx: 上下文信息，用于控制请求的超时和取消
//   - fm: 文件元数据指针，包含文件的相关信息
//
// 返回值:
//   - error: 操作过程中发生的错误，如果成功则为nil
func SetFileMetaCache(ctx context.Context, fm *meta.FileMeta) error {
	// 构建缓存键，使用文件SHA1值作为唯一标识
	key := "cache:file:meta:" + fm.FileSha1
	// 将文件元数据结构体序列化为JSON格式
	data, err := json.Marshal(fm)
	if err != nil {
		// 如果序列化失败，返回错误
		return err
	}
	// 将序列化后的数据存入Redis，并设置过期时间
	return Rdb.Set(ctx, key, data, FileMetaCacheTTL).Err()
}

// DeleteFileMetaCache 删除文件元信息缓存
// 在文件被删除时调用
// DeleteFileMetaCache 根据文件的SHA1值删除文件元数据缓存
// @param ctx 上下文，用于控制请求的取消和超时
// @param sha1 文件的SHA1值，用于唯一标识文件
// @return error 操作失败的错误信息，如果删除成功则返回nil
func DeleteFileMetaCache(ctx context.Context, sha1 string) error {
	// 构建Redis中的缓存键，格式为"cache:file:meta:" + sha1
	key := "cache:file:meta:" + sha1
	// 执行Redis删除操作，删除指定键的缓存数据
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
// GetUserInfoCache 获取用户信息缓存函数
// 该函数实现了从缓存中获取用户信息，如果缓存未命中则从数据库获取并更新缓存
//
// @param ctx 上下文信息，用于控制请求的超时和取消
// @param username 用户名，作为缓存的key
// @param dbFetcher 数据库查询函数，用于在缓存未命中时获取用户信息
// @return *User 用户信息指针
// @return error 错误信息
func GetUserInfoCache(ctx context.Context, username string, dbFetcher func(context.Context, string) (*User, error)) (*User, error) {
	// 构建Redis缓存key
	key := "cache:user:info:" + username

	// 1. 先查 Redis
	data, err := Rdb.Get(ctx, key).Result()
	if err == nil {
		// 缓存命中，解析JSON数据并返回用户信息
		var user User
		if json.Unmarshal([]byte(data), &user) == nil {
			return &user, nil
		}
	} else if err != redis.Nil {
		// Redis 出错但不是 key 不存在的情况
		// 这里可以选择记录错误日志，当前代码已注释
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
// SetUserInfoCache 将用户信息设置到缓存中
// 参数:
//   - ctx: 上下文信息，用于控制请求的超时和取消
//   - user: 用户信息结构体指针，包含需要缓存的用户数据
//
// 返回值:
//   - error: 操作过程中可能出现的错误
func SetUserInfoCache(ctx context.Context, user *User) error {
	// 构建缓存键，使用用户名作为唯一标识
	key := "cache:user:info:" + user.UserName
	// 将用户结构体序列化为JSON格式
	data, err := json.Marshal(user)
	if err != nil {
		// 如果序列化失败，直接返回错误
		return err
	}
	// 将序列化后的数据存入Redis，并设置过期时间
	return Rdb.Set(ctx, key, data, UserInfoCacheTTL).Err()
}

// DeleteUserInfoCache 删除用户信息缓存
func DeleteUserInfoCache(ctx context.Context, username string) error {
	key := "cache:user:info:" + username
	return Rdb.Del(ctx, key).Err()
}

// ======================= 分片上传缓存 =======================

// ChunkInfo 分片上传任务信息
type ChunkInfo struct {
	UploadID   string `json:"upload_id"`
	FileName   string `json:"file_name"`
	FileSize   int64  `json:"file_size"`
	ChunkSize  int64  `json:"chunk_size"`
	ChunkCount int    `json:"chunk_count"`
	FileSha1   string `json:"file_sha1,omitempty"`
	Username   string `json:"username"`
	Status     string `json:"status"` // uploading / completed / failed
}

func chunkInfoKey(uploadID string) string { return "multipart:info:" + uploadID }

func chunkSetKey(uploadID string) string { return "multipart:chunks:" + uploadID }

// GetChunkInfo 从 Redis 获取分片上传任务信息
func GetChunkInfo(ctx context.Context, uploadID string) (*ChunkInfo, error) {
	values, err := Rdb.HGetAll(ctx, chunkInfoKey(uploadID)).Result()
	if err != nil {
		return nil, err
	}
	if len(values) == 0 {
		return nil, redis.Nil
	}
	info := &ChunkInfo{
		UploadID: values["upload_id"],
		FileName: values["file_name"],
		FileSha1: values["file_sha1"],
		Username: values["username"],
		Status:   values["status"],
	}
	if info.UploadID == "" {
		info.UploadID = uploadID
	}
	if info.FileSize, err = strconv.ParseInt(values["file_size"], 10, 64); err != nil {
		return nil, fmt.Errorf("invalid upload file_size: %w", err)
	}
	if info.ChunkSize, err = strconv.ParseInt(values["chunk_size"], 10, 64); err != nil {
		return nil, fmt.Errorf("invalid upload chunk_size: %w", err)
	}
	if info.ChunkCount, err = strconv.Atoi(values["chunk_count"]); err != nil {
		return nil, fmt.Errorf("invalid upload chunk_count: %w", err)
	}
	return info, nil
}

// SetChunkInfo 设置分片上传任务信息
func SetChunkInfo(ctx context.Context, info *ChunkInfo) error {
	key := chunkInfoKey(info.UploadID)
	if _, err := Rdb.HSet(ctx, key, map[string]interface{}{
		"upload_id":   info.UploadID,
		"file_name":   info.FileName,
		"file_size":   info.FileSize,
		"chunk_size":  info.ChunkSize,
		"chunk_count": info.ChunkCount,
		"file_sha1":   info.FileSha1,
		"username":    info.Username,
		"status":      info.Status,
	}).Result(); err != nil {
		return err
	}
	return Rdb.Expire(ctx, key, ChunkInfoCacheTTL).Err()
}

// UpdateChunkStatus 更新分片上传任务状态
func UpdateChunkStatus(ctx context.Context, uploadID, status string) error {
	// 获取现有信息
	info, err := GetChunkInfo(ctx, uploadID)
	if err != nil {
		return err
	}

	// 更新状态
	info.Status = status
	return SetChunkInfo(ctx, info)
}

// GetChunkStatus 获取分片上传任务状态
func GetChunkStatus(ctx context.Context, uploadID string) (string, error) {
	info, err := GetChunkInfo(ctx, uploadID)
	if err != nil {
		return "", err
	}
	return info.Status, nil
}

// DeleteChunkCache 删除分片上传相关缓存
func DeleteChunkCache(ctx context.Context, uploadID string) error {
	// 保留任务信息，供合并状态接口读取 completed/failed 结果；只清理分片集合。
	chunksKey := chunkSetKey(uploadID)
	return Rdb.Del(ctx, chunksKey).Err()
}

// GetUploadedChunkCount 获取已上传的分片数量
func GetUploadedChunkCount(ctx context.Context, uploadID string) (int, error) {
	key := chunkSetKey(uploadID)
	count, err := Rdb.SCard(ctx, key).Result()
	if err != nil {
		return 0, err
	}
	return int(count), nil
}
