package consumer

import (
	"context"
	"log"

	cacheRedis "file-storage-linhe/internal/cache/redis"
	"file-storage-linhe/internal/mq"
)

// StartCacheInvalidationConsumer 启动缓存失效消费者
func StartCacheInvalidationConsumer() error {
	return mq.ConsumeCacheInvalidation(func(msg *mq.CacheInvalidationMessage) error {
		ctx := context.Background()

		log.Printf("收到缓存失效消息: cache_type=%s, key=%s, reason=%s",
			msg.CacheType, msg.Key, msg.Reason)

		switch msg.CacheType {
		case "file_meta":
			if err := cacheRedis.DeleteFileMetaCache(ctx, msg.Key); err != nil {
				log.Printf("删除文件元信息缓存失败: key=%s, err=%v", msg.Key, err)
				return err
			}
			log.Printf("文件元信息缓存已清理: key=%s", msg.Key)
		case "user_info":
			if err := cacheRedis.DeleteUserInfoCache(ctx, msg.Key); err != nil {
				log.Printf("删除用户信息缓存失败: key=%s, err=%v", msg.Key, err)
				return err
			}
			log.Printf("用户信息缓存已清理: key=%s", msg.Key)
		default:
			log.Printf("未知的缓存类型: %s", msg.CacheType)
		}

		return nil
	})
}
