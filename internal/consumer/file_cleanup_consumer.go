package consumer

import (
	"context"
	"fmt"
	"log"

	"file-storage-linhe/config"
	"file-storage-linhe/internal/cache/redis"
	"file-storage-linhe/internal/db"
	"file-storage-linhe/internal/mq"
	"file-storage-linhe/internal/store"

	"github.com/minio/minio-go/v7"
)

func StartFileCleanupConsumer() error {
	return mq.ConsumeFileCleanup(func(msg *mq.FileCleanupMessage) error {
		ctx := context.Background()
		if err := store.MinioClient.RemoveObject(ctx, config.MinioBucket, msg.Location, minio.RemoveObjectOptions{}); err != nil {
			return fmt.Errorf("remove object: %w", err)
		}
		if err := db.PermanentDeleteFileMeta(ctx, msg.FileHash); err != nil {
			return err
		}
		if err := redis.DeleteFileMetaCache(ctx, msg.FileHash); err != nil {
			log.Printf("delete file cache failed: %v", err)
		}
		if err := mq.PublishCacheInvalidation(ctx, &mq.CacheInvalidationMessage{CacheType: "file_meta", Key: msg.FileHash, Reason: "file_permanently_deleted"}); err != nil {
			return err
		}
		return nil
	})
}
