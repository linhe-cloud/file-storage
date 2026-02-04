package consumer

import (
    "context"
    "log"
    "time"

    "file-storage-linhe/config"
    cacheRedis "file-storage-linhe/internal/cache/redis"
    "file-storage-linhe/internal/db"
    "file-storage-linhe/internal/mq"
    "file-storage-linhe/internal/store"

    "github.com/minio/minio-go/v7"
)

// 启动文件删除消费者
func StartFileDeleteConsumer() error {
    return mq.ConsumeFileDeleteMessages(func(msg *mq.FileDeleteMessage) error {
        ctx := context.Background()

        log.Printf(
            "Delay delete task expired: username=%s, filehash=%s, deleted_at=%s",
            msg.Username, msg.FileHash, msg.DeletedAt.Format(time.RFC3339),
        )

        // 1. 再查一遍用户-文件状态，防止用户在延迟期间恢复
        status, err := db.CheckUserFileStatus(ctx, msg.Username, msg.FileHash)
        if err != nil {
            log.Printf("Failed to check user file status: %v", err)
            return err
        }
        if status == 0 {
            // 0 表示正常（已恢复或未删除），这时不应该物理删
            log.Printf("File has been restored, skip delete: filehash=%s", msg.FileHash)
            return nil
        }

        // 2. 查文件元信息（拿 MinIO key）
        fm, err := db.GetFileMeta(ctx, msg.FileHash)
        if err != nil || fm == nil {
            log.Printf("Failed to get file meta, skip: %v", err)
            return nil // 元信息都没有了，说明之前可能已经删过，跳过即可
        }

        // 3. 永久删除这一条用户-文件关系（这里是硬删）
        if err := db.PermanentDeleteUserFile(ctx, msg.Username, msg.FileHash); err != nil {
            log.Printf("Failed to permanently delete user file: %v", err)
            return err
        }

        // 4. 检查这个 filehash 是否还被其他用户使用
        stillUsed, err := db.ExistsUserFileByHash(ctx, msg.FileHash)
        if err != nil {
            log.Printf("Failed to check user file existence: %v", err)
            return err
        }

        if stillUsed {
            // 还有其他用户引用：只删当前用户关系，不动 MinIO
            log.Printf("File still used by other users, only deleted relationship: filehash=%s", msg.FileHash)
            return nil
        }

        // 5. 没人用了 → 删除 MinIO 对象 + 清缓存 + 标记 tbl_file 删除
        if err := store.MinioClient.RemoveObject(
            ctx,
            config.MinioBucket,
            fm.Location,
            minio.RemoveObjectOptions{},
        ); err != nil {
            log.Printf("Failed to delete MinIO object: %v", err)
            return err
        }

        _ = cacheRedis.DeleteFileMetaCache(ctx, msg.FileHash)
        _ = db.PermanentDeleteFileMeta(ctx, msg.FileHash)

        log.Printf("File deleted successfully: filehash=%s, location=%s", msg.FileHash, fm.Location)
        return nil
    })
}