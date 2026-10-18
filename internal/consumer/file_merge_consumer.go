package consumer

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"

	"file-storage-linhe/config"
	cacheRedis "file-storage-linhe/internal/cache/redis"
	"file-storage-linhe/internal/db"
	"file-storage-linhe/internal/meta"
	"file-storage-linhe/internal/mq"
	"file-storage-linhe/internal/store"
	"file-storage-linhe/util"

	"github.com/minio/minio-go/v7"
)

// StartFileMergeConsumer 启动分片合并消费者
func StartFileMergeConsumer() error {
	return mq.ConsumeFileMerge(func(msg *mq.FileMergeMessage) error {
		ctx := context.Background()

		log.Printf("开始合并分片: upload_id=%s, file=%s, chunks=%d, user=%s",
			msg.UploadID, msg.FileName, msg.ChunkCount, msg.Username)

		// 从 Redis 获取分片元信息
		chunkInfo, err := cacheRedis.GetChunkInfo(ctx, msg.UploadID)
		if err != nil {
			log.Printf("获取分片信息失败: %v", err)
			return err
		}
		if chunkInfo.Status == "completed" {
			log.Printf("分片合并任务已完成，跳过重复消息: upload_id=%s", msg.UploadID)
			return nil
		}

		// 创建临时合并文件
		mergedFile, err := os.CreateTemp("", "filestore-merge-*")
		if err != nil {
			log.Printf("创建合并文件失败: %v", err)
			return err
		}
		defer mergedFile.Close()
		defer os.Remove(mergedFile.Name())

		// 按序从 MinIO 读取并合并所有分片
		for i := 0; i < msg.ChunkCount; i++ {
			chunkKey := fmt.Sprintf("multipart/%s/%d", msg.UploadID, i)
			chunkObj, err := store.MinioClient.GetObject(
				ctx,
				config.MinioBucket,
				chunkKey,
				minio.GetObjectOptions{},
			)
			if err != nil {
				log.Printf("获取分片对象失败 multipart/%s/%d: %v", msg.UploadID, i, err)
				return err
			}

			// 将分片内容拷贝到合并文件
			if _, err := io.Copy(mergedFile, chunkObj); err != nil {
				chunkObj.Close()
				log.Printf("合并分片失败 multipart/%s/%d: %v", msg.UploadID, i, err)
				return err
			}
			chunkObj.Close()
		}

		// 计算完整文件的 SHA1
		mergedFile.Seek(0, 0)
		fileSize, fileSha1, err := util.ComputeSha1ByReader(mergedFile)
		if err != nil {
			log.Printf("计算文件哈希失败: %v", err)
			return err
		}
		log.Printf("文件哈希计算完成: %s, 大小: %d bytes", fileSha1, fileSize)

		// 上传到 MinIO
		mergedFile.Seek(0, 0)
		objectKey := fmt.Sprintf("files/%s", fileSha1)
		info, err := store.MinioClient.PutObject(
			ctx,
			config.MinioBucket,
			objectKey,
			mergedFile,
			fileSize,
			minio.PutObjectOptions{ContentType: "application/octet-stream"},
		)
		if err != nil {
			log.Printf("上传到 MinIO 失败: %v", err)
			return err
		}
		log.Printf("上传到 MinIO 成功: %s, 大小: %d bytes", objectKey, info.Size)

		// 写入数据库 tbl_file
		fileMeta := &meta.FileMeta{
			FileSha1: fileSha1,
			FileName: msg.FileName,
			FileSize: fileSize,
			Location: objectKey,
		}
		if err := db.InsertFileMeta(ctx, fileMeta); err != nil {
			log.Printf("写入文件元信息失败: %v", err)
			return err
		}

		// 写入 tbl_user_file
		if err := db.InsertUserFile(ctx, msg.Username, fileSha1, msg.FileName, fileSize); err != nil {
			log.Printf("写入用户文件关系失败: %v", err)
			return err
		}

		// 更新 Redis 任务状态为 completed
		if err := cacheRedis.UpdateChunkStatus(ctx, msg.UploadID, "completed"); err != nil {
			log.Printf("更新任务状态失败: %v", err)
		}

		// 更新 FileSha1 到 Redis
		if chunkInfo, err := cacheRedis.GetChunkInfo(ctx, msg.UploadID); err == nil {
			chunkInfo.FileSha1 = fileSha1
			chunkInfo.Status = "completed"
			cacheRedis.SetChunkInfo(ctx, chunkInfo)
		}

		// 清理 MinIO 中的分片对象
		for i := 0; i < msg.ChunkCount; i++ {
			chunkKey := fmt.Sprintf("multipart/%s/%d", msg.UploadID, i)
			if err := store.MinioClient.RemoveObject(
				ctx,
				config.MinioBucket,
				chunkKey,
				minio.RemoveObjectOptions{},
			); err != nil {
				log.Printf("删除分片对象失败 multipart/%s/%d: %v", msg.UploadID, i, err)
			}
		}

		if err := cacheRedis.DeleteChunkCache(ctx, msg.UploadID); err != nil {
			log.Printf("删除分片缓存失败: %v", err)
		}

		// 写入文件元信息缓存
		if err := cacheRedis.SetFileMetaCache(ctx, fileMeta); err != nil {
			log.Printf("写入文件元信息缓存失败: %v", err)
		}

		log.Printf("分片合并完成: upload_id=%s, file_sha1=%s, size=%d bytes, user=%s",
			msg.UploadID, fileSha1, fileSize, msg.Username)

		return nil
	})
}
