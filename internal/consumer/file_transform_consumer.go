package consumer

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"

	"file-storage-linhe/config"
	"file-storage-linhe/internal/db"
	"file-storage-linhe/internal/mq"
	"file-storage-linhe/internal/store"
	"file-storage-linhe/internal/transform"

	"github.com/minio/minio-go/v7"
)

func StartFileTransformConsumer() error {
	return mq.ConsumeFileTransform(func(msg *mq.FileTransformMessage) error {
		ctx := context.Background()

		log.Printf("开始文件转换: file_hash=%s, file_name=%s, user=%s",
			msg.FileHash, msg.FileName, msg.Username)

		fm, err := db.GetFileMeta(ctx, msg.FileHash)
		if err != nil || fm == nil {
			log.Printf("获取文件元信息失败: %v", err)
			return fmt.Errorf("获取文件元信息失败: %w", err)
		}

		obj, err := store.MinioClient.GetObject(
			ctx,
			config.MinioBucket,
			fm.Location,
			minio.GetObjectOptions{},
		)
		if err != nil {
			log.Printf("从 MinIO 获取文件失败: %v", err)
			return fmt.Errorf("从 MinIO 获取文件失败: %w", err)
		}
		defer obj.Close()

		tempCSVPath := filepath.Join("/tmp", fmt.Sprintf("csv_%s", msg.FileHash))
		tempCSV, err := os.Create(tempCSVPath)
		if err != nil {
			log.Printf("创建临时 CSV 文件失败: %v", err)
			return fmt.Errorf("创建临时 CSV 文件失败: %w", err)
		}

		if _, err := io.Copy(tempCSV, obj); err != nil {
			tempCSV.Close()
			os.Remove(tempCSVPath)
			log.Printf("写入临时 CSV 文件失败: %v", err)
			return fmt.Errorf("写入临时 CSV 文件失败: %w", err)
		}
		tempCSV.Close()

		tempParquetPath := filepath.Join("/tmp", fmt.Sprintf("parquet_%s", msg.FileHash))
		defer os.Remove(tempCSVPath)
		defer os.Remove(tempParquetPath)

		csvFile, err := os.Open(tempCSVPath)
		if err != nil {
			log.Printf("打开临时 CSV 文件失败: %v", err)
			return fmt.Errorf("打开临时 CSV 文件失败: %w", err)
		}
		defer csvFile.Close()

		if err := transform.ConvertCSVToParquet(ctx, csvFile, tempParquetPath); err != nil {
			log.Printf("CSV 转 Parquet 失败: %v", err)
			return fmt.Errorf("CSV 转 Parquet 失败: %w", err)
		}

		parquetFile, err := os.Open(tempParquetPath)
		if err != nil {
			log.Printf("打开临时 Parquet 文件失败: %v", err)
			return fmt.Errorf("打开临时 Parquet 文件失败: %w", err)
		}
		defer parquetFile.Close()

		parquetKey := fm.Location + ".parquet"
		_, err = store.MinioClient.PutObject(
			ctx,
			config.MinioBucket,
			parquetKey,
			parquetFile,
			-1,
			minio.PutObjectOptions{
				ContentType: "application/octet-stream",
			},
		)
		if err != nil {
			log.Printf("上传 Parquet 到 MinIO 失败: %v", err)
			return fmt.Errorf("上传 Parquet 到 MinIO 失败: %w", err)
		}

		log.Printf("文件转换完成: file_hash=%s, parquet_key=%s", msg.FileHash, parquetKey)
		return nil
	})
}
