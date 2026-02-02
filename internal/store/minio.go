package store

/**
 * @Description: MinIO初始化
 */

import (
	"context"
	"log"

	"file-storage-linhe/config"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

var MinioClient *minio.Client

func InitMinio() error {
	cli, err := minio.New(config.MinioEndpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(config.MinioAccessKey, config.MinioSecretKey, ""),
		Secure: config.MinioUseSSL,
	})
	if err != nil {
		return err
	}
	MinioClient = cli

	ctx := context.Background()
	exists, err := cli.BucketExists(ctx, config.MinioBucket)
	if err != nil {
		return err
	}
	if !exists {
		err = cli.MakeBucket(ctx, config.MinioBucket, minio.MakeBucketOptions{})
		if err != nil {
			return err
		}
	}
	log.Println("MinIO连接成功！")
	return nil
}
