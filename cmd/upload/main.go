package main

import (
	"context"
	"file-storage-linhe/internal/cache/redis"
	"file-storage-linhe/internal/consumer"
	"file-storage-linhe/internal/db"
	"file-storage-linhe/internal/handler"
	"file-storage-linhe/internal/handler/auth"
	"file-storage-linhe/internal/mq"
	"file-storage-linhe/internal/store"

	"log"
	"net/http"
)

func main() {
	if err := db.InitDB(); err != nil {
		log.Fatalf("init db failed: %v", err)
	}

	if err := store.InitMinio(); err != nil {
		log.Fatalf("init minio failed: %v", err)
	}

	if err := redis.InitRedis(context.Background()); err != nil {
		log.Fatalf("init redis failed: %v", err)
	}

	if err := mq.InitRabbitMQ(); err != nil {
		log.Fatalf("init rabbitmq failed: %v", err)
	}
	defer mq.Close()

	if err := consumer.StartFileDeleteConsumer(); err != nil {
		log.Fatalf("start file delete consumer failed: %v", err)
	}

	// 启动操作日志消费者
	if err := consumer.StartOperationLogConsumer(); err != nil {
		log.Fatalf("start operation log consumer failed: %v", err)
	}

	// 用户接口
	http.HandleFunc("/user/signup", handler.RecoverMiddleware(handler.SignupHandler))
	http.HandleFunc("/user/signin", handler.RecoverMiddleware(handler.SigninHandler))
	http.HandleFunc("/user/info", handler.RecoverMiddleware(auth.Auth(handler.UserInfoHandler)))
	http.HandleFunc("/user/signout", handler.RecoverMiddleware(auth.Auth(handler.SignoutHandler)))
	http.HandleFunc("/user/online-devices", handler.RecoverMiddleware(auth.Auth(handler.OnlineDevicesHandler)))

	// 文件接口
	http.HandleFunc("/file/upload", handler.RecoverMiddleware(auth.Auth(handler.UploadHandler)))
	http.HandleFunc("/file/download", handler.RecoverMiddleware(auth.Auth(handler.DownloadHandler)))
	http.HandleFunc("/file/meta", handler.RecoverMiddleware(auth.Auth(handler.FileMetaHandler)))
	http.HandleFunc("/file/fastupload", handler.RecoverMiddleware(auth.Auth(handler.FastUploadHandler)))
	http.HandleFunc("/file/delete", handler.RecoverMiddleware(auth.Auth(handler.DeleteHandler)))
	http.HandleFunc("/file/multipart/init", handler.RecoverMiddleware(auth.Auth(handler.MultipartInitHandler)))
	http.HandleFunc("/file/multipart/upload", handler.RecoverMiddleware(auth.Auth(handler.MultipartUploadHandler)))
	http.HandleFunc("/file/multipart/status", handler.RecoverMiddleware(auth.Auth(handler.MultipartStatusHandler)))
	http.HandleFunc("/file/multipart/complete", handler.RecoverMiddleware(auth.Auth(handler.MultipartCompleteHandler)))

	// 回收站接口
	http.HandleFunc("/file/recycle", handler.RecoverMiddleware(auth.Auth(handler.RecycleHandler)))
	http.HandleFunc("/file/restore", handler.RecoverMiddleware(auth.Auth(handler.RestoreFileHandler)))

	// 操作日志接口
	http.HandleFunc("/user/logs", handler.RecoverMiddleware(auth.Auth(handler.UserLogsHandler)))


	// 健康检查
	http.HandleFunc("/health", handler.RecoverMiddleware(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("OK"))
	}))

	addr := ":8080"
	log.Println("上传服务监听在", addr)
	if err := http.ListenAndServe(addr, nil); err != nil {
		log.Fatalf("启动服务失败: %v", err)
	}
}
