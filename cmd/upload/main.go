package main

import (
	"context"
	"file-storage-linhe/internal/cache/redis"
	"file-storage-linhe/internal/db"
	"file-storage-linhe/internal/handler"
	"file-storage-linhe/internal/handler/auth"
	"file-storage-linhe/internal/mq"
	"file-storage-linhe/internal/store"

	"log"
	"net/http"
)

// 主函数，程序的入口点
func main() {
	// 初始化数据库连接，如果失败则记录错误并退出程序
	if err := db.InitDB(); err != nil {
		log.Fatalf("init db failed: %v", err)
	}

	// 初始化Minio对象存储服务，如果失败则记录错误并退出程序
	if err := store.InitMinio(); err != nil {
		log.Fatalf("init minio failed: %v", err)
	}

	// 初始化Redis缓存服务，如果失败则记录错误并退出程序
	if err := redis.InitRedis(context.Background()); err != nil {
		log.Fatalf("init redis failed: %v", err)
	}

	// 初始化RabbitMQ消息队列服务，如果失败则记录错误并退出程序
	if err := mq.InitRabbitMQ(); err != nil {
		log.Fatalf("init rabbitmq failed: %v", err)
	}
	// 确保程序退出时关闭RabbitMQ连接
	defer mq.Close()

	// 用户接口
	http.HandleFunc("/user/signup", handler.RecoverMiddleware(handler.SignupHandler))
	http.HandleFunc("/user/signin", handler.RecoverMiddleware(handler.SigninHandler))
	http.HandleFunc("/user/info", handler.RecoverMiddleware(auth.Auth(handler.UserInfoHandler)))
	http.HandleFunc("/user/signout", handler.RecoverMiddleware(auth.Auth(handler.SignoutHandler)))
	http.HandleFunc("/user/online-devices", handler.RecoverMiddleware(auth.Auth(handler.OnlineDevicesHandler)))

	// 文件接口
	http.HandleFunc("/file/upload", handler.RecoverMiddleware(auth.Auth(handler.UploadHandler)))
	http.HandleFunc("/file/download", handler.RecoverMiddleware(auth.Auth(handler.DownloadHandler)))
	http.HandleFunc("/file/download/batch", handler.RecoverMiddleware(auth.Auth(handler.BatchDownloadHandler)))
	http.HandleFunc("/file/meta", handler.RecoverMiddleware(auth.Auth(handler.FileMetaHandler)))
	http.HandleFunc("/file/fastupload", handler.RecoverMiddleware(auth.Auth(handler.FastUploadHandler)))
	http.HandleFunc("/file/delete", handler.RecoverMiddleware(auth.Auth(handler.DeleteHandler)))
	http.HandleFunc("/file/multipart/init", handler.RecoverMiddleware(auth.Auth(handler.MultipartInitHandler)))
	http.HandleFunc("/file/multipart/upload", handler.RecoverMiddleware(auth.Auth(handler.MultipartUploadHandler)))
	http.HandleFunc("/file/multipart/status", handler.RecoverMiddleware(auth.Auth(handler.MultipartStatusHandler)))
	http.HandleFunc("/file/multipart/complete", handler.RecoverMiddleware(auth.Auth(handler.MultipartCompleteHandler)))
	http.HandleFunc("/file/multipart/merge-status", handler.RecoverMiddleware(auth.Auth(handler.MultipartMergeStatusHandler)))

	// 回收站接口
	http.HandleFunc("/file/recycle", handler.RecoverMiddleware(auth.Auth(handler.RecycleHandler)))
	http.HandleFunc("/file/restore", handler.RecoverMiddleware(auth.Auth(handler.RestoreFileHandler)))
	http.HandleFunc("/file/permanentdelete", handler.RecoverMiddleware(auth.Auth(handler.PermanentDeleteHandler)))

	// 操作日志接口
	http.HandleFunc("/user/logs", handler.RecoverMiddleware(auth.Auth(handler.UserLogsHandler)))

	// 健康检查
	http.HandleFunc("/health", handler.RecoverMiddleware(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("OK"))
	}))
	http.HandleFunc("/ready", handler.RecoverMiddleware(handler.ReadyHandler))
	http.HandleFunc("/metrics", handler.RecoverMiddleware(handler.MetricsHandler))
	http.HandleFunc("/admin/outbox", handler.RecoverMiddleware(handler.OutboxAdminHandler))
	http.HandleFunc("/admin/deadletters", handler.RecoverMiddleware(handler.DeadLetterAdminHandler))

	addr := ":8080"
	log.Println("上传服务监听在", addr)
	if err := http.ListenAndServe(addr, nil); err != nil {
		log.Fatalf("启动服务失败: %v", err)
	}
}
