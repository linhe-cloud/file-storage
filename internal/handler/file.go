package handler

/**
 * @Description: 文件处理相关
 */

import (
	"file-storage-linhe/config"
	cacheRedis "file-storage-linhe/internal/cache/redis"
	"file-storage-linhe/internal/db"
	"file-storage-linhe/internal/handler/auth"
	"file-storage-linhe/internal/meta"
	"file-storage-linhe/internal/mq"
	"file-storage-linhe/internal/store"
	"file-storage-linhe/util"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/google/uuid"
	"github.com/minio/minio-go/v7"
)

// ======================= 上传 & 下载 =======================

// 上传文件：POST /file/upload
func UploadHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	// 获取当前用户名
	username, ok := auth.UsernameFromContext(r.Context())
	if !ok || username == "" {
		w.WriteHeader(http.StatusUnauthorized)
		return
	}

	if err := r.ParseMultipartForm(32 << 20); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	file, header, err := r.FormFile("file")
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	defer file.Close()

	fileMeta := &meta.FileMeta{
		FileName:   header.Filename,
		Location:   "/tmp/" + header.Filename,
		UploadTime: time.Now(),
	}
	newFile, err := os.Create(fileMeta.Location)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	defer newFile.Close()

	fileMeta.FileSize, err = io.Copy(newFile, file)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	newFile.Seek(0, 0)
	fileMeta.FileSha1 = util.FileSha1(newFile)

	//基于文件哈希的分布式锁（避免重复上传同一底层对象）
	lockKey := "lock:" + fileMeta.FileSha1
	lock := cacheRedis.NewLock(r.Context(), lockKey, time.Second*10)
	locked, err := lock.TryLock()
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "failed to acquire lock"})
		return
	}
	if !locked {
		// 当前同一文件正在上传，提示客户端稍后重试
		writeJSON(w, http.StatusConflict, map[string]string{"error": "file is being uploaded"})
		return
	}
	defer lock.Unlock()

	// 游标重新回到文件头部，准备上传到 MinIO
	newFile.Seek(0, 0)

	// 上传到 MinIO
	objectKey := "files/" + fileMeta.FileSha1
	info, err := store.MinioClient.PutObject(
		r.Context(),
		config.MinioBucket,
		objectKey,
		newFile,
		fileMeta.FileSize,
		minio.PutObjectOptions{
			ContentType: "application/octet-stream",
		},
	)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "upload to minio failed"})
		return
	}

	// 更新 Location 为 MinIO 的对象路径
	fileMeta.Location = objectKey

	// 写入数据库
	if err := db.InsertFileMeta(r.Context(), fileMeta); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "save file meta failed"})
		return
	}

	// 写入缓存
	_ = cacheRedis.SetFileMetaCache(r.Context(), fileMeta)

	// 记录上传成功日志
	LogOperation(
		r.Context(),
		r,
		username,
		mq.OpUpload,
		mq.ResourceTypeFile,
		fileMeta.FileSha1,
		map[string]string{
			"file_name": fileMeta.FileName,
			"file_size": strconv.FormatInt(fileMeta.FileSize, 10),
		},
	)


	// 返回成功响应
	writeJSON(w, http.StatusOK, map[string]interface{}{
		"file_sha1": fileMeta.FileSha1,
		"file_name": fileMeta.FileName,
		"file_size": info.Size,
		"location":  objectKey,
	})
}

// 下载文件：GET /file/download
func DownloadHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	// 获取当前用户名
	username, ok := auth.UsernameFromContext(r.Context())
	if !ok || username == "" {
		w.WriteHeader(http.StatusUnauthorized)
		return
	}

	// 获取文件哈希
	fileHash := r.URL.Query().Get("filehash")
	if fileHash == "" {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	// 从数据库看文件元信息
	fm, err := db.GetFileMeta(r.Context(), fileHash)
	if err != nil || fm == nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	// 从 MinIO 获取对象
	obg, err := store.MinioClient.GetObject(
		r.Context(),
		config.MinioBucket,
		fm.Location,
		minio.GetObjectOptions{},
	)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	defer obg.Close()

	// 记录下载日志
	LogOperation(
		r.Context(),
		r,
		username,
		mq.OpDownload,
		mq.ResourceTypeFile,
		fileHash,
		map[string]string{
			"file_name": fm.FileName,
		},
	)

	// 设置响应头
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Content-Disposition", "attachment; filename="+fm.FileName+"\"")

	// 把 MinIO 数据流拷贝到 HTTP 响应
	if _, err := io.Copy(w, obg); err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
}

// 获取文件元信息：GET /file/meta
func FileMetaHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	// 获取文件哈希
	fileHash := r.URL.Query().Get("filehash")
	if fileHash == "" {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	// 从缓存获取文件元信息（缓存未命中时自动查DB并回写）
	fm, err := cacheRedis.GetFileMetaCache(r.Context(), fileHash, db.GetFileMeta)
	if err != nil || fm == nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	// 返回文件元信息
	writeJSON(w, http.StatusOK, map[string]interface{}{
		"file_sha1":   fm.FileSha1,
		"file_name":   fm.FileName,
		"file_size":   fm.FileSize,
		"location":    fm.Location,
		"upload_time": fm.UploadTime,
	})
}

// 快速上传：POST /file/fastupload
func FastUploadHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	_ = r.ParseForm()
	fileHash := r.FormValue("filehash")
	if fileHash == "" {
		fileHash = r.URL.Query().Get("filehash")
	}
	if fileHash == "" {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	fm, err := db.GetFileMeta(r.Context(), fileHash)
	if err != nil || fm == nil || fm.FileSha1 == "" {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"result":      "fast upload success",
		"file_sha1":   fm.FileSha1,
		"file_name":   fm.FileName,
		"file_size":   fm.FileSize,
		"location":    fm.Location,
		"upload_time": fm.UploadTime,
	})
}

// ======================= 删除 & 回收站 =======================

// 删除文件：DELETE /file/delete
func DeleteHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodDelete {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	// 从JWT context 拿当前用户名
	username, ok := auth.UsernameFromContext(r.Context())
	if !ok || username == "" {
		w.WriteHeader(http.StatusUnauthorized)
		return
	}

	fileHash := r.URL.Query().Get("filehash")
	if fileHash == "" {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	ctx := r.Context()

	// 源文件信息，拿到 MinIO 的 key
	fm, err := db.GetFileMeta(ctx, fileHash)
	if err != nil || fm == nil || fm.FileSha1 == "" {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "file not found"})
		return
	}

	// 删除当前用户这条用户文件记录（username + filehash）
	// 软删
	if err := db.DeleteUserFile(ctx, username, fileHash); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "failed to delete user file record"})
		return
	}

	// 检查该 filehash 还有其他用户
	stileUsed, err := db.ExistsUserFileByHash(ctx, fileHash)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "failed to check file usage"})
		return
	}
	if stileUsed {
		// 还有其他用户在用，不删除MinIO 和 tbl_file
		writeJSON(w, http.StatusOK, map[string]string{
			"result": "delete success (file still used by others)",
		})
		return
	}

	// 发送 MQ 延迟删除消息（3天后自动删除）
	msg := &mq.FileDeleteMessage{
		Username:  username,
		FileHash:  fileHash,
		FileName:  fm.FileName,
		DeletedAt: time.Now(),
	}
	if err := mq.PublishFileDeleteMessage(ctx, msg); err != nil {
		log.Printf("Failed to publish delete message: %v", err)
		// 注意：即使 MQ 发送失败，用户侧已软删成功，不影响用户体验
	}

	LogOperation(
		r.Context(),
		r,
		username,
		mq.OpDelete,
		mq.ResourceTypeFile,
		fileHash,
		map[string]string{
			"file_name": fm.FileName,
		},
	)

	writeJSON(w, http.StatusOK, map[string]string{
		"result": "delete success",
	})
}


// 回收站：GET /file/recycle
func RecycleHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodDelete {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	username, ok := auth.UsernameFromContext(r.Context())
	if !ok || username == "" {
		w.WriteHeader(http.StatusUnauthorized)
		return
	}

	files, err := db.GetRecycleBinFiles(r.Context(), username)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "failed to get recycle bin files"})
		return
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"files": files,
		"count": len(files),
	})
}

// 恢复文件：POST /file/restore
func RestoreFileHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	username, ok := auth.UsernameFromContext(r.Context())
	if !ok || username == "" {
		w.WriteHeader(http.StatusUnauthorized)
		return
	}

	_ = r.ParseForm()
	fileHash := r.FormValue("filehash")
	if fileHash == "" {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	if err := db.RestoreUserFile(r.Context(), username, fileHash); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "failed to restore file"})
		return
	}

	LogOperation(
		r.Context(),
		r,
		username,
		mq.OpRestore,
		mq.ResourceTypeFile,
		fileHash,
		nil,
	)

	writeJSON(w, http.StatusOK, map[string]string{
		"result": "restore success",
	})
}

func PremanentDeleteHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	username, ok := auth.UsernameFromContext(r.Context())
	if !ok || username == "" {
		w.WriteHeader(http.StatusUnauthorized)
		return
	}
	
	fileHash := r.URL.Query().Get("filehash")
	if fileHash == "" {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	ctx := r.Context()

	fm, err := db.GetFileMeta(ctx, fileHash)
	if err != nil || fm == nil || fm.FileSha1 == "" {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "file not found"})
		return
	}

	if err := db.PermanentDeleteUserFile(ctx, username, fileHash); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "failed to delete file"})
		return
	}

	stillUsed, err := db.ExistsUserFileByHash(ctx, fileHash)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "failed to check file usage"})
		return
	}

	if !stillUsed {
		if err := db.PermanentDeleteFileMeta(ctx, fileHash); err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "failed to delete file meta"})
			return
		}

		_ = cacheRedis.DeleteFileMetaCache(ctx, fileHash)
		_ = db.PermanentDeleteFileMeta(ctx, fileHash)
	}

	writeJSON(w, http.StatusOK, map[string]string{
		"result": "delete success",
	})
}

// ======================= 分片上传 =======================

// 默认分片大小：5MB
const defaultChunkSize int64 = 5 * 1024 * 1024

// 初始化分片上传：POST /file/multipart/init
func MultipartInitHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	if err := r.ParseForm(); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	fileHash := r.FormValue("filehash")
	fileName := r.FormValue("filename")
	fileSizeStr := r.FormValue("filesize")

	if fileHash == "" || fileName == "" || fileSizeStr == "" {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	fileSize, err := strconv.ParseInt(fileSizeStr, 10, 64)
	if err != nil || fileSize <= 0 {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	// 计算总分片数
	chunkCount := int((fileSize + defaultChunkSize - 1) / defaultChunkSize)

	// 生成唯一上传ID
	uploadID := uuid.NewString()

	ctx := r.Context()
	infoKey := "multipart:info:" + uploadID
	chunksKey := "multipart:chunks:" + uploadID

	// 获取当前用户名
	username, _ := auth.UsernameFromContext(r.Context())

	// 在 Redis 中写入上传任务元信息
	_, err = cacheRedis.Rdb.HSet(ctx, infoKey, map[string]interface{}{
		"file_sha1":   fileHash,
		"file_name":   fileName,
		"file_size":   fileSize,
		"chunk_count": chunkCount,
		"chunk_size":  defaultChunkSize,
		"upload_id":   uploadID,
		"status":      "init",
		"username":    username,
		"created_at":  time.Now().Format(time.RFC3339),
	}).Result()
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "failed to save upload info"})
		return
	}

	// 设置过期时间
	ttl := time.Hour * 24
	cacheRedis.Rdb.Expire(ctx, infoKey, ttl)
	cacheRedis.Rdb.Expire(ctx, chunksKey, ttl)

	// 返回前端：uploadID + 分片信息
	writeJSON(w, http.StatusOK, map[string]interface{}{
		"upload_id":   uploadID,
		"file_sha1":   fileHash,
		"file_name":   fileName,
		"file_size":   fileSize,
		"chunk_size":  defaultChunkSize,
		"chunk_count": chunkCount,
	})
}

// 上传分片：POST /file/multipart/upload
func MultipartUploadHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	if err := r.ParseMultipartForm(32 << 20); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	uploadID := r.FormValue("upload_id")
	chunkIndexStr := r.FormValue("chunk_index")

	if uploadID == "" || chunkIndexStr == "" {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	chunkIndex, err := strconv.Atoi(chunkIndexStr)
	if err != nil || chunkIndex < 0 {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	// 获取分片文件流
	chunkFile, _, err := r.FormFile("file")
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	defer chunkFile.Close()

	ctx := r.Context()
	infoKey := "multipart:info:" + uploadID
	chunksKey := "multipart:chunks:" + uploadID

	// 1. 从 redis 读取上传任务元信息，校验任务存在
	info, err := cacheRedis.Rdb.HGetAll(ctx, infoKey).Result()
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "failed to get upload info"})
		return
	}

	// 2. 校验 chunk_index 合法（范围： 0 ～ chunk_count - 1）
	chunkCount, _ := strconv.Atoi(info["chunk_count"])
	if chunkIndex >= chunkCount {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "chunk index out of range"})
		return
	}

	// 3. 幂等处理：检查该分片是否已经上传成功
	exists, err := cacheRedis.Rdb.SIsMember(ctx, chunksKey, chunkIndex).Result()
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "failed to check chunk existence"})
		return
	}
	if exists {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "chunk already uploaded"})
		return
	}

	// 4. 上传分片到 Minio
	// 分片对象 key 格式：multipart/<uploadID>/<chunkIndex>
	objectKey := fmt.Sprintf("multipart/%s/%d", uploadID, chunkIndex)

	_, err = store.MinioClient.PutObject(
		ctx,
		config.MinioBucket,
		objectKey,
		chunkFile,
		-1, // -1标识自动读取流大小
		minio.PutObjectOptions{
			ContentType: "application/octet-stream",
		},
	)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "failed to upload chunk"})
		return
	}

	// 5. 更新 Redis 进度：
	// - 把 chunk_index 加入 set（表示已上传）
	// - 把 uploaded_chunks 计数 +1
	// - 如果是首次上传分片，把 status 改为 "uploading"
	pipe := cacheRedis.Rdb.TxPipeline()
	pipe.SAdd(ctx, chunksKey, chunkIndex)
	pipe.HIncrBy(ctx, infoKey, "uploaded_chunks", 1)
	pipe.HSet(ctx, infoKey, "status", "uploading")
	_, err = pipe.Exec(ctx)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "failed to update upload progress"})
		return
	}

	// 6. 返回成功
	writeJSON(w, http.StatusOK, map[string]interface{}{
		"result":      "chunk upload success",
		"upload_id":   uploadID,
		"chunk_index": chunkIndex,
	})
}

// 分片上传查询进度：GET /file/multipart/status
func MultipartStatusHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	uploadID := r.URL.Query().Get("upload_id")
	if uploadID == "" {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	ctx := r.Context()
	infoKey := "multipart:info:" + uploadID
	chunksKey := "multipart:chunks:" + uploadID

	info, err := cacheRedis.Rdb.HGetAll(ctx, infoKey).Result()
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "failed to get upload info"})
		return
	}

	// 获取分片数和当前状态
	chunkCount, _ := strconv.Atoi(info["chunk_count"])
	status := info["status"]

	// 从Redis Set 获取实际已上传到分片数
	uploadedChunks, err := cacheRedis.Rdb.SCard(ctx, chunksKey).Result()
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "failed to get uploaded chunks"})
		return
	}

	// 计算百分比
	var progress int
	if chunkCount > 0 {
		progress = int(uploadedChunks) * 100 / chunkCount
	}

	// 判断是否全部上传完成
	completed := int(uploadedChunks) == chunkCount && chunkCount > 0

	// 返回结果
	writeJSON(w, http.StatusOK, map[string]interface{}{
		"upload_id":       uploadID,
		"chunk_count":     chunkCount,
		"uploaded_chunks": uploadedChunks,
		"progress":        progress,
		"status":          status,
		"completed":       completed,
	})
}

func MultipartCompleteHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	// 解析参数：upload_id
	if err := r.ParseForm(); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	uploadID := r.FormValue("upload_id")
	if uploadID == "" {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	ctx := r.Context()
	infoKey := "multipart:info:" + uploadID
	chunksKey := "multipart:chunks:" + uploadID

	// 从 Redis 获取上传任务元信息
	info, err := cacheRedis.Rdb.HGetAll(ctx, infoKey).Result()
	if err != nil || len(info) == 0 {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "upload task not found"})
		return
	}

	// 提取相关信息
	fileSha1 := info["file_sha1"]
	fileName := info["file_name"]
	fileSize, _ := strconv.ParseInt(info["file_size"], 10, 64)
	chunkCount, _ := strconv.Atoi(info["chunk_count"])
	username := info["username"]

	// 校验所有分片是否已上传完成
	uploadedChunks, err := cacheRedis.Rdb.SCard(ctx, chunksKey).Result()
	if err != nil || int(uploadedChunks) != chunkCount {
		writeJSON(w, http.StatusInternalServerError, map[string]string{
			"error": fmt.Sprintf("not all chunks uploaded, uploaded: %d, total: %d", uploadedChunks, chunkCount),
		})
		return
	}

	// 分布式锁：防止重复合并（基于 fileSha1）
	lockKey := "lock:merge:" + uploadID
	lock := cacheRedis.NewLock(ctx, lockKey, 10*time.Minute)
	if locked, err := lock.TryLock(); err != nil || !locked {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "failed to acquire lock"})
		return
	}
	defer lock.Unlock()

	// 检查 status，防止重复
	currentStatus := info["status"]
	if currentStatus == "completed" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "upload already completed"})
	}

	// Minio ComposeObject 合并分片
	var srcs []minio.CopySrcOptions
	for i := 0; i < chunkCount; i++ {
		chunkKey := fmt.Sprintf("multipart/%s/%d", uploadID, i)
		src := minio.CopySrcOptions{
			Bucket: config.MinioBucket,
			Object: chunkKey,
		}
		srcs = append(srcs, src)
	}

	finalObjectKey := "files/" + fileSha1

	// ComposeObject 合并分片
	_, err = store.MinioClient.ComposeObject(ctx, minio.CopyDestOptions{
		Bucket: config.MinioBucket,
		Object: finalObjectKey,
	}, srcs...)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "failed to merge chunks"})
		return
	}

	// 写入 MySQL 文件元信息
	fm := &meta.FileMeta{
		FileName:   fileName,
		FileSha1:   fileSha1,
		FileSize:   fileSize,
		Location:   finalObjectKey,
		UploadTime: time.Now(),
	}
	if err := db.InsertFileMeta(ctx, fm); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "failed to insert file meta"})
		return
	}

	// 写入缓存
	_ = cacheRedis.SetFileMetaCache(ctx, fm)

	// 写入用户-文件关系表
	if username != "" {
		if err := db.InsertUserFile(ctx, username, fileSha1, fileName, fileSize); err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "failed to insert user file relation"})
			return
		}
	}

	// 更新 redis 任务状态为 completed
	cacheRedis.Rdb.HSet(ctx, infoKey, "status", "completed")
	cacheRedis.Rdb.HSet(ctx, infoKey, "location", finalObjectKey)

	// 异步删除临时分片对象
	go func() {
		for i := 0; i < chunkCount; i++ {
			chunkKey := fmt.Sprintf("multipart/%s/%d", uploadID, i)
			store.MinioClient.RemoveObject(ctx, config.MinioBucket, chunkKey, minio.RemoveObjectOptions{})
		}
	}()

	// 设置 redis key 短期 ttl（1小时）
	cacheRedis.Rdb.Expire(ctx, infoKey, time.Hour)
	cacheRedis.Rdb.Expire(ctx, chunksKey, time.Hour)

	writeJSON(w, http.StatusOK, map[string]string{
		"result":      "multipart upload completed",
		"file_sha1":   fileSha1,
		"file_name":   fileName,
		"file_size":   strconv.FormatInt(fileSize, 10),
		"location":    finalObjectKey,
		"upload_time": fm.UploadTime.Format(time.RFC3339),
	})
}
