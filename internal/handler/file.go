package handler

/**
 * @Description: 文件处理相关
 */

import (
	"errors"
	"file-storage-linhe/config"
	cacheRedis "file-storage-linhe/internal/cache/redis"
	"file-storage-linhe/internal/db"
	"file-storage-linhe/internal/handler/auth"
	"file-storage-linhe/internal/mq"
	"file-storage-linhe/internal/service"
	"file-storage-linhe/internal/store"
	"io"
	"log"
	"net/http"
	"strconv"

	"github.com/minio/minio-go/v7"
)

var fileService = service.NewFileService()
var multipartService = service.NewMultipartService()

// userOwnsActiveFile 检查用户是否拥有指定的活跃文件
// 参数:
//
//	r - HTTP请求指针，用于获取上下文信息
//	username - 用户名
//	fileHash - 文件的哈希值
//
// 返回值:
//
//	bool - 如果用户拥有活跃文件则返回true，否则返回false
//	error - 如果发生错误则返回相应的错误信息
func userOwnsActiveFile(r *http.Request, username, fileHash string) (bool, error) {
	// 检查用户名和文件哈希是否为空
	if username == "" || fileHash == "" {
		return false, nil
	}
	// 调用数据库函数检查用户文件是否可用
	return db.IsUserFileAvailable(r.Context(), username, fileHash)
}

// ======================= 上传 & 下载 =======================

// 上传文件：POST /file/upload
// UploadHandler 处理文件上传请求的HTTP处理器函数
func UploadHandler(w http.ResponseWriter, r *http.Request) {
	// 只允许POST方法
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

	fileMeta, err := fileService.Upload(r.Context(), username, header.Filename, file)
	if err != nil {
		if errors.Is(err, service.ErrFileBeingUploaded) {
			writeJSON(w, http.StatusConflict, map[string]string{"error": err.Error()})
		} else {
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "upload failed"})
		}
		return
	}

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
		"file_size": fileMeta.FileSize,
		"location":  fileMeta.Location,
	})
}

// 下载文件：GET /file/download
// DownloadHandler 处理文件下载请求的HTTP处理函数
func DownloadHandler(w http.ResponseWriter, r *http.Request) {
	// 只允许GET方法请求
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
	owned, err := userOwnsActiveFile(r, username, fileHash)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "failed to verify file ownership"})
		return
	}
	if !owned {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "file not found"})
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
	w.Header().Set("Content-Disposition", "attachment; filename=\""+fm.FileName+"\"")

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
	username, ok := auth.UsernameFromContext(r.Context())
	if !ok || username == "" {
		w.WriteHeader(http.StatusUnauthorized)
		return
	}
	owned, err := userOwnsActiveFile(r, username, fileHash)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "failed to verify file ownership"})
		return
	}
	if !owned {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "file not found"})
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
/**
 * FastUploadHandler 处理文件快速上传的HTTP处理器
 * @param w HTTP响应写入器
 * @param r HTTP请求指针
 */
func FastUploadHandler(w http.ResponseWriter, r *http.Request) {
	// 检查请求方法是否为POST，如果不是则返回405状态码
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

	_ = r.ParseForm()
	fileHash := r.FormValue("filehash")
	if fileHash == "" {
		fileHash = r.URL.Query().Get("filehash")
	}
	if fileHash == "" {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	fm, err := fileService.FastUpload(r.Context(), username, fileHash)
	if err != nil {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "file not found"})
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

	lastReference, fm, err := fileService.Delete(r.Context(), username, fileHash)
	if err != nil {
		if errors.Is(err, service.ErrFileNotOwned) || errors.Is(err, service.ErrFileNotFound) {
			writeJSON(w, http.StatusNotFound, map[string]string{"error": "file not found"})
		} else {
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "failed to delete file"})
		}
		return
	}
	if lastReference {
		// 最后一个引用已进入延迟删除队列。
		LogOperation(r.Context(), r, username, mq.OpDelete, mq.ResourceTypeFile, fileHash, map[string]string{"file_name": fm.FileName})
		writeJSON(w, http.StatusOK, map[string]string{"result": "delete success"})
		return
	}

	LogOperation(
		r.Context(),
		r,
		username,
		mq.OpDelete,
		mq.ResourceTypeFile,
		fileHash,
		map[string]string{"file_name": fm.FileName},
	)

	writeJSON(w, http.StatusOK, map[string]string{
		"result": "delete success",
	})
}

// 回收站：GET /file/recycle
func RecycleHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
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
	if err := fileService.Restore(r.Context(), username, fileHash); err != nil {
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

// 永久删除文件：POST /file/permanentdelete
func PermanentDeleteHandler(w http.ResponseWriter, r *http.Request) {
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
	if err := fileService.PermanentDelete(r.Context(), username, fileHash); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "failed to permanently delete file"})
		return
	}

	writeJSON(w, http.StatusOK, map[string]string{
		"result": "delete success",
	})
}

// ======================= 分片上传 =======================

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

	// 获取当前用户名
	username, ok := auth.UsernameFromContext(r.Context())
	if !ok || username == "" {
		w.WriteHeader(http.StatusUnauthorized)
		return
	}
	info, err := multipartService.Init(r.Context(), username, fileHash, fileName, fileSize)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "failed to save upload info"})
		return
	}
	chunkSize := service.CalculateChunkSize(fileSize)

	// 返回前端：uploadID + 分片信息
	writeJSON(w, http.StatusOK, map[string]interface{}{
		"upload_id":   info.UploadID,
		"file_sha1":   fileHash,
		"file_name":   fileName,
		"file_size":   fileSize,
		"chunk_size":  chunkSize,
		"chunk_count": info.ChunkCount,
	})
}

// 上传分片：POST /file/multipart/upload
func MultipartUploadHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	username, ok := auth.UsernameFromContext(r.Context())
	if !ok || username == "" {
		w.WriteHeader(http.StatusUnauthorized)
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
	chunkFile, chunkHeader, err := r.FormFile("file")
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	defer chunkFile.Close()

	err = multipartService.UploadChunk(r.Context(), username, uploadID, chunkIndex, chunkFile, chunkHeader.Size)
	if err != nil {
		status := http.StatusInternalServerError
		switch {
		case errors.Is(err, service.ErrMultipartNotFound):
			status = http.StatusNotFound
		case errors.Is(err, service.ErrMultipartForbidden):
			status = http.StatusForbidden
		case errors.Is(err, service.ErrChunkAlreadyUploaded), errors.Is(err, service.ErrChunkOutOfRange), errors.Is(err, service.ErrChunkSizeInvalid), errors.Is(err, service.ErrMultipartNotAccepting):
			status = http.StatusBadRequest
		case errors.Is(err, service.ErrChunkBusy):
			status = http.StatusConflict
		}
		writeJSON(w, status, map[string]string{"error": err.Error()})
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
	username, ok := auth.UsernameFromContext(r.Context())
	if !ok || username == "" {
		w.WriteHeader(http.StatusUnauthorized)
		return
	}

	info, uploadedChunks, completed, err := multipartService.Status(r.Context(), username, uploadID)
	if err != nil {
		status := http.StatusInternalServerError
		switch {
		case errors.Is(err, service.ErrMultipartNotFound):
			status = http.StatusNotFound
		case errors.Is(err, service.ErrMultipartForbidden):
			status = http.StatusForbidden
		}
		writeJSON(w, status, map[string]string{"error": err.Error()})
		return
	}
	chunkCount := info.ChunkCount
	status := info.Status

	// 计算百分比
	var progress int
	if chunkCount > 0 {
		progress = int(uploadedChunks) * 100 / chunkCount
	}

	// 判断是否全部上传完成
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

/**
 * MultipartCompleteHandler 处理文件分片上传完成的请求
 * 该函数用于验证上传参数、检查分片完整性，并触发文件合并任务
 * @param w http.ResponseWriter - HTTP响应写入器
 * @param r *http.Request - HTTP请求指针
 */
func MultipartCompleteHandler(w http.ResponseWriter, r *http.Request) {
	// 只允许POST请求
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

	// 解析参数：upload_id, file_name, chunk_count, file_size
	if err := r.ParseForm(); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	uploadID := r.FormValue("upload_id")
	fileName := r.FormValue("file_name")
	chunkCountStr := r.FormValue("chunk_count")
	fileSizeStr := r.FormValue("file_size")

	if uploadID == "" || fileName == "" || chunkCountStr == "" || fileSizeStr == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{
			"error": "missing required parameters",
		})
		return
	}

	if _, err := strconv.Atoi(chunkCountStr); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid chunk_count"})
		return
	}
	if _, err := strconv.ParseInt(fileSizeStr, 10, 64); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid file_size"})
		return
	}
	info, err := multipartService.SubmitMerge(r.Context(), username, uploadID)
	if err != nil {
		switch {
		case errors.Is(err, service.ErrMultipartNotFound):
			writeJSON(w, http.StatusNotFound, map[string]string{"error": err.Error()})
		case errors.Is(err, service.ErrMultipartForbidden):
			writeJSON(w, http.StatusForbidden, map[string]string{"error": err.Error()})
		case errors.Is(err, service.ErrMultipartIncomplete), errors.Is(err, service.ErrMultipartAlreadyHandled):
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error(), "upload_id": uploadID})
		default:
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "failed to submit merge task"})
		}
		return
	}

	log.Printf("分片合并任务已提交: upload_id=%s, user=%s, file=%s", uploadID, username, info.FileName)

	// 立即返回，告诉用户任务已提交
	writeJSON(w, http.StatusOK, map[string]interface{}{
		"message":   "merge task submitted successfully",
		"upload_id": uploadID,
		"status":    "processing",
		"tip":       "请使用 /file/multipart/merge-status 接口查询合并进度",
	})
}

// MultipartMergeStatusHandler 查询分片合并状态
// GET /file/multipart/merge-status?upload_id=xxx
func MultipartMergeStatusHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	username, ok := auth.UsernameFromContext(r.Context())
	if !ok || username == "" {
		w.WriteHeader(http.StatusUnauthorized)
		return
	}

	uploadID := r.URL.Query().Get("upload_id")
	if uploadID == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{
			"error": "upload_id is required",
		})
		return
	}

	ctx := r.Context()

	// 从 Redis 获取任务信息
	chunkInfo, err := cacheRedis.GetChunkInfo(ctx, uploadID)
	if err != nil {
		writeJSON(w, http.StatusNotFound, map[string]string{
			"error": "upload task not found",
		})
		return
	}

	// 验证是否是当前用户的任务
	if chunkInfo.Username != username {
		writeJSON(w, http.StatusForbidden, map[string]string{
			"error": "access denied",
		})
		return
	}

	// 构造响应
	response := map[string]interface{}{
		"upload_id": uploadID,
		"status":    chunkInfo.Status, // uploading / processing / completed / failed
		"file_name": chunkInfo.FileName,
		"file_size": chunkInfo.FileSize,
	}

	// 如果已完成，返回文件哈希
	if chunkInfo.Status == "completed" && chunkInfo.FileSha1 != "" {
		response["file_sha1"] = chunkInfo.FileSha1
		response["message"] = "文件合并完成"
	} else if chunkInfo.Status == "processing" {
		response["message"] = "文件正在合并中，请稍后查询"
	} else if chunkInfo.Status == "uploading" {
		response["message"] = "分片上传中"
	} else if chunkInfo.Status == "failed" {
		response["message"] = "文件合并失败"
	}

	writeJSON(w, http.StatusOK, response)
}
