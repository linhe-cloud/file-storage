package service

import (
	"context"
	"errors"
	"file-storage-linhe/config"
	cacheRedis "file-storage-linhe/internal/cache/redis"
	"file-storage-linhe/internal/mq"
	"file-storage-linhe/internal/store"
	"fmt"
	"io"
	"time"

	"github.com/google/uuid"
	"github.com/minio/minio-go/v7"
)

var (
	ErrMultipartNotFound       = errors.New("upload task not found")
	ErrMultipartForbidden      = errors.New("upload task access denied")
	ErrMultipartIncomplete     = errors.New("not all chunks uploaded")
	ErrMultipartAlreadyHandled = errors.New("task already submitted")
	ErrChunkAlreadyUploaded    = errors.New("chunk already uploaded")
	ErrChunkOutOfRange         = errors.New("chunk index out of range")
	ErrChunkSizeInvalid        = errors.New("invalid chunk size")
	ErrChunkBusy               = errors.New("chunk is being uploaded")
	ErrMultipartNotAccepting   = errors.New("upload task is no longer accepting chunks")
)

type MultipartService struct{}

func NewMultipartService() *MultipartService { return &MultipartService{} }

func CalculateChunkSize(fileSize int64) int64 {
	switch {
	case fileSize <= 20*1024*1024:
		return fileSize
	case fileSize <= 100*1024*1024:
		return 5 * 1024 * 1024
	case fileSize <= 1024*1024*1024:
		return 10 * 1024 * 1024
	case fileSize <= 10*1024*1024*1024:
		return 50 * 1024 * 1024
	default:
		return 100 * 1024 * 1024
	}
}

func (s *MultipartService) Init(ctx context.Context, username, fileHash, fileName string, fileSize int64) (*cacheRedis.ChunkInfo, error) {
	if username == "" || fileHash == "" || fileName == "" || fileSize <= 0 {
		return nil, errors.New("invalid multipart parameters")
	}
	chunkSize := CalculateChunkSize(fileSize)
	info := &cacheRedis.ChunkInfo{
		UploadID:   uuid.NewString(),
		FileName:   fileName,
		FileSize:   fileSize,
		ChunkSize:  chunkSize,
		ChunkCount: int((fileSize + chunkSize - 1) / chunkSize),
		FileSha1:   fileHash,
		Username:   username,
		Status:     "init",
	}
	if err := cacheRedis.SetChunkInfo(ctx, info); err != nil {
		return nil, err
	}
	return info, nil
}

func (s *MultipartService) GetOwned(ctx context.Context, username, uploadID string) (*cacheRedis.ChunkInfo, error) {
	info, err := cacheRedis.GetChunkInfo(ctx, uploadID)
	if err != nil {
		return nil, ErrMultipartNotFound
	}
	if info.Username != username {
		return nil, ErrMultipartForbidden
	}
	return info, nil
}

// UploadChunk validates ownership and chunk boundaries before storing the
// chunk. Redis set membership makes retrying an already completed chunk safe.
func (s *MultipartService) UploadChunk(ctx context.Context, username, uploadID string, chunkIndex int, reader io.Reader, size int64) error {
	if uploadID == "" || chunkIndex < 0 || reader == nil || size < 0 {
		return ErrChunkSizeInvalid
	}
	info, err := s.GetOwned(ctx, username, uploadID)
	if err != nil {
		return err
	}
	if info.Status == "processing" || info.Status == "completed" {
		return ErrMultipartNotAccepting
	}
	if chunkIndex >= info.ChunkCount {
		return ErrChunkOutOfRange
	}
	expectedSize := info.ChunkSize
	if chunkIndex == info.ChunkCount-1 {
		expectedSize = info.FileSize - info.ChunkSize*int64(info.ChunkCount-1)
	}
	if expectedSize <= 0 || size != expectedSize {
		return ErrChunkSizeInvalid
	}

	lock := cacheRedis.NewLock(ctx, "lock:multipart:"+uploadID+":"+fmt.Sprint(chunkIndex), time.Minute)
	locked, err := lock.TryLock()
	if err != nil {
		return err
	}
	if !locked {
		return ErrChunkBusy
	}
	defer lock.Unlock()

	chunksKey := "multipart:chunks:" + uploadID
	exists, err := cacheRedis.Rdb.SIsMember(ctx, chunksKey, chunkIndex).Result()
	if err != nil {
		return err
	}
	if exists {
		return ErrChunkAlreadyUploaded
	}

	objectKey := fmt.Sprintf("multipart/%s/%d", uploadID, chunkIndex)
	if _, err := store.MinioClient.PutObject(ctx, config.MinioBucket, objectKey, reader, size, minio.PutObjectOptions{ContentType: "application/octet-stream"}); err != nil {
		return err
	}

	infoKey := "multipart:info:" + uploadID
	pipe := cacheRedis.Rdb.TxPipeline()
	pipe.SAdd(ctx, chunksKey, chunkIndex)
	pipe.HIncrBy(ctx, infoKey, "uploaded_chunks", 1)
	pipe.HSet(ctx, infoKey, "status", "uploading")
	pipe.Expire(ctx, chunksKey, cacheRedis.ChunkInfoCacheTTL)
	if _, err := pipe.Exec(ctx); err != nil {
		_ = store.MinioClient.RemoveObject(ctx, config.MinioBucket, objectKey, minio.RemoveObjectOptions{})
		return err
	}
	return nil
}

// Status returns progress for an owned multipart task.
func (s *MultipartService) Status(ctx context.Context, username, uploadID string) (*cacheRedis.ChunkInfo, int, bool, error) {
	info, err := s.GetOwned(ctx, username, uploadID)
	if err != nil {
		return nil, 0, false, err
	}
	count, err := cacheRedis.GetUploadedChunkCount(ctx, uploadID)
	if err != nil {
		return nil, 0, false, err
	}
	return info, count, count == info.ChunkCount && info.ChunkCount > 0, nil
}

func (s *MultipartService) SubmitMerge(ctx context.Context, username, uploadID string) (*cacheRedis.ChunkInfo, error) {
	info, err := s.GetOwned(ctx, username, uploadID)
	if err != nil {
		return nil, err
	}
	count, err := cacheRedis.GetUploadedChunkCount(ctx, uploadID)
	if err != nil {
		return nil, err
	}
	if count != info.ChunkCount {
		return nil, fmt.Errorf("%w: %d/%d", ErrMultipartIncomplete, count, info.ChunkCount)
	}
	if info.Status == "completed" || info.Status == "processing" {
		return nil, ErrMultipartAlreadyHandled
	}
	if err := cacheRedis.UpdateChunkStatus(ctx, uploadID, "processing"); err != nil {
		return nil, err
	}
	if err := mq.PublishFileMerge(ctx, mq.NewFileMergeMessage(uploadID, username, info.FileName, info.ChunkCount, info.FileSize)); err != nil {
		_ = cacheRedis.UpdateChunkStatus(ctx, uploadID, "uploading")
		return nil, err
	}
	info.Status = "processing"
	return info, nil
}
