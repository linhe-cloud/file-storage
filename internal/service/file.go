package service

import (
	"context"
	"errors"
	"file-storage-linhe/config"
	cacheRedis "file-storage-linhe/internal/cache/redis"
	"file-storage-linhe/internal/db"
	"file-storage-linhe/internal/meta"
	"file-storage-linhe/internal/mq"
	"file-storage-linhe/internal/store"
	"file-storage-linhe/util"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/minio/minio-go/v7"
)

var (
	ErrFileNotFound      = errors.New("file not found")
	ErrFileNotOwned      = errors.New("file not owned by user")
	ErrFileBeingUploaded = errors.New("file is being uploaded")
)

type FileService struct{}

func NewFileService() *FileService { return &FileService{} }

func (s *FileService) Upload(ctx context.Context, username, fileName string, src io.Reader) (*meta.FileMeta, error) {
	file, err := os.CreateTemp("", "filestore-upload-*")
	if err != nil {
		return nil, err
	}
	defer file.Close()
	defer os.Remove(file.Name())

	fileSize, err := io.Copy(file, src)
	if err != nil {
		return nil, err
	}
	if fileSize == 0 {
		return nil, errors.New("empty file is not allowed")
	}
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return nil, err
	}
	fileHash := util.FileSha1(file)
	lock := cacheRedis.NewLock(ctx, "lock:"+fileHash, 10*time.Second)
	locked, err := lock.TryLock()
	if err != nil {
		return nil, err
	}
	if !locked {
		return nil, ErrFileBeingUploaded
	}
	defer lock.Unlock()

	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return nil, err
	}
	objectKey := "files/" + fileHash
	if _, err := store.MinioClient.PutObject(ctx, config.MinioBucket, objectKey, file, fileSize, minio.PutObjectOptions{ContentType: "application/octet-stream"}); err != nil {
		return nil, err
	}
	fm := &meta.FileMeta{FileSha1: fileHash, FileName: filepath.Base(fileName), FileSize: fileSize, Location: objectKey, UploadTime: time.Now()}
	var transformMessage *mq.FileTransformMessage
	if strings.HasSuffix(strings.ToLower(fm.FileName), ".csv") {
		transformMessage = &mq.FileTransformMessage{FileHash: fm.FileSha1, FileName: fm.FileName, Username: username, FileType: "csv"}
	}
	outboxType := ""
	if transformMessage != nil {
		outboxType = "file_transform"
	}
	if err := db.SaveFileUploadWithOutbox(ctx, username, fm, outboxType, transformMessage); err != nil {
		_ = store.MinioClient.RemoveObject(ctx, config.MinioBucket, objectKey, minio.RemoveObjectOptions{})
		return nil, err
	}
	_ = cacheRedis.SetFileMetaCache(ctx, fm)
	return fm, nil
}

func (s *FileService) GetOwnedMeta(ctx context.Context, username, fileHash string) (*meta.FileMeta, error) {
	owned, err := db.IsUserFileAvailable(ctx, username, fileHash)
	if err != nil {
		return nil, err
	}
	if !owned {
		return nil, ErrFileNotOwned
	}
	fm, err := db.GetFileMeta(ctx, fileHash)
	if err != nil {
		return nil, ErrFileNotFound
	}
	return fm, nil
}

func (s *FileService) FastUpload(ctx context.Context, username, fileHash string) (*meta.FileMeta, error) {
	fm, err := db.GetFileMeta(ctx, fileHash)
	if err != nil {
		return nil, ErrFileNotFound
	}
	if err := db.InsertUserFile(ctx, username, fm.FileSha1, fm.FileName, fm.FileSize); err != nil {
		return nil, err
	}
	return fm, nil
}

func (s *FileService) Delete(ctx context.Context, username, fileHash string) (bool, *meta.FileMeta, error) {
	fm, err := s.GetOwnedMeta(ctx, username, fileHash)
	if err != nil {
		return false, nil, err
	}
	deleteMessage := mq.NewFileDeleteMessage(username, fileHash, fm.FileName)
	lastReference, err := db.DeleteUserFileWithOutbox(ctx, username, fileHash, deleteMessage)
	if err != nil {
		return false, nil, err
	}
	return lastReference, fm, nil
}

func (s *FileService) Restore(ctx context.Context, username, fileHash string) error {
	status, err := db.CheckUserFileStatus(ctx, username, fileHash)
	if err != nil {
		return ErrFileNotFound
	}
	if status != 1 {
		return errors.New("file is not in recycle bin")
	}
	return db.RestoreUserFile(ctx, username, fileHash)
}

func (s *FileService) PermanentDelete(ctx context.Context, username, fileHash string) error {
	status, err := db.CheckUserFileStatus(ctx, username, fileHash)
	if err != nil {
		return ErrFileNotFound
	}
	if status != 1 {
		return errors.New("file is not in recycle bin")
	}
	fm, err := db.GetFileMeta(ctx, fileHash)
	if err != nil {
		return ErrFileNotFound
	}
	cleanupMessage := mq.NewFileCleanupMessage(fileHash, fm.Location)
	return db.PermanentDeleteUserFileWithOutbox(ctx, username, fileHash, fm.Location, cleanupMessage)
}
