package handler

import (
	"archive/tar"
	"compress/gzip"
	"encoding/json"
	"io"
	"log"
	"net/http"

	"file-storage-linhe/config"
	"file-storage-linhe/internal/db"
	"file-storage-linhe/internal/handler/auth"
	"file-storage-linhe/internal/meta"
	"file-storage-linhe/internal/store"

	"github.com/minio/minio-go/v7"
)

// BatchDownloadHandler 批量下载打包：POST /file/download/batch
func BatchDownloadHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	username, ok := auth.UsernameFromContext(r.Context())
	if !ok || username == "" {
		w.WriteHeader(http.StatusUnauthorized)
		return
	}

	var req struct {
		FileHashes []string `json:"file_hashes"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid json body"})
		return
	}

	if len(req.FileHashes) == 0 {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "file_hashes is empty"})
		return
	}
	if len(req.FileHashes) > 100 {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "file_hashes exceeds max limit 100"})
		return
	}

	ctx := r.Context()

	userFiles, err := db.GetUserFilesByHashes(ctx, username, req.FileHashes)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "failed to query user files"})
		return
	}

	if len(userFiles) == 0 {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "no valid files found"})
		return
	}

	validHashes := make([]string, len(userFiles))
	for i, uf := range userFiles {
		validHashes[i] = uf.FileSha1
	}

	fileMetas, err := db.GetFileMetasByHashes(ctx, validHashes)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "failed to query file metas"})
		return
	}

	metaMap := make(map[string]*meta.FileMeta, len(fileMetas))
	for _, fm := range fileMetas {
		metaMap[fm.FileSha1] = fm
	}

	w.Header().Set("Content-Type", "application/gzip")
	w.Header().Set("Content-Disposition", "attachment; filename=\"download.tar.gz\"")

	gzWriter := gzip.NewWriter(w)
	defer gzWriter.Close()

	tarWriter := tar.NewWriter(gzWriter)
	defer tarWriter.Close()

	for _, uf := range userFiles {
		fm, ok := metaMap[uf.FileSha1]
		if !ok || fm == nil {
			log.Printf("file meta not found for hash: %s", uf.FileSha1)
			continue
		}

		obj, err := store.MinioClient.GetObject(ctx, config.MinioBucket, fm.Location, minio.GetObjectOptions{})
		if err != nil {
			log.Printf("failed to get object from minio: %s, err: %v", fm.Location, err)
			continue
		}

		header := &tar.Header{
			Name: fm.FileName,
			Size: fm.FileSize,
			Mode: 0644,
		}
		if err := tarWriter.WriteHeader(header); err != nil {
			log.Printf("failed to write tar header: %s, err: %v", fm.FileName, err)
			obj.Close()
			continue
		}

		if _, err := io.Copy(tarWriter, obj); err != nil {
			log.Printf("failed to copy file to tar: %s, err: %v", fm.FileName, err)
			obj.Close()
			continue
		}

		obj.Close()
	}
}
