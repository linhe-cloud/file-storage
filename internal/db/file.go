package db

import (
	"context"
	"file-storage-linhe/internal/meta"
	"strings"
)

// 写入文件
func InsertFileMeta(ctx context.Context, fm *meta.FileMeta) error {
	_, err := DB.ExecContext(ctx,
		"INSERT INTO tbl_file (file_sha1, file_name, file_size, file_addr) VALUES (?, ?, ?, ?) ON DUPLICATE KEY UPDATE status = 0",
		fm.FileSha1, fm.FileName, fm.FileSize, fm.Location)
	return err
}

// 读取文件
func GetFileMeta(ctx context.Context, sha1 string) (*meta.FileMeta, error) {
	fm := &meta.FileMeta{}
	err := DB.QueryRowContext(ctx,
		"SELECT file_sha1, file_name, file_size, file_addr FROM tbl_file WHERE file_sha1 = ? AND status = 0",
		sha1).Scan(&fm.FileSha1, &fm.FileName, &fm.FileSize, &fm.Location)
	if err != nil {
		return nil, err
	}
	return fm, err
}

// 插入用户文件关系
func InsertUserFile(ctx context.Context, username, fileSha1, fileName string, fileSize int64) error {
	_, err := DB.ExecContext(ctx,
		"INSERT INTO tbl_user_file (user_name, file_sha1, file_name, file_size) VALUES (?, ?, ?, ?) ON DUPLICATE KEY UPDATE status = 0, file_name = VALUES(file_name), file_size = VALUES(file_size)",
		username, fileSha1, fileName, fileSize,
	)
	return err
}

// 删除文件
func DeleteUserFile(ctx context.Context, username, filehash string) error {
	_, err := DB.ExecContext(ctx,
		"UPDATE tbl_user_file SET status = 1 WHERE user_name = ? AND file_sha1 = ? AND status = 0",
		username, filehash,
	)
	return err
}

// 根据文件hash值判断文件是否存在
func ExistsUserFileByHash(ctx context.Context, filehash string) (bool, error) {
	var cnt int
	err := DB.QueryRowContext(ctx,
		"SELECT COUNT(1) FROM tbl_user_file WHERE file_sha1 = ? AND status = 0",
		filehash,
	).Scan(&cnt)
	if err != nil {
		return false, err
	}
	return cnt > 0, nil
}

type RecycleBinFile struct {
	Username   string `json:"username"`
	FileHash   string `json:"file_hash"`
	FileName   string `json:"file_name"`
	UploadAt   string `json:"upload_at"`
	LastUpadte string `json:"last_update"`
}

// 获取回收站文件列表
func GetRecycleBinFiles(ctx context.Context, username string) ([]*RecycleBinFile, error) {
	rows, err := DB.QueryContext(ctx,
		`SELECT user_name, file_sha1, file_name, upload_at, last_update
		FROM tbl_user_file
		WHERE user_name = ? AND status = 1
		ORDER BY last_update DESC`,
		username,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var files []*RecycleBinFile
	for rows.Next() {
		f := &RecycleBinFile{}
		if err := rows.Scan(&f.Username, &f.FileHash, &f.FileName, &f.UploadAt, &f.LastUpadte); err != nil {
			return nil, err
		}
		files = append(files, f)
	}
	return files, nil
}

// 恢复文件
func RestoreUserFile(ctx context.Context, username, filehash string) error {
	_, err := DB.ExecContext(ctx,
		"UPDATE tbl_user_file SET status = 0 WHERE user_name = ? AND file_sha1 = ? AND status = 1",
		username, filehash,
	)
	return err
}

// 永久删除文件
func PermanentDeleteUserFile(ctx context.Context, username, filehash string) error {
	_, err := DB.ExecContext(ctx,
		"DELETE FROM tbl_user_file WHERE user_name = ? AND file_sha1 = ? AND status = 1",
		username, filehash,
	)
	return err
}

// 永久删除文件元信息
func PermanentDeleteFileMeta(ctx context.Context, filehash string) error {
	_, err := DB.ExecContext(ctx,
		"UPDATE tbl_file SET status = 1 WHERE file_sha1 = ?",
		filehash,
	)
	return err
}

// 检查用户文件状态
func CheckUserFileStatus(ctx context.Context, username, filehash string) (int, error) {
	var status int
	err := DB.QueryRowContext(ctx,
		"SELECT status FROM tbl_user_file WHERE user_name = ? AND file_sha1 = ?",
		username, filehash,
	).Scan(&status)
	if err != nil {
		return -1, err
	}
	return status, nil
}

// IsUserFileAvailable verifies that the user owns an active file relation.
func IsUserFileAvailable(ctx context.Context, username, filehash string) (bool, error) {
	var exists bool
	err := DB.QueryRowContext(ctx,
		`SELECT EXISTS(
			SELECT 1 FROM tbl_user_file
			WHERE user_name = ? AND file_sha1 = ? AND status = 0
		)`, username, filehash,
	).Scan(&exists)
	return exists, err
}

type UserFile struct {
	UserName string
	FileSha1 string
	FileName string
	FileSize int64
}

// GetFileMetasByHashes 批量查询文件元信息
func GetFileMetasByHashes(ctx context.Context, hashes []string) ([]*meta.FileMeta, error) {
	if len(hashes) == 0 {
		return nil, nil
	}

	placeholders := make([]string, len(hashes))
	args := make([]interface{}, len(hashes))
	for i, h := range hashes {
		placeholders[i] = "?"
		args[i] = h
	}

	query := "SELECT file_sha1, file_name, file_size, file_addr FROM tbl_file WHERE status = 0 AND file_sha1 IN (" + strings.Join(placeholders, ",") + ")"
	rows, err := DB.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var metas []*meta.FileMeta
	for rows.Next() {
		fm := &meta.FileMeta{}
		if err := rows.Scan(&fm.FileSha1, &fm.FileName, &fm.FileSize, &fm.Location); err != nil {
			return nil, err
		}
		metas = append(metas, fm)
	}
	return metas, nil
}

// GetUserFilesByHashes 查询用户拥有的文件（验证所属权）
func GetUserFilesByHashes(ctx context.Context, username string, hashes []string) ([]UserFile, error) {
	if len(hashes) == 0 {
		return nil, nil
	}

	placeholders := make([]string, len(hashes))
	args := make([]interface{}, 0, len(hashes)+1)
	args = append(args, username)
	for i, h := range hashes {
		placeholders[i] = "?"
		args = append(args, h)
	}

	query := "SELECT user_name, file_sha1, file_name, file_size FROM tbl_user_file WHERE user_name = ? AND status = 0 AND file_sha1 IN (" + strings.Join(placeholders, ",") + ")"
	rows, err := DB.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var files []UserFile
	for rows.Next() {
		var f UserFile
		if err := rows.Scan(&f.UserName, &f.FileSha1, &f.FileName, &f.FileSize); err != nil {
			return nil, err
		}
		files = append(files, f)
	}
	return files, nil
}
