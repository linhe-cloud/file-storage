package db

import (
	"context"
	"file-storage-linhe/internal/meta"
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
		"SELECT file_sha1, file_name, file_size, file_addr FROM tbl_file WHERE file_sha1 = ?",
		sha1).Scan(&fm.FileSha1, &fm.FileName, &fm.FileSize, &fm.Location)
	if err != nil {
		return nil, err
	}
	return fm, err
}

// 插入用户文件关系
func InsertUserFile(ctx context.Context, username, fileSha1, fileName string, fileSize int64) error {
	_, err := DB.ExecContext(ctx,
		"INSERT INTO tbl_user_file (user_name, file_sha1, file_name, file_size) VALUES (?, ?, ?, ?)",
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
