package db

import (
	"context"
	"database/sql"
	"file-storage-linhe/internal/meta"
)

func SaveFileUploadWithOutbox(ctx context.Context, username string, fm *meta.FileMeta, outboxType string, outboxPayload any) error {
	tx, err := DB.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx,
		`INSERT INTO tbl_file (file_sha1, file_name, file_size, file_addr) VALUES (?, ?, ?, ?)
		 ON DUPLICATE KEY UPDATE status = 0`, fm.FileSha1, fm.FileName, fm.FileSize, fm.Location); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx,
		`INSERT INTO tbl_user_file (user_name, file_sha1, file_name, file_size) VALUES (?, ?, ?, ?)
		 ON DUPLICATE KEY UPDATE status = 0, file_name = VALUES(file_name), file_size = VALUES(file_size)`,
		username, fm.FileSha1, fm.FileName, fm.FileSize); err != nil {
		return err
	}
	if outboxType != "" {
		if err := insertOutboxTx(ctx, tx, outboxType, fm.FileSha1, outboxPayload); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func DeleteUserFileWithOutbox(ctx context.Context, username, fileHash string, payload any) (bool, error) {
	tx, err := DB.BeginTx(ctx, nil)
	if err != nil {
		return false, err
	}
	defer tx.Rollback()
	result, err := tx.ExecContext(ctx,
		`UPDATE tbl_user_file SET status = 1 WHERE user_name = ? AND file_sha1 = ? AND status = 0`, username, fileHash)
	if err != nil {
		return false, err
	}
	if affected, _ := result.RowsAffected(); affected == 0 {
		return false, sql.ErrNoRows
	}
	count, err := countActiveFileRefsTx(ctx, tx, fileHash)
	if err != nil {
		return false, err
	}
	if count == 0 {
		if err := insertOutboxTx(ctx, tx, "file_delete", fileHash, payload); err != nil {
			return false, err
		}
	}
	if err := tx.Commit(); err != nil {
		return false, err
	}
	return count == 0, nil
}

func PermanentDeleteUserFileWithOutbox(ctx context.Context, username, fileHash, location string, payload any) error {
	tx, err := DB.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	result, err := tx.ExecContext(ctx,
		`DELETE FROM tbl_user_file WHERE user_name = ? AND file_sha1 = ? AND status = 1`, username, fileHash)
	if err != nil {
		return err
	}
	if affected, _ := result.RowsAffected(); affected == 0 {
		return sql.ErrNoRows
	}
	count, err := countActiveFileRefsTx(ctx, tx, fileHash)
	if err != nil {
		return err
	}
	if count == 0 {
		if _, err := tx.ExecContext(ctx, `UPDATE tbl_file SET status = 1 WHERE file_sha1 = ?`, fileHash); err != nil {
			return err
		}
		if err := insertOutboxTx(ctx, tx, "file_cleanup", fileHash, payload); err != nil {
			return err
		}
	}
	return tx.Commit()
}

// countActiveFileRefsTx locks the active references while deciding whether the
// last reference was removed. This prevents concurrent deletes from both
// observing a non-zero count and skipping cleanup.
func countActiveFileRefsTx(ctx context.Context, tx *sql.Tx, fileHash string) (int, error) {
	rows, err := tx.QueryContext(ctx,
		`SELECT user_name FROM tbl_user_file WHERE file_sha1 = ? AND status = 0 FOR UPDATE`, fileHash)
	if err != nil {
		return 0, err
	}
	defer rows.Close()
	count := 0
	for rows.Next() {
		var username string
		if err := rows.Scan(&username); err != nil {
			return 0, err
		}
		count++
	}
	if err := rows.Err(); err != nil {
		return 0, err
	}
	return count, nil
}
