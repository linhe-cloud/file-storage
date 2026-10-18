package db

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
)

const (
	OutboxPending     = "pending"
	OutboxProcessing  = "processing"
	OutboxPublished   = "published"
	OutboxFailed      = "failed"
	OutboxMaxAttempts = 10
)

type OutboxMessage struct {
	ID       int64
	Type     string
	Payload  []byte
	Attempts int
}

type OutboxRecord struct {
	ID           int64           `json:"id"`
	MessageType  string          `json:"message_type"`
	AggregateKey string          `json:"aggregate_key"`
	Payload      json.RawMessage `json:"payload"`
	Status       string          `json:"status"`
	Attempts     int             `json:"attempts"`
	LastError    string          `json:"last_error,omitempty"`
	CreatedAt    string          `json:"created_at"`
	NextAttempt  string          `json:"next_attempt_at"`
}

// insertOutboxTx 将消息插入到发件箱表中
// ctx: 上下文信息，用于控制请求的超时和取消
// tx: 数据库事务对象，确保操作的原子性
// messageType: 消息类型，用于区分不同类型的消息
// aggregateKey: 聚合键，用于标识消息所属的聚合根
// payload: 消息的具体内容，可以是任意类型
// 返回值: error，如果操作成功则为nil，否则返回相应的错误信息
func insertOutboxTx(ctx context.Context, tx *sql.Tx, messageType, aggregateKey string, payload any) error {
	// 将payload序列化为JSON格式
	body, err := json.Marshal(payload)
	if err != nil {
		// 如果序列化失败，直接返回错误
		return err
	}
	// 执行SQL插入操作，将消息存入发件箱表
	// 消息初始状态为'pending'，尝试次数为0，下次尝试时间为当前时间
	_, err = tx.ExecContext(ctx,
		`INSERT INTO tbl_message_outbox (message_type, aggregate_key, payload, status, attempts, next_attempt_at)
		 VALUES (?, ?, ?, 'pending', 0, CURRENT_TIMESTAMP)`,
		messageType, aggregateKey, body)
	return err
}

// EnqueueOutbox 将消息加入到输出队列中
// 使用事务确保消息的可靠存储
//
// @param ctx 上下文，用于控制请求的超时和取消
// @param messageType 消息的类型，用于区分不同种类的消息
// @param aggregateKey 聚合根的键，用于标识消息所属的业务实体
// @param payload 消息的具体内容，可以是任意类型的数据
// @return error 如果操作成功返回nil，失败返回错误信息
func EnqueueOutbox(ctx context.Context, messageType, aggregateKey string, payload any) error {
	// 开始一个数据库事务
	tx, err := DB.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	// 使用defer确保在函数返回前处理事务回滚
	defer tx.Rollback()
	// 在事务中插入输出消息
	if err := insertOutboxTx(ctx, tx, messageType, aggregateKey, payload); err != nil {
		return err
	}
	// 提交事务，完成消息的存储
	return tx.Commit()
}

func claimPendingOutbox(ctx context.Context, limit int) ([]OutboxMessage, error) {
	tx, err := DB.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	rows, err := tx.QueryContext(ctx,
		`SELECT id, message_type, payload, attempts
		 FROM tbl_message_outbox
		 WHERE (status = 'pending' AND next_attempt_at <= CURRENT_TIMESTAMP)
		    OR (status = 'processing' AND locked_at < DATE_SUB(CURRENT_TIMESTAMP, INTERVAL 5 MINUTE))
		 ORDER BY id LIMIT ? FOR UPDATE`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var messages []OutboxMessage
	for rows.Next() {
		var msg OutboxMessage
		if err := rows.Scan(&msg.ID, &msg.Type, &msg.Payload, &msg.Attempts); err != nil {
			return nil, err
		}
		messages = append(messages, msg)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	for _, msg := range messages {
		if _, err := tx.ExecContext(ctx,
			`UPDATE tbl_message_outbox SET status = 'processing', locked_at = CURRENT_TIMESTAMP WHERE id = ?`, msg.ID); err != nil {
			return nil, err
		}
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return messages, nil
}

func markOutboxPublished(ctx context.Context, id int64) error {
	_, err := DB.ExecContext(ctx,
		`UPDATE tbl_message_outbox SET status = 'published', published_at = CURRENT_TIMESTAMP, locked_at = NULL WHERE id = ?`, id)
	return err
}

func markOutboxRetry(ctx context.Context, id int64, attempts int, publishErr error) error {
	status := OutboxPending
	if attempts >= OutboxMaxAttempts {
		status = OutboxFailed
	}
	backoffSeconds := attempts * attempts
	if backoffSeconds < 5 {
		backoffSeconds = 5
	}
	_, err := DB.ExecContext(ctx,
		`UPDATE tbl_message_outbox
		 SET status = ?, attempts = ?, last_error = ?, locked_at = NULL,
		     next_attempt_at = DATE_ADD(CURRENT_TIMESTAMP, INTERVAL ? SECOND)
		 WHERE id = ?`, status, attempts, publishErr.Error(), backoffSeconds, id)
	return err
}

func EnsureOutboxTable(ctx context.Context) error {
	_, err := DB.ExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS tbl_message_outbox (
			id BIGINT NOT NULL AUTO_INCREMENT,
			message_type VARCHAR(64) NOT NULL,
			aggregate_key VARCHAR(128) NOT NULL DEFAULT '',
			payload JSON NOT NULL,
			status VARCHAR(16) NOT NULL DEFAULT 'pending',
			attempts INT NOT NULL DEFAULT 0,
			next_attempt_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
			locked_at DATETIME NULL,
			published_at DATETIME NULL,
			last_error TEXT NULL,
			created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
			PRIMARY KEY (id),
			KEY idx_outbox_pending (status, next_attempt_at),
			KEY idx_outbox_aggregate (aggregate_key)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`)
	return err
}

func ClaimPendingOutbox(ctx context.Context, limit int) ([]OutboxMessage, error) {
	if limit <= 0 || limit > 1000 {
		return nil, fmt.Errorf("invalid outbox limit")
	}
	return claimPendingOutbox(ctx, limit)
}

func MarkOutboxPublished(ctx context.Context, id int64) error { return markOutboxPublished(ctx, id) }

func MarkOutboxRetry(ctx context.Context, id int64, attempts int, publishErr error) error {
	if publishErr == nil {
		publishErr = fmt.Errorf("unknown outbox publish error")
	}
	return markOutboxRetry(ctx, id, attempts+1, publishErr)
}

func ListOutbox(ctx context.Context, status string, limit int) ([]OutboxRecord, error) {
	if limit <= 0 || limit > 500 {
		return nil, fmt.Errorf("invalid outbox limit")
	}
	query := `SELECT id, message_type, aggregate_key, payload, status, attempts,
	                 COALESCE(last_error, ''), created_at, next_attempt_at
	          FROM tbl_message_outbox`
	args := []any{}
	if status != "" {
		query += " WHERE status = ?"
		args = append(args, status)
	}
	query += " ORDER BY id DESC LIMIT ?"
	args = append(args, limit)
	rows, err := DB.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var records []OutboxRecord
	for rows.Next() {
		var record OutboxRecord
		if err := rows.Scan(&record.ID, &record.MessageType, &record.AggregateKey, &record.Payload, &record.Status, &record.Attempts, &record.LastError, &record.CreatedAt, &record.NextAttempt); err != nil {
			return nil, err
		}
		record.Payload = append(json.RawMessage(nil), record.Payload...)
		records = append(records, record)
	}
	return records, rows.Err()
}

func RetryOutbox(ctx context.Context, id int64) error {
	result, err := DB.ExecContext(ctx,
		`UPDATE tbl_message_outbox
		 SET status = 'pending', attempts = 0, next_attempt_at = CURRENT_TIMESTAMP,
		     locked_at = NULL, last_error = NULL
		 WHERE id = ? AND status = 'failed'`, id)
	if err != nil {
		return err
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if affected == 0 {
		return sql.ErrNoRows
	}
	return nil
}
