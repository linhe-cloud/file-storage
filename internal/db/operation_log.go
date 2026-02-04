package db

import (
	"context"
	"encoding/json"
	"file-storage-linhe/internal/mq"
)

// InsertOperationLog 插入操作日志
func InsertOperationLog(ctx context.Context, msg *mq.OperationLogMessage) error {
	// 将 ExtraInfo map 转为 JSON 字符串
	var extraInfoJSON string
	if msg.ExtraInfo != nil && len(msg.ExtraInfo) > 0 {
		data, err := json.Marshal(msg.ExtraInfo)
		if err != nil {
			extraInfoJSON = "{}"
		} else {
			extraInfoJSON = string(data)
		}
	}

	_, err := DB.ExecContext(ctx,
		`INSERT INTO tbl_operation_log 
		(user_name, operation, resource_type, resource_id, ip_address, 
		 user_agent, status, error_msg, extra_info, created_at) 
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		msg.UserName,
		msg.Operation,
		msg.ResourceType,
		msg.ResourceID,
		msg.IPAddress,
		msg.UserAgent,
		msg.Status,
		msg.ErrorMsg,
		extraInfoJSON,
		msg.CreatedAt,
	)
	return err
}

// GetUserOperationLogs 查询用户操作日志
func GetUserOperationLogs(ctx context.Context, username string, limit int) ([]*OperationLog, error) {
	rows, err := DB.QueryContext(ctx,
		`SELECT id, user_name, operation, resource_type, resource_id, 
				ip_address, user_agent, status, error_msg, extra_info, created_at 
		 FROM tbl_operation_log 
		 WHERE user_name = ? 
		 ORDER BY created_at DESC 
		 LIMIT ?`,
		username, limit,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var logs []*OperationLog
	for rows.Next() {
		log := &OperationLog{}
		var extraInfo string
		if err := rows.Scan(
			&log.ID,
			&log.UserName,
			&log.Operation,
			&log.ResourceType,
			&log.ResourceID,
			&log.IPAddress,
			&log.UserAgent,
			&log.Status,
			&log.ErrorMsg,
			&extraInfo,
			&log.CreatedAt,
		); err != nil {
			return nil, err
		}
		
		// 解析 JSON 格式的 extra_info
		if extraInfo != "" {
			json.Unmarshal([]byte(extraInfo), &log.ExtraInfo)
		}
		
		logs = append(logs, log)
	}
	return logs, nil
}

// OperationLog 操作日志结构体
type OperationLog struct {
	ID           int64             `json:"id"`
	UserName     string            `json:"user_name"`
	Operation    string            `json:"operation"`
	ResourceType string            `json:"resource_type"`
	ResourceID   string            `json:"resource_id"`
	IPAddress    string            `json:"ip_address"`
	UserAgent    string            `json:"user_agent"`
	Status       string            `json:"status"`
	ErrorMsg     string            `json:"error_msg,omitempty"`
	ExtraInfo    map[string]string `json:"extra_info,omitempty"`
	CreatedAt    string            `json:"created_at"`
}
