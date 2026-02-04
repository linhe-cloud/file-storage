package handler

import (
	"context"
	"log"
	"net/http"
	"strings"

	"file-storage-linhe/internal/mq"
)

// LogOperation 记录操作日志（异步）
func LogOperation(ctx context.Context, r *http.Request, username, operation, resourceType, resourceID string, extraInfo map[string]string) {
	msg := mq.NewOperationLogMessage(username, operation, resourceType, resourceID).
		WithIPAndUA(getClientIP(r), r.UserAgent()).
		WithExtraInfo(extraInfo)

	// 异步发送，不阻塞主流程
	if err := mq.PublishOperationLog(ctx, msg); err != nil {
		log.Printf("发送操作日志失败: %v", err)
		// 注意：失败不影响业务
	}
}

// LogOperationError 记录失败的操作日志
func LogOperationError(ctx context.Context, r *http.Request, username, operation, resourceType, resourceID string, errMsg string) {
	msg := mq.NewOperationLogMessage(username, operation, resourceType, resourceID).
		WithIPAndUA(getClientIP(r), r.UserAgent()).
		WithError(errMsg)

	if err := mq.PublishOperationLog(ctx, msg); err != nil {
		log.Printf("发送操作日志失败: %v", err)
	}
}

// getClientIP 获取客户端真实IP
func getClientIP(r *http.Request) string {
	// 优先从 X-Real-IP 获取
	ip := r.Header.Get("X-Real-IP")
	if ip != "" {
		return ip
	}

	// 其次从 X-Forwarded-For 获取
	ip = r.Header.Get("X-Forwarded-For")
	if ip != "" {
		// X-Forwarded-For 可能包含多个IP，取第一个
		ips := strings.Split(ip, ",")
		if len(ips) > 0 {
			return strings.TrimSpace(ips[0])
		}
	}

	// 最后从 RemoteAddr 获取
	ip = r.RemoteAddr
	// RemoteAddr 格式为 "IP:Port"，需要去掉端口
	if idx := strings.LastIndex(ip, ":"); idx != -1 {
		return ip[:idx]
	}
	return ip
}
