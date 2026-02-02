package handler

import (
	"encoding/json"
	"log"
	"net/http"
	"runtime/debug"
)

// RecoverMiddleware 统一捕获 panic 的中间件
func RecoverMiddleware(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if err := recover(); err != nil {
				// 记录详细的错误日志和堆栈信息
				log.Printf("Panic recovered: %v\n%s", err, debug.Stack())

				// 返回统一的 JSON 错误响应
				w.Header().Set("Content-Type", "application/json; charset=utf-8")
				w.WriteHeader(http.StatusInternalServerError)

				response := map[string]string{
					"error": "internal server error",
				}
				_ = json.NewEncoder(w).Encode(response)
			}
		}()

		// 执行实际的 handler
		next(w, r)
	}
}
