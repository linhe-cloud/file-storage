package config

import (
	"os"
	"strconv"
)

// getEnv 获取环境变量的值，如果环境变量不存在则返回默认值
// 参数:
//
//	key: 环境变量的名称
//	defaultValue: 当环境变量不存在时返回的默认值
//
// 返回值:
//
//	string: 环境变量的值或默认值
func getEnv(key, defaultValue string) string {
	// 尝试获取环境变量的值
	if value := os.Getenv(key); value != "" {
		// 如果环境变量存在且不为空，则返回该值
		return value
	}
	// 如果环境变量不存在或为空，则返回默认值
	return defaultValue
}

// getEnvInt 获取环境变量并转换为int，如果不存在或转换失败则返回默认值
func getEnvInt(key string, defaultValue int) int {
	if value := os.Getenv(key); value != "" {
		if intValue, err := strconv.Atoi(value); err == nil {
			return intValue
		}
	}
	return defaultValue
}

func getEnvInt64(key string, defaultValue int64) int64 {
	if value := os.Getenv(key); value != "" {
		if intValue, err := strconv.ParseInt(value, 10, 64); err == nil {
			return intValue
		}
	}
	return defaultValue
}
