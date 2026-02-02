package config

var (
	RedisAddr     = getEnv("REDIS_ADDR", "localhost:6379")
	RedisPassword = getEnv("REDIS_PASSWORD", "")
	RedisDB       = getEnvInt("REDIS_DB", 0)
)
