package config

var (
	MinioEndpoint  = getEnv("MINIO_ENDPOINT", "localhost:9000")
	MinioAccessKey = getEnv("MINIO_ACCESS_KEY", "minioadmin")
	MinioSecretKey = getEnv("MINIO_SECRET_KEY", "minioadmin")
	MinioBucket    = getEnv("MINIO_BUCKET", "userfile")
	MinioUseSSL    = getEnv("MINIO_USE_SSL", "false") == "true"
)
