package config

var (
	JWTPrivateKeyPath = getEnv("JWT_PRIVATE_KEY_PATH", "config/key/private.pem")
	JWTPublicKeyPath  = getEnv("JWT_PUBLIC_KEY_PATH", "config/key/public.pem")
)
