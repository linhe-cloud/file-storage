package config

var (
	RabbitMQURL        = getEnv("RABBITMQ_URL", "amqp://guest:guest@localhost:5672/")
	RabbitMQDelayQueue = getEnv("RABBITMQ_DELAY_QUEUE", "file_delete_delay_queue")
	RabbitMQDelayTTL   = getEnvInt64("RABBITMQ_DELAY_TTL", 259200000)
)
