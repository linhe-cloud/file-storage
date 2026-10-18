package config

var (
	OutboxAdminToken = getEnv("OUTBOX_ADMIN_TOKEN", "")
	AlertWebhookURL  = getEnv("ALERT_WEBHOOK_URL", "")
)
