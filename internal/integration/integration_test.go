//go:build integration

package integration

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"file-storage-linhe/config"

	_ "github.com/go-sql-driver/mysql"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/redis/go-redis/v9"
)

func TestRealDependencies(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	t.Run("mysql", func(t *testing.T) { testMySQL(t, ctx) })
	t.Run("redis", func(t *testing.T) { testRedis(t, ctx) })
	t.Run("rabbitmq", func(t *testing.T) { testRabbitMQ(t, ctx) })
	t.Run("minio", func(t *testing.T) { testMinIO(t, ctx) })
}

func testMySQL(t *testing.T, ctx context.Context) {
	db, err := sql.Open("mysql", config.MySQLDNS)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := db.PingContext(ctx); err != nil {
		t.Fatal(err)
	}
	var table string
	if err := db.QueryRowContext(ctx, `SELECT TABLE_NAME FROM information_schema.tables WHERE table_schema = DATABASE() AND table_name = 'tbl_message_outbox'`).Scan(&table); err != nil {
		t.Fatalf("outbox table is not available: %v", err)
	}
	if table != "tbl_message_outbox" {
		t.Fatalf("unexpected table name %q", table)
	}
}

func testRedis(t *testing.T, ctx context.Context) {
	client := redis.NewClient(&redis.Options{Addr: config.RedisAddr, Password: config.RedisPassword, DB: config.RedisDB})
	defer client.Close()
	key := "integration:test:" + fmt.Sprint(time.Now().UnixNano())
	defer client.Del(context.Background(), key)
	if err := client.Set(ctx, key, "ok", time.Minute).Err(); err != nil {
		t.Fatal(err)
	}
	if value, err := client.Get(ctx, key).Result(); err != nil || value != "ok" {
		t.Fatalf("redis round trip value=%q err=%v", value, err)
	}
}

func testRabbitMQ(t *testing.T, ctx context.Context) {
	conn, err := amqp.Dial(config.RabbitMQURL)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	channel, err := conn.Channel()
	if err != nil {
		t.Fatal(err)
	}
	defer channel.Close()
	if err := channel.Confirm(false); err != nil {
		t.Fatal(err)
	}
	queue, err := channel.QueueDeclare("", false, true, true, false, nil)
	if err != nil {
		t.Fatal(err)
	}
	confirmations := channel.NotifyPublish(make(chan amqp.Confirmation, 1))
	body := []byte(`{"integration":true}`)
	if err := channel.PublishWithContext(ctx, "", queue.Name, false, false, amqp.Publishing{DeliveryMode: amqp.Persistent, Body: body}); err != nil {
		t.Fatal(err)
	}
	select {
	case confirmation := <-confirmations:
		if !confirmation.Ack {
			t.Fatal("rabbitmq rejected published message")
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	messages, err := channel.Consume(queue.Name, "", false, false, false, false, nil)
	if err != nil {
		t.Fatal(err)
	}
	select {
	case message := <-messages:
		if string(message.Body) != string(body) {
			t.Fatalf("message body = %q", message.Body)
		}
		if err := message.Ack(false); err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
}

func testMinIO(t *testing.T, ctx context.Context) {
	client, err := minio.New(config.MinioEndpoint, &minio.Options{Creds: credentials.NewStaticV4(config.MinioAccessKey, config.MinioSecretKey, ""), Secure: config.MinioUseSSL})
	if err != nil {
		t.Fatal(err)
	}
	key := "integration/test-" + fmt.Sprint(time.Now().UnixNano())
	_, err = client.PutObject(ctx, config.MinioBucket, key, strings.NewReader("integration"), int64(len("integration")), minio.PutObjectOptions{ContentType: "text/plain"})
	if err != nil {
		t.Fatal(err)
	}
	defer client.RemoveObject(context.Background(), config.MinioBucket, key, minio.RemoveObjectOptions{})
	if _, err := client.StatObject(ctx, config.MinioBucket, key, minio.StatObjectOptions{}); err != nil {
		t.Fatal(err)
	}
}

func TestIntegrationConfiguration(t *testing.T) {
	if os.Getenv("MYSQL_PASSWORD") == "" {
		t.Log("MYSQL_PASSWORD is empty; use the deployment credential when running integration tests")
	}
}
