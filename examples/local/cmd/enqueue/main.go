package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/hacomono-lib/o4x/contrib/pgx"
)

func main() {
	// Flags
	topic := flag.String("topic", "", "Message topic (required)")
	payload := flag.String("payload", "{}", "Message payload as JSON")
	idempotencyKey := flag.String("key", "", "Idempotency key (required)")
	maxRetries := flag.Int("max-retries", 10, "Maximum retry attempts")
	flag.Parse()

	if *topic == "" || *idempotencyKey == "" {
		fmt.Fprintln(os.Stderr, "Usage: enqueue -topic <topic> -key <idempotency_key> [-payload <json>] [-max-retries <n>]")
		fmt.Fprintln(os.Stderr, "\nExamples:")
		fmt.Fprintln(os.Stderr, "  enqueue -topic order.created -key order-123 -payload '{\"order_id\":\"123\",\"total\":99.99}'")
		fmt.Fprintln(os.Stderr, "  enqueue -topic user.registered -key user-456 -payload '{\"user_id\":\"456\",\"email\":\"test@example.com\"}'")
		os.Exit(1)
	}

	// Validate JSON payload
	if !json.Valid([]byte(*payload)) {
		fmt.Fprintln(os.Stderr, "Error: invalid JSON payload")
		os.Exit(1)
	}

	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))

	ctx := context.Background()

	// Configuration from environment
	dbURL := getEnv("DATABASE_URL", "postgres://postgres:postgres@localhost:15432/o4x?sslmode=disable")

	// Initialize PostgreSQL connection pool
	pool, err := pgxpool.New(ctx, dbURL)
	if err != nil {
		logger.Error("failed to connect to database", "error", err)
		os.Exit(1)
	}
	defer pool.Close()

	// Initialize repository
	// Use WithOutboxTableName("custom_outbox") to customize table name
	repo := pgx.NewOutboxRepository(pool)

	// Insert message
	start := time.Now()
	outbox, err := repo.InsertOutboxJSON(ctx, *topic, json.RawMessage(*payload), *idempotencyKey, *maxRetries)
	if err != nil {
		logger.Error("failed to insert message", "error", err)
		os.Exit(1)
	}

	logger.Info("message enqueued successfully",
		"id", outbox.ID,
		"topic", outbox.Topic,
		"idempotency_key", outbox.IdempotencyKey,
		"status", outbox.Status,
		"duration", time.Since(start),
	)

	fmt.Printf("Enqueued: id=%s topic=%s key=%s\n", outbox.ID, outbox.Topic, outbox.IdempotencyKey)
}

func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}
