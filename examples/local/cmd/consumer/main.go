package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	awssqs "github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/hacomono-lib/o4x/contrib/pgx"
	"github.com/hacomono-lib/o4x/contrib/sqs/consumer"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	}))
	slog.SetDefault(logger)

	ctx := context.Background()

	// Configuration from environment
	dbURL := getEnv("DATABASE_URL", "postgres://postgres:postgres@localhost:15432/o4x?sslmode=disable")
	sqsEndpoint := getEnv("SQS_ENDPOINT", "http://localhost:14566")
	sqsQueueURL := getEnv("SQS_QUEUE_URL", "http://localhost:14566/000000000000/o4x-events.fifo")
	awsRegion := getEnv("AWS_REGION", "us-east-1")
	healthPort := getEnv("HEALTH_PORT", "8081")

	// Initialize PostgreSQL connection pool
	pool, err := pgxpool.New(ctx, dbURL)
	if err != nil {
		logger.Error("failed to connect to database", "error", err)
		os.Exit(1)
	}
	defer pool.Close()

	// Verify database connection
	if err := pool.Ping(ctx); err != nil {
		logger.Error("failed to ping database", "error", err)
		os.Exit(1)
	}
	logger.Info("connected to database")

	// Initialize AWS SQS client (LocalStack compatible)
	awsCfg, err := config.LoadDefaultConfig(ctx,
		config.WithRegion(awsRegion),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
	)
	if err != nil {
		logger.Error("failed to load AWS config", "error", err)
		os.Exit(1)
	}

	sqsClient := awssqs.NewFromConfig(awsCfg, func(o *awssqs.Options) {
		o.BaseEndpoint = aws.String(sqsEndpoint)
	})

	// Initialize repository
	// Use WithConsumerMessagesTableName("custom_consumer_messages") to customize table name
	repo := pgx.NewConsumerRepository(pool)

	// Revive stuck messages from previous crash (CONSUMING -> FAILED)
	revived, err := repo.ReviveStuckConsuming(ctx)
	if err != nil {
		logger.Error("failed to revive stuck consuming messages", "error", err)
		os.Exit(1)
	}
	if revived > 0 {
		logger.Info("revived stuck consuming messages", "count", revived)
	}

	// Initialize topic router with handlers
	router := consumer.NewTopicRouter()

	// Register example handlers
	router.RegisterFunc("order.created", func(ctx context.Context, msg *consumer.SQSMessage) error {
		time.Sleep(5 * time.Second)
		logger.Info("handling order.created",
			"message_id", msg.MessageID,
			"body", string(msg.Body),
		)
		// Implement actual order processing logic here
		return nil
	})

	router.RegisterFunc("user.registered", func(ctx context.Context, msg *consumer.SQSMessage) error {
		logger.Info("handling user.registered",
			"message_id", msg.MessageID,
			"body", string(msg.Body),
		)
		// Implement actual user registration logic here
		return nil
	})

	// Set fallback handler for unknown topics
	router.SetFallback(consumer.HandlerFunc(func(ctx context.Context, msg *consumer.SQSMessage) error {
		logger.Warn("unhandled topic",
			"topic", msg.Topic,
			"message_id", msg.MessageID,
		)
		return nil
	}))

	// Initialize consumer service
	service := consumer.NewService(sqsClient, repo, router, consumer.ServiceConfig{
		QueueURL:            sqsQueueURL,
		MaxNumberOfMessages: 10,
		WaitTimeSeconds:     20,
		VisibilityTimeout:   30,
		MaxRetries:          5,
		WorkerCount:         1,
		Logger:              logger,
	})

	// Start consumer service
	if err := service.Start(ctx); err != nil {
		logger.Error("failed to start consumer service", "error", err)
		os.Exit(1)
	}

	logger.Info("consumer service started",
		"queue_url", sqsQueueURL,
	)

	// Start health check HTTP server
	// This is essential for containerized deployments (ECS, Kubernetes, etc.)
	mux := http.NewServeMux()

	// /health endpoint (liveness probe)
	// Returns 200 OK if the consumer service is running and not stuck
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		status := service.HealthStatus()

		// Check if service is running and not pending shutdown
		if !status.IsHealthy() {
			w.WriteHeader(http.StatusServiceUnavailable)
			w.Write([]byte("Unhealthy: consumer service not running or shutting down\n"))
			return
		}

		// Optional: Check if processing is stale (no messages processed for >5 minutes)
		// Uncomment if you want to detect stuck consumers
		// if status.IsStale(5 * time.Minute) {
		// 	w.WriteHeader(http.StatusServiceUnavailable)
		// 	w.Write([]byte("Unhealthy: no messages processed recently\n"))
		// 	return
		// }

		w.WriteHeader(http.StatusOK)
		w.Write([]byte("OK\n"))
	})

	// /ready endpoint (readiness probe)
	// Returns 200 OK if the consumer service is ready to process messages
	// This checks database connectivity and SQS availability
	mux.HandleFunc("/ready", func(w http.ResponseWriter, r *http.Request) {
		// Check if service is running
		if !service.IsRunning() {
			w.WriteHeader(http.StatusServiceUnavailable)
			w.Write([]byte("Not ready: consumer service not running\n"))
			return
		}

		// Check database connectivity (if repo is used)
		if err := pool.Ping(r.Context()); err != nil {
			w.WriteHeader(http.StatusServiceUnavailable)
			w.Write([]byte("Not ready: database unreachable\n"))
			return
		}

		// Optional: Check SQS queue accessibility
		// This can be done by calling GetQueueAttributes
		// For simplicity, we skip it here

		w.WriteHeader(http.StatusOK)
		w.Write([]byte("Ready\n"))
	})

	healthServer := &http.Server{
		Addr:    ":" + healthPort,
		Handler: mux,
	}

	// Start health server in goroutine
	go func() {
		logger.Info("health check server starting", "port", healthPort)
		if err := healthServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Error("health server failed", "error", err)
		}
	}()

	// Wait for shutdown signal
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	logger.Info("shutdown signal received")

	// Shutdown health server
	// Use context.WithoutCancel to preserve parent's values (trace ID, logger) during shutdown
	shutdownCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	if err := healthServer.Shutdown(shutdownCtx); err != nil {
		logger.Error("health server shutdown failed", "error", err)
	}

	// Shutdown consumer service
	service.Stop()
	logger.Info("consumer service shutdown complete")
}

func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}
