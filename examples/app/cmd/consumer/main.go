package main

import (
	"context"
	"flag"
	"fmt"
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
	appconsumer "github.com/hacomono-lib/o4x/examples/app/internal/consumer"
	"github.com/hacomono-lib/o4x/examples/app/internal/repository"
	"github.com/hacomono-lib/o4x/examples/app/internal/service"
)

func main() {
	// Command-line flags
	fifo := flag.Bool("fifo", false, "Connect to FIFO queue (default: Standard queue)")
	simulateFailure := flag.Bool("simulate-failure", false, "Simulate random failures for testing")
	failureRate := flag.Float64("failure-rate", 0.3, "Failure rate for simulated failures (0.0-1.0)")
	workerCount := flag.Int("workers", 2, "Number of worker goroutines")
	messageConcurrency := flag.Int("message-concurrency", 1, "Number of messages to process concurrently within each worker (>1 only for Standard queues)")
	truncateTables := flag.Bool("truncate-tables", false, "Truncate idempotency tables on startup (for development/testing)")
	flag.Parse()

	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	}))
	slog.SetDefault(logger)

	ctx := context.Background()

	// Configuration from environment
	dbURL := getEnv("DATABASE_URL", "postgres://postgres:postgres@localhost:15432/o4x?sslmode=disable")
	sqsEndpoint := getEnv("SQS_ENDPOINT", "http://localhost:14566")

	// Default: Standard queue, --fifo flag switches to FIFO queue
	var sqsQueueURL string
	if *fifo {
		sqsQueueURL = getEnv("SQS_QUEUE_URL", "http://localhost:14566/000000000000/o4x-events.fifo")
	} else {
		sqsQueueURL = getEnv("SQS_QUEUE_URL", "http://localhost:14566/000000000000/o4x-events-standard")
	}

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

	// Truncate idempotency tables if requested (for development/testing)
	if *truncateTables {
		logger.Warn("truncating idempotency tables",
			"tables", []string{"order_confirmations", "user_welcome_credits"},
		)
		_, err := pool.Exec(ctx, `
			TRUNCATE TABLE order_confirmations;
			TRUNCATE TABLE user_welcome_credits;
		`)
		if err != nil {
			logger.Error("failed to truncate tables", "error", err)
			os.Exit(1)
		}
		logger.Info("idempotency tables truncated successfully")
	}

	// Initialize AWS SQS client
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

	// Initialize repositories
	consumerRepo := pgx.NewConsumerRepository(pool)
	notificationRepo := repository.NewNotificationRepository(pool)
	outboxRepo := pgx.NewOutboxRepository(pool)

	// Initialize services
	notificationService := service.NewNotificationService(pool, notificationRepo, outboxRepo)

	// Revive stuck messages from previous crash
	revived, err := consumerRepo.ReviveStuckConsuming(ctx)
	if err != nil {
		logger.Error("failed to revive stuck consuming messages", "error", err)
		os.Exit(1)
	}
	if revived > 0 {
		logger.Info("revived stuck consuming messages", "count", revived)
	}

	queueType := "Standard"
	if *fifo {
		queueType = "FIFO"
	}
	logger.Info("queue configuration",
		"type", queueType,
		"url", sqsQueueURL,
		"worker_count", *workerCount,
		"message_concurrency", *messageConcurrency,
		"simulate_failure", *simulateFailure,
		"failure_rate", *failureRate,
	)

	// Parse sleep configurations from environment variables
	// These simulate real-world processing times for different handler types
	orderCreatedSleep := appconsumer.SleepConfig{
		Min: appconsumer.ParseSleepDuration(os.Getenv("ORDER_CREATED_SLEEP_MIN"), 20*time.Millisecond),
		Max: appconsumer.ParseSleepDuration(os.Getenv("ORDER_CREATED_SLEEP_MAX"), 80*time.Millisecond),
	}
	orderConfirmedSleep := appconsumer.SleepConfig{
		Min: appconsumer.ParseSleepDuration(os.Getenv("ORDER_CONFIRMED_SLEEP_MIN"), 10*time.Millisecond),
		Max: appconsumer.ParseSleepDuration(os.Getenv("ORDER_CONFIRMED_SLEEP_MAX"), 40*time.Millisecond),
	}
	userRegisteredSleep := appconsumer.SleepConfig{
		Min: appconsumer.ParseSleepDuration(os.Getenv("USER_REGISTERED_SLEEP_MIN"), 5*time.Millisecond),
		Max: appconsumer.ParseSleepDuration(os.Getenv("USER_REGISTERED_SLEEP_MAX"), 20*time.Millisecond),
	}
	userUpdatedSleep := appconsumer.SleepConfig{
		Min: appconsumer.ParseSleepDuration(os.Getenv("USER_UPDATED_SLEEP_MIN"), 10*time.Millisecond),
		Max: appconsumer.ParseSleepDuration(os.Getenv("USER_UPDATED_SLEEP_MAX"), 30*time.Millisecond),
	}
	notificationEmailSleep := appconsumer.SleepConfig{
		Min: appconsumer.ParseSleepDuration(os.Getenv("NOTIFICATION_EMAIL_SLEEP_MIN"), 50*time.Millisecond),
		Max: appconsumer.ParseSleepDuration(os.Getenv("NOTIFICATION_EMAIL_SLEEP_MAX"), 200*time.Millisecond),
	}
	notificationSMSSleep := appconsumer.SleepConfig{
		Min: appconsumer.ParseSleepDuration(os.Getenv("NOTIFICATION_SMS_SLEEP_MIN"), 30*time.Millisecond),
		Max: appconsumer.ParseSleepDuration(os.Getenv("NOTIFICATION_SMS_SLEEP_MAX"), 150*time.Millisecond),
	}
	notificationPushSleep := appconsumer.SleepConfig{
		Min: appconsumer.ParseSleepDuration(os.Getenv("NOTIFICATION_PUSH_SLEEP_MIN"), 20*time.Millisecond),
		Max: appconsumer.ParseSleepDuration(os.Getenv("NOTIFICATION_PUSH_SLEEP_MAX"), 100*time.Millisecond),
	}

	logger.Info("handler sleep configuration",
		"order.created", fmt.Sprintf("%v-%v", orderCreatedSleep.Min, orderCreatedSleep.Max),
		"order.confirmed", fmt.Sprintf("%v-%v", orderConfirmedSleep.Min, orderConfirmedSleep.Max),
		"user.registered", fmt.Sprintf("%v-%v", userRegisteredSleep.Min, userRegisteredSleep.Max),
		"user.updated", fmt.Sprintf("%v-%v", userUpdatedSleep.Min, userUpdatedSleep.Max),
		"notification.email", fmt.Sprintf("%v-%v", notificationEmailSleep.Min, notificationEmailSleep.Max),
		"notification.sms", fmt.Sprintf("%v-%v", notificationSMSSleep.Min, notificationSMSSleep.Max),
		"notification.push", fmt.Sprintf("%v-%v", notificationPushSleep.Min, notificationPushSleep.Max),
	)

	// Initialize topic router with handlers
	router := consumer.NewTopicRouter()

	// Register order handlers
	orderCreatedHandler := appconsumer.NewOrderCreatedHandler(pool, notificationService, logger, orderCreatedSleep)
	orderConfirmedHandler := appconsumer.NewOrderConfirmedHandler(pool, logger, orderConfirmedSleep)
	router.Register("order.created", orderCreatedHandler)
	router.Register("order.confirmed", orderConfirmedHandler)

	// Register user handlers
	userRegisteredHandler := appconsumer.NewUserRegisteredHandler(pool, logger, userRegisteredSleep)
	userUpdatedHandler := appconsumer.NewUserUpdatedHandler(pool, logger, userUpdatedSleep)
	router.Register("user.registered", userRegisteredHandler)
	router.Register("user.updated", userUpdatedHandler)

	// Register notification handlers
	notificationEmailHandler := appconsumer.NewNotificationEmailHandler(
		pool, notificationRepo, logger, *simulateFailure, *failureRate, notificationEmailSleep,
	)
	notificationSMSHandler := appconsumer.NewNotificationSMSHandler(pool, logger, notificationSMSSleep)
	notificationPushHandler := appconsumer.NewNotificationPushHandler(pool, logger, notificationPushSleep)
	router.Register("notification.email", notificationEmailHandler)
	router.Register("notification.sms", notificationSMSHandler)
	router.Register("notification.push", notificationPushHandler)

	// Set fallback handler for unknown topics
	router.SetFallback(consumer.HandlerFunc(func(ctx context.Context, msg *consumer.SQSMessage) error {
		logger.Warn("unhandled topic",
			"topic", msg.Topic,
			"message_id", msg.MessageID,
		)
		return nil
	}))

	// Initialize consumer service
	service := consumer.NewService(sqsClient, consumerRepo, router, consumer.ServiceConfig{
		QueueURL:            sqsQueueURL,
		MaxNumberOfMessages: 10,
		WaitTimeSeconds:     20,
		VisibilityTimeout:   30,
		MaxRetries:          5,
		WorkerCount:         *workerCount,
		MessageConcurrency:  *messageConcurrency,
		Logger:              logger,
	})

	// Start consumer service
	if err := service.Start(ctx); err != nil {
		logger.Error("failed to start consumer service", "error", err)
		os.Exit(1)
	}

	logger.Info("consumer service started",
		"queue_url", sqsQueueURL,
		"worker_count", *workerCount,
	)

	// Start health check HTTP server
	mux := http.NewServeMux()

	// /health endpoint (liveness probe)
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		status := service.HealthStatus()

		if !status.IsHealthy() {
			w.WriteHeader(http.StatusServiceUnavailable)
			w.Write([]byte("Unhealthy: consumer service not running or shutting down\n"))
			return
		}

		w.WriteHeader(http.StatusOK)
		w.Write([]byte("OK\n"))
	})

	// /ready endpoint (readiness probe)
	mux.HandleFunc("/ready", func(w http.ResponseWriter, r *http.Request) {
		if !service.IsRunning() {
			w.WriteHeader(http.StatusServiceUnavailable)
			w.Write([]byte("Not ready: consumer service not running\n"))
			return
		}

		if err := pool.Ping(r.Context()); err != nil {
			w.WriteHeader(http.StatusServiceUnavailable)
			w.Write([]byte("Not ready: database unreachable\n"))
			return
		}

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
