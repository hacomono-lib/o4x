package main

import (
	"context"
	"flag"
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
	sqspub "github.com/hacomono-lib/o4x/contrib/sqs"
	"github.com/hacomono-lib/o4x/core"
)

func main() {
	// Command-line flags
	multiQueue := flag.Bool("multi-queue", false, "Enable multi-queue routing")
	workerCount := flag.Int("workers", 2, "Number of worker goroutines")
	flag.Parse()

	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	}))
	slog.SetDefault(logger)

	ctx := context.Background()

	// Configuration from environment
	dbURL := getEnv("DATABASE_URL", "postgres://postgres:postgres@localhost:15432/o4x?sslmode=disable")
	sqsEndpoint := getEnv("SQS_ENDPOINT", "http://localhost:14566")
	orderQueueURL := getEnv("ORDER_QUEUE_URL", "http://localhost:14566/000000000000/o4x-events-order.fifo")
	notificationQueueURL := getEnv("NOTIFICATION_QUEUE_URL", "http://localhost:14566/000000000000/o4x-events-notification")
	userQueueURL := getEnv("USER_QUEUE_URL", "http://localhost:14566/000000000000/o4x-events-user")
	awsRegion := getEnv("AWS_REGION", "us-east-1")
	healthPort := getEnv("HEALTH_PORT", "8080")
	pollInterval := 100 * time.Millisecond

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

	// Initialize repository and publisher
	repo := pgx.NewOutboxRepository(pool)

	var publisher core.Publisher

	if *multiQueue {
		// Multi-queue mode: route topics to different queues
		logger.Info("multi-queue mode enabled")

		// Create topic-to-queue router (default to user queue)
		router := sqspub.NewTopicQueueMap(userQueueURL)

		// Route order topics to FIFO queue (strict ordering)
		router.RegisterPrefix("order.", orderQueueURL)

		// Route notification topics to notification queue (higher throughput)
		router.RegisterPrefix("notification.", notificationQueueURL)

		// Route user topics to user queue
		router.RegisterPrefix("user.", userQueueURL)

		logger.Info("multi-queue routing configured",
			"order_queue", orderQueueURL,
			"notification_queue", notificationQueueURL,
			"user_queue", userQueueURL,
		)

		publisher = sqspub.NewMultiQueuePublisher(sqsClient, router)
	} else {
		// Single queue mode: all messages go to the order queue (default)
		logger.Info("single queue mode", "queue_url", orderQueueURL)
		publisher = sqspub.NewBatchPublisher(sqsClient, orderQueueURL)
	}

	// Revive stuck messages from previous crash
	revived, err := repo.ReviveStuckPublishing(ctx)
	if err != nil {
		logger.Error("failed to revive stuck publishing messages", "error", err)
		os.Exit(1)
	}
	if revived > 0 {
		logger.Info("revived stuck publishing messages", "count", revived)
	}

	// Initialize dispatcher
	dispatcher, err := core.NewDispatcher(repo, publisher, core.DispatcherConfig{
		PollInterval: pollInterval,
		WorkerCount:  *workerCount,
		Logger:       logger,
	})
	if err != nil {
		logger.Error("failed to create dispatcher", "error", err)
		os.Exit(1)
	}

	// Start dispatcher
	if err := dispatcher.Start(ctx); err != nil {
		logger.Error("failed to start dispatcher", "error", err)
		os.Exit(1)
	}

	mode := "single-queue"
	if *multiQueue {
		mode = "multi-queue"
	}
	logger.Info("dispatcher started",
		"mode", mode,
		"worker_count", *workerCount,
		"poll_interval", pollInterval,
	)

	// Start health check HTTP server
	mux := http.NewServeMux()

	// /health endpoint (liveness probe)
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		status := dispatcher.HealthStatus()

		if !status.IsHealthy() {
			w.WriteHeader(http.StatusServiceUnavailable)
			w.Write([]byte("Unhealthy: dispatcher not running or shutting down\n"))
			return
		}

		w.WriteHeader(http.StatusOK)
		w.Write([]byte("OK\n"))
	})

	// /ready endpoint (readiness probe)
	mux.HandleFunc("/ready", func(w http.ResponseWriter, r *http.Request) {
		if !dispatcher.IsRunning() {
			w.WriteHeader(http.StatusServiceUnavailable)
			w.Write([]byte("Not ready: dispatcher not running\n"))
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

	// Shutdown dispatcher
	dispatcher.Stop()
	logger.Info("dispatcher shutdown complete")
}

func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}
