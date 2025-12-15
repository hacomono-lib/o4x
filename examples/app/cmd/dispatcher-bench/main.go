package main

import (
	"context"
	"flag"
	"log/slog"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	awssqs "github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/smithy-go/middleware"
	smithyhttp "github.com/aws/smithy-go/transport/http"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/hacomono-lib/o4x/contrib/pgx"
	sqspub "github.com/hacomono-lib/o4x/contrib/sqs"
	"github.com/hacomono-lib/o4x/core"
	"github.com/hacomono-lib/o4x/examples/app/internal/instrumentation"
)

func main() {
	// Command-line flags
	multiQueue := flag.Bool("multi-queue", false, "Enable multi-queue routing")
	workerCount := flag.Int("workers", 2, "Number of worker goroutines")
	flag.Parse()

	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo, // Changed to Info for cleaner benchmarkoutput
	}))
	slog.SetDefault(logger)

	ctx := context.Background()

	// Configuration from environment
	dbURL := getEnv("DATABASE_URL", "postgres://postgres:postgres@localhost:15432/o4x?sslmode=disable")
	sqsEndpoint := getEnv("SQS_ENDPOINT", "http://localhost:14566")
	awsRegion := getEnv("AWS_REGION", "us-east-1")
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

	// Create SQS client with forced endpoint override for LocalStack compatibility
	// This ensures all SQS API calls use the LocalStack endpoint regardless of Queue URL format
	sqsClient := awssqs.NewFromConfig(awsCfg, func(o *awssqs.Options) {
		o.BaseEndpoint = aws.String(sqsEndpoint)
		o.EndpointOptions.DisableHTTPS = true

		// Add custom middleware to rewrite request URLs to LocalStack endpoint
		// This solves the issue where Queue URLs contain unresolvable hostnames like localhost.localstack.cloud
		o.APIOptions = append(o.APIOptions, func(stack *middleware.Stack) error {
			return stack.Build.Add(middleware.BuildMiddlewareFunc("LocalStackURLRewriter",
				func(ctx context.Context, in middleware.BuildInput, next middleware.BuildHandler) (middleware.BuildOutput, middleware.Metadata, error) {
					req, ok := in.Request.(*smithyhttp.Request)
					if ok && req.URL != nil {
						// Parse SQS endpoint to get host
						endpointURL, err := url.Parse(sqsEndpoint)
						if err == nil {
							// Replace host with LocalStack endpoint host
							req.URL.Scheme = endpointURL.Scheme
							req.URL.Host = endpointURL.Host
						}
					}
					return next.HandleBuild(ctx, in)
				}), middleware.After)
		})
	})

	// Discover actual queue URLs from SQS (LocalStack returns different URL formats)
	listResult, err := sqsClient.ListQueues(ctx, &awssqs.ListQueuesInput{})
	if err != nil {
		logger.Error("failed to list SQS queues", "error", err)
		os.Exit(1)
	}

	// Map queue names to actual URLs (normalize for docker-compose compatibility)
	queueURLs := make(map[string]string)
	for _, queueURL := range listResult.QueueUrls {
		// Extract queue name from URL (last segment)
		segments := strings.Split(queueURL, "/")
		if len(segments) > 0 {
			queueName := segments[len(segments)-1]
			// Normalize URL to use SQS endpoint host (e.g., localstack:4566)
			normalizedURL := normalizeQueueURL(queueURL, sqsEndpoint)
			queueURLs[queueName] = normalizedURL
		}
	}

	orderQueueURL := queueURLs["o4x-events-order.fifo"]
	notificationQueueURL := queueURLs["o4x-events-notification"]
	userQueueURL := queueURLs["o4x-events-user"]

	logger.Info("discovered queue URLs",
		"order", orderQueueURL,
		"notification", notificationQueueURL,
		"user", userQueueURL,
	)

	// Initialize repository and instrumentation
	baseRepo := pgx.NewOutboxRepository(pool)

	// Create metrics collector
	metricsCollector := instrumentation.NewMetricsCollector(logger)

	// Create PostgreSQL metrics collector
	pgMetricsCollector := instrumentation.NewPGMetricsCollector(pool, logger)

	// Wrap repository with instrumentation
	instrumentedRepo := instrumentation.NewInstrumentedBatchRepository(baseRepo, metricsCollector, 0)

	var publisher core.BatchPublisher

	if *multiQueue {
		// Multi-queue mode: route event types to different queues
		logger.Info("multi-queue mode enabled")

		// Create event-type-to-queue router (default to user queue)
		router := sqspub.NewEventTypeQueueMap(userQueueURL)

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

		publisher = sqspub.NewMultiBatchPublisher(sqsClient, router)
	} else {
		// Single queue mode: all messages go to the user queue (default)
		logger.Info("single queue mode", "queue_url", userQueueURL)
		publisher = sqspub.NewBatchPublisher(sqsClient, userQueueURL)
	}

	// Wrap publisher with instrumentation to record publish metrics
	publisher = instrumentation.NewInstrumentedBatchPublisher(publisher, metricsCollector, logger)

	// Revive stuck messages from previous crash
	revived, err := baseRepo.ReviveStuckPublishing(ctx)
	if err != nil {
		logger.Error("failed to revive stuck publishing messages", "error", err)
		os.Exit(1)
	}
	if revived > 0 {
		logger.Info("revived stuck publishing messages", "count", revived)
	}

	// Initialize batch dispatcher with instrumented repository
	dispatcher, err := core.NewBatchDispatcher(instrumentedRepo, publisher, core.BatchDispatcherConfig{
		PollInterval:    pollInterval,
		BatchSize:       10, // Process messages in batches of 10
		WorkerCount:     *workerCount,
		RequeueInterval: 1 * time.Second, // Retry FAILED messages every 1 second (bench environment)
		Logger:          logger,
	})
	if err != nil {
		logger.Error("failed to create batch dispatcher", "error", err)
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
	logger.Info("instrumented batch dispatcher started",
		"mode", mode,
		"worker_count", *workerCount,
		"batch_size", 10,
		"poll_interval", pollInterval,
	)

	// Start periodic metrics logging
	metricsTicker := time.NewTicker(10 * time.Second)
	defer metricsTicker.Stop()

	go func() {
		for range metricsTicker.C {
			metricsCollector.LogStats()
		}
	}()

	// Start PostgreSQL metrics collection
	pgCtx, pgCancel := context.WithCancel(context.Background())
	defer pgCancel()

	go pgMetricsCollector.StartPeriodicCollection(pgCtx, 10*time.Second)

	// Wait for shutdown signal
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	logger.Info("shutdown signal received")

	// Stop metrics logging
	metricsTicker.Stop()

	// Stop PostgreSQL metrics collection
	pgCancel()

	// Final metrics before shutdown
	logger.Info("=== FINAL METRICS ===")
	metricsCollector.LogStats()
	pgMetricsCollector.LogAggregateStats()

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

// normalizeQueueURL replaces the host in queueURL with the host from sqsEndpoint.
// This is necessary because LocalStack returns queue URLs with localhost.localstack.cloud
// which is not resolvable from within docker-compose containers.
func normalizeQueueURL(queueURL, sqsEndpoint string) string {
	u, err := url.Parse(queueURL)
	if err != nil {
		return queueURL
	}

	endpointURL, err := url.Parse(sqsEndpoint)
	if err != nil {
		return queueURL
	}

	// Replace host and scheme with endpoint's values
	u.Host = endpointURL.Host
	u.Scheme = endpointURL.Scheme

	return u.String()
}
