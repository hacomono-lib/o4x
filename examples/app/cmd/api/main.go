package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/hacomono-lib/o4x/contrib/pgx"
	"github.com/hacomono-lib/o4x/examples/app/internal/handler"
	"github.com/hacomono-lib/o4x/examples/app/internal/repository"
	"github.com/hacomono-lib/o4x/examples/app/internal/service"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))
	slog.SetDefault(logger)

	ctx := context.Background()

	// Configuration from environment
	dbURL := getEnv("DATABASE_URL", "postgres://postgres:postgres@localhost:15432/o4x?sslmode=disable")
	port := getEnv("PORT", "8000")

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

	// Initialize repositories
	orderRepo := repository.NewOrderRepository(pool)
	userRepo := repository.NewUserRepository(pool)
	inventoryRepo := repository.NewInventoryRepository(pool)
	notificationRepo := repository.NewNotificationRepository(pool)
	outboxRepo := pgx.NewOutboxRepository(pool)

	// Initialize services
	orderService := service.NewOrderService(pool, orderRepo, inventoryRepo, outboxRepo)
	userService := service.NewUserService(pool, userRepo, outboxRepo)
	notificationService := service.NewNotificationService(pool, notificationRepo, outboxRepo)

	// Initialize handlers
	orderHandler := handler.NewOrderHandler(orderService, logger)
	userHandler := handler.NewUserHandler(userService, logger)
	notificationHandler := handler.NewNotificationHandler(notificationService, logger)

	// Setup HTTP routes
	mux := http.NewServeMux()

	// Order endpoints
	mux.HandleFunc("/api/orders", orderHandler.CreateOrder)
	mux.HandleFunc("/api/orders/confirm", orderHandler.ConfirmOrder)

	// User endpoints
	mux.HandleFunc("/api/users", userHandler.RegisterUser)
	mux.HandleFunc("/api/users/update", userHandler.UpdateUser)

	// Notification endpoints
	mux.HandleFunc("/api/notifications", notificationHandler.SendNotification)

	// Health check endpoint
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		if err := pool.Ping(r.Context()); err != nil {
			w.WriteHeader(http.StatusServiceUnavailable)
			w.Write([]byte("Unhealthy: database unreachable\n"))
			return
		}
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("OK\n"))
	})

	// Start HTTP server
	server := &http.Server{
		Addr:    ":" + port,
		Handler: mux,
	}

	go func() {
		logger.Info("API server starting", "port", port)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Error("API server failed", "error", err)
		}
	}()

	// Wait for shutdown signal
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	logger.Info("shutdown signal received")

	// Graceful shutdown
	shutdownCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
	defer cancel()
	if err := server.Shutdown(shutdownCtx); err != nil {
		logger.Error("API server shutdown failed", "error", err)
	}

	logger.Info("API server shutdown complete")
}

func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}
