package core

import (
	"context"
	"log/slog"
	"sync"
	"time"
)

// ShutdownConfig holds common shutdown configuration
type ShutdownConfig struct {
	// ShutdownTimeout is the time to wait for graceful shutdown before warning
	ShutdownTimeout time.Duration
	// ForceTimeout is the hard limit after which the process exits forcefully
	ForceTimeout time.Duration
	// OnForceShutdown is called when the force timeout is exceeded
	OnForceShutdown func()
	// Logger for shutdown operations
	Logger *slog.Logger
}

// GracefulShutdown performs a graceful shutdown with timeout handling.
// It waits for the WaitGroup to complete within the configured timeouts.
// If ShutdownTimeout is exceeded, it logs a warning and waits until ForceTimeout.
// If ForceTimeout is exceeded, it calls OnForceShutdown.
//
// Parameters:
//   - name: the name of the component being shut down (for logging)
//   - wg: the WaitGroup to wait on
//   - config: shutdown configuration
func GracefulShutdown(name string, wg *sync.WaitGroup, config ShutdownConfig) {
	config.Logger.Info("stopping "+name,
		"shutdown_timeout", config.ShutdownTimeout,
		"force_timeout", config.ForceTimeout,
	)

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		config.Logger.Info(name + " stopped gracefully")
		return
	case <-time.After(config.ShutdownTimeout):
		config.Logger.Warn(name+" graceful shutdown timed out, waiting for force timeout",
			"shutdown_timeout", config.ShutdownTimeout,
			"force_timeout", config.ForceTimeout,
		)
	}

	// Wait until ForceTimeout
	remainingTime := config.ForceTimeout - config.ShutdownTimeout
	select {
	case <-done:
		config.Logger.Info(name + " stopped after extended wait")
		return
	case <-time.After(remainingTime):
		config.Logger.Error(name+" force shutdown - workers did not stop in time",
			"force_timeout", config.ForceTimeout,
		)
		if config.OnForceShutdown != nil {
			config.OnForceShutdown()
		}
	}
}

// createCleanupContext creates a context for cleanup operations that need to complete
// even when the parent context is cancelled (e.g., during shutdown).
//
// This function removes cancellation from the parent context while preserving values
// (such as trace IDs, logger context) and deadlines. A new timeout is added for the
// cleanup operation.
//
// This is critical for preventing messages from being stuck in PUBLISHING state when
// the dispatcher is shutting down. Without this, UpdateToPublished/UpdateToFailed calls
// would fail immediately if the parent context is already cancelled, leaving messages
// in an inconsistent state.
//
// Example:
//
//	cleanupCtx, cancel := createCleanupContext(ctx, 10*time.Second)
//	defer cancel()
//	repo.UpdateToPublished(cleanupCtx, msg.ID)
func createCleanupContext(parent context.Context, timeout time.Duration) (context.Context, context.CancelFunc) {
	// Remove cancellation from parent while preserving values and deadlines
	ctx := context.WithoutCancel(parent)

	// Add timeout for cleanup operation
	return context.WithTimeout(ctx, timeout)
}
