package core

import (
	"context"
	"log/slog"
	"os"
	"sync"
	"time"
)

// DispatcherConfig holds configuration for the Dispatcher
type DispatcherConfig struct {
	// PollInterval is the base interval for polling when messages are found.
	// When no messages are found, the interval increases exponentially up to MaxPollInterval.
	PollInterval time.Duration
	// MaxPollInterval is the maximum polling interval during idle periods.
	// When no messages are found, the interval doubles until reaching this limit.
	// Defaults to PollInterval * 32 (about 3.2 seconds with default 100ms).
	MaxPollInterval time.Duration
	// WorkerCount is the number of concurrent workers
	WorkerCount int
	// ShutdownTimeout is the time to wait for graceful shutdown before warning
	ShutdownTimeout time.Duration
	// ForceTimeout is the hard limit after which the process exits forcefully
	// Must be greater than ShutdownTimeout. If zero, defaults to ShutdownTimeout * 2
	ForceTimeout time.Duration
	// OnForceShutdown is called when the force timeout is exceeded.
	// If nil, defaults to os.Exit(1). Set to a custom function for graceful handling
	// or set to an empty function to disable forced shutdown.
	OnForceShutdown func()
	// AutoRecover enables automatic recovery of stuck PUBLISHING messages at startup.
	// When true, ReviveStuckPublishing is called if the repository implements OutboxRecovery.
	// Defaults to true.
	AutoRecover bool
	// RequeueInterval is how often to run RequeueFailed.
	// IMPORTANT: If set to 0, FAILED messages will NEVER be retried automatically.
	// Recommended: 10s for normal workloads, 1s for high-priority messages.
	RequeueInterval time.Duration
	// Logger for dispatcher operations
	Logger *slog.Logger
	// Hooks for observability and metrics collection (optional)
	Hooks *Hooks
	// CleanupTimeout is the timeout for database cleanup operations during shutdown.
	// This ensures DB updates (UpdateToPublished, UpdateToFailed) complete even when
	// the parent context is cancelled. Defaults to 10 seconds.
	CleanupTimeout time.Duration
}

// DefaultDispatcherConfig returns sensible defaults
func DefaultDispatcherConfig() DispatcherConfig {
	return DispatcherConfig{
		PollInterval:    100 * time.Millisecond,
		MaxPollInterval: 3200 * time.Millisecond, // 100ms * 32
		WorkerCount:     1,
		ShutdownTimeout: 30 * time.Second,
		ForceTimeout:    60 * time.Second,
		OnForceShutdown: func() { os.Exit(1) },
		AutoRecover:     true,
		RequeueInterval: 10 * time.Second,
		CleanupTimeout:  10 * time.Second,
		Logger:          slog.Default(),
	}
}

// Dispatcher orchestrates the outbox message processing
// It polls for ENQUEUED messages and dispatches them to workers
type Dispatcher struct {
	repo            OutboxRepository
	publisher       Publisher
	config          DispatcherConfig
	workers         []*Worker
	cancelFunc      context.CancelFunc
	wg              sync.WaitGroup
	mu              sync.Mutex
	running         bool
	pendingShutdown bool
	lastProcessedAt *time.Time
}

// NewDispatcher creates a new Dispatcher with configuration validation
// Returns an error if the configuration is invalid.
func NewDispatcher(repo OutboxRepository, publisher Publisher, config DispatcherConfig) (*Dispatcher, error) {
	// Validate negative values
	if config.PollInterval < 0 {
		return nil, ErrInvalidConfig
	}
	if config.MaxPollInterval < 0 {
		return nil, ErrInvalidConfig
	}
	if config.WorkerCount < 0 {
		return nil, ErrInvalidConfig
	}
	if config.ShutdownTimeout < 0 {
		return nil, ErrInvalidConfig
	}
	if config.ForceTimeout < 0 {
		return nil, ErrInvalidConfig
	}
	if config.CleanupTimeout < 0 {
		return nil, ErrInvalidConfig
	}

	// Apply defaults
	if config.PollInterval == 0 {
		config.PollInterval = 100 * time.Millisecond
	}
	if config.MaxPollInterval == 0 {
		config.MaxPollInterval = config.PollInterval * 32
	}
	if config.WorkerCount == 0 {
		config.WorkerCount = 1
	}
	if config.ShutdownTimeout == 0 {
		config.ShutdownTimeout = 30 * time.Second
	}
	if config.ForceTimeout == 0 {
		config.ForceTimeout = config.ShutdownTimeout * 2
	}
	if config.Logger == nil {
		config.Logger = slog.Default()
	}
	if config.OnForceShutdown == nil {
		config.OnForceShutdown = func() { os.Exit(1) }
	}
	if config.CleanupTimeout == 0 {
		config.CleanupTimeout = 10 * time.Second
	}

	// Validate timeout consistency
	if config.ForceTimeout < config.ShutdownTimeout {
		config.Logger.Warn("ForceTimeout is less than ShutdownTimeout, adjusting ForceTimeout",
			"shutdown_timeout", config.ShutdownTimeout,
			"force_timeout", config.ForceTimeout,
			"adjusted_force_timeout", config.ShutdownTimeout*2)
		config.ForceTimeout = config.ShutdownTimeout * 2
	}

	return &Dispatcher{
		repo:      repo,
		publisher: publisher,
		config:    config,
	}, nil
}

// Start begins the dispatcher and its workers
func (d *Dispatcher) Start(ctx context.Context) error {
	d.mu.Lock()
	if d.running {
		d.mu.Unlock()
		return ErrAlreadyRunning
	}
	d.running = true

	// Create cancellable context for workers
	workerCtx, cancel := context.WithCancel(ctx)
	d.cancelFunc = cancel

	d.mu.Unlock()

	// Auto-recover stuck messages if enabled and repository supports it
	// Run asynchronously to prevent blocking dispatcher startup
	if d.config.AutoRecover {
		if recovery, ok := d.repo.(OutboxRecovery); ok {
			go func() {
				count, err := recovery.ReviveStuckPublishing(ctx)
				if err != nil {
					d.config.Logger.ErrorContext(ctx, "failed to recover stuck messages at startup", "error", err)
				} else if count > 0 {
					d.config.Logger.InfoContext(ctx, "recovered stuck messages at startup", "count", count)
				}
			}()
		} else {
			d.config.Logger.WarnContext(ctx, "AutoRecover is enabled but repository does not implement OutboxRecovery. "+
				"Consider calling ReviveStuckPublishing manually at startup to prevent message loss.")
		}
	}

	// Warn if RequeueInterval is 0 (FAILED messages will never retry automatically)
	if d.config.RequeueInterval == 0 {
		d.config.Logger.WarnContext(ctx, "RequeueInterval is 0 - FAILED messages will not be retried automatically. "+
			"Set RequeueInterval to enable automatic retries.")
	}

	d.config.Logger.InfoContext(ctx, "starting dispatcher",
		"worker_count", d.config.WorkerCount,
		"poll_interval", d.config.PollInterval,
		"requeue_interval", d.config.RequeueInterval,
	)

	// Start workers
	for i := 0; i < d.config.WorkerCount; i++ {
		worker := NewWorker(i, d.repo, d.publisher, d.config.Logger, d.config.Hooks,
			d.config.PollInterval, d.config.MaxPollInterval, d.config.CleanupTimeout)
		// Set callback to update lastProcessedAt for health checks
		worker.onMessageProcessed = func() {
			now := time.Now()
			d.mu.Lock()
			d.lastProcessedAt = &now
			d.mu.Unlock()
		}
		d.workers = append(d.workers, worker)

		d.wg.Add(1)
		go func(w *Worker) {
			defer d.wg.Done()
			w.Run(workerCtx)
		}(worker)
	}

	// Start requeue worker if enabled
	if d.config.RequeueInterval > 0 {
		d.wg.Add(1)
		go func() {
			defer d.wg.Done()
			d.runRequeueWorker(workerCtx)
		}()
	}

	return nil
}

// Stop gracefully shuts down the dispatcher
// It waits up to ShutdownTimeout for graceful completion,
// then up to ForceTimeout before forcefully exiting.
func (d *Dispatcher) Stop() {
	d.mu.Lock()
	if !d.running {
		d.mu.Unlock()
		return
	}
	d.pendingShutdown = true
	d.running = false
	cancelFunc := d.cancelFunc
	d.mu.Unlock()

	// Cancel context to signal workers to stop
	if cancelFunc != nil {
		cancelFunc()
	}

	GracefulShutdown("dispatcher", &d.wg, ShutdownConfig{
		ShutdownTimeout: d.config.ShutdownTimeout,
		ForceTimeout:    d.config.ForceTimeout,
		OnForceShutdown: d.config.OnForceShutdown,
		Logger:          d.config.Logger,
	})

	d.workers = nil
}

// IsRunning returns whether the dispatcher is currently running
func (d *Dispatcher) IsRunning() bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.running
}

// runRequeueWorker periodically moves FAILED messages back to ENQUEUED
func (d *Dispatcher) runRequeueWorker(ctx context.Context) {
	logger := d.config.Logger.With("worker_type", "requeue")
	logger.InfoContext(ctx, "requeue worker started",
		"interval", d.config.RequeueInterval,
	)

	ticker := time.NewTicker(d.config.RequeueInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			logger.InfoContext(ctx, "requeue worker stopped")
			return
		case <-ticker.C:
			count, err := d.repo.RequeueFailed(ctx)
			if err != nil {
				logger.ErrorContext(ctx, "failed to requeue failed messages", "error", err)
			} else if count > 0 {
				logger.InfoContext(ctx, "requeued failed messages", "count", count)
			} else {
				logger.DebugContext(ctx, "requeue completed, no messages eligible")
			}
		}
	}
}

// HealthStatus returns the current health status of the dispatcher.
// Use this to implement health check endpoints for containerized deployments (ECS, Kubernetes, etc.).
//
// Example usage:
//
//	http.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
//	    status := dispatcher.HealthStatus()
//	    if !status.IsHealthy() {
//	        w.WriteHeader(http.StatusServiceUnavailable)
//	        return
//	    }
//	    // Optional: Check for stale processing
//	    if status.IsStale(5 * time.Minute) {
//	        w.WriteHeader(http.StatusServiceUnavailable)
//	        return
//	    }
//	    w.WriteHeader(http.StatusOK)
//	})
func (d *Dispatcher) HealthStatus() HealthStatus {
	d.mu.Lock()
	defer d.mu.Unlock()
	return HealthStatus{
		Running:         d.running,
		LastProcessedAt: d.lastProcessedAt,
		WorkerCount:     d.config.WorkerCount,
		PendingShutdown: d.pendingShutdown,
	}
}
