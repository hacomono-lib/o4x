package instrumentation

import (
	"context"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// PGPoolMetrics contains pgxpool statistics
type PGPoolMetrics struct {
	Timestamp time.Time

	// Pool configuration
	MaxConns int32

	// Current state
	TotalConns        int32
	AcquiredConns     int32
	IdleConns         int32
	ConstructingConns int32

	// Acquire statistics
	AcquireCount         int64
	AcquireDuration      time.Duration
	AcquiredConnsCount   int64
	CanceledAcquireCount int64
	EmptyAcquireCount    int64

	// Calculated metrics
	ConnectionUtilization float64 // AcquiredConns / MaxConns
	AvgAcquireDuration    time.Duration
}

// PGActivityMetrics contains pg_stat_activity statistics
type PGActivityMetrics struct {
	Timestamp time.Time

	// By state
	Active    int
	Idle      int
	IdleInTxn int
	Waiting   int

	// By wait_event_type
	WaitLock   int
	WaitIO     int
	WaitClient int
	WaitOther  int

	// Total connections
	TotalConns int
}

// PGMetricsCollector collects PostgreSQL connection and activity metrics
type PGMetricsCollector struct {
	pool   *pgxpool.Pool
	logger *slog.Logger

	// Atomic counters for aggregation
	totalSnapshots atomic.Int64
	totalWaiting   atomic.Int64
	totalActive    atomic.Int64
}

// NewPGMetricsCollector creates a new PostgreSQL metrics collector
func NewPGMetricsCollector(pool *pgxpool.Pool, logger *slog.Logger) *PGMetricsCollector {
	return &PGMetricsCollector{
		pool:   pool,
		logger: logger,
	}
}

// CollectPoolMetrics collects pgxpool statistics
func (c *PGMetricsCollector) CollectPoolMetrics() PGPoolMetrics {
	stats := c.pool.Stat()

	metrics := PGPoolMetrics{
		Timestamp:            time.Now(),
		MaxConns:             stats.MaxConns(),
		TotalConns:           stats.TotalConns(),
		AcquiredConns:        stats.AcquiredConns(),
		IdleConns:            stats.IdleConns(),
		ConstructingConns:    stats.ConstructingConns(),
		AcquireCount:         stats.AcquireCount(),
		AcquireDuration:      stats.AcquireDuration(),
		AcquiredConnsCount:   int64(stats.AcquiredConns()),
		CanceledAcquireCount: stats.CanceledAcquireCount(),
		EmptyAcquireCount:    stats.EmptyAcquireCount(),
	}

	// Calculate derived metrics
	if metrics.MaxConns > 0 {
		metrics.ConnectionUtilization = float64(metrics.AcquiredConns) / float64(metrics.MaxConns) * 100
	}

	if metrics.AcquireCount > 0 {
		metrics.AvgAcquireDuration = time.Duration(int64(metrics.AcquireDuration) / metrics.AcquireCount)
	}

	return metrics
}

// CollectActivityMetrics collects pg_stat_activity statistics
func (c *PGMetricsCollector) CollectActivityMetrics(ctx context.Context) (PGActivityMetrics, error) {
	query := `
		SELECT
			state,
			wait_event_type,
			COUNT(*) as count
		FROM pg_stat_activity
		WHERE pid != pg_backend_pid()
		  AND datname = current_database()
		GROUP BY state, wait_event_type
	`

	rows, err := c.pool.Query(ctx, query)
	if err != nil {
		return PGActivityMetrics{}, err
	}
	defer rows.Close()

	metrics := PGActivityMetrics{
		Timestamp: time.Now(),
	}

	for rows.Next() {
		var state, waitEventType *string
		var count int

		err := rows.Scan(&state, &waitEventType, &count)
		if err != nil {
			return metrics, err
		}

		// Count by state
		if state != nil {
			switch *state {
			case "active":
				metrics.Active += count
			case "idle":
				metrics.Idle += count
			case "idle in transaction":
				metrics.IdleInTxn += count
			}
		}

		// Count by wait_event_type
		if waitEventType != nil {
			metrics.Waiting += count
			switch *waitEventType {
			case "Lock":
				metrics.WaitLock += count
			case "IO":
				metrics.WaitIO += count
			case "Client":
				metrics.WaitClient += count
			default:
				metrics.WaitOther += count
			}
		}

		metrics.TotalConns += count
	}

	// Update atomic counters for aggregation
	c.totalSnapshots.Add(1)
	c.totalWaiting.Add(int64(metrics.Waiting))
	c.totalActive.Add(int64(metrics.Active))

	return metrics, rows.Err()
}

// LogPoolMetrics logs pool metrics in structured format
func (c *PGMetricsCollector) LogPoolMetrics(metrics PGPoolMetrics) {
	c.logger.Info("pgxpool_metrics",
		"max_conns", metrics.MaxConns,
		"total_conns", metrics.TotalConns,
		"acquired_conns", metrics.AcquiredConns,
		"idle_conns", metrics.IdleConns,
		"constructing_conns", metrics.ConstructingConns,
		"connection_utilization_pct", metrics.ConnectionUtilization,
		"acquire_count", metrics.AcquireCount,
		"acquire_duration_ms", metrics.AcquireDuration.Milliseconds(),
		"avg_acquire_duration_us", metrics.AvgAcquireDuration.Microseconds(),
		"canceled_acquire_count", metrics.CanceledAcquireCount,
		"empty_acquire_count", metrics.EmptyAcquireCount,
	)
}

// LogActivityMetrics logs pg_stat_activity metrics in structured format
func (c *PGMetricsCollector) LogActivityMetrics(metrics PGActivityMetrics) {
	c.logger.Info("pg_stat_activity",
		"total_conns", metrics.TotalConns,
		"active", metrics.Active,
		"idle", metrics.Idle,
		"idle_in_txn", metrics.IdleInTxn,
		"waiting", metrics.Waiting,
		"wait_lock", metrics.WaitLock,
		"wait_io", metrics.WaitIO,
		"wait_client", metrics.WaitClient,
		"wait_other", metrics.WaitOther,
	)
}

// LogAggregateStats logs aggregate statistics
func (c *PGMetricsCollector) LogAggregateStats() {
	snapshots := c.totalSnapshots.Load()
	if snapshots == 0 {
		return
	}

	avgWaiting := float64(c.totalWaiting.Load()) / float64(snapshots)
	avgActive := float64(c.totalActive.Load()) / float64(snapshots)

	c.logger.Info("pg_aggregate_stats",
		"total_snapshots", snapshots,
		"avg_waiting", avgWaiting,
		"avg_active", avgActive,
	)
}

// StartPeriodicCollection starts periodic metrics collection
func (c *PGMetricsCollector) StartPeriodicCollection(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			// Collect and log pool metrics
			poolMetrics := c.CollectPoolMetrics()
			c.LogPoolMetrics(poolMetrics)

			// Collect and log activity metrics
			activityMetrics, err := c.CollectActivityMetrics(ctx)
			if err != nil {
				c.logger.Error("failed to collect pg_stat_activity", "error", err)
			} else {
				c.LogActivityMetrics(activityMetrics)
			}
		}
	}
}
