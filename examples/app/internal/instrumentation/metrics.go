package instrumentation

import (
	"log/slog"
	"sync"
	"sync/atomic"
	"time"
)

// FetchMetrics contains detailed metrics for a single FetchLockAndMarkPublishing call
type FetchMetrics struct {
	StartTime      time.Time
	Duration       time.Duration // Total transaction duration (CTE execution)
	RowsReturned   int
	RequestedLimit int
	WorkerID       int
}

// TransactionMetrics contains transaction-level statistics
type TransactionMetrics struct {
	TotalTransactions atomic.Int64
	TotalDuration     atomic.Int64 // in nanoseconds
	MaxDuration       atomic.Int64 // in nanoseconds
	MinDuration       atomic.Int64 // in nanoseconds (initialized to max)
}

// PublishMetrics contains detailed metrics for a single batch publish operation
type PublishMetrics struct {
	StartTime      time.Time
	Duration       time.Duration
	SuccessCount   int
	FailureCount   int
	BatchSize      int
	FailureReasons []string // Reserved for future: error codes/messages from failed publishes
}

// MetricsCollector collects and aggregates dispatcher metrics
type MetricsCollector struct {
	// Fetch metrics
	totalFetches  atomic.Int64
	totalRows     atomic.Int64
	totalDuration atomic.Int64 // in nanoseconds
	emptyFetches  atomic.Int64

	// Transaction duration metrics (same as fetch duration, but tracked separately for clarity)
	minTxnDuration atomic.Int64 // in nanoseconds
	maxTxnDuration atomic.Int64 // in nanoseconds

	// Publish metrics
	totalPublishes       atomic.Int64
	totalPublishSuccess  atomic.Int64
	totalPublishFailure  atomic.Int64
	totalPublishDuration atomic.Int64 // in nanoseconds

	// Detailed metrics per worker
	mu          sync.Mutex
	workerStats map[int]*WorkerStats

	logger *slog.Logger
}

// WorkerStats contains per-worker statistics
type WorkerStats struct {
	WorkerID      int
	TotalFetches  int64
	TotalRows     int64
	EmptyFetches  int64
	TotalDuration time.Duration
}

// NewMetricsCollector creates a new metrics collector
func NewMetricsCollector(logger *slog.Logger) *MetricsCollector {
	return &MetricsCollector{
		workerStats: make(map[int]*WorkerStats),
		logger:      logger,
	}
}

// RecordFetch records a fetch operation
func (mc *MetricsCollector) RecordFetch(metrics FetchMetrics) {
	mc.totalFetches.Add(1)
	mc.totalRows.Add(int64(metrics.RowsReturned))
	mc.totalDuration.Add(int64(metrics.Duration))

	if metrics.RowsReturned == 0 {
		mc.emptyFetches.Add(1)
	}

	// Update transaction duration min/max
	durationNs := int64(metrics.Duration)

	// Update min (use compare-and-swap loop)
	for {
		currentMin := mc.minTxnDuration.Load()
		if currentMin == 0 || durationNs < currentMin {
			if mc.minTxnDuration.CompareAndSwap(currentMin, durationNs) {
				break
			}
		} else {
			break
		}
	}

	// Update max (use compare-and-swap loop)
	for {
		currentMax := mc.maxTxnDuration.Load()
		if durationNs > currentMax {
			if mc.maxTxnDuration.CompareAndSwap(currentMax, durationNs) {
				break
			}
		} else {
			break
		}
	}

	// Update per-worker stats
	mc.mu.Lock()
	stats, exists := mc.workerStats[metrics.WorkerID]
	if !exists {
		stats = &WorkerStats{WorkerID: metrics.WorkerID}
		mc.workerStats[metrics.WorkerID] = stats
	}
	stats.TotalFetches++
	stats.TotalRows += int64(metrics.RowsReturned)
	stats.TotalDuration += metrics.Duration
	if metrics.RowsReturned == 0 {
		stats.EmptyFetches++
	}
	mc.mu.Unlock()

	// Log detailed fetch metrics
	utilizationPct := float64(0)
	if metrics.RequestedLimit > 0 {
		utilizationPct = float64(metrics.RowsReturned) / float64(metrics.RequestedLimit) * 100
	}

	mc.logger.Debug("fetch_operation",
		"worker_id", metrics.WorkerID,
		"duration_ms", metrics.Duration.Milliseconds(),
		"duration_us", metrics.Duration.Microseconds(),
		"rows_returned", metrics.RowsReturned,
		"requested_limit", metrics.RequestedLimit,
		"utilization_pct", utilizationPct,
	)
}

// RecordPublish records a publish operation
func (mc *MetricsCollector) RecordPublish(metrics PublishMetrics) {
	mc.totalPublishes.Add(1)
	mc.totalPublishSuccess.Add(int64(metrics.SuccessCount))
	mc.totalPublishFailure.Add(int64(metrics.FailureCount))
	mc.totalPublishDuration.Add(int64(metrics.Duration))

	mc.logger.Debug("publish_operation",
		"duration_ms", metrics.Duration.Milliseconds(),
		"batch_size", metrics.BatchSize,
		"success_count", metrics.SuccessCount,
		"failure_count", metrics.FailureCount,
	)
}

// GetFetchStats returns current fetch statistics
func (mc *MetricsCollector) GetFetchStats() map[string]interface{} {
	totalFetches := mc.totalFetches.Load()
	totalRows := mc.totalRows.Load()
	totalDuration := mc.totalDuration.Load()
	emptyFetches := mc.emptyFetches.Load()
	minTxn := mc.minTxnDuration.Load()
	maxTxn := mc.maxTxnDuration.Load()

	avgDurationMs := float64(0)
	avgDurationUs := float64(0)
	avgRowsPerFetch := float64(0)
	emptyFetchPct := float64(0)

	if totalFetches > 0 {
		avgDurationMs = float64(totalDuration) / float64(totalFetches) / 1e6
		avgDurationUs = float64(totalDuration) / float64(totalFetches) / 1e3
		avgRowsPerFetch = float64(totalRows) / float64(totalFetches)
		emptyFetchPct = float64(emptyFetches) / float64(totalFetches) * 100
	}

	return map[string]interface{}{
		"total_fetches":       totalFetches,
		"total_rows":          totalRows,
		"empty_fetches":       emptyFetches,
		"empty_fetch_pct":     emptyFetchPct,
		"avg_duration_ms":     avgDurationMs,
		"avg_duration_us":     avgDurationUs,
		"avg_rows_per_fetch":  avgRowsPerFetch,
		"min_txn_duration_us": minTxn / 1000,
		"max_txn_duration_us": maxTxn / 1000,
		"min_txn_duration_ms": minTxn / 1000000,
		"max_txn_duration_ms": maxTxn / 1000000,
	}
}

// GetPublishStats returns current publish statistics
func (mc *MetricsCollector) GetPublishStats() map[string]interface{} {
	totalPublishes := mc.totalPublishes.Load()
	totalSuccess := mc.totalPublishSuccess.Load()
	totalFailure := mc.totalPublishFailure.Load()
	totalDuration := mc.totalPublishDuration.Load()

	avgDurationMs := float64(0)
	avgSuccessPerPublish := float64(0)
	if totalPublishes > 0 {
		avgDurationMs = float64(totalDuration) / float64(totalPublishes) / 1e6
		avgSuccessPerPublish = float64(totalSuccess) / float64(totalPublishes)
	}

	return map[string]interface{}{
		"total_publishes":         totalPublishes,
		"total_success":           totalSuccess,
		"total_failure":           totalFailure,
		"avg_duration_ms":         avgDurationMs,
		"avg_success_per_publish": avgSuccessPerPublish,
	}
}

// GetWorkerStats returns per-worker statistics
func (mc *MetricsCollector) GetWorkerStats() []WorkerStats {
	mc.mu.Lock()
	defer mc.mu.Unlock()

	stats := make([]WorkerStats, 0, len(mc.workerStats))
	for _, ws := range mc.workerStats {
		stats = append(stats, *ws)
	}
	return stats
}

// LogStats logs comprehensive statistics
func (mc *MetricsCollector) LogStats() {
	fetchStats := mc.GetFetchStats()
	publishStats := mc.GetPublishStats()
	workerStats := mc.GetWorkerStats()

	mc.logger.Info("=== DISPATCHER METRICS SUMMARY ===")
	mc.logger.Info("fetch_stats",
		"total_fetches", fetchStats["total_fetches"],
		"total_rows", fetchStats["total_rows"],
		"empty_fetches", fetchStats["empty_fetches"],
		"empty_fetch_pct", fetchStats["empty_fetch_pct"],
		"avg_duration_ms", fetchStats["avg_duration_ms"],
		"avg_duration_us", fetchStats["avg_duration_us"],
		"avg_rows_per_fetch", fetchStats["avg_rows_per_fetch"],
	)

	mc.logger.Info("transaction_duration_stats",
		"min_txn_duration_us", fetchStats["min_txn_duration_us"],
		"max_txn_duration_us", fetchStats["max_txn_duration_us"],
		"min_txn_duration_ms", fetchStats["min_txn_duration_ms"],
		"max_txn_duration_ms", fetchStats["max_txn_duration_ms"],
		"avg_txn_duration_us", fetchStats["avg_duration_us"],
		"avg_txn_duration_ms", fetchStats["avg_duration_ms"],
	)

	mc.logger.Info("publish_stats",
		"total_publishes", publishStats["total_publishes"],
		"total_success", publishStats["total_success"],
		"total_failure", publishStats["total_failure"],
		"avg_duration_ms", publishStats["avg_duration_ms"],
		"avg_success_per_publish", publishStats["avg_success_per_publish"],
	)

	for _, ws := range workerStats {
		avgDurationMs := float64(0)
		avgRows := float64(0)
		if ws.TotalFetches > 0 {
			avgDurationMs = float64(ws.TotalDuration.Milliseconds()) / float64(ws.TotalFetches)
			avgRows = float64(ws.TotalRows) / float64(ws.TotalFetches)
		}

		mc.logger.Info("worker_stats",
			"worker_id", ws.WorkerID,
			"total_fetches", ws.TotalFetches,
			"total_rows", ws.TotalRows,
			"empty_fetches", ws.EmptyFetches,
			"avg_duration_ms", avgDurationMs,
			"avg_rows", avgRows,
		)
	}
}
