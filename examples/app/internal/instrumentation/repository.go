package instrumentation

import (
	"context"
	"time"

	"github.com/hacomono-lib/o4x/core"
)

// InstrumentedBatchRepository wraps a BatchOutboxRepository with detailed metrics collection
type InstrumentedBatchRepository struct {
	core.BatchOutboxRepository
	collector *MetricsCollector
	workerID  int
}

// NewInstrumentedBatchRepository creates an instrumented repository for a specific worker
func NewInstrumentedBatchRepository(repo core.BatchOutboxRepository, collector *MetricsCollector, workerID int) *InstrumentedBatchRepository {
	return &InstrumentedBatchRepository{
		BatchOutboxRepository: repo,
		collector:             collector,
		workerID:              workerID,
	}
}

// FetchLockAndMarkPublishing instruments the fetch operation with detailed metrics
func (r *InstrumentedBatchRepository) FetchLockAndMarkPublishing(ctx context.Context, limit int) ([]*core.Outbox, error) {
	startTime := time.Now()

	// Call original method
	results, err := r.BatchOutboxRepository.FetchLockAndMarkPublishing(ctx, limit)

	duration := time.Since(startTime)

	// Record metrics even if error occurred (duration is still useful)
	rowsReturned := 0
	if results != nil {
		rowsReturned = len(results)
	}

	metrics := FetchMetrics{
		StartTime:      startTime,
		Duration:       duration,
		RowsReturned:   rowsReturned,
		RequestedLimit: limit,
		WorkerID:       r.workerID,
	}

	r.collector.RecordFetch(metrics)

	return results, err
}

// RepositoryFactory creates instrumented repositories for each worker
type RepositoryFactory struct {
	baseRepo  core.BatchOutboxRepository
	collector *MetricsCollector
}

// NewRepositoryFactory creates a new repository factory
func NewRepositoryFactory(baseRepo core.BatchOutboxRepository, collector *MetricsCollector) *RepositoryFactory {
	return &RepositoryFactory{
		baseRepo:  baseRepo,
		collector: collector,
	}
}

// CreateForWorker creates an instrumented repository for a specific worker
func (rf *RepositoryFactory) CreateForWorker(workerID int) core.BatchOutboxRepository {
	return NewInstrumentedBatchRepository(rf.baseRepo, rf.collector, workerID)
}
