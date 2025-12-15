package instrumentation

import (
	"context"
	"log/slog"
	"time"

	"github.com/hacomono-lib/o4x/core"
)

// InstrumentedBatchPublisher wraps a BatchPublisher with metrics collection
type InstrumentedBatchPublisher struct {
	core.BatchPublisher
	collector *MetricsCollector
	logger    *slog.Logger
}

// NewInstrumentedBatchPublisher creates an instrumented batch publisher
func NewInstrumentedBatchPublisher(publisher core.BatchPublisher, collector *MetricsCollector, logger *slog.Logger) *InstrumentedBatchPublisher {
	return &InstrumentedBatchPublisher{
		BatchPublisher: publisher,
		collector:      collector,
		logger:         logger,
	}
}

// PublishBatch instruments the batch publish operation with detailed metrics
func (p *InstrumentedBatchPublisher) PublishBatch(ctx context.Context, msgs []*core.Outbox) []core.PublishResult {
	startTime := time.Now()

	// Call original method
	results := p.BatchPublisher.PublishBatch(ctx, msgs)

	duration := time.Since(startTime)

	// Count successes and failures, collect failure reasons
	successCount := 0
	failureCount := 0
	var failureReasons []string
	for i, result := range results {
		if result.Success {
			successCount++
		} else {
			failureCount++
			// Log WARN for each failure with message ID
			msg := msgs[i]
			errMsg := "unknown error"
			if result.Error != nil {
				errMsg = result.Error.Error()
			}
			failureReasons = append(failureReasons, errMsg)

			// WARN log for visibility (not for aggregation)
			p.logger.Warn("batch_publish_failure",
				"message_id", msg.ID,
				"event_type", msg.EventType,
				"error", errMsg,
				"batch_index", i,
			)
		}
	}

	// Record metrics
	metrics := PublishMetrics{
		StartTime:      startTime,
		Duration:       duration,
		BatchSize:      len(msgs),
		SuccessCount:   successCount,
		FailureCount:   failureCount,
		FailureReasons: failureReasons, // Reserved for future aggregation
	}

	p.collector.RecordPublish(metrics)

	return results
}
