package core

import "time"

// HealthStatus represents the current health status of a Dispatcher or Consumer.
// This is designed for implementing health check endpoints in containerized environments
// like ECS, Kubernetes, etc.
type HealthStatus struct {
	// Running indicates whether the component is currently running
	Running bool

	// LastProcessedAt is the timestamp of the last successful message processing.
	// Useful for liveness checks - if this timestamp is too old, the component may be stuck.
	// Will be nil if no messages have been processed yet.
	LastProcessedAt *time.Time

	// WorkerCount is the number of active workers
	WorkerCount int

	// PendingShutdown indicates whether a shutdown has been initiated but not completed
	PendingShutdown bool
}

// IsHealthy returns true if the component is running and not pending shutdown.
// This is a simple helper for basic health checks.
func (h HealthStatus) IsHealthy() bool {
	return h.Running && !h.PendingShutdown
}

// IsStale returns true if the component hasn't processed messages for the given duration.
// Returns false if LastProcessedAt is nil (no messages processed yet).
// Useful for detecting stuck workers.
func (h HealthStatus) IsStale(maxAge time.Duration) bool {
	if h.LastProcessedAt == nil {
		return false // Not stale if never processed
	}
	return time.Since(*h.LastProcessedAt) > maxAge
}
