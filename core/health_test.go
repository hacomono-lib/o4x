package core

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestHealthStatus_IsHealthy(t *testing.T) {
	tests := []struct {
		name     string
		status   HealthStatus
		expected bool
	}{
		{
			name: "healthy: running and not pending shutdown",
			status: HealthStatus{
				Running:         true,
				PendingShutdown: false,
			},
			expected: true,
		},
		{
			name: "unhealthy: not running",
			status: HealthStatus{
				Running:         false,
				PendingShutdown: false,
			},
			expected: false,
		},
		{
			name: "unhealthy: pending shutdown",
			status: HealthStatus{
				Running:         true,
				PendingShutdown: true,
			},
			expected: false,
		},
		{
			name: "unhealthy: not running and pending shutdown",
			status: HealthStatus{
				Running:         false,
				PendingShutdown: true,
			},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.status.IsHealthy()
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestHealthStatus_IsStale(t *testing.T) {
	now := time.Now()
	fiveMinutesAgo := now.Add(-5 * time.Minute)
	oneHourAgo := now.Add(-1 * time.Hour)

	tests := []struct {
		name     string
		status   HealthStatus
		maxAge   time.Duration
		expected bool
	}{
		{
			name: "not stale: nil LastProcessedAt",
			status: HealthStatus{
				LastProcessedAt: nil,
			},
			maxAge:   10 * time.Minute,
			expected: false,
		},
		{
			name: "not stale: recent processing within maxAge",
			status: HealthStatus{
				LastProcessedAt: &fiveMinutesAgo,
			},
			maxAge:   10 * time.Minute,
			expected: false,
		},
		{
			name: "stale: processing older than maxAge",
			status: HealthStatus{
				LastProcessedAt: &oneHourAgo,
			},
			maxAge:   10 * time.Minute,
			expected: true,
		},
		{
			name: "edge case: just under maxAge boundary",
			status: HealthStatus{
				LastProcessedAt: func() *time.Time {
					t := now.Add(-9 * time.Minute)
					return &t
				}(),
			},
			maxAge:   10 * time.Minute,
			expected: false, // Just under the boundary, should not be stale
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.status.IsStale(tt.maxAge)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestHealthStatus_IsStale_EdgeCases(t *testing.T) {
	// Test zero duration
	now := time.Now()
	status := HealthStatus{
		LastProcessedAt: &now,
	}

	// Any non-zero time since now will be > 0, so it should be stale
	time.Sleep(1 * time.Millisecond) // ensure some time passes
	result := status.IsStale(0)
	assert.True(t, result, "should be stale with 0 maxAge")

	// Test very old timestamp
	veryOld := now.Add(-24 * 365 * time.Hour) // 1 year ago
	status.LastProcessedAt = &veryOld
	result = status.IsStale(1 * time.Hour)
	assert.True(t, result, "should be stale for very old timestamp")
}
