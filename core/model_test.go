package core

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// TestOutbox_CanRetry tests the CanRetry method
func TestOutbox_CanRetry(t *testing.T) {
	tests := []struct {
		name        string
		retryCount  int
		maxRetries  int
		expected    bool
	}{
		{
			name:       "can retry when under max",
			retryCount: 2,
			maxRetries: 5,
			expected:   true,
		},
		{
			name:       "cannot retry when at max",
			retryCount: 5,
			maxRetries: 5,
			expected:   false,
		},
		{
			name:       "cannot retry when over max",
			retryCount: 6,
			maxRetries: 5,
			expected:   false,
		},
		{
			name:       "can retry when zero retries",
			retryCount: 0,
			maxRetries: 3,
			expected:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			outbox := &Outbox{
				RetryCount: tt.retryCount,
				MaxRetries: tt.maxRetries,
			}
			assert.Equal(t, tt.expected, outbox.CanRetry())
		})
	}
}

// TestOutbox_ShouldMarkDead tests the ShouldMarkDead method
func TestOutbox_ShouldMarkDead(t *testing.T) {
	tests := []struct {
		name       string
		retryCount int
		maxRetries int
		expected   bool
	}{
		{
			name:       "should mark dead when at max retries",
			retryCount: 5,
			maxRetries: 5,
			expected:   true,
		},
		{
			name:       "should mark dead when over max retries",
			retryCount: 6,
			maxRetries: 5,
			expected:   true,
		},
		{
			name:       "should not mark dead when under max retries",
			retryCount: 3,
			maxRetries: 5,
			expected:   false,
		},
		{
			name:       "should not mark dead when zero retries",
			retryCount: 0,
			maxRetries: 3,
			expected:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			outbox := &Outbox{
				RetryCount: tt.retryCount,
				MaxRetries: tt.maxRetries,
			}
			assert.Equal(t, tt.expected, outbox.ShouldMarkDead())
		})
	}
}

// TestCalculateNextRetryAt tests the standalone CalculateNextRetryAt function
func TestCalculateNextRetryAt(t *testing.T) {
	baseInterval := 1 * time.Second
	maxInterval := 1 * time.Minute

	tests := []struct {
		name        string
		retryCount  int
		minDuration time.Duration
		maxDuration time.Duration
	}{
		{
			name:        "first retry",
			retryCount:  0,
			minDuration: 500 * time.Millisecond,  // baseInterval * 0.5
			maxDuration: 2 * time.Second,         // baseInterval * 2 (with jitter)
		},
		{
			name:        "second retry",
			retryCount:  1,
			minDuration: 1 * time.Second,         // baseInterval * 2 * 0.5
			maxDuration: 4 * time.Second,         // baseInterval * 4 (with jitter)
		},
		{
			name:        "many retries should cap at maxInterval",
			retryCount:  10,
			minDuration: 30 * time.Second,        // maxInterval * 0.5
			maxDuration: 2 * time.Minute,         // maxInterval * 2 (with jitter)
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			now := time.Now()
			nextRetry := CalculateNextRetryAt(now, tt.retryCount, baseInterval, maxInterval, 0.5)

			duration := nextRetry.Sub(now)
			assert.GreaterOrEqual(t, duration, tt.minDuration,
				"duration should be at least %v, got %v", tt.minDuration, duration)
			assert.LessOrEqual(t, duration, tt.maxDuration,
				"duration should be at most %v, got %v", tt.maxDuration, duration)
		})
	}
}
