package consumer

import (
	"math/rand"
	"time"
)

// SleepConfig holds min/max sleep durations for simulating processing time
type SleepConfig struct {
	Min time.Duration
	Max time.Duration
}

// Sleep sleeps for a random duration between min and max
func (c SleepConfig) Sleep() {
	if c.Min == 0 && c.Max == 0 {
		return // No sleep if both are zero
	}

	if c.Min >= c.Max {
		time.Sleep(c.Min)
		return
	}

	// Random sleep between min and max
	delta := c.Max - c.Min
	sleep := c.Min + time.Duration(rand.Int63n(int64(delta)))
	time.Sleep(sleep)
}

// ParseSleepDuration parses a duration string with fallback to default
func ParseSleepDuration(s string, defaultDuration time.Duration) time.Duration {
	if s == "" {
		return defaultDuration
	}
	d, err := time.ParseDuration(s)
	if err != nil {
		return defaultDuration
	}
	return d
}
