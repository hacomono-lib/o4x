package core

import "time"

// Clock provides time-related operations for testability
type Clock interface {
	Now() time.Time
}

// RealClock implements Clock using the system time
type RealClock struct{}

// Now returns the current time
func (RealClock) Now() time.Time {
	return time.Now()
}

// MockClock is a Clock implementation for testing
type MockClock struct {
	CurrentTime time.Time
}

// Now returns the mocked current time
func (m *MockClock) Now() time.Time {
	return m.CurrentTime
}

// Advance moves the mock clock forward by the given duration
func (m *MockClock) Advance(d time.Duration) {
	m.CurrentTime = m.CurrentTime.Add(d)
}
