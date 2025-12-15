package core

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// TestClocks tests the real and mock clock implementations
func TestClocks(t *testing.T) {
	// Test RealClock
	realClock := &RealClock{}
	now1 := realClock.Now()
	time.Sleep(10 * time.Millisecond)
	now2 := realClock.Now()
	assert.True(t, now2.After(now1))

	// Test MockClock
	fixedTime := time.Now()
	mockClock := &MockClock{CurrentTime: fixedTime}
	assert.Equal(t, fixedTime, mockClock.Now())

	// Advance time
	mockClock.Advance(1 * time.Hour)
	expectedTime := fixedTime.Add(1 * time.Hour)
	assert.Equal(t, expectedTime, mockClock.Now())

	// Advance again
	mockClock.Advance(30 * time.Minute)
	expectedTime = expectedTime.Add(30 * time.Minute)
	assert.Equal(t, expectedTime, mockClock.Now())
}
