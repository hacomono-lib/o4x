package core

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// TestNopInboxRepository tests the no-op inbox repository
func TestNopInboxRepository(t *testing.T) {
	repo := NewNopInboxRepository()

	ctx := context.Background()

	// Test TryStart
	ok, err := repo.TryStart(ctx, "test-consumer", "msg-123")
	assert.NoError(t, err)
	assert.True(t, ok, "NopInboxRepository should always return true for TryStart")

	// Test Complete
	err = repo.Complete(ctx, "test-consumer", "msg-123")
	assert.NoError(t, err)

	// Test GetByMessageID
	inbox, err := repo.GetByMessageID(ctx, "test-consumer", "msg-123")
	assert.Error(t, err)
	assert.ErrorIs(t, err, ErrNotFound)
	assert.Nil(t, inbox)

	// Test DeleteOlderThan
	count, err := repo.DeleteOlderThan(ctx, 24*time.Hour)
	assert.NoError(t, err)
	assert.Equal(t, int64(0), count)
}
