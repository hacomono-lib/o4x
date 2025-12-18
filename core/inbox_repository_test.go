package core

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
)

// TestNopInboxRepository_IsProcessed tests that NopInboxRepository always returns false
func TestNopInboxRepository_IsProcessed(t *testing.T) {
	repo := NewNopInboxRepository()

	ctx := context.Background()
	consumerName := "test-consumer"
	eventID := uuid.New()

	// Test IsProcessed - should always return false
	processed, err := repo.IsProcessed(ctx, consumerName, eventID)
	assert.NoError(t, err)
	assert.False(t, processed, "NopInboxRepository should always return false for IsProcessed")
}

// TestNopInboxRepository_IsProcessed_MultipleEvents tests IsProcessed with different events
func TestNopInboxRepository_IsProcessed_MultipleEvents(t *testing.T) {
	repo := NewNopInboxRepository()

	ctx := context.Background()
	consumerName := "test-consumer"

	// Multiple IsProcessed calls should all return false
	for i := 0; i < 3; i++ {
		eventID := uuid.New()
		processed, err := repo.IsProcessed(ctx, consumerName, eventID)
		assert.NoError(t, err)
		assert.False(t, processed, "NopInboxRepository should always return false for each event")
	}
}

// TestNopInboxRepository_Complete tests that Complete always succeeds
func TestNopInboxRepository_Complete(t *testing.T) {
	repo := NewNopInboxRepository()

	ctx := context.Background()
	consumerName := "test-consumer"
	eventID := uuid.New()

	// Test Complete - should always succeed
	err := repo.Complete(ctx, consumerName, eventID)
	assert.NoError(t, err)
}

// TestNopInboxRepository_Complete_Multiple tests Complete with multiple events
func TestNopInboxRepository_Complete_Multiple(t *testing.T) {
	repo := NewNopInboxRepository()

	ctx := context.Background()
	consumerName := "test-consumer"

	// Multiple Complete calls should all succeed
	for i := 0; i < 3; i++ {
		eventID := uuid.New()
		err := repo.Complete(ctx, consumerName, eventID)
		assert.NoError(t, err)
	}
}

// TestNopInboxRepository_GetByEventID tests that GetByEventID always returns ErrNotFound
func TestNopInboxRepository_GetByEventID(t *testing.T) {
	repo := NewNopInboxRepository()

	ctx := context.Background()
	consumerName := "test-consumer"
	eventID := uuid.New()

	// Test GetByEventID - should always return ErrNotFound
	inbox, err := repo.GetByEventID(ctx, consumerName, eventID)
	assert.Error(t, err)
	assert.ErrorIs(t, err, ErrNotFound)
	assert.Nil(t, inbox)
}

// TestNopInboxRepository_DeleteOlderThan tests that DeleteOlderThan returns 0
func TestNopInboxRepository_DeleteOlderThan(t *testing.T) {
	repo := NewNopInboxRepository()

	ctx := context.Background()

	// Test DeleteOlderThan - should always return 0
	count, err := repo.DeleteOlderThan(ctx, 24*time.Hour)
	assert.NoError(t, err)
	assert.Equal(t, int64(0), count)
}
