package consumer

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/hacomono-lib/o4x/contrib/pgx"
	"github.com/hacomono-lib/o4x/contrib/sqs/consumer"
)

// UserRegisteredHandler handles user.registered events
// Demonstrates idempotent processing with InboxRepository (Transactional Inbox Pattern)
type UserRegisteredHandler struct {
	pool        *pgxpool.Pool
	inbox       *pgx.InboxRepository
	logger      *slog.Logger
	sleepConfig SleepConfig
}

func NewUserRegisteredHandler(pool *pgxpool.Pool, inbox *pgx.InboxRepository, logger *slog.Logger, sleepConfig SleepConfig) *UserRegisteredHandler {
	return &UserRegisteredHandler{
		pool:        pool,
		inbox:       inbox,
		logger:      logger,
		sleepConfig: sleepConfig,
	}
}

type UserRegisteredEvent struct {
	UserID uuid.UUID `json:"user_id"`
	Email  string    `json:"email"`
	Name   string    `json:"name"`
}

func (h *UserRegisteredHandler) Handle(ctx context.Context, msg *consumer.SQSMessage) error {
	var event UserRegisteredEvent
	if err := json.Unmarshal(msg.Body, &event); err != nil {
		return fmt.Errorf("failed to unmarshal event: %w", err)
	}

	h.logger.Info("handling user.registered event",
		"message_id", msg.MessageID,
		"user_id", event.UserID,
		"email", event.Email,
	)

	// DB operation pattern: Begin -> TryStart -> process -> Complete -> Commit
	tx, err := h.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	// Check idempotency using InboxRepository (within transaction)
	// CRITICAL: Use msg.EventID (Outbox ID), NOT msg.MessageID
	inboxTx := h.inbox.WithTx(tx)
	processed, err := inboxTx.IsProcessed(ctx, "user", msg.EventID)
	if err != nil {
		return fmt.Errorf("failed to check inbox: %w", err)
	}
	if processed {
		h.logger.Info("user.registered event already processed (idempotent)",
			"event_id", msg.EventID,
			"message_id", msg.MessageID,
			"user_id", event.UserID,
		)
		return nil
	}

	// Simulate DB operation delay (within transaction)
	h.sleepConfig.Sleep()

	// Insert welcome credit record
	// This ensures we don't create duplicate welcome credits for the same event
	query := `
		INSERT INTO user_welcome_credits (event_id, user_id, credit_amount, granted_at)
		VALUES ($1, $2, $3, NOW())
	`
	_, err = tx.Exec(ctx, query, msg.EventID, event.UserID, 1000)
	if err != nil {
		return fmt.Errorf("failed to insert welcome credit: %w", err)
	}

	// Mark as completed in inbox
	// CRITICAL: Use msg.EventID (Outbox ID), NOT msg.MessageID
	if err := inboxTx.Complete(ctx, "user", msg.EventID); err != nil {
		return fmt.Errorf("failed to mark as completed: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	h.logger.Info("user.registered event processed successfully",
		"event_id", msg.EventID,
		"message_id", msg.MessageID,
		"user_id", event.UserID,
		"credit_granted", 1000,
	)

	return nil
}

// UserUpdatedHandler handles user.updated events
type UserUpdatedHandler struct {
	pool        *pgxpool.Pool
	logger      *slog.Logger
	sleepConfig SleepConfig
}

func NewUserUpdatedHandler(pool *pgxpool.Pool, logger *slog.Logger, sleepConfig SleepConfig) *UserUpdatedHandler {
	return &UserUpdatedHandler{
		pool:        pool,
		logger:      logger,
		sleepConfig: sleepConfig,
	}
}

type UserUpdatedEvent struct {
	UserID uuid.UUID `json:"user_id"`
	Name   string    `json:"name"`
	Status string    `json:"status"`
}

func (h *UserUpdatedHandler) Handle(ctx context.Context, msg *consumer.SQSMessage) error {
	var event UserUpdatedEvent
	if err := json.Unmarshal(msg.Body, &event); err != nil {
		return fmt.Errorf("failed to unmarshal event: %w", err)
	}

	h.logger.Info("handling user.updated event",
		"message_id", msg.MessageID,
		"user_id", event.UserID,
	)

	// Simulate syncing to external CRM system (external API call delay)
	h.sleepConfig.Sleep()

	h.logger.Info("syncing user to CRM (simulated)",
		"user_id", event.UserID,
		"name", event.Name,
		"status", event.Status,
	)

	h.logger.Info("user.updated event processed successfully",
		"message_id", msg.MessageID,
		"user_id", event.UserID,
	)

	return nil
}
