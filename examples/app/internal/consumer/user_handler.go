package consumer

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/hacomono-lib/o4x/contrib/sqs/consumer"
)

// UserRegisteredHandler handles user.registered events
// Demonstrates idempotent processing with business data check
type UserRegisteredHandler struct {
	pool        *pgxpool.Pool
	logger      *slog.Logger
	sleepConfig SleepConfig
}

func NewUserRegisteredHandler(pool *pgxpool.Pool, logger *slog.Logger, sleepConfig SleepConfig) *UserRegisteredHandler {
	return &UserRegisteredHandler{
		pool:        pool,
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

	// Simulate DB operation delay
	h.sleepConfig.Sleep()

	tx, err := h.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	// Idempotent insert - use message_id as deduplication key
	// This ensures we don't create duplicate welcome credits for the same message
	query := `
		INSERT INTO user_welcome_credits (message_id, user_id, credit_amount, granted_at)
		VALUES ($1, $2, $3, NOW())
		ON CONFLICT (message_id) DO NOTHING
		RETURNING user_id
	`

	var returnedUserID uuid.UUID
	err = tx.QueryRow(ctx, query, msg.MessageID, event.UserID, 1000).Scan(&returnedUserID)
	if err != nil {
		// If no rows returned, it means this message was already processed
		if err.Error() == "no rows in result set" {
			h.logger.Info("user.registered event already processed (idempotent)",
				"message_id", msg.MessageID,
				"user_id", event.UserID,
			)
			return nil
		}
		return fmt.Errorf("failed to insert welcome credit: %w", err)
	}

	h.logger.Info("user.registered event processed successfully",
		"message_id", msg.MessageID,
		"user_id", event.UserID,
		"credit_granted", 1000,
	)

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

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
