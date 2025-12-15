package consumer

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"math/rand"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/hacomono-lib/o4x/contrib/pgx"
	"github.com/hacomono-lib/o4x/contrib/sqs/consumer"
	"github.com/hacomono-lib/o4x/examples/app/internal/domain"
	"github.com/hacomono-lib/o4x/examples/app/internal/repository"
)

// NotificationEmailHandler handles notification.email events
// Demonstrates retry scenarios and failure handling
type NotificationEmailHandler struct {
	pool             *pgxpool.Pool
	inbox            *pgx.InboxRepository
	notificationRepo *repository.NotificationRepository
	logger           *slog.Logger
	simulateFailure  bool
	failureRate      float64
	sleepConfig      SleepConfig
}

func NewNotificationEmailHandler(
	pool *pgxpool.Pool,
	inbox *pgx.InboxRepository,
	notificationRepo *repository.NotificationRepository,
	logger *slog.Logger,
	simulateFailure bool,
	failureRate float64,
	sleepConfig SleepConfig,
) *NotificationEmailHandler {
	return &NotificationEmailHandler{
		pool:             pool,
		inbox:            inbox,
		notificationRepo: notificationRepo,
		logger:           logger,
		simulateFailure:  simulateFailure,
		failureRate:      failureRate,
		sleepConfig:      sleepConfig,
	}
}

type NotificationSendEvent struct {
	NotificationID uuid.UUID               `json:"notification_id"`
	Type           domain.NotificationType `json:"type"`
	Recipient      string                  `json:"recipient"`
	Subject        string                  `json:"subject"`
	Body           string                  `json:"body"`
}

func (h *NotificationEmailHandler) Handle(ctx context.Context, msg *consumer.SQSMessage) error {
	var event NotificationSendEvent
	if err := json.Unmarshal(msg.Body, &event); err != nil {
		return fmt.Errorf("failed to unmarshal event: %w", err)
	}

	h.logger.Info("handling notification.email event",
		"message_id", msg.MessageID,
		"notification_id", event.NotificationID,
		"recipient", event.Recipient,
	)

	// External API pattern (auto-commit): TryStart -> API call -> Complete
	// No transaction needed for external API calls

	// Check idempotency using InboxRepository (auto-commit)
	shouldProcess, err := h.inbox.TryStart(ctx, "notification", msg.MessageID)
	if err != nil {
		return fmt.Errorf("failed to check inbox: %w", err)
	}
	if !shouldProcess {
		h.logger.Info("notification.email event already processed (idempotent)",
			"message_id", msg.MessageID,
			"notification_id", event.NotificationID,
		)
		return nil
	}

	// Simulate email sending via external API (after TryStart)
	h.sleepConfig.Sleep()

	// Simulate random failures for testing retry mechanism
	if h.simulateFailure && rand.Float64() < h.failureRate {
		h.logger.Warn("simulated email sending failure (will retry)",
			"message_id", msg.MessageID,
			"notification_id", event.NotificationID,
		)
		return fmt.Errorf("simulated email sending failure")
	}

	// Update notification status (idempotent - safe to retry)
	if err := h.notificationRepo.UpdateStatusWithPool(ctx, h.pool, event.NotificationID, domain.NotificationStatusSent); err != nil {
		return fmt.Errorf("failed to update notification status: %w", err)
	}

	// Mark as completed in inbox (auto-commit)
	if err := h.inbox.Complete(ctx, "notification", msg.MessageID); err != nil {
		return fmt.Errorf("failed to mark as completed: %w", err)
	}

	h.logger.Info("notification.email event processed successfully",
		"message_id", msg.MessageID,
		"notification_id", event.NotificationID,
		"recipient", event.Recipient,
	)

	return nil
}

// NotificationSMSHandler handles notification.sms events
type NotificationSMSHandler struct {
	pool        *pgxpool.Pool
	inbox       *pgx.InboxRepository
	logger      *slog.Logger
	sleepConfig SleepConfig
}

func NewNotificationSMSHandler(pool *pgxpool.Pool, inbox *pgx.InboxRepository, logger *slog.Logger, sleepConfig SleepConfig) *NotificationSMSHandler {
	return &NotificationSMSHandler{
		pool:        pool,
		inbox:       inbox,
		logger:      logger,
		sleepConfig: sleepConfig,
	}
}

func (h *NotificationSMSHandler) Handle(ctx context.Context, msg *consumer.SQSMessage) error {
	var event NotificationSendEvent
	if err := json.Unmarshal(msg.Body, &event); err != nil {
		return fmt.Errorf("failed to unmarshal event: %w", err)
	}

	h.logger.Info("handling notification.sms event",
		"message_id", msg.MessageID,
		"notification_id", event.NotificationID,
		"recipient", event.Recipient,
	)

	// External API pattern (auto-commit): TryStart -> API call -> Complete
	// No transaction needed for external API calls

	// Check idempotency using InboxRepository (auto-commit)
	shouldProcess, err := h.inbox.TryStart(ctx, "notification", msg.MessageID)
	if err != nil {
		return fmt.Errorf("failed to check inbox: %w", err)
	}
	if !shouldProcess {
		h.logger.Info("notification.sms event already processed (idempotent)",
			"message_id", msg.MessageID,
			"notification_id", event.NotificationID,
		)
		return nil
	}

	// Simulate SMS sending via external API (after TryStart)
	h.sleepConfig.Sleep()

	h.logger.Info("sending SMS (simulated)",
		"notification_id", event.NotificationID,
		"recipient", event.Recipient,
		"body_length", len(event.Body),
	)

	// Mark as completed in inbox (auto-commit)
	if err := h.inbox.Complete(ctx, "notification", msg.MessageID); err != nil {
		return fmt.Errorf("failed to mark as completed: %w", err)
	}

	h.logger.Info("notification.sms event processed successfully",
		"message_id", msg.MessageID,
		"notification_id", event.NotificationID,
	)

	return nil
}

// NotificationPushHandler handles notification.push events
type NotificationPushHandler struct {
	pool        *pgxpool.Pool
	inbox       *pgx.InboxRepository
	logger      *slog.Logger
	sleepConfig SleepConfig
}

func NewNotificationPushHandler(pool *pgxpool.Pool, inbox *pgx.InboxRepository, logger *slog.Logger, sleepConfig SleepConfig) *NotificationPushHandler {
	return &NotificationPushHandler{
		pool:        pool,
		inbox:       inbox,
		logger:      logger,
		sleepConfig: sleepConfig,
	}
}

func (h *NotificationPushHandler) Handle(ctx context.Context, msg *consumer.SQSMessage) error {
	var event NotificationSendEvent
	if err := json.Unmarshal(msg.Body, &event); err != nil {
		return fmt.Errorf("failed to unmarshal event: %w", err)
	}

	h.logger.Info("handling notification.push event",
		"message_id", msg.MessageID,
		"notification_id", event.NotificationID,
		"recipient", event.Recipient,
	)

	// External API pattern (auto-commit): TryStart -> API call -> Complete
	// No transaction needed for external API calls

	// Check idempotency using InboxRepository (auto-commit)
	shouldProcess, err := h.inbox.TryStart(ctx, "notification", msg.MessageID)
	if err != nil {
		return fmt.Errorf("failed to check inbox: %w", err)
	}
	if !shouldProcess {
		h.logger.Info("notification.push event already processed (idempotent)",
			"message_id", msg.MessageID,
			"notification_id", event.NotificationID,
		)
		return nil
	}

	// Simulate push notification sending via external API (after TryStart)
	h.sleepConfig.Sleep()

	h.logger.Info("sending push notification (simulated)",
		"notification_id", event.NotificationID,
		"recipient", event.Recipient,
		"subject", event.Subject,
	)

	// Mark as completed in inbox (auto-commit)
	if err := h.inbox.Complete(ctx, "notification", msg.MessageID); err != nil {
		return fmt.Errorf("failed to mark as completed: %w", err)
	}

	h.logger.Info("notification.push event processed successfully",
		"message_id", msg.MessageID,
		"notification_id", event.NotificationID,
	)

	return nil
}
