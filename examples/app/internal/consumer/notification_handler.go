package consumer

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"math/rand"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/hacomono-lib/o4x/contrib/sqs/consumer"
	"github.com/hacomono-lib/o4x/examples/app/internal/domain"
	"github.com/hacomono-lib/o4x/examples/app/internal/repository"
)

// NotificationEmailHandler handles notification.email events
// Demonstrates retry scenarios and failure handling
type NotificationEmailHandler struct {
	pool             *pgxpool.Pool
	notificationRepo *repository.NotificationRepository
	logger           *slog.Logger
	simulateFailure  bool
	failureRate      float64
	sleepConfig      SleepConfig
}

func NewNotificationEmailHandler(
	pool *pgxpool.Pool,
	notificationRepo *repository.NotificationRepository,
	logger *slog.Logger,
	simulateFailure bool,
	failureRate float64,
	sleepConfig SleepConfig,
) *NotificationEmailHandler {
	return &NotificationEmailHandler{
		pool:             pool,
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

	// Simulate email sending delay (external API call)
	h.sleepConfig.Sleep()

	// Simulate random failures for testing retry mechanism
	if h.simulateFailure && rand.Float64() < h.failureRate {
		h.logger.Warn("simulated email sending failure (will retry)",
			"message_id", msg.MessageID,
			"notification_id", event.NotificationID,
		)
		return fmt.Errorf("simulated email sending failure")
	}

	tx, err := h.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	// Update notification status (idempotent - safe to retry)
	if err := h.notificationRepo.UpdateStatus(ctx, tx, event.NotificationID, domain.NotificationStatusSent); err != nil {
		return fmt.Errorf("failed to update notification status: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
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
	logger      *slog.Logger
	sleepConfig SleepConfig
}

func NewNotificationSMSHandler(pool *pgxpool.Pool, logger *slog.Logger, sleepConfig SleepConfig) *NotificationSMSHandler {
	return &NotificationSMSHandler{
		pool:        pool,
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

	// Simulate SMS sending via external API
	h.sleepConfig.Sleep()

	h.logger.Info("sending SMS (simulated)",
		"notification_id", event.NotificationID,
		"recipient", event.Recipient,
		"body_length", len(event.Body),
	)

	h.logger.Info("notification.sms event processed successfully",
		"message_id", msg.MessageID,
		"notification_id", event.NotificationID,
	)

	return nil
}

// NotificationPushHandler handles notification.push events
type NotificationPushHandler struct {
	pool        *pgxpool.Pool
	logger      *slog.Logger
	sleepConfig SleepConfig
}

func NewNotificationPushHandler(pool *pgxpool.Pool, logger *slog.Logger, sleepConfig SleepConfig) *NotificationPushHandler {
	return &NotificationPushHandler{
		pool:        pool,
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

	// Simulate push notification sending
	h.sleepConfig.Sleep()

	h.logger.Info("sending push notification (simulated)",
		"notification_id", event.NotificationID,
		"recipient", event.Recipient,
		"subject", event.Subject,
	)

	h.logger.Info("notification.push event processed successfully",
		"message_id", msg.MessageID,
		"notification_id", event.NotificationID,
	)

	return nil
}
