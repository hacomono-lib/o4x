package consumer

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/hacomono-lib/o4x/contrib/sqs/consumer"
	"github.com/hacomono-lib/o4x/examples/app/internal/domain"
	"github.com/hacomono-lib/o4x/examples/app/internal/service"
)

// OrderCreatedHandler handles order.created events
type OrderCreatedHandler struct {
	pool                *pgxpool.Pool
	notificationService *service.NotificationService
	logger              *slog.Logger
	sleepConfig         SleepConfig
}

func NewOrderCreatedHandler(
	pool *pgxpool.Pool,
	notificationService *service.NotificationService,
	logger *slog.Logger,
	sleepConfig SleepConfig,
) *OrderCreatedHandler {
	return &OrderCreatedHandler{
		pool:                pool,
		notificationService: notificationService,
		logger:              logger,
		sleepConfig:         sleepConfig,
	}
}

type OrderCreatedEvent struct {
	OrderID    uuid.UUID `json:"order_id"`
	UserID     uuid.UUID `json:"user_id"`
	ProductID  string    `json:"product_id"`
	Quantity   int       `json:"quantity"`
	TotalPrice int       `json:"total_price"`
}

func (h *OrderCreatedHandler) Handle(ctx context.Context, msg *consumer.SQSMessage) error {
	var event OrderCreatedEvent
	if err := json.Unmarshal(msg.Body, &event); err != nil {
		return fmt.Errorf("failed to unmarshal event: %w", err)
	}

	h.logger.Info("handling order.created event",
		"message_id", msg.MessageID,
		"order_id", event.OrderID,
		"user_id", event.UserID,
	)

	// Simulate DB operation + notification service call delay
	h.sleepConfig.Sleep()

	// Send order confirmation notification
	// This demonstrates calling another service from event handler
	_, err := h.notificationService.SendNotification(ctx, domain.SendNotificationRequest{
		Type:      domain.NotificationTypeEmail,
		Recipient: fmt.Sprintf("user-%s@example.com", event.UserID),
		Subject:   "Order Confirmation",
		Body:      fmt.Sprintf("Your order %s has been created successfully.", event.OrderID),
	})
	if err != nil {
		return fmt.Errorf("failed to send notification: %w", err)
	}

	h.logger.Info("order.created event processed successfully",
		"message_id", msg.MessageID,
		"order_id", event.OrderID,
	)

	return nil
}

// OrderConfirmedHandler handles order.confirmed events
type OrderConfirmedHandler struct {
	pool        *pgxpool.Pool
	logger      *slog.Logger
	sleepConfig SleepConfig
}

func NewOrderConfirmedHandler(pool *pgxpool.Pool, logger *slog.Logger, sleepConfig SleepConfig) *OrderConfirmedHandler {
	return &OrderConfirmedHandler{
		pool:        pool,
		logger:      logger,
		sleepConfig: sleepConfig,
	}
}

type OrderConfirmedEvent struct {
	OrderID   uuid.UUID `json:"order_id"`
	UserID    uuid.UUID `json:"user_id"`
	ProductID string    `json:"product_id"`
	Quantity  int       `json:"quantity"`
}

func (h *OrderConfirmedHandler) Handle(ctx context.Context, msg *consumer.SQSMessage) error {
	var event OrderConfirmedEvent
	if err := json.Unmarshal(msg.Body, &event); err != nil {
		return fmt.Errorf("failed to unmarshal event: %w", err)
	}

	h.logger.Info("handling order.confirmed event",
		"message_id", msg.MessageID,
		"order_id", event.OrderID,
	)

	// Simulate DB operation delay
	h.sleepConfig.Sleep()

	// Idempotent processing using message_id
	// Check if this message has already been processed
	tx, err := h.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	// Insert order confirmation record (idempotent via message_id)
	query := `
		INSERT INTO order_confirmations (message_id, order_id, user_id, product_id, quantity, processed_at)
		VALUES ($1, $2, $3, $4, $5, NOW())
		ON CONFLICT (message_id) DO NOTHING
		RETURNING order_id
	`

	var returnedOrderID uuid.UUID
	err = tx.QueryRow(ctx, query, msg.MessageID, event.OrderID, event.UserID, event.ProductID, event.Quantity).Scan(&returnedOrderID)
	if err != nil {
		// If no rows returned, it means this message was already processed
		if err.Error() == "no rows in result set" {
			h.logger.Info("order.confirmed event already processed (idempotent)",
				"message_id", msg.MessageID,
				"order_id", event.OrderID,
			)
			return nil
		}
		return fmt.Errorf("failed to insert order confirmation: %w", err)
	}

	h.logger.Info("order.confirmed event processed successfully",
		"message_id", msg.MessageID,
		"order_id", event.OrderID,
		"analytics_updated", true,
	)

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	return nil
}
