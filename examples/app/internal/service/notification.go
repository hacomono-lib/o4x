package service

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/hacomono-lib/o4x/contrib/pgx"
	"github.com/hacomono-lib/o4x/core"
	"github.com/hacomono-lib/o4x/examples/app/internal/domain"
	"github.com/hacomono-lib/o4x/examples/app/internal/repository"
)

type NotificationService struct {
	pool             *pgxpool.Pool
	notificationRepo *repository.NotificationRepository
	outboxRepo       *pgx.OutboxRepository
}

func NewNotificationService(
	pool *pgxpool.Pool,
	notificationRepo *repository.NotificationRepository,
	outboxRepo *pgx.OutboxRepository,
) *NotificationService {
	return &NotificationService{
		pool:             pool,
		notificationRepo: notificationRepo,
		outboxRepo:       outboxRepo,
	}
}

type NotificationSendEvent struct {
	NotificationID uuid.UUID               `json:"notification_id"`
	Type           domain.NotificationType `json:"type"`
	Recipient      string                  `json:"recipient"`
	Subject        string                  `json:"subject"`
	Body           string                  `json:"body"`
	CreatedAt      time.Time               `json:"created_at"`
}

func (s *NotificationService) SendNotification(ctx context.Context, req domain.SendNotificationRequest) (*domain.Notification, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	// Create notification record
	notification := &domain.Notification{
		ID:        uuid.New(),
		Type:      req.Type,
		Recipient: req.Recipient,
		Subject:   req.Subject,
		Body:      req.Body,
		Status:    domain.NotificationStatusPending,
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
	}

	if err := s.notificationRepo.Create(ctx, tx, notification); err != nil {
		return nil, fmt.Errorf("failed to create notification: %w", err)
	}

	// Publish notification event (using topic routing based on type)
	event := NotificationSendEvent{
		NotificationID: notification.ID,
		Type:           notification.Type,
		Recipient:      notification.Recipient,
		Subject:        notification.Subject,
		Body:           notification.Body,
		CreatedAt:      notification.CreatedAt,
	}
	payload, err := json.Marshal(event)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal event: %w", err)
	}

	eventType := fmt.Sprintf("notification.%s", notification.Type)
	if _, err := s.outboxRepo.WithTx(tx).Insert(ctx, core.OutboxInsertParams{
		EventType:      eventType,
		Payload:        payload,
		IdempotencyKey: notification.ID.String(),
		MaxAttempts:     3, // Lower retries for notifications
	}); err != nil {
		return nil, fmt.Errorf("failed to insert outbox message: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("failed to commit transaction: %w", err)
	}

	return notification, nil
}
