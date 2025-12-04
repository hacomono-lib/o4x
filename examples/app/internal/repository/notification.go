package repository

import (
	"context"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/hacomono-lib/o4x/examples/app/internal/domain"
)

type NotificationRepository struct {
	pool *pgxpool.Pool
}

func NewNotificationRepository(pool *pgxpool.Pool) *NotificationRepository {
	return &NotificationRepository{pool: pool}
}

func (r *NotificationRepository) Create(ctx context.Context, tx pgx.Tx, notification *domain.Notification) error {
	query := `
		INSERT INTO notifications (id, type, recipient, subject, body, status, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
		ON CONFLICT (id) DO NOTHING
	`
	_, err := tx.Exec(ctx, query,
		notification.ID,
		notification.Type,
		notification.Recipient,
		notification.Subject,
		notification.Body,
		notification.Status,
		notification.CreatedAt,
		notification.UpdatedAt,
	)
	if err != nil {
		return fmt.Errorf("failed to create notification: %w", err)
	}
	return nil
}

func (r *NotificationRepository) UpdateStatus(ctx context.Context, tx pgx.Tx, id uuid.UUID, status domain.NotificationStatus) error {
	query := `
		UPDATE notifications
		SET status = $1, updated_at = NOW()
		WHERE id = $2
	`
	result, err := tx.Exec(ctx, query, status, id)
	if err != nil {
		return fmt.Errorf("failed to update notification status: %w", err)
	}
	if result.RowsAffected() == 0 {
		return fmt.Errorf("notification not found: %s", id)
	}
	return nil
}

func (r *NotificationRepository) GetByID(ctx context.Context, id uuid.UUID) (*domain.Notification, error) {
	query := `
		SELECT id, type, recipient, subject, body, status, created_at, updated_at
		FROM notifications
		WHERE id = $1
	`
	var notification domain.Notification
	err := r.pool.QueryRow(ctx, query, id).Scan(
		&notification.ID,
		&notification.Type,
		&notification.Recipient,
		&notification.Subject,
		&notification.Body,
		&notification.Status,
		&notification.CreatedAt,
		&notification.UpdatedAt,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to get notification: %w", err)
	}
	return &notification, nil
}
