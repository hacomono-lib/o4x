package repository

import (
	"context"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/hacomono-lib/o4x/examples/app/internal/domain"
)

type OrderRepository struct {
	pool *pgxpool.Pool
}

func NewOrderRepository(pool *pgxpool.Pool) *OrderRepository {
	return &OrderRepository{pool: pool}
}

func (r *OrderRepository) Create(ctx context.Context, tx pgx.Tx, order *domain.Order) error {
	query := `
		INSERT INTO orders (id, user_id, product_id, quantity, total_price, status, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
	`
	_, err := tx.Exec(ctx, query,
		order.ID,
		order.UserID,
		order.ProductID,
		order.Quantity,
		order.TotalPrice,
		order.Status,
		order.CreatedAt,
		order.UpdatedAt,
	)
	if err != nil {
		return fmt.Errorf("failed to create order: %w", err)
	}
	return nil
}

func (r *OrderRepository) GetByID(ctx context.Context, id uuid.UUID) (*domain.Order, error) {
	query := `
		SELECT id, user_id, product_id, quantity, total_price, status, created_at, updated_at
		FROM orders
		WHERE id = $1
	`
	var order domain.Order
	err := r.pool.QueryRow(ctx, query, id).Scan(
		&order.ID,
		&order.UserID,
		&order.ProductID,
		&order.Quantity,
		&order.TotalPrice,
		&order.Status,
		&order.CreatedAt,
		&order.UpdatedAt,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to get order: %w", err)
	}
	return &order, nil
}

func (r *OrderRepository) UpdateStatus(ctx context.Context, tx pgx.Tx, id uuid.UUID, status domain.OrderStatus) error {
	query := `
		UPDATE orders
		SET status = $1, updated_at = NOW()
		WHERE id = $2
	`
	result, err := tx.Exec(ctx, query, status, id)
	if err != nil {
		return fmt.Errorf("failed to update order status: %w", err)
	}
	if result.RowsAffected() == 0 {
		return fmt.Errorf("order not found: %s", id)
	}
	return nil
}
