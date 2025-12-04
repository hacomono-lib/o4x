package repository

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/hacomono-lib/o4x/examples/app/internal/domain"
)

type InventoryRepository struct {
	pool *pgxpool.Pool
}

func NewInventoryRepository(pool *pgxpool.Pool) *InventoryRepository {
	return &InventoryRepository{pool: pool}
}

func (r *InventoryRepository) GetByProductID(ctx context.Context, productID string) (*domain.Inventory, error) {
	query := `
		SELECT product_id, quantity, reserved, updated_at
		FROM inventory
		WHERE product_id = $1
		FOR UPDATE
	`
	var inv domain.Inventory
	err := r.pool.QueryRow(ctx, query, productID).Scan(
		&inv.ProductID,
		&inv.Quantity,
		&inv.Reserved,
		&inv.UpdatedAt,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to get inventory: %w", err)
	}
	return &inv, nil
}

func (r *InventoryRepository) Reserve(ctx context.Context, tx pgx.Tx, productID string, quantity int) error {
	query := `
		UPDATE inventory
		SET reserved = reserved + $1, updated_at = NOW()
		WHERE product_id = $2 AND (quantity - reserved) >= $1
	`
	result, err := tx.Exec(ctx, query, quantity, productID)
	if err != nil {
		return fmt.Errorf("failed to reserve inventory: %w", err)
	}
	if result.RowsAffected() == 0 {
		return fmt.Errorf("insufficient inventory for product %s", productID)
	}
	return nil
}

func (r *InventoryRepository) Decrease(ctx context.Context, tx pgx.Tx, productID string, quantity int) error {
	query := `
		UPDATE inventory
		SET quantity = quantity - $1,
		    reserved = reserved - $1,
		    updated_at = NOW()
		WHERE product_id = $2 AND reserved >= $1
	`
	result, err := tx.Exec(ctx, query, quantity, productID)
	if err != nil {
		return fmt.Errorf("failed to decrease inventory: %w", err)
	}
	if result.RowsAffected() == 0 {
		return fmt.Errorf("insufficient reserved inventory for product %s", productID)
	}
	return nil
}

func (r *InventoryRepository) ReleaseReservation(ctx context.Context, tx pgx.Tx, productID string, quantity int) error {
	query := `
		UPDATE inventory
		SET reserved = reserved - $1, updated_at = NOW()
		WHERE product_id = $2 AND reserved >= $1
	`
	result, err := tx.Exec(ctx, query, quantity, productID)
	if err != nil {
		return fmt.Errorf("failed to release reservation: %w", err)
	}
	if result.RowsAffected() == 0 {
		return fmt.Errorf("insufficient reserved inventory for product %s", productID)
	}
	return nil
}
