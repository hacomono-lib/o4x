package repository

import (
	"context"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/hacomono-lib/o4x/examples/app/internal/domain"
)

type UserRepository struct {
	pool *pgxpool.Pool
}

func NewUserRepository(pool *pgxpool.Pool) *UserRepository {
	return &UserRepository{pool: pool}
}

func (r *UserRepository) Create(ctx context.Context, tx pgx.Tx, user *domain.User) error {
	query := `
		INSERT INTO users (id, email, name, status, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6)
	`
	_, err := tx.Exec(ctx, query,
		user.ID,
		user.Email,
		user.Name,
		user.Status,
		user.CreatedAt,
		user.UpdatedAt,
	)
	if err != nil {
		return fmt.Errorf("failed to create user: %w", err)
	}
	return nil
}

func (r *UserRepository) GetByID(ctx context.Context, id uuid.UUID) (*domain.User, error) {
	query := `
		SELECT id, email, name, status, created_at, updated_at
		FROM users
		WHERE id = $1
	`
	var user domain.User
	err := r.pool.QueryRow(ctx, query, id).Scan(
		&user.ID,
		&user.Email,
		&user.Name,
		&user.Status,
		&user.CreatedAt,
		&user.UpdatedAt,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to get user: %w", err)
	}
	return &user, nil
}

func (r *UserRepository) GetByEmail(ctx context.Context, email string) (*domain.User, error) {
	query := `
		SELECT id, email, name, status, created_at, updated_at
		FROM users
		WHERE email = $1
	`
	var user domain.User
	err := r.pool.QueryRow(ctx, query, email).Scan(
		&user.ID,
		&user.Email,
		&user.Name,
		&user.Status,
		&user.CreatedAt,
		&user.UpdatedAt,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to get user by email: %w", err)
	}
	return &user, nil
}

func (r *UserRepository) Update(ctx context.Context, tx pgx.Tx, id uuid.UUID, req domain.UpdateUserRequest) error {
	query := `
		UPDATE users
		SET name = COALESCE(NULLIF($1, ''), name),
		    status = COALESCE(NULLIF($2, ''), status),
		    updated_at = NOW()
		WHERE id = $3
	`
	result, err := tx.Exec(ctx, query, req.Name, req.Status, id)
	if err != nil {
		return fmt.Errorf("failed to update user: %w", err)
	}
	if result.RowsAffected() == 0 {
		return fmt.Errorf("user not found: %s", id)
	}
	return nil
}
