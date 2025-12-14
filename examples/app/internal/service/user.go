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

type UserService struct {
	pool       *pgxpool.Pool
	userRepo   *repository.UserRepository
	outboxRepo *pgx.OutboxRepository
}

func NewUserService(
	pool *pgxpool.Pool,
	userRepo *repository.UserRepository,
	outboxRepo *pgx.OutboxRepository,
) *UserService {
	return &UserService{
		pool:       pool,
		userRepo:   userRepo,
		outboxRepo: outboxRepo,
	}
}

type UserRegisteredEvent struct {
	UserID    uuid.UUID `json:"user_id"`
	Email     string    `json:"email"`
	Name      string    `json:"name"`
	CreatedAt time.Time `json:"created_at"`
}

func (s *UserService) RegisterUser(ctx context.Context, req domain.CreateUserRequest) (*domain.User, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	// Create user
	user := &domain.User{
		ID:        uuid.New(),
		Email:     req.Email,
		Name:      req.Name,
		Status:    domain.UserStatusActive,
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
	}

	if err := s.userRepo.Create(ctx, tx, user); err != nil {
		return nil, fmt.Errorf("failed to create user: %w", err)
	}

	// Publish user.registered event
	event := UserRegisteredEvent{
		UserID:    user.ID,
		Email:     user.Email,
		Name:      user.Name,
		CreatedAt: user.CreatedAt,
	}
	payload, err := json.Marshal(event)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal event: %w", err)
	}

	if _, err := s.outboxRepo.WithTx(tx).Insert(ctx, core.OutboxInsertParams{
		EventType:      "user.registered",
		Payload:        payload,
		IdempotencyKey: user.ID.String(),
		MaxRetries:     5,
	}); err != nil {
		return nil, fmt.Errorf("failed to insert outbox message: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("failed to commit transaction: %w", err)
	}

	return user, nil
}

type UserUpdatedEvent struct {
	UserID    uuid.UUID         `json:"user_id"`
	Name      string            `json:"name"`
	Status    domain.UserStatus `json:"status"`
	UpdatedAt time.Time         `json:"updated_at"`
}

func (s *UserService) UpdateUser(ctx context.Context, userID uuid.UUID, req domain.UpdateUserRequest) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	// Update user
	if err := s.userRepo.Update(ctx, tx, userID, req); err != nil {
		return fmt.Errorf("failed to update user: %w", err)
	}

	// Publish user.updated event
	event := UserUpdatedEvent{
		UserID:    userID,
		Name:      req.Name,
		Status:    req.Status,
		UpdatedAt: time.Now(),
	}
	payload, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("failed to marshal event: %w", err)
	}

	if _, err := s.outboxRepo.WithTx(tx).Insert(ctx, core.OutboxInsertParams{
		EventType:      "user.updated",
		Payload:        payload,
		IdempotencyKey: fmt.Sprintf("user-updated-%s-%d", userID, time.Now().UnixNano()),
		MaxRetries:     5,
	}); err != nil {
		return fmt.Errorf("failed to insert outbox message: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	return nil
}
