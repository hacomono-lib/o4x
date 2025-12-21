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

type OrderService struct {
	pool          *pgxpool.Pool
	orderRepo     *repository.OrderRepository
	inventoryRepo *repository.InventoryRepository
	outboxRepo    *pgx.OutboxRepository
}

func NewOrderService(
	pool *pgxpool.Pool,
	orderRepo *repository.OrderRepository,
	inventoryRepo *repository.InventoryRepository,
	outboxRepo *pgx.OutboxRepository,
) *OrderService {
	return &OrderService{
		pool:          pool,
		orderRepo:     orderRepo,
		inventoryRepo: inventoryRepo,
		outboxRepo:    outboxRepo,
	}
}

type OrderCreatedEvent struct {
	OrderID    uuid.UUID `json:"order_id"`
	UserID     uuid.UUID `json:"user_id"`
	ProductID  string    `json:"product_id"`
	Quantity   int       `json:"quantity"`
	TotalPrice int       `json:"total_price"`
	CreatedAt  time.Time `json:"created_at"`
}

func (s *OrderService) CreateOrder(ctx context.Context, req domain.CreateOrderRequest) (*domain.Order, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	// Reserve inventory first
	if err := s.inventoryRepo.Reserve(ctx, tx, req.ProductID, req.Quantity); err != nil {
		return nil, fmt.Errorf("failed to reserve inventory: %w", err)
	}

	// Create order
	order := &domain.Order{
		ID:         uuid.New(),
		UserID:     req.UserID,
		ProductID:  req.ProductID,
		Quantity:   req.Quantity,
		TotalPrice: req.TotalPrice,
		Status:     domain.OrderStatusPending,
		CreatedAt:  time.Now(),
		UpdatedAt:  time.Now(),
	}

	if err := s.orderRepo.Create(ctx, tx, order); err != nil {
		return nil, fmt.Errorf("failed to create order: %w", err)
	}

	// Publish order.created event via outbox
	event := OrderCreatedEvent{
		OrderID:    order.ID,
		UserID:     order.UserID,
		ProductID:  order.ProductID,
		Quantity:   order.Quantity,
		TotalPrice: order.TotalPrice,
		CreatedAt:  order.CreatedAt,
	}
	payload, err := json.Marshal(event)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal event: %w", err)
	}

	if _, err := s.outboxRepo.WithTx(tx).Insert(ctx, core.OutboxInsertParams{
		EventType:      "order.created",
		Payload:        payload,
		IdempotencyKey: order.ID.String(),
		MaxAttempts:     5,
	}); err != nil {
		return nil, fmt.Errorf("failed to insert outbox message: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("failed to commit transaction: %w", err)
	}

	return order, nil
}

type OrderConfirmedEvent struct {
	OrderID   uuid.UUID `json:"order_id"`
	UserID    uuid.UUID `json:"user_id"`
	ProductID string    `json:"product_id"`
	Quantity  int       `json:"quantity"`
}

func (s *OrderService) ConfirmOrder(ctx context.Context, orderID uuid.UUID) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	// Get order
	order, err := s.orderRepo.GetByID(ctx, orderID)
	if err != nil {
		return fmt.Errorf("failed to get order: %w", err)
	}

	// Update order status
	if err := s.orderRepo.UpdateStatus(ctx, tx, orderID, domain.OrderStatusConfirmed); err != nil {
		return fmt.Errorf("failed to update order status: %w", err)
	}

	// Decrease inventory (complete the reservation)
	if err := s.inventoryRepo.Decrease(ctx, tx, order.ProductID, order.Quantity); err != nil {
		return fmt.Errorf("failed to decrease inventory: %w", err)
	}

	// Publish order.confirmed event
	event := OrderConfirmedEvent{
		OrderID:   orderID,
		UserID:    order.UserID,
		ProductID: order.ProductID,
		Quantity:  order.Quantity,
	}
	payload, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("failed to marshal event: %w", err)
	}

	if _, err := s.outboxRepo.WithTx(tx).Insert(ctx, core.OutboxInsertParams{
		EventType:      "order.confirmed",
		Payload:        payload,
		IdempotencyKey: fmt.Sprintf("order-confirmed-%s", orderID),
		MaxAttempts:     5,
	}); err != nil {
		return fmt.Errorf("failed to insert outbox message: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	return nil
}
