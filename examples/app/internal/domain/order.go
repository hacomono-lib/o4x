package domain

import (
	"time"

	"github.com/google/uuid"
)

type OrderStatus string

const (
	OrderStatusPending   OrderStatus = "pending"
	OrderStatusConfirmed OrderStatus = "confirmed"
	OrderStatusCancelled OrderStatus = "cancelled"
	OrderStatusCompleted OrderStatus = "completed"
)

type Order struct {
	ID         uuid.UUID   `json:"id"`
	UserID     uuid.UUID   `json:"user_id"`
	ProductID  string      `json:"product_id"`
	Quantity   int         `json:"quantity"`
	TotalPrice int         `json:"total_price"`
	Status     OrderStatus `json:"status"`
	CreatedAt  time.Time   `json:"created_at"`
	UpdatedAt  time.Time   `json:"updated_at"`
}

type CreateOrderRequest struct {
	UserID     uuid.UUID `json:"user_id"`
	ProductID  string    `json:"product_id"`
	Quantity   int       `json:"quantity"`
	TotalPrice int       `json:"total_price"`
}
