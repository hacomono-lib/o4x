package domain

import (
	"time"
)

type Inventory struct {
	ProductID string    `json:"product_id"`
	Quantity  int       `json:"quantity"`
	Reserved  int       `json:"reserved"`
	UpdatedAt time.Time `json:"updated_at"`
}

type ReserveInventoryRequest struct {
	ProductID string `json:"product_id"`
	Quantity  int    `json:"quantity"`
}
