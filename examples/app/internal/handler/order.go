package handler

import (
	"encoding/json"
	"log/slog"
	"net/http"

	"github.com/google/uuid"

	"github.com/hacomono-lib/o4x/examples/app/internal/domain"
	"github.com/hacomono-lib/o4x/examples/app/internal/service"
)

type OrderHandler struct {
	orderService *service.OrderService
	logger       *slog.Logger
}

func NewOrderHandler(orderService *service.OrderService, logger *slog.Logger) *OrderHandler {
	return &OrderHandler{
		orderService: orderService,
		logger:       logger,
	}
}

func (h *OrderHandler) CreateOrder(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req domain.CreateOrderRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		h.logger.Error("failed to decode request", "error", err)
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	order, err := h.orderService.CreateOrder(r.Context(), req)
	if err != nil {
		h.logger.Error("failed to create order", "error", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(order)
}

func (h *OrderHandler) ConfirmOrder(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	orderIDStr := r.URL.Query().Get("order_id")
	if orderIDStr == "" {
		http.Error(w, "order_id is required", http.StatusBadRequest)
		return
	}

	orderID, err := uuid.Parse(orderIDStr)
	if err != nil {
		http.Error(w, "Invalid order_id", http.StatusBadRequest)
		return
	}

	if err := h.orderService.ConfirmOrder(r.Context(), orderID); err != nil {
		h.logger.Error("failed to confirm order", "error", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
	w.Write([]byte(`{"status":"confirmed"}`))
}
