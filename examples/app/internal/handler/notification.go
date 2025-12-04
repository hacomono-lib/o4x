package handler

import (
	"encoding/json"
	"log/slog"
	"net/http"

	"github.com/hacomono-lib/o4x/examples/app/internal/domain"
	"github.com/hacomono-lib/o4x/examples/app/internal/service"
)

type NotificationHandler struct {
	notificationService *service.NotificationService
	logger              *slog.Logger
}

func NewNotificationHandler(notificationService *service.NotificationService, logger *slog.Logger) *NotificationHandler {
	return &NotificationHandler{
		notificationService: notificationService,
		logger:              logger,
	}
}

func (h *NotificationHandler) SendNotification(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req domain.SendNotificationRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		h.logger.Error("failed to decode request", "error", err)
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	notification, err := h.notificationService.SendNotification(r.Context(), req)
	if err != nil {
		h.logger.Error("failed to send notification", "error", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(notification)
}
