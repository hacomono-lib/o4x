package domain

import (
	"time"

	"github.com/google/uuid"
)

type NotificationType string

const (
	NotificationTypeEmail NotificationType = "email"
	NotificationTypeSMS   NotificationType = "sms"
	NotificationTypePush  NotificationType = "push"
)

type NotificationStatus string

const (
	NotificationStatusPending NotificationStatus = "pending"
	NotificationStatusSent    NotificationStatus = "sent"
	NotificationStatusFailed  NotificationStatus = "failed"
)

type Notification struct {
	ID        uuid.UUID          `json:"id"`
	Type      NotificationType   `json:"type"`
	Recipient string             `json:"recipient"`
	Subject   string             `json:"subject"`
	Body      string             `json:"body"`
	Status    NotificationStatus `json:"status"`
	CreatedAt time.Time          `json:"created_at"`
	UpdatedAt time.Time          `json:"updated_at"`
}

type SendNotificationRequest struct {
	Type      NotificationType `json:"type"`
	Recipient string           `json:"recipient"`
	Subject   string           `json:"subject"`
	Body      string           `json:"body"`
}
