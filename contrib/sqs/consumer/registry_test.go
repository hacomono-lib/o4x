package consumer_test

import (
	"context"
	"testing"

	"github.com/hacomono-lib/o4x/contrib/sqs/consumer"
)

// TestEventTypeRouterRegistry tests the registry functionality
func TestEventTypeRouterRegistry(t *testing.T) {
	registry := consumer.NewEventTypeRouterRegistry()

	// Register two groups
	registry.RegisterGroup("group1", func(router *consumer.EventTypeRouter) {
		router.Register("event1", consumer.HandlerFunc(func(ctx context.Context, msg *consumer.SQSMessage) error {
			return nil
		}))
	})
	registry.RegisterGroup("group2", func(router *consumer.EventTypeRouter) {
		router.Register("event2", consumer.HandlerFunc(func(ctx context.Context, msg *consumer.SQSMessage) error {
			return nil
		}))
	})

	// Test GetRouter
	router1, ok := registry.GetRouter("group1")
	if !ok {
		t.Error("expected group1 to exist")
	}
	if router1 == nil {
		t.Error("expected non-nil router for group1")
	}

	router2, ok := registry.GetRouter("group2")
	if !ok {
		t.Error("expected group2 to exist")
	}
	if router2 == nil {
		t.Error("expected non-nil router for group2")
	}

	// Test GetRouter with non-existent group
	_, ok = registry.GetRouter("nonexistent")
	if ok {
		t.Error("expected nonexistent group to not exist")
	}

	// Test ValidGroups
	groups := registry.ValidGroups()
	if len(groups) != 2 {
		t.Errorf("expected 2 groups, got %d", len(groups))
	}

	hasGroup1 := false
	hasGroup2 := false
	for _, g := range groups {
		if g == "group1" {
			hasGroup1 = true
		}
		if g == "group2" {
			hasGroup2 = true
		}
	}
	if !hasGroup1 || !hasGroup2 {
		t.Error("missing expected groups")
	}
}
