package consumer

// EventTypeRouterRegistry manages multiple EventTypeRouters by group
type EventTypeRouterRegistry struct {
	routers map[string]*EventTypeRouter
}

// NewEventTypeRouterRegistry creates a new EventTypeRouterRegistry
func NewEventTypeRouterRegistry() *EventTypeRouterRegistry {
	return &EventTypeRouterRegistry{
		routers: make(map[string]*EventTypeRouter),
	}
}

// RegisterGroup creates a new EventTypeRouter for the given group and calls the setup function
func (r *EventTypeRouterRegistry) RegisterGroup(group string, setup func(*EventTypeRouter)) {
	router := NewEventTypeRouter()
	setup(router)
	r.routers[group] = router
}

// GetRouter returns the router for the given group
func (r *EventTypeRouterRegistry) GetRouter(group string) (*EventTypeRouter, bool) {
	router, ok := r.routers[group]
	return router, ok
}

// ValidGroups returns all registered group names
func (r *EventTypeRouterRegistry) ValidGroups() []string {
	groups := make([]string, 0, len(r.routers))
	for group := range r.routers {
		groups = append(groups, group)
	}
	return groups
}
