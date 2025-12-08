package consumer

// TopicRouterRegistry manages multiple TopicRouters by group
type TopicRouterRegistry struct {
	routers map[string]*TopicRouter
}

// NewTopicRouterRegistry creates a new TopicRouterRegistry
func NewTopicRouterRegistry() *TopicRouterRegistry {
	return &TopicRouterRegistry{
		routers: make(map[string]*TopicRouter),
	}
}

// RegisterGroup creates a new TopicRouter for the given group and calls the setup function
func (r *TopicRouterRegistry) RegisterGroup(group string, setup func(*TopicRouter)) {
	router := NewTopicRouter()
	setup(router)
	r.routers[group] = router
}

// GetRouter returns the router for the given group
func (r *TopicRouterRegistry) GetRouter(group string) (*TopicRouter, bool) {
	router, ok := r.routers[group]
	return router, ok
}

// ValidGroups returns all registered group names
func (r *TopicRouterRegistry) ValidGroups() []string {
	groups := make([]string, 0, len(r.routers))
	for group := range r.routers {
		groups = append(groups, group)
	}
	return groups
}
