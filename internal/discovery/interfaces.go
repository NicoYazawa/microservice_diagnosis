// Package discovery provides service registry and discovery abstractions.
package discovery

import (
	"context"
	"time"
)

// ServiceInstance represents a registered service instance.
type ServiceInstance struct {
	ID        string
	Name      string // e.g. "agent-log", "orchestrator"
	Kind      string // e.g. "log", "metric", "trace", "rca", "fix", "orchestrator"
	Version   string
	HTTPAddr  string // address exposed for health checks
	Status    string    // "healthy", "degraded", "unhealthy"
	Heartbeat time.Time // last heartbeat from Consul TTL check
}

// Registry is the service registry interface (Consul by default, K8s DNS as fallback).
type Registry interface {
	// Register registers a service instance.
	Register(ctx context.Context, inst *ServiceInstance) error
	// Deregister removes a service instance.
	Deregister(ctx context.Context, serviceID string) error
	// Discover returns all healthy instances of a service by name.
	Discover(ctx context.Context, serviceName string) ([]*ServiceInstance, error)
	// HealthCheck updates the health status of a registered instance.
	HealthCheck(ctx context.Context, serviceID string, status string) error
}

// Discovery is the service discovery interface.
type Discovery interface {
	// Discover returns all healthy instances matching the given service name pattern.
	Discover(ctx context.Context, name string) ([]*ServiceInstance, error)
}
