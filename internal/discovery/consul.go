// Package discovery provides service registry and discovery abstractions.
package discovery

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/hashicorp/consul/api"
)

// ConsulRegistry implements Registry using HashiCorp Consul.
type ConsulRegistry struct {
	client  *api.Client
	log     *slog.Logger
	selfID  string
	ttlSecs int
}

// NewConsulRegistry creates a ConsulRegistry from a config.
func NewConsulRegistry(addr string, log *slog.Logger) (*ConsulRegistry, error) {
	cfg := api.DefaultConfig()
	cfg.Address = addr
	client, err := api.NewClient(cfg)
	if err != nil {
		return nil, fmt.Errorf("consul new client: %w", err)
	}
	return &ConsulRegistry{
		client:  client,
		log:     log,
		selfID:  uuid.New().String(),
		ttlSecs: 10,
	}, nil
}

func (r *ConsulRegistry) Register(ctx context.Context, inst *ServiceInstance) error {
	svcID := inst.ID
	if svcID == "" {
		svcID = uuid.New().String()
	}
	r.selfID = svcID

	registration := &api.AgentServiceRegistration{
		ID:      svcID,
		Name:    inst.Name,
		Port:    0, // determined by httpAddr
		Address: "",
		Meta: map[string]string{
			"kind":    inst.Kind,
			"version": inst.Version,
		},
		Check: &api.AgentServiceCheck{
			TTL:                            fmt.Sprintf("%ds", r.ttlSecs),
			DeregisterCriticalServiceAfter: "30s",
		},
	}

	// Parse host:port from HTTPAddr (e.g. "localhost:8080").
	// For now store httpAddr as a tag so health checks can find the target.
	registration.Check.HTTP = fmt.Sprintf("http://%s/healthz", inst.HTTPAddr)
	// Extract port from HTTPAddr
	// registration.Port is int; AgentServiceRegistration uses separate fields.

	// Re-parse properly: split host and port from HTTPAddr.
	// HTTPAddr format: "[host:]port".
	// Use HostIP and Port instead.
	registration.Address = inst.HTTPAddr // will be split below
	// Actually we need a different approach since HTTPAddr might be ":8080".
	// Use a tag to carry the full address and parse in check.
	registration.Tags = []string{
		fmt.Sprintf("httpaddr=%s", inst.HTTPAddr),
		fmt.Sprintf("kind=%s", inst.Kind),
		fmt.Sprintf("version=%s", inst.Version),
	}
	// HTTP check needs the actual host:port, not just address.
	// Let's use the HTTPAddr directly for the check.
	registration.Check.HTTP = fmt.Sprintf("http://%s/healthz", inst.HTTPAddr)

	// AgentServiceRegistration port field is int; set to parsed port.
	// But we don't have a separate port field cleanly — the Address+Port is for DNS queries.
	// For HTTP health checks the URL already has the full address, so we don't need Port.
	registration.Port = 0

	if err := r.client.Agent().ServiceRegister(registration); err != nil {
		return fmt.Errorf("consul register: %w", err)
	}

	r.log.Info("consul registered", "id", svcID, "name", inst.Name, "addr", inst.HTTPAddr)

	// Start background TTL refresh.
	go r.refreshTTLLoop(svcID)

	return nil
}

func (r *ConsulRegistry) refreshTTLLoop(svcID string) {
	ticker := time.NewTicker(time.Duration(r.ttlSecs/2) * time.Second)
	defer ticker.Stop()
	for range ticker.C {
		if err := r.client.Agent().PassTTL("service:"+svcID, "ok"); err != nil {
			r.log.Warn("consul ttl refresh failed", "id", svcID, "error", err)
		}
	}
}

func (r *ConsulRegistry) Deregister(ctx context.Context, serviceID string) error {
	if err := r.client.Agent().ServiceDeregister(serviceID); err != nil {
		return fmt.Errorf("consul deregister: %w", err)
	}
	r.log.Info("consul deregistered", "id", serviceID)
	return nil
}

func (r *ConsulRegistry) Discover(ctx context.Context, serviceName string) ([]*ServiceInstance, error) {
	svcs, err := r.client.Agent().ServicesWithFilter(fmt.Sprintf(`Service == "%s"`, serviceName))
	if err != nil {
		return nil, fmt.Errorf("consul discover: %w", err)
	}
	var result []*ServiceInstance
	for id, svc := range svcs {
		httpAddr := ""
		for _, tag := range svc.Tags {
			if len(tag) > 9 && tag[:9] == "httpaddr=" {
				httpAddr = tag[9:]
				break
			}
		}
		kind := ""
		for _, tag := range svc.Tags {
			if len(tag) > 5 && tag[:5] == "kind=" {
				kind = tag[5:]
				break
			}
		}
		version := ""
		for _, tag := range svc.Tags {
			if len(tag) > 8 && tag[:8] == "version=" {
				version = tag[8:]
				break
			}
		}
		result = append(result, &ServiceInstance{
			ID:       id,
			Name:     svc.Service,
			Kind:     kind,
			Version:  version,
			HTTPAddr: httpAddr,
			Status:   "healthy",
		})
	}
	return result, nil
}

func (r *ConsulRegistry) HealthCheck(ctx context.Context, serviceID string, status string) error {
	switch status {
	case "healthy":
		return r.client.Agent().PassTTL("service:"+serviceID, status)
	case "degraded":
		return r.client.Agent().WarnTTL("service:"+serviceID, status)
	case "unhealthy":
		return r.client.Agent().FailTTL("service:"+serviceID, status)
	default:
		return r.client.Agent().PassTTL("service:"+serviceID, status)
	}
}

// ConsulDiscovery is a discovery-only client (read-only, no registration).
type ConsulDiscovery struct {
	client *api.Client
}

func NewConsulDiscovery(addr string) (*ConsulDiscovery, error) {
	cfg := api.DefaultConfig()
	cfg.Address = addr
	client, err := api.NewClient(cfg)
	if err != nil {
		return nil, fmt.Errorf("consul discovery client: %w", err)
	}
	return &ConsulDiscovery{client: client}, nil
}

func (d *ConsulDiscovery) Discover(ctx context.Context, name string) ([]*ServiceInstance, error) {
	svcs, err := d.client.Agent().ServicesWithFilter(fmt.Sprintf(`Service == "%s"`, name))
	if err != nil {
		return nil, fmt.Errorf("consul discover: %w", err)
	}
	var result []*ServiceInstance
	for id, svc := range svcs {
		httpAddr := ""
		kind := ""
		version := ""
		for _, tag := range svc.Tags {
			if len(tag) > 9 && tag[:9] == "httpaddr=" {
				httpAddr = tag[9:]
			}
			if len(tag) > 5 && tag[:5] == "kind=" {
				kind = tag[5:]
			}
			if len(tag) > 8 && tag[:8] == "version=" {
				version = tag[8:]
			}
		}
		result = append(result, &ServiceInstance{
			ID:       id,
			Name:     svc.Service,
			Kind:     kind,
			Version:  version,
			HTTPAddr: httpAddr,
			Status:   "healthy",
		})
	}
	return result, nil
}

// --- Mock registry for testing / no-op environments ---

type MockRegistry struct {
	mu    sync.Mutex
	svcs  map[string]*ServiceInstance
	log   *slog.Logger
}

func NewMockRegistry(log *slog.Logger) *MockRegistry {
	return &MockRegistry{svcs: make(map[string]*ServiceInstance), log: log}
}

func (m *MockRegistry) Register(ctx context.Context, inst *ServiceInstance) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if inst.ID == "" {
		inst.ID = uuid.New().String()
	}
	cp := *inst
	m.svcs[inst.ID] = &cp
	m.log.Info("mock registered", "id", inst.ID, "name", inst.Name)
	return nil
}

func (m *MockRegistry) Deregister(ctx context.Context, serviceID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.svcs, serviceID)
	return nil
}

func (m *MockRegistry) Discover(ctx context.Context, serviceName string) ([]*ServiceInstance, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var result []*ServiceInstance
	for _, svc := range m.svcs {
		if svc.Name == serviceName {
			cp := *svc
			result = append(result, &cp)
		}
	}
	return result, nil
}

func (m *MockRegistry) HealthCheck(ctx context.Context, serviceID string, status string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if svc, ok := m.svcs[serviceID]; ok {
		svc.Status = status
	}
	return nil
}
