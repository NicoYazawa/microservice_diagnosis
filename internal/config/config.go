// Package config provides service configuration loading (YAML + environment variable override).
package config

import (
	"fmt"
	"strings"

	"github.com/spf13/viper"
)

// Service holds the base service configuration.
type Service struct {
	Name     string `mapstructure:"name"`
	HTTPAddr string `mapstructure:"http_addr"`
	GRPCAddr string `mapstructure:"grpc_addr"` // optional; if set, gRPC-gateway reverse proxy is mounted
}

// Log holds the logger configuration.
type Log struct {
	Level  string `mapstructure:"level"`  // debug / info / warn / error
	Format string `mapstructure:"format"` // json / text
}

// Bus holds the Kafka message bus configuration (M2).
type Bus struct {
	Brokers []string `mapstructure:"brokers"`
}

// Database holds the PostgreSQL connection configuration.
type Database struct {
	URL string `mapstructure:"url"` // e.g. "postgres://user:pass@localhost:5432/mfdh?sslmode=disable"
}

// Consul holds the Consul service registry configuration.
type Consul struct {
	Addr string `mapstructure:"addr"` // e.g. "localhost:8500"
}

// Fix holds fix execution configuration (M6).
type Fix struct {
	AutoExecute bool   `mapstructure:"auto_execute"` // if true, execute fix actions automatically without approval
	Mode       string `mapstructure:"mode"`          // noop / k8s / cloud
	Kubeconfig  string `mapstructure:"kubeconfig"`   // path to kubeconfig for k8s mode
}

// Approval holds approval gate configuration (M6).
type Approval struct {
	Mode     string `mapstructure:"mode"`      // noop / webhook
	Callback string `mapstructure:"callback"` // webhook URL for external approval systems
}

// Notify holds notification configuration (M6).
type Notify struct {
	IncidentNotifier string `mapstructure:"incident_notifier"` // noop / jira / pagerduty
	JiraBaseURL       string `mapstructure:"jira_base_url"`
	JiraAuthToken     string `mapstructure:"jira_auth_token"`
	JiraProjectKey    string `mapstructure:"jira_project_key"`
	PDRoutingKey      string `mapstructure:"pagerduty_routing_key"`
	WebhookEnabled    bool   `mapstructure:"webhook_enabled"`
}

// Config holds the common service configuration.
type Config struct {
	Service  Service  `mapstructure:"service"`
	Log      Log      `mapstructure:"log"`
	Bus      Bus      `mapstructure:"bus"`
	Database Database `mapstructure:"database"`
	Consul   Consul   `mapstructure:"consul"`
	Fix      Fix      `mapstructure:"fix"`
	Approval Approval `mapstructure:"approval"`
	Notify   Notify   `mapstructure:"notify"`
}

// Load loads YAML configuration from path.
// Supports environment variable overrides: prefix MFDH_, dots become underscores, e.g. MFDH_LOG_LEVEL=debug.
func Load(path string) (*Config, error) {
	v := viper.New()
	v.SetConfigFile(path)

	v.SetDefault("service.http_addr", ":8080")
	v.SetDefault("log.level", "info")
	v.SetDefault("log.format", "json")
	v.SetDefault("bus.brokers", []string{"localhost:29092"})
	v.SetDefault("database.url", "postgres://postgres:postgres@localhost:5432/mfdh?sslmode=disable")
	v.SetDefault("consul.addr", "localhost:8500")

	v.SetEnvPrefix("MFDH")
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	v.AutomaticEnv()

	if err := v.ReadInConfig(); err != nil {
		return nil, fmt.Errorf("read config %s: %w", path, err)
	}

	var cfg Config
	if err := v.Unmarshal(&cfg); err != nil {
		return nil, fmt.Errorf("parse config %s: %w", path, err)
	}
	return &cfg, nil
}
