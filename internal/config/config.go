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
}

// Log holds the logger configuration.
type Log struct {
	Level  string `mapstructure:"level"`  // debug / info / warn / error
	Format string `mapstructure:"format"` // json / text
}

// Config holds the common service configuration. Future milestones will extend sub-structures (bus/store/workflow/llm etc.).
type Config struct {
	Service Service `mapstructure:"service"`
	Log     Log     `mapstructure:"log"`
}

// Load loads YAML configuration from path.
// Supports environment variable overrides: prefix MFDH_, dots become underscores, e.g. MFDH_LOG_LEVEL=debug.
func Load(path string) (*Config, error) {
	v := viper.New()
	v.SetConfigFile(path)

	v.SetDefault("service.http_addr", ":8080")
	v.SetDefault("log.level", "info")
	v.SetDefault("log.format", "json")

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
