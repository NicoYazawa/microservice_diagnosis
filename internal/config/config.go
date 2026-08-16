// Package config 提供服务配置加载能力（YAML + 环境变量覆盖）。
package config

import (
	"fmt"
	"strings"

	"github.com/spf13/viper"
)

// Service 服务基础配置。
type Service struct {
	Name     string `mapstructure:"name"`
	HTTPAddr string `mapstructure:"http_addr"`
}

// Log 日志配置。
type Log struct {
	Level  string `mapstructure:"level"`  // debug / info / warn / error
	Format string `mapstructure:"format"` // json / text
}

// Config 服务通用配置。后续里程碑按需扩展子结构（bus/store/workflow/llm 等）。
type Config struct {
	Service Service `mapstructure:"service"`
	Log     Log     `mapstructure:"log"`
}

// Load 从 path 加载 YAML 配置。
// 支持环境变量覆盖：前缀 MFDH_，层级点号转下划线，例如 MFDH_LOG_LEVEL=debug。
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
