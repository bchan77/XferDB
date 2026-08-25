package config

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

// Config holds the full migration configuration
type Config struct {
	Source  DBConfig  `yaml:"source"`
	Target  DBConfig  `yaml:"target"`
	Transfer TransferConfig `yaml:"transfer"`
}

// DBConfig holds database connection configuration
type DBConfig struct {
	Type     string `yaml:"type"`
	Host     string `yaml:"host"`
	Port     int    `yaml:"port"`
	Database string `yaml:"database"`
	User     string `yaml:"user"`
	Password string `yaml:"password"`
	SSLMode  string `yaml:"ssl_mode"`
}

// TransferConfig holds transfer behavior settings
type TransferConfig struct {
	BatchSize int  `yaml:"batch_size"`
	Workers   int  `yaml:"workers"`
	Validate  bool `yaml:"validate"`
}

// Load reads and parses a YAML config file
func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("reading config file: %w", err)
	}

	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("parsing YAML: %w", err)
	}

	// Expand env vars
	cfg.Source.Password = expandEnv(cfg.Source.Password)
	cfg.Target.Password = expandEnv(cfg.Target.Password)

	return &cfg, nil
}

func expandEnv(s string) string {
	if len(s) > 1 && s[0] == '$' {
		return os.Getenv(s[1:])
	}
	return s
}
