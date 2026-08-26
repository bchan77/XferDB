// Package registry maps adapter type strings to concrete implementations
// and parses connection strings into ConnectionConfig values.
// It is a separate package to avoid the import cycle that would arise if
// adapters/ imported its own sub-packages.
package registry

import (
	"fmt"
	"net/url"
	"strconv"
	"strings"

	"gitea.homelab.local/nextdevops/XferDB/adapters"
	"gitea.homelab.local/nextdevops/XferDB/adapters/mysql"
	"gitea.homelab.local/nextdevops/XferDB/adapters/postgres"
	"gitea.homelab.local/nextdevops/XferDB/adapters/sqlite"
)

var sourceFactories = map[string]func() adapters.SourceAdapter{
	"postgres": postgres.NewSource,
	"mysql":    mysql.NewSource,
	"sqlite":   sqlite.NewSource,
}

var targetFactories = map[string]func() adapters.TargetAdapter{
	"postgres": postgres.NewTarget,
	"mysql":    mysql.NewTarget,
	"sqlite":   sqlite.NewTarget,
}

// NewSource returns a SourceAdapter for the given adapter type string.
func NewSource(adapterType string) (adapters.SourceAdapter, error) {
	factory, ok := sourceFactories[adapterType]
	if !ok {
		return nil, fmt.Errorf("unsupported source adapter %q (supported: %s)",
			adapterType, supportedTypes())
	}
	return factory(), nil
}

// NewTarget returns a TargetAdapter for the given adapter type string.
func NewTarget(adapterType string) (adapters.TargetAdapter, error) {
	factory, ok := targetFactories[adapterType]
	if !ok {
		return nil, fmt.Errorf("unsupported target adapter %q (supported: %s)",
			adapterType, supportedTypes())
	}
	return factory(), nil
}

// ParseConnectionString parses a DSN into a ConnectionConfig.
//
// Supported formats:
//
//	postgres://user:pass@host:5432/dbname?sslmode=require
//	mysql://user:pass@host:3306/dbname
//	sqlite:///absolute/path/to/file.db
func ParseConnectionString(dsn string) (adapters.ConnectionConfig, error) {
	u, err := url.Parse(dsn)
	if err != nil {
		return adapters.ConnectionConfig{}, fmt.Errorf("invalid connection string: %w", err)
	}

	scheme := strings.ToLower(u.Scheme)
	if _, ok := sourceFactories[scheme]; !ok {
		return adapters.ConnectionConfig{}, fmt.Errorf("unknown scheme %q in connection string", scheme)
	}

	cfg := adapters.ConnectionConfig{
		Type: scheme,
		Host: u.Hostname(),
	}

	if u.User != nil {
		cfg.Username = u.User.Username()
		cfg.Password, _ = u.User.Password()
	}

	if portStr := u.Port(); portStr != "" {
		port, err := strconv.Atoi(portStr)
		if err != nil {
			return adapters.ConnectionConfig{}, fmt.Errorf("invalid port %q: %w", portStr, err)
		}
		cfg.Port = port
	}

	if scheme == "sqlite" {
		// sqlite:///path/to/file.db → u.Path is /path/to/file.db
		cfg.Database = u.Path
		cfg.DSN = u.Path
	} else {
		cfg.Database = strings.TrimPrefix(u.Path, "/")
		cfg.DSN = dsn
		if sslMode := u.Query().Get("sslmode"); sslMode != "" {
			cfg.SSLMode = sslMode
		}
	}

	return cfg, nil
}

// SupportedAdapters returns the registered adapter type names.
func SupportedAdapters() []string {
	seen := map[string]bool{}
	var types []string
	for k := range sourceFactories {
		if !seen[k] {
			seen[k] = true
			types = append(types, k)
		}
	}
	return types
}

func supportedTypes() string {
	return strings.Join(SupportedAdapters(), ", ")
}
