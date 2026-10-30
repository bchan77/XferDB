// Package registry maps adapter type strings to concrete implementations
// and parses connection strings into ConnectionConfig values.
// It is a separate package to avoid the import cycle that would arise if
// adapters/ imported its own sub-packages.
package registry

import (
	"context"
	"fmt"
	"net/url"
	"strconv"
	"strings"

	"gitea.homelab.local/nextdevops/XferDB/adapters"
	mongoadapter "gitea.homelab.local/nextdevops/XferDB/adapters/mongo"
	"gitea.homelab.local/nextdevops/XferDB/adapters/mysql"
	"gitea.homelab.local/nextdevops/XferDB/adapters/postgres"
	"gitea.homelab.local/nextdevops/XferDB/adapters/sqlite"
	"gitea.homelab.local/nextdevops/XferDB/cdc"
)

var sourceFactories = map[string]func() adapters.SourceAdapter{
	"postgres": postgres.NewSource,
	"mysql":    mysql.NewSource,
	"sqlite":   sqlite.NewSource,
	"mongodb":  mongoadapter.NewSource,
}

var targetFactories = map[string]func() adapters.TargetAdapter{
	"postgres": postgres.NewTarget,
	"mysql":    mysql.NewTarget,
	"sqlite":   sqlite.NewTarget,
	// MongoDB is source-only for this feature; no mongo target adapter planned
}

// changeSourceFactories are the source types a live migration (#102) can
// stream changes from.
var changeSourceFactories = map[string]func(ctx context.Context, cfg adapters.ConnectionConfig, stream string) (cdc.ChangeSource, error){
	"postgres": func(ctx context.Context, cfg adapters.ConnectionConfig, stream string) (cdc.ChangeSource, error) {
		return postgres.NewChangeSource(ctx, cfg, stream)
	},
}

// SupportsLive reports whether a source type can be live-migrated.
func SupportsLive(sourceType string) bool {
	_, ok := changeSourceFactories[sourceType]
	return ok
}

// NewChangeSource connects a live migration's change stream to cfg.
func NewChangeSource(ctx context.Context, cfg adapters.ConnectionConfig, stream string) (cdc.ChangeSource, error) {
	factory, ok := changeSourceFactories[cfg.Type]
	if !ok {
		return nil, fmt.Errorf("live migration from %s isn't supported yet (supported: postgres)", cfg.Type)
	}
	return factory(ctx, cfg, stream)
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
//	mongodb://user:pass@host:27017/dbname?replicaSet=rs0&authSource=admin
//	mongodb+srv://user:pass@cluster.example.net/dbname
//
// MongoDB URIs are stored verbatim in ConnectionConfig.DSN and are never
// decomposed into individual fields. See parseMongoDBConnectionString for
// the rationale.
func ParseConnectionString(dsn string) (adapters.ConnectionConfig, error) {
	// MongoDB URIs must be detected before the generic factory-scheme check
	// because (a) mongodb+srv is not a registered type name and (b) these
	// URIs must not be decomposed — see parseMongoDBConnectionString.
	lower := strings.ToLower(dsn)
	if strings.HasPrefix(lower, "mongodb://") || strings.HasPrefix(lower, "mongodb+srv://") {
		return parseMongoDBConnectionString(dsn)
	}

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

// parseMongoDBConnectionString handles mongodb:// and mongodb+srv:// URIs.
//
// Why we do not decompose the URI:
//
// MongoDB connection strings carry options that have no equivalent field in
// ConnectionConfig: multiple hosts (replica sets), replicaSet, authSource,
// readPreference, write concern, and the mongodb+srv scheme which triggers a
// DNS SRV lookup inside the driver. Breaking the URI apart and reassembling it
// later would silently drop these options.
//
// The MongoDB Go driver accepts the full URI directly via
// options.Client().ApplyURI(uri), so the adapter reads cfg.DSN and passes it
// straight through — no reconstruction needed.
//
// We only extract Database from the URI path so that commands like
// `xferdb project list` can display a human-readable database name without
// exposing the full URI (which may contain credentials).
func parseMongoDBConnectionString(dsn string) (adapters.ConnectionConfig, error) {
	cfg := adapters.ConnectionConfig{
		Type: "mongodb",
		DSN:  dsn, // full URI passed to the driver unchanged via ApplyURI
	}

	// Extract database name from the path for display only. Not url.Parse:
	// it rejects userinfo the driver accepts, and the driver is what reports
	// a malformed URI, when it connects.
	cfg.Database = mongoadapter.DatabaseFromURI(dsn)

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

// InferTypeFromDSN extracts the adapter type from a DSN scheme.
// Returns an empty string if the DSN is empty or the scheme is unrecognized.
func InferTypeFromDSN(dsn string) string {
	if dsn == "" {
		return ""
	}
	lower := strings.ToLower(dsn)
	if strings.HasPrefix(lower, "mongodb://") || strings.HasPrefix(lower, "mongodb+srv://") {
		return "mongodb"
	}
	u, err := url.Parse(dsn)
	if err != nil {
		return ""
	}
	scheme := strings.ToLower(u.Scheme)
	if _, ok := sourceFactories[scheme]; ok {
		return scheme
	}
	return ""
}
