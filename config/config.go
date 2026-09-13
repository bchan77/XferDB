// Package config provides loading, validation, and precedence-aware merging of
// XferDB's structured configuration files (xferdb.yaml / xferdb.toml / xferdb.json).
//
// Resolution precedence (highest wins):
//  1. CLI flag (e.g. --table-workers=4)
//  2. Environment variable (XFERDB_TABLE_WORKERS)
//  3. Config file value
//  4. Built-in default
//
// DSN strings in config files may reference environment variables using
// ${VAR} or ${VAR:-default} syntax; these are expanded when the file is loaded.
package config

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"gitea.homelab.local/nextdevops/XferDB/adapters"
	"gitea.homelab.local/nextdevops/XferDB/registry"
)

// File is the top-level structure of an XferDB config file.
//
// Mirrors `xferdb.yaml` in examples/ and is the canonical reference for users.
// All sections are optional; omitting a section falls back to the existing
// flag/env/default behaviour.
type File struct {
	// Server configures the `xferdb server` subcommand.
	Server ServerConfig `yaml:"server" json:"server"`

	// Defaults applies to every migration that does not override the value.
	Defaults Defaults `yaml:"defaults" json:"defaults"`

	// Projects is the list of named migration projects defined inline.
	// Projects stored in state.db still take precedence at lookup time —
	// inline projects are the recommended path going forward.
	Projects []Project `yaml:"projects" json:"projects"`
}

// ServerConfig mirrors the flags on `xferdb server`.
type ServerConfig struct {
	Addr     string `yaml:"addr" json:"addr"`
	LogLevel string `yaml:"log_level" json:"log_level"`
	LogFile  string `yaml:"log_file" json:"log_file"`
	StateDir string `yaml:"state_dir" json:"state_dir"`
}

// Defaults supplies defaults for fields that the `migrate` command would
// otherwise take from flags. Anything the user sets on the CLI overrides.
type Defaults struct {
	TableWorkers   int             `yaml:"table_workers" json:"table_workers"`
	SegmentWorkers int             `yaml:"segment_workers" json:"segment_workers"`
	BatchSize      int             `yaml:"batch_size" json:"batch_size"`
	BulkCopy       bool            `yaml:"bulk_copy" json:"bulk_copy"`
	AsyncPipeline  bool            `yaml:"async_pipeline" json:"async_pipeline"`
	OnError        adapters.ErrorPolicy `yaml:"on_error" json:"on_error"`
	Tables         []string        `yaml:"tables" json:"tables"`
}

// Project is one migration project defined inline in the config file.
type Project struct {
	Name        string                  `yaml:"name" json:"name"`
	Description string                  `yaml:"description" json:"description"`
	Source      adapters.ConnectionConfig `yaml:"source" json:"source"`
	Target      adapters.ConnectionConfig `yaml:"target" json:"target"`
	Transfer    adapters.TransferConfig   `yaml:"transfer" json:"transfer"`
}

// Loader is the entry point used by the CLI and any future programmatic callers.
// One Loader is safe for concurrent use after Load returns.
type Loader struct {
	path string
	raw  *File
}

// Path returns the resolved absolute path of the loaded config file.
// Returns "" when no file was found or loaded.
func (l *Loader) Path() string {
	if l == nil {
		return ""
	}
	return l.path
}

// Defaults returns the Defaults block of the loaded file, or the zero value
// if no file was loaded.
func (l *Loader) Defaults() Defaults {
	if l == nil || l.raw == nil {
		return Defaults{}
	}
	return l.raw.Defaults
}

// Server returns the ServerConfig block, or zero value.
func (l *Loader) Server() ServerConfig {
	if l == nil || l.raw == nil {
		return ServerConfig{}
	}
	return l.raw.Server
}

// Projects returns the inline projects defined in the file.
func (l *Loader) Projects() []Project {
	if l == nil || l.raw == nil {
		return nil
	}
	return l.raw.Projects
}

// FindProject looks up a named project in the inline list.
// Returns nil when not present.
func (l *Loader) FindProject(name string) *Project {
	if l == nil || l.raw == nil {
		return nil
	}
	for i := range l.raw.Projects {
		if l.raw.Projects[i].Name == name {
			return &l.raw.Projects[i]
		}
	}
	return nil
}

// Load reads a config file from the given path, parses it, expands env-var
// references inside DSN strings, and validates the structure.
//
// If path is "" Load searches for a config file in the standard locations:
// $XFERDB_CONFIG, then ./xferdb.{yaml,toml,json}, then ~/.xferdb/config.yaml.
//
// Missing config files are not an error — the operator may be using flags only.
// In that case the returned *Loader has Path()=="" and all accessors return zero values.
func Load(path string) (*Loader, error) {
	resolved, err := locate(path)
	if err != nil {
		return nil, err
	}
	if resolved == "" {
		// No config file found; return an empty loader so callers can branch on Path().
		return &Loader{}, nil
	}

	data, err := os.ReadFile(resolved)
	if err != nil {
		return nil, fmt.Errorf("read config %s: %w", resolved, err)
	}

	raw, err := parse(resolved, data)
	if err != nil {
		return nil, err
	}

	if err := expandEnv(raw); err != nil {
		return nil, fmt.Errorf("expand env vars in %s: %w", resolved, err)
	}

	inferTypes(raw)

	if err := raw.Validate(); err != nil {
		return nil, fmt.Errorf("invalid config %s: %w", resolved, err)
	}

	return &Loader{path: resolved, raw: raw}, nil
}

// locate returns the absolute path of the config file to load, or "" if none
// is configured and none is found in the standard search locations.
func locate(path string) (string, error) {
	if path != "" {
		abs, err := filepath.Abs(path)
		if err != nil {
			return "", fmt.Errorf("resolve config path %s: %w", path, err)
		}
		if _, err := os.Stat(abs); err != nil {
			if errors.Is(err, os.ErrNotExist) {
				return "", fmt.Errorf("config file not found: %s", abs)
			}
			return "", fmt.Errorf("stat config %s: %w", abs, err)
		}
		return abs, nil
	}

	if env := os.Getenv("XFERDB_CONFIG"); env != "" {
		abs, err := filepath.Abs(env)
		if err != nil {
			return "", fmt.Errorf("resolve $XFERDB_CONFIG: %w", err)
		}
		if _, err := os.Stat(abs); err == nil {
			return abs, nil
		}
	}

	cwd, _ := os.Getwd()
	for _, ext := range []string{".yaml", ".yml", ".toml", ".json"} {
		candidate := filepath.Join(cwd, "xferdb"+ext)
		if _, err := os.Stat(candidate); err == nil {
			abs, _ := filepath.Abs(candidate)
			return abs, nil
		}
	}

	home, err := os.UserHomeDir()
	if err == nil {
		for _, ext := range []string{".yaml", ".yml", ".toml", ".json"} {
			candidate := filepath.Join(home, ".xferdb", "config"+ext)
			if _, err := os.Stat(candidate); err == nil {
				return candidate, nil
			}
		}
	}

	return "", nil
}

// Validate enforces structural invariants on a parsed File. It is called
// automatically by Load, but is exported so callers constructing a File
// programmatically (e.g. `xferdb config init`) can validate before writing.
func (f *File) Validate() error {
	if f == nil {
		return errors.New("nil config")
	}
	var errs []string

	// Server
	if f.Server.Addr != "" && !strings.HasPrefix(f.Server.Addr, ":") && !strings.Contains(f.Server.Addr, ":") {
		errs = append(errs, fmt.Sprintf("server.addr %q is not a valid listen address (expected :PORT or HOST:PORT)", f.Server.Addr))
	}
	switch strings.ToLower(f.Server.LogLevel) {
	case "", "debug", "info", "warn", "error":
	default:
		errs = append(errs, fmt.Sprintf("server.log_level %q must be one of: debug, info, warn, error", f.Server.LogLevel))
	}

	// Defaults
	if f.Defaults.TableWorkers < 0 {
		errs = append(errs, "defaults.table_workers must be >= 0")
	}
	if f.Defaults.SegmentWorkers < 0 {
		errs = append(errs, "defaults.segment_workers must be >= 0")
	}
	if f.Defaults.BatchSize < 0 {
		errs = append(errs, "defaults.batch_size must be >= 0")
	}
	switch f.Defaults.OnError {
	case "", adapters.ErrorPolicyAbort, adapters.ErrorPolicySkip:
	default:
		errs = append(errs, fmt.Sprintf("defaults.on_error %q must be one of: abort, skip", f.Defaults.OnError))
	}

	// Projects
	seen := make(map[string]bool, len(f.Projects))
	for i, p := range f.Projects {
		prefix := fmt.Sprintf("projects[%d]", i)
		if p.Name == "" {
			errs = append(errs, prefix+".name is required")
			continue
		}
		if seen[p.Name] {
			errs = append(errs, fmt.Sprintf("%s.name %q is duplicated", prefix, p.Name))
		}
		seen[p.Name] = true

		if err := validateConnConfig(prefix+".source", p.Source); err != nil {
			errs = append(errs, err.Error())
		}
		if err := validateConnConfig(prefix+".target", p.Target); err != nil {
			errs = append(errs, err.Error())
		}
		if err := validateTransferConfig(prefix+".transfer", p.Transfer); err != nil {
			errs = append(errs, err.Error())
		}
	}

	if len(errs) > 0 {
		return errors.New(strings.Join(errs, "; "))
	}
	return nil
}

func validateConnConfig(prefix string, c adapters.ConnectionConfig) error {
	switch c.Type {
	case "postgres", "postgresql", "mysql", "mongodb", "sqlite", "sqlite3":
	case "":
		// DSN-only is allowed; type is then inferred from DSN prefix.
	default:
		return fmt.Errorf("%s.type %q is not a supported adapter (postgres, mysql, mongodb, sqlite)", prefix, c.Type)
	}
	return nil
}

// inferTypes fills in empty Type fields by extracting the scheme from the DSN.
// Called after expandEnv so environment variables in DSNs are already resolved.
func inferTypes(f *File) {
	if f == nil {
		return
	}
	for i := range f.Projects {
		p := &f.Projects[i]
		if p.Source.Type == "" && p.Source.DSN != "" {
			p.Source.Type = registry.InferTypeFromDSN(p.Source.DSN)
		}
		if p.Target.Type == "" && p.Target.DSN != "" {
			p.Target.Type = registry.InferTypeFromDSN(p.Target.DSN)
		}
	}
}

func validateTransferConfig(prefix string, t adapters.TransferConfig) error {
	if t.TableWorkers < 0 {
		return fmt.Errorf("%s.table_workers must be >= 0", prefix)
	}
	if t.SegmentWorkers < 0 {
		return fmt.Errorf("%s.segment_workers must be >= 0", prefix)
	}
	if t.BatchSize < 0 {
		return fmt.Errorf("%s.batch_size must be >= 0", prefix)
	}
	switch t.OnError {
	case "", adapters.ErrorPolicyAbort, adapters.ErrorPolicySkip:
	default:
		return fmt.Errorf("%s.on_error %q must be one of: abort, skip", prefix, t.OnError)
	}
	return nil
}
