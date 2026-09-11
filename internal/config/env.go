package config

import (
	"fmt"
	"os"
	"strings"
)

// envPrefix is the namespace used for XFERDB_* environment variables.
// Bare env names like $HOME and $DB_PROD_DSN are also supported inside DSN strings.
const envPrefix = "XFERDB_"

// expandEnv walks every DSN-bearing string field in the File and substitutes
// ${VAR} and ${VAR:-default} references with values from os.Getenv.
//
// Bare $VAR references are also supported but only when the variable exists
// in the environment; otherwise they are left untouched so the user sees the
// original string rather than an empty DSN.
//
// This intentionally does NOT walk every string in the file — only fields that
// hold DSNs or other secrets. Documented in examples/xferdb.yaml.
func expandEnv(f *File) error {
	if f == nil {
		return nil
	}
	for i := range f.Projects {
		p := &f.Projects[i]
		var err error
		if p.Source.DSN, err = expand(p.Source.DSN); err != nil {
			return fmt.Errorf("projects[%d].source.dsn: %w", i, err)
		}
		if p.Target.DSN, err = expand(p.Target.DSN); err != nil {
			return fmt.Errorf("projects[%d].target.dsn: %w", i, err)
		}
		// Username / Password are also expanded; users sometimes split DSNs into fields.
		if p.Source.Username, err = expand(p.Source.Username); err != nil {
			return fmt.Errorf("projects[%d].source.username: %w", i, err)
		}
		if p.Source.Password, err = expand(p.Source.Password); err != nil {
			return fmt.Errorf("projects[%d].source.password: %w", i, err)
		}
		if p.Target.Username, err = expand(p.Target.Username); err != nil {
			return fmt.Errorf("projects[%d].target.username: %w", i, err)
		}
		if p.Target.Password, err = expand(p.Target.Password); err != nil {
			return fmt.Errorf("projects[%d].target.password: %w", i, err)
		}
	}
	return nil
}

// expand returns s with ${VAR} and ${VAR:-default} substitutions applied.
// A missing variable with no default is an error.
func expand(s string) (string, error) {
	if !strings.Contains(s, "$") {
		return s, nil
	}
	var firstErr error
	out := os.Expand(s, func(name string) string {
		// os.Expand doesn't understand ${VAR:-default}; split manually.
		def := ""
		if idx := strings.Index(name, ":-"); idx >= 0 {
			name, def = name[:idx], name[idx+2:]
		}
		if v, ok := os.LookupEnv(name); ok {
			return v
		}
		if def != "" {
			return def
		}
		if firstErr == nil {
			firstErr = fmt.Errorf("environment variable %q is not set (and no default provided)", name)
		}
		return ""
	})
	if firstErr != nil {
		return "", firstErr
	}
	return out, nil
}
