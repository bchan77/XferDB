package config

import (
	"fmt"
	"os"
	"strings"
)

// expandEnv walks every DSN-bearing string field in the File and substitutes
// ${VAR} and ${VAR:-default} references with values from os.Getenv.
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
//
// We implement this by hand instead of using os.Expand because the latter does
// not understand the ${VAR:-default} syntax — os.Expand passes the whole
// "VAR:-default" to the mapping function, which we then split on ":-".
//
// Semantics:
//   - ${VAR}        — value of $VAR; error if unset and no default provided.
//   - ${VAR:-def}   — value of $VAR if set (and non-empty), else "def".
//   - $VAR          — value of $VAR; empty string if unset (no error).
//
// The distinction between ${VAR} and $VAR is intentional: bracketed references
// are an explicit "I require this" assertion, so we fail loudly; bare references
// degrade gracefully to an empty string.
func expand(s string) (string, error) {
	if !strings.Contains(s, "$") {
		return s, nil
	}
	var firstErr error
	// Walk the string character-by-character; treat $ as the start of a substitution.
	var out strings.Builder
	out.Grow(len(s))
	i := 0
	for i < len(s) {
		c := s[i]
		if c != '$' {
			out.WriteByte(c)
			i++
			continue
		}
		// Parse the substitution.
		if i+1 >= len(s) {
			out.WriteByte('$')
			i++
			continue
		}
		switch s[i+1] {
		case '{':
			// Bracketed: ${VAR} or ${VAR:-default}
			end := strings.IndexByte(s[i+2:], '}')
			if end < 0 {
				// Unterminated ${ — treat as literal so the error path is clear.
				out.WriteString(s[i:])
				i = len(s)
				continue
			}
			expr := s[i+2 : i+2+end]
			val, err := substituteBracketed(expr)
			if err != nil {
				if firstErr == nil {
					firstErr = err
				}
				// On error, drop the substitution to empty string so the DSN is still usable
				// and the operator sees one clear first error rather than partial output.
			}
			out.WriteString(val)
			i = i + 2 + end + 1
		default:
			// Bare $VAR: read identifier.
			j := i + 1
			for j < len(s) && (isIdentByte(s[j])) {
				j++
			}
			if j == i+1 {
				// Lone '$' followed by non-identifier (e.g. "$$", "$ "). Pass through.
				out.WriteByte('$')
				i++
				continue
			}
			name := s[i+1 : j]
			// Bare reference: empty string when unset; no error.
			out.WriteString(os.Getenv(name))
			i = j
		}
	}
	if firstErr != nil {
		return "", firstErr
	}
	return out.String(), nil
}

// substituteBracketed resolves a ${VAR} or ${VAR:-default} expression.
func substituteBracketed(expr string) (string, error) {
	name, def, hasDef := strings.Cut(expr, ":-")
	if v, ok := os.LookupEnv(name); ok && v != "" {
		return v, nil
	}
	if hasDef {
		return def, nil
	}
	return "", fmt.Errorf("environment variable %q is not set (and no default provided)", name)
}

func isIdentByte(b byte) bool {
	return (b >= 'a' && b <= 'z') || (b >= 'A' && b <= 'Z') ||
		(b >= '0' && b <= '9') || b == '_'
}
