package config

import (
	"encoding/json"
	"fmt"
	"strings"

	"gopkg.in/yaml.v3"
)

// parse dispatches to the right decoder based on file extension.
// JSON is also accepted as a fallback for users who prefer it.
// (TOML is on the v0.6.x roadmap but excluded from v0.6.0 to keep the
// dependency surface minimal — see examples/xferdb.toml for the equivalent
// YAML structure or convert with `toml2json`.)
func parse(path string, data []byte) (*File, error) {
	lower := strings.ToLower(path)
	switch {
	case strings.HasSuffix(lower, ".yaml"), strings.HasSuffix(lower, ".yml"):
		return parseYAML(data)
	case strings.HasSuffix(lower, ".json"):
		return parseJSON(data)
	case strings.HasSuffix(lower, ".toml"):
		return nil, fmt.Errorf("TOML config files are not supported in v0.6.0 — convert to YAML with `toml2json`/`yq` or upgrade to v0.6.1+")
	default:
		// Sniff: try YAML first (most permissive), then JSON.
		var f File
		if err := yaml.Unmarshal(data, &f); err == nil && looksLikeYAMLConfig(data) {
			return &f, nil
		}
		if err := json.Unmarshal(data, &f); err == nil {
			return &f, nil
		}
		return nil, fmt.Errorf("could not detect format for %s (use .yaml, .yml, or .json)", path)
	}
}

func parseYAML(data []byte) (*File, error) {
	var f File
	if err := yaml.Unmarshal(data, &f); err != nil {
		return nil, fmt.Errorf("parse yaml: %w", err)
	}
	return &f, nil
}

func parseJSON(data []byte) (*File, error) {
	var f File
	if err := json.Unmarshal(data, &f); err != nil {
		return nil, fmt.Errorf("parse json: %w", err)
	}
	return &f, nil
}

// looksLikeYAMLConfig returns true when the first non-empty line looks like
// a YAML key (no leading '{'), which is what we'd expect for our config shape.
func looksLikeYAMLConfig(data []byte) bool {
	for _, b := range data {
		switch b {
		case ' ', '\t', '\r', '\n':
			continue
		case '{':
			return false
		default:
			return true
		}
	}
	return false
}
