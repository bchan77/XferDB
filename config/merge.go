package config

import (
	"os"
	"strconv"
	"strings"

	"gitea.homelab.local/nextdevops/XferDB/adapters"
)

// envTriState reads XFERDB_NAME as a tri-state:
//
//   - Unset (or empty)  → return (false, false) — caller should use file/default value.
//   - "true"/"1"/"yes"/"on"  → return (true, true) — caller should use true.
//   - "false"/"0"/"no"/"off" → return (false, true) — caller should use false.
//
// The second return value is `set`: only when true should the caller override
// the file/default value. This avoids the bug where env_bool("false") returned
// false but didn't tell the caller to override a file-set true.
func envTriState(name string) (val, set bool) {
	v := strings.ToLower(strings.TrimSpace(os.Getenv("XFERDB_" + name)))
	if v == "" {
		return false, false
	}
	switch v {
	case "1", "true", "yes", "on":
		return true, true
	case "0", "false", "no", "off":
		return false, true
	}
	// Unrecognised value: don't override.
	return false, false
}

// envInt reads XFERDB_NAME as an int. Returns (n, true) when set+parseable, (0, false) otherwise.
func envInt(name string) (int, bool) {
	v := strings.TrimSpace(os.Getenv("XFERDB_" + name))
	if v == "" {
		return 0, false
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return 0, false
	}
	return n, true
}

// TransferSources records which TransferConfig fields a resolution took from
// the config file or an environment variable rather than the built-in
// default. A caller that starts a migration sends only these (plus its own
// flags), so that whatever the user didn't set comes from the project's saved
// transfer settings instead of a built-in default (#172).
type TransferSources struct {
	BatchSize      bool
	TableWorkers   bool
	SegmentWorkers bool
	BulkCopy       bool
	AsyncPipeline  bool
	Tables         bool
}

// ResolveTransferConfig applies the precedence rules:
//
//	CLI flag > env var > config file > built-in default
//
// to every field of a TransferConfig.
//
// builtInDefault is the floor used when nothing else provides a value.
func ResolveTransferConfig(loader *Loader, builtInDefault adapters.TransferConfig) adapters.TransferConfig {
	out, _ := ResolveTransferConfigSources(loader, builtInDefault)
	return out
}

// ResolveTransferConfigSources is ResolveTransferConfig, also reporting which
// fields came from the config file or an env var.
func ResolveTransferConfigSources(loader *Loader, builtInDefault adapters.TransferConfig) (adapters.TransferConfig, TransferSources) {
	out := builtInDefault
	var set TransferSources
	if loader != nil {
		d := loader.Defaults()
		if d.BatchSize > 0 {
			out.BatchSize, set.BatchSize = d.BatchSize, true
		}
		if d.TableWorkers > 0 {
			out.TableWorkers, set.TableWorkers = d.TableWorkers, true
		}
		if d.SegmentWorkers > 0 {
			out.SegmentWorkers, set.SegmentWorkers = d.SegmentWorkers, true
		}
		// A file's false is indistinguishable from an absent key, so only
		// true counts as set.
		if d.BulkCopy {
			out.BulkCopy, set.BulkCopy = true, true
		}
		if d.AsyncPipeline {
			out.AsyncPipeline, set.AsyncPipeline = true, true
		}
		if d.OnError != "" {
			out.OnError = d.OnError
		}
		if len(d.Tables) > 0 {
			out.Tables, set.Tables = d.Tables, true
		}
	}

	// Env vars override file/default values. Use tri-state so "false" actually disables.
	if v, ok := envTriState("BULK_COPY"); ok {
		out.BulkCopy, set.BulkCopy = v, true
	}
	if v, ok := envTriState("ASYNC_PIPELINE"); ok {
		out.AsyncPipeline, set.AsyncPipeline = v, true
	}
	if v, ok := envInt("BATCH_SIZE"); ok && v > 0 {
		out.BatchSize, set.BatchSize = v, true
	}
	if v, ok := envInt("TABLE_WORKERS"); ok && v > 0 {
		out.TableWorkers, set.TableWorkers = v, true
	}
	if v, ok := envInt("SEGMENT_WORKERS"); ok && v > 0 {
		out.SegmentWorkers, set.SegmentWorkers = v, true
	}
	return out, set
}
