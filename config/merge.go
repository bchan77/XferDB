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

// ResolveTransferConfig applies the precedence rules:
//
//	CLI flag > env var > config file > built-in default
//
// to every field of a TransferConfig.
//
// builtInDefault is the floor used when nothing else provides a value.
func ResolveTransferConfig(loader *Loader, builtInDefault adapters.TransferConfig) adapters.TransferConfig {
	out := builtInDefault
	if loader != nil {
		d := loader.Defaults()
		if d.BatchSize > 0 {
			out.BatchSize = d.BatchSize
		}
		if d.TableWorkers > 0 {
			out.TableWorkers = d.TableWorkers
		}
		if d.SegmentWorkers > 0 {
			out.SegmentWorkers = d.SegmentWorkers
		}
		if d.BulkCopy {
			out.BulkCopy = true
		}
		if d.AsyncPipeline {
			out.AsyncPipeline = true
		}
		if d.OnError != "" {
			out.OnError = d.OnError
		}
		if len(d.Tables) > 0 {
			out.Tables = d.Tables
		}
	}

	// Env vars override file/default values. Use tri-state so "false" actually disables.
	if v, set := envTriState("BULK_COPY"); set {
		out.BulkCopy = v
	}
	if v, set := envTriState("ASYNC_PIPELINE"); set {
		out.AsyncPipeline = v
	}
	if v, set := envInt("BATCH_SIZE"); set && v > 0 {
		out.BatchSize = v
	}
	if v, set := envInt("TABLE_WORKERS"); set && v > 0 {
		out.TableWorkers = v
	}
	if v, set := envInt("SEGMENT_WORKERS"); set && v > 0 {
		out.SegmentWorkers = v
	}
	return out
}
