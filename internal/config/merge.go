package config

import (
	"os"
	"strconv"
	"strings"

	"gitea.homelab.local/nextdevops/XferDB/adapters"
)

// ResolveTransferConfig applies the precedence rules:
//
//	CLI flag > env var > config file > built-in default
//
// to every field of a TransferConfig. flagSet tells ResolveTransferConfig
// whether the CLI value was explicitly set (true) or left at zero-value (false).
//
// The result is safe to use as the project's effective transfer settings.
//
// builtInDefault is used as the floor when nothing else provides a value
// (typically adapters.TransferConfig{BatchSize: 1000, TableWorkers: 1, OnError: ErrorPolicyAbort}).
func ResolveTransferConfig(loader *Loader, builtInDefault adapters.TransferConfig) adapters.TransferConfig {
	out := builtInDefault
	if loader != nil {
		d := loader.Defaults()
		// Project-level defaults are layered on top of built-in defaults
		// only when the field was at its zero value, preserving non-default
		// user intent (e.g. a negative BatchSize stays unset).
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
			out.BulkCopy = d.BulkCopy
		}
		if d.AsyncPipeline {
			out.AsyncPipeline = d.AsyncPipeline
		}
		if d.OnError != "" {
			out.OnError = d.OnError
		}
		if len(d.Tables) > 0 {
			out.Tables = d.Tables
		}
	}

	// Env vars override the file.
	if v := envInt("BATCH_SIZE"); v > 0 {
		out.BatchSize = v
	}
	if v := envInt("TABLE_WORKERS"); v > 0 {
		out.TableWorkers = v
	}
	if v := envInt("SEGMENT_WORKERS"); v > 0 {
		out.SegmentWorkers = v
	}
	if v := envBool("BULK_COPY"); v {
		out.BulkCopy = true
	}
	if v := envBool("ASYNC_PIPELINE"); v {
		out.AsyncPipeline = true
	}
	return out
}

// envInt reads XFERDB_NAME as an int; returns 0 when unset or unparseable.
func envInt(name string) int {
	v := os.Getenv(envPrefix + name)
	if v == "" {
		return 0
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return 0
	}
	return n
}

// envBool reads XFERDB_NAME as a bool. Truthy values: 1, true, yes, on (case-insensitive).
func envBool(name string) bool {
	v := strings.ToLower(os.Getenv(envPrefix + name))
	switch v {
	case "1", "true", "yes", "on":
		return true
	}
	return false
}
