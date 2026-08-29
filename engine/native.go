package engine

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"strings"

	"gitea.homelab.local/nextdevops/XferDB/adapters"
)

// hasPgTools returns true when both pg_dump and psql are on PATH.
func hasPgTools() bool {
	_, errDump := exec.LookPath("pg_dump")
	_, errPsql := exec.LookPath("psql")
	return errDump == nil && errPsql == nil
}

// pgDumpPreData transfers the pre-data schema section (table definitions,
// extensions, custom types, sequences) from source to target using pg_dump | psql.
//
// When recreate is true, --clean --if-exists is added so that existing tables
// and their dependent objects are dropped before recreation.
//
// Returns (true, nil) on success, (false, nil) when pg_dump/psql are unavailable
// (caller should fall back to introspection-based schema creation).
func pgDumpPreData(ctx context.Context, src, tgt adapters.ConnectionConfig, recreate bool) (bool, error) {
	if !hasPgTools() {
		return false, nil
	}

	args := []string{
		"--section=pre-data",
		"--no-owner",
		"--no-privileges",
		"--no-comments",
	}
	if recreate {
		args = append(args, "--clean", "--if-exists")
	}
	args = append(args, pgConnStr(src))

	return runPgDumpPsql(ctx, args, src, tgt, recreate)
}

// pgDumpPostData transfers the post-data schema section (indexes, triggers,
// FK constraints) from source to target using pg_dump | psql.
//
// Called after Phase 2 data transfer so indexes are built on populated tables,
// which is significantly faster than maintaining them during inserts.
func pgDumpPostData(ctx context.Context, src, tgt adapters.ConnectionConfig) (bool, error) {
	if !hasPgTools() {
		return false, nil
	}

	args := []string{
		"--section=post-data",
		"--no-owner",
		"--no-privileges",
		"--no-comments",
		pgConnStr(src),
	}

	return runPgDumpPsql(ctx, args, src, tgt, false)
}

// runPgDumpPsql runs pg_dump with the given args and pipes its output into psql
// on the target. stopOnError controls whether psql aborts on the first error
// (appropriate when recreating; lenient mode handles "already exists" on re-runs).
func runPgDumpPsql(ctx context.Context, dumpArgs []string, src, tgt adapters.ConnectionConfig, stopOnError bool) (bool, error) {
	dumpCmd := exec.CommandContext(ctx, "pg_dump", dumpArgs...)
	dumpCmd.Env = withPGPassword(src)

	var dumpOut, dumpErr bytes.Buffer
	dumpCmd.Stdout = &dumpOut
	dumpCmd.Stderr = &dumpErr

	if err := dumpCmd.Run(); err != nil {
		return false, fmt.Errorf("pg_dump: %w: %s", err, strings.TrimSpace(dumpErr.String()))
	}

	errStop := "off"
	if stopOnError {
		errStop = "on"
	}
	psqlArgs := []string{
		"--set", "ON_ERROR_STOP=" + errStop,
		"--quiet",
		pgConnStr(tgt),
	}
	psqlCmd := exec.CommandContext(ctx, "psql", psqlArgs...)
	psqlCmd.Env = withPGPassword(tgt)
	psqlCmd.Stdin = &dumpOut

	var psqlErr bytes.Buffer
	psqlCmd.Stderr = &psqlErr

	if err := psqlCmd.Run(); err != nil {
		return false, fmt.Errorf("psql schema restore: %w: %s", err, strings.TrimSpace(psqlErr.String()))
	}

	return true, nil
}

// pgConnStr returns the connection string to pass to pg_dump or psql.
// Uses the raw DSN when available (preserves all options like sslmode);
// otherwise builds a postgres:// URI from the individual fields.
func pgConnStr(cfg adapters.ConnectionConfig) string {
	if cfg.DSN != "" {
		return cfg.DSN
	}
	var b strings.Builder
	b.WriteString("postgres://")
	if cfg.Username != "" {
		b.WriteString(cfg.Username)
		if cfg.Password != "" {
			b.WriteString(":" + cfg.Password)
		}
		b.WriteString("@")
	}
	host := cfg.Host
	if host == "" {
		host = "localhost"
	}
	b.WriteString(host)
	if cfg.Port != 0 {
		b.WriteString(fmt.Sprintf(":%d", cfg.Port))
	}
	if cfg.Database != "" {
		b.WriteString("/" + cfg.Database)
	}
	if cfg.SSLMode != "" {
		b.WriteString("?sslmode=" + cfg.SSLMode)
	}
	return b.String()
}

// withPGPassword returns the current process environment plus PGPASSWORD when
// the config has a password. pg_dump and psql read this to skip interactive prompts.
func withPGPassword(cfg adapters.ConnectionConfig) []string {
	env := os.Environ()
	if cfg.Password != "" {
		env = append(env, "PGPASSWORD="+cfg.Password)
	}
	return env
}
