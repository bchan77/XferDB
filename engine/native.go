package engine

import (
	"bufio"
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"time"

	_ "github.com/lib/pq"

	"gitea.homelab.local/nextdevops/XferDB/adapters"
)

// hasPgTools returns true when pg_dump and psql are on PATH.
func hasPgTools() bool {
	for _, tool := range []string{"pg_dump", "psql"} {
		if _, err := exec.LookPath(tool); err != nil {
			return false
		}
	}
	return true
}

// pgDumpPreData transfers the pre-data schema section from source to target.
func (e *Engine) pgDumpPreData(ctx context.Context, src, tgt adapters.ConnectionConfig, recreate bool) (bool, error) {
	if !hasPgTools() {
		return false, nil
	}
	args := []string{"--section=pre-data", "--no-owner", "--no-privileges", "--no-comments"}
	if recreate {
		args = append(args, "--clean", "--if-exists")
	}
	args = append(args, pgConnStr(src))
	return e.runPgDumpPsql(ctx, args, src, tgt)
}

// pgDumpPostData transfers the post-data schema section (indexes, FK constraints,
// triggers) one statement at a time. Executing sequentially avoids the catalog
// version conflicts YugabyteDB raises when concurrent DDL modifies catalog state.
// Each object emits an EventPostSchemaItem before execution so the CLI shows
// what is running instead of appearing to hang.
func (e *Engine) pgDumpPostData(ctx context.Context, src, tgt adapters.ConnectionConfig, _ int) (bool, error) {
	if !hasPgTools() {
		return false, nil
	}

	// Capture the post-data SQL from the source.
	dumpArgs := []string{
		"--section=post-data",
		"--no-owner",
		"--no-privileges",
		"--no-comments",
		pgConnStr(src),
	}
	dumpCmd := exec.CommandContext(ctx, "pg_dump", dumpArgs...)
	dumpCmd.Env = withPGPassword(src)
	var dumpOut, dumpErr bytes.Buffer
	dumpCmd.Stdout = &dumpOut
	dumpCmd.Stderr = &dumpErr
	if err := dumpCmd.Run(); err != nil {
		return false, fmt.Errorf("pg_dump: %w: %s", err, strings.TrimSpace(dumpErr.String()))
	}

	// Open a direct connection to the target for one-by-one execution.
	db, err := sql.Open("postgres", pgConnStr(tgt))
	if err != nil {
		return false, fmt.Errorf("open target connection: %w", err)
	}
	defer db.Close()
	if err := db.PingContext(ctx); err != nil {
		return false, fmt.Errorf("ping target: %w", err)
	}

	// Parse the dump into named DDL blocks and execute each one individually.
	var execErr error
	for _, stmt := range parsePgDumpBlocks(dumpOut.Bytes()) {
		e.emit(ProgressEvent{
			Kind:          EventPostSchemaItem,
			PostSchemaMsg: fmt.Sprintf("%-16s %s", strings.ToLower(stmt.Type)+":", stmt.Name),
			Timestamp:     time.Now(),
		})
		if _, err := db.ExecContext(ctx, stmt.SQL); err != nil {
			// Surface the first real DDL error but keep going so the rest of
			// the schema is attempted (mirrors ON_ERROR_STOP=off behaviour).
			if execErr == nil {
				execErr = fmt.Errorf("%s %s: %w", stmt.Type, stmt.Name, err)
			}
		}
	}
	return true, execErr
}

// ddlBlock is a parsed DDL statement from a pg_dump plain-SQL output.
type ddlBlock struct {
	Name string // e.g. "idx_orders_customer_id"
	Type string // e.g. "INDEX", "FK CONSTRAINT"
	SQL  string // the actual DDL text to execute
}

// parsePgDumpBlocks splits pg_dump plain-SQL output into individual DDL
// statements. Each block is introduced by a "-- Name: X; Type: Y" comment.
// SET statements are skipped — they are session-config hints that are often
// unrecognised by non-standard targets (e.g. YugabyteDB rejects
// "SET transaction_timeout = 0" from PG17 dumps).
func parsePgDumpBlocks(data []byte) []ddlBlock {
	relevant := map[string]bool{
		"INDEX": true, "UNIQUE INDEX": true,
		"FK CONSTRAINT": true, "CONSTRAINT": true,
		"TRIGGER": true, "RULE": true,
		"SEQUENCE SET": false, // skip — data-level, not schema
	}

	var blocks []ddlBlock
	var cur ddlBlock
	var sqlBuf strings.Builder
	inSQL := false

	flush := func() {
		sql := strings.TrimSpace(sqlBuf.String())
		if cur.Name != "" && sql != "" && relevant[cur.Type] {
			cur.SQL = sql
			blocks = append(blocks, cur)
		}
		cur = ddlBlock{}
		sqlBuf.Reset()
		inSQL = false
	}

	scanner := bufio.NewScanner(bytes.NewReader(data))
	for scanner.Scan() {
		line := scanner.Text()
		trimmed := strings.TrimSpace(line)

		// Block header: "-- Name: X; Type: Y; Schema: Z; Owner: W"
		if strings.HasPrefix(trimmed, "-- Name:") {
			flush()
			obj, ok := parsePgDumpHeader(trimmed)
			if ok {
				cur = ddlBlock{Name: obj.Name, Type: obj.Type}
			}
			continue
		}

		// Skip comment lines (including the "-- " separator lines).
		if strings.HasPrefix(trimmed, "--") || trimmed == "" {
			continue
		}

		// Skip SET statements — session config hints unsupported by some targets.
		if strings.HasPrefix(trimmed, "SET ") {
			continue
		}

		// Accumulate SQL content.
		inSQL = true
		sqlBuf.WriteString(line)
		sqlBuf.WriteByte('\n')
		_ = inSQL
	}
	flush()
	return blocks
}

// parsePgDumpHeader parses a "-- Name: X; Type: Y; Schema: Z; Owner: W" line.
func parsePgDumpHeader(line string) (pgDumpObject, bool) {
	line = strings.TrimPrefix(line, "-- ")
	var obj pgDumpObject
	for _, field := range strings.Split(line, "; ") {
		k, v, ok := strings.Cut(field, ": ")
		if !ok {
			continue
		}
		switch k {
		case "Name":
			obj.Name = v
		case "Type":
			obj.Type = v
		case "Schema":
			obj.Schema = v
		}
	}
	return obj, obj.Name != "" && obj.Type != ""
}

// pgDumpObject is a named DDL object extracted from a pg_dump header comment.
type pgDumpObject struct {
	Name, Type, Schema string
}

// runPgDumpPsql runs pg_dump with the given args and pipes its output into psql.
// Used for pre-data (plain SQL piped to psql with ON_ERROR_STOP=off).
func (e *Engine) runPgDumpPsql(ctx context.Context, dumpArgs []string, src, tgt adapters.ConnectionConfig) (bool, error) {
	dumpCmd := exec.CommandContext(ctx, "pg_dump", dumpArgs...)
	dumpCmd.Env = withPGPassword(src)
	var dumpOut, dumpErr bytes.Buffer
	dumpCmd.Stdout = &dumpOut
	dumpCmd.Stderr = &dumpErr
	if err := dumpCmd.Run(); err != nil {
		return false, fmt.Errorf("pg_dump: %w: %s", err, strings.TrimSpace(dumpErr.String()))
	}

	psqlArgs := []string{"--set", "ON_ERROR_STOP=off", "--quiet", pgConnStr(tgt)}
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

// pgConnStr returns the connection string for pg_dump / psql / sql.Open.
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

// withPGPassword returns the current environment plus PGPASSWORD when set.
func withPGPassword(cfg adapters.ConnectionConfig) []string {
	env := os.Environ()
	if cfg.Password != "" {
		env = append(env, "PGPASSWORD="+cfg.Password)
	}
	return env
}
