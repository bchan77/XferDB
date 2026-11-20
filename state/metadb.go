package state

import (
	"fmt"
	"strings"

	"github.com/jmoiron/sqlx"
	_ "github.com/mattn/go-sqlite3"
)

// MetaDB is the embedded SQLite state store for all migration state.
type MetaDB struct {
	db *sqlx.DB
}

// Open opens (or creates) the state database at the given file path.
func Open(path string) (*MetaDB, error) {
	db, err := sqlx.Open("sqlite3", path)
	if err != nil {
		return nil, fmt.Errorf("state: open %s: %w", path, err)
	}
	// One connection: SQLite allows a single writer per file, and every
	// writer here lives in this process (table workers saving checkpoints and
	// progress, the stats saver, API handlers). With a pool, concurrent
	// writers compete for SQLite's file lock and one that waits longer than
	// the busy timeout fails with "database is locked" -- which failed whole
	// tables mid-migration (#124). Through a single connection they queue in
	// Go instead and never meet the lock. Any code using this handle must
	// close rows before issuing another query, or it waits on itself.
	db.SetMaxOpenConns(1)
	m := &MetaDB{db: db}
	if err := m.migrate(); err != nil {
		db.Close()
		return nil, fmt.Errorf("state: schema migration: %w", err)
	}
	return m, nil
}

// Close closes the state database.
func (m *MetaDB) Close() error {
	return m.db.Close()
}

// migrate runs the schema creation DDL idempotently.
func (m *MetaDB) migrate() error {
	_, err := m.db.Exec(`
		CREATE TABLE IF NOT EXISTS projects (
			id             TEXT PRIMARY KEY,
			name           TEXT UNIQUE NOT NULL,
			description    TEXT NOT NULL DEFAULT '',
			source_config  TEXT NOT NULL,
			target_config  TEXT NOT NULL,
			transfer_config TEXT NOT NULL DEFAULT '{}',
			created_at     DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
		);

		CREATE TABLE IF NOT EXISTS migrations (
			id           TEXT PRIMARY KEY,
			project_id   TEXT NOT NULL REFERENCES projects(id),
			status       TEXT NOT NULL DEFAULT 'pending',
			created_at   DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
			started_at   DATETIME,
			completed_at DATETIME,
			error        TEXT NOT NULL DEFAULT ''
		);

		CREATE TABLE IF NOT EXISTS migration_tables (
			migration_id     TEXT NOT NULL REFERENCES migrations(id),
			table_name       TEXT NOT NULL,
			status           TEXT NOT NULL DEFAULT 'pending',
			rows_total       INTEGER NOT NULL DEFAULT 0,
			rows_transferred INTEGER NOT NULL DEFAULT 0,
			started_at       DATETIME,
			completed_at     DATETIME,
			PRIMARY KEY (migration_id, table_name)
		);

		CREATE TABLE IF NOT EXISTS project_tables (
			project_id        TEXT NOT NULL,
			table_name        TEXT NOT NULL,
			status            TEXT NOT NULL DEFAULT 'not_started',
			rows_transferred  INTEGER NOT NULL DEFAULT 0,
			rows_total        INTEGER NOT NULL DEFAULT 0,
			last_migration_id TEXT,
			updated_at        DATETIME DEFAULT CURRENT_TIMESTAMP,
			PRIMARY KEY (project_id, table_name)
		);

		CREATE TABLE IF NOT EXISTS checkpoints (
			migration_id TEXT NOT NULL REFERENCES migrations(id),
			table_name   TEXT NOT NULL,
			batch_id     INTEGER NOT NULL,
			last_pk      TEXT NOT NULL DEFAULT '',
			rows_in_batch INTEGER NOT NULL DEFAULT 0,
			created_at   DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
			PRIMARY KEY (migration_id, table_name, batch_id)
		);

		CREATE TABLE IF NOT EXISTS schema_plans (
			project_id  TEXT     NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
			collection  TEXT     NOT NULL,
			field_name  TEXT     NOT NULL,
			pg_column   TEXT     NOT NULL DEFAULT '',
			pg_type     TEXT     NOT NULL DEFAULT '',
			strategy    TEXT     NOT NULL,
			is_pk       INTEGER  NOT NULL DEFAULT 0,
			nullable    INTEGER  NOT NULL DEFAULT 1,
			overridden  INTEGER  NOT NULL DEFAULT 0,
			created_at  DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
			PRIMARY KEY (project_id, collection, field_name)
		);

		CREATE TABLE IF NOT EXISTS migration_stats (
			id               INTEGER PRIMARY KEY AUTOINCREMENT,
			migration_id     TEXT NOT NULL REFERENCES migrations(id) ON DELETE CASCADE,
			timestamp        DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
			elapsed_secs     REAL NOT NULL DEFAULT 0,
			phase            TEXT NOT NULL DEFAULT '',
			rows_total       INTEGER NOT NULL DEFAULT 0,
			rows_transferred INTEGER NOT NULL DEFAULT 0,
			rate_per_sec     REAL NOT NULL DEFAULT 0,
			read_rate        REAL NOT NULL DEFAULT 0,
			write_rate       REAL NOT NULL DEFAULT 0,
			tables_total     INTEGER NOT NULL DEFAULT 0,
			tables_done      INTEGER NOT NULL DEFAULT 0,
			tables_failed    INTEGER NOT NULL DEFAULT 0,
			goroutines       INTEGER NOT NULL DEFAULT 0,
			mem_alloc_mb     REAL NOT NULL DEFAULT 0,
			mem_sys_mb       REAL NOT NULL DEFAULT 0,
			cpu_percent      REAL NOT NULL DEFAULT 0
		);

		CREATE INDEX IF NOT EXISTS idx_migration_stats_migration_id
			ON migration_stats(migration_id);

		CREATE TABLE IF NOT EXISTS migration_schema_steps (
			id           INTEGER PRIMARY KEY AUTOINCREMENT,
			migration_id TEXT NOT NULL REFERENCES migrations(id) ON DELETE CASCADE,
			kind         TEXT NOT NULL,
			table_name   TEXT NOT NULL,
			name         TEXT NOT NULL,
			started_at   DATETIME NOT NULL,
			finished_at  DATETIME,
			status       TEXT NOT NULL DEFAULT 'running',
			attempts     INTEGER NOT NULL DEFAULT 1,
			error        TEXT NOT NULL DEFAULT ''
		);
		CREATE INDEX IF NOT EXISTS idx_migration_schema_steps_migration_id
			ON migration_schema_steps(migration_id);

		CREATE TABLE IF NOT EXISTS validations (
			id           TEXT PRIMARY KEY,
			migration_id TEXT NOT NULL REFERENCES migrations(id) ON DELETE CASCADE,
			level        TEXT NOT NULL,
			status       TEXT NOT NULL DEFAULT 'pending',
			options      TEXT NOT NULL DEFAULT '{}',
			created_at   DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
			started_at   DATETIME,
			completed_at DATETIME,
			error        TEXT NOT NULL DEFAULT ''
		);

		CREATE INDEX IF NOT EXISTS idx_validations_migration_id
			ON validations(migration_id);

		CREATE TABLE IF NOT EXISTS validation_tables (
			validation_id TEXT NOT NULL REFERENCES validations(id) ON DELETE CASCADE,
			table_name    TEXT NOT NULL,
			result        TEXT NOT NULL DEFAULT 'pending',
			source_rows   INTEGER,
			target_rows   INTEGER,
			source_hash   TEXT NOT NULL DEFAULT '',
			target_hash   TEXT NOT NULL DEFAULT '',
			rows_checked  INTEGER NOT NULL DEFAULT 0,
			level_run     TEXT NOT NULL DEFAULT '',
			level_note    TEXT NOT NULL DEFAULT '',
			detail        TEXT NOT NULL DEFAULT '',
			PRIMARY KEY (validation_id, table_name)
		);

		CREATE TABLE IF NOT EXISTS cdc_streams (
			migration_id TEXT PRIMARY KEY REFERENCES migrations(id),
			slot_name    TEXT NOT NULL,
			publication  TEXT NOT NULL DEFAULT '',
			start_pos    TEXT NOT NULL,
			catchup_pos  TEXT NOT NULL DEFAULT '',
			cutover_pos  TEXT NOT NULL DEFAULT '',
			updated_at   DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
		);

		CREATE TABLE IF NOT EXISTS validation_diffs (
			validation_id TEXT NOT NULL REFERENCES validations(id) ON DELETE CASCADE,
			table_name    TEXT NOT NULL,
			seq           INTEGER NOT NULL,
			kind          TEXT NOT NULL,
			key           TEXT NOT NULL,
			columns       TEXT NOT NULL DEFAULT '[]',
			source_row    TEXT NOT NULL DEFAULT '',
			target_row    TEXT NOT NULL DEFAULT '',
			PRIMARY KEY (validation_id, table_name, seq)
		);
	`)
	if err != nil {
		return err
	}
	// schema_plan is a nullable JSON column added to an existing table.
	// ALTER TABLE in SQLite does not support IF NOT EXISTS, so we swallow
	// "duplicate column name" when the column already exists.
	if err := m.addColumnIfMissing("migrations", "schema_plan TEXT NOT NULL DEFAULT ''"); err != nil {
		return err
	}
	// Live migration (#102): snapshot or live, and a live run's phase.
	for _, col := range []string{
		"mode TEXT NOT NULL DEFAULT 'snapshot'",
		"phase TEXT NOT NULL DEFAULT ''",
		// A live run's effective transfer config (JSON), to resume it
		// after a server restart.
		"run_config TEXT NOT NULL DEFAULT ''",
	} {
		if err := m.addColumnIfMissing("migrations", col); err != nil {
			return err
		}
	}
	// The schema change a live stream is paused at (#102 phase 4), as JSON.
	if err := m.addColumnIfMissing("cdc_streams", "schema_change TEXT NOT NULL DEFAULT ''"); err != nil {
		return err
	}
	// L3 row-diff totals (#103 phase 2) on a validation_tables created before them.
	for _, col := range []string{
		"diff_missing INTEGER NOT NULL DEFAULT 0",
		"diff_extra INTEGER NOT NULL DEFAULT 0",
		"diff_mismatch INTEGER NOT NULL DEFAULT 0",
		"sample INTEGER NOT NULL DEFAULT 0",
		// Rows lossy plan overrides changed (#103 phase 3).
		"lossy_rows INTEGER NOT NULL DEFAULT 0",
	} {
		if err := m.addColumnIfMissing("validation_tables", col); err != nil {
			return err
		}
	}
	return nil
}

// addColumnIfMissing runs ALTER TABLE … ADD COLUMN and ignores the error when
// the column already exists (SQLite returns "duplicate column name: <col>").
func (m *MetaDB) addColumnIfMissing(table, colDef string) error {
	_, err := m.db.Exec(fmt.Sprintf("ALTER TABLE %s ADD COLUMN %s", table, colDef))
	if err != nil && !strings.Contains(err.Error(), "duplicate column name") {
		return err
	}
	return nil
}
