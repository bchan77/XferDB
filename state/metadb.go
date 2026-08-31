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
	`)
	if err != nil {
		return err
	}
	// schema_plan is a nullable JSON column added to an existing table.
	// ALTER TABLE in SQLite does not support IF NOT EXISTS, so we swallow
	// "duplicate column name" when the column already exists.
	return m.addColumnIfMissing("migrations", "schema_plan TEXT NOT NULL DEFAULT ''")
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
