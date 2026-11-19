package adapters

import (
	"context"
	"encoding/json"
	"fmt"
	"time"
)

// ConnectionConfig holds database connection parameters.
type ConnectionConfig struct {
	Type     string `json:"type" yaml:"type"`
	Host     string `json:"host" yaml:"host"`
	Port     int    `json:"port" yaml:"port"`
	Database string `json:"database" yaml:"database"`
	Username string `json:"username" yaml:"username"`
	Password string `json:"password" yaml:"password"`
	SSLMode  string `json:"ssl_mode" yaml:"ssl_mode"`
	// Schema is the PostgreSQL schema to read tables from or write them to;
	// empty means public. Other adapters ignore it.
	Schema string `json:"schema,omitempty" yaml:"schema,omitempty"`
	DSN    string `json:"dsn" yaml:"dsn"` // optional raw DSN; overrides individual fields if set
}

// ErrorPolicy defines how transfer errors are handled.
type ErrorPolicy string

const (
	ErrorPolicyAbort ErrorPolicy = "abort"
	ErrorPolicySkip  ErrorPolicy = "skip"
)

// TransferConfig controls batch transfer behaviour.
type TransferConfig struct {
	BatchSize      int         `json:"batch_size" yaml:"batch_size"`
	TableWorkers   int         `json:"table_workers" yaml:"table_workers"`     // tables to migrate concurrently (inter-table)
	SegmentWorkers int         `json:"segment_workers" yaml:"segment_workers"` // workers per table, splitting by PK range (intra-table)
	OffsetFallback bool        `json:"offset_fallback" yaml:"offset_fallback"` // force offset-based segments even when PK range is available
	OnError        ErrorPolicy `json:"on_error" yaml:"on_error"`
	DataOnly       bool        `json:"data_only" yaml:"data_only"`             // skip schema creation; target schema must already exist
	SchemaOnly     bool        `json:"schema_only" yaml:"schema_only"`         // create schema on target but do not transfer data
	RecreateSchema bool        `json:"recreate_schema" yaml:"recreate_schema"` // drop and recreate tables before migrating
	Truncate       bool        `json:"truncate" yaml:"truncate"`               // truncate tables before loading data (keeps schema)
	Tables         []string    `json:"tables" yaml:"tables"`                   // if non-empty, only migrate these tables; supports "schema.table" notation
	BulkCopy       bool        `json:"bulk_copy" yaml:"bulk_copy"`             // use COPY protocol instead of INSERT; requires target table to be empty
	AccurateCounts bool        `json:"accurate_counts" yaml:"accurate_counts"` // use accurate row counts (slower for MongoDB)
	AsyncPipeline  bool        `json:"async_pipeline" yaml:"async_pipeline"`   // overlap read/write for faster throughput

	// Validation decides when a finished migration is validated on its own:
	// "after" (the default when empty) or "off". ValidationLevel is the check
	// it runs: "L1" (row counts), "L2" (the default when empty), or "L3" (L2,
	// plus a row diff of any table that fails). They replace an earlier
	// `validate` bool that nothing ever read. ValidationSample is the
	// percentage of a failing table the L3 diff checks; 0 means every row.
	Validation       string `json:"validation,omitempty" yaml:"validation"`
	ValidationLevel  string `json:"validation_level,omitempty" yaml:"validation_level"`
	ValidationSample int    `json:"validation_sample,omitempty" yaml:"validation_sample"`

	// RunOptionsSaved is set once the web UI's run form has saved its
	// choices here as the project's defaults. Until then the form keeps its
	// own defaults for the fields whose zero value isn't one (async pipeline
	// on), since an older project's false means "never set", not "off".
	RunOptionsSaved bool `json:"run_options_saved,omitempty" yaml:"-"`
}

// Project is a named migration project with a fixed source/target configuration.
type Project struct {
	ID             string           `json:"id"`
	Name           string           `json:"name"`
	Description    string           `json:"description"`
	SourceConfig   ConnectionConfig `json:"source_config"`
	TargetConfig   ConnectionConfig `json:"target_config"`
	TransferConfig TransferConfig   `json:"transfer_config"`
	CreatedAt      time.Time        `json:"created_at"`
}

// MigrationStatus represents the lifecycle state of a migration.
type MigrationStatus string

const (
	StatusPending    MigrationStatus = "pending"
	StatusInProgress MigrationStatus = "in_progress"
	StatusPaused     MigrationStatus = "paused"
	StatusCompleted  MigrationStatus = "completed"
	StatusFailed     MigrationStatus = "failed"
	StatusCancelled  MigrationStatus = "cancelled"
)

// Migration represents a single migration run scoped to a project.
type Migration struct {
	ID          string          `json:"id"`
	ProjectID   string          `json:"project_id"`
	Status      MigrationStatus `json:"status"`
	CreatedAt   time.Time       `json:"created_at"`
	StartedAt   *time.Time      `json:"started_at,omitempty"`
	CompletedAt *time.Time      `json:"completed_at,omitempty"`
	Error       string          `json:"error,omitempty"`
	// Mode is MigrationModeSnapshot (a one-time copy) or MigrationModeLive
	// (a copy, then streaming changes until cutover, #102). Phase is where a
	// live migration is: setup, snapshot, catch_up, streaming, cutover.
	Mode  string `json:"mode"`
	Phase string `json:"phase,omitempty"`
}

// Migration modes.
const (
	MigrationModeSnapshot = "snapshot"
	MigrationModeLive     = "live"
)

// TableProgress tracks per-table progress within a migration.
type TableProgress struct {
	TableName       string          `json:"table_name"`
	Status          MigrationStatus `json:"status"`
	RowsTotal       int64           `json:"rows_total"`
	RowsTransferred int64           `json:"rows_transferred"`
	StartedAt       *time.Time      `json:"started_at,omitempty"`
	CompletedAt     *time.Time      `json:"completed_at,omitempty"`
}

// Checkpoint records the last successfully written batch for a table,
// enabling pause/resume and crash recovery.
type Checkpoint struct {
	MigrationID string
	TableName   string
	BatchID     int
	LastPK      string // serialised primary key value for keyset pagination
	RowsInBatch int
	CreatedAt   time.Time
}

// ColumnDef describes a single column in a table schema.
type ColumnDef struct {
	Name         string  `json:"name"`
	Type         string  `json:"type"`
	Nullable     bool    `json:"nullable"`
	PrimaryKey   bool    `json:"primary_key"`
	DefaultValue *string `json:"default_value,omitempty"`
	// DefaultDialect is the SQL dialect DefaultValue is written in (a
	// Dialect* constant), set by the source that read it, so a target of
	// another dialect can translate it (TranslateDefault).
	DefaultDialect string `json:"-"`
	// Identity marks a column whose value the database generates when an
	// insert leaves it out (#194): an Oracle or PostgreSQL identity column,
	// a PostgreSQL serial, a MySQL AUTO_INCREMENT. A target creates it as
	// its own kind where it can, so the application's inserts still get a
	// key after the switch-over; DefaultValue is empty for such a column.
	Identity bool `json:"identity,omitempty"`
}

// IndexDef describes an index on a table (excluding the primary key index).
type IndexDef struct {
	Name    string   `json:"name"`
	Columns []string `json:"columns"`
	Unique  bool     `json:"unique"`
}

// ForeignKey describes a foreign key constraint.
type ForeignKey struct {
	Name       string   `json:"name"`
	Columns    []string `json:"columns"`
	RefTable   string   `json:"ref_table"`
	RefColumns []string `json:"ref_columns"`
	OnDelete   string   `json:"on_delete,omitempty"`
	OnUpdate   string   `json:"on_update,omitempty"`
}

// CheckConstraint describes a CHECK constraint on a table.
type CheckConstraint struct {
	Name       string `json:"name"`
	Expression string `json:"expression"`
}

// TableSchema describes the full structure of a table.
type TableSchema struct {
	Name        string            `json:"name"`
	Columns     []ColumnDef       `json:"columns"`
	Indexes     []IndexDef        `json:"indexes,omitempty"`
	ForeignKeys []ForeignKey      `json:"foreign_keys,omitempty"`
	Checks      []CheckConstraint `json:"checks,omitempty"`
}

// ChangeType classifies a schema alteration.
type ChangeType string

const (
	ChangeAddColumn   ChangeType = "add_column"
	ChangeDropColumn  ChangeType = "drop_column"
	ChangeAlterColumn ChangeType = "alter_column"
)

// SchemaChange describes a single alteration to a table schema.
type SchemaChange struct {
	Type   ChangeType
	Column ColumnDef
}

// BatchOptions controls how a batch is read from a source table.
type BatchOptions struct {
	Offset int
	Limit  int
	LastPK interface{} // set on resume to continue from last checkpoint
	// PK-range fields for intra-table parallel workers (all zero/empty when not in use):
	PKCol     string // primary key column name; when set, use range query instead of OFFSET
	PKMin     int64  // inclusive lower bound for the segment range
	PKMax     int64  // upper bound for the segment range
	PKMaxIncl bool   // when true use <= for PKMax (last segment); otherwise use <
	// Keyset paging by the whole primary key, for a live migration's
	// snapshot (#102), where rows change while they are read and OFFSET
	// would skip or repeat some: rows after KeyAfter (nil: from the first)
	// up to and including KeyUpTo, in key order. Keys are the key columns'
	// text forms, in KeysetSource.KeyColumns order. The batch's LastKey is
	// the last row's key, as EncodeKey makes it.
	Keyset   bool
	KeyAfter []string
	KeyUpTo  []string
}

// Batch holds a set of records read from a source table.
type Batch struct {
	Records []map[string]interface{}
	Size    int
	LastPK  int64  // set by ReadBatch when PKCol is used; last PK value in this batch (integer PK-range workers)
	LastKey string // opaque resume token for keyset adapters (e.g. MongoDB _id as hex); empty for SQL adapters
}

// KeysetSource is implemented by sources that can page a table by its
// primary key (BatchOptions.Keyset).
type KeysetSource interface {
	// KeyColumns returns a table's primary key columns, in paging order;
	// none means the table has no primary key.
	KeyColumns(ctx context.Context, table string) ([]string, error)
	// MaxKey returns the largest key in a table now, nil when it is empty.
	MaxKey(ctx context.Context, table string) ([]string, error)
}

// EncodeKey and DecodeKey turn a keyset key into Batch.LastKey's form and
// back, so it can be saved in a checkpoint.
func EncodeKey(key []string) string {
	b, _ := json.Marshal(key)
	return string(b)
}

func DecodeKey(s string) ([]string, error) {
	if s == "" {
		return nil, nil
	}
	var key []string
	if err := json.Unmarshal([]byte(s), &key); err != nil {
		return nil, fmt.Errorf("checkpoint key %q: %w", s, err)
	}
	return key, nil
}

// PermissionCheck reports what the connected credential is allowed to do.
type PermissionCheck struct {
	CanRead        bool     `json:"can_read"`
	CanWrite       bool     `json:"can_write"`
	CanCreateTable bool     `json:"can_create_table"`
	Errors         []string `json:"errors,omitempty"`
}

// DatabaseInfo holds metadata about a connected database.
type DatabaseInfo struct {
	Type     string `json:"type"`             // e.g. "PostgreSQL", "MongoDB", "MySQL"
	Version  string `json:"version"`          // e.g. "13.23", "8.0.3"
	Host     string `json:"host"`             // e.g. "postgres.homelab.local:5432"
	Database string `json:"database"`         // database/schema name
	Schema   string `json:"schema,omitempty"` // postgres: the one schema configured, empty when migrating all
	// Schemas (postgres, all-schemas mode) lists the schemas holding tables.
	Schemas []string `json:"schemas,omitempty"`
	// OtherSchemas (postgres, only when Tables is 0) lists the schemas that do
	// hold tables, so a client can point at the Schema setting.
	OtherSchemas []string `json:"other_schemas,omitempty"`
	Tables       int      `json:"tables"`                // number of tables/collections
	SizeBytes    int64    `json:"size_bytes,omitempty"`  // total size in bytes (0 if unknown)
	SizeHuman    string   `json:"size_human,omitempty"`  // human-readable size e.g. "1.2 GB"
	SSL          string   `json:"ssl,omitempty"`         // "enabled", "disabled", or ""
	ServerInfo   string   `json:"server_info,omitempty"` // additional server info
}

// SourceAdapter is implemented by any database that can act as a migration source.
type SourceAdapter interface {
	Connect(ctx context.Context, config ConnectionConfig) error
	Close() error
	Ping(ctx context.Context) error
	ListTables(ctx context.Context) ([]TableSchema, error)
	GetSchema(ctx context.Context, table string) (*TableSchema, error)
	GetRowCount(ctx context.Context, table string) (int64, error)
	// GetPKRange returns the inclusive min and max values of an integer PK column.
	// Used to split a table into parallel segments. Returns an error when the table is empty.
	GetPKRange(ctx context.Context, table, pkColumn string) (min, max int64, err error)
	ReadBatch(ctx context.Context, table string, opts BatchOptions) (*Batch, error)
	CheckPermissions(ctx context.Context) (*PermissionCheck, error)
	// GetInfo returns metadata about the connected database (version, size, etc.).
	GetInfo(ctx context.Context) (*DatabaseInfo, error)
}

// DatabaseLister is an optional interface for source adapters on a server
// that hosts several databases (postgres, mysql, mongodb). ListDatabases returns the
// user databases the connected login can see, so a client can offer them as
// a choice instead of making the user type the name. System databases
// (templates, information_schema, ...) are left out.
type DatabaseLister interface {
	ListDatabases(ctx context.Context) ([]string, error)
}

// SchemaLister is an optional interface for source adapters whose database
// holds several schemas (postgres, yugabyte, oracle). ListSchemas returns the
// schemas the connected login can use, so a client can offer them as a
// choice for ConnectionConfig.Schema. System schemas are left out.
type SchemaLister interface {
	ListSchemas(ctx context.Context) ([]string, error)
}

// DatabaseCreator is an optional interface for target adapters that can
// create a database on their server (postgres, mysql), so a project whose
// target database doesn't exist yet can have it made instead of failing
// preflight. CreateDatabase succeeds if the database already exists.
type DatabaseCreator interface {
	CreateDatabase(ctx context.Context, name string) error
}

// BulkCopyWriter is an optional interface for target adapters that support a
// fast bulk-loading write path in place of their normal upsert-capable one —
// postgres's COPY protocol, mysql's LOAD DATA LOCAL INFILE, sqlite's
// multi-row inserts with synchronous=OFF. The engine enables this when the
// target table is guaranteed clean (truncate or recreate-schema mode): the
// postgres and sqlite implementations aren't safe in upsert mode (postgres's
// raw COPY has no ON CONFLICT support at all; sqlite's synchronous=OFF trades
// away crash durability), though mysql's REPLACE-based LOAD DATA tolerates a
// non-empty target same as its regular path.
type BulkCopyWriter interface {
	EnableCopy()
}

// TargetAdapter is implemented by any database that can act as a migration target.
type TargetAdapter interface {
	Connect(ctx context.Context, config ConnectionConfig) error
	Close() error
	Ping(ctx context.Context) error
	ListTables(ctx context.Context) ([]TableSchema, error)
	GetSchema(ctx context.Context, table string) (*TableSchema, error)
	CreateTable(ctx context.Context, schema *TableSchema) error
	DropTable(ctx context.Context, table string) error
	TruncateTable(ctx context.Context, table string) error
	AlterTable(ctx context.Context, table string, changes []SchemaChange) error
	// CreateIndexes creates non-primary-key indexes. Called after all data is transferred
	// so bulk inserts are faster and FK checks don't fire during the copy.
	CreateIndexes(ctx context.Context, table string, indexes []IndexDef) error
	// CreateConstraints adds foreign key and CHECK constraints. Called last so that
	// all tables exist before cross-table references are validated.
	CreateConstraints(ctx context.Context, table string, fks []ForeignKey, checks []CheckConstraint) error
	WriteBatch(ctx context.Context, table string, batch *Batch) error
	CheckPermissions(ctx context.Context) (*PermissionCheck, error)
	// GetInfo returns metadata about the connected database (version, size, etc.).
	GetInfo(ctx context.Context) (*DatabaseInfo, error)
}
