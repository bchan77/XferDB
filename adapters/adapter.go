package adapters

import (
	"context"
	"time"
)

// ConnectionConfig holds database connection parameters.
type ConnectionConfig struct {
	Type     string `json:"type"`
	Host     string `json:"host"`
	Port     int    `json:"port"`
	Database string `json:"database"`
	Username string `json:"username"`
	Password string `json:"password"`
	SSLMode  string `json:"ssl_mode"`
	DSN      string `json:"dsn"` // optional raw DSN; overrides individual fields if set
}

// ErrorPolicy defines how transfer errors are handled.
type ErrorPolicy string

const (
	ErrorPolicyAbort ErrorPolicy = "abort"
	ErrorPolicySkip  ErrorPolicy = "skip"
)

// TransferConfig controls batch transfer behaviour.
type TransferConfig struct {
	BatchSize  int         `json:"batch_size"`
	Workers    int         `json:"workers"`
	Validate   bool        `json:"validate"`
	OnError    ErrorPolicy `json:"on_error"`
	DataOnly   bool        `json:"data_only"`   // skip schema creation; target schema must already exist
	SchemaOnly bool        `json:"schema_only"` // create schema on target but do not transfer data
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
}

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
}

// Batch holds a set of records read from a source table.
type Batch struct {
	Records []map[string]interface{}
	Size    int
}

// PermissionCheck reports what the connected credential is allowed to do.
type PermissionCheck struct {
	CanRead        bool     `json:"can_read"`
	CanWrite       bool     `json:"can_write"`
	CanCreateTable bool     `json:"can_create_table"`
	Errors         []string `json:"errors,omitempty"`
}

// SourceAdapter is implemented by any database that can act as a migration source.
type SourceAdapter interface {
	Connect(ctx context.Context, config ConnectionConfig) error
	Close() error
	Ping(ctx context.Context) error
	ListTables(ctx context.Context) ([]TableSchema, error)
	GetSchema(ctx context.Context, table string) (*TableSchema, error)
	GetRowCount(ctx context.Context, table string) (int64, error)
	ReadBatch(ctx context.Context, table string, opts BatchOptions) (*Batch, error)
	CheckPermissions(ctx context.Context) (*PermissionCheck, error)
}

// TargetAdapter is implemented by any database that can act as a migration target.
type TargetAdapter interface {
	Connect(ctx context.Context, config ConnectionConfig) error
	Close() error
	Ping(ctx context.Context) error
	ListTables(ctx context.Context) ([]TableSchema, error)
	GetSchema(ctx context.Context, table string) (*TableSchema, error)
	CreateTable(ctx context.Context, schema *TableSchema) error
	AlterTable(ctx context.Context, table string, changes []SchemaChange) error
	// CreateIndexes creates non-primary-key indexes. Called after all data is transferred
	// so bulk inserts are faster and FK checks don't fire during the copy.
	CreateIndexes(ctx context.Context, table string, indexes []IndexDef) error
	// CreateConstraints adds foreign key and CHECK constraints. Called last so that
	// all tables exist before cross-table references are validated.
	CreateConstraints(ctx context.Context, table string, fks []ForeignKey, checks []CheckConstraint) error
	WriteBatch(ctx context.Context, table string, batch *Batch) error
	CheckPermissions(ctx context.Context) (*PermissionCheck, error)
}
