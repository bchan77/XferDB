package adapter

import "context"

// Adapter defines the interface for source and target database adapters.
type Adapter interface {
	// Connect establishes connection to the database
	Connect(ctx context.Context) error

	// Close closes the database connection
	Close(ctx context.Context) error

	// ListTables returns all tables/collections in the database
	ListTables(ctx context.Context) ([]string, error)

	// ReadBatch reads a batch of records from a table
	ReadBatch(ctx context.Context, table string, offset, limit int) ([]map[string]interface{}, error)

	// WriteBatch writes a batch of records to the target
	WriteBatch(ctx context.Context, table string, records []map[string]interface{}) error

	// GetTableCount returns total record count for a table
	GetTableCount(ctx context.Context, table string) (int64, error)
}

// Config holds common adapter configuration
type Config struct {
	Type     string // postgres, mysql, sqlite, mongo, etc.
	Host     string
	Port     int
	Database string
	Username string
	Password string
	SSLMode  string
}
