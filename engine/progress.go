package engine

import "time"

// EventKind classifies a progress event.
type EventKind string

const (
	EventMigrationStart  EventKind = "migration_start"  // emitted once with all table names
	EventTableStart      EventKind = "table_start"
	EventBatch           EventKind = "batch"
	EventTableDone       EventKind = "table_done"
	EventComplete        EventKind = "complete"
	EventError           EventKind = "error"
	EventPaused          EventKind = "paused"
	EventResumed         EventKind = "resumed"
	EventSchemaPhase     EventKind = "schema_phase"
	EventPostSchemaPhase EventKind = "post_schema_phase"
)

// ProgressEvent is emitted by the engine after each meaningful state change.
// The stats collector and WebSocket handler both consume this channel.
type ProgressEvent struct {
	MigrationID     string
	Kind            EventKind
	TableName       string
	TableNames      []string          // set only for EventMigrationStart
	TableCounts     map[string]int64  // set only for EventMigrationStart: table → row count
	RowsTransferred int64
	RowsTotal       int64
	Timestamp       time.Time
	Err             error // set only for EventError
	// Batch timing — set only for EventBatch; used to compute separate read/write rates.
	BatchRows     int           // rows in this specific batch (not cumulative)
	ReadDuration  time.Duration // time spent in source.ReadBatch
	WriteDuration time.Duration // time spent in target.WriteBatch
}
