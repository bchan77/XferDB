package engine

import "time"

// EventKind classifies a progress event.
type EventKind string

const (
	EventTableStart     EventKind = "table_start"
	EventBatch          EventKind = "batch"
	EventTableDone      EventKind = "table_done"
	EventComplete       EventKind = "complete"
	EventError          EventKind = "error"
	EventPaused         EventKind = "paused"
	EventResumed        EventKind = "resumed"
	EventSchemaPhase    EventKind = "schema_phase"
	EventPostSchemaPhase EventKind = "post_schema_phase"
)

// ProgressEvent is emitted by the engine after each meaningful state change.
// The stats collector and WebSocket handler both consume this channel.
type ProgressEvent struct {
	MigrationID     string
	Kind            EventKind
	TableName       string
	RowsTransferred int64
	RowsTotal       int64
	Timestamp       time.Time
	Err             error // set only for EventError
}
