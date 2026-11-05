// Package cdc holds the engine-neutral parts of a live migration (#102): the
// Change a source's change log is decoded into, the ChangeSource interface a
// source implements to produce them, and the disk queue between the capture
// and apply workers. Nothing here knows which database a change came from.
package cdc

import (
	"context"
	"time"
)

// Op is the kind of row change.
type Op byte

const (
	OpInsert   Op = 'I'
	OpUpdate   Op = 'U'
	OpDelete   Op = 'D'
	OpTruncate Op = 'T'
	// OpSchema marks a table whose columns changed on the source: a schema
	// change (DDL) apply stops at until it is accepted (#102). It carries
	// the columns before and after the change, and no row.
	OpSchema Op = 'S'
)

func (o Op) String() string {
	switch o {
	case OpInsert:
		return "insert"
	case OpUpdate:
		return "update"
	case OpDelete:
		return "delete"
	case OpTruncate:
		return "truncate"
	case OpSchema:
		return "schema change"
	}
	return "unknown"
}

// Position is a source's resume point (a PostgreSQL LSN, a MySQL GTID set, a
// MongoDB resume token), opaque to everything but the source that made it.
type Position []byte

// Change is one row change, in source commit order.
//
// Values in Key, OldKey and After must be what the source adapter's ReadBatch
// returns for the same column, so a row the snapshot copied and a row the
// stream replayed reach the target as the same Go value. Only the types the
// queue codec stores are allowed (see codec.go).
type Change struct {
	// Seq is assigned by the queue on append: 1, 2, 3, ... per stream.
	Seq   uint64
	Table string
	Op    Op
	// Key is the row's primary key after the change (before it, for a delete).
	Key map[string]interface{}
	// OldKey is set on an update that changed the key: the key it had before.
	OldKey map[string]interface{}
	// After is the row after an insert or update. Columns in Unchanged are
	// missing from it: the source didn't send them (PostgreSQL leaves out an
	// unchanged TOASTed value), so the update only sets the others.
	After     map[string]interface{}
	Unchanged []string
	// Pos is set on the last change of a source transaction, and only there:
	// it is the point to resume from once everything up to it is applied.
	Pos      Position
	CommitAt time.Time
	// Columns and OldColumns are an OpSchema change's columns after and
	// before it; OldColumns is nil for a table the stream didn't know by
	// that name (a renamed table).
	Columns    []Column
	OldColumns []Column
}

// Column is a table column as a source describes it, in the source's own
// type names (PostgreSQL: format_type, as in numeric(10,2)).
type Column struct {
	Name string `json:"name"`
	Type string `json:"type"`
}

// SameColumns reports whether a and b are the same columns in the same order.
func SameColumns(a, b []Column) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// Retention is how much of its log a source keeps for a stream that hasn't
// been confirmed yet (PostgreSQL: WAL held by the replication slot).
type Retention struct {
	// RetainedBytes is the log kept for the stream.
	RetainedBytes int64 `json:"retained_bytes"`
	// SafeBytes is how much more log may be written before the source may
	// drop what the stream still needs (PostgreSQL: safe_wal_size, set when
	// max_slot_wal_keep_size limits it); nil when there is no such limit.
	SafeBytes *int64 `json:"safe_bytes,omitempty"`
	// State is the source's own word for it (PostgreSQL: wal_status:
	// reserved, extended, unreserved or lost), when it has one.
	State string `json:"state,omitempty"`
	// Lost means the source has dropped log the stream needs: changes are
	// gone, and the stream can't go on.
	Lost bool `json:"lost,omitempty"`
}

// Partial reports whether the change leaves some columns as they are.
func (c *Change) Partial() bool { return len(c.Unchanged) > 0 }

// ChangeSource reads a source database's change log. One is opened per live
// migration, by the capture worker.
type ChangeSource interface {
	// Prepare creates what the source needs to keep changes for these tables
	// (PostgreSQL: a publication and a replication slot) and returns the
	// position from which every later change will be streamed. It must run
	// before the snapshot copy starts. Calling it again for the same stream
	// returns the existing start point instead of a new one.
	Prepare(ctx context.Context, tables []string) (start Position, err error)
	// Stream sends changes after from to out, in commit order, until ctx is
	// done or the stream fails. Each transaction's changes are sent whole,
	// with Pos set on its last one.
	Stream(ctx context.Context, from Position, out chan<- Change) error
	// Ack tells the source every change up to upTo is stored durably, so it
	// may free its log up to there.
	Ack(ctx context.Context, upTo Position) error
	// CurrentPosition is the source's position now, for lag and cutover.
	CurrentPosition(ctx context.Context) (Position, error)
	// Sequences returns each sequence's current value, for syncing them to
	// the target at cutover.
	Sequences(ctx context.Context) (map[string]int64, error)
	// Teardown removes what Prepare created.
	Teardown(ctx context.Context) error
	// Confirmed is how far the stream has been confirmed to the source:
	// every change up to it is queued. It moves past the last acked change
	// when the source's log goes on without any change to our tables, so an
	// idle source still reaches a position recorded later.
	Confirmed() Position
	// Columns returns each table's columns as they are now, the baseline a
	// schema change is told by. Stream sends an OpSchema change, with only
	// Columns set, whenever it learns a table's columns (PostgreSQL: each
	// Relation message, also sent without any change once per session);
	// the capturer keeps those that differ from what it knew. It comes before
	// the change it describes, so it never carries Pos.
	Columns(ctx context.Context, tables []string) (map[string][]Column, error)
	// Retention reports how much log the source keeps for this stream.
	Retention(ctx context.Context) (Retention, error)
	// Behind reports how far pos is behind target, in the source's own
	// unit (PostgreSQL: WAL bytes); 0 when pos has reached target.
	Behind(pos, target Position) int64
	// FormatPosition renders a position for people and the metadb, and
	// ParsePosition reads it back.
	FormatPosition(p Position) string
	ParsePosition(s string) (Position, error)
	Close() error
}

// TimePositions is implemented by a source whose positions are times rather
// than offsets in a log (YugabyteDB: hybrid times). Its Behind is in
// microseconds, and it keeps no log whose size Retention could report.
type TimePositions interface {
	PositionsAreTimes() bool
}

// PositionsAreTimes reports whether src's positions are times.
func PositionsAreTimes(src ChangeSource) bool {
	tp, ok := src.(TimePositions)
	return ok && tp.PositionsAreTimes()
}
