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
	// FormatPosition renders a position for people and the metadb.
	FormatPosition(p Position) string
	Close() error
}
