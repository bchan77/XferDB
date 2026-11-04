package cdc

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sync"
	"time"
)

// CaptureStatus is what the capture loop reports while it runs.
type CaptureStatus struct {
	QueuedSeq     uint64    `json:"queued_seq"`
	QueuedPos     string    `json:"queued_pos,omitempty"`
	QueuedCommit  time.Time `json:"queued_commit_at,omitempty"`
	ChangesQueued int64     `json:"changes_queued"`
	LastSyncAt    time.Time `json:"last_sync_at,omitempty"`
	// Confirmed is the source's confirmed position (ChangeSource.Confirmed):
	// every change up to it is at or below QueuedSeq.
	Confirmed    Position `json:"confirmed,omitempty"`
	ConfirmedPos string   `json:"confirmed_pos,omitempty"`
}

// Capturer streams a source's changes into a queue.
type Capturer struct {
	Source ChangeSource
	Queue  *Writer
	// Start is where to stream from when the queue is empty: the position
	// Prepare returned, before the snapshot copy.
	Start Position
	// SyncEvery bounds how long a finished transaction may wait to be
	// fsynced (and acked) while more changes keep arriving; 0 means 200ms.
	SyncEvery time.Duration
	// Schema is each table's columns when the stream began
	// (ChangeSource.Columns), and SchemaFile where the columns known since
	// are kept. A table's columns changing queues one OpSchema change;
	// learning the same columns again (a new session) queues nothing.
	// Without Schema, the first columns seen for a table are taken as they
	// are.
	Schema     map[string][]Column
	SchemaFile string

	mu     sync.Mutex
	status CaptureStatus
}

// loadSchema returns the columns known when the capturer last stopped, or
// Schema when it has saved none.
func (c *Capturer) loadSchema() (map[string][]Column, error) {
	known := map[string][]Column{}
	if c.SchemaFile != "" {
		b, err := os.ReadFile(c.SchemaFile)
		if err == nil {
			if err := json.Unmarshal(b, &known); err != nil {
				return nil, fmt.Errorf("%s: %w", c.SchemaFile, err)
			}
			return known, nil
		}
		if !os.IsNotExist(err) {
			return nil, err
		}
	}
	for t, cols := range c.Schema {
		known[t] = cols
	}
	return known, nil
}

func (c *Capturer) saveSchema(known map[string][]Column) error {
	if c.SchemaFile == "" {
		return nil
	}
	b, err := json.Marshal(known)
	if err != nil {
		return err
	}
	tmp := c.SchemaFile + ".tmp"
	if err := os.WriteFile(tmp, b, 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, c.SchemaFile)
}

// Status returns a snapshot of the loop's progress.
func (c *Capturer) Status() CaptureStatus {
	c.mu.Lock()
	defer c.mu.Unlock()
	st := c.status
	// Read under mu: sync acks and raises QueuedSeq together under it, so
	// a change below Confirmed is never above the QueuedSeq reported.
	if p := c.Source.Confirmed(); len(p) > 0 {
		st.Confirmed, st.ConfirmedPos = p, c.Source.FormatPosition(p)
	}
	return st
}

// Run captures until ctx is done or the stream fails. It resumes after the
// last whole transaction in the queue, so a restart neither loses nor
// repeats a change.
func (c *Capturer) Run(ctx context.Context) error {
	if c.SyncEvery <= 0 {
		c.SyncEvery = 200 * time.Millisecond
	}
	known, err := c.loadSchema()
	if err != nil {
		return err
	}
	schemaDirty := false
	seq, from := c.Queue.Last()
	if from == nil {
		from = c.Start
	}
	c.mu.Lock()
	c.status.QueuedSeq = seq
	if from != nil {
		c.status.QueuedPos = c.Source.FormatPosition(from)
	}
	c.mu.Unlock()

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	out := make(chan Change, 4096)
	streamErr := make(chan error, 1)
	go func() { streamErr <- c.Source.Stream(ctx, from, out) }()

	var (
		lastSync = time.Now()
		unsynced Position // Pos of the last whole transaction not yet synced
		at       time.Time
		appended int64
	)
	sync := func() error {
		if unsynced == nil {
			return nil
		}
		seq, err := c.Queue.Sync()
		if err != nil {
			return err
		}
		// The columns a queued schema change moved to are known from now
		// on, also after a restart: the source won't resend that change
		// once acked. Saved after the queue, so a crash in between at worst
		// queues the change twice, never loses it.
		if schemaDirty {
			if err := c.saveSchema(known); err != nil {
				return err
			}
			schemaDirty = false
		}
		// Only now, with the changes on disk, may the source free them.
		c.mu.Lock()
		if err := c.Source.Ack(ctx, unsynced); err != nil {
			c.mu.Unlock()
			return err
		}
		c.status.QueuedSeq = seq
		c.status.QueuedPos = c.Source.FormatPosition(unsynced)
		if !at.IsZero() {
			c.status.QueuedCommit = at
		}
		c.status.ChangesQueued += appended
		c.status.LastSyncAt = time.Now()
		c.mu.Unlock()
		unsynced, appended, lastSync = nil, 0, time.Now()
		return nil
	}
	tick := time.NewTicker(c.SyncEvery)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			// Keep what's whole; the rest the source resends.
			_ = sync()
			return ctx.Err()
		case err := <-streamErr:
			if syncErr := sync(); syncErr != nil && err == nil {
				err = syncErr
			}
			if err == nil {
				err = errors.New("change stream ended")
			}
			return err
		case <-tick.C:
			if err := sync(); err != nil {
				return err
			}
		case ch := <-out:
			keep := true
			if ch.Op == OpSchema {
				old, ok := known[ch.Table]
				switch {
				case ok && SameColumns(old, ch.Columns):
					keep = false // known: a new session describing it again
				case !ok && len(known) == 0:
					keep = false // no baseline: take it as it is
					known[ch.Table], schemaDirty = ch.Columns, true
				default:
					ch.OldColumns = old // nil: a name the stream didn't know
					known[ch.Table], schemaDirty = ch.Columns, true
				}
			}
			if !keep && ch.Pos != nil {
				// The queue marks a transaction's end by its Pos: dropped,
				// the transaction would read as unfinished.
				return fmt.Errorf("%s: a schema change ends its transaction", ch.Table)
			}
			if keep {
				if err := c.Queue.Append(&ch); err != nil {
					return err
				}
				appended++
			}
			if ch.Pos != nil {
				unsynced, at = ch.Pos, ch.CommitAt
				// Sync at a transaction's end when the stream has gone
				// quiet or the last sync is old enough.
				if len(out) == 0 || time.Since(lastSync) >= c.SyncEvery {
					if err := sync(); err != nil {
						return err
					}
				}
			}
		}
	}
}
