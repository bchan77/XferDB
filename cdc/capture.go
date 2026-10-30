package cdc

import (
	"context"
	"errors"
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

	mu     sync.Mutex
	status CaptureStatus
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
			if err := c.Queue.Append(&ch); err != nil {
				return err
			}
			appended++
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
