package stats

import (
	"context"
	"sync"
	"time"

	"gitea.homelab.local/nextdevops/XferDB/engine"
)

// Collector consumes a stream of engine.ProgressEvents and maintains a
// thread-safe rolling snapshot of the migration's current statistics.
type Collector struct {
	migrationID string
	events      <-chan engine.ProgressEvent

	mu        sync.RWMutex
	snapshot  StatsSnapshot
	startedAt time.Time

	// per-table tracking — order preserved, status updated as events arrive
	tableIndex map[string]int // name → index in snapshot.TableDetails
	tableRows  map[string]int64

	// rolling rate calculation
	lastSampleTime time.Time
	lastSampleRows int64
}

// NewCollector creates a Collector for the given migration event stream.
func NewCollector(migrationID string, events <-chan engine.ProgressEvent) *Collector {
	now := time.Now()
	return &Collector{
		migrationID: migrationID,
		events:      events,
		startedAt:   now,
		tableIndex:  make(map[string]int),
		tableRows:   make(map[string]int64),
		snapshot: StatsSnapshot{
			MigrationID: migrationID,
			Phase:       "pending",
			StartedAt:   now,
		},
		lastSampleTime: now,
	}
}

// Start begins consuming events in a background goroutine. It returns when the
// events channel is closed or ctx is cancelled.
func (c *Collector) Start(ctx context.Context) {
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case ev, ok := <-c.events:
				if !ok {
					return
				}
				c.apply(ev)
			}
		}
	}()
}

// Snapshot returns a point-in-time copy of the current statistics.
func (c *Collector) Snapshot() StatsSnapshot {
	c.mu.RLock()
	defer c.mu.RUnlock()
	s := c.snapshot
	s.ElapsedSeconds = time.Since(c.startedAt).Seconds()
	return s
}

// apply processes a single event and updates the internal snapshot.
func (c *Collector) apply(ev engine.ProgressEvent) {
	c.mu.Lock()
	defer c.mu.Unlock()

	s := &c.snapshot

	switch ev.Kind {
	case engine.EventTableStart:
		s.Phase = "in_progress"
		s.CurrentTable = ev.TableName
		s.Rows.Total += ev.RowsTotal
		s.Tables.Total++
		s.Tables.InProgress++
		idx := len(s.TableDetails)
		s.TableDetails = append(s.TableDetails, TableDetail{
			Name:   ev.TableName,
			Status: "in_progress",
			Total:  ev.RowsTotal,
		})
		c.tableIndex[ev.TableName] = idx

	case engine.EventBatch:
		s.CurrentTable = ev.TableName
		s.Rows.Transferred = c.totalTransferred(ev)
		c.updateRate(s)
		if idx, ok := c.tableIndex[ev.TableName]; ok {
			s.TableDetails[idx].Transferred = ev.RowsTransferred
		}
		if s.Rows.RatePerSecond > 0 && s.Rows.Total > s.Rows.Transferred {
			s.ETASeconds = float64(s.Rows.Total-s.Rows.Transferred) / s.Rows.RatePerSecond
		}

	case engine.EventTableDone:
		s.Tables.InProgress--
		s.Tables.Completed++
		s.Rows.Transferred = c.totalTransferred(ev)
		if idx, ok := c.tableIndex[ev.TableName]; ok {
			s.TableDetails[idx].Status = "done"
			s.TableDetails[idx].Transferred = ev.RowsTransferred
		}

	case engine.EventComplete:
		s.Phase = "complete"
		s.CurrentTable = ""
		s.ETASeconds = 0

	case engine.EventError:
		s.Phase = "failed"
		if ev.Err != nil {
			s.Errors = append(s.Errors, ev.Err.Error())
		}

	case engine.EventPaused:
		s.Phase = "paused"

	case engine.EventResumed:
		s.Phase = "in_progress"
		// Reset rate sample so we don't compute a stale rate over the pause gap.
		c.lastSampleTime = time.Now()
		c.lastSampleRows = s.Rows.Transferred

	case engine.EventSchemaPhase:
		s.Phase = "schema"

	case engine.EventPostSchemaPhase:
		s.Phase = "post_schema"
	}

	s.Tables.Pending = s.Tables.Total - s.Tables.Completed - s.Tables.InProgress
}

// totalTransferred records the latest per-table row count from the event and
// returns the sum across all tables. Each engine event carries a cumulative
// per-table value, so we track them separately and sum to get the true total.
func (c *Collector) totalTransferred(ev engine.ProgressEvent) int64 {
	c.tableRows[ev.TableName] = ev.RowsTransferred
	var total int64
	for _, n := range c.tableRows {
		total += n
	}
	return total
}

// updateRate computes an exponentially-smoothed rows/sec rate.
func (c *Collector) updateRate(s *StatsSnapshot) {
	now := time.Now()
	elapsed := now.Sub(c.lastSampleTime).Seconds()
	if elapsed < 1.0 {
		return
	}
	delta := s.Rows.Transferred - c.lastSampleRows
	if delta > 0 && elapsed > 0 {
		instant := float64(delta) / elapsed
		if s.Rows.RatePerSecond == 0 {
			s.Rows.RatePerSecond = instant
		} else {
			// Smooth with α=0.3
			s.Rows.RatePerSecond = 0.3*instant + 0.7*s.Rows.RatePerSecond
		}
	}
	c.lastSampleTime = now
	c.lastSampleRows = s.Rows.Transferred
}
