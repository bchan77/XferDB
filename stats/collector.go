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
	projectID   string
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
// config carries the effective transfer settings (batch size, worker counts) for display.
func NewCollector(migrationID, projectID string, events <-chan engine.ProgressEvent, config MigrationConfig) *Collector {
	now := time.Now()
	return &Collector{
		migrationID: migrationID,
		projectID:   projectID,
		events:      events,
		startedAt:   now,
		tableIndex:  make(map[string]int),
		tableRows:   make(map[string]int64),
		snapshot: StatsSnapshot{
			MigrationID: migrationID,
			ProjectID:   projectID,
			Phase:       "pending",
			StartedAt:   now,
			Config:      config,
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
	case engine.EventMigrationStart:
		for _, name := range ev.TableNames {
			if _, exists := c.tableIndex[name]; !exists {
				count := ev.TableCounts[name]
				s.Rows.Total += count
				idx := len(s.TableDetails)
				s.TableDetails = append(s.TableDetails, TableDetail{
					Name:   name,
					Status: "pending",
					Total:  count,
				})
				c.tableIndex[name] = idx
			}
		}

	case engine.EventTableStart:
		s.Phase = "in_progress"
		s.CurrentTable = ev.TableName
		s.Tables.Total++
		s.Tables.InProgress++
		if idx, ok := c.tableIndex[ev.TableName]; ok {
			// Already pre-populated from EventMigrationStart; just update status.
			// Only add to total if it wasn't counted upfront (count was 0/unknown).
			if s.TableDetails[idx].Total == 0 && ev.RowsTotal > 0 {
				s.Rows.Total += ev.RowsTotal
				s.TableDetails[idx].Total = ev.RowsTotal
			}
			s.TableDetails[idx].Status = "in_progress"
		} else {
			s.Rows.Total += ev.RowsTotal
			idx = len(s.TableDetails)
			s.TableDetails = append(s.TableDetails, TableDetail{
				Name:   ev.TableName,
				Status: "in_progress",
				Total:  ev.RowsTotal,
			})
			c.tableIndex[ev.TableName] = idx
		}

	case engine.EventBatch:
		s.CurrentTable = ev.TableName
		s.Rows.Transferred = c.totalTransferred(ev)
		c.updateRate(s)
		c.updateReadWriteRates(s, ev)
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

	case engine.EventTableFailed:
		s.Tables.InProgress--
		s.Tables.Failed++
		if idx, ok := c.tableIndex[ev.TableName]; ok {
			s.TableDetails[idx].Status = "failed"
			if ev.Err != nil {
				s.TableDetails[idx].Error = ev.Err.Error()
			}
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

	s.Tables.Pending = s.Tables.Total - s.Tables.Completed - s.Tables.InProgress - s.Tables.Failed
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

// updateReadWriteRates computes per-batch EMA read and write rates from the timing
// data attached to each EventBatch. Each batch carries its own ReadDuration and
// WriteDuration so the rates reflect actual source/target throughput independently.
func (c *Collector) updateReadWriteRates(s *StatsSnapshot, ev engine.ProgressEvent) {
	if ev.BatchRows <= 0 {
		return
	}
	const alpha = 0.3
	if ev.ReadDuration > 0 {
		instant := float64(ev.BatchRows) / ev.ReadDuration.Seconds()
		if s.Rows.ReadRate == 0 {
			s.Rows.ReadRate = instant
		} else {
			s.Rows.ReadRate = alpha*instant + (1-alpha)*s.Rows.ReadRate
		}
	}
	if ev.WriteDuration > 0 {
		instant := float64(ev.BatchRows) / ev.WriteDuration.Seconds()
		if s.Rows.WriteRate == 0 {
			s.Rows.WriteRate = instant
		} else {
			s.Rows.WriteRate = alpha*instant + (1-alpha)*s.Rows.WriteRate
		}
	}
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
