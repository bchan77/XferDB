package stats

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"gitea.homelab.local/nextdevops/XferDB/engine"
)

// resourceSampleInterval is how often the collector samples process resource usage.
const resourceSampleInterval = 3 * time.Second

// Collector consumes a stream of engine.ProgressEvents and maintains a
// thread-safe rolling snapshot of the migration's current statistics.
type Collector struct {
	migrationID string
	projectID   string
	events      <-chan engine.ProgressEvent
	log         *slog.Logger

	mu        sync.RWMutex
	snapshot  StatsSnapshot
	startedAt time.Time

	// per-table tracking — order preserved, status updated as events arrive
	tableIndex    map[string]int // name → index in snapshot.TableDetails
	tableRows     map[string]int64
	tableStarted  map[string]time.Time // for per-table elapsed logging

	// rolling rate calculation
	lastSampleTime time.Time
	lastSampleRows int64
}

// NewCollector creates a Collector for the given migration event stream.
// config carries the effective transfer settings (batch size, worker counts) for display.
func NewCollector(migrationID, projectID string, events <-chan engine.ProgressEvent, config MigrationConfig, log *slog.Logger) *Collector {
	now := time.Now()
	if log == nil {
		log = slog.Default()
	}
	return &Collector{
		migrationID:  migrationID,
		projectID:    projectID,
		events:       events,
		log:          log.With("migration_id", migrationID),
		startedAt:    now,
		tableIndex:   make(map[string]int),
		tableRows:    make(map[string]int64),
		tableStarted: make(map[string]time.Time),
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

	// Periodically sample resource usage in the background.
	go func() {
		ticker := time.NewTicker(resourceSampleInterval)
		defer ticker.Stop()
		// Fire immediately so the first stats snapshot already has resource data.
		c.apply(engine.ProgressEvent{
			Kind:     engine.EventResourceSample,
			Resource: engine.SampleResource(),
		})
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				c.apply(engine.ProgressEvent{
					Kind:     engine.EventResourceSample,
					Resource: engine.SampleResource(),
				})
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
	// Recompute Rows.Total from per-table totals to ensure consistency.
	// This is more robust than tracking increments across multiple event handlers.
	var total int64
	for _, t := range s.TableDetails {
		total += t.Total
	}
	s.Rows.Total = total
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
		// Log schema for each table.
		for _, schema := range ev.TableSchemas {
			cols := make([]string, len(schema.Columns))
			for i, col := range schema.Columns {
				cols[i] = col.Name + ":" + col.Type
			}
			c.log.Info("migration.table_schema",
				"table", schema.Name,
				"columns", len(schema.Columns),
				"schema", strings.Join(cols, ", "),
				"indexes", len(schema.Indexes),
				"foreign_keys", len(schema.ForeignKeys),
			)
		}

	case engine.EventTableStart:
		s.Phase = "in_progress"
		s.CurrentTable = ev.TableName
		c.tableStarted[ev.TableName] = time.Now()
		if idx, ok := c.tableIndex[ev.TableName]; ok {
			// Table already registered from EventMigrationStart.
			// Only update total if it was zero (GetRowCount failed earlier).
			if s.TableDetails[idx].Total == 0 && ev.RowsTotal > 0 {
				s.Rows.Total += ev.RowsTotal
				s.TableDetails[idx].Total = ev.RowsTotal
			}
			// Only count as new in-progress if not already in progress.
			if s.TableDetails[idx].Status != "in_progress" {
				s.Tables.Total++
				s.Tables.InProgress++
			}
			s.TableDetails[idx].Status = "in_progress"
		} else {
			// Table not yet registered (EventMigrationStart was dropped).
			s.Rows.Total += ev.RowsTotal
			s.Tables.Total++
			s.Tables.InProgress++
			idx = len(s.TableDetails)
			s.TableDetails = append(s.TableDetails, TableDetail{
				Name:   ev.TableName,
				Status: "in_progress",
				Total:  ev.RowsTotal,
			})
			c.tableIndex[ev.TableName] = idx
		}
		c.log.Info("migration.table_started",
			"table", ev.TableName,
			"rows_total", ev.RowsTotal,
		)

	case engine.EventBatch:
		s.CurrentTable = ev.TableName
		s.Rows.Transferred = c.totalTransferred(ev)
		c.updateRate(s)
		c.updateReadWriteRates(s, ev)
		// Ensure table is in tableIndex (EventTableStart may have been dropped).
		idx, ok := c.tableIndex[ev.TableName]
		if !ok {
			// Table not registered yet. Add it but DON'T add to Rows.Total here —
			// it may have been counted in EventMigrationStart already. We'll fix
			// the total below if transferred exceeds it.
			idx = len(s.TableDetails)
			s.TableDetails = append(s.TableDetails, TableDetail{
				Name:   ev.TableName,
				Status: "in_progress",
				Total:  ev.RowsTotal,
			})
			c.tableIndex[ev.TableName] = idx
		}
		s.TableDetails[idx].Transferred = ev.RowsTransferred
		s.TableDetails[idx].Status = "in_progress"
		// Update total if transferred exceeds it (MongoDB estimates can be low).
		if ev.RowsTransferred > s.TableDetails[idx].Total {
			diff := ev.RowsTransferred - s.TableDetails[idx].Total
			s.TableDetails[idx].Total = ev.RowsTransferred
			s.Rows.Total += diff
		}
		if s.Rows.RatePerSecond > 0 && s.Rows.Total > s.Rows.Transferred {
			s.ETASeconds = float64(s.Rows.Total-s.Rows.Transferred) / s.Rows.RatePerSecond
		}

	case engine.EventTableDone:
		s.Rows.Transferred = c.totalTransferred(ev)
		// Ensure table is in tableIndex (earlier events may have been dropped).
		idx, ok := c.tableIndex[ev.TableName]
		if !ok {
			// Table not registered yet. Don't add to Rows.Total here — it may
			// have been counted in EventMigrationStart.
			idx = len(s.TableDetails)
			s.TableDetails = append(s.TableDetails, TableDetail{
				Name:  ev.TableName,
				Total: ev.RowsTotal,
			})
			c.tableIndex[ev.TableName] = idx
		} else {
			// Only decrement InProgress if we previously counted it.
			if s.TableDetails[idx].Status == "in_progress" {
				s.Tables.InProgress--
			}
		}
		s.Tables.Completed++
		s.TableDetails[idx].Status = "done"
		s.TableDetails[idx].Transferred = ev.RowsTransferred
		// Update total if transferred exceeds it (MongoDB estimates can be low).
		if ev.RowsTransferred > s.TableDetails[idx].Total {
			diff := ev.RowsTransferred - s.TableDetails[idx].Total
			s.TableDetails[idx].Total = ev.RowsTransferred
			s.Rows.Total += diff
		}
		elapsed := time.Since(c.tableStarted[ev.TableName])
		c.log.Info("migration.table_completed",
			"table", ev.TableName,
			"rows_transferred", ev.RowsTransferred,
			"elapsed", fmt.Sprintf("%.1fs", elapsed.Seconds()),
		)

	case engine.EventTableFailed:
		// Ensure table is in tableIndex (earlier events may have been dropped).
		idx, ok := c.tableIndex[ev.TableName]
		if !ok {
			// Table not registered yet. Don't add to Rows.Total here — it may
			// have been counted in EventMigrationStart.
			idx = len(s.TableDetails)
			s.TableDetails = append(s.TableDetails, TableDetail{
				Name:  ev.TableName,
				Total: ev.RowsTotal,
			})
			c.tableIndex[ev.TableName] = idx
		} else {
			if s.TableDetails[idx].Status == "in_progress" {
				s.Tables.InProgress--
			}
		}
		s.Tables.Failed++
		s.TableDetails[idx].Status = "failed"
		if ev.Err != nil {
			s.TableDetails[idx].Error = ev.Err.Error()
		}
		c.log.Error("migration.table_failed",
			"table", ev.TableName,
			"error", ev.Err,
		)

	case engine.EventComplete:
		s.Phase = "complete"
		s.CurrentTable = ""
		s.ETASeconds = 0
		elapsed := time.Since(c.startedAt)
		c.log.Info("migration.completed",
			"rows_transferred", s.Rows.Transferred,
			"elapsed", fmt.Sprintf("%.1fs", elapsed.Seconds()),
			"tables_completed", s.Tables.Completed,
			"tables_failed", s.Tables.Failed,
		)

	case engine.EventError:
		s.Phase = "failed"
		if ev.Err != nil {
			s.Errors = append(s.Errors, ev.Err.Error())
		}
		elapsed := time.Since(c.startedAt)
		c.log.Error("migration.failed",
			"error", ev.Err,
			"elapsed", fmt.Sprintf("%.1fs", elapsed.Seconds()),
		)

	case engine.EventPaused:
		s.Phase = "paused"
		c.log.Info("migration.paused")

	case engine.EventResumed:
		s.Phase = "in_progress"
		c.lastSampleTime = time.Now()
		c.lastSampleRows = s.Rows.Transferred
		c.log.Info("migration.resumed")

	case engine.EventSchemaPhase:
		s.Phase = "schema"

	case engine.EventPostSchemaPhase:
		s.Phase = "post_schema"

	case engine.EventPostSchemaItem:
		s.PostSchemaStatus = ev.PostSchemaMsg

	case engine.EventResourceSample:
		if ev.Resource == nil {
			break
		}
		s.Resource = &ResourceStats{
			Goroutines: ev.Resource.Goroutines,
			MemAllocMB: ev.Resource.MemAllocMB,
			MemSysMB:   ev.Resource.MemSysMB,
			GCNum:      ev.Resource.GCNum,
			CPUPercent: ev.Resource.CPUPercent,
		}
	}

	s.Tables.Pending = s.Tables.Total - s.Tables.Completed - s.Tables.InProgress - s.Tables.Failed
}

// totalTransferred records the latest per-table row count from the event and
// returns the sum across all tables.
func (c *Collector) totalTransferred(ev engine.ProgressEvent) int64 {
	c.tableRows[ev.TableName] = ev.RowsTransferred
	var total int64
	for _, n := range c.tableRows {
		total += n
	}
	return total
}

// updateReadWriteRates computes per-batch EMA read and write rates from timing data.
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
			s.Rows.RatePerSecond = 0.3*instant + 0.7*s.Rows.RatePerSecond
		}
	}
	c.lastSampleTime = now
	c.lastSampleRows = s.Rows.Transferred
}
