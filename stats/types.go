package stats

import "time"

// MigrationConfig captures the effective settings used for a migration run.
// Shown in the --status display so the user can confirm what is actually running.
type MigrationConfig struct {
	BatchSize      int `json:"batch_size"`
	TableWorkers   int `json:"table_workers"`
	SegmentWorkers int `json:"segment_workers"`
}

// StatsSnapshot is a point-in-time view of a migration's progress.
type StatsSnapshot struct {
	MigrationID    string
	Phase          string
	StartedAt      time.Time
	ElapsedSeconds float64
	Config         MigrationConfig
	Tables         TableStats
	TableDetails   []TableDetail
	Rows           RowStats
	CurrentTable   string
	ETASeconds     float64
	Errors         []string
}

// TableStats counts tables by state.
type TableStats struct {
	Total      int
	Completed  int
	InProgress int
	Pending    int
}

// TableDetail holds per-table progress for display.
type TableDetail struct {
	Name        string
	Status      string // "pending", "in_progress", "done"
	Transferred int64
	Total       int64
}

// RowStats tracks row-level transfer progress and throughput.
type RowStats struct {
	Total         int64
	Transferred   int64
	RatePerSecond float64 // overall throughput (rows written/s), time-windowed
	ReadRate      float64 // source read throughput (rows/s), per-batch EMA
	WriteRate     float64 // target write throughput (rows/s), per-batch EMA
}
