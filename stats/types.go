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
	ProjectID      string
	Phase          string
	StartedAt      time.Time
	ElapsedSeconds float64
	Config         MigrationConfig
	Tables         TableStats
	TableDetails   []TableDetail
	Rows           RowStats
	CurrentTable     string
	ETASeconds       float64
	PostSchemaStatus string // last post-schema item being built (index/constraint name)
	Errors           []string
	Resource       *ResourceStats `json:"resource,omitempty"`
}

// TableStats counts tables by state.
type TableStats struct {
	Total      int
	Completed  int
	InProgress int
	Pending    int
	Failed     int
	Cancelled  int
}

// TableDetail holds per-table progress for display.
type TableDetail struct {
	Name        string
	Status      string // "pending", "in_progress", "done", "failed", "cancelled"
	Transferred int64
	Total       int64
	Error       string // set when Status == "failed"
}

// RowStats tracks row-level transfer progress and throughput.
type RowStats struct {
	Total         int64
	Transferred   int64
	RatePerSecond float64 // overall throughput (rows written/s), time-windowed
	ReadRate      float64 // source read throughput (rows/s), per-batch EMA
	WriteRate     float64 // target write throughput (rows/s), per-batch EMA
}

// ResourceStats holds point-in-time process resource consumption for a migration.
type ResourceStats struct {
	Goroutines int     // current goroutine count
	MemAllocMB float64 // heap allocation (MiB)
	MemSysMB   float64 // total OS memory obtained (MiB)
	GCNum      uint32  // completed GC cycles
	CPUPercent  float64 // fraction of one CPU; multiply by GOMAXPROCS for total
}
