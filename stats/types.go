package stats

import "time"

// StatsSnapshot is a point-in-time view of a migration's progress.
type StatsSnapshot struct {
	MigrationID    string
	Phase          string
	StartedAt      time.Time
	ElapsedSeconds float64
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
	RatePerSecond float64
}
