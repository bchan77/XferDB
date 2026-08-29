package engine

import (
	"runtime"
	"runtime/metrics"
	"time"
)

// EventKind classifies a progress event.
type EventKind string

const (
	EventMigrationStart  EventKind = "migration_start" // emitted once with all table names
	EventTableStart      EventKind = "table_start"
	EventBatch           EventKind = "batch"
	EventTableDone       EventKind = "table_done"
	EventTableFailed     EventKind = "table_failed" // table failed; migration continues
	EventComplete        EventKind = "complete"
	EventError           EventKind = "error"
	EventPaused          EventKind = "paused"
	EventResumed         EventKind = "resumed"
	EventSchemaPhase     EventKind = "schema_phase"
	EventPostSchemaPhase EventKind = "post_schema_phase"
	EventResourceSample  EventKind = "resource_sample"
)

// ResourceSample holds a point-in-time snapshot of process resource usage.
type ResourceSample struct {
	Goroutines int     // current number of goroutines
	MemAllocMB float64 // heap memory currently allocated (MiB)
	MemSysMB   float64 // total memory obtained from the OS (MiB)
	GCNum      uint32  // number of completed GC cycles
	CPUPercent float64 // % of one logical CPU; >100 means multiple cores in use
}

// ProgressEvent is emitted by the engine after each meaningful state change.
// The stats collector and WebSocket handler both consume this channel.
type ProgressEvent struct {
	MigrationID     string
	Kind            EventKind
	TableName       string
	TableNames      []string         // set only for EventMigrationStart
	TableCounts     map[string]int64 // set only for EventMigrationStart: table → row count
	RowsTransferred int64
	RowsTotal       int64
	Timestamp       time.Time
	Err             error // set only for EventError
	// Batch timing — set only for EventBatch; used to compute separate read/write rates.
	BatchRows     int           // rows in this specific batch (not cumulative)
	ReadDuration  time.Duration // time spent in source.ReadBatch
	WriteDuration time.Duration // time spent in target.WriteBatch
	// Resource sample — set only for EventResourceSample.
	Resource *ResourceSample
}

// --- Resource sampling -------------------------------------------------------

var prevCPU struct {
	t       time.Time
	cpuSecs float64
}

// SampleResource reads current process resource usage and returns a ResourceSample.
// CPUPercent is derived from the Go runtime's scheduler CPU accounting
// (/cpu/classes/total:cpu-seconds), sampled as a delta between calls.
// Values above 100 indicate multiple cores in use (same semantics as top(1)).
func SampleResource() *ResourceSample {
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)

	samples := []metrics.Sample{
		{Name: "/cpu/classes/total:cpu-seconds"},
		{Name: "/cpu/classes/idle:cpu-seconds"},
	}
	metrics.Read(samples)
	cpuNow := samples[0].Value.Float64() - samples[1].Value.Float64()
	wallNow := time.Now()

	var cpuPercent float64
	if !prevCPU.t.IsZero() {
		wallDelta := wallNow.Sub(prevCPU.t).Seconds()
		if wallDelta > 0 {
			cpuPercent = (cpuNow - prevCPU.cpuSecs) / wallDelta * 100
		}
	}
	prevCPU.t = wallNow
	prevCPU.cpuSecs = cpuNow

	return &ResourceSample{
		Goroutines: runtime.NumGoroutine(),
		MemAllocMB: float64(ms.Alloc) / 1024 / 1024,
		MemSysMB:   float64(ms.Sys) / 1024 / 1024,
		GCNum:      ms.NumGC,
		CPUPercent: cpuPercent,
	}
}
