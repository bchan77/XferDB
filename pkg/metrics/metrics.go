package metrics

import (
	"sync/atomic"
	"time"
)

// Metrics tracks transfer progress
type Metrics struct {
	BytesRead    atomic.Uint64
	BytesWritten atomic.Uint64
	RowsRead     atomic.Uint64
	RowsWritten  atomic.Uint64
	StartTime    time.Time
}

// New creates a new Metrics tracker
func New() *Metrics {
	return &Metrics{
		StartTime: time.Now(),
	}
}

// AddBytesRead adds to the bytes read counter
func (m *Metrics) AddBytesRead(n uint64) {
	m.BytesRead.Add(n)
}

// AddBytesWritten adds to the bytes written counter
func (m *Metrics) AddBytesWritten(n uint64) {
	m.BytesWritten.Add(n)
}

// AddRowsRead adds to the rows read counter
func (m *Metrics) AddRowsRead(n uint64) {
	m.RowsRead.Add(n)
}

// AddRowsWritten adds to the rows written counter
func (m *Metrics) AddRowsWritten(n uint64) {
	m.RowsWritten.Add(n)
}

// Elapsed returns the time elapsed since start
func (m *Metrics) Elapsed() time.Duration {
	return time.Since(m.StartTime)
}
