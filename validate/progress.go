package validate

import (
	"sync"
	"sync/atomic"
	"time"
)

// Steps a table's validation goes through, in Progress.Step.
const (
	StepCounting = "counting" // L1: counting rows on both sides
	StepReading  = "reading"  // L2: fingerprinting the source and target rows, both at once
	StepRowDiff  = "row_diff" // L3: diffing by primary key; no row total
)

// Progress is how far the validation of one table has got. RowsTotal is
// every row L2 reads, source and target together (the two counts), so
// RowsDone/RowsTotal is the share of the reading done; it is 0 while
// counting, at L1, and during an L3 row diff, whose length isn't known up
// front.
type Progress struct {
	Table     string    `json:"table_name"`
	Step      string    `json:"step"`
	RowsDone  int64     `json:"rows_done"`
	RowsTotal int64     `json:"rows_total"`
	StartedAt time.Time `json:"started_at"`
}

// tracker reports a table's Progress to fn: at every step change, and while
// reading, once per `every` rows rather than per row.
type tracker struct {
	fn    func(Progress)
	every int64
	// done counts rows read so far and last the count at the latest emit;
	// both are atomics so the source and target readers, which run at once,
	// can count without taking mu for every row.
	done atomic.Int64
	last atomic.Int64

	mu sync.Mutex // guards p and serialises fn
	p  Progress
}

func newTracker(fn func(Progress), table string, every int) *tracker {
	if every <= 0 {
		every = defaultBatchSize
	}
	return &tracker{fn: fn, p: Progress{Table: table, StartedAt: time.Now()}, every: int64(every)}
}

func (t *tracker) step(step string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.p.Step = step
	t.emitLocked()
}

func (t *tracker) total(rows int64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.p.RowsTotal = rows
}

// row counts one row read, on either side; safe for concurrent use.
func (t *tracker) row() {
	n := t.done.Add(1)
	if n-t.last.Load() < t.every {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.done.Load()-t.last.Load() >= t.every { // not emitted by the other side meanwhile
		t.emitLocked()
	}
}

func (t *tracker) emitLocked() {
	t.p.RowsDone = t.done.Load()
	t.last.Store(t.p.RowsDone)
	if t.fn != nil {
		t.fn(t.p)
	}
}
