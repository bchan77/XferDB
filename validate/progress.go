package validate

import "time"

// Steps a table's validation goes through, in Progress.Step.
const (
	StepCounting      = "counting"       // L1: counting rows on both sides
	StepReadingSource = "reading_source" // L2: fingerprinting the source rows
	StepReadingTarget = "reading_target" // L2: fingerprinting the target rows
	StepRowDiff       = "row_diff"       // L3: diffing by primary key; no row total
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
	p     Progress
	every int64
	last  int64
}

func newTracker(fn func(Progress), table string, every int) *tracker {
	if every <= 0 {
		every = defaultBatchSize
	}
	return &tracker{fn: fn, p: Progress{Table: table, StartedAt: time.Now()}, every: int64(every)}
}

func (t *tracker) step(step string) {
	t.p.Step = step
	t.emit()
}

func (t *tracker) total(rows int64) { t.p.RowsTotal = rows }

func (t *tracker) row() {
	t.p.RowsDone++
	if t.p.RowsDone-t.last >= t.every {
		t.emit()
	}
}

func (t *tracker) emit() {
	t.last = t.p.RowsDone
	if t.fn != nil {
		t.fn(t.p)
	}
}
