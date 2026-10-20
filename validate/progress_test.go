package validate

import (
	"context"
	"reflect"
	"testing"

	"gitea.homelab.local/nextdevops/XferDB/state"
)

// TestValidateReportsProgress checks the progress of an L2 run over items (3
// rows, so 6 read across both sides) with a batch size of 2: every step in
// order, reports while reading, and the reading done by the end.
func TestValidateReportsProgress(t *testing.T) {
	p, _ := migrated(t)
	var got []Progress
	err := Run(context.Background(), Request{
		Project: p, Tables: []string{"items"}, Level: LevelL2, BatchSize: 2,
		OnProgress: func(pr Progress) { got = append(got, pr) },
	}, func(state.ValidationTable) {})
	if err != nil {
		t.Fatal(err)
	}

	var steps []string
	for i, pr := range got {
		if pr.Table != "items" || pr.StartedAt.IsZero() {
			t.Errorf("report %d = %+v, want table items with a start time", i, pr)
		}
		if i > 0 && pr.RowsDone < got[i-1].RowsDone {
			t.Errorf("rows went backwards: %+v after %+v", pr, got[i-1])
		}
		if len(steps) == 0 || steps[len(steps)-1] != pr.Step {
			steps = append(steps, pr.Step)
		}
	}
	if want := []string{StepCounting, StepReadingSource, StepReadingTarget}; !reflect.DeepEqual(steps, want) {
		t.Errorf("steps %v, want %v", steps, want)
	}
	last := got[len(got)-1]
	if last.RowsTotal != 6 || last.RowsDone < 4 {
		t.Errorf("last report %+v, want 6 rows in total and most of them read", last)
	}
	// One report per step change plus one per 2 rows read: not one per row.
	if len(got) > 3+6/2 {
		t.Errorf("%d reports for 6 rows at batch size 2: %+v", len(got), got)
	}
}
