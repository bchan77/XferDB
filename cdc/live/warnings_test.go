package live

import (
	"strings"
	"testing"
	"time"

	"gitea.homelab.local/nextdevops/XferDB/cdc"
)

func TestWarnings(t *testing.T) {
	gib := int64(1 << 30)
	free := 100 * gib
	queueDiskFree = func(string) (int64, int64, bool) { return free, 500 * gib, true }
	defer func() { queueDiskFree = diskFree }()
	safe := func(n int64) *int64 { return &n }
	running := &cdc.CaptureStatus{}
	for _, c := range []struct {
		name string
		st   Status
		down time.Duration // how long capture has been seen stopped
		want string        // "" for none
	}{
		{"all well", Status{Capture: running, Retention: &cdc.Retention{RetainedBytes: gib, State: "reserved"}}, 0, ""},
		{"no limit", Status{Capture: running, Retention: &cdc.Retention{RetainedBytes: 50 * gib}}, 0, ""},
		{"capture just stopped", Status{Retention: &cdc.Retention{RetainedBytes: gib}}, 10 * time.Second, ""},
		{"capture stopped long", Status{Retention: &cdc.Retention{RetainedBytes: 3 * gib / 2}}, 2 * time.Minute, "(1.5 GiB of WAL so far)"},
		{"past the limit", Status{Capture: running, Retention: &cdc.Retention{RetainedBytes: 9 * gib, State: "unreserved"}}, 0, "more WAL for this migration (9.0 GiB) than max_slot_wal_keep_size"},
		{"near the limit", Status{Capture: running, Retention: &cdc.Retention{RetainedBytes: 8 * gib, SafeBytes: safe(gib), State: "extended"}}, 0, "Only 1.0 GiB more WAL"},
		{"well within the limit", Status{Capture: running, Retention: &cdc.Retention{RetainedBytes: gib, SafeBytes: safe(8 * gib), State: "reserved"}}, 0, ""},
	} {
		t.Run(c.name, func(t *testing.T) {
			r := &run{}
			if c.down > 0 {
				r.captureDown = time.Now().Add(-c.down)
			}
			got := strings.Join(r.warnings(c.st), " ")
			if (c.want == "") != (got == "") || !strings.Contains(got, c.want) {
				t.Fatalf("warnings = %q, want one with %q", got, c.want)
			}
		})
	}
	// The disk the queue is on filling up.
	free = gib / 2
	if got := strings.Join((&run{}).warnings(Status{Capture: running, QueueBytes: 3 * gib}), " "); !strings.Contains(got, "(3.0 GiB now) has only 512.0 MiB free") {
		t.Fatalf("low disk: %q", got)
	}
	free = 100 * gib

	// Capture seen running again clears its clock.
	r := &run{captureDown: time.Now().Add(-time.Hour)}
	r.warnings(Status{Capture: running})
	if !r.captureDown.IsZero() {
		t.Fatal("captureDown not reset once capture runs")
	}
}

// The change rate is over the last minute, and nil until there are two
// samples.
func TestChangeRate(t *testing.T) {
	r := &run{}
	t0 := time.Now()
	if got := r.changeRate(t0, 100); got != nil {
		t.Fatalf("one sample: %v", *got)
	}
	if got := r.changeRate(t0.Add(10*time.Second), 150); got == nil || *got != 5 {
		t.Fatalf("10s, 50 changes: %v", got)
	}
	// Two minutes later only the last minute counts.
	r.changeRate(t0.Add(70*time.Second), 400)
	if got := r.changeRate(t0.Add(130*time.Second), 400); got == nil || *got != 0 {
		t.Fatalf("idle minute: %v", *got)
	}
}
