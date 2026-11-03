package live

import (
	"strings"
	"testing"
	"time"

	"gitea.homelab.local/nextdevops/XferDB/cdc"
)

func TestWarnings(t *testing.T) {
	gib := int64(1 << 30)
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
	// Capture seen running again clears its clock.
	r := &run{captureDown: time.Now().Add(-time.Hour)}
	r.warnings(Status{Capture: running})
	if !r.captureDown.IsZero() {
		t.Fatal("captureDown not reset once capture runs")
	}
}
