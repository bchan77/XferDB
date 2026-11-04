package cdc_test

import (
	"context"
	"path/filepath"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"gitea.homelab.local/nextdevops/XferDB/adapters"
	"gitea.homelab.local/nextdevops/XferDB/cdc"
)

// fakeSource streams a fixed list of changes, then waits.
type fakeSource struct {
	changes []cdc.Change
}

func (f *fakeSource) Prepare(context.Context, []string) (cdc.Position, error) { return nil, nil }
func (f *fakeSource) Stream(ctx context.Context, _ cdc.Position, out chan<- cdc.Change) error {
	for _, c := range f.changes {
		select {
		case out <- c:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	<-ctx.Done()
	return ctx.Err()
}
func (f *fakeSource) Ack(context.Context, cdc.Position) error               { return nil }
func (f *fakeSource) CurrentPosition(context.Context) (cdc.Position, error) { return nil, nil }
func (f *fakeSource) Sequences(context.Context) (map[string]int64, error)   { return nil, nil }
func (f *fakeSource) Teardown(context.Context) error                        { return nil }
func (f *fakeSource) Confirmed() cdc.Position                               { return nil }
func (f *fakeSource) Columns(context.Context, []string) (map[string][]cdc.Column, error) {
	return nil, nil
}
func (f *fakeSource) Retention(context.Context) (cdc.Retention, error) { return cdc.Retention{}, nil }
func (f *fakeSource) Behind(_, _ cdc.Position) int64                   { return 0 }
func (f *fakeSource) FormatPosition(p cdc.Position) string             { return string(p) }
func (f *fakeSource) ParsePosition(s string) (cdc.Position, error)     { return cdc.Position(s), nil }
func (f *fakeSource) Close() error                                     { return nil }

// capture runs a capturer over src until it has queued seq changes.
func capture(t *testing.T, c *cdc.Capturer, seq uint64) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- c.Run(ctx) }()
	deadline := time.After(10 * time.Second)
	for c.Status().QueuedSeq < seq {
		select {
		case err := <-done:
			t.Fatalf("capture stopped: %v", err)
		case <-deadline:
			t.Fatalf("queued %d of %d", c.Status().QueuedSeq, seq)
		case <-time.After(10 * time.Millisecond):
		}
	}
	cancel()
	if err := <-done; err != context.Canceled {
		t.Fatalf("Run: %v", err)
	}
}

// TestCaptureQueuesOnlyRealSchemaChanges: a source describes a table's
// columns at the start of every session as well as after DDL. Only a
// description that differs from what the capturer knows is queued, also
// across a restart, and a table it doesn't know by name (renamed) is one.
func TestCaptureQueuesOnlyRealSchemaChanges(t *testing.T) {
	v1 := []cdc.Column{{Name: "id", Type: "integer"}, {Name: "v", Type: "text"}}
	v2 := append(append([]cdc.Column{}, v1...), cdc.Column{Name: "note", Type: "character varying(40)"})
	schema := func(table string, cols []cdc.Column) cdc.Change {
		return cdc.Change{Table: table, Op: cdc.OpSchema, Columns: cols}
	}
	ins := func(table string, id int64, pos string) cdc.Change {
		return cdc.Change{Table: table, Op: cdc.OpInsert, Key: key(id), After: map[string]interface{}{"id": id}, Pos: cdc.Position(pos)}
	}
	dir := t.TempDir()
	schemaFile := filepath.Join(t.TempDir(), "schema.json")
	w, err := cdc.OpenWriter(dir, 0)
	if err != nil {
		t.Fatal(err)
	}
	src := &fakeSource{changes: []cdc.Change{
		schema("a", v1), ins("a", 1, "p1"), // as known: nothing queued
		schema("a", v2), ins("a", 2, "p2"), // a column added
		schema("a2", v2), ins("a2", 3, "p3"), // a renamed
	}}
	capture(t, &cdc.Capturer{Source: src, Queue: w, Schema: map[string][]cdc.Column{"a": v1}, SchemaFile: schemaFile}, 5)

	// Restarted, the source describes a2 again as it now is: known.
	src.changes = []cdc.Change{schema("a2", v2), ins("a2", 4, "p4")}
	capture(t, &cdc.Capturer{Source: src, Queue: w, Schema: map[string][]cdc.Column{"a": v1}, SchemaFile: schemaFile}, 6)
	w.Close()

	var got []string
	r, err := cdc.OpenReader(dir, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	for {
		c, ok, err := r.TryNext()
		if err != nil {
			t.Fatal(err)
		}
		if !ok {
			break
		}
		s := c.Op.String() + " " + c.Table
		if c.Op == cdc.OpSchema {
			s += ": " + strings.Join(cdc.DescribeColumns(c.OldColumns, c.Columns), "; ")
		}
		got = append(got, s)
	}
	want := []string{
		"insert a",
		"schema change a: column note added (character varying(40))",
		"insert a",
		"schema change a2: a table this migration doesn't know by that name: was a table renamed?",
		"insert a2",
		"insert a2",
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("queued:\n%s\nwant:\n%s", strings.Join(got, "\n"), strings.Join(want, "\n"))
	}
}

func TestDescribeColumns(t *testing.T) {
	old := []cdc.Column{{Name: "id", Type: "integer"}, {Name: "v", Type: "text"}, {Name: "price", Type: "numeric(10,2)"}}
	new := []cdc.Column{{Name: "id", Type: "bigint"}, {Name: "price", Type: "numeric(10,2)"}, {Name: "w", Type: "text"}}
	got := cdc.DescribeColumns(old, new)
	want := []string{"column id changed type: integer → bigint", "column w added (text)", "column v dropped"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %q, want %q", got, want)
	}
}

// TestApplyStopsAtSchemaChange: apply writes every change before a schema
// change, then stops and reports it, writing nothing after it (of any
// table) until it is accepted.
func TestApplyStopsAtSchemaChange(t *testing.T) {
	tgt, path := sqliteTarget(t)
	dir := t.TempDir()
	w, err := cdc.OpenWriter(dir, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	ins := func(table string, id int64, v string) cdc.Change {
		return cdc.Change{Table: table, Op: cdc.OpInsert, Key: key(id), After: row(id, v, "B")}
	}
	appendTxn(t, w, ins("a", 1, "x"), ins("a", 2, "y"))
	appendTxn(t, w, cdc.Change{Table: "a", Op: cdc.OpSchema,
		Columns:    []cdc.Column{{Name: "id", Type: "integer"}, {Name: "v", Type: "text"}, {Name: "big", Type: "text"}, {Name: "note", Type: "text"}},
		OldColumns: []cdc.Column{{Name: "id", Type: "integer"}, {Name: "v", Type: "text"}, {Name: "big", Type: "text"}}},
		ins("a", 3, "z"))
	appendTxn(t, w, ins("b", 1, "p"))
	last, _ := w.Last()

	var accepted atomic.Uint64
	a := &cdc.Applier{Stream: "s1", Dir: dir, Target: tgt.(adapters.ChangeApplier),
		Opts:           cdc.ApplyOptions{MaxWait: 10 * time.Millisecond},
		SchemaAccepted: accepted.Load}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- a.Run(ctx) }()
	defer func() {
		cancel()
		<-done
	}()
	wait := func(what string, ok func(cdc.ApplyStatus) bool) cdc.ApplyStatus {
		t.Helper()
		deadline := time.After(10 * time.Second)
		for {
			st := a.Status()
			if ok(st) {
				return st
			}
			select {
			case err := <-done:
				t.Fatalf("apply stopped: %v", err)
			case <-deadline:
				t.Fatalf("waiting for %s: %+v", what, st)
			case <-time.After(10 * time.Millisecond):
			}
		}
	}

	st := wait("the schema change", func(s cdc.ApplyStatus) bool { return s.Blocked != nil })
	if st.AppliedSeq != 2 || st.Blocked.Seq != 3 || st.Blocked.Table != "a" ||
		!reflect.DeepEqual(st.Blocked.Summary, []string{"column note added (text)"}) {
		t.Fatalf("blocked: %+v %+v", st, st.Blocked)
	}
	time.Sleep(100 * time.Millisecond) // nothing more goes in meanwhile
	if got := dump(t, path, "a") + " | " + dump(t, path, "b"); got != "1:x:B 2:y:B | " {
		t.Fatalf("before accepting: %q", got)
	}

	accepted.Store(3)
	wait("the rest", func(s cdc.ApplyStatus) bool { return s.AppliedSeq == last })
	if a.Status().Blocked != nil {
		t.Fatal("still blocked once accepted")
	}
	if got := dump(t, path, "a") + " | " + dump(t, path, "b"); got != "1:x:B 2:y:B 3:z:B | 1:p:B" {
		t.Fatalf("after accepting: %q", got)
	}
}
