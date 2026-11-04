package cdc

import (
	"context"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"
)

func TestCodecRoundTrip(t *testing.T) {
	ts := time.Date(2026, 10, 27, 10, 30, 0, 123456000, time.FixedZone("", -4*3600))
	c := Change{
		Seq:    42,
		Table:  "public.orders",
		Op:     OpUpdate,
		Key:    map[string]interface{}{"id": int64(7)},
		OldKey: map[string]interface{}{"id": int64(6)},
		After: map[string]interface{}{
			"id": int64(7), "name": "Bob", "ok": true, "no": false, "f32": float32(1.5),
			"f64": 2.25, "u": uint64(1 << 63), "raw": []byte{0, 1, 2}, "empty": []byte{},
			"null": nil, "at": ts, "num": []byte("12.50"),
		},
		Unchanged: []string{"big_text"},
		Pos:       Position{1, 2, 3},
		CommitAt:  time.Unix(1700000000, 5).UTC(),
	}
	b, err := encodeChange(nil, &c)
	if err != nil {
		t.Fatal(err)
	}
	got, err := decodeChange(b)
	if err != nil {
		t.Fatal(err)
	}
	gotAt := got.After["at"].(time.Time)
	if !gotAt.Equal(ts) {
		t.Fatalf("time: got %v, want %v", gotAt, ts)
	}
	if _, off := gotAt.Zone(); off != -4*3600 {
		t.Fatalf("time offset lost: %d", off)
	}
	delete(got.After, "at")
	delete(c.After, "at")
	if !reflect.DeepEqual(got, c) {
		t.Fatalf("round trip:\n got %#v\nwant %#v", got, c)
	}

	// nil maps stay nil and empty ones stay empty.
	b, _ = encodeChange(nil, &Change{Table: "t", Op: OpTruncate, Key: map[string]interface{}{}})
	got, err = decodeChange(b)
	if err != nil {
		t.Fatal(err)
	}
	if got.Key == nil || got.After != nil || got.OldKey != nil {
		t.Fatalf("nil/empty maps: %#v", got)
	}

	// A schema change keeps both column lists; a renamed table has no old one.
	for _, c := range []Change{
		{Seq: 9, Table: "orders", Op: OpSchema,
			Columns:    []Column{{"id", "integer"}, {"note", "character varying(40)"}},
			OldColumns: []Column{{"id", "integer"}}},
		{Seq: 10, Table: "orders2", Op: OpSchema, Columns: []Column{}},
	} {
		b, err := encodeChange(nil, &c)
		if err != nil {
			t.Fatal(err)
		}
		got, err := decodeChange(b)
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(got, c) {
			t.Fatalf("schema change round trip:\n got %#v\nwant %#v", got, c)
		}
	}

	if _, err := encodeChange(nil, &Change{After: map[string]interface{}{"x": 3}}); err == nil {
		t.Fatal("int (not int64) should be refused")
	}
	if _, err := decodeChange(b[:len(b)-1]); err == nil {
		t.Fatal("truncated record should fail")
	}
}

func txn(table string, n int, pos byte) []*Change {
	out := make([]*Change, n)
	for i := range out {
		out[i] = &Change{Table: table, Op: OpInsert, Key: map[string]interface{}{"id": int64(i)}}
	}
	out[n-1].Pos = Position{pos}
	return out
}

func readAll(t *testing.T, dir string, after uint64) []Change {
	t.Helper()
	r, err := OpenReader(dir, after)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	var out []Change
	for {
		c, ok, err := r.TryNext()
		if err != nil {
			t.Fatal(err)
		}
		if !ok {
			return out
		}
		out = append(out, c)
	}
}

func TestQueueAppendReadRotate(t *testing.T) {
	dir := t.TempDir()
	w, err := OpenWriter(dir, 200) // tiny segments to force rotation
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 10; i++ {
		if err := w.Append(txn("t", 3, byte(i+1))...); err != nil {
			t.Fatal(err)
		}
	}
	if got := readAll(t, dir, 0); len(got) != 0 {
		t.Fatalf("read %d changes before Sync", len(got))
	}
	if seq, err := w.Sync(); err != nil || seq != 30 {
		t.Fatalf("Sync = %d, %v", seq, err)
	}
	segs, _ := listSegments(dir)
	if len(segs) < 3 {
		t.Fatalf("expected rotation, got %d segments", len(segs))
	}
	got := readAll(t, dir, 0)
	if len(got) != 30 {
		t.Fatalf("read %d changes, want 30", len(got))
	}
	for i, c := range got {
		if c.Seq != uint64(i+1) {
			t.Fatalf("change %d has seq %d", i, c.Seq)
		}
	}
	if got := readAll(t, dir, 17); len(got) != 13 || got[0].Seq != 18 {
		t.Fatalf("read after 17: %d changes", len(got))
	}

	// Removing what's applied keeps every later seq readable.
	if err := RemoveThrough(dir, 20); err != nil {
		t.Fatal(err)
	}
	if got := readAll(t, dir, 20); len(got) != 10 || got[0].Seq != 21 {
		t.Fatalf("after RemoveThrough: %d changes", len(got))
	}
	if _, err := OpenReader(dir, 0); err != nil {
		t.Fatal(err)
	}
	r, _ := OpenReader(dir, 0)
	if _, _, err := r.TryNext(); err != ErrNotFound {
		t.Fatalf("reading a removed seq: %v", err)
	}
	w.Close()
}

func TestQueueRecoversAfterCrash(t *testing.T) {
	dir := t.TempDir()
	w, err := OpenWriter(dir, 0)
	if err != nil {
		t.Fatal(err)
	}
	w.Append(txn("t", 2, 1)...)
	w.Append(txn("t", 2, 2)...)
	w.Sync()
	// A transaction cut off before its commit, then a torn record.
	w.Append(txn("t", 3, 3)[:2]...)
	w.w.Flush()
	segs, _ := listSegments(dir)
	f, _ := os.OpenFile(filepath.Join(dir, segName(segs[0])), os.O_WRONLY|os.O_APPEND, 0)
	f.Write([]byte{0, 0, 0, 50, 1, 2})
	f.Close()
	// Simulate the process dying: drop the writer without Close.

	w2, err := OpenWriter(dir, 0)
	if err != nil {
		t.Fatal(err)
	}
	seq, pos := w2.Last()
	if seq != 4 || !reflect.DeepEqual(pos, Position{2}) {
		t.Fatalf("Last = %d %v, want 4 [2]", seq, pos)
	}
	w2.Append(txn("t", 1, 9)...)
	w2.Sync()
	got := readAll(t, dir, 0)
	if len(got) != 5 || got[4].Seq != 5 || !reflect.DeepEqual(got[4].Pos, Position{9}) {
		t.Fatalf("after recovery read %d changes: %+v", len(got), got)
	}
	w2.Close()
}

func TestQueueReaderSeesOnlyWholeTransactions(t *testing.T) {
	dir := t.TempDir()
	w, _ := OpenWriter(dir, 0)
	defer w.Close()
	w.Append(txn("t", 2, 1)...)
	w.Append(txn("t", 3, 2)[:2]...) // no Pos yet
	if seq, _ := w.Sync(); seq != 2 {
		t.Fatalf("Sync exposed seq %d mid-transaction", seq)
	}
	if got := readAll(t, dir, 0); len(got) != 2 {
		t.Fatalf("reader saw %d changes, want 2", len(got))
	}
}

func TestQueueReaderWaits(t *testing.T) {
	dir := t.TempDir()
	w, _ := OpenWriter(dir, 0)
	defer w.Close()
	r, _ := OpenReader(dir, 0)
	defer r.Close()
	go func() {
		time.Sleep(100 * time.Millisecond)
		w.Append(txn("t", 1, 1)...)
		w.Sync()
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	c, err := r.Next(ctx)
	if err != nil || c.Seq != 1 {
		t.Fatalf("Next = %+v, %v", c, err)
	}
}
