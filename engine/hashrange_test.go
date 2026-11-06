package engine

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"gitea.homelab.local/nextdevops/XferDB/adapters"
)

// The chunks cover the whole hash space, contiguously, about a batch of
// rows each, never fewer than the workers or more than the hash codes.
func TestHashChunks(t *testing.T) {
	for _, c := range []struct {
		total          int64
		batch, workers int
		want           int
	}{
		{1_000_000, 50_000, 4, 20},
		{1_000_000, 1000, 4, 1000},
		{10, 50_000, 6, 6},
		{0, 1000, 1, 1},
		{1 << 40, 1, 4, adapters.HashSpace},
	} {
		n := hashChunks(c.total, c.batch, c.workers)
		if n != c.want {
			t.Errorf("hashChunks(%d, %d, %d) = %d, want %d", c.total, c.batch, c.workers, n, c.want)
		}
		next := 0
		for i := 0; i < n; i++ {
			lo, hi := hashChunkRange(i, n)
			if lo != next || hi <= lo {
				t.Fatalf("%d chunks: chunk %d is [%d, %d), expected to start at %d", n, i, lo, hi, next)
			}
			next = hi
		}
		if next != adapters.HashSpace {
			t.Fatalf("%d chunks end at %d", n, next)
		}
	}
}

func TestParseHashCheckpoint(t *testing.T) {
	if n, i, ok := parseHashCheckpoint("yb_hash:20:7"); !ok || n != 20 || i != 7 {
		t.Fatalf("got %d %d %v", n, i, ok)
	}
	for _, bad := range []string{"", `["5"]`, "yb_hash:20", "yb_hash:20:20", "yb_hash:0:0", "yb_hash:x:1"} {
		if _, _, ok := parseHashCheckpoint(bad); ok {
			t.Errorf("parsed %q", bad)
		}
	}
}

// hashSource is a hash-sharded table of n rows, row id's hash code being
// id*7919 mod HashSpace. Workers read it concurrently.
type hashSource struct {
	offsetSource
	n      int
	mu     sync.Mutex
	ranges [][2]int // every range read
}

func (s *hashSource) GetRowCount(context.Context, string) (int64, error) { return int64(s.n), nil }
func (s *hashSource) HashColumns(context.Context, string) ([]string, error) {
	return []string{"id"}, nil
}
func (s *hashSource) ReadHashRange(_ context.Context, _ string, _ []string, lo, hi int) (*adapters.Batch, error) {
	s.mu.Lock()
	s.ranges = append(s.ranges, [2]int{lo, hi})
	s.mu.Unlock()
	var recs []map[string]interface{}
	for id := 0; id < s.n; id++ {
		if h := id * 7919 % adapters.HashSpace; h >= lo && h < hi {
			recs = append(recs, map[string]interface{}{"id": id})
		}
	}
	return &adapters.Batch{Records: recs, Size: len(recs)}, nil
}

// idTarget counts how often each id is written.
type idTarget struct {
	devNullTarget
	mu     sync.Mutex
	writes map[int]int
}

func (t *idTarget) WriteBatch(_ context.Context, _ string, b *adapters.Batch) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.writes == nil {
		t.writes = map[int]int{}
	}
	for _, r := range b.Records {
		t.writes[r["id"].(int)]++
	}
	return nil
}

// Every row is copied exactly once, by several workers, with one
// checkpoint per chunk and the table recorded as completed.
func TestHashRangeCopy(t *testing.T) {
	src, tgt := &hashSource{n: 10000}, &idTarget{}
	eng, db, cleanup := newTestEngine(t, src, tgt)
	defer cleanup()
	eng.project.TransferConfig.BatchSize = 500
	eng.project.TransferConfig.SegmentWorkers = 4
	ctx := context.Background()

	if err := eng.transferTable(ctx, adapters.TableSchema{Name: "orders"}); err != nil {
		t.Fatal(err)
	}
	if len(tgt.writes) != 10000 {
		t.Fatalf("%d rows written, want 10000", len(tgt.writes))
	}
	for id, n := range tgt.writes {
		if n != 1 {
			t.Fatalf("row %d written %d times", id, n)
		}
	}
	if len(src.ranges) != 20 {
		t.Errorf("%d ranges read, want 20 (10000 rows / 500)", len(src.ranges))
	}
	cps, _ := db.ListCheckpoints(ctx, eng.migrationID)
	if len(cps) != 20 {
		t.Errorf("%d checkpoints, want 20", len(cps))
	}
	tp, err := db.GetTableProgress(ctx, eng.migrationID)
	if err != nil || len(tp) != 1 || tp[0].Status != adapters.StatusCompleted || tp[0].RowsTransferred != 10000 {
		t.Fatalf("table progress = %+v, %v", tp, err)
	}
}

// A resumed copy reads only the chunks without a checkpoint, cut the way
// the first run cut them even though the row count has changed since.
func TestHashRangeResume(t *testing.T) {
	src, tgt := &hashSource{n: 10000}, &idTarget{}
	eng, db, cleanup := newTestEngine(t, src, tgt)
	defer cleanup()
	eng.project.TransferConfig.BatchSize = 1000 // would now cut 10 chunks
	eng.project.TransferConfig.SegmentWorkers = 3
	ctx := context.Background()

	// The first run cut 20 chunks and copied the first 8 (500 rows each).
	for i := 0; i < 8; i++ {
		if err := db.SaveCheckpoint(ctx, adapters.Checkpoint{
			MigrationID: eng.migrationID, TableName: "orders", BatchID: i + 1,
			LastPK: fmt.Sprintf("yb_hash:20:%d", i), RowsInBatch: 500, CreatedAt: time.Now(),
		}); err != nil {
			t.Fatal(err)
		}
	}
	if err := eng.transferTable(ctx, adapters.TableSchema{Name: "orders"}); err != nil {
		t.Fatal(err)
	}
	if len(src.ranges) != 12 {
		t.Fatalf("%d ranges read on resume, want 12", len(src.ranges))
	}
	firstUncopied, _ := hashChunkRange(8, 20)
	for _, r := range src.ranges {
		if r[0] < firstUncopied {
			t.Fatalf("re-read an already copied range %v", r)
		}
	}
	tp, _ := db.GetTableProgress(ctx, eng.migrationID)
	if len(tp) != 1 || tp[0].Status != adapters.StatusCompleted || tp[0].RowsTransferred != 8*500+int64(len(tgt.writes)) {
		t.Fatalf("table progress = %+v (wrote %d)", tp, len(tgt.writes))
	}
}

// A table a previous run began copying in key order keeps that way: its
// checkpoints are keys, not chunks.
func TestHashRangeNotAfterKeyCheckpoints(t *testing.T) {
	src := &hashSource{n: 10}
	eng, db, cleanup := newTestEngine(t, src, &idTarget{})
	defer cleanup()
	ctx := context.Background()
	if cols, hrs := eng.hashRangeColumns(ctx, "orders"); hrs == nil || len(cols) != 1 {
		t.Fatal("a hash-sharded table without checkpoints should be copied by hash ranges")
	}
	if err := db.SaveCheckpoint(ctx, adapters.Checkpoint{
		MigrationID: eng.migrationID, TableName: "orders", BatchID: 1, LastPK: `["5"]`, RowsInBatch: 5, CreatedAt: time.Now(),
	}); err != nil {
		t.Fatal(err)
	}
	if _, hrs := eng.hashRangeColumns(ctx, "orders"); hrs != nil {
		t.Fatal("a table with key checkpoints switched to hash ranges")
	}
}
