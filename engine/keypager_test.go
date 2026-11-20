package engine

import (
	"context"
	"strconv"
	"testing"
	"time"

	"gitea.homelab.local/nextdevops/XferDB/adapters"
)

// pkSource is a SQL-like source of rows with ids 1..rows that can page by
// key (adapters.KeysetSource), like postgres and oracle. keyless makes its
// table have no primary key.
type pkSource struct {
	offsetSource
	rows    int
	keyless bool
}

func (s *pkSource) KeyColumns(context.Context, string) ([]string, error) {
	if s.keyless {
		return nil, nil
	}
	return []string{"id"}, nil
}

func (s *pkSource) MaxKey(context.Context, string) ([]string, error) {
	if s.rows == 0 {
		return nil, nil
	}
	return []string{strconv.Itoa(s.rows)}, nil
}

func (s *pkSource) GetRowCount(context.Context, string) (int64, error) { return int64(s.rows), nil }

func (s *pkSource) ReadBatch(ctx context.Context, table string, opts adapters.BatchOptions) (*adapters.Batch, error) {
	if !opts.Keyset {
		s.offsetSource.totalRows = s.rows
		return s.offsetSource.ReadBatch(ctx, table, opts)
	}
	s.calls = append(s.calls, opts)
	if opts.KeyUpTo == nil {
		return &adapters.Batch{}, nil
	}
	from := 1
	if opts.KeyAfter != nil {
		n, _ := strconv.Atoi(opts.KeyAfter[0])
		from = n + 1
	}
	upTo, _ := strconv.Atoi(opts.KeyUpTo[0])
	b := &adapters.Batch{}
	for id := from; id <= upTo && b.Size < opts.Limit; id++ {
		b.Records = append(b.Records, map[string]interface{}{"id": id})
		b.Size++
		b.LastKey = adapters.EncodeKey([]string{strconv.Itoa(id)})
	}
	return b, nil
}

// countingTarget counts the rows written.
type countingTarget struct {
	devNullTarget
	rows int
}

func (t *countingTarget) WriteBatch(_ context.Context, _ string, b *adapters.Batch) error {
	t.rows += b.Size
	return nil
}

// A single-worker copy of a table with a primary key pages by key, in
// both the sequential and the pipelined loop, and never by OFFSET.
func TestSingleWorkerPagesByKey(t *testing.T) {
	for _, pipelined := range []bool{false, true} {
		src := &pkSource{rows: 5}
		tgt := &countingTarget{}
		eng, db, cleanup := newTestEngine(t, src, tgt)
		eng.project.TransferConfig.BatchSize = 2
		ctx := context.Background()
		var err error
		if pipelined {
			err = eng.transferTablePipelined(ctx, adapters.TableSchema{Name: "t"})
		} else {
			err = eng.transferTableSequential(ctx, adapters.TableSchema{Name: "t"})
		}
		if err != nil {
			t.Fatalf("pipelined=%v: %v", pipelined, err)
		}
		if tgt.rows != 5 {
			t.Errorf("pipelined=%v: wrote %d rows, want 5", pipelined, tgt.rows)
		}
		var after []string
		for i, c := range src.calls {
			if !c.Keyset || c.Offset != 0 {
				t.Fatalf("pipelined=%v: call %d isn't a key read: %+v", pipelined, i, c)
			}
			if c.KeyUpTo[0] != "5" {
				t.Errorf("pipelined=%v: call %d bound %v, want [5]", pipelined, i, c.KeyUpTo)
			}
			if i == 0 && c.KeyAfter != nil || i > 0 && (c.KeyAfter == nil || c.KeyAfter[0] != after[0]) {
				t.Errorf("pipelined=%v: call %d after %v, want %v", pipelined, i, c.KeyAfter, after)
			}
			after = []string{strconv.Itoa(min(2*(i+1), 5))} // the last row of this read
		}
		cp, _ := db.GetLastCheckpoint(ctx, eng.migrationID, "t")
		if cp == nil || cp.LastPK != `["5"]` {
			t.Errorf("pipelined=%v: checkpoint %+v, want key [\"5\"]", pipelined, cp)
		}
		cleanup()
	}
}

// A copy resumed from a key checkpoint continues after that key.
func TestKeyPagingResumesAfterCheckpointedKey(t *testing.T) {
	src := &pkSource{rows: 5}
	tgt := &countingTarget{}
	eng, db, cleanup := newTestEngine(t, src, tgt)
	defer cleanup()
	eng.project.TransferConfig.BatchSize = 2
	ctx := context.Background()
	if err := db.SaveCheckpoint(ctx, adapters.Checkpoint{MigrationID: eng.migrationID, TableName: "t",
		BatchID: 1, LastPK: `["2"]`, RowsInBatch: 2, CreatedAt: time.Now()}); err != nil {
		t.Fatal(err)
	}
	if err := db.UpsertTableProgress(ctx, eng.migrationID, adapters.TableProgress{
		TableName: "t", Status: adapters.StatusInProgress, RowsTotal: 5, RowsTransferred: 2}); err != nil {
		t.Fatal(err)
	}
	if err := eng.transferTableSequential(ctx, adapters.TableSchema{Name: "t"}); err != nil {
		t.Fatal(err)
	}
	if first := src.calls[0]; !first.Keyset || first.KeyAfter[0] != "2" {
		t.Errorf("first read %+v, want by key after [2]", first)
	}
	if tgt.rows != 3 {
		t.Errorf("wrote %d rows, want the 3 after key 2", tgt.rows)
	}
}

// OFFSET stays for a table without a primary key, and for a copy an
// earlier version began by OFFSET (rows transferred, no key checkpoint):
// switching it to keys would start the table over.
func TestOffsetKeptWithoutKeyOrForOffsetResume(t *testing.T) {
	ctx := context.Background()

	src := &pkSource{rows: 3, keyless: true}
	eng, _, cleanup := newTestEngine(t, src, &devNullTarget{})
	eng.project.TransferConfig.BatchSize = 2
	if err := eng.transferTableSequential(ctx, adapters.TableSchema{Name: "t"}); err != nil {
		t.Fatal(err)
	}
	for _, c := range src.calls {
		if c.Keyset {
			t.Errorf("keyless table read by key: %+v", c)
		}
	}
	cleanup()

	src = &pkSource{rows: 5}
	eng, db, cleanup := newTestEngine(t, src, &devNullTarget{})
	defer cleanup()
	eng.project.TransferConfig.BatchSize = 2
	if err := db.UpsertTableProgress(ctx, eng.migrationID, adapters.TableProgress{
		TableName: "t", Status: adapters.StatusInProgress, RowsTotal: 5, RowsTransferred: 2}); err != nil {
		t.Fatal(err)
	}
	if err := eng.transferTableSequential(ctx, adapters.TableSchema{Name: "t"}); err != nil {
		t.Fatal(err)
	}
	if first := src.calls[0]; first.Keyset || first.Offset != 2 {
		t.Errorf("offset resume read %+v, want OFFSET 2", first)
	}
}
