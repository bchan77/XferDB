package sqlite

import (
	"context"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"gitea.homelab.local/nextdevops/XferDB/adapters"
)

// TestWriteBatchParallelTablesDoesNotLock runs several table workers writing
// to one SQLite target at once, as the engine does with --table-workers > 1.
// SQLite admits one writer per file; with a connection pool, a writer that
// waited past the busy timeout failed with "database is locked" (#124).
//
// The DSN's 1 ms busy timeout (default 5 s) makes any contention fail at
// once, so this passes only if the writers never meet the lock at all.
func TestWriteBatchParallelTablesDoesNotLock(t *testing.T) {
	for _, bulk := range []bool{false, true} {
		t.Run(fmt.Sprintf("bulk=%v", bulk), func(t *testing.T) {
			ctx := context.Background()
			dsn := filepath.Join(t.TempDir(), "target.db") + "?_busy_timeout=1"
			tgt := &Target{}
			if err := tgt.Connect(ctx, adapters.ConnectionConfig{Type: "sqlite", DSN: dsn}); err != nil {
				t.Fatalf("connect: %v", err)
			}
			defer tgt.Close()

			const workers, batches, rows = 4, 40, 200
			for w := 0; w < workers; w++ {
				if err := tgt.CreateTable(ctx, &adapters.TableSchema{
					Name: fmt.Sprintf("t%d", w),
					Columns: []adapters.ColumnDef{
						{Name: "id", Type: "INTEGER", PrimaryKey: true},
						{Name: "label", Type: "TEXT", Nullable: true},
					},
				}); err != nil {
					t.Fatalf("create table: %v", err)
				}
			}
			if bulk {
				tgt.EnableCopy()
			}

			errs := make(chan error, workers)
			var wg sync.WaitGroup
			for w := 0; w < workers; w++ {
				wg.Add(1)
				go func(w int) {
					defer wg.Done()
					table := fmt.Sprintf("t%d", w)
					for b := 0; b < batches; b++ {
						recs := make([]map[string]interface{}, rows)
						for i := range recs {
							id := b*rows + i
							recs[i] = map[string]interface{}{"id": id, "label": fmt.Sprintf("row-%d", id)}
						}
						if err := tgt.WriteBatch(ctx, table, &adapters.Batch{Records: recs, Size: rows}); err != nil {
							errs <- fmt.Errorf("%s batch %d: %w", table, b, err)
							return
						}
					}
				}(w)
			}
			wg.Wait()
			close(errs)
			for err := range errs {
				t.Error(err)
			}

			for w := 0; w < workers; w++ {
				var n int
				if err := tgt.db.QueryRowContext(ctx, fmt.Sprintf(`SELECT COUNT(*) FROM t%d`, w)).Scan(&n); err != nil {
					t.Fatal(err)
				}
				if n != batches*rows {
					t.Errorf("t%d: %d rows, want %d", w, n, batches*rows)
				}
			}
		})
	}
}

// TestCheckPermissionsReleasesConnection guards the single-connection pool:
// CheckPermissions used to leave its probe query's rows open, which with one
// connection would block every later statement forever.
func TestCheckPermissionsReleasesConnection(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	tgt := &Target{}
	if err := tgt.Connect(ctx, adapters.ConnectionConfig{Type: "sqlite", DSN: filepath.Join(t.TempDir(), "perm.db")}); err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer tgt.Close()

	check, err := tgt.CheckPermissions(ctx)
	if err != nil || !check.CanRead || !check.CanWrite {
		t.Fatalf("CheckPermissions = %+v, %v", check, err)
	}
	if err := tgt.CreateTable(ctx, &adapters.TableSchema{
		Name:    "after_check",
		Columns: []adapters.ColumnDef{{Name: "id", Type: "INTEGER", PrimaryKey: true}},
	}); err != nil {
		t.Fatalf("statement after CheckPermissions: %v (a leaked connection would time out here)", err)
	}
}
