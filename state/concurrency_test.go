package state

import (
	"context"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"gitea.homelab.local/nextdevops/XferDB/adapters"
)

// TestConcurrentBookkeepingDoesNotLock reproduces the write pattern that
// failed tables with "database is locked" (#124): several table workers each
// saving a checkpoint and a progress row after every batch while readers poll
// progress, all against one state file. Every write must succeed.
func TestConcurrentBookkeepingDoesNotLock(t *testing.T) {
	// A 1 ms busy timeout (default 5 s) turns any contention for SQLite's
	// file lock into an immediate "database is locked", so the test passes
	// only if writers never meet the lock at all -- not just because a fast
	// local disk kept every wait under 5 s.
	m, err := Open(filepath.Join(t.TempDir(), "state.db") + "?_busy_timeout=1")
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()

	ctx := context.Background()
	p := &adapters.Project{ID: "p", Name: "p", CreatedAt: time.Now()}
	if err := m.CreateProject(ctx, p); err != nil {
		t.Fatal(err)
	}
	if err := m.CreateMigration(ctx, &adapters.Migration{ID: "m", ProjectID: "p", Status: adapters.StatusInProgress, CreatedAt: time.Now()}); err != nil {
		t.Fatal(err)
	}

	const workers, batches = 8, 200
	errs := make(chan error, workers*batches*2+1)
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			table := fmt.Sprintf("t%d", w)
			for b := 1; b <= batches; b++ {
				if err := m.SaveCheckpoint(ctx, adapters.Checkpoint{
					MigrationID: "m", TableName: table, BatchID: b, RowsInBatch: 1000, CreatedAt: time.Now(),
				}); err != nil {
					errs <- fmt.Errorf("worker %d checkpoint %d: %w", w, b, err)
					return
				}
				if err := m.UpsertTableProgress(ctx, "m", adapters.TableProgress{
					TableName: table, Status: adapters.StatusInProgress, RowsTransferred: int64(b * 1000),
				}); err != nil {
					errs <- fmt.Errorf("worker %d progress %d: %w", w, b, err)
					return
				}
			}
		}(w)
	}

	// Readers, like the CLI polling /stats while the migration runs.
	stop := make(chan struct{})
	var readers sync.WaitGroup
	for r := 0; r < 2; r++ {
		readers.Add(1)
		go func() {
			defer readers.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				if _, err := m.GetTableProgress(ctx, "m"); err != nil {
					errs <- fmt.Errorf("reader: %w", err)
					return
				}
				if _, err := m.GetProjectTables(ctx, "p"); err != nil {
					errs <- fmt.Errorf("reader: %w", err)
					return
				}
			}
		}()
	}

	wg.Wait()
	close(stop)
	readers.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}

	progress, err := m.GetTableProgress(ctx, "m")
	if err != nil {
		t.Fatal(err)
	}
	if len(progress) != workers {
		t.Fatalf("got progress for %d tables, want %d", len(progress), workers)
	}
	for _, tp := range progress {
		if tp.RowsTransferred != batches*1000 {
			t.Errorf("%s: rows %d, want %d", tp.TableName, tp.RowsTransferred, batches*1000)
		}
	}
}
