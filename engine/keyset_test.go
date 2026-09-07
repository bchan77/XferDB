package engine

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"gitea.homelab.local/nextdevops/XferDB/adapters"
	"gitea.homelab.local/nextdevops/XferDB/state"
)

// ── mock source adapters ────────────────────────────────────────────────────

// keysetSource simulates a keyset-capable adapter (e.g. MongoDB).
// Each call to ReadBatch returns one batch of records and sets LastKey.
// On resume it expects BatchOptions.LastPK to equal the last key returned.
type keysetSource struct {
	t         *testing.T
	calls     []adapters.BatchOptions // every BatchOptions passed to ReadBatch
	batches   int                     // total batches to serve before EOF
	served    int
	lastKeyFn func(n int) string // produces the LastKey for batch n (1-indexed)
}

func (s *keysetSource) ReadBatch(_ context.Context, _ string, opts adapters.BatchOptions) (*adapters.Batch, error) {
	s.calls = append(s.calls, opts)
	if s.served >= s.batches {
		return &adapters.Batch{}, nil
	}
	s.served++
	key := s.lastKeyFn(s.served)
	return &adapters.Batch{
		Records: []map[string]interface{}{{"_id": key, "n": s.served}},
		Size:    1,
		LastKey: key,
	}, nil
}

func (s *keysetSource) Connect(_ context.Context, _ adapters.ConnectionConfig) error { return nil }
func (s *keysetSource) Close() error                                                   { return nil }
func (s *keysetSource) Ping(_ context.Context) error                                   { return nil }
func (s *keysetSource) ListTables(_ context.Context) ([]adapters.TableSchema, error) {
	return nil, nil
}
func (s *keysetSource) GetSchema(_ context.Context, _ string) (*adapters.TableSchema, error) {
	return &adapters.TableSchema{Name: "col"}, nil
}
func (s *keysetSource) GetRowCount(_ context.Context, _ string) (int64, error) {
	return int64(s.batches), nil
}
func (s *keysetSource) GetPKRange(_ context.Context, _, _ string) (int64, int64, error) {
	return 0, 0, fmt.Errorf("not supported")
}
func (s *keysetSource) CheckPermissions(_ context.Context) (*adapters.PermissionCheck, error) {
	return &adapters.PermissionCheck{CanRead: true}, nil
}
func (s *keysetSource) GetInfo(_ context.Context) (*adapters.DatabaseInfo, error) {
	return &adapters.DatabaseInfo{Type: "MockKeyset"}, nil
}

// offsetSource simulates a SQL adapter that never sets LastKey.
type offsetSource struct {
	calls     []adapters.BatchOptions
	totalRows int
	batchSize int
}

func (s *offsetSource) ReadBatch(_ context.Context, _ string, opts adapters.BatchOptions) (*adapters.Batch, error) {
	s.calls = append(s.calls, opts)
	if opts.Offset >= s.totalRows {
		return &adapters.Batch{}, nil
	}
	remaining := s.totalRows - opts.Offset
	size := s.batchSize
	if remaining < size {
		size = remaining
	}
	records := make([]map[string]interface{}, size)
	for i := range records {
		records[i] = map[string]interface{}{"id": opts.Offset + i + 1}
	}
	// LastKey deliberately left empty — SQL adapters never set it.
	return &adapters.Batch{Records: records, Size: size}, nil
}

func (s *offsetSource) Connect(_ context.Context, _ adapters.ConnectionConfig) error { return nil }
func (s *offsetSource) Close() error                                                   { return nil }
func (s *offsetSource) Ping(_ context.Context) error                                   { return nil }
func (s *offsetSource) ListTables(_ context.Context) ([]adapters.TableSchema, error) {
	return nil, nil
}
func (s *offsetSource) GetSchema(_ context.Context, _ string) (*adapters.TableSchema, error) {
	return &adapters.TableSchema{Name: "col"}, nil
}
func (s *offsetSource) GetRowCount(_ context.Context, _ string) (int64, error) {
	return int64(s.totalRows), nil
}
func (s *offsetSource) GetPKRange(_ context.Context, _, _ string) (int64, int64, error) {
	return 0, 0, fmt.Errorf("not supported")
}
func (s *offsetSource) CheckPermissions(_ context.Context) (*adapters.PermissionCheck, error) {
	return &adapters.PermissionCheck{CanRead: true}, nil
}
func (s *offsetSource) GetInfo(_ context.Context) (*adapters.DatabaseInfo, error) {
	return &adapters.DatabaseInfo{Type: "MockOffset"}, nil
}

// ── mock target ─────────────────────────────────────────────────────────────

type devNullTarget struct{}

func (t *devNullTarget) WriteBatch(_ context.Context, _ string, _ *adapters.Batch) error {
	return nil
}
func (t *devNullTarget) Connect(_ context.Context, _ adapters.ConnectionConfig) error { return nil }
func (t *devNullTarget) Close() error                                                   { return nil }
func (t *devNullTarget) Ping(_ context.Context) error                                   { return nil }
func (t *devNullTarget) ListTables(_ context.Context) ([]adapters.TableSchema, error) {
	return nil, nil
}
func (t *devNullTarget) GetSchema(_ context.Context, _ string) (*adapters.TableSchema, error) {
	return nil, nil
}
func (t *devNullTarget) CreateTable(_ context.Context, _ *adapters.TableSchema) error  { return nil }
func (t *devNullTarget) DropTable(_ context.Context, _ string) error                   { return nil }
func (t *devNullTarget) TruncateTable(_ context.Context, _ string) error               { return nil }
func (t *devNullTarget) AlterTable(_ context.Context, _ string, _ []adapters.SchemaChange) error {
	return nil
}
func (t *devNullTarget) CreateIndexes(_ context.Context, _ string, _ []adapters.IndexDef) error {
	return nil
}
func (t *devNullTarget) CreateConstraints(_ context.Context, _ string, _ []adapters.ForeignKey, _ []adapters.CheckConstraint) error {
	return nil
}
func (t *devNullTarget) CheckPermissions(_ context.Context) (*adapters.PermissionCheck, error) {
	return &adapters.PermissionCheck{CanWrite: true}, nil
}
func (t *devNullTarget) GetInfo(_ context.Context) (*adapters.DatabaseInfo, error) {
	return &adapters.DatabaseInfo{Type: "MockTarget"}, nil
}

// ── test helpers ────────────────────────────────────────────────────────────

func newTestEngine(t *testing.T, src adapters.SourceAdapter, tgt adapters.TargetAdapter) (*Engine, *state.MetaDB, func()) {
	t.Helper()
	f, err := os.CreateTemp("", "xferdb-test-*.db")
	if err != nil {
		t.Fatalf("create temp db: %v", err)
	}
	f.Close()

	db, err := state.Open(f.Name())
	if err != nil {
		os.Remove(f.Name())
		t.Fatalf("open state db: %v", err)
	}

	ctx := context.Background()
	proj := &adapters.Project{
		ID:   "test-proj",
		Name: "test",
		SourceConfig: adapters.ConnectionConfig{Type: "mock"},
		TargetConfig: adapters.ConnectionConfig{Type: "mock"},
		TransferConfig: adapters.TransferConfig{BatchSize: 1},
	}
	if err := db.CreateProject(ctx, proj); err != nil {
		t.Fatalf("create project: %v", err)
	}

	migID := "test-mig-" + fmt.Sprint(time.Now().UnixNano())
	now := time.Now()
	if err := db.CreateMigration(ctx, &adapters.Migration{
		ID: migID, ProjectID: proj.ID,
		Status: adapters.StatusInProgress, CreatedAt: now, StartedAt: &now,
	}); err != nil {
		t.Fatalf("create migration: %v", err)
	}

	eng := &Engine{
		migrationID: migID,
		project:     proj,
		db:          db,
		source:      src,
		target:      tgt,
		events:      make(chan ProgressEvent, 256),
		pauseCh:     make(chan struct{}, 1),
		resumeCh:    make(chan struct{}, 1),
	}

	cleanup := func() {
		db.Close()
		os.Remove(f.Name())
	}
	return eng, db, cleanup
}

// ── tests ───────────────────────────────────────────────────────────────────

// TestKeysetLastKeyCheckpointed verifies that after each batch the checkpoint
// stores the LastKey returned by the source adapter.
func TestKeysetLastKeyCheckpointed(t *testing.T) {
	src := &keysetSource{
		t:       t,
		batches: 3,
		lastKeyFn: func(n int) string {
			return fmt.Sprintf("key_%03d", n)
		},
	}
	eng, db, cleanup := newTestEngine(t, src, &devNullTarget{})
	defer cleanup()

	ctx := context.Background()
	schema := adapters.TableSchema{Name: "docs"}

	if err := eng.transferTableSequential(ctx, schema); err != nil {
		t.Fatalf("transfer failed: %v", err)
	}

	cp, err := db.GetLastCheckpoint(ctx, eng.migrationID, "docs")
	if err != nil {
		t.Fatalf("get checkpoint: %v", err)
	}
	if cp == nil {
		t.Fatal("expected a checkpoint, got nil")
	}
	if cp.LastPK != "key_003" {
		t.Errorf("checkpoint LastPK = %q, want %q", cp.LastPK, "key_003")
	}
}

// TestKeysetResumeUsesLastKey verifies that when the last checkpoint has a
// non-empty LastPK, the sequential loop passes it as BatchOptions.LastPK
// on the first ReadBatch call after resume, and does not use Offset.
func TestKeysetResumeUsesLastKey(t *testing.T) {
	const table = "docs"
	src := &keysetSource{
		t:       t,
		batches: 2,
		lastKeyFn: func(n int) string {
			return fmt.Sprintf("key_%03d", n)
		},
	}
	eng, db, cleanup := newTestEngine(t, src, &devNullTarget{})
	defer cleanup()

	ctx := context.Background()

	// Seed a checkpoint as if a previous run processed one batch and stored "key_001".
	if err := db.SaveCheckpoint(ctx, adapters.Checkpoint{
		MigrationID: eng.migrationID,
		TableName:   table,
		BatchID:     1,
		LastPK:      "key_001",
		RowsInBatch: 1,
		CreatedAt:   time.Now(),
	}); err != nil {
		t.Fatalf("seed checkpoint: %v", err)
	}
	// Also seed table progress so rowsTransferred is consistent.
	if err := db.UpsertTableProgress(ctx, eng.migrationID, adapters.TableProgress{
		TableName: table, Status: adapters.StatusInProgress,
		RowsTotal: 2, RowsTransferred: 1,
	}); err != nil {
		t.Fatalf("seed progress: %v", err)
	}

	schema := adapters.TableSchema{Name: table}
	if err := eng.transferTableSequential(ctx, schema); err != nil {
		t.Fatalf("transfer failed: %v", err)
	}

	if len(src.calls) == 0 {
		t.Fatal("ReadBatch was never called")
	}
	first := src.calls[0]

	// Resume must use LastPK, not Offset.
	if first.LastPK != "key_001" {
		t.Errorf("first call LastPK = %v, want %q", first.LastPK, "key_001")
	}
	if first.Offset != 0 {
		t.Errorf("first call Offset = %d, want 0 (keyset mode must not use offset)", first.Offset)
	}
}

// TestOffsetAdapterUnchanged verifies that a SQL-style adapter that never sets
// LastKey continues to use LIMIT/OFFSET pagination without regression.
func TestOffsetAdapterUnchanged(t *testing.T) {
	src := &offsetSource{totalRows: 5, batchSize: 2}
	eng, _, cleanup := newTestEngine(t, src, &devNullTarget{})
	defer cleanup()

	// Set batch size to 2 to get predictable call sequence.
	eng.project.TransferConfig.BatchSize = 2

	ctx := context.Background()
	schema := adapters.TableSchema{Name: "rows"}

	if err := eng.transferTableSequential(ctx, schema); err != nil {
		t.Fatalf("transfer failed: %v", err)
	}

	// Expect three calls: offset 0, 2, 4 (last returns 1 row then EOF).
	wantOffsets := []int{0, 2, 4}
	if len(src.calls) < len(wantOffsets) {
		t.Fatalf("expected at least %d ReadBatch calls, got %d", len(wantOffsets), len(src.calls))
	}
	for i, want := range wantOffsets {
		if src.calls[i].Offset != want {
			t.Errorf("call[%d].Offset = %d, want %d", i, src.calls[i].Offset, want)
		}
		if src.calls[i].LastPK != nil {
			t.Errorf("call[%d].LastPK = %v, want nil (SQL adapters must not receive a keyset token)", i, src.calls[i].LastPK)
		}
	}
}

// TestOffsetResumeUnchanged verifies that a SQL adapter resumes via offset
// (rowsTransferred) when no keyset checkpoint exists, preserving the
// existing behaviour.
func TestOffsetResumeUnchanged(t *testing.T) {
	const table = "rows"
	src := &offsetSource{totalRows: 4, batchSize: 2}
	eng, db, cleanup := newTestEngine(t, src, &devNullTarget{})
	defer cleanup()
	eng.project.TransferConfig.BatchSize = 2

	ctx := context.Background()

	// Seed progress as if 2 rows were already transferred (no LastPK in checkpoint).
	if err := db.UpsertTableProgress(ctx, eng.migrationID, adapters.TableProgress{
		TableName: table, Status: adapters.StatusInProgress,
		RowsTotal: 4, RowsTransferred: 2,
	}); err != nil {
		t.Fatalf("seed progress: %v", err)
	}
	// Checkpoint exists but has empty LastPK (SQL style).
	if err := db.SaveCheckpoint(ctx, adapters.Checkpoint{
		MigrationID: eng.migrationID, TableName: table,
		BatchID: 1, LastPK: "", RowsInBatch: 2, CreatedAt: time.Now(),
	}); err != nil {
		t.Fatalf("seed checkpoint: %v", err)
	}

	schema := adapters.TableSchema{Name: table}
	if err := eng.transferTableSequential(ctx, schema); err != nil {
		t.Fatalf("transfer failed: %v", err)
	}

	// First call must resume at offset 2, not 0.
	if len(src.calls) == 0 {
		t.Fatal("ReadBatch was never called")
	}
	if src.calls[0].Offset != 2 {
		t.Errorf("resume offset = %d, want 2", src.calls[0].Offset)
	}
	if src.calls[0].LastPK != nil {
		t.Errorf("resume LastPK = %v, want nil (must use offset, not keyset)", src.calls[0].LastPK)
	}
}
