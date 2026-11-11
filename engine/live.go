package engine

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"gitea.homelab.local/nextdevops/XferDB/adapters"
	"gitea.homelab.local/nextdevops/XferDB/state"
)

// Live makes a run the snapshot of a live migration (#102). The engine
// copies the tables as usual, with three differences: Setup runs before any
// row is copied, so the change stream starts first; each table is paged by
// its primary key up to the largest key there was when its copy began
// (rows inserted after that arrive through the stream); and the constraints
// replaying changes could break are left for later stages (DeferredSchema).
// The migration is left in progress for the stream to continue.
type Live struct {
	// Setup runs once the target tables exist, before any row is copied,
	// with the tables being migrated. It also runs when an interrupted
	// snapshot is resumed, so it must be idempotent.
	Setup func(ctx context.Context, tables []adapters.TableSchema) error
}

// SetLive makes this run a live migration's snapshot. Call before Run.
func (e *Engine) SetLive(l *Live) { e.live = l }

// SchemaStage is when a live migration creates part of the target's
// schema. Tables and primary keys come first, as for any migration.
type SchemaStage string

const (
	// StageSnapshot follows the copy: plain indexes, and on a pg_dump
	// schema the primary keys, which apply's upserts need.
	StageSnapshot SchemaStage = "snapshot"
	// StageCatchUp follows catch-up: unique indexes and constraints. While
	// older changes replay over newer copied rows, two rows can briefly
	// share a value that never clashed on the source.
	StageCatchUp SchemaStage = "catch_up"
	// StageCutover is the rest: foreign keys and checks (tables apply in
	// parallel, so a child row can land before its parent), and triggers
	// and rules, which must not fire on replayed changes.
	StageCutover SchemaStage = "cutover"
)

// stageOfIndex is the stage a target index is created at.
func stageOfIndex(ix adapters.IndexDef) SchemaStage {
	if ix.Unique {
		return StageCatchUp
	}
	return StageSnapshot
}

// stageOfBlock is the stage a pg_dump post-data statement runs at.
func stageOfBlock(b ddlBlock) SchemaStage {
	upper := strings.ToUpper(b.SQL)
	switch b.Type {
	case "INDEX":
		if strings.Contains(upper, "CREATE UNIQUE INDEX") {
			return StageCatchUp
		}
		return StageSnapshot
	case "CONSTRAINT":
		switch {
		case strings.Contains(upper, "PRIMARY KEY"):
			return StageSnapshot
		case strings.Contains(upper, " UNIQUE") || strings.Contains(upper, " EXCLUDE"):
			return StageCatchUp
		}
	}
	return StageCutover
}

// CheckLive refuses settings a live migration can't run with.
func CheckLive(cfg adapters.TransferConfig) error {
	switch {
	case cfg.SchemaOnly:
		return errors.New("a live migration copies data: schema_only can't be used")
	case cfg.DataOnly, cfg.Truncate:
		// XferDB must create the target schema itself, to hold back the
		// constraints replay could break until it is safe to add them.
		return errors.New("a live migration creates the target schema: data_only and truncate can't be used (recreate_schema can)")
	}
	return nil
}

// transferTableKeyset copies a table for a live migration: in key order,
// after the last checkpointed key, up to the largest key when the copy
// began. While the source is written, OFFSET paging skips or repeats rows,
// and a skipped row that no later change rewrites in full is never fixed.
func (e *Engine) transferTableKeyset(ctx context.Context, sourceSchema adapters.TableSchema) error {
	table := sourceSchema.Name
	ks, ok := e.source.(adapters.KeysetSource)
	if !ok {
		return fmt.Errorf("a %s source can't page by key for a live migration", e.project.SourceConfig.Type)
	}
	now := time.Now()
	e.emit(ProgressEvent{Kind: EventTableCounting, TableName: table, Timestamp: now})
	total, err := e.source.GetRowCount(ctx, table)
	if err != nil {
		return fmt.Errorf("row count(%s): %w", table, err)
	}
	_, rowsTransferred, lastKey := e.resumeState(ctx, table)
	after, err := adapters.DecodeKey(lastKey)
	if err != nil {
		return err
	}
	// Rows above the bound were inserted after the stream began, so the
	// stream has them; chasing them would never end on a busy table.
	upTo, err := ks.MaxKey(ctx, table)
	if err != nil {
		return err
	}
	batchSize := e.project.TransferConfig.BatchSize
	if batchSize <= 0 {
		batchSize = defaultBatchSize
	}
	if err := e.db.UpsertTableProgress(ctx, e.migrationID, adapters.TableProgress{
		TableName: table, Status: adapters.StatusInProgress,
		RowsTotal: total, RowsTransferred: rowsTransferred, StartedAt: &now,
	}); err != nil {
		return err
	}
	e.emit(ProgressEvent{Kind: EventTableStart, TableName: table, RowsTotal: total, RowsTransferred: rowsTransferred, Timestamp: time.Now()})

	cp, _ := e.db.GetLastCheckpoint(ctx, e.migrationID, table)
	batchID := 0
	if cp != nil {
		batchID = cp.BatchID
	}
	for {
		if err := e.checkPause(ctx); err != nil {
			return err
		}
		t0 := time.Now()
		batch, err := e.source.ReadBatch(ctx, table, adapters.BatchOptions{
			Keyset: true, KeyAfter: after, KeyUpTo: upTo, Limit: batchSize,
		})
		readDur := time.Since(t0)
		if err != nil {
			return fmt.Errorf("read batch %s: %w", table, err)
		}
		if batch.Size == 0 {
			break
		}
		t1 := time.Now()
		if err := e.target.WriteBatch(ctx, table, batch); err != nil {
			return fmt.Errorf("write batch %s: %w", table, err)
		}
		writeDur := time.Since(t1)
		rowsTransferred += int64(batch.Size)
		batchID++
		if after, err = adapters.DecodeKey(batch.LastKey); err != nil {
			return err
		}
		if err := e.db.SaveCheckpoint(ctx, adapters.Checkpoint{
			MigrationID: e.migrationID, TableName: table, BatchID: batchID,
			LastPK: batch.LastKey, RowsInBatch: batch.Size, CreatedAt: time.Now(),
		}); err != nil {
			return fmt.Errorf("save checkpoint: %w", err)
		}
		if err := e.db.UpsertTableProgress(ctx, e.migrationID, adapters.TableProgress{
			TableName: table, Status: adapters.StatusInProgress, RowsTotal: total, RowsTransferred: rowsTransferred,
		}); err != nil {
			return err
		}
		e.emit(ProgressEvent{
			Kind: EventBatch, TableName: table, RowsTransferred: rowsTransferred, RowsTotal: total,
			BatchRows: batch.Size, ReadDuration: readDur, WriteDuration: writeDur, Timestamp: time.Now(),
		})
	}

	done := time.Now()
	if err := e.db.UpsertTableProgress(ctx, e.migrationID, adapters.TableProgress{
		TableName: table, Status: adapters.StatusCompleted,
		RowsTotal: total, RowsTransferred: rowsTransferred, CompletedAt: &done,
	}); err != nil {
		return err
	}
	e.emit(ProgressEvent{Kind: EventTableDone, TableName: table, RowsTransferred: rowsTransferred, RowsTotal: total, Timestamp: done})
	e.tableDone(table)
	return nil
}

// stageSchema keeps the parts of tables' indexes and constraints created at
// stage.
func stageSchema(tables []adapters.TableSchema, stage SchemaStage) []adapters.TableSchema {
	out := make([]adapters.TableSchema, 0, len(tables))
	for _, t := range tables {
		s := t
		s.Indexes = nil
		for _, ix := range t.Indexes {
			if stageOfIndex(ix) == stage {
				s.Indexes = append(s.Indexes, ix)
			}
		}
		if stage != StageCutover {
			s.ForeignKeys, s.Checks = nil, nil
		}
		out = append(out, s)
	}
	return out
}

// nativeSchemaFor reports whether a run of project uses pg_dump for the
// target schema, as Run decides it.
func nativeSchemaFor(project *adapters.Project, plan []state.SchemaPlanRow) bool {
	if !adapters.PostgresFamily(project.SourceConfig.Type) || !adapters.PostgresFamily(project.TargetConfig.Type) || len(plan) > 0 || !hasPgTools() {
		return false
	}
	_, ok := pgDumpSchemaArgs(project.SourceConfig, project.TargetConfig)
	return ok
}

// DeferredSchema creates the part of a live migration's target schema due
// at stage (StageCatchUp or StageCutover) for tables. Creating something
// that already exists is skipped, so a stage interrupted by a restart can
// run again.
func DeferredSchema(ctx context.Context, project *adapters.Project, db *state.MetaDB, tables []string, stage SchemaStage) error {
	e := New("", project, db)
	if err := e.connect(ctx); err != nil {
		return err
	}
	defer e.disconnect()
	var plan []state.SchemaPlanRow
	if project.SourceConfig.Type != "mongodb" {
		plan, _ = db.GetPlanForProject(ctx, project.ID)
	}
	if nativeSchemaFor(project, plan) {
		_, err := e.pgDumpPostData(ctx, project.SourceConfig, project.TargetConfig, func(b ddlBlock) bool {
			return stageOfBlock(b) == stage
		})
		return err
	}
	schemas := make([]adapters.TableSchema, 0, len(tables))
	for _, name := range tables {
		s, err := e.source.GetSchema(ctx, name)
		if err != nil {
			return fmt.Errorf("schema of %s: %w", name, err)
		}
		schemas = append(schemas, *s)
	}
	workers := project.TransferConfig.TableWorkers
	if workers < 1 {
		workers = 1
	}
	return e.createPostSchema(ctx, stageSchema(schemas, stage), workers)
}

// nullableColumns returns t with every column but its key nullable. A
// MongoDB plan marks a field NOT NULL when every sampled document had it,
// but while a live migration streams, any document can lose a field ($unset)
// or be inserted without one, and the row it becomes must still be stored.
func nullableColumns(t adapters.TableSchema) adapters.TableSchema {
	cols := make([]adapters.ColumnDef, len(t.Columns))
	for i, c := range t.Columns {
		if !c.PrimaryKey {
			c.Nullable = true
		}
		cols[i] = c
	}
	t.Columns = cols
	return t
}
