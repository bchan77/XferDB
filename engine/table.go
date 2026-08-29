package engine

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"gitea.homelab.local/nextdevops/XferDB/adapters"
)

const defaultBatchSize = 1000

// transferTable is the dispatcher: sequential for BatchWorkers<=1, parallel otherwise.
func (e *Engine) transferTable(ctx context.Context, sourceSchema adapters.TableSchema) error {
	cfg := e.project.TransferConfig
	bw := cfg.BatchWorkers
	if bw <= 1 {
		return e.transferTableSequential(ctx, sourceSchema)
	}

	// Prefer PK-range splitting unless user opted into offset mode.
	if !cfg.OffsetFallback {
		if pkCol := detectIntegerPK(sourceSchema); pkCol != "" {
			return e.transferTableParallelPK(ctx, sourceSchema, pkCol, bw)
		}
	}
	return e.transferTableParallelOffset(ctx, sourceSchema, bw)
}

// transferTableSequential is the existing single-goroutine batch loop with checkpoint resume.
func (e *Engine) transferTableSequential(ctx context.Context, sourceSchema adapters.TableSchema) error {
	table := sourceSchema.Name

	total, err := e.source.GetRowCount(ctx, table)
	if err != nil {
		return fmt.Errorf("row count(%s): %w", table, err)
	}

	offset, rowsTransferred := e.resumeOffset(ctx, table)

	batchSize := e.project.TransferConfig.BatchSize
	if batchSize <= 0 {
		batchSize = defaultBatchSize
	}

	now := time.Now()
	if err := e.db.UpsertTableProgress(ctx, e.migrationID, adapters.TableProgress{
		TableName:       table,
		Status:          adapters.StatusInProgress,
		RowsTotal:       total,
		RowsTransferred: rowsTransferred,
		StartedAt:       &now,
	}); err != nil {
		return err
	}

	e.emit(ProgressEvent{
		Kind:            EventTableStart,
		TableName:       table,
		RowsTotal:       total,
		RowsTransferred: rowsTransferred,
		Timestamp:       now,
	})

	batchID := offset / batchSize

	for {
		if err := e.checkPause(ctx); err != nil {
			return err
		}

		batch, err := e.source.ReadBatch(ctx, table, adapters.BatchOptions{
			Offset: offset,
			Limit:  batchSize,
		})
		if err != nil {
			return fmt.Errorf("read batch %s offset %d: %w", table, offset, err)
		}
		if batch.Size == 0 {
			break
		}

		if err := e.target.WriteBatch(ctx, table, batch); err != nil {
			return fmt.Errorf("write batch %s offset %d: %w", table, offset, err)
		}

		offset += batch.Size
		rowsTransferred += int64(batch.Size)
		batchID++

		if err := e.db.SaveCheckpoint(ctx, adapters.Checkpoint{
			MigrationID: e.migrationID,
			TableName:   table,
			BatchID:     batchID,
			RowsInBatch: batch.Size,
			CreatedAt:   time.Now(),
		}); err != nil {
			return fmt.Errorf("save checkpoint: %w", err)
		}

		if err := e.db.UpsertTableProgress(ctx, e.migrationID, adapters.TableProgress{
			TableName:       table,
			Status:          adapters.StatusInProgress,
			RowsTotal:       total,
			RowsTransferred: rowsTransferred,
		}); err != nil {
			return err
		}

		e.emit(ProgressEvent{
			Kind:            EventBatch,
			TableName:       table,
			RowsTransferred: rowsTransferred,
			RowsTotal:       total,
			Timestamp:       time.Now(),
		})
	}

	done := time.Now()
	if err := e.db.UpsertTableProgress(ctx, e.migrationID, adapters.TableProgress{
		TableName:       table,
		Status:          adapters.StatusCompleted,
		RowsTotal:       total,
		RowsTransferred: rowsTransferred,
		CompletedAt:     &done,
	}); err != nil {
		return err
	}

	e.emit(ProgressEvent{
		Kind:            EventTableDone,
		TableName:       table,
		RowsTransferred: rowsTransferred,
		RowsTotal:       total,
		Timestamp:       done,
	})
	return nil
}

// transferTableParallelPK splits the table by primary key range: N workers each own a
// [pkMin, pkMax) segment and read/write independently using PK-ordered range queries.
// No per-segment checkpointing — on crash the table restarts from scratch (safe with upsert).
func (e *Engine) transferTableParallelPK(ctx context.Context, schema adapters.TableSchema, pkCol string, workers int) error {
	table := schema.Name
	cfg := e.project.TransferConfig

	batchSize := cfg.BatchSize
	if batchSize <= 0 {
		batchSize = defaultBatchSize
	}

	total, err := e.source.GetRowCount(ctx, table)
	if err != nil {
		return fmt.Errorf("row count(%s): %w", table, err)
	}

	pkMin, pkMax, err := e.source.GetPKRange(ctx, table, pkCol)
	if err != nil {
		// Table is empty or PK range unavailable — fall through to offset mode.
		return e.transferTableParallelOffset(ctx, schema, workers)
	}

	now := time.Now()
	e.db.UpsertTableProgress(ctx, e.migrationID, adapters.TableProgress{ //nolint:errcheck
		TableName: table, Status: adapters.StatusInProgress, RowsTotal: total, StartedAt: &now,
	})
	e.emit(ProgressEvent{Kind: EventTableStart, TableName: table, RowsTotal: total, Timestamp: now})

	// Divide [pkMin, pkMax] into N equal-width ranges.
	span := pkMax - pkMin + 1
	segSpan := (span + int64(workers) - 1) / int64(workers) // ceiling division

	var rowsWritten atomic.Int64
	workerCtx, cancelWorkers := context.WithCancel(ctx)
	defer cancelWorkers()

	var wg sync.WaitGroup
	var mu sync.Mutex
	var firstErr error

	for i := 0; i < workers; i++ {
		segFrom := pkMin + int64(i)*segSpan
		segTo := segFrom + segSpan - 1
		if segTo > pkMax {
			segTo = pkMax
		}
		if segFrom > pkMax {
			break // more workers than PK range width
		}
		isLast := segTo == pkMax

		wg.Add(1)
		go func(from, to int64, last bool) {
			defer wg.Done()
			if err := e.transferSegmentPK(workerCtx, table, pkCol, from, to, last, total, &rowsWritten, batchSize); err != nil {
				mu.Lock()
				if firstErr == nil {
					firstErr = err
					cancelWorkers()
				}
				mu.Unlock()
			}
		}(segFrom, segTo, isLast)
	}

	wg.Wait()

	if firstErr != nil {
		return firstErr
	}

	done := time.Now()
	written := rowsWritten.Load()
	e.db.UpsertTableProgress(ctx, e.migrationID, adapters.TableProgress{ //nolint:errcheck
		TableName: table, Status: adapters.StatusCompleted,
		RowsTotal: total, RowsTransferred: written, CompletedAt: &done,
	})
	e.emit(ProgressEvent{
		Kind: EventTableDone, TableName: table,
		RowsTransferred: written, RowsTotal: total, Timestamp: done,
	})
	return nil
}

// transferTableParallelOffset splits the table into N offset-based segments.
// Falls back automatically when the table has no single integer PK or when
// OffsetFallback is set.
func (e *Engine) transferTableParallelOffset(ctx context.Context, schema adapters.TableSchema, workers int) error {
	table := schema.Name
	cfg := e.project.TransferConfig

	batchSize := cfg.BatchSize
	if batchSize <= 0 {
		batchSize = defaultBatchSize
	}

	total, err := e.source.GetRowCount(ctx, table)
	if err != nil {
		return fmt.Errorf("row count(%s): %w", table, err)
	}

	now := time.Now()
	e.db.UpsertTableProgress(ctx, e.migrationID, adapters.TableProgress{ //nolint:errcheck
		TableName: table, Status: adapters.StatusInProgress, RowsTotal: total, StartedAt: &now,
	})
	e.emit(ProgressEvent{Kind: EventTableStart, TableName: table, RowsTotal: total, Timestamp: now})

	// Divide rows into N equal segments.
	segSize := (int(total) + workers - 1) / workers // ceiling division

	var rowsWritten atomic.Int64
	workerCtx, cancelWorkers := context.WithCancel(ctx)
	defer cancelWorkers()

	var wg sync.WaitGroup
	var mu sync.Mutex
	var firstErr error

	for i := 0; i < workers; i++ {
		startOff := i * segSize
		maxRows := segSize
		if startOff >= int(total) {
			break // more workers than rows
		}

		wg.Add(1)
		go func(start, max int) {
			defer wg.Done()
			if err := e.transferSegmentOffset(workerCtx, table, start, max, total, &rowsWritten, batchSize); err != nil {
				mu.Lock()
				if firstErr == nil {
					firstErr = err
					cancelWorkers()
				}
				mu.Unlock()
			}
		}(startOff, maxRows)
	}

	wg.Wait()

	if firstErr != nil {
		return firstErr
	}

	done := time.Now()
	written := rowsWritten.Load()
	e.db.UpsertTableProgress(ctx, e.migrationID, adapters.TableProgress{ //nolint:errcheck
		TableName: table, Status: adapters.StatusCompleted,
		RowsTotal: total, RowsTransferred: written, CompletedAt: &done,
	})
	e.emit(ProgressEvent{
		Kind: EventTableDone, TableName: table,
		RowsTransferred: written, RowsTotal: total, Timestamp: done,
	})
	return nil
}

// transferSegmentPK reads rows in the PK range [pkFrom, pkTo] (inclusive) in batches
// and writes them to the target. Advances pkFrom past the last seen PK after each batch.
func (e *Engine) transferSegmentPK(ctx context.Context, table, pkCol string, pkFrom, pkTo int64, isLast bool, total int64, rowsWritten *atomic.Int64, batchSize int) error {
	current := pkFrom
	for {
		if err := e.checkPause(ctx); err != nil {
			return err
		}

		batch, err := e.source.ReadBatch(ctx, table, adapters.BatchOptions{
			Limit:     batchSize,
			PKCol:     pkCol,
			PKMin:     current,
			PKMax:     pkTo,
			PKMaxIncl: isLast,
		})
		if err != nil {
			return fmt.Errorf("read pk-segment %s [%d, %d]: %w", table, current, pkTo, err)
		}
		if batch.Size == 0 {
			break
		}

		if err := e.target.WriteBatch(ctx, table, batch); err != nil {
			return fmt.Errorf("write pk-segment %s [%d, %d]: %w", table, current, pkTo, err)
		}

		written := rowsWritten.Add(int64(batch.Size))
		e.emit(ProgressEvent{
			Kind:            EventBatch,
			TableName:       table,
			RowsTransferred: written,
			RowsTotal:       total,
			Timestamp:       time.Now(),
		})

		// Advance past the last PK we just wrote.
		current = batch.LastPK + 1
		if current > pkTo {
			break
		}
	}
	return nil
}

// transferSegmentOffset reads up to maxRows rows starting at startOff using LIMIT/OFFSET.
func (e *Engine) transferSegmentOffset(ctx context.Context, table string, startOff, maxRows int, total int64, rowsWritten *atomic.Int64, batchSize int) error {
	offset := startOff
	rowsRead := 0

	for rowsRead < maxRows {
		if err := e.checkPause(ctx); err != nil {
			return err
		}

		limit := batchSize
		if remaining := maxRows - rowsRead; remaining < limit {
			limit = remaining
		}

		batch, err := e.source.ReadBatch(ctx, table, adapters.BatchOptions{
			Offset: offset,
			Limit:  limit,
		})
		if err != nil {
			return fmt.Errorf("read offset-segment %s offset %d: %w", table, offset, err)
		}
		if batch.Size == 0 {
			break
		}

		if err := e.target.WriteBatch(ctx, table, batch); err != nil {
			return fmt.Errorf("write offset-segment %s offset %d: %w", table, offset, err)
		}

		offset += batch.Size
		rowsRead += batch.Size
		written := rowsWritten.Add(int64(batch.Size))
		e.emit(ProgressEvent{
			Kind:            EventBatch,
			TableName:       table,
			RowsTransferred: written,
			RowsTotal:       total,
			Timestamp:       time.Now(),
		})
	}
	return nil
}

// resumeOffset returns the offset to start reading from and the rows already
// transferred, based on persisted table progress from a previous run.
func (e *Engine) resumeOffset(ctx context.Context, table string) (offset int, rowsTransferred int64) {
	progress, err := e.db.GetTableProgress(ctx, e.migrationID)
	if err != nil {
		return 0, 0
	}
	for _, tp := range progress {
		if tp.TableName == table {
			rowsTransferred = tp.RowsTransferred
			offset = int(rowsTransferred)
			return
		}
	}
	return 0, 0
}

// detectIntegerPK returns the name of a single-column integer primary key, or ""
// if the table has no PK, a composite PK, or a non-integer PK type.
func detectIntegerPK(schema adapters.TableSchema) string {
	pkCount := 0
	for _, col := range schema.Columns {
		if col.PrimaryKey {
			pkCount++
		}
	}
	if pkCount != 1 {
		return ""
	}
	for _, col := range schema.Columns {
		if col.PrimaryKey && isIntegerColType(col.Type) {
			return col.Name
		}
	}
	return ""
}

// isIntegerColType returns true for SQL integer column types that can be used
// as a range-split key (int, bigint, serial, and their variants).
func isIntegerColType(colType string) bool {
	t := strings.ToLower(strings.TrimSpace(colType))
	// Strip precision/modifier suffix: "int(11)" → "int", "bigint unsigned" → "bigint"
	if idx := strings.IndexAny(t, "( "); idx != -1 {
		t = t[:idx]
	}
	switch t {
	case "int", "int2", "int4", "int8",
		"integer", "bigint", "smallint", "tinyint", "mediumint",
		"serial", "bigserial", "smallserial":
		return true
	}
	return false
}
