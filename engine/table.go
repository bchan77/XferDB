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

// transferTable is the dispatcher: sequential for SegmentWorkers<=1, parallel otherwise.
func (e *Engine) transferTable(ctx context.Context, sourceSchema adapters.TableSchema) error {
	cfg := e.project.TransferConfig
	bw := cfg.SegmentWorkers
	if bw <= 1 {
		// Use pipelined mode if enabled (overlaps read/write for better throughput).
		if cfg.AsyncPipeline {
			return e.transferTablePipelined(ctx, sourceSchema)
		}
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

// transferTableSequential is the single-goroutine batch loop with checkpoint resume.
//
// It supports two resume modes:
//   - Keyset (used by MongoDB and other adapters that set Batch.LastKey): resumes by
//     passing the last token back as BatchOptions.LastPK; no OFFSET scan.
//   - Offset (used by all SQL adapters): resumes via LIMIT/OFFSET using rowsTransferred
//     as the offset; Batch.LastKey is always empty for these adapters.
//
// Which mode is active is determined entirely by whether the last checkpoint has a
// non-empty LastPK. SQL adapters never write LastKey so they always use offset mode.
func (e *Engine) transferTableSequential(ctx context.Context, sourceSchema adapters.TableSchema) error {
	table := sourceSchema.Name

	// Emit EventTableStart immediately so the UI shows "in_progress" while
	// we count rows (which can be slow for large MongoDB collections).
	now := time.Now()
	e.emit(ProgressEvent{
		Kind:      EventTableStart,
		TableName: table,
		RowsTotal: 0, // will be updated after count completes
		Timestamp: now,
	})

	total, err := e.source.GetRowCount(ctx, table)
	if err != nil {
		return fmt.Errorf("row count(%s): %w", table, err)
	}

	offset, rowsTransferred, lastKey := e.resumeState(ctx, table)

	batchSize := e.project.TransferConfig.BatchSize
	if batchSize <= 0 {
		batchSize = defaultBatchSize
	}

	if err := e.db.UpsertTableProgress(ctx, e.migrationID, adapters.TableProgress{
		TableName:       table,
		Status:          adapters.StatusInProgress,
		RowsTotal:       total,
		RowsTransferred: rowsTransferred,
		StartedAt:       &now,
	}); err != nil {
		return err
	}

	// Emit updated event with actual row count now that counting is done.
	e.emit(ProgressEvent{
		Kind:            EventTableStart,
		TableName:       table,
		RowsTotal:       total,
		RowsTransferred: rowsTransferred,
		Timestamp:       time.Now(),
	})

	batchID := offset / batchSize

	for {
		if err := e.checkPause(ctx); err != nil {
			return err
		}

		// Build read options. Keyset mode when we have a resume token from the
		// last checkpoint; offset mode otherwise (SQL adapters).
		var opts adapters.BatchOptions
		if lastKey != "" {
			opts = adapters.BatchOptions{LastPK: lastKey, Limit: batchSize}
		} else {
			opts = adapters.BatchOptions{Offset: offset, Limit: batchSize}
		}

		t0 := time.Now()
		batch, err := e.source.ReadBatch(ctx, table, opts)
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

		// Advance the resume cursor. Keyset adapters set LastKey; SQL adapters do not.
		if batch.LastKey != "" {
			lastKey = batch.LastKey
		} else {
			offset += batch.Size
		}

		if err := e.db.SaveCheckpoint(ctx, adapters.Checkpoint{
			MigrationID: e.migrationID,
			TableName:   table,
			BatchID:     batchID,
			LastPK:      lastKey, // empty string for SQL adapters; keyset token for others
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
			BatchRows:       batch.Size,
			ReadDuration:    readDur,
			WriteDuration:   writeDur,
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

// pipelineBatch holds a batch plus timing info for the pipelined transfer.
type pipelineBatch struct {
	batch    *adapters.Batch
	readDur  time.Duration
	lastKey  string // resume token for keyset mode
	offset   int    // for offset mode
	batchID  int
	readErr  error // non-nil signals reader error
}

// transferTablePipelined overlaps reading and writing for better throughput.
// While batch N is being written, batch N+1 is read concurrently.
func (e *Engine) transferTablePipelined(ctx context.Context, sourceSchema adapters.TableSchema) error {
	table := sourceSchema.Name

	// Emit EventTableStart immediately so the UI shows "in_progress" while
	// we count rows (which can be slow for large MongoDB collections).
	now := time.Now()
	e.emit(ProgressEvent{
		Kind:      EventTableStart,
		TableName: table,
		RowsTotal: 0, // will be updated after count completes
		Timestamp: now,
	})

	total, err := e.source.GetRowCount(ctx, table)
	if err != nil {
		return fmt.Errorf("row count(%s): %w", table, err)
	}

	offset, rowsTransferred, lastKey := e.resumeState(ctx, table)

	batchSize := e.project.TransferConfig.BatchSize
	if batchSize <= 0 {
		batchSize = defaultBatchSize
	}

	if err := e.db.UpsertTableProgress(ctx, e.migrationID, adapters.TableProgress{
		TableName:       table,
		Status:          adapters.StatusInProgress,
		RowsTotal:       total,
		RowsTransferred: rowsTransferred,
		StartedAt:       &now,
	}); err != nil {
		return err
	}

	// Emit updated event with actual row count now that counting is done.
	e.emit(ProgressEvent{
		Kind:            EventTableStart,
		TableName:       table,
		RowsTotal:       total,
		RowsTransferred: rowsTransferred,
		Timestamp:       time.Now(),
	})

	batchID := offset / batchSize

	// Channel for passing batches from reader to writer.
	// Buffer of 2 allows reader to get ahead while writer processes.
	batchCh := make(chan pipelineBatch, 2)

	// Context for coordinating shutdown between reader and writer.
	pipeCtx, cancelPipe := context.WithCancel(ctx)
	defer cancelPipe()

	// Reader goroutine: reads batches and sends to channel.
	var readerWg sync.WaitGroup
	readerWg.Add(1)
	go func() {
		defer readerWg.Done()
		defer close(batchCh)

		currentOffset := offset
		currentLastKey := lastKey
		currentBatchID := batchID

		for {
			select {
			case <-pipeCtx.Done():
				return
			default:
			}

			var opts adapters.BatchOptions
			if currentLastKey != "" {
				opts = adapters.BatchOptions{LastPK: currentLastKey, Limit: batchSize}
			} else {
				opts = adapters.BatchOptions{Offset: currentOffset, Limit: batchSize}
			}

			t0 := time.Now()
			batch, err := e.source.ReadBatch(pipeCtx, table, opts)
			readDur := time.Since(t0)

			if err != nil {
				select {
				case batchCh <- pipelineBatch{readErr: err}:
				case <-pipeCtx.Done():
				}
				return
			}

			if batch.Size == 0 {
				return // No more data
			}

			currentBatchID++
			pb := pipelineBatch{
				batch:   batch,
				readDur: readDur,
				batchID: currentBatchID,
				offset:  currentOffset,
			}

			// Update cursor for next read.
			if batch.LastKey != "" {
				currentLastKey = batch.LastKey
				pb.lastKey = batch.LastKey
			} else {
				currentOffset += batch.Size
			}

			select {
			case batchCh <- pb:
			case <-pipeCtx.Done():
				return
			}
		}
	}()

	// Writer loop: receives batches from channel and writes them.
	var writeErr error
	for pb := range batchCh {
		// Check for reader error.
		if pb.readErr != nil {
			writeErr = fmt.Errorf("read batch %s: %w", table, pb.readErr)
			break
		}

		// Check for pause.
		if err := e.checkPause(ctx); err != nil {
			writeErr = err
			break
		}

		t1 := time.Now()
		if err := e.target.WriteBatch(ctx, table, pb.batch); err != nil {
			writeErr = fmt.Errorf("write batch %s: %w", table, err)
			break
		}
		writeDur := time.Since(t1)

		rowsTransferred += int64(pb.batch.Size)

		// Update lastKey for checkpoint (use the one from the batch we just wrote).
		if pb.lastKey != "" {
			lastKey = pb.lastKey
		} else {
			offset = pb.offset + pb.batch.Size
		}

		if err := e.db.SaveCheckpoint(ctx, adapters.Checkpoint{
			MigrationID: e.migrationID,
			TableName:   table,
			BatchID:     pb.batchID,
			LastPK:      lastKey,
			RowsInBatch: pb.batch.Size,
			CreatedAt:   time.Now(),
		}); err != nil {
			writeErr = fmt.Errorf("save checkpoint: %w", err)
			break
		}

		if err := e.db.UpsertTableProgress(ctx, e.migrationID, adapters.TableProgress{
			TableName:       table,
			Status:          adapters.StatusInProgress,
			RowsTotal:       total,
			RowsTransferred: rowsTransferred,
		}); err != nil {
			writeErr = err
			break
		}

		e.emit(ProgressEvent{
			Kind:            EventBatch,
			TableName:       table,
			RowsTransferred: rowsTransferred,
			RowsTotal:       total,
			BatchRows:       pb.batch.Size,
			ReadDuration:    pb.readDur,
			WriteDuration:   writeDur,
			Timestamp:       time.Now(),
		})
	}

	// Cancel reader if we exited early due to error.
	cancelPipe()
	readerWg.Wait()

	if writeErr != nil {
		return writeErr
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

	// Emit EventTableStart immediately so the UI shows "in_progress" while counting.
	now := time.Now()
	e.emit(ProgressEvent{Kind: EventTableStart, TableName: table, RowsTotal: 0, Timestamp: now})

	total, err := e.source.GetRowCount(ctx, table)
	if err != nil {
		return fmt.Errorf("row count(%s): %w", table, err)
	}

	pkMin, pkMax, err := e.source.GetPKRange(ctx, table, pkCol)
	if err != nil {
		// Table is empty or PK range unavailable — fall through to offset mode.
		return e.transferTableParallelOffset(ctx, schema, workers)
	}

	e.db.UpsertTableProgress(ctx, e.migrationID, adapters.TableProgress{ //nolint:errcheck
		TableName: table, Status: adapters.StatusInProgress, RowsTotal: total, StartedAt: &now,
	})
	e.emit(ProgressEvent{Kind: EventTableStart, TableName: table, RowsTotal: total, Timestamp: time.Now()})

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

		wg.Add(1)
		go func(from, to int64) {
			defer wg.Done()
			if err := e.transferSegmentPK(workerCtx, table, pkCol, from, to, total, &rowsWritten, batchSize); err != nil {
				mu.Lock()
				if firstErr == nil {
					firstErr = err
					cancelWorkers()
				}
				mu.Unlock()
			}
		}(segFrom, segTo)
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

	// Emit EventTableStart immediately so the UI shows "in_progress" while counting.
	now := time.Now()
	e.emit(ProgressEvent{Kind: EventTableStart, TableName: table, RowsTotal: 0, Timestamp: now})

	total, err := e.source.GetRowCount(ctx, table)
	if err != nil {
		return fmt.Errorf("row count(%s): %w", table, err)
	}

	e.db.UpsertTableProgress(ctx, e.migrationID, adapters.TableProgress{ //nolint:errcheck
		TableName: table, Status: adapters.StatusInProgress, RowsTotal: total, StartedAt: &now,
	})
	e.emit(ProgressEvent{Kind: EventTableStart, TableName: table, RowsTotal: total, Timestamp: time.Now()})

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

// transferSegmentPK reads rows in the PK range [pkFrom, pkTo] (both inclusive) in batches
// and writes them to the target. Advances pkFrom past the last seen PK after each batch.
func (e *Engine) transferSegmentPK(ctx context.Context, table, pkCol string, pkFrom, pkTo int64, total int64, rowsWritten *atomic.Int64, batchSize int) error {
	current := pkFrom
	for {
		if err := e.checkPause(ctx); err != nil {
			return err
		}

		t0 := time.Now()
		batch, err := e.source.ReadBatch(ctx, table, adapters.BatchOptions{
			Limit:     batchSize,
			PKCol:     pkCol,
			PKMin:     current,
			PKMax:     pkTo,
			PKMaxIncl: true, // always inclusive — ranges are non-overlapping by construction
		})
		readDur := time.Since(t0)
		if err != nil {
			return fmt.Errorf("read pk-segment %s [%d, %d]: %w", table, current, pkTo, err)
		}
		if batch.Size == 0 {
			break
		}

		t1 := time.Now()
		if err := e.target.WriteBatch(ctx, table, batch); err != nil {
			return fmt.Errorf("write pk-segment %s [%d, %d]: %w", table, current, pkTo, err)
		}
		writeDur := time.Since(t1)

		written := rowsWritten.Add(int64(batch.Size))
		e.emit(ProgressEvent{
			Kind:            EventBatch,
			TableName:       table,
			RowsTransferred: written,
			RowsTotal:       total,
			BatchRows:       batch.Size,
			ReadDuration:    readDur,
			WriteDuration:   writeDur,
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

		t0 := time.Now()
		batch, err := e.source.ReadBatch(ctx, table, adapters.BatchOptions{
			Offset: offset,
			Limit:  limit,
		})
		readDur := time.Since(t0)
		if err != nil {
			return fmt.Errorf("read offset-segment %s offset %d: %w", table, offset, err)
		}
		if batch.Size == 0 {
			break
		}

		t1 := time.Now()
		if err := e.target.WriteBatch(ctx, table, batch); err != nil {
			return fmt.Errorf("write offset-segment %s offset %d: %w", table, offset, err)
		}
		writeDur := time.Since(t1)

		offset += batch.Size
		rowsRead += batch.Size
		written := rowsWritten.Add(int64(batch.Size))
		e.emit(ProgressEvent{
			Kind:            EventBatch,
			TableName:       table,
			RowsTransferred: written,
			RowsTotal:       total,
			BatchRows:       batch.Size,
			ReadDuration:    readDur,
			WriteDuration:   writeDur,
			Timestamp:       time.Now(),
		})
	}
	return nil
}

// resumeState returns the offset, rows already transferred, and keyset resume token
// for a table in the current migration.
//
// lastKey is non-empty only when the last checkpoint was written by a keyset adapter
// (e.g. MongoDB). SQL adapters always leave it empty, so offset mode is used instead.
func (e *Engine) resumeState(ctx context.Context, table string) (offset int, rowsTransferred int64, lastKey string) {
	progress, err := e.db.GetTableProgress(ctx, e.migrationID)
	if err != nil {
		return 0, 0, ""
	}
	for _, tp := range progress {
		if tp.TableName == table {
			rowsTransferred = tp.RowsTransferred
			offset = int(rowsTransferred)
			break
		}
	}
	// Load the keyset token from the most recent checkpoint, if any.
	cp, err := e.db.GetLastCheckpoint(ctx, e.migrationID, table)
	if err == nil && cp != nil && cp.LastPK != "" {
		lastKey = cp.LastPK
		offset = 0 // keyset mode — offset is irrelevant
	}
	return
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
