package engine

import (
	"context"
	"fmt"
	"time"

	"gitea.homelab.local/nextdevops/XferDB/adapters"
)

const defaultBatchSize = 1000

// transferTable copies all rows from source to target for a single table,
// resuming from the last checkpoint if one exists.
// Table creation is handled in Phase 1 of Engine.Run before this is called.
func (e *Engine) transferTable(ctx context.Context, sourceSchema adapters.TableSchema) error {
	table := sourceSchema.Name

	total, err := e.source.GetRowCount(ctx, table)
	if err != nil {
		return fmt.Errorf("row count(%s): %w", table, err)
	}

	// Determine starting offset from any existing progress (supports resume).
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

	batchID := offset / batchSize // approximate batch counter for the checkpoint key

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
			break // all rows transferred
		}

		if err := e.target.WriteBatch(ctx, table, batch); err != nil {
			return fmt.Errorf("write batch %s offset %d: %w", table, offset, err)
		}

		offset += batch.Size
		rowsTransferred += int64(batch.Size)
		batchID++

		// Checkpoint saved AFTER the write — ensures at-least-once delivery.
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
