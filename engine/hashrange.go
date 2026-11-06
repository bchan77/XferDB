package engine

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"gitea.homelab.local/nextdevops/XferDB/adapters"
)

// A hash-sharded YugabyteDB table is copied by ranges of its hash code
// (adapters.HashRangeSource) rather than in key order, which costs a full
// scan and sort of the rest of the table for every page. The hash space
// [0, adapters.HashSpace) is cut into chunks of about a batch of rows each,
// and the segment workers take chunks from a shared queue, so one table is
// copied in parallel and the work stays balanced. Each copied chunk is a
// checkpoint, "yb_hash:<chunks>:<index>" (the chunk count, so a resume
// cuts the hash space the same way), and a resumed run copies only the
// chunks that have none.
//
// A live snapshot needs no upper bound here, unlike its key-order copy: a
// chunk is a bounded read, and a row inserted while it is copied is also
// in the stream, whose replay upserts it again.

const hashCheckpointPrefix = "yb_hash:"

// hashRangeColumns returns the hash columns to copy table by, or nil to
// copy it the usual way: a source that can't read hash ranges, a table
// whose key isn't hash-sharded, or one a previous run began copying the
// other way (its checkpoints mean something else).
func (e *Engine) hashRangeColumns(ctx context.Context, table string) ([]string, adapters.HashRangeSource) {
	hrs, ok := e.source.(adapters.HashRangeSource)
	if !ok {
		return nil, nil
	}
	cols, err := hrs.HashColumns(ctx, table)
	if err != nil || len(cols) == 0 {
		return nil, nil
	}
	if cp, _ := e.db.GetLastCheckpoint(ctx, e.migrationID, table); cp != nil && !strings.HasPrefix(cp.LastPK, hashCheckpointPrefix) {
		return nil, nil
	}
	return cols, hrs
}

// hashChunks is how many chunks to cut the hash space into: about a batch
// of rows each, at least one per worker, at most one per hash code.
func hashChunks(total int64, batchSize, workers int) int {
	n := int((total + int64(batchSize) - 1) / int64(batchSize))
	if n < workers {
		n = workers
	}
	if n < 1 {
		n = 1
	}
	if n > adapters.HashSpace {
		n = adapters.HashSpace
	}
	return n
}

// hashChunkRange is chunk i's hash codes, [lo, hi), of n chunks.
func hashChunkRange(i, n int) (lo, hi int) {
	return i * adapters.HashSpace / n, (i + 1) * adapters.HashSpace / n
}

// parseHashCheckpoint reads a "yb_hash:<chunks>:<index>" checkpoint.
func parseHashCheckpoint(s string) (chunks, index int, ok bool) {
	rest, found := strings.CutPrefix(s, hashCheckpointPrefix)
	if !found {
		return 0, 0, false
	}
	a, b, found := strings.Cut(rest, ":")
	if !found {
		return 0, 0, false
	}
	n, err1 := strconv.Atoi(a)
	i, err2 := strconv.Atoi(b)
	if err1 != nil || err2 != nil || n < 1 || i < 0 || i >= n {
		return 0, 0, false
	}
	return n, i, true
}

func (e *Engine) transferTableHashRanges(ctx context.Context, schema adapters.TableSchema, hrs adapters.HashRangeSource, cols []string) error {
	table := schema.Name
	cfg := e.project.TransferConfig
	batchSize := cfg.BatchSize
	if batchSize <= 0 {
		batchSize = defaultBatchSize
	}
	workers := cfg.SegmentWorkers
	if workers < 1 {
		workers = 1
	}

	now := time.Now()
	e.emit(ProgressEvent{Kind: EventTableCounting, TableName: table, Timestamp: now})
	total, err := e.source.GetRowCount(ctx, table)
	if err != nil {
		return fmt.Errorf("row count(%s): %w", table, err)
	}

	// What an earlier run of this migration already copied.
	chunks := hashChunks(total, batchSize, workers)
	done := map[int]bool{}
	var copied int64
	cps, err := e.db.ListCheckpoints(ctx, e.migrationID)
	if err != nil {
		return err
	}
	for _, cp := range cps {
		if cp.TableName != table {
			continue
		}
		if n, i, ok := parseHashCheckpoint(cp.LastPK); ok {
			chunks = n
			if !done[i] {
				done[i] = true
				copied += int64(cp.RowsInBatch)
			}
		}
	}

	if err := e.db.UpsertTableProgress(ctx, e.migrationID, adapters.TableProgress{
		TableName: table, Status: adapters.StatusInProgress,
		RowsTotal: total, RowsTransferred: copied, StartedAt: &now,
	}); err != nil {
		return err
	}
	e.emit(ProgressEvent{Kind: EventTableStart, TableName: table, RowsTotal: total, RowsTransferred: copied, Timestamp: time.Now()})

	queue := make(chan int, chunks)
	for i := 0; i < chunks; i++ {
		if !done[i] {
			queue <- i
		}
	}
	close(queue)

	var written atomic.Int64
	written.Store(copied)
	var progressMu sync.Mutex // progress rows and events in order
	workerCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	var wg sync.WaitGroup
	var errMu sync.Mutex
	var firstErr error
	fail := func(err error) {
		errMu.Lock()
		if firstErr == nil {
			firstErr = err
			cancel()
		}
		errMu.Unlock()
	}

	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range queue {
				if err := e.checkPause(workerCtx); err != nil {
					fail(err)
					return
				}
				lo, hi := hashChunkRange(i, chunks)
				t0 := time.Now()
				batch, err := hrs.ReadHashRange(workerCtx, table, cols, lo, hi)
				readDur := time.Since(t0)
				if err != nil {
					fail(err)
					return
				}
				t1 := time.Now()
				if batch.Size > 0 {
					if err := e.target.WriteBatch(workerCtx, table, batch); err != nil {
						fail(fmt.Errorf("write hash range [%d, %d) of %s: %w", lo, hi, table, err))
						return
					}
				}
				writeDur := time.Since(t1)

				progressMu.Lock()
				err = e.db.SaveCheckpoint(workerCtx, adapters.Checkpoint{
					MigrationID: e.migrationID, TableName: table, BatchID: i + 1,
					LastPK:      fmt.Sprintf("%s%d:%d", hashCheckpointPrefix, chunks, i),
					RowsInBatch: batch.Size, CreatedAt: time.Now(),
				})
				n := written.Add(int64(batch.Size))
				if err == nil {
					err = e.db.UpsertTableProgress(workerCtx, e.migrationID, adapters.TableProgress{
						TableName: table, Status: adapters.StatusInProgress, RowsTotal: total, RowsTransferred: n,
					})
				}
				if err == nil && batch.Size > 0 {
					e.emit(ProgressEvent{
						Kind: EventBatch, TableName: table, RowsTransferred: n, RowsTotal: total,
						BatchRows: batch.Size, ReadDuration: readDur, WriteDuration: writeDur, Timestamp: time.Now(),
					})
				}
				progressMu.Unlock()
				if err != nil {
					fail(fmt.Errorf("checkpoint %s: %w", table, err))
					return
				}
			}
		}()
	}
	wg.Wait()
	if firstErr != nil {
		return firstErr
	}

	finished := time.Now()
	n := written.Load()
	if err := e.db.UpsertTableProgress(ctx, e.migrationID, adapters.TableProgress{
		TableName: table, Status: adapters.StatusCompleted,
		RowsTotal: total, RowsTransferred: n, CompletedAt: &finished,
	}); err != nil {
		return err
	}
	e.emit(ProgressEvent{Kind: EventTableDone, TableName: table, RowsTransferred: n, RowsTotal: total, Timestamp: finished})
	e.tableDone(table)
	return nil
}
