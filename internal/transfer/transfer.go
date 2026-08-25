package transfer

import (
	"context"
	"fmt"

	"gitea.homelab.local/nextdevops/XferDB/internal/adapter"
	"gitea.homelab.local/nextdevops/XferDB/pkg/logger"
)

// Transfer handles the actual data migration between two adapters
type Transfer struct {
	source adapter.Adapter
	target adapter.Adapter
	batch  int
	log    *logger.Logger
}

// New creates a new Transfer instance
func New(source, target adapter.Adapter, batchSize int) *Transfer {
	return &Transfer{
		source: source,
		target: target,
		batch:  batchSize,
		log:    logger.New("transfer"),
	}
}

// Run executes the full transfer from source to target
func (t *Transfer) Run(ctx context.Context) error {
	t.log.Info("starting transfer")

	tables, err := t.source.ListTables(ctx)
	if err != nil {
		return fmt.Errorf("listing tables: %w", err)
	}

	for _, table := range tables {
		if err := t.transferTable(ctx, table); err != nil {
			return fmt.Errorf("transferring table %s: %w", table, err)
		}
	}

	t.log.Info("transfer complete")
	return nil
}

func (t *Transfer) transferTable(ctx context.Context, table string) error {
	count, err := t.source.GetTableCount(ctx, table)
	if err != nil {
		return err
	}

	t.log.Info("transferring table %s (%d rows)", table, count)

	offset := 0
	transferred := 0

	for {
		records, err := t.source.ReadBatch(ctx, table, offset, t.batch)
		if err != nil {
			return fmt.Errorf("reading batch at offset %d: %w", offset, err)
		}

		if len(records) == 0 {
			break
		}

		if err := t.target.WriteBatch(ctx, table, records); err != nil {
			return fmt.Errorf("writing batch at offset %d: %w", offset, err)
		}

		transferred += len(records)
		offset += len(records)
		t.log.Info("  progress: %d/%d", transferred, count)
	}

	return nil
}
