package validator

import (
	"context"
	"fmt"

	"gitea.homelab.local/nextdevops/XferDB/internal/adapter"
	"gitea.homelab.local/nextdevops/XferDB/pkg/logger"
)

// Validator verifies data integrity after transfer
type Validator struct {
	source adapter.Adapter
	target adapter.Adapter
	log    *logger.Logger
}

// New creates a new Validator
func New(source, target adapter.Adapter) *Validator {
	return &Validator{
		source: source,
		target: target,
		log:    logger.New("validator"),
	}
}

// Validate runs integrity checks on all tables
func (v *Validator) Validate(ctx context.Context) error {
	v.log.Info("starting validation")

	tables, err := v.source.ListTables(ctx)
	if err != nil {
		return fmt.Errorf("listing tables: %w", err)
	}

	for _, table := range tables {
		if err := v.validateTable(ctx, table); err != nil {
			v.log.Error("validation failed for table %s: %v", table, err)
			return err
		}
		v.log.Info("  ✅ table %s passed", table)
	}

	v.log.Info("validation complete — all tables OK")
	return nil
}

func (v *Validator) validateTable(ctx context.Context, table string) error {
	srcCount, err := v.source.GetTableCount(ctx, table)
	if err != nil {
		return err
	}

	tgtCount, err := v.target.GetTableCount(ctx, table)
	if err != nil {
		return err
	}

	if srcCount != tgtCount {
		return fmt.Errorf("row count mismatch: source=%d target=%d", srcCount, tgtCount)
	}

	return nil
}
