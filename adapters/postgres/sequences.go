package postgres

import (
	"context"
	"fmt"
	"sort"
)

// SetSequences implements adapters.SequenceSyncer.
func (t *Target) SetSequences(ctx context.Context, values map[string]int64) (int, error) {
	names := make([]string, 0, len(values))
	for name := range values {
		names = append(names, name)
	}
	sort.Strings(names)
	set := 0
	for _, name := range names {
		var exists bool
		if err := t.db.QueryRowContext(ctx, `SELECT to_regclass($1) IS NOT NULL`, qualify(name)).Scan(&exists); err != nil {
			return set, fmt.Errorf("look up sequence %s: %w", name, err)
		}
		if !exists {
			continue
		}
		if _, err := t.db.ExecContext(ctx, `SELECT setval($1::regclass, $2, true)`, qualify(name), values[name]); err != nil {
			return set, fmt.Errorf("set sequence %s: %w", name, err)
		}
		set++
	}
	return set, nil
}
