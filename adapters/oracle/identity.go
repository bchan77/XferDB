package oracle

import (
	"context"
	"fmt"
)

// IdentityNext implements adapters.IdentitySource: for each identity
// column, its sequence's LAST_NUMBER, the next value Oracle writes to disk.
// With a sequence cache that is up to a cache's worth past the last value
// handed out, which only leaves a gap on the target; never a reused key.
func (s *Source) IdentityNext(ctx context.Context, table string) (map[string]int64, error) {
	cols, otable, err := s.columns(ctx, table)
	if err != nil {
		return nil, err
	}
	rows, err := s.db.QueryContext(ctx, `
SELECT ic.column_name, TO_CHAR(sq.last_number)
FROM all_tab_identity_columns ic
JOIN all_sequences sq ON sq.sequence_owner = ic.owner AND sq.sequence_name = ic.sequence_name
WHERE ic.owner = :1 AND ic.table_name = :2 AND sq.increment_by > 0`, s.owner, otable)
	if err != nil {
		return nil, fmt.Errorf("oracle identity columns(%s): %w", table, err)
	}
	defer rows.Close()
	out := map[string]int64{}
	for rows.Next() {
		var col, next string
		if err := rows.Scan(&col, &next); err != nil {
			return nil, err
		}
		for _, c := range cols {
			if c.oracleName == col {
				if n := pkInt64(next); n > 0 {
					out[c.name] = n
				}
			}
		}
	}
	return out, rows.Err()
}
