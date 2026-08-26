package analyzer

import "gitea.homelab.local/nextdevops/XferDB/adapters"

// Compare returns the list of issues between a source and target table schema.
// If tgt is nil, the entire table is flagged as missing.
func Compare(src, tgt *adapters.TableSchema) []Issue {
	if tgt == nil || len(tgt.Columns) == 0 {
		return []Issue{{
			Type:     "missing_table",
			Severity: "error",
		}}
	}

	// Index target columns by name for O(1) lookup.
	tgtCols := make(map[string]adapters.ColumnDef, len(tgt.Columns))
	for _, c := range tgt.Columns {
		tgtCols[c.Name] = c
	}

	var issues []Issue
	for _, sc := range src.Columns {
		tc, ok := tgtCols[sc.Name]
		if !ok {
			issues = append(issues, Issue{
				Type:       "missing_column",
				Column:     sc.Name,
				SourceType: sc.Type,
				Severity:   "error",
			})
			continue
		}
		if sc.Type != tc.Type {
			issues = append(issues, Issue{
				Type:       "type_mismatch",
				Column:     sc.Name,
				SourceType: sc.Type,
				TargetType: tc.Type,
				Severity:   "warning",
			})
		}
	}
	return issues
}
