package analyzer

import (
	"context"
	"fmt"

	"gitea.homelab.local/nextdevops/XferDB/adapters"
)

// Issue describes a single schema incompatibility between source and target.
type Issue struct {
	Type       string // "missing_table" | "missing_column" | "type_mismatch"
	Column     string
	SourceType string
	TargetType string
	Severity   string // "error" | "warning"
	Suggestion string
}

// TableAnalysis holds the diff result for a single table.
type TableAnalysis struct {
	Name        string
	Status      string // "compatible" | "incompatible"
	Issues      []Issue
	Suggestions []string
}

// Analysis is the full result of comparing source and target schemas.
type Analysis struct {
	SourceType string
	TargetType string
	Compatible bool // false if any error-severity issue exists
	Tables     []TableAnalysis
}

// Analyzer compares source and target schemas.
type Analyzer struct{}

// New returns a new Analyzer.
func New() *Analyzer { return &Analyzer{} }

// Analyze introspects source and target, diffs each table, and returns an Analysis.
func (a *Analyzer) Analyze(ctx context.Context, source adapters.SourceAdapter, target adapters.TargetAdapter) (*Analysis, error) {
	srcTables, err := source.ListTables(ctx)
	if err != nil {
		return nil, fmt.Errorf("list source tables: %w", err)
	}

	// Index target tables by name.
	tgtTables, err := target.ListTables(ctx)
	if err != nil {
		return nil, fmt.Errorf("list target tables: %w", err)
	}
	tgtIndex := make(map[string]*adapters.TableSchema, len(tgtTables))
	for i := range tgtTables {
		tgtIndex[tgtTables[i].Name] = &tgtTables[i]
	}

	analysis := &Analysis{
		Compatible: true,
	}

	for _, st := range srcTables {
		issues := Compare(&st, tgtIndex[st.Name])
		suggestions := Generate(issues)

		// Attach suggestions to their issues.
		for i := range issues {
			if i < len(suggestions) {
				issues[i].Suggestion = suggestions[i]
			}
		}

		status := "compatible"
		for _, iss := range issues {
			if iss.Severity == "error" {
				status = "incompatible"
				analysis.Compatible = false
				break
			}
		}

		analysis.Tables = append(analysis.Tables, TableAnalysis{
			Name:        st.Name,
			Status:      status,
			Issues:      issues,
			Suggestions: suggestions,
		})
	}

	return analysis, nil
}
