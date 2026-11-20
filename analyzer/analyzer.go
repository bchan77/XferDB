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
	Severity   string // "error" | "warning" | "info"
	Suggestion string
}

// TableAnalysis holds the diff result for a single table.
type TableAnalysis struct {
	Name        string
	Status      string // "compatible" | "new" (not on the target yet) | "incompatible"
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
		t := AnalyzeTable(&st, tgtIndex[st.Name])
		if t.Status == "incompatible" {
			analysis.Compatible = false
		}
		analysis.Tables = append(analysis.Tables, t)
	}

	return analysis, nil
}

// AnalyzeTable compares one source table with the target's table of the same
// name (nil when the target has none): its issues, each with its
// suggestion, and its status.
func AnalyzeTable(src, tgt *adapters.TableSchema) TableAnalysis {
	issues := Compare(src, tgt)
	suggestions := Generate(issues)
	for i := range issues {
		if i < len(suggestions) {
			issues[i].Suggestion = suggestions[i]
		}
	}
	return TableAnalysis{Name: src.Name, Status: tableStatus(issues), Issues: issues, Suggestions: suggestions}
}

// tableStatus sums up a table's issues: "new" for a table the target doesn't
// have yet (the migration creates it, so that is no fault), "incompatible"
// when any issue is an error, else "compatible" (warnings included).
func tableStatus(issues []Issue) string {
	status := "compatible"
	for _, iss := range issues {
		switch {
		case iss.Type == "missing_table":
			return "new"
		case iss.Severity == "error":
			status = "incompatible"
		}
	}
	return status
}
