package analyzer

import "fmt"

// Generate produces human-readable suggestions for a list of schema issues.
func Generate(issues []Issue) []string {
	var suggestions []string
	for _, issue := range issues {
		switch issue.Type {
		case "missing_table":
			suggestions = append(suggestions, "Table will be created on target.")
		case "missing_column":
			suggestions = append(suggestions,
				fmt.Sprintf("Column %q (%s) is missing on target; it will be skipped. Add it to the target schema manually if required.",
					issue.Column, issue.SourceType))
		case "type_mismatch":
			suggestions = append(suggestions,
				fmt.Sprintf("Column %q: type %q may lose precision or behave differently when stored as %q on the target.",
					issue.Column, issue.SourceType, issue.TargetType))
		}
	}
	return suggestions
}
