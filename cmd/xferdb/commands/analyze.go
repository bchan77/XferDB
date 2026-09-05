package commands

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/spf13/cobra"
)

var projectAnalyzeCmd = &cobra.Command{
	Use:   "analyze [name]",
	Short: "Sample source collections and infer schema plan (MongoDB sources)",
	Long: `Connects to the project's MongoDB source, samples documents from each
collection, and infers a recommended Postgres schema plan. The draft plan is
saved and can be reviewed with 'project schema'.

For relational sources (Postgres, MySQL, SQLite) this command runs a schema
diff against the target and reports compatibility issues.`,
	Args: cobra.MaximumNArgs(1),
	RunE: runProjectAnalyze,
}

var projectSchemaCmd = &cobra.Command{
	Use:   "schema [name]",
	Short: "Review or override the inferred schema plan",
	Args:  cobra.MaximumNArgs(1),
	RunE:  runProjectSchema,
}

func init() {
	// analyze flags
	projectAnalyzeCmd.Flags().Int("sample-size", 0, "fixed number of documents to sample per collection, below --sample-threshold (default 2000)")
	projectAnalyzeCmd.Flags().Float64("sample-pct", 0, "percentage of documents to sample once a collection reaches --sample-threshold (default 1.0)")
	projectAnalyzeCmd.Flags().Int64("sample-threshold", 0, "estimated document count at which sampling switches from --sample-size to --sample-pct (default 100000)")
	projectAnalyzeCmd.Flags().Bool("ai", false, "enable AI annotations for ambiguous fields")

	// schema flags
	projectSchemaCmd.Flags().StringArray("set", nil, "override a field: collection.field=type (can repeat)")
	projectSchemaCmd.Flags().Bool("reset", false, "wipe the schema plan; combine with --collection to scope it")
	projectSchemaCmd.Flags().String("collection", "", "filter output or --reset to a single collection")

	projectCmd.AddCommand(projectAnalyzeCmd)
	projectCmd.AddCommand(projectSchemaCmd)
}

func runProjectAnalyze(cmd *cobra.Command, args []string) error {
	var projName string
	if len(args) > 0 {
		projName = args[0]
	}
	name, err := currentProject(projName)
	if err != nil {
		return err
	}
	projID, err := resolveProjectID(name)
	if err != nil {
		return err
	}

	sampleSize, _ := cmd.Flags().GetInt("sample-size")
	samplePct, _ := cmd.Flags().GetFloat64("sample-pct")
	sampleThreshold, _ := cmd.Flags().GetInt64("sample-threshold")
	ai, _ := cmd.Flags().GetBool("ai")

	body := map[string]any{}
	if sampleSize > 0 {
		body["sample_size"] = sampleSize
	}
	if samplePct > 0 {
		body["sample_pct"] = samplePct
	}
	if sampleThreshold > 0 {
		body["sample_threshold"] = sampleThreshold
	}
	if ai {
		body["ai"] = true
	}

	data, _ := json.Marshal(body)
	resp, err := http.Post(
		ServerAddr+"/api/v1/projects/"+projID+"/analyze",
		"application/json",
		bytes.NewReader(data),
	)
	if err != nil {
		return fmt.Errorf("API request failed: %w", err)
	}
	defer resp.Body.Close()

	raw, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		var e map[string]any
		json.Unmarshal(raw, &e)
		return fmt.Errorf("server error %d: %v", resp.StatusCode, e["error"])
	}

	var result map[string]any
	if err := json.Unmarshal(raw, &result); err != nil {
		return fmt.Errorf("decode response: %w", err)
	}

	if t, _ := result["type"].(string); t == "mongo_infer" {
		printMongoAnalysis(result)
	} else {
		out, _ := json.MarshalIndent(result, "", "  ")
		fmt.Println(string(out))
	}
	return nil
}

func printMongoAnalysis(result map[string]any) {
	colls, _ := result["collections"].([]any)
	fmt.Printf("Analyzed %d collection(s)\n\n", len(colls))
	for _, c := range colls {
		m, _ := c.(map[string]any)
		coll, _ := m["collection"].(string)
		schema, _ := m["schema"].(map[string]any)

		fmt.Printf("Collection: %s\n", coll)

		// Print sample stats if available
		if schema != nil {
			estCount, _ := schema["estimated_count"].(float64)
			sampleSize, _ := schema["sample_size"].(float64)
			if estCount > 0 {
				pct := sampleSize / estCount * 100
				fmt.Printf("  Sampled: %d / %d documents (%.2f%%)\n", int(sampleSize), int(estCount), pct)
			}
		}
		if schema == nil {
			warns, _ := m["warnings"].([]any)
			for _, w := range warns {
				fmt.Printf("  WARNING: %v\n", w)
			}
			fmt.Println()
			continue
		}

		fields, _ := schema["fields"].([]any)
		fmt.Printf("  %-30s  %-20s  %-10s  %s\n", "Field", "Postgres Type", "Strategy", "Coverage")
		fmt.Printf("  %s\n", strings.Repeat("-", 75))
		for _, f := range fields {
			fm, _ := f.(map[string]any)
			planRow, _ := fm["plan_row"].(map[string]any)
			freq, _ := fm["frequency"].(map[string]any)

			fieldName, _ := planRow["field_name"].(string)
			pgType, _ := planRow["pg_type"].(string)
			strategy, _ := planRow["strategy"].(string)

			totalDocs, _ := freq["total_docs"].(float64)
			occurrences, _ := freq["occurrences"].(float64)
			coverage := ""
			if totalDocs > 0 {
				coverage = fmt.Sprintf("%.0f%%", occurrences/totalDocs*100)
			}

			polymorphic := ""
			if p, _ := freq["polymorphic"].(bool); p {
				polymorphic = " [polymorphic]"
			}

			fmt.Printf("  %-30s  %-20s  %-10s  %s%s\n", fieldName, pgType, strategy, coverage, polymorphic)
		}
		fmt.Println()
	}
	fmt.Println("Run 'xferdb project schema' to review and override field decisions.")
}

func runProjectSchema(cmd *cobra.Command, args []string) error {
	var projName string
	if len(args) > 0 {
		projName = args[0]
	}
	name, err := currentProject(projName)
	if err != nil {
		return err
	}
	projID, err := resolveProjectID(name)
	if err != nil {
		return err
	}

	reset, _ := cmd.Flags().GetBool("reset")
	setFlags, _ := cmd.Flags().GetStringArray("set")
	collection, _ := cmd.Flags().GetString("collection")

	// --reset: wipe plan.
	if reset {
		url := ServerAddr + "/api/v1/projects/" + projID + "/schema-plan"
		if collection != "" {
			url += "?collection=" + collection
		}
		req, err := http.NewRequest(http.MethodDelete, url, nil)
		if err != nil {
			return fmt.Errorf("build request: %w", err)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return fmt.Errorf("API request failed: %w", err)
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusNoContent {
			var e map[string]any
			json.NewDecoder(resp.Body).Decode(&e)
			return fmt.Errorf("server error %d: %v", resp.StatusCode, e["error"])
		}
		if collection != "" {
			fmt.Printf("Schema plan for collection %q wiped. Run 'project analyze' to re-analyze.\n", collection)
		} else {
			fmt.Println("All schema plans wiped. Run 'project analyze' to re-analyze.")
		}
		return nil
	}

	// --set collection.field=type_or_strategy (repeatable).
	if len(setFlags) > 0 {
		return runSchemaSet(projID, setFlags)
	}

	// Default: list the plan.
	resp, err := http.Get(ServerAddr + "/api/v1/projects/" + projID + "/schema-plan")
	if err != nil {
		return fmt.Errorf("API request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		var e map[string]any
		json.NewDecoder(resp.Body).Decode(&e)
		return fmt.Errorf("server error %d: %v", resp.StatusCode, e["error"])
	}

	var plan map[string][]map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&plan); err != nil {
		return fmt.Errorf("decode schema plan: %w", err)
	}

	if len(plan) == 0 {
		fmt.Println("No schema plan found. Run 'project analyze' first.")
		return nil
	}

	for coll, fields := range plan {
		if collection != "" && coll != collection {
			continue
		}
		fmt.Printf("Collection: %s\n", coll)
		fmt.Printf("  %-30s  %-20s  %-10s  %-8s  %s\n", "Mongo Field", "PG Column", "PG Type", "Strategy", "Flags")
		fmt.Printf("  %s\n", strings.Repeat("-", 85))
		for _, f := range fields {
			fieldName, _ := f["field_name"].(string)
			pgCol, _ := f["pg_column"].(string)
			pgType, _ := f["pg_type"].(string)
			strategy, _ := f["strategy"].(string)

			flags := ""
			if pk, _ := f["is_pk"].(bool); pk {
				flags += "PK "
			}
			if ov, _ := f["overridden"].(bool); ov {
				flags += "overridden"
			}

			fmt.Printf("  %-30s  %-20s  %-10s  %-8s  %s\n", fieldName, pgCol, pgType, strategy, flags)
		}
		fmt.Println()
	}
	return nil
}

// runSchemaSet parses "collection.field=value" entries and calls the override API.
// value can be a pg_type ("numeric") or a strategy ("skip", "flatten", "as_jsonb").
func runSchemaSet(projID string, setFlags []string) error {
	strategies := map[string]bool{"direct": true, "as_jsonb": true, "flatten": true, "skip": true}

	var overrides []map[string]any
	for _, flag := range setFlags {
		eqIdx := strings.Index(flag, "=")
		if eqIdx < 0 {
			return fmt.Errorf("--set format is collection.field=value, got %q", flag)
		}
		lhs := flag[:eqIdx]
		value := flag[eqIdx+1:]

		dotIdx := strings.Index(lhs, ".")
		if dotIdx < 0 {
			return fmt.Errorf("--set format is collection.field=value, got %q (missing collection.field)", flag)
		}
		collection := lhs[:dotIdx]
		field := lhs[dotIdx+1:]

		override := map[string]any{
			"collection": collection,
			"field_name": field,
		}
		if strategies[value] {
			override["strategy"] = value
		} else {
			override["pg_type"] = value
			override["strategy"] = "direct"
		}
		overrides = append(overrides, override)
	}

	data, _ := json.Marshal(overrides)
	req, err := http.NewRequest(http.MethodPut,
		ServerAddr+"/api/v1/projects/"+projID+"/schema-plan",
		bytes.NewReader(data))
	if err != nil {
		return fmt.Errorf("build request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("API request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusNoContent {
		var e map[string]any
		json.NewDecoder(resp.Body).Decode(&e)
		return fmt.Errorf("server error %d: %v", resp.StatusCode, e["error"])
	}

	for _, o := range overrides {
		val := o["strategy"]
		if val == nil {
			val = o["pg_type"]
		}
		fmt.Printf("Override applied: %s.%s = %v\n", o["collection"], o["field_name"], val)
	}
	return nil
}
