package commands

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/spf13/cobra"
	"golang.org/x/term"
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

	if resp.StatusCode != http.StatusOK {
		var e map[string]any
		json.NewDecoder(resp.Body).Decode(&e)
		return fmt.Errorf("server error %d: %v", resp.StatusCode, e["error"])
	}

	// Check if response is streaming NDJSON.
	contentType := resp.Header.Get("Content-Type")
	if strings.Contains(contentType, "application/x-ndjson") {
		return handleStreamingAnalyze(resp)
	}

	// Fallback: non-streaming JSON response (relational sources).
	var result map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return fmt.Errorf("decode response: %w", err)
	}
	out, _ := json.MarshalIndent(result, "", "  ")
	fmt.Println(string(out))
	return nil
}

// progressState tracks the current sampling progress for the spinner.
type progressState struct {
	mu         sync.Mutex
	collection string
	scanned    int
	target     int
	startTime  time.Time
	done       bool
}

func (p *progressState) update(collection string, scanned, target int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.collection != collection {
		p.collection = collection
		p.startTime = time.Now()
	}
	p.scanned = scanned
	p.target = target
}

func (p *progressState) markDone() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.done = true
}

func (p *progressState) isDone() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.done
}

func (p *progressState) get() (string, int, int, time.Duration) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.collection, p.scanned, p.target, time.Since(p.startTime)
}

// handleStreamingAnalyze processes NDJSON streaming response from MongoDB analyze.
func handleStreamingAnalyze(resp *http.Response) error {
	isTTY := term.IsTerminal(int(os.Stdout.Fd()))
	scanner := bufio.NewScanner(resp.Body)

	var collections []map[string]any
	spinnerChars := []string{"⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"}
	spinnerIdx := 0

	state := &progressState{startTime: time.Now()}
	var lastLineLen int

	// Spinner goroutine - updates display every 100ms even when no new events.
	stopSpinner := make(chan struct{})
	var spinnerWg sync.WaitGroup

	clearLine := func() {
		if isTTY && lastLineLen > 0 {
			fmt.Printf("\r%s\r", strings.Repeat(" ", lastLineLen))
			lastLineLen = 0
		}
	}

	if isTTY {
		spinnerWg.Add(1)
		go func() {
			defer spinnerWg.Done()
			ticker := time.NewTicker(100 * time.Millisecond)
			defer ticker.Stop()

			for {
				select {
				case <-stopSpinner:
					return
				case <-ticker.C:
					if state.isDone() {
						continue
					}
					coll, scanned, target, elapsed := state.get()
					if coll == "" {
						continue
					}

					pct := float64(0)
					if target > 0 {
						pct = float64(scanned) / float64(target) * 100
					}

					// Calculate rows/sec.
					rowsPerSec := float64(0)
					if elapsed.Seconds() > 0 {
						rowsPerSec = float64(scanned) / elapsed.Seconds()
					}

					spinner := spinnerChars[spinnerIdx%len(spinnerChars)]
					spinnerIdx++

					line := fmt.Sprintf("  %s Sampling %s: %d / %d (%.1f%%)  [%s, %.0f rows/s]",
						spinner, coll, scanned, target, pct, formatDuration(elapsed), rowsPerSec)

					// Clear and rewrite.
					if lastLineLen > 0 {
						fmt.Printf("\r%s\r", strings.Repeat(" ", lastLineLen))
					}
					fmt.Print(line)
					lastLineLen = len(line)
				}
			}
		}()
	}

	for scanner.Scan() {
		line := scanner.Text()
		if line == "" {
			continue
		}

		var event map[string]any
		if err := json.Unmarshal([]byte(line), &event); err != nil {
			continue
		}

		eventType, _ := event["type"].(string)

		switch eventType {
		case "progress":
			coll, _ := event["collection"].(string)
			scanned, _ := event["scanned"].(float64)
			target, _ := event["target"].(float64)
			state.update(coll, int(scanned), int(target))

		case "collection":
			clearLine()

			coll, _ := event["collection"].(string)
			schema, _ := event["schema"].(map[string]any)
			warnings, _ := event["warnings"].([]any)

			fmt.Printf("Collection: %s\n", coll)

			if schema != nil {
				estCount, _ := schema["estimated_count"].(float64)
				sampleSize, _ := schema["sample_size"].(float64)
				if estCount > 0 {
					pct := sampleSize / estCount * 100
					fmt.Printf("  Sampled: %d / %d documents (%.2f%%)\n", int(sampleSize), int(estCount), pct)
				}
				collections = append(collections, map[string]any{"collection": coll, "schema": schema})
				printCollectionFields(schema)
			} else if len(warnings) > 0 {
				for _, w := range warnings {
					fmt.Printf("  WARNING: %v\n", w)
				}
			}
			fmt.Println()

			// Reset state for next collection.
			state.update("", 0, 0)

		case "error":
			state.markDone()
			close(stopSpinner)
			spinnerWg.Wait()
			clearLine()
			errMsg, _ := event["error"].(string)
			return fmt.Errorf("server error: %s", errMsg)

		case "done":
			state.markDone()
			close(stopSpinner)
			spinnerWg.Wait()
			clearLine()
			totalColls, _ := event["total_collections"].(float64)
			fmt.Printf("Analyzed %d collection(s)\n", int(totalColls))
			fmt.Println("Run 'xferdb project schema' to review and override field decisions.")
		}
	}

	// Ensure spinner is stopped if we exit early.
	if !state.isDone() {
		state.markDone()
		close(stopSpinner)
		spinnerWg.Wait()
	}

	return scanner.Err()
}

// formatDuration formats a duration as "1m23s" or "45s".
func formatDuration(d time.Duration) string {
	d = d.Round(time.Second)
	m := d / time.Minute
	s := (d % time.Minute) / time.Second
	if m > 0 {
		return fmt.Sprintf("%dm%02ds", m, s)
	}
	return fmt.Sprintf("%ds", s)
}

// printCollectionFields prints the field table for a collection schema.
func printCollectionFields(schema map[string]any) {
	fields, _ := schema["fields"].([]any)
	if len(fields) == 0 {
		return
	}

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
