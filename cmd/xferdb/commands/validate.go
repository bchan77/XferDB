package commands

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strconv"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/spf13/cobra"
	"gitea.homelab.local/nextdevops/XferDB/validate"
)

var validateCmd = &cobra.Command{
	Use:   "validate",
	Short: "Check that a migration's target matches its source",
	Long: `Compare the rows a migration intended to write with the rows the target holds.

Levels:
  L1  row counts per table
  L2  row counts plus a fingerprint of every row (default)
  L3  L2, then a row diff of each table that fails, listing the rows missing
      in the target, the rows only in the target, and the rows whose values
      differ. Needs a primary key of a type it can pair rows by; other tables
      keep their L2 result. --sample checks only part of a failing table.

A column narrowed by a schema-plan type override (numeric(10,4) to
numeric(10,2), double to integer, timestamp to date) is converted by the
target as it is stored. Validation applies the same conversion to the source
value, and a table whose only differences are those conversions is reported
as "lossy (plan)": not a failure, with the changed values listed.

Only tables that completed in the migration are validated. The command waits
for the result and exits non-zero if any table fails, so it can gate CI.

Examples:
  xferdb validate
  xferdb validate --level L1
  xferdb validate --level L3 --sample 10
  xferdb validate --migration 3f2a... --tables orders,customers`,
	RunE: func(cmd *cobra.Command, args []string) error {
		projectFlag, _ := cmd.Flags().GetString("project")
		migrationID, _ := cmd.Flags().GetString("migration")
		level, _ := cmd.Flags().GetString("level")
		tablesFlag, _ := cmd.Flags().GetString("tables")
		sample, _ := cmd.Flags().GetInt("sample")

		if err := validate.CheckLevel(level); err != nil {
			return err
		}
		if cmd.Flags().Changed("sample") {
			if err := validate.CheckSample(sample); err != nil {
				return err
			}
			if level != validate.LevelL3 {
				return fmt.Errorf("--sample applies only to --level L3")
			}
		}
		var tables []string
		for _, t := range strings.Split(tablesFlag, ",") {
			if t = strings.TrimSpace(t); t != "" {
				tables = append(tables, t)
			}
		}

		if migrationID == "" {
			projectName, err := currentProject(projectFlag)
			if err != nil {
				return err
			}
			projectID, err := resolveProjectID(projectName)
			if err != nil {
				return err
			}
			if migrationID, err = latestMigrationID(projectID); err != nil {
				return err
			}
		}

		req := map[string]any{}
		if level != "" {
			req["level"] = level
		}
		if len(tables) > 0 {
			req["tables"] = tables
		}
		if cmd.Flags().Changed("sample") {
			req["sample"] = sample
		}
		body, _ := json.Marshal(req)
		resp, err := http.Post(ServerAddr+"/api/v1/migrations/"+migrationID+"/validations",
			"application/json", bytes.NewReader(body))
		if err != nil {
			return fmt.Errorf("start validation: %w", err)
		}
		defer resp.Body.Close()
		var started struct {
			ID    string `json:"id"`
			Level string `json:"level"`
			Error string `json:"error"`
		}
		json.NewDecoder(resp.Body).Decode(&started)
		if resp.StatusCode != http.StatusCreated {
			return fmt.Errorf("server error %d: %s", resp.StatusCode, started.Error)
		}
		fmt.Printf("Validation started: %s (level %s, migration %s)\n", started.ID, started.Level, migrationID)
		return followValidation(started.ID)
	},
}

func init() {
	RootCmd.AddCommand(validateCmd)
	validateCmd.Flags().StringP("project", "p", "", "Project name (overrides current context)")
	validateCmd.Flags().String("migration", "", "Migration ID to validate (default: the project's latest migration)")
	validateCmd.Flags().String("level", "", "L1 (row counts), L2 (compare every row) or L3 (L2, then list the differing rows of a failing table); default: the project setting, else L2")
	validateCmd.Flags().Int("sample", 0, "With --level L3: percentage of a failing table to diff; 0 checks every row")
	validateCmd.Flags().String("tables", "", "Comma-separated tables to validate (default: every table that completed)")
}

// awaitAutoValidation follows the validation the server starts on its own
// when a migration completes.
func awaitAutoValidation(migrationID string) error {
	fmt.Println("\nWaiting for validation (skip it next time with --skip-validation)...")
	url := ServerAddr + "/api/v1/migrations/" + migrationID + "/validations"
	// The server starts the run right after marking the migration complete;
	// give it a moment to appear.
	for i := 0; i < 30; i++ {
		resp, err := http.Get(url)
		if err == nil {
			var vs []struct {
				ID string `json:"id"`
			}
			json.NewDecoder(resp.Body).Decode(&vs)
			resp.Body.Close()
			if len(vs) > 0 {
				return followValidation(vs[0].ID)
			}
		}
		time.Sleep(time.Second)
	}
	return fmt.Errorf("no validation appeared for migration %s; start one with 'xferdb validate --migration %s'", migrationID, migrationID)
}

type validationView struct {
	ID     string `json:"id"`
	Level  string `json:"level"`
	Status string `json:"status"`
	Error  string `json:"error"`
	Tables []struct {
		TableName    string `json:"table_name"`
		Result       string `json:"result"`
		SourceRows   *int64 `json:"source_rows"`
		TargetRows   *int64 `json:"target_rows"`
		Detail       string `json:"detail"`
		Stale        bool   `json:"stale"`
		LevelRun     string `json:"level_run"`
		LevelNote    string `json:"level_note"`
		DiffMissing  int64  `json:"diff_missing"`
		DiffExtra    int64  `json:"diff_extra"`
		DiffMismatch int64  `json:"diff_mismatch"`
		LossyRows    int64  `json:"lossy_rows"`
	} `json:"tables"`
}

// followValidation polls a validation until it finishes, prints each table's
// result, and returns an error when any table failed or the run itself did.
func followValidation(id string) error {
	url := ServerAddr + "/api/v1/validations/" + id
	var v validationView
	for {
		resp, err := http.Get(url)
		if err != nil {
			fmt.Printf("  [warn] validation fetch failed: %v\n", err)
			time.Sleep(2 * time.Second)
			continue
		}
		v = validationView{}
		json.NewDecoder(resp.Body).Decode(&v)
		resp.Body.Close()

		done := 0
		for _, t := range v.Tables {
			if t.Result != "skipped" {
				done++
			}
		}
		if v.Status == "completed" || v.Status == "failed" || v.Status == "cancelled" {
			fmt.Printf("\r\033[2K")
			break
		}
		fmt.Printf("\r\033[2KValidating (%s)... %d table(s) checked", v.Level, done)
		time.Sleep(2 * time.Second)
	}

	fmt.Printf("Validation %s (%s): %s\n", v.ID, v.Level, v.Status)
	failed, lossy := 0, 0
	if len(v.Tables) > 0 {
		w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
		fmt.Fprintln(w, "  TABLE\tRESULT\tSOURCE ROWS\tTARGET ROWS\tDETAIL")
		for _, t := range v.Tables {
			result := t.Result
			switch result {
			case "fail":
				failed++
				result = "FAIL"
			case "expected_lossy":
				lossy++
				result = "lossy (plan)"
			}
			if t.Stale {
				result += " (stale)"
			}
			fmt.Fprintf(w, "  %s\t%s\t%s\t%s\t%s\n", t.TableName, result, optCount(t.SourceRows), optCount(t.TargetRows), t.Detail)
		}
		w.Flush()
		for _, t := range v.Tables {
			if t.LevelNote != "" && t.Result != "skipped" {
				fmt.Printf("  note: %s: %s\n", t.TableName, t.LevelNote)
			}
		}
		for _, t := range v.Tables {
			if t.DiffMissing+t.DiffExtra+t.DiffMismatch+t.LossyRows > 0 {
				printDiffExamples(v.ID, t.TableName)
			}
		}
	}

	switch {
	case v.Status == "failed":
		return fmt.Errorf("validation could not finish: %s", v.Error)
	case v.Status == "cancelled":
		return fmt.Errorf("validation cancelled")
	case failed > 0:
		return fmt.Errorf("validation failed: %d table(s) differ between source and target", failed)
	}
	if lossy > 0 {
		// Not a failure: the user's own type overrides changed these values.
		fmt.Printf("%d table(s) match once the plan's lossy type overrides are applied; the values they changed are listed above.\n", lossy)
	}
	return nil
}

// diffExamplesShown is how many of a table's recorded diff examples the CLI
// prints; the rest are in the API and the web UI.
const diffExamplesShown = 5

type diffExample struct {
	Kind    string             `json:"kind"`
	Key     map[string]*string `json:"key"`
	Columns []string           `json:"columns"`
	Source  map[string]*string `json:"source"`
	Target  map[string]*string `json:"target"`
}

// printDiffExamples prints the first few rows an L3 diff found for table.
func printDiffExamples(validationID, table string) {
	resp, err := http.Get(ServerAddr + "/api/v1/validations/" + validationID + "/tables/" + url.PathEscape(table) + "/diffs")
	if err != nil {
		return
	}
	defer resp.Body.Close()
	var diffs []diffExample
	if json.NewDecoder(resp.Body).Decode(&diffs) != nil || len(diffs) == 0 {
		return
	}
	fmt.Printf("\n  %s: differing rows (first %d of %d recorded)\n", table, min(diffExamplesShown, len(diffs)), len(diffs))
	for _, d := range diffs[:min(diffExamplesShown, len(diffs))] {
		key := showValues(d.Key)
		switch d.Kind {
		case "missing_in_target":
			fmt.Printf("    missing in target   %s\n", key)
		case "extra_in_target":
			fmt.Printf("    only in target      %s\n", key)
		case "lossy_by_plan":
			fmt.Printf("    changed by plan     %s  %s\n", key, changedColumns(d))
		default:
			fmt.Printf("    values differ       %s  %s\n", key, changedColumns(d))
		}
	}
}

// changedColumns renders a diff's columns as col: source -> target.
func changedColumns(d diffExample) string {
	var cols []string
	for _, c := range d.Columns {
		cols = append(cols, fmt.Sprintf("%s: %s -> %s", c, showValue(d.Source[c]), showValue(d.Target[c])))
	}
	return strings.Join(cols, ", ")
}

// showValues renders a key as col=value pairs in column order.
func showValues(m map[string]*string) string {
	names := make([]string, 0, len(m))
	for k := range m {
		names = append(names, k)
	}
	sort.Strings(names)
	parts := make([]string, len(names))
	for i, k := range names {
		parts[i] = k + "=" + showValue(m[k])
	}
	return strings.Join(parts, " ")
}

func showValue(v *string) string {
	if v == nil {
		return "NULL"
	}
	return strconv.Quote(*v)
}

func optCount(n *int64) string {
	if n == nil {
		return "-"
	}
	return fmtInt(*n)
}
