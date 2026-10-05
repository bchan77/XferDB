package commands

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
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

Only tables that completed in the migration are validated. The command waits
for the result and exits non-zero if any table fails, so it can gate CI.

Examples:
  xferdb validate
  xferdb validate --level L1
  xferdb validate --migration 3f2a... --tables orders,customers`,
	RunE: func(cmd *cobra.Command, args []string) error {
		projectFlag, _ := cmd.Flags().GetString("project")
		migrationID, _ := cmd.Flags().GetString("migration")
		level, _ := cmd.Flags().GetString("level")
		tablesFlag, _ := cmd.Flags().GetString("tables")

		if err := validate.CheckLevel(level); err != nil {
			return err
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
	validateCmd.Flags().String("level", "", "L1 (row counts) or L2 (compare every row); default: the project setting, else L2")
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
		TableName  string `json:"table_name"`
		Result     string `json:"result"`
		SourceRows *int64 `json:"source_rows"`
		TargetRows *int64 `json:"target_rows"`
		Detail     string `json:"detail"`
		Stale      bool   `json:"stale"`
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
	failed := 0
	if len(v.Tables) > 0 {
		w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
		fmt.Fprintln(w, "  TABLE\tRESULT\tSOURCE ROWS\tTARGET ROWS\tDETAIL")
		for _, t := range v.Tables {
			result := t.Result
			if result == "fail" {
				failed++
				result = "FAIL"
			}
			if t.Stale {
				result += " (stale)"
			}
			fmt.Fprintf(w, "  %s\t%s\t%s\t%s\t%s\n", t.TableName, result, optCount(t.SourceRows), optCount(t.TargetRows), t.Detail)
		}
		w.Flush()
	}

	switch {
	case v.Status == "failed":
		return fmt.Errorf("validation could not finish: %s", v.Error)
	case v.Status == "cancelled":
		return fmt.Errorf("validation cancelled")
	case failed > 0:
		return fmt.Errorf("validation failed: %d table(s) differ between source and target", failed)
	}
	return nil
}

func optCount(n *int64) string {
	if n == nil {
		return "-"
	}
	return fmtInt(*n)
}
