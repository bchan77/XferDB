package commands

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strings"

	"github.com/spf13/cobra"
)

// resumeCmd resumes the project's paused migration. A live migration paused
// at a schema change on the source (#102) is resumed past it: the change
// should be on the target by then.
var resumeCmd = &cobra.Command{
	Use:   "resume",
	Short: "Resume the project's paused, failed or cancelled migration",
	Long: `Resume the project's latest migration if it is paused, or if it failed or
was cancelled (a snapshot migration only).

A failed or cancelled migration runs again under the same id and settings:
tables it completed are skipped, the others continue from their last
checkpoint, nothing on the target is dropped or truncated, and of the indexes
and constraints created after the copy only those not yet created are run.
--table-workers may change how many tables are copied at once.

A live migration (beta) pauses by itself when a table's columns change on the
source (a column added, dropped or retyped): every change before it is
applied, none after it. Make the same change on the target, then resume:
the changes after it are applied to the target as it now is. The command
shows the change and asks first, unless --yes is given.`,
	RunE: func(cmd *cobra.Command, args []string) error {
		projectFlag, _ := cmd.Flags().GetString("project")
		yes, _ := cmd.Flags().GetBool("yes")

		projectName, err := currentProject(projectFlag)
		if err != nil {
			return err
		}
		projectID, err := resolveProjectID(projectName)
		if err != nil {
			return err
		}
		migrationID, err := latestMigrationID(projectID)
		if err != nil {
			return err
		}
		m, err := getMigration(migrationID)
		if err != nil {
			return err
		}
		if m.Status == "failed" || m.Status == "cancelled" {
			if m.Mode == "live" {
				return fmt.Errorf("live migration %s %s; its stream is gone, so it can't be resumed: start a new one", migrationID, m.Status)
			}
			workers, _ := cmd.Flags().GetInt("table-workers")
			if err := patchMigration(migrationID, map[string]any{"action": "resume", "table_workers": workers}); err != nil {
				return err
			}
			fmt.Printf("Resumed %s migration %s.\n", m.Status, migrationID)
			return pollStats(migrationID)
		}
		if m.Status != "paused" {
			return fmt.Errorf("migration %s isn't paused, failed or cancelled (%s)", migrationID, m.Status)
		}
		if m.Live != nil && m.Live.SchemaChange != nil && !yes {
			sc := m.Live.SchemaChange
			fmt.Printf("Live migration %s is paused at a schema change on the source, table %s:\n    %s\n",
				migrationID, sc.Table, strings.Join(sc.Summary, "\n    "))
			fmt.Print("Have you made the same change on the target? [y/N] ")
			answer, _ := bufio.NewReader(os.Stdin).ReadString('\n')
			if a := strings.ToLower(strings.TrimSpace(answer)); a != "y" && a != "yes" {
				return fmt.Errorf("not resumed")
			}
		}
		if err := patchMigrationAction(migrationID, "resume"); err != nil {
			return err
		}
		fmt.Printf("Resumed migration %s.\n", migrationID)
		if m.Mode == "live" {
			return followLive(migrationID)
		}
		return nil
	},
}

func init() {
	resumeCmd.Flags().StringP("project", "p", "", "Project name (overrides current context)")
	resumeCmd.Flags().BoolP("yes", "y", false, "Don't ask to confirm a schema change is on the target")
	resumeCmd.Flags().Int("table-workers", 0, "For a failed or cancelled migration: tables copied at once (default: as the run began)")
	RootCmd.AddCommand(resumeCmd)
}

// patchMigrationAction pauses, resumes or cancels a migration.
func patchMigrationAction(id, action string) error {
	return patchMigration(id, map[string]any{"action": action})
}

// patchMigration sends PATCH /migrations/:id with fields.
func patchMigration(id string, fields map[string]any) error {
	action, _ := fields["action"].(string)
	body, _ := json.Marshal(fields)
	req, err := http.NewRequest(http.MethodPatch, ServerAddr+"/api/v1/migrations/"+id, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("%s: %w", action, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		var out struct {
			Error string `json:"error"`
		}
		json.NewDecoder(resp.Body).Decode(&out)
		return fmt.Errorf("%s refused: %s %s", action, resp.Status, out.Error)
	}
	return nil
}
