package commands

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"gitea.homelab.local/nextdevops/XferDB/validate"
)

// cutoverCmd ends a live migration (#102): the server drains the stream,
// checks the source has stopped writing, sets sequences, adds the
// constraints held back until now, drops the replication slot and marks the
// migration completed, which is then validated.
var cutoverCmd = &cobra.Command{
	Use:   "cutover",
	Short: "Cut a live migration over to the target",
	Long: `Cut the project's live migration over to the target.

Stop every application writing to the source first: the cutover drains the
changes made so far, then checks for a few seconds that no more arrive. If
some do, it stops and the migration keeps streaming; cut over again once the
writes have stopped, or pass --ignore-writes to leave later writes behind.

Once completed, the migration is validated by the project's settings, and
the command exits non-zero when a table fails.`,
	RunE: func(cmd *cobra.Command, args []string) error {
		projectFlag, _ := cmd.Flags().GetString("project")
		yes, _ := cmd.Flags().GetBool("yes")
		ignoreWrites, _ := cmd.Flags().GetBool("ignore-writes")

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
		if m.Mode != "live" {
			return fmt.Errorf("migration %s isn't a live migration", migrationID)
		}
		if !yes {
			fmt.Printf("Cut over live migration %s of project %s.\n", migrationID, projectName)
			fmt.Print("Have you stopped every application writing to the source? [y/N] ")
			answer, _ := bufio.NewReader(os.Stdin).ReadString('\n')
			if a := strings.ToLower(strings.TrimSpace(answer)); a != "y" && a != "yes" {
				return fmt.Errorf("cutover not started")
			}
		}
		validation, err := startCutover(migrationID, ignoreWrites)
		if err != nil {
			return err
		}
		if err := followCutover(migrationID); err != nil {
			return err
		}
		if validation == validate.ModeOff {
			fmt.Println("Validation skipped (validation: off).")
			return nil
		}
		return awaitAutoValidation(migrationID)
	},
}

func init() {
	cutoverCmd.Flags().StringP("project", "p", "", "Project name (overrides current context)")
	cutoverCmd.Flags().BoolP("yes", "y", false, "Don't ask to confirm the source's writes have stopped")
	cutoverCmd.Flags().Bool("ignore-writes", false, "Cut over even if changes keep arriving after the cutover point; they are drained once more, and anything written later is left behind")
	RootCmd.AddCommand(cutoverCmd)
}

// startCutover asks the server to cut a migration over and returns the
// validation mode that will follow.
func startCutover(migrationID string, ignoreWrites bool) (string, error) {
	body, _ := json.Marshal(map[string]bool{"ignore_writes": ignoreWrites})
	resp, err := http.Post(ServerAddr+"/api/v1/migrations/"+migrationID+"/cutover", "application/json", bytes.NewReader(body))
	if err != nil {
		return "", fmt.Errorf("start cutover: %w", err)
	}
	defer resp.Body.Close()
	var out struct {
		Validation string `json:"validation"`
		Error      string `json:"error"`
	}
	json.NewDecoder(resp.Body).Decode(&out)
	if resp.StatusCode != http.StatusAccepted {
		return "", fmt.Errorf("cutover refused: %s", out.Error)
	}
	return out.Validation, nil
}

// followCutover prints the cutover's steps as they finish, until the
// migration completes or the cutover stops short.
func followCutover(migrationID string) error {
	printed := map[string]bool{}
	for {
		time.Sleep(time.Second)
		m, err := getMigration(migrationID)
		if err != nil {
			fmt.Printf("  [warn] %v\n", err)
			continue
		}
		var c *cutoverView
		if m.Live != nil {
			c = m.Live.Cutover
		}
		if c != nil {
			for _, s := range c.Steps {
				if (s.State == "done" || s.State == "skipped" || s.State == "failed") && !printed[s.Name] {
					printed[s.Name] = true
					mark := map[string]string{"done": "✓", "skipped": "–", "failed": "✗"}[s.State]
					line := fmt.Sprintf("  %s %s", mark, s.Label)
					if s.Detail != "" {
						line += ": " + s.Detail
					}
					fmt.Println(line)
				}
			}
		}
		switch {
		case m.Status == "completed":
			fmt.Println("Cutover complete: the migration is completed and the replication slot dropped.")
			return nil
		case m.Status == "failed" || m.Status == "cancelled":
			return fmt.Errorf("live migration %s: %s", m.Status, m.Error)
		case c != nil && !c.Running && c.Error != "":
			return fmt.Errorf("cutover stopped: %s", c.Error)
		}
	}
}

// waitStreaming follows a live migration until it is streaming.
func waitStreaming(id string) error {
	seen := map[string]bool{}
	for {
		m, err := getMigration(id)
		if err == nil {
			printLiveNotices(m, seen)
			switch {
			case m.Status == "failed" || m.Status == "cancelled":
				return fmt.Errorf("live migration %s: %s", m.Status, m.Error)
			case m.Phase == "streaming":
				fmt.Println("\nStreaming: the target follows the source. Cut over with 'xferdb cutover'.")
				return nil
			}
			fmt.Printf("\r\033[2K%s", liveLine(m))
		}
		time.Sleep(time.Second)
	}
}
