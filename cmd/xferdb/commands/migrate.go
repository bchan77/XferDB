package commands

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/spf13/cobra"
	"gitea.homelab.local/nextdevops/XferDB/stats"
)

var migrateCmd = &cobra.Command{
	Use:   "migrate",
	Short: "Run a migration for the current project",
	Long: `Start a migration for the active project (set via 'xferdb project use') or --project.

Examples:
  xferdb migrate
  xferdb migrate --project prod-to-staging`,
	RunE: func(cmd *cobra.Command, args []string) error {
		projectFlag, _ := cmd.Flags().GetString("project")
		projectName, err := currentProject(projectFlag)
		if err != nil {
			return err
		}

		// Resolve project name → ID via the API.
		projectID, err := resolveProjectID(projectName)
		if err != nil {
			return err
		}

		// Start the migration.
		migrationID, err := startMigration(projectID)
		if err != nil {
			return err
		}
		fmt.Printf("Migration started: %s\n", migrationID)
		fmt.Println("Tracking progress (Ctrl+C to detach)...")

		// Poll stats until complete or failed.
		return pollStats(migrationID)
	},
}

func init() {
	migrateCmd.Flags().StringP("project", "p", "", "Project name (overrides current context)")
}

// resolveProjectID fetches the project list and returns the ID for the given name.
func resolveProjectID(name string) (string, error) {
	resp, err := http.Get(ServerAddr + "/api/v1/projects")
	if err != nil {
		return "", fmt.Errorf("list projects: %w", err)
	}
	defer resp.Body.Close()

	var projects []struct {
		ID   string `json:"ID"`
		Name string `json:"Name"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&projects); err != nil {
		return "", fmt.Errorf("decode projects: %w", err)
	}
	for _, p := range projects {
		if p.Name == name {
			return p.ID, nil
		}
	}
	return "", fmt.Errorf("project %q not found", name)
}

// startMigration POSTs to create a new migration and returns its ID.
func startMigration(projectID string) (string, error) {
	resp, err := http.Post(
		ServerAddr+"/api/v1/projects/"+projectID+"/migrations",
		"application/json",
		strings.NewReader("{}"),
	)
	if err != nil {
		return "", fmt.Errorf("start migration: %w", err)
	}
	defer resp.Body.Close()

	var result struct {
		ID    string `json:"ID"`
		Error string `json:"error"`
	}
	json.NewDecoder(resp.Body).Decode(&result)

	if resp.StatusCode != http.StatusCreated {
		return "", fmt.Errorf("server error %d: %s", resp.StatusCode, result.Error)
	}
	return result.ID, nil
}

// pollStats polls the stats endpoint every second and prints progress until done.
func pollStats(migrationID string) error {
	url := fmt.Sprintf("%s/api/v1/migrations/%s/stats", ServerAddr, migrationID)
	for {
		time.Sleep(time.Second)

		resp, err := http.Get(url)
		if err != nil {
			fmt.Printf("  [warn] stats fetch failed: %v\n", err)
			continue
		}

		var snap stats.StatsSnapshot
		json.NewDecoder(resp.Body).Decode(&snap)
		resp.Body.Close()

		fmt.Printf("\r  phase=%-12s  table=%-20s  rows=%d/%d  rate=%.0f/s  eta=%.0fs",
			snap.Phase,
			truncate(snap.CurrentTable, 20),
			snap.Rows.Transferred,
			snap.Rows.Total,
			snap.Rows.RatePerSecond,
			snap.ETASeconds,
		)

		switch snap.Phase {
		case "complete":
			fmt.Printf("\nMigration complete. Transferred %d rows in %.1fs.\n",
				snap.Rows.Transferred, snap.ElapsedSeconds)
			return nil
		case "failed":
			fmt.Println()
			return fmt.Errorf("migration failed: %s", strings.Join(snap.Errors, "; "))
		}
	}
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n-1] + "…"
}
