package commands

import (
	"bytes"
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
		preflightOnly, _ := cmd.Flags().GetBool("preflight")
		statusOnly, _ := cmd.Flags().GetBool("status")
		cancelOnly, _ := cmd.Flags().GetBool("cancel")
		recreateSchema, _ := cmd.Flags().GetBool("recreate-schema")
		truncate, _ := cmd.Flags().GetBool("truncate")
		tableWorkers, _ := cmd.Flags().GetInt("table-workers")
		segmentWorkers, _ := cmd.Flags().GetInt("segment-workers")
		offsetSegments, _ := cmd.Flags().GetBool("offset-segments")
		batchSize, _ := cmd.Flags().GetInt("batch-size")
		bulkCopy, _ := cmd.Flags().GetBool("bulk-copy")
		tablesFlag, _ := cmd.Flags().GetString("tables")

		if recreateSchema && truncate {
			return fmt.Errorf("--recreate-schema and --truncate are mutually exclusive")
		}

		// Parse comma-separated table list; strip whitespace.
		var tables []string
		if tablesFlag != "" {
			for _, t := range strings.Split(tablesFlag, ",") {
				if t = strings.TrimSpace(t); t != "" {
					tables = append(tables, t)
				}
			}
		}

		projectName, err := currentProject(projectFlag)
		if err != nil {
			return err
		}

		projectID, err := resolveProjectID(projectName)
		if err != nil {
			return err
		}

		if preflightOnly {
			return runPreflight(projectName, projectID)
		}

		if statusOnly {
			migrationID, err := latestMigrationID(projectID)
			if err != nil {
				return err
			}
			fmt.Printf("Attaching to migration %s...\n", migrationID)
			return pollStats(migrationID)
		}

		if cancelOnly {
			migrationID, err := latestMigrationID(projectID)
			if err != nil {
				return err
			}
			return cancelMigration(migrationID)
		}

		migrationID, err := startMigration(projectID, recreateSchema, truncate, tableWorkers, segmentWorkers, batchSize, offsetSegments, bulkCopy, tables)
		if err != nil {
			return err
		}
		fmt.Printf("Migration started: %s\n", migrationID)
		fmt.Println("Tracking progress (Ctrl+C to detach)...")

		return pollStats(migrationID)
	},
}

func init() {
	migrateCmd.Flags().StringP("project", "p", "", "Project name (overrides current context)")
	migrateCmd.Flags().Bool("preflight", false, "Check connectivity and permissions without migrating")
	migrateCmd.Flags().Bool("status", false, "Re-attach to the latest migration and show live progress")
	migrateCmd.Flags().Bool("cancel", false, "Cancel the currently running migration")
	migrateCmd.Flags().Bool("recreate-schema", false, "Drop and recreate target tables before migrating")
	migrateCmd.Flags().Bool("truncate", false, "Truncate target tables before loading data (keeps schema)")
	migrateCmd.Flags().Int("table-workers", 1, "Number of tables to migrate concurrently")
	migrateCmd.Flags().Int("segment-workers", 1, "Number of parallel workers per table (splits by PK range; falls back to OFFSET when no integer PK)")
	migrateCmd.Flags().Bool("offset-segments", false, "Force OFFSET-based segment splitting even when a PK is available (use with --segment-workers)")
	migrateCmd.Flags().Int("batch-size", 0, "Rows per batch (overrides the project default; 0 = use project default)")
	migrateCmd.Flags().Bool("bulk-copy", false, "Use PostgreSQL COPY protocol for writes (faster; requires target table to be empty — combine with --truncate or --recreate-schema)")
	migrateCmd.Flags().String("tables", "", "Comma-separated list of tables to migrate (e.g. orders,public.customers)")
}

// runPreflight calls the preflight API and prints a human-readable result.
func runPreflight(name, projectID string) error {
	resp, err := http.Post(ServerAddr+"/api/v1/projects/"+projectID+"/preflight", "application/json", nil)
	if err != nil {
		return fmt.Errorf("API request failed: %w", err)
	}
	defer resp.Body.Close()

	var result struct {
		Status string `json:"status"`
		Source *struct {
			CanRead        bool     `json:"can_read"`
			CanWrite       bool     `json:"can_write"`
			CanCreateTable bool     `json:"can_create_table"`
			Errors         []string `json:"errors"`
		} `json:"source"`
		Target *struct {
			CanRead        bool     `json:"can_read"`
			CanWrite       bool     `json:"can_write"`
			CanCreateTable bool     `json:"can_create_table"`
			Errors         []string `json:"errors"`
		} `json:"target"`
		Error string `json:"error"`
	}
	json.NewDecoder(resp.Body).Decode(&result)

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("preflight failed: %s", result.Error)
	}

	fmt.Printf("Project:  %s\n", name)
	fmt.Printf("Status:   %s\n", result.Status)
	if result.Source != nil {
		fmt.Printf("\nSource:\n")
		fmt.Printf("  can_read:         %v\n", result.Source.CanRead)
		fmt.Printf("  can_write:        %v\n", result.Source.CanWrite)
		fmt.Printf("  can_create_table: %v\n", result.Source.CanCreateTable)
		for _, e := range result.Source.Errors {
			fmt.Printf("  ERROR: %s\n", e)
		}
	}
	if result.Target != nil {
		fmt.Printf("\nTarget:\n")
		fmt.Printf("  can_read:         %v\n", result.Target.CanRead)
		fmt.Printf("  can_write:        %v\n", result.Target.CanWrite)
		fmt.Printf("  can_create_table: %v\n", result.Target.CanCreateTable)
		for _, e := range result.Target.Errors {
			fmt.Printf("  ERROR: %s\n", e)
		}
	}

	if result.Status != "ready" {
		return fmt.Errorf("preflight check failed — fix the errors above before migrating")
	}
	return nil
}

// resolveProjectID fetches the project list and returns the ID for the given name.
func resolveProjectID(name string) (string, error) {
	resp, err := http.Get(ServerAddr + "/api/v1/projects")
	if err != nil {
		return "", fmt.Errorf("list projects: %w", err)
	}
	defer resp.Body.Close()

	var projects []struct {
		ID   string `json:"id"`
		Name string `json:"name"`
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

// cancelMigration sends a cancel action to the migration and reports the result.
func cancelMigration(migrationID string) error {
	body, _ := json.Marshal(map[string]string{"action": "cancel"})
	req, err := http.NewRequest(http.MethodPatch,
		ServerAddr+"/api/v1/migrations/"+migrationID,
		bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("build request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("API request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return fmt.Errorf("migration %s is not running (already finished or never started)", migrationID)
	}
	if resp.StatusCode != http.StatusAccepted {
		var e map[string]any
		json.NewDecoder(resp.Body).Decode(&e)
		return fmt.Errorf("server error %d: %v", resp.StatusCode, e["error"])
	}
	fmt.Printf("Migration %s cancelled.\n", migrationID)
	return nil
}

// latestMigrationID returns the most recently created migration ID for a project.
func latestMigrationID(projectID string) (string, error) {
	resp, err := http.Get(ServerAddr + "/api/v1/projects/" + projectID + "/migrations")
	if err != nil {
		return "", fmt.Errorf("list migrations: %w", err)
	}
	defer resp.Body.Close()

	var migrations []struct {
		ID        string `json:"id"`
		Status    string `json:"status"`
		CreatedAt string `json:"created_at"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&migrations); err != nil {
		return "", fmt.Errorf("decode migrations: %w", err)
	}
	if len(migrations) == 0 {
		return "", fmt.Errorf("no migrations found for this project — run 'xferdb migrate' to start one")
	}
	// API returns newest first; take the first entry.
	return migrations[0].ID, nil
}

// startMigration POSTs to create a new migration and returns its ID.
func startMigration(projectID string, recreateSchema, truncate bool, tableWorkers, segmentWorkers, batchSize int, offsetSegments, bulkCopy bool, tables []string) (string, error) {
	req := map[string]any{
		"recreate_schema": recreateSchema,
		"truncate":        truncate,
		"table_workers":   tableWorkers,
		"segment_workers": segmentWorkers,
		"offset_fallback": offsetSegments,
		"bulk_copy":       bulkCopy,
	}
	if batchSize > 0 {
		req["batch_size"] = batchSize
	}
	if len(tables) > 0 {
		req["tables"] = tables
	}
	body, _ := json.Marshal(req)
	resp, err := http.Post(
		ServerAddr+"/api/v1/projects/"+projectID+"/migrations",
		"application/json",
		bytes.NewReader(body),
	)
	if err != nil {
		return "", fmt.Errorf("start migration: %w", err)
	}
	defer resp.Body.Close()

	var result struct {
		ID    string `json:"id"`
		Error string `json:"error"`
	}
	json.NewDecoder(resp.Body).Decode(&result)

	if resp.StatusCode != http.StatusCreated {
		return "", fmt.Errorf("server error %d: %s", resp.StatusCode, result.Error)
	}
	return result.ID, nil
}

// pollStats polls the stats endpoint every second and prints a full table-by-table
// progress view until the migration completes or fails.
func pollStats(migrationID string) error {
	url := fmt.Sprintf("%s/api/v1/migrations/%s/stats", ServerAddr, migrationID)
	var lastLines int
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

		// Move cursor up to overwrite previous output.
		if lastLines > 0 {
			fmt.Printf("\033[%dA", lastLines)
		}

		lines := renderProgress(snap)
		for _, l := range lines {
			fmt.Printf("\033[2K%s\n", l) // clear line then print
		}
		lastLines = len(lines)

		switch snap.Phase {
		case "complete":
			fmt.Printf("\nMigration complete. Transferred %s rows in %.1fs.\n",
				fmtInt(snap.Rows.Transferred), snap.ElapsedSeconds)
			return nil
		case "failed":
			fmt.Println()
			return fmt.Errorf("migration failed: %s", strings.Join(snap.Errors, "; "))
		}
	}
}

func renderProgress(snap stats.StatsSnapshot) []string {
	var lines []string
	lines = append(lines, fmt.Sprintf("Phase: %-14s  Elapsed: %s  ETA: %s",
		snap.Phase, fmtDuration(snap.ElapsedSeconds), fmtDuration(snap.ETASeconds)))
	lines = append(lines, fmt.Sprintf("Rows:  %s / %s   Rate: %.0f/s",
		fmtInt(snap.Rows.Transferred), fmtInt(snap.Rows.Total), snap.Rows.RatePerSecond))
	if snap.Rows.ReadRate > 0 || snap.Rows.WriteRate > 0 {
		lines = append(lines, fmt.Sprintf("Read:  %.0f/s   Write: %.0f/s",
			snap.Rows.ReadRate, snap.Rows.WriteRate))
	}
	cfg := snap.Config
	lines = append(lines, fmt.Sprintf("Batch: %s rows   Table workers: %d   Segment workers: %d",
		fmtInt(int64(cfg.BatchSize)), cfg.TableWorkers, cfg.SegmentWorkers))
	if snap.Resource != nil {
		r := snap.Resource
		lines = append(lines, fmt.Sprintf("Goroutines: %-5d  Heap: %.1f MiB  Sys: %.1f MiB  CPU: %.1f%%  GC: %d",
			r.Goroutines, r.MemAllocMB, r.MemSysMB, r.CPUPercent, r.GCNum))
		lines = append(lines, strings.Repeat("─", 60))
	} else {
		lines = append(lines, strings.Repeat("─", 60))
	}

	for _, t := range snap.TableDetails {
		var marker, detail string
		switch t.Status {
		case "done":
			marker = "✓"
			detail = fmt.Sprintf("%s rows", fmtInt(t.Transferred))
		case "in_progress":
			marker = "●"
			pct := 0
			if t.Total > 0 {
				pct = int(t.Transferred * 100 / t.Total)
			}
			detail = fmt.Sprintf("%s / %s rows  (%d%%)",
				fmtInt(t.Transferred), fmtInt(t.Total), pct)
		case "failed":
			marker = "✗"
			detail = "failed"
		case "not_started":
			marker = "·"
			detail = "not started"
		default:
			marker = "○"
			detail = "pending"
		}
		lines = append(lines, fmt.Sprintf("  %s  %-30s  %s", marker, truncate(t.Name, 30), detail))
		if t.Status == "failed" && t.Error != "" {
			lines = append(lines, fmt.Sprintf("     └─ %s", truncate(t.Error, 72)))
		}
	}

	for _, e := range snap.Errors {
		lines = append(lines, "  ERROR: "+e)
	}
	return lines
}

func fmtDuration(seconds float64) string {
	s := int(seconds)
	if s < 60 {
		return fmt.Sprintf("%ds", s)
	}
	if s < 3600 {
		return fmt.Sprintf("%dm %ds", s/60, s%60)
	}
	if s < 86400 {
		return fmt.Sprintf("%dh %dm", s/3600, (s%3600)/60)
	}
	return fmt.Sprintf("%dd %dh", s/86400, (s%86400)/3600)
}

func fmtInt(n int64) string {
	s := fmt.Sprintf("%d", n)
	// Insert commas every 3 digits from the right.
	out := ""
	for i, c := range s {
		if i > 0 && (len(s)-i)%3 == 0 {
			out += ","
		}
		out += string(c)
	}
	return out
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n-1] + "…"
}

