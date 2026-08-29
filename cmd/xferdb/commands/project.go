package commands

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/spf13/cobra"
	"gitea.homelab.local/nextdevops/XferDB/adapters"
)

var projectCmd = &cobra.Command{
	Use:   "project",
	Short: "Manage migration projects",
}

func init() {
	projectCmd.AddCommand(projectCreateCmd)
	projectCmd.AddCommand(projectListCmd)
	projectCmd.AddCommand(projectUseCmd)
	projectCmd.AddCommand(projectShowCmd)
	projectCmd.AddCommand(projectDeleteCmd)
	projectCmd.AddCommand(projectPreflightCmd)
}

var projectCreateCmd = &cobra.Command{
	Use:   "create",
	Short: "Create a new migration project",
	RunE: func(cmd *cobra.Command, args []string) error {
		name, _ := cmd.Flags().GetString("name")
		srcDSN, _ := cmd.Flags().GetString("source")
		tgtDSN, _ := cmd.Flags().GetString("target")
		desc, _ := cmd.Flags().GetString("description")
		batchSize, _ := cmd.Flags().GetInt("batch-size")

		if name == "" || srcDSN == "" || tgtDSN == "" {
			return fmt.Errorf("--name, --source, and --target are required")
		}

		srcType := schemeOf(srcDSN)
		tgtType := schemeOf(tgtDSN)

		body := map[string]any{
			"name":        name,
			"description": desc,
			"source_config": adapters.ConnectionConfig{
				Type: srcType,
				DSN:  srcDSN,
			},
			"target_config": adapters.ConnectionConfig{
				Type: tgtType,
				DSN:  tgtDSN,
			},
			"transfer_config": adapters.TransferConfig{
				BatchSize: batchSize,
				OnError:   adapters.ErrorPolicyAbort,
			},
		}

		data, _ := json.Marshal(body)
		resp, err := http.Post(ServerAddr+"/api/v1/projects", "application/json", bytes.NewReader(data))
		if err != nil {
			return fmt.Errorf("API request failed: %w", err)
		}
		defer resp.Body.Close()

		var result map[string]any
		json.NewDecoder(resp.Body).Decode(&result)

		if resp.StatusCode != http.StatusCreated {
			return fmt.Errorf("server error %d: %v", resp.StatusCode, result["error"])
		}
		fmt.Printf("Created project %q (id: %v)\n", name, result["id"])
		return nil
	},
}

func init() {
	projectCreateCmd.Flags().StringP("name", "n", "", "Project name")
	projectCreateCmd.Flags().String("source", "", "Source database DSN")
	projectCreateCmd.Flags().String("target", "", "Target database DSN")
	projectCreateCmd.Flags().StringP("description", "d", "", "Project description")
	projectCreateCmd.Flags().Int("batch-size", 1000, "Rows per batch (higher = faster, more memory)")
}

var projectListCmd = &cobra.Command{
	Use:   "list",
	Short: "List all migration projects",
	RunE: func(cmd *cobra.Command, args []string) error {
		resp, err := http.Get(ServerAddr + "/api/v1/projects")
		if err != nil {
			return fmt.Errorf("API request failed: %w", err)
		}
		defer resp.Body.Close()

		var projects []struct {
			ID          string    `json:"id"`
			Name        string    `json:"name"`
			Description string    `json:"description"`
			CreatedAt   time.Time `json:"created_at"`
		}
		json.NewDecoder(resp.Body).Decode(&projects)

		if len(projects) == 0 {
			fmt.Println("No projects found.")
			return nil
		}
		fmt.Printf("%-36s  %-20s  %s\n", "ID", "Name", "Created")
		fmt.Println(strings.Repeat("-", 72))
		for _, p := range projects {
			fmt.Printf("%-36s  %-20s  %s\n", p.ID, p.Name, p.CreatedAt.Format("2006-01-02 15:04"))
		}
		return nil
	},
}

var projectUseCmd = &cobra.Command{
	Use:   "use <name>",
	Short: "Set the current project context",
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx := xferdbContextPath()
		if err := os.MkdirAll(xferdbDir(), 0o755); err != nil {
			return err
		}
		if err := os.WriteFile(ctx, []byte(args[0]), 0o644); err != nil {
			return err
		}
		fmt.Printf("Now using project %q\n", args[0])
		return nil
	},
}

var projectShowCmd = &cobra.Command{
	Use:   "show",
	Short: "Show the current project",
	RunE: func(cmd *cobra.Command, args []string) error {
		name, err := currentProject("")
		if err != nil {
			return err
		}

		resp, err := http.Get(ServerAddr + "/api/v1/projects")
		if err != nil {
			return fmt.Errorf("API request failed: %w", err)
		}
		defer resp.Body.Close()

		var projects []map[string]any
		json.NewDecoder(resp.Body).Decode(&projects)

		for _, p := range projects {
			if p["name"] == name {
				data, _ := json.MarshalIndent(p, "", "  ")
				fmt.Println(string(data))
				return nil
			}
		}
		return fmt.Errorf("project %q not found", name)
	},
}

var projectPreflightCmd = &cobra.Command{
	Use:   "preflight [name]",
	Short: "Check connectivity and permissions for a project",
	Args:  cobra.MaximumNArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		var name string
		if len(args) > 0 {
			name = args[0]
		} else {
			var err error
			name, err = currentProject("")
			if err != nil {
				return err
			}
		}

		projectID, err := resolveProjectID(name)
		if err != nil {
			return err
		}
		return runPreflight(name, projectID)
	},
}

var projectDeleteCmd = &cobra.Command{
	Use:   "delete <name>",
	Short: "Delete a migration project",
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		name := args[0]

		// Resolve name → ID.
		resp, err := http.Get(ServerAddr + "/api/v1/projects")
		if err != nil {
			return fmt.Errorf("API request failed: %w", err)
		}
		defer resp.Body.Close()

		var projects []map[string]any
		json.NewDecoder(resp.Body).Decode(&projects)

		var projectID string
		for _, p := range projects {
			if p["name"] == name {
				projectID, _ = p["id"].(string)
				break
			}
		}
		if projectID == "" {
			return fmt.Errorf("project %q not found", name)
		}

		req, err := http.NewRequest(http.MethodDelete, ServerAddr+"/api/v1/projects/"+projectID, nil)
		if err != nil {
			return fmt.Errorf("build request: %w", err)
		}
		delResp, err := http.DefaultClient.Do(req)
		if err != nil {
			return fmt.Errorf("API request failed: %w", err)
		}
		defer delResp.Body.Close()

		if delResp.StatusCode != http.StatusNoContent && delResp.StatusCode != http.StatusOK {
			var errBody map[string]any
			json.NewDecoder(delResp.Body).Decode(&errBody)
			return fmt.Errorf("server error %d: %v", delResp.StatusCode, errBody["error"])
		}

		fmt.Printf("Deleted project %q\n", name)

		// Clear the active context if it pointed at the deleted project.
		if current, err := currentProject(""); err == nil && current == name {
			_ = os.WriteFile(xferdbContextPath(), []byte(""), 0o644)
			fmt.Println("Cleared active project context.")
		}
		return nil
	},
}

// schemeOf extracts the scheme (adapter type) from a DSN.
func schemeOf(dsn string) string {
	if idx := strings.Index(dsn, "://"); idx >= 0 {
		return dsn[:idx]
	}
	return dsn
}

// xferdbDir returns the ~/.xferdb directory path.
func xferdbDir() string {
	home, _ := os.UserHomeDir()
	return home + "/.xferdb"
}

// currentProject returns the current project name from the --project flag or context file.
func currentProject(flagValue string) (string, error) {
	if flagValue != "" {
		return flagValue, nil
	}
	data, err := os.ReadFile(xferdbContextPath())
	if err != nil {
		return "", fmt.Errorf("no project selected — run 'xferdb project use <name>' or pass --project")
	}
	name := strings.TrimSpace(string(data))
	if name == "" {
		return "", fmt.Errorf("current project context is empty — run 'xferdb project use <name>'")
	}
	return name, nil
}
