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
}

var projectCreateCmd = &cobra.Command{
	Use:   "create",
	Short: "Create a new migration project",
	RunE: func(cmd *cobra.Command, args []string) error {
		name, _ := cmd.Flags().GetString("name")
		srcDSN, _ := cmd.Flags().GetString("source")
		tgtDSN, _ := cmd.Flags().GetString("target")
		desc, _ := cmd.Flags().GetString("description")

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
				BatchSize: 1000,
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
		fmt.Printf("Created project %q (id: %v)\n", name, result["ID"])
		return nil
	},
}

func init() {
	projectCreateCmd.Flags().StringP("name", "n", "", "Project name")
	projectCreateCmd.Flags().String("source", "", "Source database DSN")
	projectCreateCmd.Flags().String("target", "", "Target database DSN")
	projectCreateCmd.Flags().StringP("description", "d", "", "Project description")
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
			ID          string    `json:"ID"`
			Name        string    `json:"Name"`
			Description string    `json:"Description"`
			CreatedAt   time.Time `json:"CreatedAt"`
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
			if p["Name"] == name {
				data, _ := json.MarshalIndent(p, "", "  ")
				fmt.Println(string(data))
				return nil
			}
		}
		return fmt.Errorf("project %q not found", name)
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
