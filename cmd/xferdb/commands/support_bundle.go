package commands

import (
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"time"

	"github.com/spf13/cobra"
)

var supportBundleCmd = &cobra.Command{
	Use:   "support-bundle [project-name]",
	Short: "Generate a diagnostic support bundle for a project",
	Long: `Generate a tar.gz file containing diagnostic information for troubleshooting.

The bundle includes:
  - System info (XferDB version, OS, Go version)
  - Project configuration (credentials redacted)
  - Source and target database info
  - Preflight/permissions check results
  - Migration history and errors
  - Table schemas
  - Schema plan (for MongoDB projects)

Sensitive information like passwords and API keys are automatically redacted.`,
	Args: cobra.MaximumNArgs(1),
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

		outputPath, _ := cmd.Flags().GetString("output")
		return runSupportBundle(name, projectID, outputPath)
	},
}

func init() {
	supportBundleCmd.Flags().StringP("output", "o", "", "Output file path (default: xferdb-support-<project>-<timestamp>.tar.gz)")
}

func runSupportBundle(name, projectID, outputPath string) error {
	fmt.Printf("Generating support bundle for project %q...\n", name)

	resp, err := http.Get(ServerAddr + "/api/v1/projects/" + projectID + "/support-bundle")
	if err != nil {
		return fmt.Errorf("API request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("server error %d: %s", resp.StatusCode, string(body))
	}

	// Determine output filename.
	if outputPath == "" {
		// Try to extract filename from Content-Disposition header.
		if cd := resp.Header.Get("Content-Disposition"); cd != "" {
			re := regexp.MustCompile(`filename="?([^"]+)"?`)
			if matches := re.FindStringSubmatch(cd); len(matches) > 1 {
				outputPath = matches[1]
			}
		}
		// Fallback to default name.
		if outputPath == "" {
			outputPath = fmt.Sprintf("xferdb-support-%s-%s.tar.gz",
				sanitizeForFilename(name),
				time.Now().Format("20060102-150405"))
		}
	}

	// Create output file.
	f, err := os.Create(outputPath)
	if err != nil {
		return fmt.Errorf("create output file: %w", err)
	}
	defer f.Close()

	// Copy response body to file.
	n, err := io.Copy(f, resp.Body)
	if err != nil {
		return fmt.Errorf("write output file: %w", err)
	}

	absPath, _ := filepath.Abs(outputPath)
	fmt.Printf("Support bundle saved to: %s (%s)\n", absPath, humanSize(n))
	fmt.Println("\nBundle contents:")
	fmt.Println("  - system.json         System and version info")
	fmt.Println("  - project.json        Project config (credentials redacted)")
	fmt.Println("  - source_info.json    Source database metadata")
	fmt.Println("  - target_info.json    Target database metadata")
	fmt.Println("  - preflight.json      Permissions check results")
	fmt.Println("  - migrations.json     Migration history")
	fmt.Println("  - checkpoints.json    Resume checkpoints")
	fmt.Println("  - source_schema.json  Source table schemas")
	fmt.Println("  - target_schema.json  Target table schemas")
	fmt.Println("  - schema_plan.json    MongoDB schema plan (if applicable)")

	return nil
}

// sanitizeForFilename removes unsafe characters from a filename.
func sanitizeForFilename(s string) string {
	var result []byte
	for i := 0; i < len(s); i++ {
		c := s[i]
		if (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '-' || c == '_' {
			result = append(result, c)
		} else {
			result = append(result, '_')
		}
	}
	return string(result)
}

// humanSize formats bytes as a human-readable string.
func humanSize(bytes int64) string {
	const (
		KB = 1024
		MB = KB * 1024
		GB = MB * 1024
	)
	switch {
	case bytes >= GB:
		return fmt.Sprintf("%.1f GB", float64(bytes)/GB)
	case bytes >= MB:
		return fmt.Sprintf("%.1f MB", float64(bytes)/MB)
	case bytes >= KB:
		return fmt.Sprintf("%.1f KB", float64(bytes)/KB)
	default:
		return fmt.Sprintf("%d bytes", bytes)
	}
}
