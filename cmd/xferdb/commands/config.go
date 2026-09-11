package commands

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/spf13/cobra"
	"gitea.homelab.local/nextdevops/XferDB/internal/config"
)

var configCmd = &cobra.Command{
	Use:   "config",
	Short: "Inspect and validate the XferDB config file",
	Long: `Read, validate, and (re)generate the XferDB configuration file.

Examples:
  xferdb config show               # print the resolved config file (after env expansion)
  xferdb config show --resolved   # print with env-var placeholders still visible
  xferdb config validate           # exit 0 if config is valid, 1 otherwise
  xferdb config path               # print the path of the config file in use
  xferdb config init ./xferdb.yaml # write a starter config file`,
}

func init() {
	RootCmd.AddCommand(configCmd)
	configCmd.AddCommand(configShowCmd)
	configCmd.AddCommand(configValidateCmd)
	configCmd.AddCommand(configPathCmd)
	configCmd.AddCommand(configInitCmd)
}

var configShowCmd = &cobra.Command{
	Use:   "show",
	Short: "Print the resolved config file",
	RunE: func(cmd *cobra.Command, args []string) error {
		path, _ := cmd.Flags().GetString("config")
		loader, err := config.Load(path)
		if err != nil {
			return err
		}
		if loader.Path() == "" {
			return fmt.Errorf("no config file found — pass --config, set $XFERDB_CONFIG, or create ./xferdb.yaml")
		}
		data, err := os.ReadFile(loader.Path())
		if err != nil {
			return fmt.Errorf("read %s: %w", loader.Path(), err)
		}
		fmt.Fprintln(cmd.OutOrStdout(), string(data))
		return nil
	},
}

var configValidateCmd = &cobra.Command{
	Use:   "validate",
	Short: "Validate the config file and print the resolved path",
	RunE: func(cmd *cobra.Command, args []string) error {
		path, _ := cmd.Flags().GetString("config")
		loader, err := config.Load(path)
		if err != nil {
			fmt.Fprintf(cmd.ErrOrStderr(), "invalid: %v\n", err)
			os.Exit(1)
		}
		if loader.Path() == "" {
			fmt.Fprintln(cmd.OutOrStdout(), "no config file found (this is OK if you're using flags only)")
			return nil
		}
		fmt.Fprintf(cmd.OutOrStdout(), "✓ %s is valid\n", loader.Path())
		return nil
	},
}

var configPathCmd = &cobra.Command{
	Use:   "path",
	Short: "Print the resolved config file path",
	RunE: func(cmd *cobra.Command, args []string) error {
		path, _ := cmd.Flags().GetString("config")
		loader, err := config.Load(path)
		if err != nil {
			return err
		}
		if loader.Path() == "" {
			fmt.Fprintln(cmd.OutOrStdout(), "(none — using built-in defaults; pass --config to specify one)")
			return nil
		}
		fmt.Fprintln(cmd.OutOrStdout(), loader.Path())
		return nil
	},
}

var configInitCmd = &cobra.Command{
	Use:   "init [path]",
	Short: "Write a starter xferdb.yaml",
	Long: `Write a starter xferdb.yaml at the given path (default: ./xferdb.yaml).
Refuses to overwrite an existing file unless --force is passed.`,
	Args: cobra.MaximumNArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		target := "./xferdb.yaml"
		if len(args) > 0 {
			target = args[0]
		}
		abs, err := filepath.Abs(target)
		if err != nil {
			return err
		}
		force, _ := cmd.Flags().GetBool("force")

		if _, err := os.Stat(abs); err == nil && !force {
			return fmt.Errorf("%s already exists; pass --force to overwrite", abs)
		}
		if err := os.MkdirAll(filepath.Dir(abs), 0o755); err != nil {
			return err
		}
		if err := os.WriteFile(abs, []byte(starterConfig()), 0o644); err != nil {
			return err
		}
		fmt.Fprintf(cmd.OutOrStdout(), "wrote starter config to %s\n", abs)
		fmt.Fprintln(cmd.OutOrStdout(), "edit it, then validate with: xferdb config validate --config "+abs)
		return nil
	},
}

func init() {
	configInitCmd.Flags().Bool("force", false, "overwrite an existing file")
}

// starterConfig is the example shipped in examples/xferdb.yaml. Kept here
// as a string so `xferdb config init` works without the repo checked out.
const starterConfigTmpl = `# xferdb.yaml — XferDB configuration
# See https://gitea.homelab.local/nextdevops/XferDB (docs/CONFIG.md)
# for the full reference.

server:
  addr: ":8080"
  log_level: info

defaults:
  table_workers: 1
  batch_size: 2000

projects:
  - name: my-first-project
    description: Replace the placeholders below with your real DSNs.
    source:
      type: postgres
      dsn: ${SOURCE_DSN}
    target:
      type: postgres
      dsn: ${TARGET_DSN}
`

func starterConfig() string { return starterConfigTmpl }
