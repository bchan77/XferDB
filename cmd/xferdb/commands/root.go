package commands

import (
	"fmt"
	"log"
	"os"
	"path/filepath"

	"github.com/spf13/cobra"
	"gitea.homelab.local/nextdevops/XferDB/api"
	"gitea.homelab.local/nextdevops/XferDB/state"
)

// ServerAddr is the base URL of the XferDB API server, set via --server flag.
var ServerAddr string

var RootCmd = &cobra.Command{
	Use:   "xferdb",
	Short: "Universal database transfer tool",
	Long: `XferDB migrates data between any supported databases.

Start the API server first:
  xferdb server

Then use projects and migrations:
  xferdb project create --name myproject --source sqlite:///src.db --target sqlite:///dst.db
  xferdb project use myproject
  xferdb migrate`,
	Version: "0.2.0",
}

func Execute() error {
	return RootCmd.Execute()
}

func init() {
	RootCmd.PersistentFlags().StringVar(&ServerAddr, "server", "http://localhost:8080",
		"XferDB API server address")

	RootCmd.AddCommand(serverCmd)
	RootCmd.AddCommand(projectCmd)
	RootCmd.AddCommand(migrateCmd)
	RootCmd.AddCommand(versionCmd)
	RootCmd.AddCommand(listCmd)
}

var versionCmd = &cobra.Command{
	Use:   "version",
	Short: "Print XferDB version",
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Println("XferDB v0.2.0")
	},
}

var listCmd = &cobra.Command{
	Use:   "list",
	Short: "List supported database adapters",
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Println("Supported adapters:")
		fmt.Println("  postgres")
		fmt.Println("  mysql")
		fmt.Println("  sqlite")
	},
}

var serverCmd = &cobra.Command{
	Use:   "server",
	Short: "Start the XferDB API server",
	RunE: func(cmd *cobra.Command, args []string) error {
		addr, _ := cmd.Flags().GetString("addr")
		dbPath := xferdbStatePath()
		if err := os.MkdirAll(filepath.Dir(dbPath), 0o755); err != nil {
			return err
		}
		db, err := state.Open(dbPath)
		if err != nil {
			return err
		}
		defer db.Close()
		log.Printf("XferDB API server listening on %s (state: %s)", addr, dbPath)
		return api.NewServer(db).ListenAndServe(addr)
	},
}

func init() {
	serverCmd.Flags().String("addr", ":8080", "Address to listen on")
}

// xferdbStatePath returns the default state DB path (~/.xferdb/state.db).
func xferdbStatePath() string {
	home, _ := os.UserHomeDir()
	return filepath.Join(home, ".xferdb", "state.db")
}

// xferdbContextPath returns the path to the current project context file.
func xferdbContextPath() string {
	home, _ := os.UserHomeDir()
	return filepath.Join(home, ".xferdb", "current_project")
}
