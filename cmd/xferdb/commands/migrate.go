package commands

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

var migrateCmd = &cobra.Command{
	Use:   "migrate",
	Short: "Migrate data from source to target database",
	Long: `Migrate data between any supported databases.

Examples:
  xferdb migrate --from postgres://localhost:5432/source --to mysql://localhost:3306/target
  xferdb migrate --config migration.yaml`,
	Run: func(cmd *cobra.Command, args []string) {
		fromConn := cmd.Flags().Lookup("from").Value.String()
		toConn := cmd.Flags().Lookup("to").Value.String()
		configFile := cmd.Flags().Lookup("config").Value.String()

		if fromConn == "" && toConn == "" && configFile == "" {
			fmt.Println("Error: specify --from and --to, or --config")
			os.Exit(1)
		}

		fmt.Printf("XferDB migrate called\n")
		fmt.Printf("  From: %s\n", fromConn)
		fmt.Printf("  To:   %s\n", toConn)
		fmt.Println("\nMigrator not yet implemented — coming soon!")
	},
}

func init() {
	migrateCmd.Flags().StringP("from", "f", "", "Source database connection string")
	migrateCmd.Flags().StringP("to", "t", "", "Target database connection string")
	migrateCmd.Flags().StringP("config", "c", "", "Config file (YAML)")
}
