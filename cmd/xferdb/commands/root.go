package commands

import (
	"fmt"

	"github.com/spf13/cobra"
)

var RootCmd = &cobra.Command{
	Use:   "xferdb",
	Short: "Universal database transfer tool",
	Long: `XferDB migrates data between any supported databases with a single command.

Supported sources: PostgreSQL, MySQL, SQLite, MongoDB (soon)
Supported targets: PostgreSQL, MySQL, SQLite, MongoDB (soon)`,
	Version: "0.1.0",
}

func Execute() error {
	return RootCmd.Execute()
}

func init() {
	RootCmd.AddCommand(migrateCmd)
	RootCmd.AddCommand(versionCmd)
	RootCmd.AddCommand(listCmd)
}

var versionCmd = &cobra.Command{
	Use:   "version",
	Short: "Print XferDB version",
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Println("XferDB v0.1.0")
	},
}

var listCmd = &cobra.Command{
	Use:   "list",
	Short: "List supported source and target databases",
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Println("Supported Sources & Targets:")
		fmt.Println("  ✅ PostgreSQL")
		fmt.Println("  ✅ MySQL")
		fmt.Println("  ✅ SQLite")
		fmt.Println("  🔜 MongoDB (coming soon)")
		fmt.Println("  🔜 Cassandra (coming soon)")
		fmt.Println("  🔜 Pinecone (coming soon)")
		fmt.Println("  🔜 Qdrant (coming soon)")
		fmt.Println("  🔜 Weaviate (coming soon)")
		fmt.Println("  🔜 Chroma (coming soon)")
	},
}
