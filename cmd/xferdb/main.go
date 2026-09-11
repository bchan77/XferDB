package main

import (
	"fmt"
	"os"

	"gitea.homelab.local/nextdevops/XferDB/cmd/xferdb/commands"
)

func main() {
	if err := commands.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}
