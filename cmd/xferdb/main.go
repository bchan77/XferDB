package main

import (
	"log"
	"os"

	"gitea.homelab.local/nextdevops/XferDB/cmd/xferdb/commands"
)

func main() {
	if err := commands.RootCmd.Execute(); err != nil {
		log.Printf("ERROR: %v", err)
		os.Exit(1)
	}
}
