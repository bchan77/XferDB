package main

import (
	"fmt"
	"os"

	"gitea.homelab.local/nextdevops/XferDB/cmd/xferdb/commands"
	"gitea.homelab.local/nextdevops/XferDB/pkg/logger"
)

func main() {
	log := logger.New("xferdb")

	if err := commands.RootCmd.Execute(); err != nil {
		log.Error("failed to execute command: %v", err)
		os.Exit(1)
	}

	fmt.Println("XferDB v0.1.0 — Universal Database Transfer Tool")
}
