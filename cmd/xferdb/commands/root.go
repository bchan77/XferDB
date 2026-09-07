package commands

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"

	"github.com/spf13/cobra"
	"gitea.homelab.local/nextdevops/XferDB/api"
	"gitea.homelab.local/nextdevops/XferDB/state"
	"gitea.homelab.local/nextdevops/XferDB/version"
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
	Version:      version.Version,
	SilenceUsage: true,
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
	RootCmd.AddCommand(supportBundleCmd)
}

var versionCmd = &cobra.Command{
	Use:   "version",
	Short: "Print XferDB version",
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Printf("XferDB v%s\n", version.Version)
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
		logFile, _ := cmd.Flags().GetString("log-file")
		logLevel, _ := cmd.Flags().GetString("log-level")

		log := newLogger(logLevel, logFile)

		dbPath := xferdbStatePath()
		if err := os.MkdirAll(filepath.Dir(dbPath), 0o755); err != nil {
			return err
		}
		db, err := state.Open(dbPath)
		if err != nil {
			return err
		}
		defer db.Close()

		log.Info("server.init", "state", dbPath, "log_file", logFile, "log_level", logLevel)
		return api.NewServer(db, log).ListenAndServe(addr)
	},
}

func init() {
	serverCmd.Flags().String("addr", ":8080", "Address to listen on")
	serverCmd.Flags().String("log-file", "", "Path to JSON log file (in addition to stderr text output)")
	serverCmd.Flags().String("log-level", "info", "Log level: debug, info, warn, error")
}

// newLogger builds a slog.Logger that always writes text to stderr.
// When logFile is set, it additionally writes JSON to that file.
func newLogger(level, logFile string) *slog.Logger {
	var lvl slog.Level
	switch strings.ToLower(level) {
	case "debug":
		lvl = slog.LevelDebug
	case "warn":
		lvl = slog.LevelWarn
	case "error":
		lvl = slog.LevelError
	default:
		lvl = slog.LevelInfo
	}

	opts := &slog.HandlerOptions{Level: lvl}
	textHandler := slog.NewTextHandler(os.Stderr, opts)

	if logFile == "" {
		return slog.New(textHandler)
	}

	f, err := os.OpenFile(logFile, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		slog.New(textHandler).Warn("could not open log file, logging to stderr only",
			"path", logFile, "error", err)
		return slog.New(textHandler)
	}

	jsonHandler := slog.NewJSONHandler(f, opts)
	return slog.New(&multiHandler{handlers: []slog.Handler{textHandler, jsonHandler}})
}

// multiHandler fans out log records to multiple slog.Handler implementations.
type multiHandler struct {
	handlers []slog.Handler
}

func (m *multiHandler) Enabled(ctx context.Context, level slog.Level) bool {
	for _, h := range m.handlers {
		if h.Enabled(ctx, level) {
			return true
		}
	}
	return false
}

func (m *multiHandler) Handle(ctx context.Context, r slog.Record) error {
	for _, h := range m.handlers {
		if h.Enabled(ctx, r.Level) {
			_ = h.Handle(ctx, r.Clone())
		}
	}
	return nil
}

func (m *multiHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	handlers := make([]slog.Handler, len(m.handlers))
	for i, h := range m.handlers {
		handlers[i] = h.WithAttrs(attrs)
	}
	return &multiHandler{handlers: handlers}
}

func (m *multiHandler) WithGroup(name string) slog.Handler {
	handlers := make([]slog.Handler, len(m.handlers))
	for i, h := range m.handlers {
		handlers[i] = h.WithGroup(name)
	}
	return &multiHandler{handlers: handlers}
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

