package commands

import (
	"context"
	"errors"
	"os/signal"
	"syscall"

	"gitea.homelab.local/nextdevops/XferDB/cdc/worker"
	"github.com/spf13/cobra"
)

// The cdc commands are the worker processes of a live migration (#102),
// started by `xferdb server`, one capture and one apply per migration. Like
// `xferdb server` they run engine code rather than calling the API, and are
// hidden: nobody needs to run them by hand.
var cdcCmd = &cobra.Command{
	Use:    "cdc",
	Short:  "Live migration worker processes (started by xferdb server)",
	Hidden: true,
}

func init() {
	for _, role := range []string{worker.RoleCapture, worker.RoleApply} {
		role := role
		var dir string
		c := &cobra.Command{
			Use:   role,
			Short: "Run a live migration's " + role + " worker",
			RunE: func(cmd *cobra.Command, args []string) error {
				ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
				defer stop()
				err := worker.Run(ctx, dir, role)
				if errors.Is(err, context.Canceled) {
					return nil
				}
				return err
			},
		}
		c.Flags().StringVar(&dir, "dir", "", "the live migration's worker directory")
		c.MarkFlagRequired("dir") //nolint:errcheck
		cdcCmd.AddCommand(c)
	}
	RootCmd.AddCommand(cdcCmd)
}
