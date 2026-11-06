package commands

import (
	"bufio"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"strings"

	"github.com/spf13/cobra"
)

// leftoversCmd lists the live migration streams on the project's source
// that no running migration owns, and drops one on request.
var leftoversCmd = &cobra.Command{
	Use:   "leftovers [drop NAME]",
	Short: "List (or drop) leftover live migration streams on the source",
	Long: `List the live migration streams (replication slot, publication and, on
YugabyteDB, heartbeat row) on the project's source, marking the leftovers:
those no migration still running on this server owns. The source keeps every
change for a replication slot until it is dropped, so a leftover costs disk.

  xferdb leftovers               list them
  xferdb leftovers drop NAME     drop one (asks first; --yes doesn't)

A stream this server doesn't know may belong to another XferDB server using
the same source: drop it only if none does. A stream a running migration
owns, or whose slot is in use, is refused.`,
	Args: cobra.RangeArgs(0, 2),
	RunE: func(cmd *cobra.Command, args []string) error {
		projectFlag, _ := cmd.Flags().GetString("project")
		yes, _ := cmd.Flags().GetBool("yes")
		projectName, err := currentProject(projectFlag)
		if err != nil {
			return err
		}
		projectID, err := resolveProjectID(projectName)
		if err != nil {
			return err
		}
		switch {
		case len(args) == 0:
			return listLeftovers(projectID)
		case len(args) == 2 && args[0] == "drop":
			name := args[1]
			if !yes {
				fmt.Printf("Drop the replication slot, publication and heartbeat %s from the source of project %s? [y/N] ", name, projectName)
				answer, _ := bufio.NewReader(os.Stdin).ReadString('\n')
				if a := strings.ToLower(strings.TrimSpace(answer)); a != "y" && a != "yes" {
					return fmt.Errorf("not dropped")
				}
			}
			return dropLeftover(projectID, name)
		}
		return fmt.Errorf("usage: xferdb leftovers [drop NAME]")
	},
}

func init() {
	leftoversCmd.Flags().StringP("project", "p", "", "Project name (overrides current context)")
	leftoversCmd.Flags().BoolP("yes", "y", false, "Drop without asking")
	RootCmd.AddCommand(leftoversCmd)
}

type liveStreamView struct {
	Name          string `json:"name"`
	Slot          bool   `json:"slot"`
	Publication   bool   `json:"publication"`
	Heartbeat     bool   `json:"heartbeat"`
	Active        bool   `json:"active"`
	RetainedBytes *int64 `json:"retained_bytes"`
	Leftover      bool   `json:"leftover"`
	Reason        string `json:"reason"`
}

func listLeftovers(projectID string) error {
	resp, err := http.Get(ServerAddr + "/api/v1/projects/" + url.PathEscape(projectID) + "/live-streams")
	if err != nil {
		return fmt.Errorf("API request failed: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		var e struct {
			Error string `json:"error"`
		}
		json.NewDecoder(resp.Body).Decode(&e)
		return fmt.Errorf("list live streams: %s %s", resp.Status, e.Error)
	}
	var out struct {
		Streams []liveStreamView `json:"streams"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return err
	}
	if len(out.Streams) == 0 {
		fmt.Println("No live migration streams on the source.")
		return nil
	}
	leftovers := 0
	for _, s := range out.Streams {
		var parts []string
		if s.Slot {
			parts = append(parts, "slot")
		}
		if s.Publication {
			parts = append(parts, "publication")
		}
		if s.Heartbeat {
			parts = append(parts, "heartbeat")
		}
		mark := "  "
		if s.Leftover {
			mark = "! "
			leftovers++
		}
		line := fmt.Sprintf("%s%s  [%s]", mark, s.Name, strings.Join(parts, ", "))
		if s.Active {
			line += " in use"
		}
		if s.RetainedBytes != nil {
			line += "  WAL held: " + fmtBytes(*s.RetainedBytes)
		}
		fmt.Println(line)
		fmt.Println("    " + s.Reason)
	}
	if leftovers > 0 {
		fmt.Printf("\n%d leftover(s), marked !. Drop one with: xferdb leftovers drop NAME\n", leftovers)
	}
	return nil
}

func dropLeftover(projectID, name string) error {
	req, err := http.NewRequest(http.MethodDelete,
		ServerAddr+"/api/v1/projects/"+url.PathEscape(projectID)+"/live-streams/"+url.PathEscape(name), nil)
	if err != nil {
		return err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("API request failed: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		var e struct {
			Error string `json:"error"`
		}
		json.NewDecoder(resp.Body).Decode(&e)
		return fmt.Errorf("drop %s refused: %s %s", name, resp.Status, e.Error)
	}
	fmt.Printf("Dropped %s from the source.\n", name)
	return nil
}
