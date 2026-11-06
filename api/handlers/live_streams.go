package handlers

import (
	"context"
	"fmt"
	"net/http"
	"strings"

	"gitea.homelab.local/nextdevops/XferDB/adapters"
	"gitea.homelab.local/nextdevops/XferDB/adapters/postgres"
	"gitea.homelab.local/nextdevops/XferDB/state"
)

// liveStreamInfo is a live migration's stream found on a project's source,
// with the migration it belongs to on this server, if any.
//
// A stream is a leftover when no migration still running on this server
// owns it: its migration ended but the stream wasn't dropped (the source
// was unreachable at cancel, the server stopped mid-teardown), the
// migration was deleted, or it belongs to another XferDB server using the
// same source, which this server can't tell apart. So a leftover is never
// dropped automatically: only on request, and never while its slot is in
// use.
type liveStreamInfo struct {
	adapters.LiveStream
	MigrationID     string `json:"migration_id,omitempty"`
	ProjectID       string `json:"project_id,omitempty"`
	ProjectName     string `json:"project_name,omitempty"`
	MigrationStatus string `json:"migration_status,omitempty"`
	Leftover        bool   `json:"leftover"`
	// Reason says why it is (or isn't) a leftover.
	Reason string `json:"reason"`
}

// streamOwner is a migration and its project, by its stream's name.
type streamOwner struct {
	migration adapters.Migration
	project   adapters.Project
}

// streamOwners maps every live migration on this server, in any project,
// by the name its stream has on the source.
func streamOwners(ctx context.Context, db *state.MetaDB) (map[string]streamOwner, error) {
	projects, err := db.ListProjects(ctx)
	if err != nil {
		return nil, err
	}
	out := map[string]streamOwner{}
	for _, p := range projects {
		migs, err := db.ListMigrations(ctx, p.ID)
		if err != nil {
			return nil, err
		}
		for _, m := range migs {
			if m.Mode == adapters.MigrationModeLive {
				out[postgres.SlotName(m.ID)] = streamOwner{migration: m, project: p}
			}
		}
	}
	return out, nil
}

// classifyStreams lists the live migration streams on src, telling which
// are leftovers. ok is false when src can't list them.
func classifyStreams(ctx context.Context, db *state.MetaDB, src adapters.SourceAdapter) (streams []liveStreamInfo, ok bool, err error) {
	lister, isLister := src.(adapters.LiveStreamLister)
	if !isLister {
		return nil, false, nil
	}
	found, err := lister.LiveStreams(ctx)
	if err != nil {
		return nil, true, err
	}
	owners, err := streamOwners(ctx, db)
	if err != nil {
		return nil, true, err
	}
	for _, st := range found {
		info := liveStreamInfo{LiveStream: st}
		owner, known := owners[st.Name]
		switch {
		case !known:
			info.Leftover = true
			info.Reason = "no migration on this server owns it: left by a deleted migration, or used by another XferDB server"
		case running(owner.migration.Status):
			info.Reason = fmt.Sprintf("in use by migration %s (%s)", owner.migration.ID, owner.migration.Status)
		default:
			info.Leftover = true
			info.Reason = fmt.Sprintf("its migration %s is %s, but the stream wasn't dropped", owner.migration.ID, owner.migration.Status)
		}
		if known {
			info.MigrationID, info.MigrationStatus = owner.migration.ID, string(owner.migration.Status)
			info.ProjectID, info.ProjectName = owner.project.ID, owner.project.Name
		}
		streams = append(streams, info)
	}
	return streams, true, nil
}

func running(s adapters.MigrationStatus) bool {
	return s == adapters.StatusPending || s == adapters.StatusInProgress || s == adapters.StatusPaused
}

// leftoverCheck is the readiness warning for leftover streams, or false
// when there are none (or the source can't list them).
func leftoverCheck(streams []liveStreamInfo) (adapters.LiveCheck, bool) {
	var names []string
	for _, st := range streams {
		if st.Leftover {
			names = append(names, st.Name)
		}
	}
	if len(names) == 0 {
		return adapters.LiveCheck{}, false
	}
	return adapters.LiveCheck{
		Name:    "no leftover live migration streams",
		OK:      true,
		Warning: true,
		Detail:  fmt.Sprintf("%d on the source: %s", len(names), strings.Join(names, ", ")),
		Fix:     "a leftover replication slot keeps the source's changes (WAL) until it is dropped; check that no other XferDB server uses it, then drop it in Settings → Live Migration",
	}, true
}

// ListLiveStreams handles GET /api/v1/projects/{id}/live-streams: the live
// migration streams on the project's source, leftovers marked.
func (h *ProjectsHandler) ListLiveStreams(w http.ResponseWriter, r *http.Request) {
	p, err := h.DB.GetProject(r.Context(), r.PathValue("id"))
	if err != nil {
		writeError(w, http.StatusNotFound, "project not found")
		return
	}
	src, err := connectSource(r.Context(), p.SourceConfig)
	if err != nil {
		writeError(w, http.StatusBadGateway, "connect to source: "+friendlyConnectError(err))
		return
	}
	defer src.Close()
	streams, ok, err := classifyStreams(r.Context(), h.DB, src)
	if !ok {
		writeError(w, http.StatusBadRequest, "a "+p.SourceConfig.Type+" source has no live migration streams")
		return
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	if streams == nil {
		streams = []liveStreamInfo{}
	}
	writeJSON(w, http.StatusOK, map[string]any{"streams": streams})
}

// DropLiveStream handles DELETE /api/v1/projects/{id}/live-streams/{name}:
// drops a leftover stream from the project's source. A stream a running
// migration owns, or whose slot is in use, is refused (409).
func (h *ProjectsHandler) DropLiveStream(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	p, err := h.DB.GetProject(ctx, r.PathValue("id"))
	if err != nil {
		writeError(w, http.StatusNotFound, "project not found")
		return
	}
	name := r.PathValue("name")
	src, err := connectSource(ctx, p.SourceConfig)
	if err != nil {
		writeError(w, http.StatusBadGateway, "connect to source: "+friendlyConnectError(err))
		return
	}
	defer src.Close()
	streams, ok, err := classifyStreams(ctx, h.DB, src)
	if !ok {
		writeError(w, http.StatusBadRequest, "a "+p.SourceConfig.Type+" source has no live migration streams")
		return
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	var found *liveStreamInfo
	for i := range streams {
		if streams[i].Name == name {
			found = &streams[i]
		}
	}
	switch {
	case found == nil:
		writeError(w, http.StatusNotFound, "no live migration stream "+name+" on the source")
		return
	case !found.Leftover:
		writeError(w, http.StatusConflict, name+" isn't a leftover: "+found.Reason+"; cancel or delete that migration instead")
		return
	case found.Active:
		msg := "replication slot " + name + " is in use: something (maybe another XferDB server) is streaming from it"
		if p.SourceConfig.Type == adapters.TypeYugabyte {
			msg += ". YugabyteDB also counts a slot as in use for a minute after its stream stops: if nothing streams from it, try again then"
		}
		writeError(w, http.StatusConflict, msg)
		return
	}
	if err := src.(adapters.LiveStreamLister).DropLiveStream(ctx, name); err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	h.Log.Info("live.leftover_dropped", "project_id", p.ID, "stream", name, "migration_id", found.MigrationID)
	w.WriteHeader(http.StatusNoContent)
}
