package handlers

import (
	"archive/tar"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	"gitea.homelab.local/nextdevops/XferDB/adapters"
	"gitea.homelab.local/nextdevops/XferDB/state"
)

// statsMigrations is how many of the newest migrations get their stats
// samples in a support bundle (one every few seconds of each run).
const statsMigrations = 10

// bundleMigration is everything a support bundle records about one migration.
type bundleMigration struct {
	m           adapters.Migration
	tables      []adapters.TableProgress
	runConfig   *adapters.TransferConfig
	steps       []state.SchemaStep
	stats       []state.MigrationStatsRecord
	validations []bundleValidation
}

type bundleValidation struct {
	state.Validation
	Tables []state.ValidationTable `json:"tables"`
}

// loadBundleMigrations reads the project's migrations, newest first, with what
// the metadb holds for each.
func (h *ProjectsHandler) loadBundleMigrations(ctx context.Context, projectID string) ([]bundleMigration, error) {
	migs, err := h.DB.ListMigrations(ctx, projectID)
	if err != nil {
		return nil, err
	}
	out := make([]bundleMigration, 0, len(migs))
	for i, m := range migs {
		bm := bundleMigration{m: m}
		bm.tables, _ = h.DB.GetTableProgress(ctx, m.ID)
		if cfg, ok, err := h.DB.GetRunConfig(ctx, m.ID); err == nil && ok {
			bm.runConfig = &cfg
		}
		bm.steps, _ = h.DB.ListSchemaSteps(ctx, m.ID)
		if i < statsMigrations {
			bm.stats, _ = h.DB.GetMigrationStats(ctx, m.ID)
		}
		vals, _ := h.DB.ListValidations(ctx, m.ID)
		for _, v := range vals {
			vt, _ := h.DB.GetValidationTables(ctx, v.ID)
			bm.validations = append(bm.validations, bundleValidation{Validation: v, Tables: vt})
		}
		out = append(out, bm)
	}
	return out, nil
}

// writeMigrations writes migrations.json: each migration with its tables
// (and their errors), the settings it actually ran with, and its post-schema
// steps.
func writeMigrations(tw *tar.Writer, migs []bundleMigration) {
	result := make([]map[string]any, 0, len(migs))
	for _, bm := range migs {
		m := bm.m
		result = append(result, map[string]any{
			"id":           m.ID,
			"status":       m.Status,
			"mode":         m.Mode,
			"phase":        m.Phase,
			"created_at":   m.CreatedAt,
			"started_at":   m.StartedAt,
			"completed_at": m.CompletedAt,
			"error":        m.Error,
			"run_config":   bm.runConfig,
			"tables":       bm.tables,
			"schema_steps": bm.steps,
		})
	}
	writeTarJSON(tw, "migrations.json", result)
}

// writeStats writes migration_stats.json: the progress samples (rows, rate,
// CPU, memory) of the newest migrations, by migration id.
func writeStats(tw *tar.Writer, migs []bundleMigration) {
	out := map[string][]state.MigrationStatsRecord{}
	for _, bm := range migs {
		if len(bm.stats) > 0 {
			out[bm.m.ID] = bm.stats
		}
	}
	writeTarJSON(tw, "migration_stats.json", out)
}

// writeValidations writes validations.json: each migration's validation runs
// with their per-table results.
func writeValidations(tw *tar.Writer, migs []bundleMigration) {
	out := map[string][]bundleValidation{}
	for _, bm := range migs {
		if len(bm.validations) > 0 {
			out[bm.m.ID] = bm.validations
		}
	}
	writeTarJSON(tw, "validations.json", out)
}

// projectLogLines returns the kept log lines that belong to this project: those
// naming it or one of its migrations, and server-wide lines that name no
// project or migration at all. Lines about other projects are left out.
func (h *ProjectsHandler) projectLogLines(p *adapters.Project, migs []bundleMigration) []map[string]any {
	if h.Logs == nil {
		return nil
	}
	ours := map[string]bool{p.ID: true}
	for _, bm := range migs {
		ours[bm.m.ID] = true
	}
	var out []map[string]any
	for _, line := range h.Logs.Lines() {
		var rec map[string]any
		if json.Unmarshal(line, &rec) != nil {
			continue
		}
		keep := true
		for _, k := range []string{"migration_id", "project_id"} {
			if v, ok := rec[k].(string); ok && v != "" && !ours[v] {
				keep = false
			}
		}
		// A request line names its project or migration only in its path.
		if path, ok := rec["path"].(string); ok {
			if id := pathID(path); id != "" && !ours[id] {
				keep = false
			}
		}
		if keep {
			out = append(out, rec)
		}
	}
	return out
}

// pathID returns the project or migration id in an API path
// (/api/v1/projects/<id>/..., /api/v1/migrations/<id>/...), "" if none.
func pathID(path string) string {
	parts := strings.Split(strings.Trim(path, "/"), "/")
	for i := 0; i+1 < len(parts); i++ {
		if parts[i] == "projects" || parts[i] == "migrations" {
			return parts[i+1]
		}
	}
	return ""
}

// writeLogs writes logs.jsonl: this project's recent server log lines.
func writeLogs(tw *tar.Writer, lines []map[string]any) {
	var buf bytes.Buffer
	for _, rec := range lines {
		b, _ := json.Marshal(rec)
		buf.Write(b)
		buf.WriteByte('\n')
	}
	writeTarFile(tw, "logs.jsonl", buf.Bytes())
}

// timelineEvent is one line of timeline.json.
type timelineEvent struct {
	Time      time.Time `json:"time"`
	Migration string    `json:"migration,omitempty"`
	Event     string    `json:"event"`
	Table     string    `json:"table,omitempty"`
	Status    string    `json:"status,omitempty"`
	Detail    string    `json:"detail,omitempty"`
}

// buildTimeline merges what the metadb and the log hold into one list in time
// order: migrations and their tables starting and ending (with errors), post-
// schema steps, schema overrides being saved, stats phase changes,
// validations, and warning/error log lines.
func buildTimeline(plan []state.SchemaPlanRow, migs []bundleMigration, logs []map[string]any) []timelineEvent {
	var ev []timelineEvent
	add := func(t *time.Time, e timelineEvent) {
		if t == nil || t.IsZero() {
			return
		}
		e.Time = *t
		ev = append(ev, e)
	}
	for _, r := range plan {
		if !r.Overridden {
			continue
		}
		t := r.CreatedAt
		add(&t, timelineEvent{Event: "schema.override", Table: r.Collection,
			Detail: fmt.Sprintf("%s -> %s (%s); first saved at this time, later edits keep it", r.FieldName, r.PgType, r.Strategy)})
	}
	for _, bm := range migs {
		m, id := bm.m, bm.m.ID
		created := m.CreatedAt
		detail := ""
		if bm.runConfig != nil {
			b, _ := json.Marshal(bm.runConfig)
			detail = "run_config " + string(b)
		}
		add(&created, timelineEvent{Migration: id, Event: "migration.created", Status: m.Mode, Detail: detail})
		add(m.StartedAt, timelineEvent{Migration: id, Event: "migration.started"})
		add(m.CompletedAt, timelineEvent{Migration: id, Event: "migration.ended", Status: string(m.Status), Detail: m.Error})
		for _, t := range bm.tables {
			add(t.StartedAt, timelineEvent{Migration: id, Event: "table.started", Table: t.TableName})
			// A failed or cancelled table's counts are reset to 0 (resume
			// starts it over), so 0/0 there says nothing: leave it out.
			rows := ""
			if t.RowsTotal > 0 || t.RowsTransferred > 0 || t.Status == adapters.StatusCompleted {
				rows = fmt.Sprintf("%d/%d rows", t.RowsTransferred, t.RowsTotal)
			}
			add(t.CompletedAt, timelineEvent{Migration: id, Event: "table.ended", Table: t.TableName, Status: string(t.Status),
				Detail: strings.TrimSpace(rows + " " + t.Error)})
		}
		for _, s := range bm.steps {
			st := s.StartedAt
			name := s.Kind + " " + s.Name
			add(&st, timelineEvent{Migration: id, Event: "schema_step.started", Table: s.Table, Detail: name})
			add(s.FinishedAt, timelineEvent{Migration: id, Event: "schema_step.ended", Table: s.Table, Status: s.Status,
				Detail: strings.TrimSpace(fmt.Sprintf("%s, %d attempt(s) %s", name, s.Attempts, s.Error))})
		}
		phase := ""
		for _, s := range bm.stats {
			if s.Phase == phase {
				continue
			}
			phase = s.Phase
			ts := s.Timestamp
			add(&ts, timelineEvent{Migration: id, Event: "stats.phase", Status: s.Phase,
				Detail: fmt.Sprintf("%d/%d rows, %d/%d tables done, %d failed", s.RowsTransferred, s.RowsTotal, s.TablesDone, s.TablesTotal, s.TablesFailed)})
		}
		for _, v := range bm.validations {
			add(v.StartedAt, timelineEvent{Migration: id, Event: "validation.started", Status: v.Level})
			add(v.CompletedAt, timelineEvent{Migration: id, Event: "validation.ended", Status: string(v.Status), Detail: v.Error})
		}
	}
	for _, rec := range logs {
		level, _ := rec["level"].(string)
		if level != "WARN" && level != "ERROR" {
			continue
		}
		ts, _ := rec["time"].(string)
		t, err := time.Parse(time.RFC3339Nano, ts)
		if err != nil {
			continue
		}
		mig, _ := rec["migration_id"].(string)
		table, _ := rec["table"].(string)
		msg, _ := rec["msg"].(string)
		var rest []string
		for k, v := range rec {
			switch k {
			case "time", "level", "msg", "migration_id", "table":
				continue
			}
			rest = append(rest, fmt.Sprintf("%s=%v", k, v))
		}
		sort.Strings(rest)
		add(&t, timelineEvent{Migration: mig, Event: "log." + strings.ToLower(level), Table: table,
			Status: msg, Detail: strings.Join(rest, " ")})
	}
	sort.SliceStable(ev, func(i, j int) bool { return ev[i].Time.Before(ev[j].Time) })
	return ev
}

// bundleReadme explains the bundle's files.
const bundleReadme = `XferDB support bundle

timeline.json         Start here: everything below in time order (migrations,
                      tables with their errors, post-schema steps, schema
                      overrides, stats phase changes, validations, and
                      warning/error log lines).
logs.jsonl            The server's recent log lines for this project (kept in
                      memory, so lines from before the last server restart are
                      only in the --log-file, if one was set). Credentials are
                      masked.
migrations.json       Every migration: status, error, the settings it ran with
                      (run_config), per-table progress and errors, and
                      post-schema steps (indexes, constraints) with timings.
migration_stats.json  Progress samples (rows, rate, CPU, memory) of the newest
                      migrations. A failed or cancelled table's row counts in
                      migrations.json are reset to 0; these samples (and the
                      log) show how far it got.
validations.json      Validation runs and per-table results.
schema_plan.json      The project's schema plan, including type overrides.
project.json          Project settings (passwords removed).
source_*/target_*     Connection checks, database info and schemas, read when
                      the bundle was made.
checkpoints.json      The latest migration's checkpoints.
system.json           XferDB version, platform and logging setup.
`

// writeTarFile writes raw bytes to the tar archive.
func writeTarFile(tw *tar.Writer, name string, data []byte) {
	if err := tw.WriteHeader(&tar.Header{Name: name, Mode: 0o644, Size: int64(len(data)), ModTime: time.Now()}); err != nil {
		return
	}
	tw.Write(data)
}
