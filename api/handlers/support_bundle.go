package handlers

import (
	"archive/tar"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"runtime"
	"strings"
	"time"

	"gitea.homelab.local/nextdevops/XferDB/adapters"
	"gitea.homelab.local/nextdevops/XferDB/registry"
	"gitea.homelab.local/nextdevops/XferDB/state"
	"gitea.homelab.local/nextdevops/XferDB/version"
)

// SupportBundle handles GET /api/v1/projects/{id}/support-bundle.
// Returns a tar.gz file containing diagnostic information for support.
func (h *ProjectsHandler) SupportBundle(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	p, err := h.DB.GetProject(r.Context(), id)
	if err != nil {
		writeError(w, http.StatusNotFound, "project not found")
		return
	}

	ctx := r.Context()

	// Set response headers for tar.gz download.
	filename := fmt.Sprintf("xferdb-support-%s-%s.tar.gz", sanitizeFilename(p.Name), time.Now().Format("20060102-150405"))
	w.Header().Set("Content-Type", "application/gzip")
	w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=%q", filename))

	// Create gzip writer wrapping the response.
	gw := gzip.NewWriter(w)
	defer gw.Close()

	// Create tar writer.
	tw := tar.NewWriter(gw)
	defer tw.Close()

	// Collect and write each section.
	h.writeSystemInfo(tw)
	h.writeProjectInfo(tw, p)
	h.writeSourceInfo(ctx, tw, p)
	h.writeTargetInfo(ctx, tw, p)
	h.writePreflight(ctx, tw, p)
	h.writeMigrations(ctx, tw, p)
	h.writeCheckpoints(ctx, tw, p)
	h.writeSourceSchema(ctx, tw, p)
	h.writeTargetSchema(ctx, tw, p)
	h.writeSchemaPlan(ctx, tw, p)

	h.Log.Info("support_bundle.generated", "project_id", id, "project_name", p.Name)
}

// writeSystemInfo writes system.json with version and environment info.
func (h *ProjectsHandler) writeSystemInfo(tw *tar.Writer) {
	info := map[string]any{
		"xferdb_version": version.Version,
		"go_version":     runtime.Version(),
		"os":             runtime.GOOS,
		"arch":           runtime.GOARCH,
		"generated_at":   time.Now().UTC().Format(time.RFC3339),
	}
	writeTarJSON(tw, "system.json", info)
}

// writeProjectInfo writes project.json with credentials redacted.
func (h *ProjectsHandler) writeProjectInfo(tw *tar.Writer, p *adapters.Project) {
	// Create a copy with redacted credentials.
	redacted := map[string]any{
		"id":          p.ID,
		"name":        p.Name,
		"description": p.Description,
		"created_at":  p.CreatedAt,
		"source_config": map[string]any{
			"type":     p.SourceConfig.Type,
			"host":     p.SourceConfig.Host,
			"port":     p.SourceConfig.Port,
			"database": p.SourceConfig.Database,
			"username": p.SourceConfig.Username,
			"password": redactPassword(p.SourceConfig.Password),
			"ssl_mode": p.SourceConfig.SSLMode,
			"dsn":      redactDSN(p.SourceConfig.DSN),
		},
		"target_config": map[string]any{
			"type":     p.TargetConfig.Type,
			"host":     p.TargetConfig.Host,
			"port":     p.TargetConfig.Port,
			"database": p.TargetConfig.Database,
			"username": p.TargetConfig.Username,
			"password": redactPassword(p.TargetConfig.Password),
			"ssl_mode": p.TargetConfig.SSLMode,
			"dsn":      redactDSN(p.TargetConfig.DSN),
		},
		"transfer_config": p.TransferConfig,
	}
	writeTarJSON(tw, "project.json", redacted)
}

// writeSourceInfo writes source_info.json with database metadata.
func (h *ProjectsHandler) writeSourceInfo(ctx context.Context, tw *tar.Writer, p *adapters.Project) {
	info, err := collectDatabaseInfo(ctx, p.SourceConfig, true)
	if err != nil {
		info = map[string]any{"error": err.Error()}
	}
	writeTarJSON(tw, "source_info.json", info)
}

// writeTargetInfo writes target_info.json with database metadata.
func (h *ProjectsHandler) writeTargetInfo(ctx context.Context, tw *tar.Writer, p *adapters.Project) {
	info, err := collectDatabaseInfo(ctx, p.TargetConfig, false)
	if err != nil {
		info = map[string]any{"error": err.Error()}
	}
	writeTarJSON(tw, "target_info.json", info)
}

// writePreflight writes preflight.json with permissions check.
func (h *ProjectsHandler) writePreflight(ctx context.Context, tw *tar.Writer, p *adapters.Project) {
	result := map[string]any{}

	// Source permissions.
	if src, err := registry.NewSource(p.SourceConfig.Type); err != nil {
		result["source"] = map[string]any{"error": err.Error()}
	} else if err := src.Connect(ctx, p.SourceConfig); err != nil {
		result["source"] = map[string]any{"error": friendlyConnectError(err)}
	} else {
		defer src.Close()
		perms, _ := src.CheckPermissions(ctx)
		result["source"] = perms
	}

	// Target permissions.
	if tgt, err := registry.NewTarget(p.TargetConfig.Type); err != nil {
		result["target"] = map[string]any{"error": err.Error()}
	} else if err := tgt.Connect(ctx, p.TargetConfig); err != nil {
		result["target"] = map[string]any{"error": friendlyConnectError(err)}
	} else {
		defer tgt.Close()
		perms, _ := tgt.CheckPermissions(ctx)
		result["target"] = perms
	}

	writeTarJSON(tw, "preflight.json", result)
}

// writeMigrations writes migrations.json with recent migration history.
func (h *ProjectsHandler) writeMigrations(ctx context.Context, tw *tar.Writer, p *adapters.Project) {
	migrations, err := h.DB.ListMigrations(ctx, p.ID)
	if err != nil {
		writeTarJSON(tw, "migrations.json", map[string]any{"error": err.Error()})
		return
	}

	// Include table progress for each migration.
	var result []map[string]any
	for _, m := range migrations {
		entry := map[string]any{
			"id":           m.ID,
			"status":       m.Status,
			"created_at":   m.CreatedAt,
			"started_at":   m.StartedAt,
			"completed_at": m.CompletedAt,
			"error":        m.Error,
		}

		// Get table progress.
		tables, _ := h.DB.GetTableProgress(ctx, m.ID)
		entry["tables"] = tables

		result = append(result, entry)
	}
	writeTarJSON(tw, "migrations.json", result)
}

// writeCheckpoints writes checkpoints.json for the latest migration.
func (h *ProjectsHandler) writeCheckpoints(ctx context.Context, tw *tar.Writer, p *adapters.Project) {
	migrations, err := h.DB.ListMigrations(ctx, p.ID)
	if err != nil || len(migrations) == 0 {
		writeTarJSON(tw, "checkpoints.json", []any{})
		return
	}

	// Get checkpoints for the most recent migration.
	latest := migrations[0]
	checkpoints, err := h.DB.ListCheckpoints(ctx, latest.ID)
	if err != nil {
		writeTarJSON(tw, "checkpoints.json", map[string]any{"error": err.Error()})
		return
	}
	writeTarJSON(tw, "checkpoints.json", checkpoints)
}

// writeSourceSchema writes source_schema.json with table schemas.
func (h *ProjectsHandler) writeSourceSchema(ctx context.Context, tw *tar.Writer, p *adapters.Project) {
	// For MongoDB, we need to load and set the schema plan first.
	if p.SourceConfig.Type == "mongodb" {
		schemas, err := h.collectMongoSchemas(ctx, p)
		if err != nil {
			writeTarJSON(tw, "source_schema.json", map[string]any{"error": err.Error()})
			return
		}
		writeTarJSON(tw, "source_schema.json", schemas)
		return
	}

	schemas, err := collectSchemas(ctx, p.SourceConfig, true)
	if err != nil {
		writeTarJSON(tw, "source_schema.json", map[string]any{"error": err.Error()})
		return
	}
	writeTarJSON(tw, "source_schema.json", schemas)
}

// collectMongoSchemas loads the schema plan and uses it to get MongoDB schemas.
func (h *ProjectsHandler) collectMongoSchemas(ctx context.Context, p *adapters.Project) ([]adapters.TableSchema, error) {
	// Load the schema plan from the database.
	plan, err := h.DB.GetPlanForProject(ctx, p.ID)
	if err != nil {
		return nil, fmt.Errorf("load schema plan: %w", err)
	}
	if len(plan) == 0 {
		return nil, fmt.Errorf("no schema plan found — run 'project analyze' first")
	}

	// Create and connect the source adapter.
	src, err := registry.NewSource(p.SourceConfig.Type)
	if err != nil {
		return nil, err
	}
	if err := src.Connect(ctx, p.SourceConfig); err != nil {
		return nil, err
	}
	defer src.Close()

	// Set the plan on the MongoDB source adapter.
	type planConsumer interface {
		SetPlan([]state.SchemaPlanRow) error
	}
	if pc, ok := src.(planConsumer); ok {
		if err := pc.SetPlan(plan); err != nil {
			return nil, fmt.Errorf("set plan: %w", err)
		}
	}

	return src.ListTables(ctx)
}

// writeTargetSchema writes target_schema.json with table schemas.
func (h *ProjectsHandler) writeTargetSchema(ctx context.Context, tw *tar.Writer, p *adapters.Project) {
	schemas, err := collectSchemas(ctx, p.TargetConfig, false)
	if err != nil {
		writeTarJSON(tw, "target_schema.json", map[string]any{"error": err.Error()})
		return
	}
	writeTarJSON(tw, "target_schema.json", schemas)
}

// writeSchemaPlan writes schema_plan.json for MongoDB projects.
func (h *ProjectsHandler) writeSchemaPlan(ctx context.Context, tw *tar.Writer, p *adapters.Project) {
	if p.SourceConfig.Type != "mongodb" {
		// Not a MongoDB project, skip.
		return
	}

	plan, err := h.DB.GetPlanForProject(ctx, p.ID)
	if err != nil {
		writeTarJSON(tw, "schema_plan.json", map[string]any{"error": err.Error()})
		return
	}
	writeTarJSON(tw, "schema_plan.json", plan)
}

// ── helpers ─────────────────────────────────────────────────────────────────

// writeTarJSON writes a JSON file to the tar archive.
func writeTarJSON(tw *tar.Writer, name string, v any) {
	data, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		data = []byte(fmt.Sprintf(`{"error": "marshal failed: %s"}`, err.Error()))
	}

	hdr := &tar.Header{
		Name:    name,
		Mode:    0644,
		Size:    int64(len(data)),
		ModTime: time.Now(),
	}
	if err := tw.WriteHeader(hdr); err != nil {
		return
	}
	tw.Write(data)
}

// collectDatabaseInfo connects to a database and returns its info.
func collectDatabaseInfo(ctx context.Context, cfg adapters.ConnectionConfig, isSource bool) (map[string]any, error) {
	if isSource {
		src, err := registry.NewSource(cfg.Type)
		if err != nil {
			return nil, err
		}
		if err := src.Connect(ctx, cfg); err != nil {
			return nil, err
		}
		defer src.Close()

		info, err := src.GetInfo(ctx)
		if err != nil {
			return nil, err
		}
		return map[string]any{
			"type":       info.Type,
			"version":    info.Version,
			"host":       info.Host,
			"database":   info.Database,
			"tables":     info.Tables,
			"size_bytes": info.SizeBytes,
			"size_human": info.SizeHuman,
			"ssl":        info.SSL,
		}, nil
	}

	tgt, err := registry.NewTarget(cfg.Type)
	if err != nil {
		return nil, err
	}
	if err := tgt.Connect(ctx, cfg); err != nil {
		return nil, err
	}
	defer tgt.Close()

	info, err := tgt.GetInfo(ctx)
	if err != nil {
		return nil, err
	}
	return map[string]any{
		"type":       info.Type,
		"version":    info.Version,
		"host":       info.Host,
		"database":   info.Database,
		"tables":     info.Tables,
		"size_bytes": info.SizeBytes,
		"size_human": info.SizeHuman,
		"ssl":        info.SSL,
	}, nil
}

// collectSchemas connects to a database and returns table schemas.
func collectSchemas(ctx context.Context, cfg adapters.ConnectionConfig, isSource bool) ([]adapters.TableSchema, error) {
	if isSource {
		src, err := registry.NewSource(cfg.Type)
		if err != nil {
			return nil, err
		}
		if err := src.Connect(ctx, cfg); err != nil {
			return nil, err
		}
		defer src.Close()
		return src.ListTables(ctx)
	}

	tgt, err := registry.NewTarget(cfg.Type)
	if err != nil {
		return nil, err
	}
	if err := tgt.Connect(ctx, cfg); err != nil {
		return nil, err
	}
	defer tgt.Close()
	return tgt.ListTables(ctx)
}

// redactPassword replaces a password with asterisks.
func redactPassword(password string) string {
	if password == "" {
		return ""
	}
	return "***"
}

// redactDSN redacts credentials from a connection string.
func redactDSN(dsn string) string {
	if dsn == "" {
		return ""
	}

	// Handle mongodb:// and mongodb+srv://
	if strings.HasPrefix(dsn, "mongodb://") || strings.HasPrefix(dsn, "mongodb+srv://") {
		return redactMongoURI(dsn)
	}

	// Handle postgres:// and postgresql://
	if strings.HasPrefix(dsn, "postgres://") || strings.HasPrefix(dsn, "postgresql://") {
		return redactPostgresURI(dsn)
	}

	// Handle mysql:// or user:pass@tcp(host)/db format
	if strings.Contains(dsn, "@") {
		// Generic redaction: replace everything before @ with ***:***
		atIdx := strings.LastIndex(dsn, "@")
		return "***:***" + dsn[atIdx:]
	}

	return dsn
}

// redactMongoURI redacts credentials from a MongoDB URI.
func redactMongoURI(uri string) string {
	// mongodb://user:pass@host:port/db -> mongodb://***:***@host:port/db
	var prefix string
	var rest string

	if strings.HasPrefix(uri, "mongodb+srv://") {
		prefix = "mongodb+srv://"
		rest = uri[14:]
	} else if strings.HasPrefix(uri, "mongodb://") {
		prefix = "mongodb://"
		rest = uri[10:]
	} else {
		return uri
	}

	atIdx := strings.Index(rest, "@")
	if atIdx < 0 {
		// No credentials in URI.
		return uri
	}

	return prefix + "***:***@" + rest[atIdx+1:]
}

// redactPostgresURI redacts credentials from a PostgreSQL URI.
func redactPostgresURI(uri string) string {
	// postgres://user:pass@host:port/db -> postgres://***:***@host:port/db
	var prefix string
	var rest string

	if strings.HasPrefix(uri, "postgresql://") {
		prefix = "postgresql://"
		rest = uri[13:]
	} else if strings.HasPrefix(uri, "postgres://") {
		prefix = "postgres://"
		rest = uri[11:]
	} else {
		return uri
	}

	atIdx := strings.Index(rest, "@")
	if atIdx < 0 {
		// No credentials in URI.
		return uri
	}

	return prefix + "***:***@" + rest[atIdx+1:]
}

// sanitizeFilename removes characters that are unsafe for filenames.
func sanitizeFilename(s string) string {
	// Replace unsafe characters with underscores.
	var result strings.Builder
	for _, r := range s {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '-' || r == '_' {
			result.WriteRune(r)
		} else {
			result.WriteRune('_')
		}
	}
	return result.String()
}
