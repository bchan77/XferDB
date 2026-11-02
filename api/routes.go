package api

import (
	"net/http"

	"gitea.homelab.local/nextdevops/XferDB/api/handlers"
	"gitea.homelab.local/nextdevops/XferDB/api/middleware"
)

// routes builds the ServeMux with all registered routes.
func (s *Server) routes() http.Handler {
	mux := http.NewServeMux()

	ph := &handlers.ProjectsHandler{DB: s.db, Log: s.log}
	mh := s.migrationsHandler()

	// Health
	mux.HandleFunc("GET /api/v1/health", handlers.Health)

	// Projects
	mux.HandleFunc("POST /api/v1/projects", ph.CreateProject)
	mux.HandleFunc("GET /api/v1/projects", ph.ListProjects)
	mux.HandleFunc("GET /api/v1/projects/{id}", ph.GetProject)
	mux.HandleFunc("PUT /api/v1/projects/{id}", ph.UpdateProject)
	mux.HandleFunc("DELETE /api/v1/projects/{id}", ph.DeleteProject)
	mux.HandleFunc("POST /api/v1/projects/{id}/preflight", ph.Preflight)
	mux.HandleFunc("POST /api/v1/connections/databases", ph.ListDatabases)
	mux.HandleFunc("POST /api/v1/connections/create-database", ph.CreateDatabase)
	mux.HandleFunc("POST /api/v1/projects/{id}/target-database", ph.CreateTargetDatabase)
	mux.HandleFunc("GET /api/v1/projects/{id}/support-bundle", ph.SupportBundle)
	mux.HandleFunc("POST /api/v1/projects/{id}/analyze", ph.Analyze)
	mux.HandleFunc("GET /api/v1/projects/{id}/tables/{table}", ph.GetTableSchema)
	mux.HandleFunc("GET /api/v1/projects/{id}/schema-plan", ph.GetSchemaPlan)
	mux.HandleFunc("PUT /api/v1/projects/{id}/schema-plan", ph.OverrideSchemaPlan)
	mux.HandleFunc("DELETE /api/v1/projects/{id}/schema-plan", ph.DeleteSchemaPlan)

	// Migrations (nested under project for creation/listing)
	mux.HandleFunc("POST /api/v1/projects/{id}/migrations", mh.StartMigration)
	mux.HandleFunc("GET /api/v1/projects/{id}/migrations", mh.ListMigrations)

	// Migrations (direct access by migration ID)
	mux.HandleFunc("GET /api/v1/migrations/{id}", mh.GetMigration)
	mux.HandleFunc("PATCH /api/v1/migrations/{id}", mh.PatchMigration)
	mux.HandleFunc("DELETE /api/v1/migrations/{id}", mh.DeleteMigration)
	mux.HandleFunc("POST /api/v1/migrations/{id}/cutover", mh.Cutover)
	mux.HandleFunc("GET /api/v1/migrations/{id}/stats", mh.GetStats)
	mux.HandleFunc("GET /api/v1/migrations/{id}/stats/history", mh.GetStatsHistory)

	// Validations: compare a migration's source and target after the fact.
	vh := s.validations
	mux.HandleFunc("POST /api/v1/migrations/{id}/validations", vh.StartValidation)
	mux.HandleFunc("GET /api/v1/migrations/{id}/validations", vh.ListValidations)
	mux.HandleFunc("GET /api/v1/validations/{id}", vh.GetValidation)
	mux.HandleFunc("PATCH /api/v1/validations/{id}", vh.PatchValidation)
	mux.HandleFunc("GET /api/v1/validations/{id}/tables/{table}/diffs", vh.GetValidationDiffs)
	mux.HandleFunc("GET /api/v1/projects/{id}/validation-status", vh.ValidationStatus)

	// TODO: WebSocket live progress at GET /api/v1/migrations/{id}/ws

	// Prometheus scrapes /metrics every few seconds, so it stays out of the
	// request log.
	root := http.NewServeMux()
	root.Handle("GET /metrics", middleware.Recovery(s.log, s.metrics.Handler()))
	root.Handle("/", middleware.Logging(s.log, middleware.Recovery(s.log, mux)))
	return root
}
