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
	mh := &handlers.MigrationsHandler{
		DB:         s.db,
		Mu:         &s.mu,
		Engines:    s.engines,
		Collectors: s.collectors,
		Cancels:    s.cancels,
		Log:        s.log,
	}

	// Health
	mux.HandleFunc("GET /api/v1/health", handlers.Health)

	// Projects
	mux.HandleFunc("POST /api/v1/projects", ph.CreateProject)
	mux.HandleFunc("GET /api/v1/projects", ph.ListProjects)
	mux.HandleFunc("GET /api/v1/projects/{id}", ph.GetProject)
	mux.HandleFunc("PUT /api/v1/projects/{id}", ph.UpdateProject)
	mux.HandleFunc("DELETE /api/v1/projects/{id}", ph.DeleteProject)
	mux.HandleFunc("POST /api/v1/projects/{id}/preflight", ph.Preflight)
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
	mux.HandleFunc("GET /api/v1/migrations/{id}/stats", mh.GetStats)
	mux.HandleFunc("GET /api/v1/migrations/{id}/stats/history", mh.GetStatsHistory)

	// TODO: WebSocket live progress at GET /api/v1/migrations/{id}/ws

	return middleware.Logging(s.log, middleware.Recovery(s.log, mux))
}
