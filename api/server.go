package api

import (
	"context"
	"log/slog"
	"net/http"
	"sync"

	"gitea.homelab.local/nextdevops/XferDB/api/handlers"
	"gitea.homelab.local/nextdevops/XferDB/engine"
	"gitea.homelab.local/nextdevops/XferDB/metrics"
	"gitea.homelab.local/nextdevops/XferDB/state"
	"gitea.homelab.local/nextdevops/XferDB/stats"
)

// Server is the XferDB REST API server.
type Server struct {
	db  *state.MetaDB
	log *slog.Logger

	mu         sync.Mutex
	engines    map[string]*engine.Engine
	collectors map[string]*stats.Collector
	cancels    map[string]context.CancelFunc

	// validations is shared by every routes() call so a validation started
	// through one handler can be found and cancelled through another.
	validations *handlers.ValidationsHandler

	// metrics is served at GET /metrics; shared by every routes() call, since
	// a registry accepts each collector only once.
	metrics *metrics.Metrics
}

// NewServer creates a Server backed by the given state database.
func NewServer(db *state.MetaDB, log *slog.Logger) *Server {
	if log == nil {
		log = slog.Default()
	}
	return &Server{
		db:          db,
		log:         log,
		engines:     make(map[string]*engine.Engine),
		collectors:  make(map[string]*stats.Collector),
		cancels:     make(map[string]context.CancelFunc),
		validations: handlers.NewValidationsHandler(db, log),
		metrics:     metrics.New(),
	}
}

// Handler returns the HTTP handler for the server, useful for testing.
func (s *Server) Handler() http.Handler {
	return s.routes()
}

// ListenAndServe starts the HTTP server on the given address.
func (s *Server) ListenAndServe(addr string) error {
	if n, err := s.db.MarkInterruptedMigrations(context.Background()); err != nil {
		s.log.Warn("could not mark interrupted migrations", "error", err)
	} else if n > 0 {
		s.log.Warn("marked interrupted migrations as failed", "count", n)
	}
	if n, err := s.db.MarkInterruptedValidations(context.Background()); err != nil {
		s.log.Warn("could not mark interrupted validations", "error", err)
	} else if n > 0 {
		s.log.Warn("marked interrupted validations as failed", "count", n)
	}

	s.log.Info("server.started", "addr", addr)
	mux := s.routes()
	return http.ListenAndServe(addr, mux)
}
