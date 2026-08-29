package api

import (
	"context"
	"log"
	"net/http"
	"sync"

	"gitea.homelab.local/nextdevops/XferDB/engine"
	"gitea.homelab.local/nextdevops/XferDB/state"
	"gitea.homelab.local/nextdevops/XferDB/stats"
)

// Server is the XferDB REST API server.
type Server struct {
	db *state.MetaDB

	mu         sync.Mutex
	engines    map[string]*engine.Engine          // keyed by migration ID
	collectors map[string]*stats.Collector        // keyed by migration ID
	cancels    map[string]context.CancelFunc      // keyed by migration ID
}

// NewServer creates a Server backed by the given state database.
func NewServer(db *state.MetaDB) *Server {
	return &Server{
		db:         db,
		engines:    make(map[string]*engine.Engine),
		collectors: make(map[string]*stats.Collector),
		cancels:    make(map[string]context.CancelFunc),
	}
}

// ListenAndServe starts the HTTP server on the given address.
func (s *Server) ListenAndServe(addr string) error {
	// Clean up any migrations that were running when the server last stopped.
	if n, err := s.db.MarkInterruptedMigrations(context.Background()); err != nil {
		log.Printf("warn: could not mark interrupted migrations: %v", err)
	} else if n > 0 {
		log.Printf("Marked %d interrupted migration(s) as failed (server was restarted)", n)
	}

	mux := s.routes()
	return http.ListenAndServe(addr, mux)
}
