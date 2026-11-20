package api

import (
	"context"
	"gitea.homelab.local/nextdevops/XferDB/logring"
	"log/slog"
	"net/http"
	"sync"

	"gitea.homelab.local/nextdevops/XferDB/adapters"
	"gitea.homelab.local/nextdevops/XferDB/api/handlers"
	"gitea.homelab.local/nextdevops/XferDB/cdc/live"
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

	// live runs live migrations (#102); nil until EnableLive.
	live *live.Manager

	// logs keeps the recent log lines for support bundles; nil until
	// SetLogRing. logFile is the --log-file path, if any.
	logs    *logring.Ring
	logFile string
}

// SetLogRing gives support bundles the server's recent log lines, and the
// path of its log file (empty if none).
func (s *Server) SetLogRing(r *logring.Ring, logFile string) {
	s.logs, s.logFile = r, logFile
}

// EnableLive lets the server run live migrations, with each one's worker
// directory under dir and its workers started by launcher.
func (s *Server) EnableLive(dir string, launcher live.Launcher) {
	s.live = &live.Manager{DB: s.db, Dir: dir, Launcher: launcher, Log: s.log}
	s.live.OnCompleted = func(id string, p *adapters.Project) {
		s.validations.AfterCutover(id, p, s.log.With("migration_id", id))
	}
	s.metrics.SetLive(func() map[string]metrics.LiveStream {
		out := map[string]metrics.LiveStream{}
		for id, st := range s.live.Statuses() {
			ls := metrics.LiveStream{
				Phase: st.Phase, LagBytes: st.LagBytes, LagChanges: st.LagChanges,
				LagSeconds: st.LagSeconds, QueueBytes: st.QueueBytes,
			}
			if st.Capture != nil {
				ls.ChangesQueued = st.Capture.ChangesQueued
			}
			if st.Apply != nil {
				ls.ChangesApplied = st.Apply.ChangesApplied
			}
			if st.Retention != nil {
				ls.RetainedBytes = &st.Retention.RetainedBytes
			}
			ls.SchemaChange = st.SchemaChange != nil
			out[id] = ls
		}
		return out
	})
}

// Live returns the server's live migration manager, nil when not enabled.
func (s *Server) Live() *live.Manager { return s.live }

func (s *Server) migrationsHandler() *handlers.MigrationsHandler {
	return &handlers.MigrationsHandler{
		DB:          s.db,
		Mu:          &s.mu,
		Engines:     s.engines,
		Collectors:  s.collectors,
		Cancels:     s.cancels,
		Log:         s.log,
		Validations: s.validations,
		Metrics:     s.metrics,
		Live:        s.live,
	}
}

// ResumeLive resumes the live migrations a restart interrupted. Call once,
// at startup, after MarkInterruptedMigrations.
func (s *Server) ResumeLive(ctx context.Context) {
	s.migrationsHandler().ResumeLive(ctx)
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
	s.Recover(context.Background())
	s.log.Info("server.started", "addr", addr)
	mux := s.routes()
	return http.ListenAndServe(addr, mux)
}

// Recover cleans up after the last run of the server, before it serves:
// migrations and validations it was running (or had paused) fail as
// interrupted, and live migrations are resumed. Call once, at startup.
func (s *Server) Recover(ctx context.Context) {
	if n, err := s.db.MarkInterruptedMigrations(ctx); err != nil {
		s.log.Warn("could not mark interrupted migrations", "error", err)
	} else if n > 0 {
		s.log.Warn("marked interrupted migrations as failed", "count", n)
	}
	if n, err := s.db.MarkInterruptedValidations(ctx); err != nil {
		s.log.Warn("could not mark interrupted validations", "error", err)
	} else if n > 0 {
		s.log.Warn("marked interrupted validations as failed", "count", n)
	}
	s.ResumeLive(ctx)
}
