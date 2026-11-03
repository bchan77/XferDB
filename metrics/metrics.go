// Package metrics exposes migration progress in the Prometheus text format
// at GET /metrics on xferdb server (#105).
//
// Counters are fed from each migration's engine events through its stats
// collector (stats.Observer). Per-migration gauges are read from the active
// migrations' collector snapshots at scrape time, so nothing here keeps a copy
// of that state, and a migration's gauges disappear once it ends.
package metrics

import (
	"net/http"
	"sync"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"gitea.homelab.local/nextdevops/XferDB/engine"
	"gitea.homelab.local/nextdevops/XferDB/stats"
)

// Metrics owns the registry served at /metrics. One per server.
type Metrics struct {
	reg *prometheus.Registry

	rowsTransferred    *prometheus.CounterVec
	tablesFinished     *prometheus.CounterVec
	migrationsFinished *prometheus.CounterVec
	batchRead          *prometheus.HistogramVec
	batchWrite         *prometheus.HistogramVec

	mu   sync.Mutex
	runs map[string]*Run // active migrations, by migration ID
	// live reports the live migrations' streams at scrape time (#102).
	live func() map[string]LiveStream
}

// LiveStream is one live migration's stream, as its gauges report it.
type LiveStream struct {
	Phase          string
	LagBytes       int64
	LagChanges     uint64
	LagSeconds     *float64
	ChangesQueued  int64
	ChangesApplied int64
	QueueBytes     int64
	// RetainedBytes is the log the source keeps for the stream (nil when
	// unknown), and SchemaChange whether it is paused at a schema change.
	RetainedBytes *int64
	SchemaChange  bool
}

// SetLive makes the live migrations' streams, read by streams at scrape
// time, part of the metrics, labelled by migration.
func (m *Metrics) SetLive(streams func() map[string]LiveStream) {
	m.mu.Lock()
	m.live = streams
	m.mu.Unlock()
}

// New creates a Metrics with its own registry, including the standard Go
// runtime and process collectors.
func New() *Metrics {
	m := &Metrics{
		reg: prometheus.NewRegistry(),
		rowsTransferred: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "xferdb_rows_transferred_total",
			Help: "Rows written to the target.",
		}, []string{"project", "table"}),
		tablesFinished: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "xferdb_tables_finished_total",
			Help: "Tables that finished transferring, by outcome (completed, failed, cancelled).",
		}, []string{"project", "status"}),
		migrationsFinished: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "xferdb_migrations_finished_total",
			Help: "Migrations that ended, by outcome (completed, failed, cancelled).",
		}, []string{"project", "status"}),
		batchRead: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "xferdb_batch_read_duration_seconds",
			Help:    "Time to read one batch from the source. Sampled: batch events may be dropped under load.",
			Buckets: prometheus.DefBuckets,
		}, []string{"project"}),
		batchWrite: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "xferdb_batch_write_duration_seconds",
			Help:    "Time to write one batch to the target. Sampled: batch events may be dropped under load.",
			Buckets: prometheus.DefBuckets,
		}, []string{"project"}),
		runs: make(map[string]*Run),
	}
	m.reg.MustRegister(
		m.rowsTransferred, m.tablesFinished, m.migrationsFinished, m.batchRead, m.batchWrite,
		gaugeCollector{m},
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
	)
	return m
}

// Handler serves the registry in the Prometheus exposition format.
func (m *Metrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.reg, promhttp.HandlerOpts{})
}

// Track starts recording a migration. The returned Run is the collector's
// observer: pass it to col.SetObserver before col.Start.
func (m *Metrics) Track(migrationID, project string, col *stats.Collector) *Run {
	r := &Run{m: m, migrationID: migrationID, project: project, col: col, last: make(map[string]int64)}
	m.mu.Lock()
	m.runs[migrationID] = r
	m.mu.Unlock()
	return r
}

// Run records one migration's events. It implements stats.Observer and is
// only called from that collector's goroutine.
type Run struct {
	m           *Metrics
	migrationID string
	project     string
	col         *stats.Collector

	// last is the highest running row total seen per table. Events carry a
	// table's running total rather than a delta, so the counter adds only
	// the increase: a dropped batch event loses nothing (the next one, or
	// the guaranteed table-done event, covers it), and segment workers'
	// events arriving out of order can't count rows twice.
	last map[string]int64
}

// Observe implements stats.Observer.
func (r *Run) Observe(ev engine.ProgressEvent) {
	m := r.m
	switch ev.Kind {
	case engine.EventTableStart:
		// A resumed table starts from its checkpointed total, which an
		// earlier run already counted.
		if ev.RowsTransferred > r.last[ev.TableName] {
			r.last[ev.TableName] = ev.RowsTransferred
		}
	case engine.EventBatch:
		r.addRows(ev)
		if ev.ReadDuration > 0 {
			m.batchRead.WithLabelValues(r.project).Observe(ev.ReadDuration.Seconds())
		}
		if ev.WriteDuration > 0 {
			m.batchWrite.WithLabelValues(r.project).Observe(ev.WriteDuration.Seconds())
		}
	case engine.EventTableDone:
		r.addRows(ev)
		m.tablesFinished.WithLabelValues(r.project, "completed").Inc()
	case engine.EventTableFailed:
		m.tablesFinished.WithLabelValues(r.project, "failed").Inc()
	case engine.EventTableCancelled:
		m.tablesFinished.WithLabelValues(r.project, "cancelled").Inc()
	case engine.EventComplete:
		m.migrationsFinished.WithLabelValues(r.project, "completed").Inc()
	case engine.EventError:
		m.migrationsFinished.WithLabelValues(r.project, "failed").Inc()
	case engine.EventCancelled:
		m.migrationsFinished.WithLabelValues(r.project, "cancelled").Inc()
	}
}

func (r *Run) addRows(ev engine.ProgressEvent) {
	if d := ev.RowsTransferred - r.last[ev.TableName]; d > 0 {
		r.m.rowsTransferred.WithLabelValues(r.project, ev.TableName).Add(float64(d))
		r.last[ev.TableName] = ev.RowsTransferred
	}
}

// Done implements stats.Observer: the migration has ended, so its gauges go.
func (r *Run) Done() {
	r.m.mu.Lock()
	if r.m.runs[r.migrationID] == r {
		delete(r.m.runs, r.migrationID)
	}
	r.m.mu.Unlock()
}

// activePhases are the snapshot phases of a migration that hasn't ended.
var activePhases = []string{"pending", "schema", "in_progress", "post_schema", "paused"}

var (
	descActive = prometheus.NewDesc("xferdb_migrations_active",
		"Migrations currently running, by phase.", []string{"phase"}, nil)
	descPhase = prometheus.NewDesc("xferdb_migration_phase",
		"1 for an active migration's current phase.", []string{"project", "migration", "phase"}, nil)
	descProgress = prometheus.NewDesc("xferdb_migration_progress_ratio",
		"Rows transferred over rows expected (0-1).", []string{"project", "migration"}, nil)
	descRowsExpected = prometheus.NewDesc("xferdb_migration_rows_expected",
		"Rows the active migration expects to transfer (source estimate until a table finishes).", []string{"project", "migration"}, nil)
	descRowsDone = prometheus.NewDesc("xferdb_migration_rows_transferred",
		"Rows the active migration has transferred so far.", []string{"project", "migration"}, nil)
	descRate = prometheus.NewDesc("xferdb_migration_rows_per_second",
		"Smoothed write throughput of the active migration.", []string{"project", "migration"}, nil)
	descETA = prometheus.NewDesc("xferdb_migration_eta_seconds",
		"Estimated seconds until the active migration finishes (0 when unknown).", []string{"project", "migration"}, nil)
	descTables = prometheus.NewDesc("xferdb_migration_tables",
		"Tables of the active migration, by state.", []string{"project", "migration", "state"}, nil)

	descLivePhase = prometheus.NewDesc("xferdb_cdc_phase",
		"1 for a live migration's current phase (snapshot, catch_up, streaming).", []string{"migration", "phase"}, nil)
	descLagBytes = prometheus.NewDesc("xferdb_cdc_lag_bytes",
		"How far a live migration's capture is behind the source's log (PostgreSQL: WAL bytes).", []string{"migration"}, nil)
	descLagChanges = prometheus.NewDesc("xferdb_cdc_lag_changes",
		"Changes a live migration has queued but not yet applied to the target.", []string{"migration"}, nil)
	descLagSeconds = prometheus.NewDesc("xferdb_cdc_lag_seconds",
		"Age of the oldest change not yet applied to the target (absent when unknown).", []string{"migration"}, nil)
	descQueued = prometheus.NewDesc("xferdb_cdc_changes_queued_total",
		"Changes a live migration's capture worker has queued since it started.", []string{"migration"}, nil)
	descApplied = prometheus.NewDesc("xferdb_cdc_changes_applied_total",
		"Changes a live migration's apply worker has applied since it started.", []string{"migration"}, nil)
	descQueueBytes = prometheus.NewDesc("xferdb_cdc_queue_bytes",
		"Size of a live migration's change queue on the server's disk.", []string{"migration"}, nil)
	descRetained = prometheus.NewDesc("xferdb_cdc_source_retained_bytes",
		"Log the source keeps for a live migration's stream (PostgreSQL: WAL held by its replication slot).", []string{"migration"}, nil)
	descSchemaChange = prometheus.NewDesc("xferdb_cdc_schema_change_paused",
		"1 while a live migration is paused at a schema change on the source.", []string{"migration"}, nil)
)

// gaugeCollector reads the active migrations' snapshots at scrape time.
type gaugeCollector struct{ m *Metrics }

func (g gaugeCollector) Describe(ch chan<- *prometheus.Desc) {
	for _, d := range []*prometheus.Desc{descActive, descPhase, descProgress, descRowsExpected, descRowsDone, descRate, descETA, descTables,
		descLivePhase, descLagBytes, descLagChanges, descLagSeconds, descQueued, descApplied, descQueueBytes,
		descRetained, descSchemaChange} {
		ch <- d
	}
}

func (g gaugeCollector) Collect(ch chan<- prometheus.Metric) {
	g.m.mu.Lock()
	runs := make([]*Run, 0, len(g.m.runs))
	for _, r := range g.m.runs {
		runs = append(runs, r)
	}
	liveStreams := g.m.live
	g.m.mu.Unlock()

	if liveStreams != nil {
		for id, st := range liveStreams() {
			gauge := func(d *prometheus.Desc, t prometheus.ValueType, v float64, extra ...string) {
				ch <- prometheus.MustNewConstMetric(d, t, v, append([]string{id}, extra...)...)
			}
			if st.Phase != "" {
				gauge(descLivePhase, prometheus.GaugeValue, 1, st.Phase)
			}
			gauge(descLagBytes, prometheus.GaugeValue, float64(st.LagBytes))
			gauge(descLagChanges, prometheus.GaugeValue, float64(st.LagChanges))
			if st.LagSeconds != nil {
				gauge(descLagSeconds, prometheus.GaugeValue, *st.LagSeconds)
			}
			gauge(descQueued, prometheus.CounterValue, float64(st.ChangesQueued))
			gauge(descApplied, prometheus.CounterValue, float64(st.ChangesApplied))
			gauge(descQueueBytes, prometheus.GaugeValue, float64(st.QueueBytes))
			if st.RetainedBytes != nil {
				gauge(descRetained, prometheus.GaugeValue, float64(*st.RetainedBytes))
			}
			paused := 0.0
			if st.SchemaChange {
				paused = 1
			}
			gauge(descSchemaChange, prometheus.GaugeValue, paused)
		}
	}

	active := make(map[string]int, len(activePhases))
	for _, p := range activePhases {
		active[p] = 0
	}
	for _, r := range runs {
		s := r.col.Snapshot()
		// Done runs after the terminal event, so a finished migration can
		// be scraped once more in between; leave it out.
		if _, ok := active[s.Phase]; !ok {
			continue
		}
		active[s.Phase]++
		gauge := func(d *prometheus.Desc, v float64, extra ...string) {
			ch <- prometheus.MustNewConstMetric(d, prometheus.GaugeValue, v, append([]string{r.project, r.migrationID}, extra...)...)
		}
		gauge(descPhase, 1, s.Phase)
		var ratio float64
		if s.Rows.Total > 0 {
			ratio = float64(s.Rows.Transferred) / float64(s.Rows.Total)
		}
		gauge(descProgress, ratio)
		gauge(descRowsExpected, float64(s.Rows.Total))
		gauge(descRowsDone, float64(s.Rows.Transferred))
		gauge(descRate, s.Rows.RatePerSecond)
		gauge(descETA, s.ETASeconds)
		gauge(descTables, float64(s.Tables.Pending), "pending")
		gauge(descTables, float64(s.Tables.InProgress), "in_progress")
		gauge(descTables, float64(s.Tables.Completed), "completed")
		gauge(descTables, float64(s.Tables.Failed), "failed")
		gauge(descTables, float64(s.Tables.Cancelled), "cancelled")
	}
	for _, p := range activePhases {
		ch <- prometheus.MustNewConstMetric(descActive, prometheus.GaugeValue, float64(active[p]), p)
	}
}
