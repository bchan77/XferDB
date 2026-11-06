package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"regexp"
	"sort"

	"gitea.homelab.local/nextdevops/XferDB/adapters"
)

// streamName is the form SlotName gives a live migration's slot and
// publication; only names of this form are listed or dropped.
var streamName = regexp.MustCompile(`^xferdb_[a-z0-9]+$`)

// LiveStreams implements adapters.LiveStreamLister: the slots (of this
// database), publications and YugabyteDB heartbeat rows named like a live
// migration's.
func (s *Source) LiveStreams(ctx context.Context) ([]adapters.LiveStream, error) {
	yb := s.config.Type == adapters.TypeYugabyte
	found := map[string]*adapters.LiveStream{}
	get := func(name string) *adapters.LiveStream {
		if found[name] == nil {
			found[name] = &adapters.LiveStream{Name: name}
		}
		return found[name]
	}

	retained := `COALESCE(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn), 0)::bigint`
	if yb {
		retained = `NULL::bigint` // no WAL
	}
	rows, err := s.db.QueryContext(ctx, `
		SELECT slot_name::text, active, `+retained+`
		FROM pg_replication_slots
		WHERE slot_name LIKE 'xferdb\_%' AND database = current_database()`)
	if err != nil {
		return nil, fmt.Errorf("list replication slots: %w", err)
	}
	for rows.Next() {
		var name string
		var active bool
		var bytes sql.NullInt64
		if err := rows.Scan(&name, &active, &bytes); err != nil {
			rows.Close()
			return nil, err
		}
		if !streamName.MatchString(name) {
			continue
		}
		st := get(name)
		st.Slot, st.Active = true, active
		if bytes.Valid {
			st.RetainedBytes = &bytes.Int64
		}
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}

	names, err := s.stringColumn(ctx, `SELECT pubname::text FROM pg_publication WHERE pubname LIKE 'xferdb\_%'`)
	if err != nil {
		return nil, fmt.Errorf("list publications: %w", err)
	}
	for _, name := range names {
		if streamName.MatchString(name) {
			get(name).Publication = true
		}
	}

	var hasHeartbeat bool
	if err := s.db.QueryRowContext(ctx, `SELECT to_regclass($1) IS NOT NULL`, quote(heartbeatTable)).Scan(&hasHeartbeat); err != nil {
		return nil, err
	}
	if hasHeartbeat {
		names, err := s.stringColumn(ctx, `SELECT stream FROM `+quote(heartbeatTable))
		if err != nil {
			return nil, fmt.Errorf("list heartbeats: %w", err)
		}
		for _, name := range names {
			if streamName.MatchString(name) {
				get(name).Heartbeat = true
			}
		}
	}

	out := make([]adapters.LiveStream, 0, len(found))
	for _, st := range found {
		out = append(out, *st)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out, nil
}

// DropLiveStream implements adapters.LiveStreamLister: the slot, the
// publication and the heartbeat, as a migration's own teardown drops them.
// An active slot is refused: something still streams from it.
func (s *Source) DropLiveStream(ctx context.Context, name string) error {
	if !streamName.MatchString(name) {
		return fmt.Errorf("%q isn't a live migration's stream name", name)
	}
	var active sql.NullBool
	err := s.db.QueryRowContext(ctx, `SELECT active FROM pg_replication_slots WHERE slot_name = $1`, name).Scan(&active)
	if err != nil && err != sql.ErrNoRows {
		return fmt.Errorf("check replication slot %s: %w", name, err)
	}
	if active.Bool {
		return fmt.Errorf("replication slot %s is in use: something is streaming from it", name)
	}
	cs := &ChangeSource{cfg: s.config, slot: name, pub: name, db: s.db, yb: s.config.Type == adapters.TypeYugabyte}
	return cs.Teardown(ctx)
}

func (s *Source) stringColumn(ctx context.Context, q string) ([]string, error) {
	rows, err := s.db.QueryContext(ctx, q)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var v string
		if err := rows.Scan(&v); err != nil {
			return nil, err
		}
		out = append(out, v)
	}
	return out, rows.Err()
}
