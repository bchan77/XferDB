package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/jackc/pglogrepl"
	"github.com/lib/pq"

	"gitea.homelab.local/nextdevops/XferDB/cdc"
)

// YugabyteDB's logical replication (#158) differs from PostgreSQL's in what
// a live migration relies on:
//
//   - Positions. A slot's LSNs aren't WAL positions: by default they are a
//     per-slot counter, which nothing outside the stream can be compared
//     with. XferDB creates its slots with yb_lsn_type HYBRID_TIME instead,
//     where a commit's LSN is its hybrid time, and yb_get_current_hybrid_time_lsn()
//     is the position now on the same scale.
//   - An idle stream doesn't move. PostgreSQL's keepalives carry the end of
//     WAL, so a stream with nothing to send still confirms a position
//     recorded earlier; YugabyteDB's stay at the last commit sent. So a
//     position is recorded and then a heartbeat committed after it, an
//     update of this stream's row in heartbeatTable, which is in the
//     publication: the stream sends that commit, past the position, and
//     the capture drops its change and confirms past it as for any
//     transaction without changes of ours.
//   - No WAL. A stream keeps changes for the tservers'
//     cdc_intent_retention_ms; there is no size to report.
//   - A slot stays active for ysql_cdc_active_replication_slot_window_ms
//     (60s by default) after its stream stops, and can't be dropped until
//     then.

// heartbeatTable is the table the heartbeats go to, in the connection's own
// schema. Listing the source's tables leaves it out.
const heartbeatTable = "xferdb_heartbeat"

// ybSlotDropWait is how long Teardown waits for a stopped stream's slot to
// stop being active on YugabyteDB.
const ybSlotDropWait = 75

// PositionsAreTimes implements cdc.TimePositions on YugabyteDB: positions
// are hybrid times, and Behind is in microseconds.
func (s *ChangeSource) PositionsAreTimes() bool { return s.yb }

// prepareHeartbeat creates the heartbeat table and this stream's row in it.
// A new YugabyteDB table has replica identity CHANGE, which pgoutput can't
// stream, so it is set to DEFAULT.
func (s *ChangeSource) prepareHeartbeat(ctx context.Context) error {
	for _, q := range []string{
		`CREATE TABLE IF NOT EXISTS ` + quote(heartbeatTable) + ` (stream text PRIMARY KEY, beat bigint NOT NULL, at timestamptz NOT NULL)`,
		`ALTER TABLE ` + quote(heartbeatTable) + ` REPLICA IDENTITY DEFAULT`,
	} {
		if _, err := s.db.ExecContext(ctx, q); err != nil {
			return fmt.Errorf("heartbeat table %s: %w", heartbeatTable, err)
		}
	}
	_, err := s.db.ExecContext(ctx, `INSERT INTO `+quote(heartbeatTable)+` (stream, beat, at) VALUES ($1, 0, now())
		ON CONFLICT (stream) DO NOTHING`, s.slot)
	if err != nil {
		return fmt.Errorf("heartbeat row: %w", err)
	}
	return nil
}

// createHybridTimeSlot creates the slot with hybrid-time LSNs, which the
// replication protocol's CREATE_REPLICATION_SLOT can't ask for.
func (s *ChangeSource) createHybridTimeSlot(ctx context.Context) (cdc.Position, error) {
	var text string
	if err := s.db.QueryRowContext(ctx,
		`SELECT lsn::text FROM pg_create_logical_replication_slot($1, 'pgoutput', false, false, 'HYBRID_TIME')`, s.slot).Scan(&text); err != nil {
		return nil, fmt.Errorf("create replication slot %s: %w", s.slot, err)
	}
	lsn, err := pglogrepl.ParseLSN(text)
	if err != nil {
		return nil, err
	}
	return encodeLSN(lsn), nil
}

// ybCurrentPosition records the hybrid time now, then commits a heartbeat
// after it, so the stream reaches the position even with no other writes.
func (s *ChangeSource) ybCurrentPosition(ctx context.Context) (cdc.Position, error) {
	var now int64
	if err := s.db.QueryRowContext(ctx, `SELECT yb_get_current_hybrid_time_lsn()`).Scan(&now); err != nil {
		return nil, fmt.Errorf("current hybrid time: %w", err)
	}
	res, err := s.db.ExecContext(ctx, `UPDATE `+quote(heartbeatTable)+` SET beat = beat + 1, at = now() WHERE stream = $1`, s.slot)
	if err == nil {
		if n, _ := res.RowsAffected(); n == 0 {
			err = fmt.Errorf("no row for stream %s", s.slot)
		}
	}
	if err != nil {
		return nil, fmt.Errorf("heartbeat: %w", err)
	}
	return encodeLSN(pglogrepl.LSN(now)), nil
}

// ybRetention reports only whether the slot is still there: YugabyteDB
// keeps no WAL to measure.
func (s *ChangeSource) ybRetention(ctx context.Context) (cdc.Retention, error) {
	var r cdc.Retention
	var one int
	err := s.db.QueryRowContext(ctx, `SELECT 1 FROM pg_replication_slots WHERE slot_name = $1`, s.slot).Scan(&one)
	if err == sql.ErrNoRows {
		r.State, r.Lost = "missing", true
		return r, nil
	}
	if err != nil {
		return r, fmt.Errorf("read replication slot %s: %w", s.slot, err)
	}
	return r, nil
}

// deleteHeartbeat removes this stream's heartbeat row.
func (s *ChangeSource) deleteHeartbeat(ctx context.Context) error {
	var exists bool
	if err := s.db.QueryRowContext(ctx, `SELECT to_regclass($1) IS NOT NULL`, quote(heartbeatTable)).Scan(&exists); err != nil || !exists {
		return err
	}
	if _, err := s.db.ExecContext(ctx, `DELETE FROM `+quote(heartbeatTable)+` WHERE stream = $1`, s.slot); err != nil {
		return fmt.Errorf("delete heartbeat row: %w", err)
	}
	return nil
}

// dropHeartbeatTable drops the heartbeat table once no stream has a row in
// it. It must come after the publication is dropped: YugabyteDB doesn't
// drop a table that is part of one, and another stream's publication
// keeps it until that stream ends (and drops it then).
func (s *ChangeSource) dropHeartbeatTable(ctx context.Context) error {
	var exists bool
	if err := s.db.QueryRowContext(ctx, `SELECT to_regclass($1) IS NOT NULL`, quote(heartbeatTable)).Scan(&exists); err != nil || !exists {
		return err
	}
	var rows bool
	if err := s.db.QueryRowContext(ctx, `SELECT EXISTS (SELECT 1 FROM `+quote(heartbeatTable)+`)`).Scan(&rows); err != nil || rows {
		return err
	}
	if _, err := s.db.ExecContext(ctx, `DROP TABLE IF EXISTS `+quote(heartbeatTable)); err != nil {
		var pqErr *pq.Error
		if errors.As(err, &pqErr) && pqErr.Code == "55006" {
			return nil // still in another stream's publication
		}
		return fmt.Errorf("drop heartbeat table: %w", err)
	}
	return nil
}

// ybBehind is how far pos is behind target in microseconds: a hybrid time
// is the physical time in microseconds shifted left 12 bits, with a logical
// counter in those bits. Anything behind counts as at least 1.
func ybBehind(pos, target pglogrepl.LSN) int64 {
	if pos >= target {
		return 0
	}
	if d := int64((target - pos) >> 12); d > 0 {
		return d
	}
	return 1
}

// isHeartbeat reports whether relid is the heartbeat table.
func (s *ChangeSource) isHeartbeat(relid uint32) bool {
	if !s.yb {
		return false
	}
	rel, err := s.relation(relid)
	return err == nil && rel.heartbeat
}

// ybSequences reads each sequence's value from the sequence itself:
// YugabyteDB's pg_sequences.last_value is always NULL. Its last_value is
// the end of the last block of values handed to a session (cache_size), so
// it may be past what the source has used: the target then continues past
// every value the source could have handed out, with a gap.
func (s *ChangeSource) ybSequences(ctx context.Context) (map[string]int64, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT CASE WHEN schemaname = current_schema() THEN sequencename
		            ELSE schemaname || '.' || sequencename END
		FROM pg_sequences
		WHERE schemaname NOT IN ('pg_catalog', 'information_schema')`)
	if err != nil {
		return nil, fmt.Errorf("read sequences: %w", err)
	}
	var names []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			rows.Close()
			return nil, err
		}
		names = append(names, name)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}
	out := map[string]int64{}
	for _, name := range names {
		var v int64
		var called bool
		if err := s.db.QueryRowContext(ctx, `SELECT last_value, is_called FROM `+qualify(name)).Scan(&v, &called); err != nil {
			return nil, fmt.Errorf("read sequence %s: %w", name, err)
		}
		if called { // else no value handed out yet, as pg_sequences' NULL
			out[name] = v
		}
	}
	return out, nil
}
