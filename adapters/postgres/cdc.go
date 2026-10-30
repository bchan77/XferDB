package postgres

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/lib/pq"

	"gitea.homelab.local/nextdevops/XferDB/adapters"
	"gitea.homelab.local/nextdevops/XferDB/cdc"
)

// ChangeSource reads a PostgreSQL database's changes through logical
// replication with the built-in pgoutput plugin (#102): nothing is installed
// on the source, which only needs wal_level=logical. It implements
// cdc.ChangeSource.
//
// Values are decoded from pgoutput's text format into exactly what lib/pq
// returns for the same column on a SELECT (see decodeText), so a row the
// snapshot copied and a row the stream replays reach the target as the same
// Go value.
type ChangeSource struct {
	cfg  adapters.ConnectionConfig
	slot string
	pub  string

	db        *sql.DB // catalog queries, with the source's search_path
	ownSchema string  // current_schema(): its tables are named bare

	mu        sync.Mutex
	rels      map[uint32]*relation
	baseTypes map[uint32]uint32 // domain OID -> base type OID

	// acked is the highest position the caller has stored durably (Ack).
	// sent is the Pos of the last transaction handed to out. While the two
	// are equal nothing is in flight, so between transactions the slot may
	// be confirmed up to whatever WAL has been read (idle).
	acked, sent, idle pglogrepl.LSN
}

type relation struct {
	name    string
	columns []*pglogrepl.RelationMessageColumn
	key     []string
}

// SlotName returns the replication slot and publication name used for a
// live migration: "xferdb_" + the id with anything but letters and digits
// dropped, lowercased, as slot names allow only [a-z0-9_].
func SlotName(stream string) string {
	var b strings.Builder
	b.WriteString("xferdb_")
	for _, r := range strings.ToLower(stream) {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') {
			b.WriteRune(r)
		}
	}
	s := b.String()
	if len(s) > 63 {
		s = s[:63]
	}
	return s
}

// SlotName returns this stream's slot (and publication) name.
func (s *ChangeSource) SlotName() string { return s.slot }

// NewChangeSource connects to the source for the live migration stream.
func NewChangeSource(ctx context.Context, cfg adapters.ConnectionConfig, stream string) (*ChangeSource, error) {
	db, err := sql.Open("postgres", buildDSN(cfg))
	if err != nil {
		return nil, fmt.Errorf("postgres open: %w", err)
	}
	s := &ChangeSource{
		cfg: cfg, slot: SlotName(stream), pub: SlotName(stream), db: db,
		rels: map[uint32]*relation{}, baseTypes: map[uint32]uint32{},
	}
	if err := db.QueryRowContext(ctx, `SELECT current_schema()`).Scan(&s.ownSchema); err != nil {
		db.Close()
		return nil, fmt.Errorf("postgres connect: %w", err)
	}
	return s, nil
}

// Close implements cdc.ChangeSource.
func (s *ChangeSource) Close() error { return s.db.Close() }

// replConn opens a replication connection. Its session settings match what
// lib/pq's connections use, so values are rendered the way a SELECT renders
// them: ISO dates, hex bytea, round-trippable floats, and timestamptz in UTC
// (lib/pq keeps the instant either way).
func (s *ChangeSource) replConn(ctx context.Context) (*pgconn.PgConn, error) {
	pc, err := pgconn.ParseConfig(buildDSN(s.cfg))
	if err != nil {
		return nil, fmt.Errorf("postgres replication config: %w", err)
	}
	for k, v := range map[string]string{
		"replication":        "database",
		"DateStyle":          "ISO, MDY",
		"TimeZone":           "UTC",
		"bytea_output":       "hex",
		"extra_float_digits": "3",
		"IntervalStyle":      "postgres",
		"client_encoding":    "UTF8",
	} {
		pc.RuntimeParams[k] = v
	}
	conn, err := pgconn.ConnectConfig(ctx, pc)
	if err != nil {
		return nil, fmt.Errorf("postgres replication connect: %w", err)
	}
	return conn, nil
}

// Prepare implements cdc.ChangeSource: it creates the publication for tables
// and then the slot, whose consistent point is the start position. The
// publication must exist first: pgoutput looks it up as of each change it
// decodes.
func (s *ChangeSource) Prepare(ctx context.Context, tables []string) (cdc.Position, error) {
	if len(tables) == 0 {
		return nil, errors.New("live migration: no tables")
	}
	if missing, err := s.tablesWithoutKey(ctx, tables); err != nil {
		return nil, err
	} else if len(missing) > 0 {
		return nil, fmt.Errorf("live migration needs a primary key on every table; missing on: %s", strings.Join(missing, ", "))
	}

	var exists bool
	if err := s.db.QueryRowContext(ctx, `SELECT EXISTS (SELECT 1 FROM pg_publication WHERE pubname = $1)`, s.pub).Scan(&exists); err != nil {
		return nil, fmt.Errorf("check publication: %w", err)
	}
	if !exists {
		names := make([]string, len(tables))
		for i, t := range tables {
			names[i] = qualify(t)
		}
		if _, err := s.db.ExecContext(ctx, fmt.Sprintf(`CREATE PUBLICATION %s FOR TABLE %s`, quote(s.pub), strings.Join(names, ", "))); err != nil {
			return nil, fmt.Errorf("create publication %s: %w", s.pub, err)
		}
	}

	var lsn sql.NullString
	err := s.db.QueryRowContext(ctx,
		`SELECT COALESCE(confirmed_flush_lsn, restart_lsn)::text FROM pg_replication_slots WHERE slot_name = $1`, s.slot).Scan(&lsn)
	if err == nil {
		if !lsn.Valid {
			return nil, fmt.Errorf("replication slot %s exists but has no position", s.slot)
		}
		p, err := pglogrepl.ParseLSN(lsn.String)
		if err != nil {
			return nil, err
		}
		return encodeLSN(p), nil
	}
	if err != sql.ErrNoRows {
		return nil, fmt.Errorf("check replication slot: %w", err)
	}
	conn, err := s.replConn(ctx)
	if err != nil {
		return nil, err
	}
	defer conn.Close(context.Background())
	res, err := pglogrepl.CreateReplicationSlot(ctx, conn, s.slot, "pgoutput", pglogrepl.CreateReplicationSlotOptions{
		Mode:           pglogrepl.LogicalReplication,
		SnapshotAction: "NOEXPORT_SNAPSHOT",
	})
	if err != nil {
		return nil, fmt.Errorf("create replication slot %s: %w", s.slot, err)
	}
	p, err := pglogrepl.ParseLSN(res.ConsistentPoint)
	if err != nil {
		return nil, err
	}
	return encodeLSN(p), nil
}

// tablesWithoutKey returns those of tables that have no primary key.
func (s *ChangeSource) tablesWithoutKey(ctx context.Context, tables []string) ([]string, error) {
	var missing []string
	for _, t := range tables {
		schema, table := splitName(t)
		var has bool
		err := s.db.QueryRowContext(ctx, `
			SELECT EXISTS (
				SELECT 1 FROM pg_index i
				WHERE i.indrelid = format('%I.%I', `+schemaExpr(2)+`, $1::text)::regclass AND i.indisprimary)`,
			table, schema).Scan(&has)
		if err != nil {
			return nil, fmt.Errorf("check primary key of %s: %w", t, err)
		}
		if !has {
			missing = append(missing, t)
		}
	}
	return missing, nil
}

// Stream implements cdc.ChangeSource.
func (s *ChangeSource) Stream(ctx context.Context, from cdc.Position, out chan<- cdc.Change) error {
	start, err := decodeLSN(from)
	if err != nil {
		return err
	}
	if err := s.loadBaseTypes(ctx); err != nil {
		return err
	}
	conn, err := s.replConn(ctx)
	if err != nil {
		return err
	}
	defer conn.Close(context.Background())
	err = pglogrepl.StartReplication(ctx, conn, s.slot, start, pglogrepl.StartReplicationOptions{
		Mode:       pglogrepl.LogicalReplication,
		PluginArgs: []string{"proto_version '1'", "publication_names '" + s.pub + "'"},
	})
	if err != nil {
		return fmt.Errorf("start replication on %s: %w", s.slot, err)
	}
	s.mu.Lock()
	if s.acked < start {
		s.acked = start
	}
	s.sent, s.idle = s.acked, s.acked
	s.mu.Unlock()

	var (
		inTxn      bool
		commitAt   time.Time
		pending    *cdc.Change // held back until we know whether it ends the transaction
		lastStatus time.Time
		reported   pglogrepl.LSN
	)
	send := func(c *cdc.Change) error {
		select {
		case out <- *c:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	emit := func(c cdc.Change) error {
		c.CommitAt = commitAt
		if pending != nil {
			if err := send(pending); err != nil {
				return err
			}
		}
		pending = &c
		return nil
	}

	for {
		// Report progress at least every 10s (wal_sender_timeout is 60s
		// by default) and within a second of a new ack.
		flush := s.flushLSN()
		if time.Since(lastStatus) > 10*time.Second || (flush > reported && time.Since(lastStatus) > time.Second) {
			if err := pglogrepl.SendStandbyStatusUpdate(ctx, conn, pglogrepl.StandbyStatusUpdate{
				WALWritePosition: flush, WALFlushPosition: flush, WALApplyPosition: flush,
			}); err != nil {
				return fmt.Errorf("send standby status: %w", err)
			}
			lastStatus, reported = time.Now(), flush
		}

		rctx, cancel := context.WithTimeout(ctx, time.Second)
		raw, err := conn.ReceiveMessage(rctx)
		cancel()
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			if pgconn.Timeout(err) {
				continue
			}
			return fmt.Errorf("receive replication message: %w", err)
		}
		switch msg := raw.(type) {
		case *pgproto3.ErrorResponse:
			return fmt.Errorf("replication: %s", pgconn.ErrorResponseToPgError(msg).Error())
		case *pgproto3.CopyData:
			if len(msg.Data) == 0 {
				continue
			}
			switch msg.Data[0] {
			case pglogrepl.PrimaryKeepaliveMessageByteID:
				ka, err := pglogrepl.ParsePrimaryKeepaliveMessage(msg.Data[1:])
				if err != nil {
					return err
				}
				if !inTxn {
					s.noteIdle(ka.ServerWALEnd)
				}
				if ka.ReplyRequested {
					lastStatus = time.Time{}
				}
			case pglogrepl.XLogDataByteID:
				xld, err := pglogrepl.ParseXLogData(msg.Data[1:])
				if err != nil {
					return err
				}
				lm, err := pglogrepl.Parse(xld.WALData)
				if err != nil {
					return fmt.Errorf("parse pgoutput message: %w", err)
				}
				switch m := lm.(type) {
				case *pglogrepl.RelationMessage:
					if err := s.addRelation(ctx, m); err != nil {
						return err
					}
				case *pglogrepl.BeginMessage:
					inTxn, commitAt, pending = true, m.CommitTime.UTC(), nil
				case *pglogrepl.CommitMessage:
					inTxn = false
					end := m.TransactionEndLSN
					if pending == nil {
						// Nothing published in this transaction.
						s.noteIdle(end)
						continue
					}
					pending.Pos = encodeLSN(end)
					s.mu.Lock()
					s.sent = end
					s.mu.Unlock()
					if err := send(pending); err != nil {
						return err
					}
					pending = nil
				case *pglogrepl.InsertMessage:
					c, err := s.rowChange(m.RelationID, cdc.OpInsert, nil, m.Tuple)
					if err != nil {
						return err
					}
					if err := emit(c); err != nil {
						return err
					}
				case *pglogrepl.UpdateMessage:
					c, err := s.rowChange(m.RelationID, cdc.OpUpdate, m.OldTuple, m.NewTuple)
					if err != nil {
						return err
					}
					if err := emit(c); err != nil {
						return err
					}
				case *pglogrepl.DeleteMessage:
					c, err := s.rowChange(m.RelationID, cdc.OpDelete, m.OldTuple, nil)
					if err != nil {
						return err
					}
					if err := emit(c); err != nil {
						return err
					}
				case *pglogrepl.TruncateMessage:
					for _, id := range m.RelationIDs {
						rel, err := s.relation(id)
						if err != nil {
							return err
						}
						if err := emit(cdc.Change{Table: rel.name, Op: cdc.OpTruncate}); err != nil {
							return err
						}
					}
				}
			}
		default:
			return fmt.Errorf("unexpected replication message %T", raw)
		}
	}
}

// noteIdle records WAL read up to lsn with nothing of ours in it, which may
// be confirmed once everything sent before it is acked.
func (s *ChangeSource) noteIdle(lsn pglogrepl.LSN) {
	s.mu.Lock()
	if lsn > s.idle {
		s.idle = lsn
	}
	s.mu.Unlock()
}

// flushLSN is the position to confirm to the server: what the caller acked,
// or further, read WAL that held none of our changes, when nothing sent is
// still waiting for its ack.
func (s *ChangeSource) flushLSN() pglogrepl.LSN {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.acked >= s.sent && s.idle > s.acked {
		return s.idle
	}
	return s.acked
}

// Ack implements cdc.ChangeSource. Stream reports it to the server.
func (s *ChangeSource) Ack(_ context.Context, upTo cdc.Position) error {
	lsn, err := decodeLSN(upTo)
	if err != nil {
		return err
	}
	s.mu.Lock()
	if lsn > s.acked {
		s.acked = lsn
	}
	s.mu.Unlock()
	return nil
}

// CurrentPosition implements cdc.ChangeSource.
func (s *ChangeSource) CurrentPosition(ctx context.Context) (cdc.Position, error) {
	var text string
	if err := s.db.QueryRowContext(ctx, `SELECT pg_current_wal_lsn()::text`).Scan(&text); err != nil {
		return nil, fmt.Errorf("current WAL position: %w", err)
	}
	lsn, err := pglogrepl.ParseLSN(text)
	if err != nil {
		return nil, err
	}
	return encodeLSN(lsn), nil
}

// Sequences implements cdc.ChangeSource, naming sequences like tables.
func (s *ChangeSource) Sequences(ctx context.Context) (map[string]int64, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT CASE WHEN schemaname = current_schema() THEN sequencename
		            ELSE schemaname || '.' || sequencename END, last_value
		FROM pg_sequences
		WHERE last_value IS NOT NULL
		  AND schemaname NOT IN ('pg_catalog', 'information_schema')`)
	if err != nil {
		return nil, fmt.Errorf("read sequences: %w", err)
	}
	defer rows.Close()
	out := map[string]int64{}
	for rows.Next() {
		var name string
		var v int64
		if err := rows.Scan(&name, &v); err != nil {
			return nil, err
		}
		out[name] = v
	}
	return out, rows.Err()
}

// Teardown implements cdc.ChangeSource. The slot must not be streaming:
// once its stream has stopped, the server may still hold it as active for
// a moment, which is waited out, up to 10s.
func (s *ChangeSource) Teardown(ctx context.Context) error {
	for wait := 0; ; wait++ {
		_, err := s.db.ExecContext(ctx,
			`SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name = $1`, s.slot)
		if err == nil {
			break
		}
		var pqErr *pq.Error
		if !errors.As(err, &pqErr) || pqErr.Code != "55006" || wait >= 50 { // object_in_use
			return fmt.Errorf("drop replication slot %s: %w", s.slot, err)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(200 * time.Millisecond):
		}
	}
	if _, err := s.db.ExecContext(ctx, `DROP PUBLICATION IF EXISTS `+quote(s.pub)); err != nil {
		return fmt.Errorf("drop publication %s: %w", s.pub, err)
	}
	return nil
}

// FormatPosition implements cdc.ChangeSource: an LSN like 0/16B3748.
func (s *ChangeSource) FormatPosition(p cdc.Position) string {
	lsn, err := decodeLSN(p)
	if err != nil {
		return fmt.Sprintf("invalid(%x)", []byte(p))
	}
	return lsn.String()
}

// ParsePosition implements cdc.ChangeSource.
func (s *ChangeSource) ParsePosition(text string) (cdc.Position, error) {
	lsn, err := pglogrepl.ParseLSN(text)
	if err != nil {
		return nil, err
	}
	return encodeLSN(lsn), nil
}

// Confirmed implements cdc.ChangeSource: the position Stream reports to the
// server as flushed.
func (s *ChangeSource) Confirmed() cdc.Position {
	return encodeLSN(s.flushLSN())
}

// Behind implements cdc.ChangeSource, in WAL bytes.
func (s *ChangeSource) Behind(pos, target cdc.Position) int64 {
	p, err1 := decodeLSN(pos)
	t, err2 := decodeLSN(target)
	if err1 != nil || err2 != nil || p >= t {
		return 0
	}
	return int64(t - p)
}

func encodeLSN(l pglogrepl.LSN) cdc.Position {
	return binary.BigEndian.AppendUint64(nil, uint64(l))
}

func decodeLSN(p cdc.Position) (pglogrepl.LSN, error) {
	if len(p) == 0 {
		return 0, nil
	}
	if len(p) != 8 {
		return 0, fmt.Errorf("not a PostgreSQL position: %x", []byte(p))
	}
	return pglogrepl.LSN(binary.BigEndian.Uint64(p)), nil
}

// addRelation caches a table's columns as pgoutput describes them. It is
// sent before the first change to a table and again after its schema
// changes.
func (s *ChangeSource) addRelation(ctx context.Context, m *pglogrepl.RelationMessage) error {
	name := m.RelationName
	if m.Namespace != s.ownSchema {
		name = m.Namespace + "." + m.RelationName
	}
	key, err := s.primaryKey(ctx, m.RelationID)
	if err != nil {
		return err
	}
	for _, c := range m.Columns {
		if _, known := s.baseTypes[c.DataType]; !known && c.DataType >= 16384 {
			// A type created after Stream started, maybe a new domain.
			if err := s.loadBaseTypes(ctx); err != nil {
				return err
			}
			break
		}
	}
	s.mu.Lock()
	s.rels[m.RelationID] = &relation{name: name, columns: m.Columns, key: key}
	s.mu.Unlock()
	return nil
}

func (s *ChangeSource) relation(id uint32) (*relation, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	rel, ok := s.rels[id]
	if !ok {
		return nil, fmt.Errorf("change to relation %d before its description", id)
	}
	return rel, nil
}

// primaryKey returns a table's primary key columns. pgoutput flags the
// replica identity's columns, which is every column under REPLICA IDENTITY
// FULL, so the key is read from the catalog instead.
func (s *ChangeSource) primaryKey(ctx context.Context, relid uint32) ([]string, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT a.attname
		FROM pg_index i
		JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY (i.indkey)
		WHERE i.indrelid = $1 AND i.indisprimary
		ORDER BY array_position(i.indkey, a.attnum)`, relid)
	if err != nil {
		return nil, fmt.Errorf("read primary key of relation %d: %w", relid, err)
	}
	defer rows.Close()
	var key []string
	for rows.Next() {
		var c string
		if err := rows.Scan(&c); err != nil {
			return nil, err
		}
		key = append(key, c)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if len(key) == 0 {
		return nil, fmt.Errorf("relation %d has no primary key", relid)
	}
	return key, nil
}

// loadBaseTypes maps every domain to its base type. A SELECT describes a
// domain column by its base type, so lib/pq decodes it as that; pgoutput
// sends the domain's own OID.
func (s *ChangeSource) loadBaseTypes(ctx context.Context) error {
	rows, err := s.db.QueryContext(ctx, `SELECT oid, typbasetype FROM pg_type WHERE typtype = 'd'`)
	if err != nil {
		return fmt.Errorf("read domain types: %w", err)
	}
	defer rows.Close()
	m := map[uint32]uint32{}
	for rows.Next() {
		var oid, base uint32
		if err := rows.Scan(&oid, &base); err != nil {
			return err
		}
		m[oid] = base
	}
	if err := rows.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	s.baseTypes = m
	s.mu.Unlock()
	return nil
}

func (s *ChangeSource) baseType(oid uint32) uint32 {
	s.mu.Lock()
	defer s.mu.Unlock()
	for i := 0; i < 32; i++ { // a domain over a domain
		base, ok := s.baseTypes[oid]
		if !ok {
			return oid
		}
		oid = base
	}
	return oid
}

// rowChange decodes an insert (new only), update (old optional, new) or
// delete (old only) into a Change.
func (s *ChangeSource) rowChange(relid uint32, op cdc.Op, old, new *pglogrepl.TupleData) (cdc.Change, error) {
	rel, err := s.relation(relid)
	if err != nil {
		return cdc.Change{}, err
	}
	c := cdc.Change{Table: rel.name, Op: op}
	var oldRow map[string]interface{}
	if old != nil {
		if oldRow, _, err = s.decodeTuple(rel, old); err != nil {
			return cdc.Change{}, err
		}
	}
	if new != nil {
		if c.After, c.Unchanged, err = s.decodeTuple(rel, new); err != nil {
			return cdc.Change{}, err
		}
	}
	keyFrom := c.After
	if op == cdc.OpDelete {
		keyFrom = oldRow
	}
	if keyFrom == nil {
		return cdc.Change{}, fmt.Errorf("%s change to %s without row data", op, rel.name)
	}
	c.Key = make(map[string]interface{}, len(rel.key))
	for _, k := range rel.key {
		v, ok := keyFrom[k]
		if !ok {
			return cdc.Change{}, fmt.Errorf("%s change to %s is missing key column %s", op, rel.name, k)
		}
		c.Key[k] = v
	}
	if op == cdc.OpUpdate && oldRow != nil {
		// An old tuple comes with an update only when the key changed
		// (or always, under REPLICA IDENTITY FULL): compare to tell.
		oldKey := make(map[string]interface{}, len(rel.key))
		changed := false
		for _, k := range rel.key {
			oldKey[k] = oldRow[k]
			if !sameValue(oldRow[k], c.Key[k]) {
				changed = true
			}
		}
		if changed {
			c.OldKey = oldKey
		}
	}
	return c, nil
}

// decodeTuple decodes a tuple's columns. Null columns are nil; columns
// pgoutput marks unchanged (a TOASTed value an update didn't touch) are left
// out and listed in unchanged; so are a key-only old tuple's non-key
// columns, which it sends as nulls.
func (s *ChangeSource) decodeTuple(rel *relation, t *pglogrepl.TupleData) (map[string]interface{}, []string, error) {
	if len(t.Columns) != len(rel.columns) {
		return nil, nil, fmt.Errorf("%s: tuple has %d columns, relation has %d", rel.name, len(t.Columns), len(rel.columns))
	}
	row := make(map[string]interface{}, len(t.Columns))
	var unchanged []string
	for i, col := range t.Columns {
		name := rel.columns[i].Name
		switch col.DataType {
		case pglogrepl.TupleDataTypeNull:
			row[name] = nil
		case pglogrepl.TupleDataTypeToast:
			unchanged = append(unchanged, name)
		case pglogrepl.TupleDataTypeText:
			v, err := decodeText(col.Data, s.baseType(rel.columns[i].DataType))
			if err != nil {
				return nil, nil, fmt.Errorf("%s.%s: %w", rel.name, name, err)
			}
			row[name] = v
		default:
			return nil, nil, fmt.Errorf("%s.%s: unexpected tuple data type %q", rel.name, name, col.DataType)
		}
	}
	return row, unchanged, nil
}

func sameValue(a, b interface{}) bool {
	switch x := a.(type) {
	case []byte:
		y, ok := b.([]byte)
		return ok && bytes.Equal(x, y)
	case time.Time:
		y, ok := b.(time.Time)
		return ok && x.Equal(y)
	}
	return a == b
}

// Type OIDs lib/pq decodes specially (lib/pq's oid package).
const (
	oidBool        = 16
	oidBytea       = 17
	oidChar        = 18
	oidInt8        = 20
	oidInt2        = 21
	oidInt4        = 23
	oidText        = 25
	oidFloat4      = 700
	oidFloat8      = 701
	oidBpchar      = 1042
	oidVarchar     = 1043
	oidDate        = 1082
	oidTime        = 1083
	oidTimestamp   = 1114
	oidTimestamptz = 1184
	oidTimetz      = 1266
)

// decodeText converts a value in PostgreSQL's text format to what lib/pq's
// textDecode returns for that type on a SELECT; everything else stays the
// raw text as []byte, as lib/pq leaves it. TestDecodeMatchesSnapshot checks
// the two agree type by type.
func decodeText(s []byte, typ uint32) (interface{}, error) {
	switch typ {
	case oidChar, oidBpchar, oidVarchar, oidText:
		return string(s), nil
	case oidBytea:
		return parseBytea(s)
	case oidTimestamptz, oidTimestamp, oidDate:
		str := string(s)
		if str == "infinity" || str == "-infinity" {
			return []byte(str), nil
		}
		return pq.ParseTimestamp(nil, str)
	case oidTime, oidTimetz:
		return parseClock(typ, string(s))
	case oidBool:
		return len(s) > 0 && s[0] == 't', nil
	case oidInt8, oidInt4, oidInt2:
		return strconv.ParseInt(string(s), 10, 64)
	case oidFloat4, oidFloat8:
		return strconv.ParseFloat(string(s), 64)
	}
	return append([]byte(nil), s...), nil
}

// parseBytea decodes bytea's hex output (replConn sets bytea_output=hex).
func parseBytea(s []byte) ([]byte, error) {
	if !bytes.HasPrefix(s, []byte(`\x`)) {
		return nil, fmt.Errorf("bytea not in hex format")
	}
	out := make([]byte, hex.DecodedLen(len(s)-2))
	if _, err := hex.Decode(out, s[2:]); err != nil {
		return nil, err
	}
	return out, nil
}

// parseClock parses time and timetz the way lib/pq's parseTime does: a
// time.Time on 0000-01-01, with 24:00:00 as midnight of the next day.
func parseClock(typ uint32, str string) (time.Time, error) {
	f := "15:04:05"
	if typ == oidTimetz {
		f = "15:04:05-07"
		if c := strings.Count(str, ":"); c > 3 {
			f = "15:04:05-07:00:00"
		} else if c > 2 {
			f = "15:04:05-07:00"
		}
	}
	is2400 := strings.HasPrefix(str, "24:00:00")
	if is2400 {
		str = "00:00:00" + str[8:]
	}
	t, err := time.Parse(f, str)
	if err != nil {
		return time.Time{}, err
	}
	if is2400 {
		t = t.Add(24 * time.Hour)
	}
	return t, nil
}
