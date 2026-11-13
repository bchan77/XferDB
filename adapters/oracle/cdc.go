package oracle

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math/big"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/sijms/go-ora/v2/network"

	"gitea.homelab.local/nextdevops/XferDB/adapters"
	"gitea.homelab.local/nextdevops/XferDB/cdc"
)

// ChangeSource streams an Oracle schema's changes through LogMiner (#195).
// It implements cdc.ChangeSource.
//
// LogMiner runs in the source session (DBMS_LOGMNR, in every edition), from
// inside the pluggable database: START_LOGMNR(STARTSCN, ENDSCN) finds the
// redo itself (ADD_LOGFILE is refused in a PDB). 23ai mines the online redo;
// 21c's per-PDB mining sees archived logs only, so there the stream is as
// far behind as archive_lag_target lets the current log stay open. Nothing
// is created on the source.
//
// The stream only learns from the redo which rows a transaction touched:
// MINE_VALUE gives each change's primary key, before (UNDO_VALUE) and after
// (REDO_VALUE). A change to a LOB stored out of line logs no row change,
// only LOB operations without a ROWID; the SEL_LOB_LOCATOR that starts them
// names the row by its key in SQL_REDO (lobLocatorKey), the one place
// SQL_REDO is parsed. When the transaction commits,
// every row it touched is read back as of its commit SCN (a flashback
// query), through the same conversion ReadBatch uses, so a streamed row
// reaches the target exactly as a copied one (cdc.Change's contract): a row
// found is sent whole, a key not found is a delete. That covers inserts,
// updates, deletes, key changes and partial rollbacks alike. When the undo for that SCN is gone (ORA-01555 and
// friends), or the table's definition changed since, the row is read as it
// is now instead; every later change is replayed too, so the target still
// converges.
//
// Transactions are collected by XID and sent whole at COMMIT, in commit
// order; a rollback drops one. Positions say where to restart mining (the
// first change of the oldest transaction still open) and which commit was
// sent last, so a restart re-reads open transactions from their start and
// skips what was already sent.
type ChangeSource struct {
	src    *Source
	stream string

	names   []string             // the migrated tables (SetTables); empty: all
	tables  map[string]*liveInfo // Oracle table name -> what the stream needs
	objects map[string]string    // object ids of their tables and partitions -> reported name

	mu                sync.Mutex
	acked, sent, idle position
	archivedOnly      bool
	samples           []scnSample // the current SCN as polled, for flushed

	poll   time.Duration // wait between mining windows once caught up
	window uint64        // most SCNs mined in one window
}

// liveInfo is a migrated table as the stream reads it.
type liveInfo struct {
	name   string // reported name
	otable string // Oracle name
	cols   []column
	key    []column
}

// NewChangeSource connects to the source for a live migration stream.
func NewChangeSource(ctx context.Context, cfg adapters.ConnectionConfig, stream string) (*ChangeSource, error) {
	src := &Source{}
	if err := src.Connect(ctx, cfg); err != nil {
		return nil, err
	}
	return &ChangeSource{src: src, stream: stream, poll: time.Second, window: 500000}, nil
}

// SetTables tells the stream which tables are migrated; the capture worker
// calls it before Stream.
func (c *ChangeSource) SetTables(names []string) error {
	c.names = append([]string(nil), names...)
	return nil
}

// Close implements cdc.ChangeSource.
func (c *ChangeSource) Close() error { return c.src.Close() }

// position is where a stream is: mining restarts at Restart, and every
// transaction committed up to (Commit, XID) has been sent. At is Commit's
// time by the source's clock, in Unix microseconds, for lag.
type position struct {
	Restart uint64
	Commit  uint64
	XID     string
	At      int64
}

func (p position) zero() bool { return p.Commit == 0 && p.Restart == 0 }

// less orders positions by what has been sent: commit SCN, then XID (two
// transactions can commit at one SCN).
func (p position) less(q position) bool {
	if p.Commit != q.Commit {
		return p.Commit < q.Commit
	}
	return p.XID < q.XID
}

func (p position) encode() cdc.Position {
	return cdc.Position(fmt.Sprintf("%d/%d/%s/%d", p.Restart, p.Commit, p.XID, p.At))
}

func decodePosition(b cdc.Position) (position, error) {
	if len(b) == 0 {
		return position{}, nil
	}
	parts := strings.Split(string(b), "/")
	if len(parts) != 4 {
		return position{}, fmt.Errorf("oracle position %q: want restart/commit/xid/time", string(b))
	}
	var p position
	var err error
	if p.Restart, err = strconv.ParseUint(parts[0], 10, 64); err != nil {
		return position{}, fmt.Errorf("oracle position %q: %w", string(b), err)
	}
	if p.Commit, err = strconv.ParseUint(parts[1], 10, 64); err != nil {
		return position{}, fmt.Errorf("oracle position %q: %w", string(b), err)
	}
	p.XID = parts[2]
	if p.At, err = strconv.ParseInt(parts[3], 10, 64); err != nil {
		return position{}, fmt.Errorf("oracle position %q: %w", string(b), err)
	}
	return p, nil
}

// FormatPosition implements cdc.ChangeSource.
func (c *ChangeSource) FormatPosition(p cdc.Position) string { return string(p) }

// ParsePosition implements cdc.ChangeSource.
func (c *ChangeSource) ParsePosition(s string) (cdc.Position, error) {
	if _, err := decodePosition(cdc.Position(s)); err != nil {
		return nil, err
	}
	return cdc.Position(s), nil
}

// PositionsAreTimes implements cdc.TimePositions: lag is reported in time.
func (c *ChangeSource) PositionsAreTimes() bool { return true }

// Behind implements cdc.ChangeSource, in microseconds of the source's clock.
func (c *ChangeSource) Behind(pos, target cdc.Position) int64 {
	p, err1 := decodePosition(pos)
	t, err2 := decodePosition(target)
	if err1 != nil || err2 != nil || !p.less(t) || p.Commit >= t.Commit {
		return 0
	}
	if d := t.At - p.At; d > 0 {
		return d
	}
	return 1
}

// sourceTime reads a DATE or TIMESTAMP the source formatted with
// dateFormat as Unix microseconds. Oracle's SYSDATE and LogMiner's
// TIMESTAMP are both the server's local time without a zone: read alike,
// their differences are right, which is all lag needs.
func sourceTime(s string) int64 {
	t, err := time.ParseInLocation("2006-01-02 15:04:05", strings.TrimSpace(s), time.UTC)
	if err != nil {
		return 0
	}
	return t.UnixMicro()
}

// now returns the source's current SCN and time.
func now(ctx context.Context, q interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}) (uint64, int64, error) {
	var scn, at string
	err := q.QueryRowContext(ctx, `SELECT TO_CHAR(current_scn), TO_CHAR(SYSDATE, '`+dateFormat+`') FROM v$database`).Scan(&scn, &at)
	if err != nil {
		return 0, 0, fmt.Errorf("current SCN: %w", err)
	}
	n, err := strconv.ParseUint(scn, 10, 64)
	return n, sourceTime(at), err
}

// Prepare implements cdc.ChangeSource: the stream starts at the current
// SCN, and mining at the start of the oldest open transaction, whose
// changes before that SCN the snapshot can't see yet.
func (c *ChangeSource) Prepare(ctx context.Context, _ []string) (cdc.Position, error) {
	scn, at, err := now(ctx, c.src.db)
	if err != nil {
		return nil, err
	}
	restart := scn
	var oldest sql.NullString
	if err := c.src.db.QueryRowContext(ctx, `SELECT TO_CHAR(MIN(start_scn)) FROM v$transaction`).Scan(&oldest); err != nil {
		return nil, fmt.Errorf("open transactions (needs SELECT on V_$TRANSACTION): %w", err)
	}
	if oldest.Valid {
		if n, err := strconv.ParseUint(oldest.String, 10, 64); err == nil && n < restart {
			restart = n
		}
	}
	return position{Restart: restart, Commit: scn, At: at}.encode(), nil
}

// CurrentPosition implements cdc.ChangeSource.
func (c *ChangeSource) CurrentPosition(ctx context.Context) (cdc.Position, error) {
	scn, at, err := now(ctx, c.src.db)
	if err != nil {
		return nil, err
	}
	return position{Restart: scn, Commit: scn, At: at}.encode(), nil
}

// Ack implements cdc.ChangeSource. Oracle keeps nothing for the stream;
// archived logs are the DBA's to keep or delete.
func (c *ChangeSource) Ack(_ context.Context, upTo cdc.Position) error {
	p, err := decodePosition(upTo)
	if err != nil {
		return err
	}
	c.mu.Lock()
	if c.acked.less(p) {
		c.acked = p
	}
	c.mu.Unlock()
	return nil
}

// Confirmed implements cdc.ChangeSource: what was acked, or how far the
// stream has mined when everything sent is acked.
func (c *ChangeSource) Confirmed() cdc.Position {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.acked.less(c.sent) && c.acked.less(c.idle) {
		return c.idle.encode()
	}
	if c.acked.zero() {
		return nil
	}
	return c.acked.encode()
}

func (c *ChangeSource) noteSent(p position) {
	c.mu.Lock()
	if c.sent.less(p) {
		c.sent = p
	}
	c.mu.Unlock()
}

func (c *ChangeSource) noteIdle(p position) {
	c.mu.Lock()
	if c.idle.less(p) {
		c.idle = p
	}
	c.mu.Unlock()
}

// Teardown implements cdc.ChangeSource: nothing was created on the source.
func (c *ChangeSource) Teardown(context.Context) error { return nil }

// Columns implements cdc.ChangeSource, in the types XferDB reports.
func (c *ChangeSource) Columns(ctx context.Context, tables []string) (map[string][]cdc.Column, error) {
	out := make(map[string][]cdc.Column, len(tables))
	for _, t := range tables {
		cols, _, err := c.src.columns(ctx, t)
		if err != nil {
			return nil, err
		}
		out[t] = cdcColumns(cols)
	}
	return out, nil
}

func cdcColumns(cols []column) []cdc.Column {
	out := make([]cdc.Column, len(cols))
	for i, col := range cols {
		out[i] = cdc.Column{Name: col.name, Type: col.typ}
	}
	return out
}

// Sequences implements cdc.ChangeSource: each standalone sequence's last
// value handed out at most (LAST_NUMBER is past the cached block, so the
// target may skip values; it never reuses one). Identity columns'
// sequences are left out: their columns are reset by the engine (#194).
func (c *ChangeSource) Sequences(ctx context.Context) (map[string]int64, error) {
	rows, err := c.src.db.QueryContext(ctx, `
SELECT sequence_name, TO_CHAR(last_number - increment_by)
FROM all_sequences
WHERE sequence_owner = :1 AND sequence_name NOT LIKE 'ISEQ$$%' AND increment_by > 0`, c.src.owner)
	if err != nil {
		return nil, fmt.Errorf("oracle sequences: %w", err)
	}
	defer rows.Close()
	out := map[string]int64{}
	for rows.Next() {
		var name, last string
		if err := rows.Scan(&name, &last); err != nil {
			return nil, err
		}
		if n, err := strconv.ParseInt(last, 10, 64); err == nil {
			out[foldName(name)] = n
		}
	}
	return out, rows.Err()
}

// Retention implements cdc.ChangeSource; see RetentionAt.
func (c *ChangeSource) Retention(context.Context) (cdc.Retention, error) {
	return cdc.Retention{}, nil
}

// RetentionAt implements cdc.PositionRetention: the stream is lost once
// the redo still on the source (online logs and archived logs not deleted)
// no longer reaches back to the SCN mining would restart from.
func (c *ChangeSource) RetentionAt(ctx context.Context, resume cdc.Position) (cdc.Retention, error) {
	p, err := decodePosition(resume)
	if err != nil || p.zero() {
		return cdc.Retention{}, err
	}
	var oldest, from sql.NullString
	var bytes sql.NullInt64
	err = c.src.db.QueryRowContext(ctx, `
SELECT TO_CHAR(MIN(first_change#)), TO_CHAR(MIN(first_time), '`+dateFormat+`'),
       SUM(CASE WHEN next_change# > :1 THEN size_bytes END)
FROM (SELECT first_change#, next_change#, first_time, blocks * block_size size_bytes
      FROM v$archived_log WHERE deleted = 'NO' AND status = 'A'
      UNION ALL
      SELECT first_change#, next_change#, first_time, 0 FROM v$log)`, int64(p.Restart)).Scan(&oldest, &from, &bytes)
	if err != nil {
		return cdc.Retention{}, fmt.Errorf("oracle redo retention: %w", err)
	}
	ret := cdc.Retention{RetainedBytes: bytes.Int64}
	if from.Valid {
		ret.State = "redo from " + from.String
	}
	if n, err := strconv.ParseUint(oldest.String, 10, 64); err == nil && p.Restart < n {
		ret.Lost = true
	}
	return ret, nil
}

// LogMiner operation codes.
const (
	opInsert        = 1
	opDelete        = 2
	opUpdate        = 3
	opDDL           = 5
	opCommit        = 7
	opSelLobLocator = 9
	opLobWrite      = 10
	opLobTrim       = 11
	opLobErase      = 29
	opRollback      = 36
)

// minedRow is one row of V$LOGMNR_CONTENTS.
type minedRow struct {
	scn   uint64
	op    int
	xid   string
	table string // Oracle name
	rowid string
	at    int64
	info  string           // LogMiner's note: "Dictionary Mismatch" when it can't decode the row
	csf   int              // 1 when SQL_REDO goes on in the next row
	lob   string           // a SEL_LOB_LOCATOR's SQL_REDO, which names the row by its key
	redo  []sql.NullString // the key's columns after the change
	undo  []sql.NullString // and before it
}

// txn is a transaction being collected.
type txn struct {
	xid    string
	first  uint64 // SCN of its first change to a migrated table
	commit uint64
	at     int64
	keys   map[string]map[string][]string // table -> key id -> key texts
	rowids map[string]map[string]bool     // table -> ROWIDs of LOB writes
	ddl    map[string]bool                // tables whose definition it changed
	err    error                          // why its changes can't be sent, if it commits
}

// merged is each value of a, or of b where a has none.
func merged(a, b []sql.NullString) []sql.NullString {
	out := make([]sql.NullString, len(a))
	for i := range a {
		out[i] = a[i]
		if !a[i].Valid && i < len(b) {
			out[i] = b[i]
		}
	}
	return out
}

func newTxn(xid string, scn uint64) *txn {
	return &txn{xid: xid, first: scn, keys: map[string]map[string][]string{},
		rowids: map[string]map[string]bool{}, ddl: map[string]bool{}}
}

func (t *txn) pos(restart uint64) position {
	return position{Restart: restart, Commit: t.commit, XID: t.xid, At: t.at}
}

// addKey records a key the transaction touched, its texts as MINE_VALUE
// wrote them, under keyID.
func (t *txn) addKey(info *liveInfo, vals []sql.NullString) error {
	texts := make([]string, len(info.key))
	for i := range info.key {
		if i >= len(vals) || !vals[i].Valid {
			// With primary key supplemental logging every change carries
			// its key.
			return fmt.Errorf("%s: a change without its key %s; is primary key supplemental logging on?", info.name, info.key[i].name)
		}
		texts[i] = vals[i].String
	}
	m := t.keys[info.otable]
	if m == nil {
		m = map[string][]string{}
		t.keys[info.otable] = m
	}
	m[keyID(info.key, texts)] = texts
	return nil
}

// keyID identifies a key by its texts, however they were written.
func keyID(key []column, texts []string) string {
	ids := make([]string, len(texts))
	for i, s := range texts {
		ids[i] = normKey(key[i], s)
	}
	return strings.Join(ids, "\x00")
}

// normKey spells a key text one way, whichever wrote it: MINE_VALUE and
// TO_CHAR may differ in leading or trailing zeros or exponent form (an
// exact fraction then), RAW in case, CHAR in its padding.
func normKey(c column, s string) string {
	switch kindOfKey(c.dataType) {
	case keyNumber:
		if r, ok := new(big.Rat).SetString(strings.TrimSpace(s)); ok {
			if r.IsInt() {
				return r.Num().String()
			}
			return r.RatString()
		}
	case keyRaw:
		return strings.ToUpper(s)
	case keyChar:
		return strings.TrimRight(s, " ")
	}
	return s
}

// Stream implements cdc.ChangeSource.
func (c *ChangeSource) Stream(ctx context.Context, from cdc.Position, out chan<- cdc.Change) error {
	p, err := decodePosition(from)
	if err != nil {
		return err
	}
	if err := c.loadTables(ctx); err != nil {
		return err
	}
	// Each table's columns, once per session, as PostgreSQL's stream does:
	// the capturer keeps only those that differ from what it knew.
	for _, info := range c.tables {
		if err := send(ctx, out, cdc.Change{Table: info.name, Op: cdc.OpSchema, Columns: cdcColumns(info.cols)}); err != nil {
			return err
		}
	}
	conn, err := c.src.session(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()

	open := map[string]*txn{}
	sentUpTo := p // (Commit, XID) of the last transaction already sent
	mined := p.Restart - 1
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		end, endAt, err := c.endSCN(ctx, conn)
		if err != nil {
			return err
		}
		if end <= mined {
			if err := sleep(ctx, c.poll); err != nil {
				return err
			}
			continue
		}
		caughtUp := true
		if end-mined > c.window {
			end, caughtUp = mined+c.window, false
			endAt = 0
		}
		rows, covered, err := c.mine(ctx, conn, mined+1, end)
		if isOracleError(err, 1291) && !c.archived() {
			// Per-PDB mining before 23ai sees archived logs only: mine up to
			// the newest one from now on.
			c.mu.Lock()
			c.archivedOnly = true
			c.mu.Unlock()
			continue
		}
		if err != nil {
			return err
		}
		if covered < end {
			// The logs LogMiner found end before the window does: the rest
			// is still in a log it can't read yet.
			endAt, caughtUp = 0, true
		}
		// Rows at the window's last SCN are left for the next one, which
		// starts there: more can still be written at the current SCN, and
		// whether ENDSCN itself is mined doesn't matter then.
		end = covered - 1
		if !c.archived() {
			// The online log may not hold the newest redo yet: LGWR writes it
			// at each commit and at least every 3 seconds. Redo up to the
			// last COMMIT mined is on disk, and so is what was the current
			// SCN a few seconds ago; what lies past both is mined again.
			safe := c.flushed(time.Now())
			for _, r := range rows {
				if r.op == opCommit && r.scn > safe {
					safe = r.scn
				}
			}
			if safe < end {
				end, endAt, caughtUp = safe, 0, true
			}
		}
		kept := rows[:0]
		for _, r := range rows {
			if r.scn <= end {
				kept = append(kept, r)
			}
		}
		rows = kept
		if end <= mined {
			if err := sleep(ctx, c.poll); err != nil {
				return err
			}
			continue
		}
		committed, err := c.collect(rows, open)
		if err != nil {
			return err
		}
		sort.Slice(committed, func(i, j int) bool {
			return committed[i].pos(0).less(committed[j].pos(0))
		})
		for i, t := range committed {
			if !sentUpTo.less(t.pos(0)) {
				continue // sent before a restart
			}
			restart := t.commit
			for _, o := range open {
				restart = min(restart, o.first)
			}
			for _, later := range committed[i+1:] {
				restart = min(restart, later.first)
			}
			changes, err := c.changes(ctx, conn, t)
			if err != nil {
				return err
			}
			pos := t.pos(restart)
			for k := range changes {
				// A schema change never carries Pos: a transaction that only
				// changed a definition moves the stream on as idle time does.
				if k == len(changes)-1 && changes[k].Op != cdc.OpSchema {
					changes[k].Pos = pos.encode()
				}
				if err := send(ctx, out, changes[k]); err != nil {
					return err
				}
			}
			c.noteSent(pos)
			sentUpTo = pos
		}
		mined = end
		// Nothing of ours is open before here: an idle stream is confirmed
		// up to what it mined.
		restart := end + 1
		for _, o := range open {
			restart = min(restart, o.first)
		}
		if endAt == 0 {
			endAt = sentUpTo.At
		}
		c.noteIdle(position{Restart: restart, Commit: end, XID: "~", At: endAt})
		if caughtUp {
			if err := sleep(ctx, c.poll); err != nil {
				return err
			}
		}
	}
}

func send(ctx context.Context, out chan<- cdc.Change, ch cdc.Change) error {
	select {
	case out <- ch:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func sleep(ctx context.Context, d time.Duration) error {
	select {
	case <-time.After(d):
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (c *ChangeSource) archived() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.archivedOnly
}

// endSCN is how far mining can go now: the current SCN, or with archived
// logs only, the end of the newest archived log.
func (c *ChangeSource) endSCN(ctx context.Context, conn *sql.Conn) (uint64, int64, error) {
	if !c.archived() {
		scn, at, err := now(ctx, conn)
		if err == nil {
			c.sample(time.Now(), scn)
		}
		return scn, at, err
	}
	var scn, at sql.NullString
	err := conn.QueryRowContext(ctx, `
SELECT TO_CHAR(MAX(next_change#) - 1), TO_CHAR(MAX(next_time), '`+dateFormat+`')
FROM v$archived_log WHERE status = 'A' AND deleted = 'NO'`).Scan(&scn, &at)
	if err != nil {
		return 0, 0, fmt.Errorf("newest archived log: %w", err)
	}
	if !scn.Valid {
		return 0, 0, nil
	}
	n, err := strconv.ParseUint(scn.String, 10, 64)
	return n, sourceTime(at.String), err
}

// flushLag is how long redo can wait in the log buffer: LGWR writes it at
// least every 3 seconds.
const flushLag = 5 * time.Second

// sample remembers the current SCN at a time, for flushed.
func (c *ChangeSource) sample(at time.Time, scn uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.samples = append(c.samples, scnSample{at, scn})
	// Keep the newest sample old enough, and those after it.
	keep := 0
	for i, s := range c.samples {
		if at.Sub(s.at) >= flushLag {
			keep = i
		}
	}
	c.samples = c.samples[keep:]
}

// flushed is an SCN whose redo is on disk by now: the current SCN at least
// flushLag ago, or 0 when the stream hasn't run that long.
func (c *ChangeSource) flushed(now time.Time) uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	var scn uint64
	for _, s := range c.samples {
		if now.Sub(s.at) >= flushLag {
			scn = s.scn
		}
	}
	return scn
}

type scnSample struct {
	at  time.Time
	scn uint64
}

// loadTables resolves the migrated tables and their keys.
func (c *ChangeSource) loadTables(ctx context.Context) error {
	names := c.names
	if len(names) == 0 {
		all, err := c.src.refreshTables(ctx)
		if err != nil {
			return err
		}
		names = all
	}
	c.tables = map[string]*liveInfo{}
	for _, name := range names {
		cols, key, otable, err := c.src.tableKey(ctx, name)
		if err != nil {
			return err
		}
		if len(key) == 0 {
			return fmt.Errorf("%s has no primary key: a live migration needs one", name)
		}
		c.tables[otable] = &liveInfo{name: name, otable: otable, cols: cols, key: key}
	}
	c.objects = map[string]string{}
	rows, err := c.src.db.QueryContext(ctx, `
SELECT object_name, TO_CHAR(object_id) FROM all_objects
WHERE owner = :1 AND object_type IN ('TABLE', 'TABLE PARTITION', 'TABLE SUBPARTITION')`, c.src.owner)
	if err != nil {
		return fmt.Errorf("oracle object ids: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var name, id string
		if err := rows.Scan(&name, &id); err != nil {
			return err
		}
		if info := c.tables[name]; info != nil {
			c.objects[id] = info.name
		}
	}
	return rows.Err()
}

// mine reads the changes to the migrated tables from SCN from up to, not
// including, the SCN it returns, and every COMMIT and ROLLBACK, in redo
// order. That SCN is to, or less where the logs LogMiner found end sooner:
// a window can reach past them (21c's per-PDB mining finds archived logs
// only, and then mines what they hold without an error), and what lies
// beyond must be mined later.
func (c *ChangeSource) mine(ctx context.Context, conn *sql.Conn, from, to uint64) ([]minedRow, uint64, error) {
	if _, err := conn.ExecContext(ctx, `BEGIN DBMS_LOGMNR.START_LOGMNR(STARTSCN => :1, ENDSCN => :2, OPTIONS => DBMS_LOGMNR.DICT_FROM_ONLINE_CATALOG); END;`,
		int64(from), int64(to)); err != nil {
		return nil, 0, fmt.Errorf("start LogMiner at SCN %d: %w", from, err)
	}
	defer conn.ExecContext(context.WithoutCancel(ctx), `BEGIN DBMS_LOGMNR.END_LOGMNR; END;`) //nolint:errcheck
	covered := to
	var high sql.NullString
	if err := conn.QueryRowContext(ctx, `SELECT TO_CHAR(MAX(next_scn)) FROM v$logmnr_logs`).Scan(&high); err != nil {
		return nil, 0, fmt.Errorf("LogMiner's logs: %w", err)
	}
	// NEXT_SCN is the first SCN after a log, or for the current online log
	// one far past now.
	if n, err := strconv.ParseUint(high.String, 10, 64); err == nil && n > 0 && n-1 < covered {
		covered = n - 1
	}
	query, nkey := c.contentsQuery(covered - 1)
	rows, err := conn.QueryContext(ctx, query)
	if err != nil {
		return nil, 0, fmt.Errorf("read LogMiner contents: %w", err)
	}
	defer rows.Close()
	var out []minedRow
	for rows.Next() {
		var r minedRow
		var scn, xid, at string
		var table, rowid, info sql.NullString
		r.redo = make([]sql.NullString, nkey)
		r.undo = make([]sql.NullString, nkey)
		var lob sql.NullString
		dest := []interface{}{&scn, &r.op, &xid, &table, &rowid, &at, &info, &r.csf, &lob}
		for i := 0; i < nkey; i++ {
			dest = append(dest, &r.redo[i])
		}
		for i := 0; i < nkey; i++ {
			dest = append(dest, &r.undo[i])
		}
		if err := rows.Scan(dest...); err != nil {
			return nil, 0, err
		}
		if r.scn, err = strconv.ParseUint(scn, 10, 64); err != nil {
			return nil, 0, err
		}
		r.xid, r.table, r.rowid, r.at, r.info, r.lob = xid, table.String, rowid.String, sourceTime(at), info.String, lob.String
		// A statement longer than a row goes on in the next ones (CSF 1).
		if n := len(out); n > 0 && out[n-1].op == opSelLobLocator && out[n-1].csf == 1 && r.op == opSelLobLocator && r.xid == out[n-1].xid {
			out[n-1].lob += r.lob
			out[n-1].csf = r.csf
			continue
		}
		out = append(out, r)
	}
	return out, covered, rows.Err()
}

// contentsQuery selects what the stream needs from V$LOGMNR_CONTENTS:
// each change's key columns as MINE_VALUE reads them, column i of every
// table's key in the i-th pair of columns.
func (c *ChangeSource) contentsQuery(upTo uint64) (string, int) {
	nkey := 0
	var names []string
	for otable, info := range c.tables {
		nkey = max(nkey, len(info.key))
		names = append(names, otable)
	}
	sort.Strings(names)
	value := func(side string, i int) string {
		var whens []string
		for _, otable := range names {
			info := c.tables[otable]
			if i < len(info.key) {
				// Quoted, or a mixed-case name isn't found.
				path := quote(c.src.owner) + "." + quote(otable) + "." + quote(info.key[i].oracleName)
				whens = append(whens, fmt.Sprintf("WHEN %s THEN DBMS_LOGMNR.MINE_VALUE(%s, %s)", sqlString(otable), side, sqlString(path)))
			}
		}
		return "CASE TABLE_NAME " + strings.Join(whens, " ") + " END"
	}
	cols := []string{"TO_CHAR(SCN)", "OPERATION_CODE", "RAWTOHEX(XID)", "TABLE_NAME", "ROW_ID", "TO_CHAR(TIMESTAMP, '" + dateFormat + "')", "INFO",
		"CSF", fmt.Sprintf("CASE WHEN OPERATION_CODE = %d THEN SQL_REDO END", opSelLobLocator)}
	for i := 0; i < nkey; i++ {
		cols = append(cols, value("REDO_VALUE", i))
	}
	for i := 0; i < nkey; i++ {
		cols = append(cols, value("UNDO_VALUE", i))
	}
	// IN lists hold at most 1000 entries.
	var ins []string
	for i := 0; i < len(names); i += 900 {
		quoted := make([]string, 0, 900)
		for _, n := range names[i:min(i+900, len(names))] {
			quoted = append(quoted, sqlString(n))
		}
		ins = append(ins, "TABLE_NAME IN ("+strings.Join(quoted, ", ")+")")
	}
	ours := fmt.Sprintf("SEG_OWNER = %s AND (%s) AND OPERATION_CODE IN (%d, %d, %d, %d, %d, %d, %d, %d)",
		sqlString(c.src.owner), strings.Join(ins, " OR "),
		opInsert, opDelete, opUpdate, opDDL, opSelLobLocator, opLobWrite, opLobTrim, opLobErase)
	// A change LogMiner can't decode names its table "OBJ# <object id>":
	// redo written before the table's definition changed, read with the
	// dictionary as it is now.
	undecoded := fmt.Sprintf("TABLE_NAME LIKE 'OBJ# %%' AND OPERATION_CODE IN (%d, %d, %d)", opInsert, opDelete, opUpdate)
	return fmt.Sprintf("SELECT %s FROM V$LOGMNR_CONTENTS WHERE SCN <= %d AND ((%s) OR (%s) OR OPERATION_CODE IN (%d, %d))",
		strings.Join(cols, ", "), upTo, ours, undecoded, opCommit, opRollback), nkey
}

func sqlString(s string) string { return "'" + strings.ReplaceAll(s, "'", "''") + "'" }

// errUndecodable is a change to a migrated table that LogMiner can't
// decode: written before the table's definition changed, it was mined
// after, and the online dictionary only knows the table as it is now.
// Which rows it changed is unknown, so the stream can't go on.
var errUndecodable = errors.New("LogMiner can't decode changes made before the table's definition changed")

// collect adds mined rows to the open transactions, and returns those that
// committed; rolled-back ones are dropped.
func (c *ChangeSource) collect(rows []minedRow, open map[string]*txn) ([]*txn, error) {
	var committed []*txn
	for _, r := range rows {
		if id, ok := strings.CutPrefix(r.table, "OBJ# "); ok {
			if name, ours := c.objects[id]; ours {
				return nil, fmt.Errorf("%s: %w; the stream fell behind across a schema change (ALTER TABLE) and must start over", name, errUndecodable)
			}
			continue
		}
		switch r.op {
		case opCommit:
			if t := open[r.xid]; t != nil {
				t.commit, t.at = r.scn, r.at
				committed = append(committed, t)
				delete(open, r.xid)
			}
			continue
		case opRollback:
			delete(open, r.xid)
			continue
		}
		info := c.tables[r.table]
		if info == nil {
			continue
		}
		t := open[r.xid]
		if t == nil {
			t = newTxn(r.xid, r.scn)
			open[r.xid] = t
		}
		// A problem only matters if the transaction commits: a rolled-back
		// one is dropped whatever it held.
		if r.op != opDDL && strings.Contains(r.info, "Dictionary Mismatch") && t.err == nil {
			t.err = fmt.Errorf("%s: %w; the stream fell behind across a schema change (ALTER TABLE) and must start over", info.name, errUndecodable)
		}
		var err error
		switch r.op {
		case opInsert:
			err = t.addKey(info, r.redo)
		case opDelete:
			err = t.addKey(info, r.undo)
		case opUpdate:
			// The redo carries the columns the update set; a key it didn't
			// change is only in the undo, where supplemental logging puts it.
			if err = t.addKey(info, r.undo); err == nil {
				err = t.addKey(info, merged(r.redo, r.undo))
			}
		case opDDL:
			t.ddl[r.table] = true
		case opSelLobLocator:
			// Changing a LOB stored out of line logs no UPDATE: only LOB
			// operations, whose ROWID is empty. The locator's SELECT that
			// starts them names the row by its key.
			var texts []string
			if texts, err = lobLocatorKey(c.src.owner, info, r.lob); err == nil {
				err = t.addKey(info, nullStrings(texts))
			}
		default: // other LOB operations: the row is known by its ROWID, if any
			if r.rowid != "" && strings.Trim(r.rowid, "A") != "" {
				m := t.rowids[r.table]
				if m == nil {
					m = map[string]bool{}
					t.rowids[r.table] = m
				}
				m[r.rowid] = true
			}
		}
		if err != nil && t.err == nil {
			t.err = err
		}
	}
	for _, t := range committed {
		if t.err != nil {
			return nil, t.err
		}
	}
	return committed, nil
}

// changes reads back the rows a committed transaction touched and returns
// its changes: a schema change for each table whose definition it changed,
// then per table the deletes (keys with no row) and the rows as they were
// at its commit.
func (c *ChangeSource) changes(ctx context.Context, conn *sql.Conn, t *txn) ([]cdc.Change, error) {
	var out []cdc.Change
	commitAt := time.UnixMicro(t.at).UTC()
	for _, otable := range sortedKeys(t.ddl) {
		info := c.tables[otable]
		c.src.forgetColumns(info.name)
		cols, key, _, err := c.src.tableKey(ctx, info.name)
		if err != nil {
			return nil, err
		}
		info.cols, info.key = cols, key
		out = append(out, cdc.Change{Table: info.name, Op: cdc.OpSchema, Columns: cdcColumns(cols), CommitAt: commitAt})
	}
	tables := map[string]bool{}
	for otable := range t.keys {
		tables[otable] = true
	}
	for otable := range t.rowids {
		tables[otable] = true
	}
	for _, otable := range sortedKeys(tables) {
		info := c.tables[otable]
		found, err := c.readBack(ctx, conn, info, t.keys[otable], sortedKeys(t.rowids[otable]), t.commit)
		if err != nil {
			return nil, err
		}
		for _, id := range sortedKeys(t.keys[otable]) {
			if _, ok := found[id]; ok {
				continue
			}
			key := map[string]interface{}{}
			for i, kc := range info.key {
				v, err := keyValue(kc, t.keys[otable][id][i])
				if err != nil {
					return nil, fmt.Errorf("%s key %s: %w", info.name, kc.name, err)
				}
				key[kc.name] = v
			}
			out = append(out, cdc.Change{Table: info.name, Op: cdc.OpDelete, Key: key, CommitAt: commitAt})
		}
		for _, id := range sortedKeys(found) {
			rec := found[id]
			key := map[string]interface{}{}
			for _, kc := range info.key {
				key[kc.name] = rec[kc.name]
			}
			out = append(out, cdc.Change{Table: info.name, Op: cdc.OpUpdate, Key: key, After: rec, CommitAt: commitAt})
		}
	}
	return out, nil
}

func sortedKeys[V any](m map[string]V) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// flashbackGone reports an error that means a flashback query can't see
// that SCN: its undo is gone (ORA-01555, 08180, 08181) or the table's
// definition changed since (ORA-01466).
func flashbackGone(err error) bool {
	return isOracleError(err, 1555) || isOracleError(err, 8180) || isOracleError(err, 8181) || isOracleError(err, 1466)
}

func isOracleError(err error, code int) bool {
	if err == nil {
		return false
	}
	var oe *network.OracleError
	if errors.As(err, &oe) {
		return oe.ErrCode == code
	}
	return strings.Contains(err.Error(), fmt.Sprintf("ORA-%05d", code))
}

// readBack reads the rows with the given keys and ROWIDs as of scn, or as
// they are now when the source can't go back that far. It returns them by
// key id (normKey texts joined, as txn.keys holds them).
func (c *ChangeSource) readBack(ctx context.Context, conn *sql.Conn, info *liveInfo, keys map[string][]string, rowids []string, scn uint64) (map[string]map[string]interface{}, error) {
	found, err := c.readRows(ctx, conn, info, keys, rowids, fmt.Sprintf(" AS OF SCN %d", scn))
	if flashbackGone(err) {
		found, err = c.readRows(ctx, conn, info, keys, rowids, "")
	}
	return found, err
}

func (c *ChangeSource) readRows(ctx context.Context, conn *sql.Conn, info *liveInfo, keys map[string][]string, rowids []string, asOf string) (map[string]map[string]interface{}, error) {
	texts := make([]string, len(info.key))
	for i, kc := range info.key {
		texts[i] = keyText(kc)
	}
	head := fmt.Sprintf("SELECT %s, %s FROM %s%s WHERE ", selectList(info.cols), strings.Join(texts, ", "), c.src.from(info.otable), asOf)
	found := map[string]map[string]interface{}{}
	run := func(where string, args []interface{}) error {
		rows, err := conn.QueryContext(ctx, head+where, args...)
		if err != nil {
			return err
		}
		defer rows.Close()
		return scanRowsWithKey(rows, info.cols, len(info.key), func(rec map[string]interface{}, k []string) error {
			found[keyID(info.key, k)] = rec
			return nil
		})
	}
	// Bound parameters per query, as for any IN list.
	per := max(1, adapters.MaxLookupParams/len(info.key))
	ids := sortedKeys(keys)
	for i := 0; i < len(ids); i += per {
		var ors []string
		var args []interface{}
		for _, id := range ids[i:min(i+per, len(ids))] {
			var ands []string
			for k, kc := range info.key {
				args = append(args, keys[id][k])
				ands = append(ands, quote(kc.oracleName)+" = "+keyParam(kc, len(args)))
			}
			ors = append(ors, "("+strings.Join(ands, " AND ")+")")
		}
		if err := run(strings.Join(ors, " OR "), args); err != nil {
			return nil, err
		}
	}
	for i := 0; i < len(rowids); i += adapters.MaxLookupParams {
		var ps []string
		var args []interface{}
		for _, r := range rowids[i:min(i+adapters.MaxLookupParams, len(rowids))] {
			args = append(args, r)
			ps = append(ps, fmt.Sprintf("CHARTOROWID(:%d)", len(args)))
		}
		if err := run("ROWID IN ("+strings.Join(ps, ", ")+")", args); err != nil {
			return nil, err
		}
	}
	return found, nil
}
