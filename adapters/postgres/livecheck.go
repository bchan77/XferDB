package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"time"

	"gitea.homelab.local/nextdevops/XferDB/adapters"
)

// liveCheckTimeout bounds each query of the live readiness check. A server
// that doesn't answer one (YugabyteDB's pg_stat_replication can wait two
// minutes for a table it never creates, #158) fails that check, not the
// whole readiness check.
var liveCheckTimeout = 15 * time.Second

// queryRow runs a one-row query and scans it into dest, giving up once
// liveCheckTimeout has passed. Cancelling the context isn't enough on its
// own: lib/pq then asks the server to cancel the query but still waits for
// its answer, and a server stuck on the query (or on the connection's
// startup) may never give one. So the query runs in a goroutine scanning
// into copies of dest, and queryRow stops waiting at the deadline; the
// abandoned query ends whenever the server answers, its connection marked
// bad by the cancelled context.
func queryRow(ctx context.Context, db *sql.DB, dest []any, q string, args ...any) error {
	ctx, cancel := context.WithTimeout(ctx, liveCheckTimeout)
	defer cancel()
	copies := make([]any, len(dest))
	for i, d := range dest {
		copies[i] = reflect.New(reflect.TypeOf(d).Elem()).Interface()
	}
	done := make(chan error, 1)
	go func() { done <- db.QueryRowContext(ctx, q, args...).Scan(copies...) }()
	select {
	case err := <-done:
		if err != nil {
			return errors.New(queryError(ctx, err))
		}
		for i, d := range dest {
			reflect.ValueOf(d).Elem().Set(reflect.ValueOf(copies[i]).Elem())
		}
		return nil
	case <-ctx.Done():
		return errors.New(queryError(ctx, ctx.Err()))
	}
}

// isYugabyte reports whether version(), as the server returns it, is
// YugabyteDB's: "PostgreSQL 15.12-YB-2025.2.5.2-b0 on x86_64-pc-linux-gnu, …".
func isYugabyte(version string) bool {
	return strings.Contains(version, "-YB-")
}

// yugabyteRelease is the YugabyteDB release in a YugabyteDB version()
// string ("2025.2.5.2-b0"), or "" when there is none.
func yugabyteRelease(version string) string {
	_, rest, ok := strings.Cut(version, "-YB-")
	if !ok {
		return ""
	}
	if i := strings.IndexByte(rest, ' '); i >= 0 {
		rest = rest[:i]
	}
	return rest
}

// LiveChecks implements adapters.LivePreflighter: what streaming changes
// from this server with logical replication needs (#102). Each check is
// independent: a query that fails or times out fails its own check and the
// rest still run, so the result always shows everything that can be known.
// The error is always nil.
func (s *Source) LiveChecks(ctx context.Context, tables []string) ([]adapters.LiveCheck, error) {
	var checks []adapters.LiveCheck
	add := func(c adapters.LiveCheck) { checks = append(checks, c) }

	// query runs one check's query with its own timeout. On failure it adds
	// a failed check named name and returns false.
	query := func(name, q string, dest []any, args ...any) bool {
		if err := queryRow(ctx, s.db, dest, q, args...); err != nil {
			add(adapters.LiveCheck{Name: name, Detail: err.Error()})
			return false
		}
		return true
	}

	// Which server this is decides most of what follows: YugabyteDB has no
	// WAL, its own slot bookkeeping, and its own replica identity.
	var version string
	var versionNum int
	versionOK := query("PostgreSQL 10 or later",
		`SELECT version(), current_setting('server_version_num')::int`, []any{&version, &versionNum})
	yb := versionOK && isYugabyte(version)
	if versionOK {
		if yb {
			add(adapters.LiveCheck{
				Name:   "YugabyteDB",
				OK:     true,
				Detail: fmt.Sprintf("YugabyteDB %s (PostgreSQL server_version_num = %d)", yugabyteRelease(version), versionNum),
			})
		} else {
			add(adapters.LiveCheck{
				Name:   "PostgreSQL 10 or later",
				OK:     versionNum >= 100000,
				Detail: fmt.Sprintf("server_version_num = %d", versionNum),
				Fix:    "logical replication with pgoutput needs PostgreSQL 10 or later",
			})
		}
		switch configured := s.config.Type; {
		case yb && configured != adapters.TypeYugabyte:
			add(adapters.LiveCheck{
				Name:   "source type matches the server",
				Detail: fmt.Sprintf("the source type is %s, but the server is YugabyteDB", configured),
				Fix:    "set the project's source type to yugabyte: live migration from YugabyteDB differs from PostgreSQL's",
			})
		case !yb && configured == adapters.TypeYugabyte:
			add(adapters.LiveCheck{
				Name:   "source type matches the server",
				Detail: "the source type is yugabyte, but the server is PostgreSQL",
				Fix:    "set the project's source type to postgres",
			})
		}
	}

	if yb {
		var enabled string
		if query("replication commands enabled", `SELECT current_setting('yb_enable_replication_commands', true)`, []any{&enabled}) {
			add(adapters.LiveCheck{
				Name:   "replication commands enabled",
				OK:     enabled == "on",
				Detail: "yb_enable_replication_commands = " + orUnset(enabled),
				Fix:    "set the gflag ysql_yb_enable_replication_commands=true on every tserver; it needs a YugabyteDB release with PostgreSQL logical replication",
			})
		}
	}

	if yb {
		// Live migration needs hybrid-time slot positions, comparable with
		// the time now, to tell when the target has caught up.
		var lsnTypes string
		var hasNow bool
		if query("hybrid-time replication slots", `SELECT current_setting('yb_allow_replication_slot_lsn_types', true),
				to_regproc('yb_get_current_hybrid_time_lsn') IS NOT NULL`, []any{&lsnTypes, &hasNow}) {
			add(adapters.LiveCheck{
				Name:   "hybrid-time replication slots",
				OK:     lsnTypes == "on" && hasNow,
				Detail: fmt.Sprintf("yb_allow_replication_slot_lsn_types = %s, yb_get_current_hybrid_time_lsn() %s", orUnset(lsnTypes), map[bool]string{true: "available", false: "missing"}[hasNow]),
				Fix:    "XferDB tells when the target has caught up by the hybrid time of each change: it needs a YugabyteDB release with HYBRID_TIME replication slots, and the gflag ysql_yb_allow_replication_slot_lsn_types=true on every tserver",
			})
		}
	}

	var walLevel string
	if query("wal_level = logical", `SHOW wal_level`, []any{&walLevel}) {
		add(adapters.LiveCheck{
			Name:   "wal_level = logical",
			OK:     walLevel == "logical",
			Detail: "wal_level = " + walLevel,
			Fix:    "set wal_level = logical and restart PostgreSQL (on Amazon RDS / Aurora: set the rds.logical_replication parameter to 1 and reboot)",
		})
	}

	user := "<user>"
	var canReplicate, superuser bool
	if query("replication privilege", `
		SELECT current_user, r.rolreplication OR r.rolsuper
		       OR EXISTS (SELECT 1 FROM pg_roles g WHERE g.rolname = 'rds_replication'
		                  AND pg_has_role(current_user, g.oid, 'member')),
		       r.rolsuper
		FROM pg_roles r WHERE r.rolname = current_user`, []any{&user, &canReplicate, &superuser}) {
		add(adapters.LiveCheck{
			Name:   "replication privilege",
			OK:     canReplicate,
			Detail: "role " + user,
			Fix:    fmt.Sprintf("ALTER ROLE %s REPLICATION; (on Amazon RDS: GRANT rds_replication TO %s;)", quote(user), quote(user)),
		})
	}

	var freeSlots, maxSlots int
	if query("a free replication slot", `
		SELECT current_setting('max_replication_slots')::int - (SELECT count(*) FROM pg_replication_slots),
		       current_setting('max_replication_slots')::int`, []any{&freeSlots, &maxSlots}) {
		add(adapters.LiveCheck{
			Name:   "a free replication slot",
			OK:     freeSlots > 0,
			Detail: fmt.Sprintf("%d of max_replication_slots = %d free", freeSlots, maxSlots),
			Fix:    "raise max_replication_slots (needs a restart) or drop an unused slot",
		})
	}

	// YugabyteDB has no WAL senders to run out of, and its
	// pg_stat_replication waits for system.cdc_state, which only the first
	// replication slot creates: on a cluster that never had one, it blocks
	// until the master times out (#158).
	if !yb {
		var freeSenders, maxSenders int
		if query("a free WAL sender", `
			SELECT current_setting('max_wal_senders')::int - (SELECT count(*) FROM pg_stat_replication),
			       current_setting('max_wal_senders')::int`, []any{&freeSenders, &maxSenders}) {
			add(adapters.LiveCheck{
				Name:   "a free WAL sender",
				OK:     freeSenders > 0,
				Detail: fmt.Sprintf("%d of max_wal_senders = %d free", freeSenders, maxSenders),
				Fix:    "raise max_wal_senders (needs a restart)",
			})
		}
	}

	notOwned, tablesOK := s.tableChecks(ctx, tables, yb, superuser, add)

	if yb {
		// The heartbeats that move an idle stream along go to a small table
		// XferDB creates in the connection's schema.
		var canCreateTable bool
		var schema string
		if query("permission to create the heartbeat table", `SELECT current_schema(), has_schema_privilege(current_schema(), 'CREATE')`, []any{&schema, &canCreateTable}) {
			add(adapters.LiveCheck{
				Name:   "permission to create the heartbeat table",
				OK:     canCreateTable,
				Detail: fmt.Sprintf("CREATE on schema %s, for %s.%s", schema, schema, heartbeatTable),
				Fix:    fmt.Sprintf("GRANT CREATE ON SCHEMA %s TO %s; XferDB writes a heartbeat row there during the migration so it can tell when the target has caught up, and drops the table at the end", quote(schema), quote(user)),
			})
		}
	}

	var canCreate bool
	if query("permission to create the publication", `SELECT has_database_privilege(current_database(), 'CREATE')`, []any{&canCreate}) {
		pubOK := canCreate && notOwned == "" && tablesOK
		detail := "CREATE on the database"
		if !canCreate {
			detail = "no CREATE privilege on the database"
		}
		if notOwned != "" {
			detail += "; " + notOwned
		}
		if !tablesOK {
			detail += "; table ownership couldn't be checked"
		}
		add(adapters.LiveCheck{
			Name:   "permission to create the publication",
			OK:     pubOK,
			Detail: detail,
			Fix:    fmt.Sprintf("CREATE PUBLICATION needs CREATE on the database (GRANT CREATE ON DATABASE <db> TO %s;) and ownership of every published table: connect as the tables' owner, or grant %s membership in it", quote(user), quote(user)),
		})
	}

	switch {
	case yb:
		// No WAL: a YugabyteDB stream keeps changes for the tservers'
		// cdc_intent_retention_ms, a gflag SQL can't read.
		add(adapters.LiveCheck{
			Name:    "how long the source keeps changes",
			OK:      true,
			Warning: true,
			Detail:  "the tservers' cdc_intent_retention_ms (8 hours unless changed)",
			Fix:     "if XferDB's capture stops for longer than that, the stream expires and the live migration fails; raise cdc_intent_retention_ms on every tserver if a longer outage is possible",
		})
	case versionNum >= 130000:
		var keep string
		if queryRow(ctx, s.db, []any{&keep}, `SHOW max_slot_wal_keep_size`) == nil {
			c := adapters.LiveCheck{Name: "WAL a slot may hold", OK: true, Detail: "max_slot_wal_keep_size = " + keep}
			if keep == "-1" {
				c.Warning = true
				c.Detail += " (unlimited)"
				c.Fix = "if XferDB's capture stops for long, the slot keeps WAL until the source's disk fills; consider a limit, which fails the live migration instead"
			}
			add(c)
		}
	}
	return checks, nil
}

// tableChecks adds the per-table checks: a primary key, and a replica
// identity pgoutput can stream. It returns a description of the tables the
// login doesn't own ("" when it owns them all or is a superuser), for the
// publication check, and whether the tables could be checked at all.
func (s *Source) tableChecks(ctx context.Context, tables []string, yb, superuser bool, add func(adapters.LiveCheck)) (notOwned string, ok bool) {
	const keyCheck = "a primary key on every table"
	if len(tables) == 0 {
		// The same deadline as queryRow, for a query of many rows.
		lctx, cancel := context.WithTimeout(ctx, liveCheckTimeout)
		type listed struct {
			tables []string
			err    error
		}
		done := make(chan listed, 1)
		go func() {
			t, err := listTables(lctx, s.db, s.config.Schema == "")
			done <- listed{t, err}
		}()
		var err error
		select {
		case l := <-done:
			tables, err = l.tables, l.err
		case <-lctx.Done():
			err = lctx.Err()
		}
		if err != nil {
			err = errors.New(queryError(lctx, err))
		}
		cancel()
		if err != nil {
			add(adapters.LiveCheck{Name: keyCheck, Detail: "list the tables: " + err.Error()})
			return "", false
		}
	}

	var noKey, others, badIdentity []string
	identities := map[string]bool{}
	for _, t := range tables {
		schema, table := splitName(t)
		var hasKey, owns bool
		var identity string
		err := queryRow(ctx, s.db, []any{&hasKey, &owns, &identity}, `
			SELECT EXISTS (SELECT 1 FROM pg_index i WHERE i.indrelid = c.oid AND i.indisprimary),
			       pg_has_role(current_user, c.relowner, 'USAGE'),
			       c.relreplident::text
			FROM pg_class c
			WHERE c.oid = format('%I.%I', `+schemaExpr(2)+`, $1::text)::regclass`, table, schema)
		if err != nil {
			add(adapters.LiveCheck{Name: keyCheck, Detail: fmt.Sprintf("check table %s: %v", t, err)})
			return "", false
		}
		if !hasKey {
			noKey = append(noKey, t)
		}
		if !owns && !superuser {
			others = append(others, t)
		}
		if !replicaIdentityOK(identity) {
			badIdentity = append(badIdentity, fmt.Sprintf("%s (%s)", t, replicaIdentityName(identity)))
			identities[identity] = true
		}
	}
	add(adapters.LiveCheck{
		Name:   keyCheck,
		OK:     len(noKey) == 0,
		Detail: listDetail(len(tables), noKey, "without one"),
		Fix:    "add a primary key to each table listed, or leave them out of the migration",
	})

	// A table without a key has its own failing check above; DEFAULT on it
	// is that same problem, so only identities that fail in their own right
	// are listed here.
	identity := adapters.LiveCheck{
		Name:   "a replica identity logical replication can use",
		OK:     len(badIdentity) == 0,
		Detail: listDetail(len(tables), badIdentity, "with one it can't"),
	}
	if !identity.OK {
		var why []string
		if identities["c"] {
			why = append(why, "pgoutput can't stream a table with YugabyteDB's CHANGE replica identity, the default for new YugabyteDB tables")
		}
		if identities["n"] {
			if yb {
				why = append(why, "pgoutput can't stream a table with replica identity NOTHING")
			} else {
				why = append(why, "with replica identity NOTHING, updates and deletes on the source fail once the table is published")
			}
		}
		identity.Fix = strings.Join(why, "; ") + ". For each table: ALTER TABLE <table> REPLICA IDENTITY DEFAULT;"
		if yb {
			identity.Fix += " YugabyteDB fixes a table's replica identity when the replication slot is created, so change it before starting."
		}
	}
	add(identity)

	if len(others) == 0 {
		return "", true
	}
	return listDetail(len(tables), others, "owned by another role"), true
}

// replicaIdentityOK reports whether pgoutput can stream a table with
// pg_class.relreplident identity: d(efault, the primary key), f(ull),
// i(ndex), but not n(othing) or YugabyteDB's c(hange). DEFAULT without a
// primary key is the missing key, which has its own check.
func replicaIdentityOK(identity string) bool {
	switch identity {
	case "d", "f", "i":
		return true
	}
	return false
}

func replicaIdentityName(identity string) string {
	switch identity {
	case "d":
		return "DEFAULT"
	case "f":
		return "FULL"
	case "i":
		return "INDEX"
	case "n":
		return "NOTHING"
	case "c":
		return "CHANGE"
	}
	return identity
}

// queryError describes a failed check query, saying so when it ran out of
// time rather than failing outright.
func queryError(ctx context.Context, err error) string {
	if errors.Is(ctx.Err(), context.DeadlineExceeded) {
		return fmt.Sprintf("no answer within %s: the server is slow or stuck on this query (%v)", liveCheckTimeout, err)
	}
	return err.Error()
}

func orUnset(v string) string {
	if v == "" {
		return "(not set)"
	}
	return v
}

func listDetail(total int, names []string, what string) string {
	if len(names) == 0 {
		return fmt.Sprintf("all %d tables", total)
	}
	shown := names
	if len(shown) > 10 {
		shown = shown[:10]
	}
	s := fmt.Sprintf("%d of %d tables %s: %s", len(names), total, what, strings.Join(shown, ", "))
	if len(names) > len(shown) {
		s += fmt.Sprintf(" and %d more", len(names)-len(shown))
	}
	return s
}
