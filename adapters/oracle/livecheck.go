package oracle

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"strings"

	"gitea.homelab.local/nextdevops/XferDB/adapters"
)

// LiveChecks implements adapters.LivePreflighter: what a live migration
// from this source needs (#195). XferDB never changes a source setting, so
// each failed check says what a DBA runs to fix it.
func (s *Source) LiveChecks(ctx context.Context, tables []string) ([]adapters.LiveCheck, error) {
	var checks []adapters.LiveCheck
	add := func(c adapters.LiveCheck) { checks = append(checks, c) }
	user := s.config.Username
	if user == "" {
		user = "<user>"
	}

	var logMode, suppMin, suppPK, cdb string
	err := s.db.QueryRowContext(ctx, `SELECT log_mode, supplemental_log_data_min, supplemental_log_data_pk, cdb FROM v$database`).
		Scan(&logMode, &suppMin, &suppPK, &cdb)
	if err != nil {
		add(adapters.LiveCheck{Name: "can read V$DATABASE", Detail: err.Error(),
			Fix: privilegesFix(user)})
		return checks, nil
	}
	if logMode == "ARCHIVELOG" {
		add(adapters.LiveCheck{Name: "ARCHIVELOG mode", OK: true, Detail: "the database archives its redo"})
	} else {
		add(adapters.LiveCheck{Name: "ARCHIVELOG mode", Detail: "the database is in " + logMode + " mode: redo is overwritten before it can be mined",
			Fix: "as SYSDBA (in the root container on a CDB): SHUTDOWN IMMEDIATE; STARTUP MOUNT; ALTER DATABASE ARCHIVELOG; ALTER DATABASE OPEN; (and ALTER PLUGGABLE DATABASE ALL OPEN on a CDB)"})
	}

	// Minimal supplemental logging database-wide, and the primary key logged
	// with every change: database-wide, or for each table.
	if suppMin == "NO" {
		add(adapters.LiveCheck{Name: "supplemental logging", Detail: "minimal supplemental logging is off",
			Fix: "ALTER DATABASE ADD SUPPLEMENTAL LOG DATA; and ALTER DATABASE ADD SUPPLEMENTAL LOG DATA (PRIMARY KEY) COLUMNS; (or per table: ALTER TABLE <table> ADD SUPPLEMENTAL LOG DATA (PRIMARY KEY) COLUMNS;)"})
	} else if suppPK == "YES" {
		add(adapters.LiveCheck{Name: "supplemental logging", OK: true, Detail: "minimal and primary key, database-wide"})
	} else {
		missing, err := s.tablesWithoutPKLogging(ctx, tables)
		switch {
		case err != nil:
			add(adapters.LiveCheck{Name: "supplemental logging", Detail: err.Error(), Fix: privilegesFix(user)})
		case len(missing) == 0:
			add(adapters.LiveCheck{Name: "supplemental logging", OK: true, Detail: "minimal database-wide, and primary key on each table"})
		default:
			add(adapters.LiveCheck{Name: "supplemental logging", Detail: listOf(missing) + " don't log their primary key with each change",
				Fix: "ALTER DATABASE ADD SUPPLEMENTAL LOG DATA (PRIMARY KEY) COLUMNS; or for each table: ALTER TABLE <table> ADD SUPPLEMENTAL LOG DATA (PRIMARY KEY) COLUMNS;"})
		}
	}

	// Mining is what the privileges are for, so mine: a window of no
	// length, which also tells whether this database mines its online redo
	// or, per PDB before 23ai, archived logs only.
	archivedOnly := false
	mineErr := s.tryMining(ctx)
	if isOracleError(mineErr, 1291) {
		archivedOnly, mineErr = true, nil
	}
	if mineErr != nil {
		add(adapters.LiveCheck{Name: "can run LogMiner", Detail: mineErr.Error(), Fix: privilegesFix(user)})
	} else {
		detail := "LogMiner runs from this connection"
		if cdb == "YES" {
			detail += " (inside the pluggable database)"
		}
		add(adapters.LiveCheck{Name: "can run LogMiner", OK: true, Detail: detail})
	}
	if archivedOnly {
		lag, err := s.parameter(ctx, "archive_lag_target")
		check := adapters.LiveCheck{Name: "redo archived often enough", Warning: true}
		switch n, _ := strconv.Atoi(lag); {
		case err != nil:
			check.Detail = "this database mines archived logs only, and archive_lag_target can't be read: " + err.Error()
		case n == 0:
			check.Detail = "this database mines archived logs only, and archive_lag_target is 0: changes arrive only when a log fills up"
			check.Fix = "ALTER SYSTEM SET archive_lag_target = 60; (seconds; in the root container) bounds how far behind the stream runs"
		default:
			check.OK = true
			check.Detail = fmt.Sprintf("this database mines archived logs only; archive_lag_target %ds bounds the lag", n)
		}
		add(check)
	}

	if err := s.probe(ctx, `SELECT 1 FROM v$transaction WHERE 1 = 0`); err != nil {
		add(adapters.LiveCheck{Name: "can read open transactions", Detail: err.Error(), Fix: privilegesFix(user)})
	}

	// Every table: a primary key of a type the stream can carry, and
	// readable as of an SCN.
	if len(tables) == 0 {
		all, err := s.refreshTables(ctx)
		if err != nil {
			return nil, err
		}
		tables = all
	}
	var noKey, badKey, noFlashback []string
	scn, _, err := now(ctx, s.db)
	if err != nil {
		return nil, err
	}
	for _, t := range tables {
		_, key, otable, err := s.tableKey(ctx, t)
		if err != nil {
			return nil, err
		}
		if len(key) == 0 {
			noKey = append(noKey, t)
			continue
		}
		for _, k := range key {
			if kindOfKey(k.dataType) == keyUnsupported {
				badKey = append(badKey, fmt.Sprintf("%s (%s %s)", t, k.name, k.dataType))
				break
			}
		}
		// AS OF takes no subquery (ORA-22818): the SCN goes in as it is.
		if err := s.probe(ctx, fmt.Sprintf(`SELECT 1 FROM %s AS OF SCN %d WHERE 1 = 0`, s.from(otable), scn)); err != nil {
			if !flashbackGone(err) { // a table created moments ago is fine later
				noFlashback = append(noFlashback, fmt.Sprintf("%s (%v)", t, firstLine(err)))
			}
		}
	}
	add(adapters.LiveCheck{Name: "every table has a primary key", OK: len(noKey) == 0, Detail: detailOr(noKey, "without one", len(tables)),
		Fix: fixIf(noKey, "add a primary key to each, or leave them out of the migration")})
	add(adapters.LiveCheck{Name: "primary keys of supported types", OK: len(badKey) == 0, Detail: detailOr(badKey, "with a key the stream can't carry", len(tables)),
		Fix: fixIf(badKey, "a live migration needs keys of NUMBER, FLOAT, CHAR, VARCHAR2, DATE, TIMESTAMP or RAW columns; leave these tables out, or migrate them offline")})
	add(adapters.LiveCheck{Name: "flashback queries on the tables", OK: len(noFlashback) == 0, Detail: detailOr(noFlashback, "that can't be read AS OF an SCN", len(tables)),
		Fix: fixIf(noFlashback, fmt.Sprintf("GRANT FLASHBACK ON <table> TO %s; for each, or GRANT FLASHBACK ANY TABLE TO %s;", user, user))})

	if undo, err := s.parameter(ctx, "undo_retention"); err == nil {
		add(adapters.LiveCheck{Name: "undo retention", OK: true, Warning: true,
			Detail: "undo_retention is " + undo + "s: a change read back later than that comes from the row as it is now, which still converges"})
	}
	return checks, nil
}

// tryMining starts and ends a LogMiner session over no redo.
func (s *Source) tryMining(ctx context.Context) error {
	conn, err := s.db.Conn(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()
	scn, _, err := now(ctx, conn)
	if err != nil {
		return err
	}
	if _, err := conn.ExecContext(ctx, `BEGIN DBMS_LOGMNR.START_LOGMNR(STARTSCN => :1, ENDSCN => :2, OPTIONS => DBMS_LOGMNR.DICT_FROM_ONLINE_CATALOG); END;`, int64(scn), int64(scn)); err != nil {
		return err
	}
	_, err = conn.ExecContext(ctx, `BEGIN DBMS_LOGMNR.END_LOGMNR; END;`)
	return err
}

// probe runs a query for whether it can run at all.
func (s *Source) probe(ctx context.Context, query string) error {
	rows, err := s.db.QueryContext(ctx, query)
	if err != nil {
		return err
	}
	rows.Close()
	return rows.Err()
}

// parameter reads an initialization parameter.
func (s *Source) parameter(ctx context.Context, name string) (string, error) {
	var v sql.NullString
	err := s.db.QueryRowContext(ctx, `SELECT value FROM v$parameter WHERE name = :1`, name).Scan(&v)
	return v.String, err
}

// tablesWithoutPKLogging returns the tables with no primary key log group.
func (s *Source) tablesWithoutPKLogging(ctx context.Context, tables []string) ([]string, error) {
	if len(tables) == 0 {
		all, err := s.refreshTables(ctx)
		if err != nil {
			return nil, err
		}
		tables = all
	}
	rows, err := s.db.QueryContext(ctx, `SELECT table_name FROM all_log_groups WHERE owner = :1 AND log_group_type = 'PRIMARY KEY LOGGING'`, s.owner)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	logged := map[string]bool{}
	for rows.Next() {
		var t string
		if err := rows.Scan(&t); err != nil {
			return nil, err
		}
		logged[t] = true
	}
	var missing []string
	for _, t := range tables {
		otable, err := s.oracleTable(ctx, t)
		if err != nil {
			return nil, err
		}
		if !logged[otable] {
			missing = append(missing, t)
		}
	}
	return missing, rows.Err()
}

func privilegesFix(user string) string {
	return fmt.Sprintf("GRANT CREATE SESSION, LOGMINING, SELECT ANY TRANSACTION TO %[1]s; GRANT EXECUTE ON DBMS_LOGMNR TO %[1]s; "+
		"GRANT SELECT ON V_$DATABASE TO %[1]s; GRANT SELECT ON V_$LOGMNR_CONTENTS TO %[1]s; GRANT SELECT ON V_$LOGMNR_LOGS TO %[1]s; "+
		"GRANT SELECT ON V_$ARCHIVED_LOG TO %[1]s; GRANT SELECT ON V_$LOG TO %[1]s; GRANT SELECT ON V_$TRANSACTION TO %[1]s; "+
		"GRANT SELECT ON V_$PARAMETER TO %[1]s; (as SYSDBA, in the pluggable database)", user)
}

func firstLine(err error) string {
	msg, _, _ := strings.Cut(err.Error(), "\n")
	return msg
}

func listOf(names []string) string {
	if len(names) > 5 {
		return strings.Join(names[:5], ", ") + fmt.Sprintf(" and %d more", len(names)-5)
	}
	return strings.Join(names, ", ")
}

func detailOr(bad []string, what string, total int) string {
	if len(bad) == 0 {
		return fmt.Sprintf("all %d table(s)", total)
	}
	return fmt.Sprintf("%d of %d table(s) %s: %s", len(bad), total, what, listOf(bad))
}

func fixIf(bad []string, fix string) string {
	if len(bad) == 0 {
		return ""
	}
	return fix
}
