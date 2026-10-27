package postgres

import (
	"context"
	"fmt"
	"strings"

	"gitea.homelab.local/nextdevops/XferDB/adapters"
)

// LiveChecks implements adapters.LivePreflighter: what streaming changes
// from this server with logical replication needs (#102).
func (s *Source) LiveChecks(ctx context.Context, tables []string) ([]adapters.LiveCheck, error) {
	if len(tables) == 0 {
		var err error
		if tables, err = listTables(ctx, s.db, s.config.Schema == ""); err != nil {
			return nil, err
		}
	}
	var checks []adapters.LiveCheck
	add := func(c adapters.LiveCheck) { checks = append(checks, c) }

	var versionNum int
	if err := s.db.QueryRowContext(ctx, `SELECT current_setting('server_version_num')::int`).Scan(&versionNum); err != nil {
		return nil, fmt.Errorf("read server version: %w", err)
	}
	add(adapters.LiveCheck{
		Name:   "PostgreSQL 10 or later",
		OK:     versionNum >= 100000,
		Detail: fmt.Sprintf("server_version_num = %d", versionNum),
		Fix:    "logical replication with pgoutput needs PostgreSQL 10 or later",
	})

	var walLevel string
	if err := s.db.QueryRowContext(ctx, `SHOW wal_level`).Scan(&walLevel); err != nil {
		return nil, fmt.Errorf("read wal_level: %w", err)
	}
	add(adapters.LiveCheck{
		Name:   "wal_level = logical",
		OK:     walLevel == "logical",
		Detail: "wal_level = " + walLevel,
		Fix:    "set wal_level = logical and restart PostgreSQL (on Amazon RDS / Aurora: set the rds.logical_replication parameter to 1 and reboot)",
	})

	var user string
	var canReplicate, superuser bool
	if err := s.db.QueryRowContext(ctx, `
		SELECT current_user, r.rolreplication OR r.rolsuper
		       OR EXISTS (SELECT 1 FROM pg_roles g WHERE g.rolname = 'rds_replication'
		                  AND pg_has_role(current_user, g.oid, 'member')),
		       r.rolsuper
		FROM pg_roles r WHERE r.rolname = current_user`).Scan(&user, &canReplicate, &superuser); err != nil {
		return nil, fmt.Errorf("read role attributes: %w", err)
	}
	add(adapters.LiveCheck{
		Name:   "replication privilege",
		OK:     canReplicate,
		Detail: "role " + user,
		Fix:    fmt.Sprintf("ALTER ROLE %s REPLICATION; (on Amazon RDS: GRANT rds_replication TO %s;)", quote(user), quote(user)),
	})

	var freeSlots, maxSlots int
	if err := s.db.QueryRowContext(ctx, `
		SELECT current_setting('max_replication_slots')::int - (SELECT count(*) FROM pg_replication_slots),
		       current_setting('max_replication_slots')::int`).Scan(&freeSlots, &maxSlots); err != nil {
		return nil, fmt.Errorf("read replication slots: %w", err)
	}
	add(adapters.LiveCheck{
		Name:   "a free replication slot",
		OK:     freeSlots > 0,
		Detail: fmt.Sprintf("%d of max_replication_slots = %d free", freeSlots, maxSlots),
		Fix:    "raise max_replication_slots (needs a restart) or drop an unused slot",
	})

	var freeSenders, maxSenders int
	if err := s.db.QueryRowContext(ctx, `
		SELECT current_setting('max_wal_senders')::int - (SELECT count(*) FROM pg_stat_replication),
		       current_setting('max_wal_senders')::int`).Scan(&freeSenders, &maxSenders); err != nil {
		return nil, fmt.Errorf("read WAL senders: %w", err)
	}
	add(adapters.LiveCheck{
		Name:   "a free WAL sender",
		OK:     freeSenders > 0,
		Detail: fmt.Sprintf("%d of max_wal_senders = %d free", freeSenders, maxSenders),
		Fix:    "raise max_wal_senders (needs a restart)",
	})

	var noKey, notOwned []string
	for _, t := range tables {
		schema, table := splitName(t)
		var hasKey, owns bool
		err := s.db.QueryRowContext(ctx, `
			SELECT EXISTS (SELECT 1 FROM pg_index i WHERE i.indrelid = c.oid AND i.indisprimary),
			       pg_has_role(current_user, c.relowner, 'USAGE')
			FROM pg_class c
			WHERE c.oid = format('%I.%I', `+schemaExpr(2)+`, $1::text)::regclass`, table, schema).Scan(&hasKey, &owns)
		if err != nil {
			return nil, fmt.Errorf("check table %s: %w", t, err)
		}
		if !hasKey {
			noKey = append(noKey, t)
		}
		if !owns && !superuser {
			notOwned = append(notOwned, t)
		}
	}
	add(adapters.LiveCheck{
		Name:   "a primary key on every table",
		OK:     len(noKey) == 0,
		Detail: listDetail(len(tables), noKey, "without one"),
		Fix:    "add a primary key to each table listed, or leave them out of the migration",
	})

	var canCreate bool
	if err := s.db.QueryRowContext(ctx, `SELECT has_database_privilege(current_database(), 'CREATE')`).Scan(&canCreate); err != nil {
		return nil, fmt.Errorf("check CREATE privilege: %w", err)
	}
	pubOK := canCreate && len(notOwned) == 0
	detail := "CREATE on the database"
	if !canCreate {
		detail = "no CREATE privilege on the database"
	}
	if len(notOwned) > 0 {
		detail += "; " + listDetail(len(tables), notOwned, "owned by another role")
	}
	add(adapters.LiveCheck{
		Name:   "permission to create the publication",
		OK:     pubOK,
		Detail: detail,
		Fix:    fmt.Sprintf("CREATE PUBLICATION needs CREATE on the database (GRANT CREATE ON DATABASE <db> TO %s;) and ownership of every published table: connect as the tables' owner, or grant %s membership in it", quote(user), quote(user)),
	})

	if versionNum >= 130000 {
		var keep string
		if err := s.db.QueryRowContext(ctx, `SHOW max_slot_wal_keep_size`).Scan(&keep); err == nil {
			unlimited := keep == "-1"
			c := adapters.LiveCheck{Name: "WAL a slot may hold", OK: true, Detail: "max_slot_wal_keep_size = " + keep}
			if unlimited {
				c.Warning = true
				c.Detail += " (unlimited)"
				c.Fix = "if XferDB's capture stops for long, the slot keeps WAL until the source's disk fills; consider a limit, which fails the live migration instead"
			}
			add(c)
		}
	}
	return checks, nil
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
