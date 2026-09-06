package state

import (
	"context"
	"time"
)

// MigrationStatsRecord is a single point-in-time snapshot of migration statistics.
type MigrationStatsRecord struct {
	ID              int64     `json:"id" db:"id"`
	MigrationID     string    `json:"migration_id" db:"migration_id"`
	Timestamp       time.Time `json:"timestamp" db:"timestamp"`
	ElapsedSecs     float64   `json:"elapsed_secs" db:"elapsed_secs"`
	Phase           string    `json:"phase" db:"phase"`
	RowsTotal       int64     `json:"rows_total" db:"rows_total"`
	RowsTransferred int64     `json:"rows_transferred" db:"rows_transferred"`
	RatePerSec      float64   `json:"rate_per_sec" db:"rate_per_sec"`
	ReadRate        float64   `json:"read_rate" db:"read_rate"`
	WriteRate       float64   `json:"write_rate" db:"write_rate"`
	TablesTotal     int       `json:"tables_total" db:"tables_total"`
	TablesDone      int       `json:"tables_done" db:"tables_done"`
	TablesFailed    int       `json:"tables_failed" db:"tables_failed"`
	Goroutines      int       `json:"goroutines" db:"goroutines"`
	MemAllocMB      float64   `json:"mem_alloc_mb" db:"mem_alloc_mb"`
	MemSysMB        float64   `json:"mem_sys_mb" db:"mem_sys_mb"`
	CPUPercent      float64   `json:"cpu_percent" db:"cpu_percent"`
}

// SaveMigrationStats inserts a single stats snapshot for a migration.
func (m *MetaDB) SaveMigrationStats(ctx context.Context, rec MigrationStatsRecord) error {
	_, err := m.db.ExecContext(ctx, `
		INSERT INTO migration_stats (
			migration_id, timestamp, elapsed_secs, phase,
			rows_total, rows_transferred, rate_per_sec, read_rate, write_rate,
			tables_total, tables_done, tables_failed,
			goroutines, mem_alloc_mb, mem_sys_mb, cpu_percent
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		rec.MigrationID, rec.Timestamp, rec.ElapsedSecs, rec.Phase,
		rec.RowsTotal, rec.RowsTransferred, rec.RatePerSec, rec.ReadRate, rec.WriteRate,
		rec.TablesTotal, rec.TablesDone, rec.TablesFailed,
		rec.Goroutines, rec.MemAllocMB, rec.MemSysMB, rec.CPUPercent)
	return err
}

// GetMigrationStats retrieves all stats snapshots for a migration, ordered by timestamp.
func (m *MetaDB) GetMigrationStats(ctx context.Context, migrationID string) ([]MigrationStatsRecord, error) {
	var records []MigrationStatsRecord
	err := m.db.SelectContext(ctx, &records, `
		SELECT id, migration_id, timestamp, elapsed_secs, phase,
		       rows_total, rows_transferred, rate_per_sec, read_rate, write_rate,
		       tables_total, tables_done, tables_failed,
		       goroutines, mem_alloc_mb, mem_sys_mb, cpu_percent
		FROM migration_stats
		WHERE migration_id = ?
		ORDER BY timestamp`, migrationID)
	return records, err
}

// GetMigrationStatsSummary returns the first and last stats records for a migration,
// useful for showing a quick summary without loading all records.
func (m *MetaDB) GetMigrationStatsSummary(ctx context.Context, migrationID string) (first, last *MigrationStatsRecord, err error) {
	records, err := m.GetMigrationStats(ctx, migrationID)
	if err != nil || len(records) == 0 {
		return nil, nil, err
	}
	return &records[0], &records[len(records)-1], nil
}

// DeleteMigrationStats removes all stats records for a migration.
func (m *MetaDB) DeleteMigrationStats(ctx context.Context, migrationID string) error {
	_, err := m.db.ExecContext(ctx, `DELETE FROM migration_stats WHERE migration_id = ?`, migrationID)
	return err
}
