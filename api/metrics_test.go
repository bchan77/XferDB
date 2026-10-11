package api

import (
	"io"
	"net/http"
	"strings"
	"testing"
)

func scrape(t *testing.T, url string) string {
	t.Helper()
	resp, err := http.Get(url + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /metrics: status %d", resp.StatusCode)
	}
	return string(body)
}

// A migration run through the API shows up in GET /metrics.
func TestMetricsEndpointRecordsMigration(t *testing.T) {
	srv, projectID := newSQLiteProject(t)

	if body := scrape(t, srv.URL); !strings.Contains(body, `xferdb_migrations_active{phase="in_progress"} 0`) ||
		!strings.Contains(body, "go_goroutines") {
		t.Fatalf("idle scrape missing xferdb or Go runtime metrics:\n%s", body)
	}

	startMigration(t, srv, projectID, map[string]any{"validation": "off"})

	want := []string{
		`xferdb_rows_transferred_total{project="s2s",table="items"} 3`,
		`xferdb_tables_finished_total{project="s2s",status="completed"} 1`,
		`xferdb_migrations_finished_total{project="s2s",status="completed"} 1`,
	}
	var body string
	waitFor(t, "migration metrics", func() bool {
		body = scrape(t, srv.URL)
		for _, w := range want {
			if !strings.Contains(body, w) {
				return false
			}
		}
		return !strings.Contains(body, "xferdb_migration_phase{")
	})
}
