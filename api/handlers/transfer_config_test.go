package handlers

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// A project can't be saved with a negative worker count or batch size (one
// was saved with table_workers -75 from a number box); 0 is the default.
func TestProjectRejectsNegativeTransferSettings(t *testing.T) {
	h, _, cleanup := newTestHandler(t)
	defer cleanup()
	n := 0
	create := func(transfer string) *httptest.ResponseRecorder {
		n++
		body := `{"name":"p` + strings.Repeat("x", n) + `","source_config":{"type":"sqlite","database":"/tmp/a.db"},` +
			`"target_config":{"type":"sqlite","database":"/tmp/b.db"},"transfer_config":` + transfer + `}`
		rec := httptest.NewRecorder()
		h.CreateProject(rec, httptest.NewRequest(http.MethodPost, "/api/v1/projects", strings.NewReader(body)))
		return rec
	}
	for _, bad := range []string{`{"table_workers":-75}`, `{"segment_workers":-1}`, `{"batch_size":-5}`} {
		if rec := create(bad); rec.Code != http.StatusBadRequest || !strings.Contains(rec.Body.String(), "must be 0 or more") {
			t.Errorf("create with %s: %d %s", bad, rec.Code, rec.Body.String())
		}
	}
	if rec := create(`{"table_workers":0,"segment_workers":0,"batch_size":0}`); rec.Code != http.StatusCreated {
		t.Fatalf("create with defaults: %d %s", rec.Code, rec.Body.String())
	}
	id := strings.Split(strings.Split(create(`{"table_workers":4,"batch_size":500}`).Body.String(), `"id":"`)[1], `"`)[0]
	req := httptest.NewRequest(http.MethodPut, "/api/v1/projects/"+id, strings.NewReader(
		`{"name":"x","source_config":{"type":"sqlite","database":"/tmp/a.db"},"target_config":{"type":"sqlite","database":"/tmp/b.db"},"transfer_config":{"table_workers":-75}}`))
	req.SetPathValue("id", id)
	rec := httptest.NewRecorder()
	h.UpdateProject(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("update with table_workers -75: %d %s", rec.Code, rec.Body.String())
	}
}
