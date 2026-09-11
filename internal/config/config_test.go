package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"gitea.homelab.local/nextdevops/XferDB/adapters"
)

func TestLoad_YAMLFull(t *testing.T) {
	yaml := `
server:
  addr: ":9090"
  log_level: debug
  log_file: /var/log/xferdb/server.log
  state_dir: /var/lib/xferdb

defaults:
  table_workers: 2
  segment_workers: 1
  batch_size: 5000
  bulk_copy: true
  async_pipeline: true
  on_error: abort

projects:
  - name: prod-to-staging
    description: Hourly clone of prod for staging
    source:
      type: postgres
      dsn: ${PROD_DSN}
    target:
      type: postgres
      dsn: ${STAGING_DSN}
    transfer:
      tables: [orders, customers]
      batch_size: 2500
  - name: mongo-archive
    source:
      type: mongodb
      dsn: mongodb://reader@mongo.internal:27017/shop
    target:
      type: sqlite
      dsn: file:/srv/archive/shop.db
`
	dir := t.TempDir()
	path := filepath.Join(dir, "xferdb.yaml")
	if err := os.WriteFile(path, []byte(yaml), 0o644); err != nil {
		t.Fatal(err)
	}

	t.Setenv("PROD_DSN", "postgres://u:p@db1/p")
	t.Setenv("STAGING_DSN", "postgres://u:p@db2/s")

	loader, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if loader.Path() != path {
		t.Errorf("Path() = %q, want %q", loader.Path(), path)
	}

	srv := loader.Server()
	if srv.Addr != ":9090" {
		t.Errorf("server.addr = %q, want :9090", srv.Addr)
	}
	if srv.LogLevel != "debug" {
		t.Errorf("server.log_level = %q, want debug", srv.LogLevel)
	}

	def := loader.Defaults()
	if def.TableWorkers != 2 || def.SegmentWorkers != 1 || def.BatchSize != 5000 {
		t.Errorf("defaults: %+v", def)
	}
	if !def.BulkCopy || !def.AsyncPipeline {
		t.Errorf("defaults: bulk_copy=%v async=%v, want both true", def.BulkCopy, def.AsyncPipeline)
	}

	projs := loader.Projects()
	if len(projs) != 2 {
		t.Fatalf("len(projects) = %d, want 2", len(projs))
	}

	p := loader.FindProject("prod-to-staging")
	if p == nil {
		t.Fatal("FindProject(prod-to-staging) = nil")
	}
	if p.Source.DSN != "postgres://u:p@db1/p" {
		t.Errorf("source DSN expansion wrong: %q", p.Source.DSN)
	}
	if p.Target.DSN != "postgres://u:p@db2/s" {
		t.Errorf("target DSN expansion wrong: %q", p.Target.DSN)
	}
	if len(p.Transfer.Tables) != 2 {
		t.Errorf("project tables: %v", p.Transfer.Tables)
	}
}

func TestLoad_JSON(t *testing.T) {
	json := `{
  "server": {"addr": ":7070"},
  "defaults": {"batch_size": 250},
  "projects": [
    {"name": "p1", "source": {"type": "sqlite", "dsn": "file:/a.db"}, "target": {"type": "sqlite", "dsn": "file:/b.db"}}
  ]
}`
	dir := t.TempDir()
	path := filepath.Join(dir, "xferdb.json")
	if err := os.WriteFile(path, []byte(json), 0o644); err != nil {
		t.Fatal(err)
	}
	loader, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if loader.Server().Addr != ":7070" {
		t.Errorf("server.addr = %q", loader.Server().Addr)
	}
	if loader.Defaults().BatchSize != 250 {
		t.Errorf("batch_size = %d", loader.Defaults().BatchSize)
	}
	if p := loader.FindProject("p1"); p == nil || p.Source.Type != "sqlite" {
		t.Errorf("project p1 wrong: %+v", p)
	}
}

func TestLoad_TOML_Rejected(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "xferdb.toml")
	if err := os.WriteFile(path, []byte(`[server]`+"\naddr = \":0\"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	_, err := Load(path)
	if err == nil {
		t.Fatal("expected error: TOML is not supported in v0.6.0")
	}
	if !strings.Contains(err.Error(), "TOML") {
		t.Errorf("error should mention TOML: %v", err)
	}
}

func TestLoad_EnvExpansion_Default(t *testing.T) {
	yaml := `
projects:
  - name: p1
    source: {type: postgres, dsn: "postgres://${DB_HOST:-localhost}/p"}
    target: {type: postgres, dsn: "postgres://${DB_HOST:-localhost}/t"}
`
	dir := t.TempDir()
	path := filepath.Join(dir, "xferdb.yaml")
	if err := os.WriteFile(path, []byte(yaml), 0o644); err != nil {
		t.Fatal(err)
	}
	t.Setenv("DB_HOST", "")
	loader, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	p := loader.FindProject("p1")
	if p == nil {
		t.Fatal("project not found")
	}
	if p.Source.DSN != "postgres://localhost/p" {
		t.Errorf("default expansion wrong: %q", p.Source.DSN)
	}
}

func TestLoad_EnvExpansion_Missing(t *testing.T) {
	yaml := `
projects:
  - name: p1
    source: {type: postgres, dsn: "postgres://${MISSING_VAR}/p"}
    target: {type: postgres, dsn: "postgres://x/y"}
`
	dir := t.TempDir()
	path := filepath.Join(dir, "xferdb.yaml")
	if err := os.WriteFile(path, []byte(yaml), 0o644); err != nil {
		t.Fatal(err)
	}
	os.Unsetenv("MISSING_VAR")
	if _, err := Load(path); err == nil {
		t.Fatal("expected error for missing env var, got nil")
	} else if !strings.Contains(err.Error(), "MISSING_VAR") {
		t.Errorf("error should name the missing var: %v", err)
	}
}

func TestLoad_NoFile(t *testing.T) {
	dir := t.TempDir()
	cwd, _ := os.Getwd()
	defer os.Chdir(cwd)
	if err := os.Chdir(dir); err != nil {
		t.Fatal(err)
	}
	os.Unsetenv("XFERDB_CONFIG")
	loader, err := Load("")
	if err != nil {
		t.Fatalf("Load(\"\"): %v", err)
	}
	if loader.Path() != "" {
		t.Errorf("Path() = %q, want empty", loader.Path())
	}
	if loader.Defaults().TableWorkers != 0 {
		t.Errorf("Defaults() not zero: %+v", loader.Defaults())
	}
}

func TestLoad_FileNotFound(t *testing.T) {
	dir := t.TempDir()
	if _, err := Load(filepath.Join(dir, "nope.yaml")); err == nil {
		t.Fatal("expected error for missing file")
	}
}

func TestLoad_InvalidConfig(t *testing.T) {
	cases := []struct {
		name string
		body string
		want string
	}{
		{
			name: "duplicate project name",
			body: `
projects:
  - {name: p1, source: {type: postgres, dsn: x}, target: {type: postgres, dsn: y}}
  - {name: p1, source: {type: postgres, dsn: x}, target: {type: postgres, dsn: y}}
`,
			want: "duplicated",
		},
		{
			name: "unknown adapter",
			body: `
projects:
  - {name: p1, source: {type: oracle, dsn: x}, target: {type: postgres, dsn: y}}
`,
			want: "oracle",
		},
		{
			name: "invalid log level",
			body: `
server: {log_level: verbose}
`,
			want: "log_level",
		},
		{
			name: "invalid on_error",
			body: `
defaults: {on_error: panic}
`,
			want: "on_error",
		},
		{
			name: "negative table_workers",
			body: `
defaults: {table_workers: -1}
`,
			want: "table_workers",
		},
		{
			name: "missing project name",
			body: `
projects:
  - {source: {type: postgres, dsn: x}, target: {type: postgres, dsn: y}}
`,
			want: "name is required",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			path := filepath.Join(dir, "xferdb.yaml")
			if err := os.WriteFile(path, []byte(tc.body), 0o644); err != nil {
				t.Fatal(err)
			}
			_, err := Load(path)
			if err == nil {
				t.Fatalf("expected error containing %q, got nil", tc.want)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Errorf("error = %v, want substring %q", err, tc.want)
			}
		})
	}
}

func TestResolveTransferConfig_Precedence(t *testing.T) {
	yaml := `
defaults:
  batch_size: 5000
  table_workers: 2
  bulk_copy: true
`
	dir := t.TempDir()
	path := filepath.Join(dir, "xferdb.yaml")
	if err := os.WriteFile(path, []byte(yaml), 0o644); err != nil {
		t.Fatal(err)
	}
	loader, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}

	// Baseline: file values.
	got := ResolveTransferConfig(loader, adapters.TransferConfig{BatchSize: 1000, TableWorkers: 1, OnError: adapters.ErrorPolicyAbort})
	if got.BatchSize != 5000 || got.TableWorkers != 2 || !got.BulkCopy {
		t.Errorf("baseline merge wrong: %+v", got)
	}

	// Env override beats file.
	t.Setenv("XFERDB_BATCH_SIZE", "999")
	t.Setenv("XFERDB_BULK_COPY", "false")
	got = ResolveTransferConfig(loader, adapters.TransferConfig{BatchSize: 1000, TableWorkers: 1, OnError: adapters.ErrorPolicyAbort})
	if got.BatchSize != 999 {
		t.Errorf("env override: batch_size = %d, want 999", got.BatchSize)
	}
	if got.BulkCopy {
		t.Errorf("env override: bulk_copy still true; XFERDB_BULK_COPY=false should win")
	}

	// Built-in default preserved when file has nothing to say.
	noFile := &Loader{}
	got = ResolveTransferConfig(noFile, adapters.TransferConfig{BatchSize: 42, TableWorkers: 3, OnError: adapters.ErrorPolicyAbort})
	if got.BatchSize != 42 || got.TableWorkers != 3 {
		t.Errorf("defaults lost: %+v", got)
	}
}

func TestValidate_Nil(t *testing.T) {
	var f *File
	if err := f.Validate(); err == nil {
		t.Error("nil File should fail Validate")
	}
}

func TestValidate_EmptyIsOK(t *testing.T) {
	var f File
	if err := f.Validate(); err != nil {
		t.Errorf("empty File should validate, got: %v", err)
	}
}
