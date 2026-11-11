package commands

import (
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"testing"

	"github.com/spf13/pflag"

	"gitea.homelab.local/nextdevops/XferDB/config"
)

// migrateFlags returns a fresh copy of the migrate command's flags, so each
// case starts with nothing Changed.
func migrateFlags(t *testing.T, args ...string) *pflag.FlagSet {
	t.Helper()
	fs := pflag.NewFlagSet("migrate", pflag.ContinueOnError)
	migrateCmd.Flags().VisitAll(func(f *pflag.Flag) {
		switch f.Value.Type() {
		case "int":
			n, _ := strconv.Atoi(f.DefValue)
			fs.Int(f.Name, n, f.Usage)
		case "bool":
			fs.Bool(f.Name, f.DefValue == "true", f.Usage)
		case "string":
			fs.String(f.Name, f.DefValue, f.Usage)
		}
	})
	if err := fs.Parse(args); err != nil {
		t.Fatal(err)
	}
	return fs
}

func clearTransferEnv(t *testing.T) {
	for _, k := range []string{"BATCH_SIZE", "TABLE_WORKERS", "SEGMENT_WORKERS", "BULK_COPY", "ASYNC_PIPELINE"} {
		t.Setenv("XFERDB_"+k, "")
	}
}

// #172: with no flag, env var or config file value, a project setting is left
// out of the request, so the server uses the project's saved transfer_config.
// Only the per-run fields (schema mode, bulk copy) are always sent.
func TestTransferRequest(t *testing.T) {
	clearTransferEnv(t)
	perRunOff := map[string]any{"recreate_schema": false, "truncate": false, "bulk_copy": false}
	with := func(extra map[string]any) map[string]any {
		m := map[string]any{}
		for k, v := range perRunOff {
			m[k] = v
		}
		for k, v := range extra {
			m[k] = v
		}
		return m
	}

	cases := []struct {
		name string
		args []string
		want map[string]any
	}{
		{"no flags: only the per-run fields", nil, perRunOff},
		{"table workers flag", []string{"--table-workers", "3"},
			with(map[string]any{"table_workers": 3})},
		{"flag equal to the old built-in default is still sent", []string{"--table-workers", "1"},
			with(map[string]any{"table_workers": 1})},
		{"segment workers and offset segments", []string{"--segment-workers", "4", "--offset-segments"},
			with(map[string]any{"segment_workers": 4, "offset_fallback": true})},
		{"batch size", []string{"--batch-size", "500"},
			with(map[string]any{"batch_size": 500})},
		{"batch size 0 means the project's", []string{"--batch-size", "0"}, perRunOff},
		{"workers 0 means the project's", []string{"--table-workers", "0", "--segment-workers", "0"}, perRunOff},
		{"async pipeline off is sent", []string{"--async-pipeline=false"},
			with(map[string]any{"async_pipeline": false})},
		{"accurate counts", []string{"--accurate-counts"},
			with(map[string]any{"accurate_counts": true})},
		{"tables", []string{"--tables", " orders, public.customers ,"},
			with(map[string]any{"tables": []string{"orders", "public.customers"}})},
		{"schema mode and bulk copy", []string{"--truncate", "--bulk-copy"},
			map[string]any{"recreate_schema": false, "truncate": true, "bulk_copy": true}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := transferRequest(migrateFlags(t, tc.args...), nil)
			if !reflect.DeepEqual(got, tc.want) {
				t.Errorf("transferRequest(%v)\n got  %v\n want %v", tc.args, got, tc.want)
			}
		})
	}
}

// An env var or the config file's defaults still beat the project's saved
// settings, and a flag beats both.
func TestTransferRequestEnvAndConfig(t *testing.T) {
	clearTransferEnv(t)
	path := filepath.Join(t.TempDir(), "xferdb.yaml")
	yaml := "defaults:\n  table_workers: 4\n  bulk_copy: true\n  async_pipeline: true\n"
	if err := os.WriteFile(path, []byte(yaml), 0o644); err != nil {
		t.Fatal(err)
	}
	loader, err := config.Load(path)
	if err != nil {
		t.Fatal(err)
	}

	got := transferRequest(migrateFlags(t), loader)
	want := map[string]any{"table_workers": 4, "async_pipeline": true, "bulk_copy": true, "recreate_schema": false, "truncate": false}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("config file\n got  %v\n want %v", got, want)
	}

	t.Setenv("XFERDB_SEGMENT_WORKERS", "2")
	got = transferRequest(migrateFlags(t, "--table-workers", "8"), loader)
	if got["table_workers"] != 8 || got["segment_workers"] != 2 {
		t.Errorf("flag over file, env: got %v, want table_workers 8, segment_workers 2", got)
	}
}
