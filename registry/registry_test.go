package registry

import (
	"testing"
)

func TestParseConnectionString_MongoDB(t *testing.T) {
	tests := []struct {
		name        string
		dsn         string
		wantType    string
		wantDB      string
		wantDSN     string // full URI must be preserved verbatim
	}{
		{
			name:     "standard mongodb URI",
			dsn:      "mongodb://user:pass@host:27017/mydb",
			wantType: "mongodb",
			wantDB:   "mydb",
			wantDSN:  "mongodb://user:pass@host:27017/mydb",
		},
		{
			name:     "mongodb with replica set and authSource",
			dsn:      "mongodb://user:pass@h1:27017,h2:27017/mydb?replicaSet=rs0&authSource=admin",
			wantType: "mongodb",
			wantDB:   "mydb",
			wantDSN:  "mongodb://user:pass@h1:27017,h2:27017/mydb?replicaSet=rs0&authSource=admin",
		},
		{
			name:     "mongodb+srv (Atlas / DNS SRV)",
			dsn:      "mongodb+srv://user:pass@cluster.example.net/mydb?retryWrites=true&w=majority",
			wantType: "mongodb",
			wantDB:   "mydb",
			wantDSN:  "mongodb+srv://user:pass@cluster.example.net/mydb?retryWrites=true&w=majority",
		},
		{
			name:     "mongodb without database",
			dsn:      "mongodb://host:27017",
			wantType: "mongodb",
			wantDB:   "",
			wantDSN:  "mongodb://host:27017",
		},
		{
			name:     "mongodb with tls option",
			dsn:      "mongodb://host:27017/mydb?tls=true",
			wantType: "mongodb",
			wantDB:   "mydb",
			wantDSN:  "mongodb://host:27017/mydb?tls=true",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg, err := ParseConnectionString(tt.dsn)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if cfg.Type != tt.wantType {
				t.Errorf("Type = %q, want %q", cfg.Type, tt.wantType)
			}
			if cfg.Database != tt.wantDB {
				t.Errorf("Database = %q, want %q", cfg.Database, tt.wantDB)
			}
			if cfg.DSN != tt.wantDSN {
				t.Errorf("DSN = %q, want %q (URI must be preserved verbatim)", cfg.DSN, tt.wantDSN)
			}
			// Individual fields must NOT be populated for MongoDB — the driver
			// takes the full URI; decomposing it would silently drop options.
			if cfg.Host != "" {
				t.Errorf("Host = %q, want empty (MongoDB URIs are not decomposed)", cfg.Host)
			}
			if cfg.Username != "" {
				t.Errorf("Username = %q, want empty (credentials stay in DSN)", cfg.Username)
			}
			if cfg.Password != "" {
				t.Errorf("Password = %q, want empty (credentials stay in DSN)", cfg.Password)
			}
		})
	}
}

func TestParseConnectionString_ExistingAdaptersUnchanged(t *testing.T) {
	tests := []struct {
		name     string
		dsn      string
		wantType string
		wantHost string
		wantDB   string
	}{
		{
			name:     "postgres",
			dsn:      "postgres://user:pass@localhost:5432/mydb?sslmode=require",
			wantType: "postgres",
			wantHost: "localhost",
			wantDB:   "mydb",
		},
		{
			name:     "mysql",
			dsn:      "mysql://user:pass@localhost:3306/mydb",
			wantType: "mysql",
			wantHost: "localhost",
			wantDB:   "mydb",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg, err := ParseConnectionString(tt.dsn)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if cfg.Type != tt.wantType {
				t.Errorf("Type = %q, want %q", cfg.Type, tt.wantType)
			}
			if cfg.Host != tt.wantHost {
				t.Errorf("Host = %q, want %q", cfg.Host, tt.wantHost)
			}
			if cfg.Database != tt.wantDB {
				t.Errorf("Database = %q, want %q", cfg.Database, tt.wantDB)
			}
		})
	}
}

func TestParseConnectionString_UnknownScheme(t *testing.T) {
	_, err := ParseConnectionString("cassandra://host:9042/keyspace")
	if err == nil {
		t.Error("expected error for unknown scheme, got nil")
	}
}
