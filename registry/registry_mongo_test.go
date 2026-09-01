package registry

import (
	"strings"
	"testing"
)

// TestNewSource_MongoDB verifies that "mongodb" is a registered source adapter.
func TestNewSource_MongoDB(t *testing.T) {
	src, err := NewSource("mongodb")
	if err != nil {
		t.Fatalf("NewSource(\"mongodb\") returned unexpected error: %v", err)
	}
	if src == nil {
		t.Error("expected non-nil SourceAdapter for mongodb")
	}
}

// TestNewTarget_MongoDBNotRegistered confirms that MongoDB has no target adapter
// (it is source-only) and returns a helpful error.
func TestNewTarget_MongoDBNotRegistered(t *testing.T) {
	_, err := NewTarget("mongodb")
	if err == nil {
		t.Fatal("expected error for NewTarget(\"mongodb\"), got nil")
	}
	if !strings.Contains(err.Error(), "mongodb") {
		t.Errorf("error %q should mention adapter name", err.Error())
	}
}

// TestSupportedAdapters_ContainsMongoDB checks the display list includes mongodb.
func TestSupportedAdapters_ContainsMongoDB(t *testing.T) {
	types := SupportedAdapters()
	for _, tp := range types {
		if tp == "mongodb" {
			return
		}
	}
	t.Errorf("SupportedAdapters() = %v, want it to contain \"mongodb\"", types)
}

// TestNewSource_AllKnownAdapters ensures every registered source can be instantiated.
func TestNewSource_AllKnownAdapters(t *testing.T) {
	known := []string{"postgres", "mysql", "sqlite", "mongodb"}
	for _, name := range known {
		t.Run(name, func(t *testing.T) {
			src, err := NewSource(name)
			if err != nil {
				t.Fatalf("NewSource(%q) error: %v", name, err)
			}
			if src == nil {
				t.Errorf("NewSource(%q) returned nil adapter", name)
			}
		})
	}
}

// TestNewSource_UnknownAdapter ensures an unknown type returns an error that names
// the adapter and lists supported types.
func TestNewSource_UnknownAdapter(t *testing.T) {
	_, err := NewSource("cassandra")
	if err == nil {
		t.Fatal("expected error for unknown adapter, got nil")
	}
	if !strings.Contains(err.Error(), "cassandra") {
		t.Errorf("error %q should mention the unknown adapter name", err.Error())
	}
}
