package config

import (
	"os"
	"testing"
)

func TestExpand_NoOp(t *testing.T) {
	got, err := expand("plain string with no vars")
	if err != nil {
		t.Fatal(err)
	}
	if got != "plain string with no vars" {
		t.Errorf("got %q", got)
	}
}

func TestExpand_Set(t *testing.T) {
	t.Setenv("FOO", "bar")
	got, err := expand("hello ${FOO}")
	if err != nil {
		t.Fatal(err)
	}
	if got != "hello bar" {
		t.Errorf("got %q", got)
	}
}

func TestExpand_Default(t *testing.T) {
	os.Unsetenv("XFERDB_TEST_BAZ")
	got, err := expand("v=${XFERDB_TEST_BAZ:-fallback}")
	if err != nil {
		t.Fatal(err)
	}
	if got != "v=fallback" {
		t.Errorf("got %q", got)
	}
}

func TestExpand_Missing(t *testing.T) {
	os.Unsetenv("XFERDB_TEST_MISSING")
	_, err := expand("${XFERDB_TEST_MISSING}")
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestExpand_BareDollarUnset(t *testing.T) {
	os.Unsetenv("XFERDB_TEST_BARE")
	got, err := expand("value=$XFERDB_TEST_BARE")
	if err != nil {
		t.Fatal(err)
	}
	if got != "value=" {
		t.Errorf("got %q", got)
	}
}
