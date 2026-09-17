package engine

import (
	"context"
	"errors"
	"testing"
	"time"

	"gitea.homelab.local/nextdevops/XferDB/adapters"
)

// TestFailWithCancelledContextMarksCancelled verifies that when ctx has
// already been cancelled — the only way that happens mid-Run is an explicit
// user Cancel/Delete — fail() records the migration as cancelled (not
// failed) with a clean message, and emits EventCancelled rather than
// EventError. This is what makes migration history show "cancelled" instead
// of the driver-level error text produced by stopping mid-transfer (e.g. a
// Postgres "canceling statement due to user request").
func TestFailWithCancelledContextMarksCancelled(t *testing.T) {
	eng, db, cleanup := newTestEngine(t, &offsetSource{}, &devNullTarget{})
	defer cleanup()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	driverErr := errors.New(`pq: canceling statement due to user request (57014)`)
	if err := eng.fail(ctx, driverErr); err != driverErr {
		t.Fatalf("fail() returned %v, want the original error unchanged", err)
	}

	mig, err := db.GetMigration(context.Background(), eng.migrationID)
	if err != nil {
		t.Fatalf("get migration: %v", err)
	}
	if mig.Status != adapters.StatusCancelled {
		t.Errorf("status = %q, want %q", mig.Status, adapters.StatusCancelled)
	}
	if mig.Error != "cancelled by user" {
		t.Errorf("error message = %q, want a clean %q (not the driver error)", mig.Error, "cancelled by user")
	}

	select {
	case ev := <-eng.events:
		if ev.Kind != EventCancelled {
			t.Errorf("event kind = %q, want %q", ev.Kind, EventCancelled)
		}
	case <-time.After(time.Second):
		t.Fatal("no event emitted")
	}
}

// TestFailWithoutCancelledContextMarksFailed is the regression companion:
// a genuine failure (ctx not cancelled) must still mark the migration
// failed with the real error text and emit EventError, unchanged from
// before EventCancelled existed.
func TestFailWithoutCancelledContextMarksFailed(t *testing.T) {
	eng, db, cleanup := newTestEngine(t, &offsetSource{}, &devNullTarget{})
	defer cleanup()

	realErr := errors.New("connection refused")
	if err := eng.fail(context.Background(), realErr); err != realErr {
		t.Fatalf("fail() returned %v, want the original error unchanged", err)
	}

	mig, err := db.GetMigration(context.Background(), eng.migrationID)
	if err != nil {
		t.Fatalf("get migration: %v", err)
	}
	if mig.Status != adapters.StatusFailed {
		t.Errorf("status = %q, want %q", mig.Status, adapters.StatusFailed)
	}
	if mig.Error != realErr.Error() {
		t.Errorf("error message = %q, want %q", mig.Error, realErr.Error())
	}

	select {
	case ev := <-eng.events:
		if ev.Kind != EventError {
			t.Errorf("event kind = %q, want %q", ev.Kind, EventError)
		}
	case <-time.After(time.Second):
		t.Fatal("no event emitted")
	}
}
