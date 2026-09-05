package operator

import (
	"context"
	"os/exec"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"

	"github.com/andrew01234567890/pgshard/internal/dockertest"
)

// queryProbe reads one value, failing the test on error.
func queryProbe[T any](t *testing.T, conn *pgx.Conn, sql string) T {
	t.Helper()
	var v T
	if err := conn.QueryRow(context.Background(), sql).Scan(&v); err != nil {
		t.Fatalf("%s: %v", sql, err)
	}
	return v
}

// TestDropSlotRemovesAnActiveSlotOnPostgres is the defect a retirement hit
// in the e2e: EnsureSlots drops only an INACTIVE slot, and a member being
// retired streams from its slot until the moment its pod goes, so the drop
// did nothing, said nothing, and left a slot pinning WAL on the primary for
// a standby that was never coming back.
//
// Both halves are asserted against a real walsender, because "active" is a
// state only a real replication connection produces.
func TestDropSlotRemovesAnActiveSlotOnPostgres(t *testing.T) {
	if err := exec.Command("docker", "info").Run(); err != nil {
		dockertest.Unavailable(t, "docker unavailable")
	}
	ctx := context.Background()
	dsn := startProbePostgres(t)
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close(ctx) }()
	mustProbeExec(t, conn, `SELECT pg_create_physical_replication_slot('retired_member', true)`)

	// A physical walsender holds the slot open, which is what a standby
	// does and what makes the slot active. START_REPLICATION puts the
	// connection into CopyBoth and leaves it there, so the command is sent
	// on the frontend and the response read directly rather than through
	// Exec, which waits for a completion that never comes.
	cfg, err := pgconn.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.RuntimeParams == nil {
		cfg.RuntimeParams = map[string]string{}
	}
	cfg.RuntimeParams["replication"] = "true"
	rc, err := pgconn.ConnectConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = rc.Close(ctx) }()
	lsn := queryProbe[string](t, conn, `SELECT pg_current_wal_lsn()::text`)
	fe := rc.Frontend()
	fe.SendQuery(&pgproto3.Query{String: "START_REPLICATION SLOT retired_member PHYSICAL " + lsn})
	if err := fe.Flush(); err != nil {
		t.Fatal(err)
	}
	for streaming := false; !streaming; {
		msg, rerr := rc.ReceiveMessage(ctx)
		if rerr != nil {
			t.Fatalf("start replication: %v", rerr)
		}
		switch m := msg.(type) {
		case *pgproto3.CopyBothResponse:
			streaming = true
		case *pgproto3.ErrorResponse:
			t.Fatalf("start replication: %v", pgconn.ErrorResponseToPgError(m))
		}
	}
	// The slot goes active when the walsender picks it up, which is not
	// the same instant. A failure here is a failure: skipping would make
	// this test indistinguishable from one that never tried.
	active := false
	for i := 0; i < 200 && !active; i++ {
		active = queryProbe[bool](t, conn, `SELECT coalesce(bool_or(active), false) FROM pg_replication_slots WHERE slot_name = 'retired_member'`)
		if !active {
			time.Sleep(50 * time.Millisecond)
		}
	}
	if !active {
		t.Fatal("the slot never went active, so this test would prove nothing")
	}

	// What retirement used to do, and why it silently left the slot.
	if err := (PgxProber{}).EnsureSlots(ctx, dsn, nil, "retired_member"); err != nil {
		t.Fatalf("EnsureSlots: %v", err)
	}
	if n := queryProbe[int64](t, conn, `SELECT count(*) FROM pg_replication_slots WHERE slot_name = 'retired_member'`); n != 1 {
		t.Fatalf("EnsureSlots dropped an active slot after all (%d); this test no longer covers the defect", n)
	}

	if err := (PgxProber{}).DropSlot(ctx, dsn, "retired_member"); err != nil {
		t.Fatalf("DropSlot: %v", err)
	}
	if n := queryProbe[int64](t, conn, `SELECT count(*) FROM pg_replication_slots WHERE slot_name = 'retired_member'`); n != 0 {
		t.Fatalf("the slot is still there (%d)", n)
	}
	// Dropping one that is not there is not an error: a retirement retries.
	if err := (PgxProber{}).DropSlot(ctx, dsn, "retired_member"); err != nil {
		t.Fatalf("DropSlot on a slot that is gone: %v", err)
	}
}
