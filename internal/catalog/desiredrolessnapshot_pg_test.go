package catalog

import (
	"context"
	"fmt"
	"testing"

	"github.com/jackc/pgx/v5"
)

// writeBeforeGeneration commits a desired-state row into the gap between
// the last data read of LoadDesiredRoles and its generation read, which is
// the only window the bug lives in and is far too narrow to hit by timing.
type writeBeforeGeneration struct {
	pgx.Tx
	reads int
	write func()
}

func (w *writeBeforeGeneration) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	// LoadDesiredRoles reads roles, role_members, grants, role_settings and
	// then the generation. The write goes in after the fourth.
	if w.reads == 4 {
		w.write()
	}
	w.reads++
	return w.Tx.Query(ctx, sql, args...)
}

// seam wraps the transaction LoadDesiredRoles opens so the write lands
// mid-load. It takes the isolation level the caller asked for and leaves it
// alone unless force names another -- so the subtest that asserts the fix
// runs at whatever LoadDesiredRoles itself requested, and only the control
// overrides it.
type seam struct {
	inner Beginner
	force pgx.TxIsoLevel
	asked pgx.TxIsoLevel
	write func()
}

func (s *seam) BeginTx(ctx context.Context, opts pgx.TxOptions) (pgx.Tx, error) {
	s.asked = opts.IsoLevel
	if s.force != "" {
		opts.IsoLevel = s.force
	}
	tx, err := s.inner.BeginTx(ctx, opts)
	if err != nil {
		return nil, err
	}
	return &writeBeforeGeneration{Tx: tx, write: s.write}, nil
}

// TestTheDesiredRolesLoadIsOneSnapshot: the generation LoadDesiredRoles
// returns is the promise that the desired state beside it is what that
// generation names. A group is recorded in pgshard.role_group_status at the
// generation it was materialized from, and every waiter treats a group at
// the current generation as up to date -- so a generation that outruns its
// own data records a group as carrying a change it never received, and the
// repair falls to the verifier's drift check instead.
//
// READ COMMITTED takes a fresh snapshot per statement, inside a transaction
// as much as outside one, so wrapping the five reads in a transaction fixes
// nothing unless the isolation level makes the snapshot hold. The control
// subtest is here because that is not visible from reading the fixed code:
// it forces READ COMMITTED through the same seam and shows the split answer
// come back.
func TestTheDesiredRolesLoadIsOneSnapshot(t *testing.T) {
	requireDocker(t)
	ctx := context.Background()
	dsn := startPostgres(t, candidateImages[0])
	conn := connect(t, dsn)
	if err := Migrate(ctx, conn); err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Exec(ctx, `INSERT INTO pgshard.roles (rolname, login) VALUES ('app', true)`); err != nil {
		t.Fatal(err)
	}

	// The interloper needs its own connection: the load holds the one it
	// was given for the whole transaction.
	writer := connect(t, dsn)
	n := 0
	// Each call inserts a row under its own name and leaves it there. It
	// cannot clean up after itself: deleting the row again drops
	// role_settings back to its previous max, and the generation with it.
	var writeErr error
	write := func() {
		n++
		if _, err := writer.Exec(ctx,
			`INSERT INTO pgshard.role_settings (rolname, database, name, value) VALUES ('app', '', $1, '64MB')`,
			settingName(n)); err != nil {
			writeErr = err
		}
	}

	// load runs LoadDesiredRoles with one write committed into the gap, and
	// returns what it read, the generation before the load, and the name of
	// the setting that appeared during it.
	load := func(t *testing.T, force pgx.TxIsoLevel) (*DesiredRoles, int64, string) {
		t.Helper()
		was := n
		writeErr = nil
		before := generationNow(t, ctx, conn)
		sm := &seam{inner: conn, force: force, write: write}
		d, err := LoadDesiredRoles(ctx, sm)
		if err != nil {
			t.Fatal(err)
		}
		if force == "" && sm.asked != pgx.RepeatableRead {
			t.Fatalf("LoadDesiredRoles asked for %q, so the subtest below is not testing the snapshot it claims to", sm.asked)
		}
		if writeErr != nil {
			t.Fatalf("interloping write: %v", writeErr)
		}
		if n != was+1 {
			t.Fatalf("the interloping write ran %d times, not once; the seam counts the wrong read", n-was)
		}
		if got := generationNow(t, ctx, conn); got <= before {
			t.Fatalf("the interloping write did not move the generation (%d -> %d)", before, got)
		}
		return d, before, settingName(n)
	}

	has := func(d *DesiredRoles, name string) bool {
		for _, st := range d.Settings {
			if st.Name == name {
				return true
			}
		}
		return false
	}

	t.Run("the_load_answers_from_one_snapshot", func(t *testing.T) {
		d, before, appeared := load(t, "")
		if d.Generation != before {
			t.Fatalf("generation %d, but the load began at %d: it reported a change it did not read",
				d.Generation, before)
		}
		if has(d, appeared) {
			t.Fatalf("%s was written during the load and read anyway; the snapshot did not hold", appeared)
		}
	})

	t.Run("read_committed_splits_the_answer", func(t *testing.T) {
		d, before, appeared := load(t, pgx.ReadCommitted)
		if has(d, appeared) {
			t.Fatalf("%s was read by the data queries, which run BEFORE the write; the seam is in the wrong place", appeared)
		}
		if d.Generation == before {
			t.Fatalf("the write committed in the gap (the generation moved past %d on another connection) "+
				"but the generation read still returned %d under READ COMMITTED: the seam is not forcing "+
				"the isolation level it thinks it is", before, d.Generation)
		}
		t.Logf("as expected: generation %d beside data as of %d, which is the split this fix removes", d.Generation, before)
	})
}

// settingName is a real GUC per call: pgshard.role_settings refuses a
// setting that may not be set per role (0044), so the interloper cannot
// just number its rows.
func settingName(n int) string {
	names := []string{"work_mem", "maintenance_work_mem", "temp_buffers"}
	if n < 1 || n > len(names) {
		panic(fmt.Sprintf("the interloper ran %d times and has only %d names", n, len(names)))
	}
	return names[n-1]
}

func generationNow(t *testing.T, ctx context.Context, conn Querier) int64 {
	t.Helper()
	rows, err := conn.Query(ctx, `SELECT pgshard.roles_desired_generation()`)
	if err != nil {
		t.Fatal(err)
	}
	gen, err := pgx.CollectOneRow(rows, pgx.RowTo[int64])
	if err != nil {
		t.Fatal(err)
	}
	return gen
}
