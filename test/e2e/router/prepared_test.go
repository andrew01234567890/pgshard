//go:build integration

package router

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"
)

// TestRouterPreparedStatementsSurviveBackendReuse drives the router the way
// pgx's default statement cache does: named prepared statements that stay
// cached across shard hops and multi-shard transactions. A reused backend
// must never be asked to PREPARE a name it still holds (42P05).
func TestRouterPreparedStatementsSurviveBackendReuse(t *testing.T) {
	s := startShardedStackWith(t, []string{preparedXacts}, []string{preparedXacts})
	ctx := context.Background()
	t0, t1 := twoTenants(t)
	setup := s.connect(t)
	s.awaitSharded(t, setup)
	_ = setup.Close(ctx)

	const duration = 30 * time.Second

	t.Run("concurrent multi-shard 2PC workers", func(t *testing.T) {
		var wg sync.WaitGroup
		errs := make(chan error, 64)
		stop := time.Now().Add(duration)
		for w := range 4 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				conn := s.connect(t)
				defer func() { _ = conn.Close(ctx) }()
				base := 1_000_000 * (w + 1)
				for i := 0; time.Now().Before(stop); i++ {
					id := base + i
					tx, err := conn.Begin(ctx)
					if err != nil {
						errs <- fmt.Errorf("worker %d begin: %w", w, err)
						return
					}
					for _, tenant := range []int64{t0, t1} {
						if _, err := tx.Exec(ctx, "insert into orders (tenant_id, id) values ($1, $2)", tenant, id); err != nil {
							errs <- fmt.Errorf("worker %d insert %d: %w", w, id, err)
							_ = tx.Rollback(ctx)
							return
						}
					}
					var n int
					if err := tx.QueryRow(ctx, "select count(*) from orders where tenant_id = $1 and id = $2", t0, id).Scan(&n); err != nil {
						errs <- fmt.Errorf("worker %d select: %w", w, err)
						_ = tx.Rollback(ctx)
						return
					}
					if err := tx.Commit(ctx); err != nil {
						errs <- fmt.Errorf("worker %d commit %d: %w", w, id, err)
						return
					}
				}
			}()
		}
		wg.Wait()
		close(errs)
		for err := range errs {
			t.Errorf("%v", err)
		}
		if t.Failed() {
			t.Logf("router log:\n%s", s.routerLog.String())
		}
	})

	t.Run("one session hops shards with the same prepared statement", func(t *testing.T) {
		conn := s.connect(t)
		defer func() { _ = conn.Close(ctx) }()
		stop := time.Now().Add(duration)
		for i := 0; time.Now().Before(stop); i++ {
			tenant := t0
			if i%2 == 1 {
				tenant = t1
			}
			var n int
			if err := conn.QueryRow(ctx, "select count(*) from orders where tenant_id = $1", tenant).Scan(&n); err != nil {
				t.Fatalf("iteration %d: %v\n%s", i, err, s.routerLog.String())
			}
			if i%7 == 0 {
				if _, err := conn.Exec(ctx, "insert into orders (tenant_id, id) values ($1, $2)", tenant, 9_000_000+i); err != nil {
					t.Fatalf("iteration %d insert: %v\n%s", i, err, s.routerLog.String())
				}
			}
		}
	})
}

// TestSQLPrepareOutlivesItsTransaction pins pgshard to what PostgreSQL
// actually does with SQL-level PREPARE, which is not what the documentation
// leads people to expect: a prepared statement is backend state, not
// transaction state. Checked directly against 18 and 19 -- a PREPARE
// survives both an explicit ROLLBACK and a transaction aborted by an error,
// a DEALLOCATE is not undone by a rollback, and ROLLBACK TO a savepoint does
// not drop a PREPARE made after it.
//
// The router replays sqlPrepared onto a backend it moves a session to, so
// the interesting half is the second: the same names must still execute
// after the session has been forced onto another backend, and must behave
// the same either side of that move.
func TestSQLPrepareOutlivesItsTransaction(t *testing.T) {
	s := startShardedStackWith(t, []string{preparedXacts}, []string{preparedXacts})
	ctx := context.Background()
	t0, t1 := twoTenants(t)
	conn := s.connect(t)
	s.awaitSharded(t, conn)
	defer func() { _ = conn.Close(ctx) }()

	mustExec := func(sql string, args ...any) {
		t.Helper()
		if _, err := conn.Exec(ctx, sql, args...); err != nil {
			t.Fatalf("%s: %v", sql, err)
		}
	}
	executes := func(name string, want bool) {
		t.Helper()
		_, err := conn.Exec(ctx, "execute "+name)
		switch {
		case want && err != nil:
			t.Fatalf("execute %s: %v", name, err)
		case !want && err == nil:
			t.Fatalf("execute %s succeeded; the statement should be gone", name)
		}
	}

	mustExec("prepare kept as select 1")
	mustExec("begin")
	mustExec("prepare made_in_txn as select 2")
	mustExec("deallocate kept")
	mustExec("rollback")

	// PostgreSQL's answer, which is the one pgshard must give: the PREPARE
	// stands and the DEALLOCATE stands, whatever the transaction did.
	executes("made_in_txn", true)
	executes("kept", false)

	// An error inside the transaction is the commoner way to reach a
	// rollback, and it makes no difference.
	mustExec("begin")
	mustExec("prepare made_before_error as select 3")
	if _, err := conn.Exec(ctx, "select 1/0"); err == nil {
		t.Fatal("select 1/0 did not fail")
	}
	mustExec("rollback")
	executes("made_before_error", true)

	// A savepoint does not scope it either.
	mustExec("begin")
	mustExec("savepoint sp")
	mustExec("prepare made_after_savepoint as select 4")
	mustExec("rollback to sp")
	mustExec("commit")
	executes("made_after_savepoint", true)

	// Force the session onto another backend: a multi-shard transaction
	// moves it, and the router replays what it holds. The same three names
	// must behave identically on the other side.
	mustExec("begin")
	for _, tenant := range []int64{t0, t1} {
		mustExec("insert into orders (tenant_id, id) values ($1, 424242)", tenant)
	}
	mustExec("commit")

	for _, name := range []string{"made_in_txn", "made_before_error", "made_after_savepoint"} {
		executes(name, true)
	}
	executes("kept", false)
}
