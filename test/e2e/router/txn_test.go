//go:build integration

package router

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/andrew01234567890/pgshard/internal/controller"
	"github.com/andrew01234567890/pgshard/internal/router"
)

const preparedXacts = "-c max_prepared_transactions=64"

// twoTenants finds one tenant per shard.
func twoTenants(tb testing.TB) (t0, t1 int64) {
	t0, t1 = -1, -1
	for i := int64(1); i < 50 && (t0 < 0 || t1 < 0); i++ {
		if shardOf(tb, i) == 0 && t0 < 0 {
			t0 = i
		}
		if shardOf(tb, i) == 1 && t1 < 0 {
			t1 = i
		}
	}
	return t0, t1
}

func (s *shardedStack) appDSN(shard int) string {
	dsn := s.shardDSN
	if shard == 1 {
		dsn = s.shard1DSN
	}
	return strings.Replace(dsn, "/postgres?", "/"+appDatabase+"?", 1)
}

func (s *shardedStack) preparedOn(tb testing.TB, shard int) []string {
	tb.Helper()
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, s.appDSN(shard))
	if err != nil {
		tb.Fatal(err)
	}
	defer func() { _ = conn.Close(ctx) }()
	rows, err := conn.Query(ctx, "select gid from pg_prepared_xacts order by gid")
	if err != nil {
		tb.Fatal(err)
	}
	gids, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		tb.Fatal(err)
	}
	return gids
}

func (s *shardedStack) decisions(tb testing.TB) []string {
	tb.Helper()
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, s.catalogDSN)
	if err != nil {
		tb.Fatal(err)
	}
	defer func() { _ = conn.Close(ctx) }()
	rows, err := conn.Query(ctx, "select gid || ':' || state from pgshard.xact_decisions order by gid")
	if err != nil {
		tb.Fatal(err)
	}
	out, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		tb.Fatal(err)
	}
	return out
}

// resolver builds the controller's resolver against the stack's shards.
func (s *shardedStack) resolver(tb testing.TB) *controller.Resolver {
	tb.Helper()
	pool, err := pgxpool.New(context.Background(), s.catalogDSN)
	if err != nil {
		tb.Fatal(err)
	}
	tb.Cleanup(pool.Close)
	dsns := map[controller.ShardRef]string{
		{Set: router.DefaultShardSet, ID: 0}: s.appDSN(0),
		{Set: router.DefaultShardSet, ID: 1}: s.appDSN(1),
	}
	return &controller.Resolver{Pool: pool, Shards: &controller.PgxShardDialer{Pool: pool, DSNs: dsns}, PreparingTimeout: time.Millisecond}
}

func TestRouterCrossShardTransactions(t *testing.T) {
	s := startShardedStackWith(t, []string{preparedXacts}, []string{preparedXacts})
	ctx := context.Background()
	t0, t1 := twoTenants(t)
	conn := s.connect(t)
	s.awaitSharded(t, conn)

	t.Run("commit is atomic across shards", func(t *testing.T) {
		tx, err := conn.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := tx.Exec(ctx, "insert into orders (tenant_id, id) values ($1, 1)", t0); err != nil {
			t.Fatal(err)
		}
		if _, err := tx.Exec(ctx, "insert into orders (tenant_id, id) values ($1, 1)", t1); err != nil {
			t.Fatalf("second shard: %v\n%s", err, s.routerLog.String())
		}
		if err := tx.Commit(ctx); err != nil {
			t.Fatalf("commit: %v\n%s", err, s.routerLog.String())
		}
		if s.rowsOn(t, 0, t0) != 1 || s.rowsOn(t, 1, t1) != 1 {
			t.Fatalf("rows: shard0=%d shard1=%d", s.rowsOn(t, 0, t0), s.rowsOn(t, 1, t1))
		}
		if got := s.decisions(t); len(got) != 0 {
			t.Fatalf("decision rows left after a clean commit: %v", got)
		}
		if p0, p1 := s.preparedOn(t, 0), s.preparedOn(t, 1); len(p0)+len(p1) != 0 {
			t.Fatalf("prepared transactions left: %v %v", p0, p1)
		}
	})

	t.Run("rollback undoes both shards", func(t *testing.T) {
		tx, err := conn.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		for _, tenant := range []int64{t0, t1} {
			if _, err := tx.Exec(ctx, "insert into orders (tenant_id, id) values ($1, 2)", tenant); err != nil {
				t.Fatal(err)
			}
		}
		if err := tx.Rollback(ctx); err != nil {
			t.Fatal(err)
		}
		if s.rowsOn(t, 0, t0) != 1 || s.rowsOn(t, 1, t1) != 1 {
			t.Fatalf("rows after rollback: shard0=%d shard1=%d", s.rowsOn(t, 0, t0), s.rowsOn(t, 1, t1))
		}
	})

	t.Run("single mode refuses the second writable shard", func(t *testing.T) {
		if _, err := conn.Exec(ctx, "set pgshard.transaction_mode = single"); err != nil {
			t.Fatal(err)
		}
		defer func() { _, _ = conn.Exec(ctx, "reset pgshard.transaction_mode") }()
		tx, err := conn.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := tx.Exec(ctx, "insert into orders (tenant_id, id) values ($1, 3)", t0); err != nil {
			t.Fatal(err)
		}
		_, err = tx.Exec(ctx, "insert into orders (tenant_id, id) values ($1, 3)", t1)
		var pe *pgconn.PgError
		if !errors.As(err, &pe) || pe.Code != "0A000" || !strings.Contains(pe.Message, "pgshard.transaction_mode is single") {
			t.Fatalf("expected 0A000 single-mode refusal, got %v", err)
		}
		if err := tx.Rollback(ctx); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("read on another shard needs no prepare", func(t *testing.T) {
		tx, err := conn.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := tx.Exec(ctx, "insert into orders (tenant_id, id) values ($1, 4)", t0); err != nil {
			t.Fatal(err)
		}
		var n int
		if err := tx.QueryRow(ctx, "select count(*) from orders where tenant_id = $1", t1).Scan(&n); err != nil || n != 1 {
			t.Fatalf("read on shard 1 inside the transaction: n=%d %v", n, err)
		}
		if err := tx.Commit(ctx); err != nil {
			t.Fatal(err)
		}
		if s.rowsOn(t, 0, t0) != 2 {
			t.Fatalf("shard 0 rows %d", s.rowsOn(t, 0, t0))
		}
	})

	t.Run("orphan prepared transaction is rolled back and a committed decision is honoured", func(t *testing.T) {
		shard1, err := pgx.Connect(ctx, s.appDSN(1))
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = shard1.Close(ctx) }()
		for _, sql := range []string{"begin", "insert into orders (tenant_id, id) values (" + fmt.Sprint(t1) + ", 90)", "prepare transaction 'pgshard-orphan-1'"} {
			if _, err := shard1.Exec(ctx, sql); err != nil {
				t.Fatalf("%s: %v", sql, err)
			}
		}
		for _, sql := range []string{"begin", "insert into orders (tenant_id, id) values (" + fmt.Sprint(t1) + ", 91)", "prepare transaction 'pgshard-decided-1'"} {
			if _, err := shard1.Exec(ctx, sql); err != nil {
				t.Fatalf("%s: %v", sql, err)
			}
		}
		cat, err := pgx.Connect(ctx, s.catalogDSN)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = cat.Close(ctx) }()
		if _, err := cat.Exec(ctx, "insert into pgshard.xact_decisions (gid, state, participants, decided_at) values ('pgshard-decided-1', 'commit', '{1}', now())"); err != nil {
			t.Fatal(err)
		}
		out, err := s.resolver(t).Resolve(ctx, "")
		if err != nil {
			t.Fatal(err)
		}
		if out.Committed != 1 || out.RolledBack != 1 || out.Unresolved != 0 {
			t.Fatalf("outcome %+v", out)
		}
		if got := s.preparedOn(t, 1); len(got) != 0 {
			t.Fatalf("prepared left on shard 1: %v", got)
		}
		if s.rowsOn(t, 1, t1) != 2 {
			t.Fatalf("shard 1 rows %d: the committed decision must be applied and the orphan undone", s.rowsOn(t, 1, t1))
		}
	})
}

func TestRouterRefusesTwoPhaseWithoutPreparedCapacity(t *testing.T) {
	s := startShardedStackWith(t, []string{preparedXacts}, nil)
	ctx := context.Background()
	t0, t1 := twoTenants(t)
	conn := s.connect(t)
	s.awaitSharded(t, conn)
	tx, err := conn.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, "insert into orders (tenant_id, id) values ($1, 1)", t0); err != nil {
		t.Fatal(err)
	}
	_, err = tx.Exec(ctx, "insert into orders (tenant_id, id) values ($1, 1)", t1)
	var pe *pgconn.PgError
	if !errors.As(err, &pe) || pe.Code != "0A000" || !strings.Contains(pe.Message, "shard default/1 has max_prepared_transactions = 0") {
		t.Fatalf("expected 0A000 capacity refusal, got %v", err)
	}
	if err := tx.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	if s.rowsOn(t, 0, t0) != 0 {
		t.Fatalf("shard 0 rows %d", s.rowsOn(t, 0, t0))
	}
}

// TestRouterCrashMatrix kills the router at every point of the commit
// protocol and checks the resolver brings both shards to the recorded
// decision.
func TestRouterCrashMatrix(t *testing.T) {
	s := startShardedStackWith(t, []string{preparedXacts}, []string{preparedXacts})
	_, crashRouter := buildBinariesTagged(t, "pgshard_crashpoints")
	ctx := context.Background()
	t0, t1 := twoTenants(t)
	cases := []struct {
		point     string
		committed bool
		// prepared is how many participants hold a prepared transaction
		// when the router dies.
		prepared int
	}{
		{"before_prepare", false, 0},
		{"after_prepare", false, 2},
		{"after_decision", true, 2},
		{"during_commit_prepared", true, 1},
	}
	for i, c := range cases {
		t.Run(c.point, func(t *testing.T) {
			id := 100 + i
			p := &routerProc{log: &logBuffer{}}
			port := freePort(t)
			p.addr = fmt.Sprintf("127.0.0.1:%d", port)
			p.cmd, p.exited = startProcessEnv(t, p.log, "listening on", []string{"PGSHARD_TEST_CRASH_POINT=" + c.point}, crashRouter,
				"serve", "--insecure-dev", "--listen", "0.0.0.0:"+fmt.Sprint(port), "--catalog-dsn", s.catalogDSN,
				"--instance-id", fmt.Sprint(10+i), "--drain-timeout", "5s", "--drain-delay", "1s")
			conn := s.connectTo(t, p.addr)
			s.awaitSharded(t, conn)
			tx, err := conn.Begin(ctx)
			if err != nil {
				t.Fatal(err)
			}
			for _, tenant := range []int64{t0, t1} {
				if _, err := tx.Exec(ctx, "insert into orders (tenant_id, id) values ($1, $2)", tenant, id); err != nil {
					t.Fatal(err)
				}
			}
			cctx, cancel := context.WithTimeout(ctx, 20*time.Second)
			defer cancel()
			err = tx.Commit(cctx)
			if err == nil {
				t.Fatalf("commit succeeded although the router was armed to crash at %s", c.point)
			}
			select {
			case <-p.exited:
			case <-time.After(20 * time.Second):
				t.Fatalf("router did not die at %s:\n%s", c.point, p.log.String())
			}
			if st := p.cmd.ProcessState; st == nil || st.String() != "signal: killed" {
				t.Fatalf("router exit %v, want SIGKILL", st)
			}
			decisions := s.decisions(t)
			wantState := "preparing"
			if c.committed {
				wantState = "commit"
			}
			if len(decisions) != 1 || !strings.HasSuffix(decisions[0], ":"+wantState) {
				t.Fatalf("decision rows %v, want one in state %s", decisions, wantState)
			}
			if p0, p1 := s.preparedOn(t, 0), s.preparedOn(t, 1); len(p0)+len(p1) != c.prepared {
				t.Fatalf("prepared at the crash: %v %v, want %d", p0, p1, c.prepared)
			}
			// The resolver reads the decision, never the crash point.
			out, err := s.resolver(t).Resolve(ctx, "")
			if err != nil {
				t.Fatal(err)
			}
			if out.Unresolved != 0 {
				t.Fatalf("outcome %+v", out)
			}
			r0, r1 := rowsWithID(t, s, 0, id), rowsWithID(t, s, 1, id)
			if c.committed && (r0 != 1 || r1 != 1) {
				t.Fatalf("decision commit but rows shard0=%d shard1=%d", r0, r1)
			}
			if !c.committed && (r0 != 0 || r1 != 0) {
				t.Fatalf("decision abort but rows shard0=%d shard1=%d", r0, r1)
			}
			if p0, p1 := s.preparedOn(t, 0), s.preparedOn(t, 1); len(p0)+len(p1) != 0 {
				t.Fatalf("prepared transactions left: %v %v", p0, p1)
			}
			if got := s.decisions(t); len(got) != 0 {
				t.Fatalf("decision rows left: %v", got)
			}
		})
	}
}

func rowsWithID(tb testing.TB, s *shardedStack, shard, id int) int {
	tb.Helper()
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, s.appDSN(shard))
	if err != nil {
		tb.Fatal(err)
	}
	defer func() { _ = conn.Close(ctx) }()
	var n int
	if err := conn.QueryRow(ctx, "select count(*) from orders where id = $1", id).Scan(&n); err != nil {
		tb.Fatal(err)
	}
	return n
}
