//go:build integration

package router

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// declareReferenceAndSequences adds a reference table and a sharded table
// with a router-filled sequence column to the sharded stack, in the catalog
// and on both shards.
func (s *shardedStack) declareReferenceAndSequences(tb testing.TB) {
	tb.Helper()
	ctx := context.Background()
	cat, err := pgx.Connect(ctx, s.catalogDSN)
	if err != nil {
		tb.Fatal(err)
	}
	defer func() { _ = cat.Close(ctx) }()
	// One transaction, because the reconciler upserts a status row for any
	// table it finds declared without one, and that upsert clears the
	// inspection these rows are standing in for. Committing a declaration
	// and its status together leaves no moment where it could.
	tx, err := cat.Begin(ctx)
	if err != nil {
		tb.Fatal(err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	for _, sql := range []string{
		`INSERT INTO pgshard.tables (database, schema_name, table_name, placement) VALUES ('app', 'public', 'regions', 'reference')`,
		// Standing in for the controller's inspection pass, which this
		// stack does not run: regions evaluates nothing per shard, and
		// stamped carries a default that every shard would evaluate for
		// itself.
		`INSERT INTO pgshard.table_status (database, schema_name, table_name, effective_placement, reference_checked_generation) VALUES ('app', 'public', 'regions', 'reference', 0)`,
		`INSERT INTO pgshard.tables (database, schema_name, table_name, placement) VALUES ('app', 'public', 'stamped', 'reference')`,
		`INSERT INTO pgshard.table_status (database, schema_name, table_name, effective_placement, reference_checked_generation, reference_hazards)
			VALUES ('app', 'public', 'stamped', 'reference', 0, '{"the default of column seen calls now(), which pg_proc marks STABLE"}')`,
		`INSERT INTO pgshard.tables (database, schema_name, table_name, placement, shard_key, sequence_columns) VALUES ('app', 'public', 'tickets', 'sharded', 'tenant_id', '{id}')`,
		`INSERT INTO pgshard.table_status (database, schema_name, table_name, effective_placement, effective_shard_key) VALUES ('app', 'public', 'tickets', 'sharded', 'tenant_id')`,
	} {
		if _, err := tx.Exec(ctx, sql); err != nil {
			tb.Fatalf("%s: %v", sql, err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		tb.Fatal(err)
	}
	for shard := 0; shard < 2; shard++ {
		conn, err := pgx.Connect(ctx, s.appDSN(shard))
		if err != nil {
			tb.Fatal(err)
		}
		if _, err := conn.Exec(ctx, `create table regions (id int primary key, name text, stamp timestamptz);
			create table stamped (id int primary key, seen timestamptz default now());
			create table tickets (tenant_id int8, id int8, body text, primary key (tenant_id, id));
			grant all on regions, tickets to `+appRole); err != nil {
			tb.Fatal(err)
		}
		_ = conn.Close(ctx)
	}
}

// awaitReference waits until the router knows regions as a reference table
// (a volatile write is then refused by the router itself).
//
// The probe is a write, and until the router has learned the placement
// nothing refuses it: every poll before the last one inserts a row for
// real. How many of those there are is a race between the catalog
// reaching the router and this loop, so on a loaded runner the table
// starts out holding rows a test that counts them never wrote. The probe
// cleans up after itself for that reason.
func (s *shardedStack) awaitReference(tb testing.TB, conn *pgx.Conn) {
	tb.Helper()
	ctx := context.Background()
	deadline := time.Now().Add(30 * time.Second)
	for {
		// The mode has to be the first argument pgx sees, so the id is in
		// the statement rather than a parameter.
		_, err := conn.Exec(ctx, fmt.Sprintf("insert into regions (id, name) values (%d, now()::text)", probeID), pgx.QueryExecModeSimpleProtocol)
		if sqlstate(err) == "0A000" && strings.Contains(err.Error(), "cannot call now()") {
			// Straight to each shard, not through the router: the landed
			// probes were routed by hash before the placement was known,
			// so they are spread, and one subtest arms the router to die
			// on its next multi-shard write -- which a delete through it
			// would be, killing the router before the test's own.
			s.clearProbeRows(tb)
			return
		}
		if time.Now().After(deadline) {
			tb.Fatalf("router never learned the reference placement (last: %v)\nrouter log:\n%s", err, s.routerLog.String())
		}
		time.Sleep(200 * time.Millisecond)
	}
}

// probeID is the row awaitReference writes while it waits. No test uses it
// for anything else, so a leftover is always the probe's.
const probeID = 0

func (s *shardedStack) clearProbeRows(tb testing.TB) {
	tb.Helper()
	ctx := context.Background()
	for shard := 0; shard < 2; shard++ {
		conn, err := pgx.Connect(ctx, s.appDSN(shard))
		if err != nil {
			tb.Fatal(err)
		}
		_, err = conn.Exec(ctx, "delete from regions where id = $1", probeID)
		_ = conn.Close(ctx)
		if err != nil {
			tb.Fatalf("clearing the reference probe rows on shard %d: %v", shard, err)
		}
	}
}

func (s *shardedStack) regionsOn(tb testing.TB, shard int) []string {
	tb.Helper()
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, s.appDSN(shard))
	if err != nil {
		tb.Fatal(err)
	}
	defer func() { _ = conn.Close(ctx) }()
	rows, err := conn.Query(ctx, "select id || ':' || name from regions order by id")
	if err != nil {
		tb.Fatal(err)
	}
	out, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		tb.Fatal(err)
	}
	return out
}

func TestRouterReferenceTableWrites(t *testing.T) {
	s := startShardedStackWith(t, []string{preparedXacts}, []string{preparedXacts})
	s.declareReferenceAndSequences(t)
	ctx := context.Background()
	conn := s.connect(t)
	s.awaitSharded(t, conn)
	s.awaitReference(t, conn)

	t.Run("insert reaches every shard atomically", func(t *testing.T) {
		var id int
		if err := conn.QueryRow(ctx, "insert into regions (id, name) values ($1, $2) returning id", 1, "eu").Scan(&id); err != nil || id != 1 {
			t.Fatalf("insert: id=%d %v\n%s", id, err, s.routerLog.String())
		}
		if _, err := conn.Exec(ctx, "insert into regions (id, name) values (2, 'us'), (3, 'apac')", pgx.QueryExecModeSimpleProtocol); err != nil {
			t.Fatal(err)
		}
		want := "1:eu,2:us,3:apac"
		for shard := 0; shard < 2; shard++ {
			if got := strings.Join(s.regionsOn(t, shard), ","); got != want {
				t.Fatalf("shard %d has %q, want %q", shard, got, want)
			}
		}
		if got := s.decisions(t); len(got) != 0 {
			t.Fatalf("decision rows left: %v", got)
		}
		if p0, p1 := s.preparedOn(t, 0), s.preparedOn(t, 1); len(p0)+len(p1) != 0 {
			t.Fatalf("prepared transactions left: %v %v", p0, p1)
		}
	})

	t.Run("update and delete in a transaction with a sharded write", func(t *testing.T) {
		t0, _ := twoTenants(t)
		tx, err := conn.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := tx.Exec(ctx, "update regions set name = 'europe' where id = 1"); err != nil {
			t.Fatal(err)
		}
		if _, err := tx.Exec(ctx, "insert into orders (tenant_id, id) values ($1, 500)", t0); err != nil {
			t.Fatal(err)
		}
		if _, err := tx.Exec(ctx, "delete from regions where id = 3"); err != nil {
			t.Fatal(err)
		}
		var name string
		if err := tx.QueryRow(ctx, "select name from regions where id = 1").Scan(&name); err != nil || name != "europe" {
			t.Fatalf("own write not visible inside the transaction: %q %v", name, err)
		}
		if err := tx.Commit(ctx); err != nil {
			t.Fatalf("commit: %v\n%s", err, s.routerLog.String())
		}
		for shard := 0; shard < 2; shard++ {
			if got := strings.Join(s.regionsOn(t, shard), ","); got != "1:europe,2:us" {
				t.Fatalf("shard %d has %q", shard, got)
			}
		}
		if s.rowsOn(t, 0, t0) != 1 {
			t.Fatalf("sharded write of the same transaction missing")
		}
	})

	t.Run("rollback undoes every shard", func(t *testing.T) {
		tx, err := conn.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := tx.Exec(ctx, "delete from regions"); err != nil {
			t.Fatal(err)
		}
		if err := tx.Rollback(ctx); err != nil {
			t.Fatal(err)
		}
		for shard := 0; shard < 2; shard++ {
			if got := strings.Join(s.regionsOn(t, shard), ","); got != "1:europe,2:us" {
				t.Fatalf("shard %d has %q after rollback", shard, got)
			}
		}
	})

	t.Run("a shard-side failure aborts the write everywhere", func(t *testing.T) {
		shard1, err := pgx.Connect(ctx, s.appDSN(1))
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = shard1.Close(ctx) }()
		if _, err := shard1.Exec(ctx, "insert into regions (id, name) values (9, 'only-on-1')"); err != nil {
			t.Fatal(err)
		}
		_, err = conn.Exec(ctx, "insert into regions (id, name) values (9, 'x')")
		var pe *pgconn.PgError
		if !errors.As(err, &pe) || pe.Code != "23505" {
			t.Fatalf("expected the unique violation of shard 1, got %v", err)
		}
		if got := s.regionsOn(t, 0); strings.Contains(strings.Join(got, ","), "9:") {
			t.Fatalf("shard 0 kept the row the other shard refused: %v", got)
		}
		if got := s.decisions(t); len(got) != 0 {
			t.Fatalf("decision rows left: %v", got)
		}
		if _, err := shard1.Exec(ctx, "delete from regions where id = 9"); err != nil {
			t.Fatal(err)
		}
	})

	// The statement names no volatile value and still cannot be planned:
	// the default is on the table, and only the shards can report it.
	t.Run("a hazard the statement cannot show is refused", func(t *testing.T) {
		_, err := conn.Exec(ctx, "insert into stamped (id) values (1)")
		if sqlstate(err) != "0A000" || !strings.Contains(err.Error(), "the default of column seen calls now()") {
			t.Fatalf("expected the recorded hazard to refuse the write, got %v", err)
		}
	})

	t.Run("volatile values are refused", func(t *testing.T) {
		for _, sql := range []string{
			"insert into regions (id, name, stamp) values (5, 'x', now())",
			"insert into regions (id, name) values (5, gen_random_uuid()::text)",
			"update regions set stamp = clock_timestamp() where id = 1",
			"delete from regions where random() < 2",
		} {
			_, err := conn.Exec(ctx, sql)
			if sqlstate(err) != "0A000" || !strings.Contains(err.Error(), "cannot call") {
				t.Fatalf("%s: expected 0A000 refusal, got %v", sql, err)
			}
		}
	})

	t.Run("router crash after the commit decision converges through the resolver", func(t *testing.T) {
		_, crashRouter := buildBinariesTagged(t, "pgshard_crashpoints")
		p := &routerProc{log: &logBuffer{}}
		port := freePort(t)
		p.addr = fmt.Sprintf("127.0.0.1:%d", port)
		p.cmd, p.exited = startProcessEnv(t, p.log, "listening on", []string{"PGSHARD_TEST_CRASH_POINT=after_decision"}, crashRouter,
			"serve", "--insecure-dev", "--listen", "0.0.0.0:"+fmt.Sprint(port), "--catalog-dsn", s.catalogDSN,
			"--instance-id", "77", "--drain-timeout", "5s", "--drain-delay", "1s")
		c := s.connectTo(t, p.addr)
		s.awaitSharded(t, c)
		s.awaitReference(t, c)
		cctx, cancel := context.WithTimeout(ctx, 20*time.Second)
		defer cancel()
		if _, err := c.Exec(cctx, "insert into regions (id, name) values (7, 'crash')"); err == nil {
			t.Fatal("insert succeeded although the router was armed to crash")
		}
		select {
		case <-p.exited:
		case <-time.After(20 * time.Second):
			t.Fatalf("router did not die:\n%s", p.log.String())
		}
		if d := s.decisions(t); len(d) != 1 || !strings.HasSuffix(d[0], ":commit") {
			t.Fatalf("decision rows %v, want one committed", d)
		}
		out, err := s.resolver(t).Resolve(ctx, "")
		if err != nil {
			t.Fatal(err)
		}
		if out.Unresolved != 0 {
			t.Fatalf("outcome %+v", out)
		}
		for shard := 0; shard < 2; shard++ {
			if got := strings.Join(s.regionsOn(t, shard), ","); !strings.Contains(got, "7:crash") {
				t.Fatalf("shard %d lacks the committed row: %q", shard, got)
			}
		}
		if p0, p1 := s.preparedOn(t, 0), s.preparedOn(t, 1); len(p0)+len(p1) != 0 {
			t.Fatalf("prepared transactions left: %v %v", p0, p1)
		}
	})
}

func TestRouterGlobalSequences(t *testing.T) {
	s := startShardedStackWith(t, []string{preparedXacts}, []string{preparedXacts})
	s.declareReferenceAndSequences(t)
	ctx := context.Background()
	conn := s.connect(t)
	s.awaitSharded(t, conn)
	s.awaitReference(t, conn)
	second := s.startRouter(t, 2, nil)
	conn2 := s.connectTo(t, second.addr)
	s.awaitSharded(t, conn2)
	s.awaitReference(t, conn2)
	t0, t1 := twoTenants(t)

	t.Run("insert without the id column gets a routed value and RETURNING works", func(t *testing.T) {
		var id int64
		if err := conn.QueryRow(ctx, "insert into tickets (tenant_id, body) values ($1, $2) returning id", t1, "hello").Scan(&id); err != nil {
			t.Fatalf("insert: %v\n%s", err, s.routerLog.String())
		}
		if id < 1 {
			t.Fatalf("id %d", id)
		}
		var id2 int64
		if err := conn.QueryRow(ctx, "insert into tickets (tenant_id, id, body) values ("+fmt.Sprint(t0)+", default, 'x') returning id", pgx.QueryExecModeSimpleProtocol).Scan(&id2); err != nil {
			t.Fatalf("simple insert: %v", err)
		}
		if id2 <= id {
			t.Fatalf("ids not increasing within one router: %d then %d", id, id2)
		}
		for shard, tenant := range map[int]int64{1: t1, 0: t0} {
			c, err := pgx.Connect(ctx, s.appDSN(shard))
			if err != nil {
				t.Fatal(err)
			}
			var n int
			if err := c.QueryRow(ctx, "select count(*) from tickets where tenant_id = $1", tenant).Scan(&n); err != nil || n != 1 {
				t.Fatalf("shard %d rows for tenant %d: %d %v", shard, tenant, n, err)
			}
			_ = c.Close(ctx)
		}
	})

	t.Run("two routers allocate concurrently without collisions", func(t *testing.T) {
		const per = 300
		var wg sync.WaitGroup
		errs := make(chan error, 2)
		for _, c := range []*pgx.Conn{conn, conn2} {
			wg.Add(1)
			go func(c *pgx.Conn) {
				defer wg.Done()
				for i := 0; i < per; i++ {
					if _, err := c.Exec(ctx, "insert into tickets (tenant_id, body) values ($1, 'load')", t1); err != nil {
						errs <- err
						return
					}
				}
			}(c)
		}
		wg.Wait()
		close(errs)
		for err := range errs {
			t.Fatalf("insert: %v\n%s\n%s", err, s.routerLog.String(), second.log.String())
		}
		c, err := pgx.Connect(ctx, s.appDSN(1))
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = c.Close(ctx) }()
		var rows, distinct int
		if err := c.QueryRow(ctx, "select count(*), count(distinct id) from tickets where body = 'load'").Scan(&rows, &distinct); err != nil {
			t.Fatal(err)
		}
		if rows != 2*per || distinct != 2*per {
			t.Fatalf("rows=%d distinct=%d, want %d each", rows, distinct, 2*per)
		}
		var blocks int64
		cat, err := pgx.Connect(ctx, s.catalogDSN)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = cat.Close(ctx) }()
		if err := cat.QueryRow(ctx, "select next_value from pgshard.sequences where name = 'app.public.tickets.id'").Scan(&blocks); err != nil {
			t.Fatalf("sequence row: %v", err)
		}
		if blocks < 2*per {
			t.Fatalf("catalog next_value %d after %d values", blocks, 2*per)
		}
	})

	t.Run("nextval over the global sequence and the declared one", func(t *testing.T) {
		cat, err := pgx.Connect(ctx, s.catalogDSN)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := cat.Exec(ctx, "insert into pgshard.sequences (name, next_value, block_size) values ('invoice_numbers', 5000, 10)"); err != nil {
			t.Fatal(err)
		}
		_ = cat.Close(ctx)
		var a, b int64
		if err := conn.QueryRow(ctx, "select nextval('tickets.id')").Scan(&a); err != nil {
			t.Fatal(err)
		}
		if err := conn.QueryRow(ctx, "select nextval('public.tickets.id')", pgx.QueryExecModeSimpleProtocol).Scan(&b); err != nil {
			t.Fatal(err)
		}
		if b <= a {
			t.Fatalf("nextval %d then %d", a, b)
		}
		deadline := time.Now().Add(45 * time.Second)
		for {
			var v int64
			err := conn.QueryRow(ctx, "select nextval('invoice_numbers')").Scan(&v)
			if err == nil && v == 5000 {
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("declared sequence not served after the snapshot reload: v=%d err=%v", v, err)
			}
			time.Sleep(time.Second)
		}
	})
}
