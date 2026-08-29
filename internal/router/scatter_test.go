package router

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/andrew01234567890/pgshard/internal/catalog/snapshot"
	"github.com/jackc/pgx/v5/pgproto3"
)

func int4Rows(vals ...string) script {
	sc := script{cols: []scriptCol{{name: "v", oid: 23}}}
	for _, v := range vals {
		sc.rows = append(sc.rows, []string{v})
	}
	return sc
}

func collectInts(t *testing.T, conn *pgx.Conn, sql string) []int {
	t.Helper()
	rows, err := conn.Query(context.Background(), sql)
	if err != nil {
		t.Fatal(err)
	}
	var out []int
	for rows.Next() {
		var v int
		if err := rows.Scan(&v); err != nil {
			t.Fatal(err)
		}
		out = append(out, v)
	}
	if rows.Err() != nil {
		t.Fatal(rows.Err())
	}
	return out
}

func equalInts(a, b []int) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func TestScatterConcatenatesEveryShard(t *testing.T) {
	h := newShardedHarness(t)
	conn := h.connect(t, h.dsn()+"&default_query_exec_mode=simple_protocol")
	rows, err := conn.Query(context.Background(), "select * from orders")
	if err != nil {
		t.Fatal(err)
	}
	n := 0
	for rows.Next() {
		n++
	}
	if rows.Err() != nil || n != 4 {
		t.Fatalf("rows=%d err=%v, want one row per shard", n, rows.Err())
	}
	if tag := rows.CommandTag(); tag.String() != "SELECT 4" {
		t.Fatalf("command tag %q", tag)
	}
	for i := range h.poolers {
		if !h.ranOn(i, "select * from orders") {
			t.Fatalf("shard %d did not run the scatter", i)
		}
	}
}

func TestScatterMergesOrderedStreamsWithLimitPushdown(t *testing.T) {
	h := newShardedHarness(t)
	shardRows := [][]string{{"1", "5", "9"}, {"2", "6"}, {"3"}, {"4", "8"}}
	for i, fp := range h.poolers {
		fp.script("select v from orders order by v", int4Rows(shardRows[i]...))
		fp.script("SELECT v FROM orders ORDER BY v LIMIT 4", int4Rows(shardRows[i]...))
		fp.script("SELECT v FROM orders ORDER BY v DESC LIMIT 2", int4Rows(reverse(shardRows[i])...))
	}
	for _, mode := range []string{"simple_protocol", "cache_statement"} {
		conn := h.connect(t, h.dsn()+"&default_query_exec_mode="+mode)
		got := collectInts(t, conn, "select v from orders order by v")
		if want := []int{1, 2, 3, 4, 5, 6, 8, 9}; !equalInts(got, want) {
			t.Fatalf("%s: merged %v, want %v", mode, got, want)
		}
		got = collectInts(t, conn, "select v from orders order by v limit 3 offset 1")
		if want := []int{2, 3, 4}; !equalInts(got, want) {
			t.Fatalf("%s: limit/offset gave %v, want %v", mode, got, want)
		}
		got = collectInts(t, conn, "select v from orders order by v desc limit 2")
		if want := []int{9, 8}; !equalInts(got, want) {
			t.Fatalf("%s: desc gave %v, want %v", mode, got, want)
		}
	}
	for i := range h.poolers {
		if !h.ranOn(i, "select v from orders order by v limit 4") {
			t.Fatalf("shard %d did not receive the pushed-down LIMIT 4 (limit+offset): %v", i, h.poolers[i].ran())
		}
	}
}

func reverse(s []string) []string {
	out := make([]string, len(s))
	for i, v := range s {
		out[len(s)-1-i] = v
	}
	return out
}

func TestScatterCombinesDistributiveAggregates(t *testing.T) {
	h := newShardedHarness(t)
	sql := "select count(*), sum(v), min(v), max(v) from orders"
	cols := []scriptCol{{"count", 20}, {"sum", 20}, {"min", 23}, {"max", 23}}
	perShard := [][]string{{"2", "10", "1", "9"}, {"0", "NULL", "NULL", "NULL"}, {"1", "5", "5", "5"}, {"3", "6", "2", "3"}}
	for i, fp := range h.poolers {
		fp.script(sql, script{cols: cols, rows: [][]string{perShard[i]}})
	}
	conn := h.connect(t, h.dsn())
	var count, sum int64
	var mn, mx int
	if err := conn.QueryRow(context.Background(), sql).Scan(&count, &sum, &mn, &mx); err != nil {
		t.Fatal(err)
	}
	if count != 6 || sum != 21 || mn != 1 || mx != 9 {
		t.Fatalf("combined (%d, %d, %d, %d), want (6, 21, 1, 9)", count, sum, mn, mx)
	}
	for _, fp := range h.poolers {
		fp.script("select sum(v) from orders", script{cols: cols[1:2], rows: [][]string{{"NULL"}}})
	}
	var null *int64
	if err := conn.QueryRow(context.Background(), "select sum(v) from orders").Scan(&null); err != nil || null != nil {
		t.Fatalf("all-NULL sum: %v %v", null, err)
	}
}

func TestScatterRefusesTextOrderWithoutCCollation(t *testing.T) {
	h := newShardedHarness(t)
	for _, fp := range h.poolers {
		fp.script("select name from orders order by name", script{cols: []scriptCol{{"name", 25}}, rows: [][]string{{"b"}}})
		fp.script(`select name from orders order by name collate "c"`, script{cols: []scriptCol{{"name", 25}}, rows: [][]string{{"b"}}})
	}
	conn := h.connect(t, h.dsn()+"&default_query_exec_mode=simple_protocol")
	ctx := context.Background()
	_, err := conn.Exec(ctx, "select name from orders order by name")
	_ = expectRefusal(t, err, `multi-shard ORDER BY on a text column needs an explicit COLLATE "C"`)
	rows, err := conn.Query(ctx, `select name from orders order by name collate "C"`)
	if err != nil {
		t.Fatal(err)
	}
	n := 0
	for rows.Next() {
		n++
	}
	if rows.Err() != nil || n != 4 {
		t.Fatalf("collated order: rows=%d err=%v", n, rows.Err())
	}
}

func TestScatterFirstErrorCancelsTheOthers(t *testing.T) {
	h := newShardedHarness(t)
	for i, fp := range h.poolers {
		if i == 2 {
			fp.script("select v from orders", script{err: `relation "orders" does not exist on shard 2`})
		} else {
			fp.script("select v from orders", int4Rows("1"))
		}
	}
	conn := h.connect(t, h.dsn()+"&default_query_exec_mode=simple_protocol")
	_, err := conn.Exec(context.Background(), "select v from orders")
	var pe *pgconn.PgError
	if !errors.As(err, &pe) || pe.Code != "42P01" || !strings.Contains(pe.Message, "shard 2") {
		t.Fatalf("expected the shard error, got %v", err)
	}
	var n int
	if err := conn.QueryRow(context.Background(), "select 1").Scan(&n); err != nil {
		t.Fatalf("session unusable after a shard error: %v", err)
	}
}

func TestScatterRowDescriptionMismatchIsAnError(t *testing.T) {
	h := newShardedHarness(t)
	for i, fp := range h.poolers {
		sc := int4Rows("1")
		if i == 3 {
			sc.cols[0].oid = 20
		}
		fp.script("select v from orders", sc)
	}
	conn := h.connect(t, h.dsn()+"&default_query_exec_mode=simple_protocol")
	_, err := conn.Exec(context.Background(), "select v from orders")
	var pe *pgconn.PgError
	if !errors.As(err, &pe) || pe.Code != "XX000" || !strings.Contains(pe.Message, "disagree on the result shape") {
		t.Fatalf("expected a shape mismatch error, got %v", err)
	}
}

// TestScatterWaitsOutARewriteCutover: a rewrite cuts over one shard at a
// time, and between the first shard's swap and the last a merge cannot
// describe the result. Refusing every multi-shard read for that window
// made a migration that is supposed to be online an outage for scatter
// reads; the read now waits the window out, which is what the router does
// for the other cutover it knows about.
func TestScatterWaitsOutARewriteCutover(t *testing.T) {
	h := newShardedHarness(t)
	key := snapshot.TableKey{Database: "app", SchemaName: "public", TableName: "orders"}
	pl := h.snap.Tables[key]
	pl.HiddenColumns = []string{"_pgshard_new_v"}
	h.snap.Tables[key] = pl
	const sql = "select v from orders"
	lagging := h.poolers[3]
	for i, fp := range h.poolers {
		sc := int4Rows("1")
		if i == 3 {
			sc.cols[0].oid = 20
		}
		fp.script(sql, sc)
	}
	// The last shard swaps once the read has already seen it disagree.
	go func() {
		for {
			if len(lagging.ran()) > 0 {
				lagging.script(sql, int4Rows("1"))
				return
			}
			time.Sleep(time.Millisecond)
		}
	}()

	conn := h.connect(t, h.dsn()+"&default_query_exec_mode=simple_protocol")
	if got := collectInts(t, conn, sql); len(got) != 4 {
		t.Fatalf("rows %v, want one per shard once the cutover finished", got)
	}
}

// TestScatterRefusesWhenARewriteCutoverOutlastsTheWindow: the wait is
// bounded. A shard whose swap is stuck behind a lock can hold the window
// open for as long as the lock does, and the read has to come back with
// the condition that says to retry rather than hang.
func TestScatterRefusesWhenARewriteCutoverOutlastsTheWindow(t *testing.T) {
	h := newShardedHarness(t)
	key := snapshot.TableKey{Database: "app", SchemaName: "public", TableName: "orders"}
	pl := h.snap.Tables[key]
	pl.HiddenColumns = []string{"_pgshard_new_v"}
	h.snap.Tables[key] = pl
	for i, fp := range h.poolers {
		sc := int4Rows("1")
		if i == 3 {
			sc.cols[0].oid = 20
		}
		fp.script("select v from orders", sc)
	}
	conn := h.connect(t, h.dsn()+"&default_query_exec_mode=simple_protocol")
	_, err := conn.Exec(context.Background(), "select v from orders")
	var pe *pgconn.PgError
	if !errors.As(err, &pe) || pe.Code != "55000" {
		t.Fatalf("a cutover that never finishes must still be refused with 55000, got %v", err)
	}
}

// TestScatterMismatchMidRewriteNamesTheMigration: a rewrite cuts over one
// shard at a time, so between the first shard's swap and the last the same
// column has a different type OID on different shards. Reporting that as
// XX000 "align the schema on every shard" told the client to undo the
// migration, and read as a router fault rather than a stage to wait out.
func TestScatterMismatchMidRewriteNamesTheMigration(t *testing.T) {
	h := newShardedHarness(t)
	key := snapshot.TableKey{Database: "app", SchemaName: "public", TableName: "orders"}
	pl := h.snap.Tables[key]
	pl.HiddenColumns = []string{"_pgshard_new_v"}
	h.snap.Tables[key] = pl
	for i, fp := range h.poolers {
		sc := int4Rows("1")
		if i == 3 {
			sc.cols[0].oid = 20
		}
		fp.script("select v from orders", sc)
	}
	conn := h.connect(t, h.dsn()+"&default_query_exec_mode=simple_protocol")
	_, err := conn.Exec(context.Background(), "select v from orders")
	var pe *pgconn.PgError
	if !errors.As(err, &pe) {
		t.Fatalf("expected a PostgreSQL error, got %v", err)
	}
	if pe.Code != "55000" {
		t.Errorf("SQLSTATE %s, want 55000: a rewrite in progress is a condition to retry, not a fault", pe.Code)
	}
	if !strings.Contains(pe.Message, "public.orders is being rewritten") {
		t.Errorf("message %q does not name the table being rewritten", pe.Message)
	}
	// The shape detail still has to survive: it is what tells an operator
	// which column disagrees if the cause turns out not to be the rewrite.
	if !strings.Contains(pe.Message, "oid 20") {
		t.Errorf("message %q dropped the shape detail", pe.Message)
	}
	if !strings.Contains(pe.Hint, "retry") {
		t.Errorf("hint %q does not say to retry", pe.Hint)
	}
}

func TestScatterClientCancelReachesEveryShard(t *testing.T) {
	h := newShardedHarness(t)
	conn := h.connect(t, h.dsn()+"&default_query_exec_mode=simple_protocol")
	ctx := context.Background()
	go func() {
		deadline := time.Now().Add(5 * time.Second)
		for time.Now().Before(deadline) {
			asleep := 0
			for _, fp := range h.poolers {
				fp.mu.Lock()
				asleep += len(fp.sleeping)
				fp.mu.Unlock()
			}
			if asleep == len(h.poolers) {
				_ = conn.PgConn().CancelRequest(ctx)
				return
			}
			time.Sleep(5 * time.Millisecond)
		}
	}()
	_, err := conn.Exec(ctx, "select pg_sleep(10) from orders")
	if sqlstate(err) != "57014" {
		t.Fatalf("cancel: %v", err)
	}
	for i, fp := range h.poolers {
		cancels := fp.cancelled()
		found := false
		for _, c := range cancels {
			if strings.HasPrefix(c, h.sidOf(t)+"-x") && strings.HasSuffix(c, "-"+itoa(int64(i))) {
				found = true
			}
		}
		if !found {
			t.Fatalf("shard %d cancels %v, want the scatter participant", i, cancels)
		}
	}
	var n int
	if err := conn.QueryRow(ctx, "select 1").Scan(&n); err != nil || n != 1 {
		t.Fatalf("session after cancel: %v", err)
	}
}

func TestScatterRefusalsThroughTheWire(t *testing.T) {
	h := newShardedHarness(t)
	ctx := context.Background()
	conn := h.connect(t, h.dsn()+"&default_query_exec_mode=simple_protocol")
	cases := []struct{ sql, msg string }{
		{"select avg(id) from orders", "multi-shard avg() is not available yet"},
		{"select * from orders limit $1", "multi-shard LIMIT must be an integer constant"},
		{"select id from orders group by id", "multi-shard GROUP BY without the shard key is not available yet"},
		{"explain select * from orders", "only a plain SELECT can run on multiple shards"},
	}
	for _, c := range cases {
		_, err := conn.Exec(ctx, c.sql)
		_ = expectRefusal(t, err, c.msg)
	}
	tx, err := conn.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, "insert into orders (tenant_id, id) values (1, 1)"); err != nil {
		t.Fatal(err)
	}
	_, err = tx.Exec(ctx, "select * from orders")
	_ = expectRefusal(t, err, "multi-shard read inside a transaction pinned to shard")
	_ = tx.Rollback(ctx)
	if _, err := conn.Exec(ctx, "begin isolation level repeatable read"); err != nil {
		t.Fatal(err)
	}
	_, err = conn.Exec(ctx, "select * from orders")
	_ = expectRefusal(t, err, "multi-shard reads under REPEATABLE READ or SERIALIZABLE isolation are not available yet")
	if _, err := conn.Exec(ctx, "rollback"); err != nil {
		t.Fatal(err)
	}
	// A read-only transaction that has not touched a shard may scatter.
	if _, err := conn.Exec(ctx, "begin"); err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Exec(ctx, "select * from orders"); err != nil {
		t.Fatalf("scatter in an untouched transaction: %v", err)
	}
	if _, err := conn.Exec(ctx, "commit"); err != nil {
		t.Fatal(err)
	}
}

func TestScatterStatementMustBeAloneInItsBatch(t *testing.T) {
	h := newShardedHarness(t)
	conn, err := pgx.Connect(context.Background(), h.dsn())
	if err != nil {
		t.Fatal(err)
	}
	hj, err := conn.PgConn().Hijack()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = hj.Conn.Close() })
	fe := hj.Frontend
	send := func(msgs ...pgproto3.FrontendMessage) {
		for _, m := range msgs {
			fe.Send(m)
		}
		if err := fe.Flush(); err != nil {
			t.Fatal(err)
		}
	}
	drain := func() *pgproto3.ErrorResponse {
		var first *pgproto3.ErrorResponse
		for {
			msg, err := fe.Receive()
			if err != nil {
				t.Fatal(err)
			}
			if e, ok := msg.(*pgproto3.ErrorResponse); ok && first == nil {
				first = e
			}
			if _, ok := msg.(*pgproto3.ReadyForQuery); ok {
				return first
			}
		}
	}
	send(&pgproto3.Parse{Query: "select * from orders"}, &pgproto3.Bind{}, &pgproto3.Execute{},
		&pgproto3.Parse{Query: "select 1"}, &pgproto3.Bind{}, &pgproto3.Execute{}, &pgproto3.Sync{})
	if e := drain(); e == nil || e.Code != "0A000" || !strings.HasPrefix(e.Message, "a multi-shard statement must be the only statement of its batch") {
		t.Fatalf("expected the mixed-batch refusal, got %+v", e)
	}
	// Partial fetches from a multi-shard portal are refused.
	send(&pgproto3.Parse{Query: "select * from orders"}, &pgproto3.Bind{}, &pgproto3.Execute{MaxRows: 5}, &pgproto3.Sync{})
	if e := drain(); e == nil || e.Code != "0A000" || !strings.HasPrefix(e.Message, "partial fetches (Execute with a row limit) from a multi-shard portal") {
		t.Fatalf("expected the partial-fetch refusal, got %+v", e)
	}
	// A portal bound in an earlier batch cannot be executed later.
	send(&pgproto3.Parse{Query: "select * from orders"}, &pgproto3.Bind{}, &pgproto3.Sync{})
	if e := drain(); e != nil {
		t.Fatalf("bind-only batch failed: %+v", e)
	}
	send(&pgproto3.Execute{}, &pgproto3.Sync{})
	if e := drain(); e == nil || e.Code != "0A000" || !strings.HasPrefix(e.Message, "a multi-shard portal must be bound and executed in the same batch") {
		t.Fatalf("expected the late-execute refusal, got %+v", e)
	}
	for i := range h.poolers {
		for _, q := range h.poolers[i].ran() {
			if q == "select 1" || q == "select * from orders" {
				t.Fatalf("refused batch reached shard %d: %v", i, h.poolers[i].ran())
			}
		}
	}
	send(&pgproto3.Terminate{})
}

func TestScatterHonoursTheShardLimit(t *testing.T) {
	h := newShardedHarnessWith(t, Config{Scatter: ScatterConfig{MaxShards: 2}})
	conn := h.connect(t, h.dsn()+"&default_query_exec_mode=simple_protocol")
	_, err := conn.Exec(context.Background(), "select * from orders")
	_ = expectRefusal(t, err, "statement fans out to 4 shards, more than the router's limit of 2")
}

func TestScatterAppliesTheSessionSearchPath(t *testing.T) {
	h := newShardedHarness(t)
	ctx := context.Background()
	conn := h.connect(t, h.dsn()+"&options=-c%20search_path%3Daudit,public&default_query_exec_mode=simple_protocol")

	pathApplied := func(want string) {
		t.Helper()
		for i, fp := range h.poolers {
			setAt, selAt := -1, -1
			for j, q := range fp.ran() {
				if strings.HasPrefix(q, "select set_config('search_path', '"+want+"'") && setAt < 0 {
					setAt = j
				}
				if strings.HasPrefix(q, "select * from events") {
					selAt = j
				}
			}
			if selAt < 0 {
				t.Fatalf("shard %d did not run the scatter", i)
			}
			if setAt < 0 || setAt > selAt {
				t.Fatalf("shard %d must apply search_path %q before the scatter statement (set at %d, select at %d)", i, want, setAt, selAt)
			}
		}
	}
	if _, err := conn.Exec(ctx, "select * from events"); err != nil {
		t.Fatal(err)
	}
	pathApplied(`"audit", "public"`)

	if _, err := conn.Exec(ctx, "set search_path = audit"); err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Exec(ctx, "select * from events"); err != nil {
		t.Fatal(err)
	}
	pathApplied(`"audit"`)

	if _, err := conn.Exec(ctx, "reset search_path"); err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Exec(ctx, "select * from events"); err != nil {
		t.Fatal(err)
	}
	pathApplied(`"audit", "public"`)

	deadline := time.Now().Add(2 * time.Second)
	for {
		h.poolers[1].mu.Lock()
		reserves, releases := len(h.poolers[1].reserves), len(h.poolers[1].releases)
		h.poolers[1].mu.Unlock()
		if reserves > 0 && releases >= reserves {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("scatter backends must be released after the read: reserves=%d releases=%d", reserves, releases)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// TestScatterReplaysSetRole: a scatter opens fresh backends, and PostgreSQL
// decides grants and row-level security from the role that is current on
// the backend running the query. A session that logs in powerful and then
// assumes a restricted role would otherwise have its scatter evaluated as
// the login role, quietly ignoring the restriction it asked for.
func TestScatterReplaysSetRole(t *testing.T) {
	h := newShardedHarness(t)
	ctx := context.Background()
	conn := h.connect(t, h.dsn()+"&default_query_exec_mode=simple_protocol")

	if _, err := conn.Exec(ctx, "set role tenant_ro"); err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Exec(ctx, "select * from orders"); err != nil {
		t.Fatal(err)
	}
	took := 0
	for i, fp := range h.poolers {
		sawRole, selAt := -1, -1
		for j, q := range fp.ran() {
			if strings.HasPrefix(strings.ToLower(q), "set role tenant_ro") && sawRole < 0 {
				sawRole = j
			}
			if strings.HasPrefix(q, "select * from orders") {
				selAt = j
			}
		}
		if selAt < 0 {
			continue // this shard did not take part
		}
		took++
		if sawRole < 0 {
			t.Fatalf("shard %d ran the scatter without the session's SET ROLE: %v", i, fp.ran())
		}
		if sawRole > selAt {
			t.Fatalf("shard %d applied SET ROLE after the scatter statement (role at %d, select at %d)", i, sawRole, selAt)
		}
	}
	if took < 2 {
		t.Fatalf("only %d shard(s) took part; the statement did not fan out, so this proves nothing", took)
	}
}

// TestScatterSetsUpEveryShardConcurrently pins the shape of fan-out
// setup: it costs a round trip, not one per shard. Setting a participant
// up (Reserve, then the session state statement drained before the next
// shard is touched) used to run in a sequential loop, so a wide fan-out
// spent N x RTT before any shard had begun executing.
func TestScatterSetsUpEveryShardConcurrently(t *testing.T) {
	const shards = 32
	h := newShardedHarnessShards(t, Config{}, shards)
	gate := &reserveGate{width: shards}
	for _, fp := range h.poolers {
		fp.gate = gate
	}
	ctx := context.Background()
	// The extended protocol on purpose: every participant then marshals
	// the same Bind, with its parameters and format vectors, at the same
	// time, which is the sharing perShard relies on.
	conn := h.connect(t, h.dsn())
	// Any session setting makes the participants reserve a backend.
	if _, err := conn.Exec(ctx, "set timezone to 'UTC'"); err != nil {
		t.Fatal(err)
	}

	if _, err := conn.Exec(ctx, "select * from orders where v > $1", 1); err != nil {
		t.Fatal(err)
	}
	if !gate.reached() {
		t.Errorf("no two of the %d shards were ever set up at the same time", shards)
	}
}

// TestReadsAPoolerThatCannotPackRows: the router asks every pooler for
// packed rows, and a pooler that predates the field ignores the request
// and answers with a Value submessage per column. The rows still have to
// arrive, or a rolling upgrade breaks reads for as long as one side is
// behind.
func TestReadsAPoolerThatCannotPackRows(t *testing.T) {
	h := newShardedHarness(t)
	for _, fp := range h.poolers {
		fp.legacyRows = true
	}
	conn := h.connect(t, h.dsn()+"&default_query_exec_mode=simple_protocol")
	if got := collectInts(t, conn, "select * from orders"); len(got) != 4 {
		t.Fatalf("rows %v, want one per shard from a pooler answering the old way", got)
	}
}

// TestScatterRowsAreBoundedByBytes: the merge queue used to be bounded by
// row count alone, at 64 rows a shard. A row is anything from a few bytes
// to a megabyte, so a wide result held 64 megabytes per shard and a
// fan-out over a large topology multiplied that by the shard count.
func TestScatterRowsAreBoundedByBytes(t *testing.T) {
	p := &participant{rows: make(chan [][]byte, 64), stop: make(chan struct{}), taken: make(chan struct{}, 1)}
	wide := [][]byte{make([]byte, scatterRowBudget/2)}

	// Under the budget the producer is never held up: the merge needs a
	// head row from every shard, so a participant holding nothing has to
	// get through whatever the others are doing.
	for range 2 {
		if !p.waitForRoom() {
			t.Fatal("a participant under its budget was held")
		}
		p.queued.Add(rowBytes(wide))
		p.rows <- wide
	}

	held := make(chan bool, 1)
	go func() { held <- p.waitForRoom() }()
	select {
	case <-held:
		t.Fatal("a participant over its byte budget kept producing")
	case <-time.After(50 * time.Millisecond):
	}

	// Taking a row releases it.
	if _, ok, _ := p.Next(); !ok {
		t.Fatal("no row to take")
	}
	select {
	case ok := <-held:
		if !ok {
			t.Error("the producer was told to stop rather than let through")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("taking a row did not release the producer")
	}
}

// TestScatterRowGateReleasesOnStop: a merge that no longer wants rows
// closes stop, and a producer parked on the byte budget has to notice --
// otherwise the pump goroutine outlives the statement.
func TestScatterRowGateReleasesOnStop(t *testing.T) {
	p := &participant{rows: make(chan [][]byte, 64), stop: make(chan struct{}), taken: make(chan struct{}, 1)}
	p.queued.Add(scatterRowBudget)
	held := make(chan bool, 1)
	go func() { held <- p.waitForRoom() }()
	p.stopRows()
	select {
	case ok := <-held:
		if ok {
			t.Error("a stopped participant was told to keep producing")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("stop did not release the producer")
	}
}
