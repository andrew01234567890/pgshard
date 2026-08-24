package router

import (
	"context"
	"errors"
	"log/slog"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/andrew01234567890/pgshard/internal/catalog"
	"github.com/andrew01234567890/pgshard/internal/catalog/snapshot"
	"github.com/andrew01234567890/pgshard/internal/placement"
)

// shardedHarness runs the router in front of four fake poolers, one per
// shard of the default shard set, with the sharded fixture tables.
type shardedHarness struct {
	*harness
	poolers []*fakePooler
	snap    *snapshot.Snapshot
}

func newShardedHarness(t *testing.T) *shardedHarness {
	t.Helper()
	return newShardedHarnessWith(t, Config{})
}

// newShardedHarnessWith is newShardedHarness with cfg's Scatter and
// Decisions settings.
func newShardedHarnessWith(t *testing.T, cfg Config) *shardedHarness {
	t.Helper()
	ranges, err := placement.Split(4)
	if err != nil {
		t.Fatal(err)
	}
	snap := &snapshot.Snapshot{
		ShardMapGeneration: 7,
		ShardSets:          map[string][]snapshot.Range{},
		Serving:            map[snapshot.ShardKey]snapshot.Serving{},
		Databases:          map[string]catalog.Database{"app": {Name: "app", HomeShard: 0, DefaultPlacement: "unsharded"}},
		Tables:             map[snapshot.TableKey]snapshot.Placement{},
	}
	var poolers []*fakePooler
	for i, r := range ranges {
		fp := newFakePooler()
		addr := startFakePooler(t, fp)
		poolers = append(poolers, fp)
		snap.ShardSets[DefaultShardSet] = append(snap.ShardSets[DefaultShardSet], snapshot.Range{ShardID: int32(i), Start: r.Start, End: r.End})
		snap.Serving[snapshot.ShardKey{ShardSet: DefaultShardSet, ShardID: int32(i)}] = snapshot.Serving{Epoch: 2, PrimaryEndpoint: addr, State: "serving"}
	}
	tbl := func(name, placement, key string) {
		snap.Tables[snapshot.TableKey{Database: "app", SchemaName: "public", TableName: name}] = snapshot.Placement{Placement: placement, ShardKey: key}
	}
	tbl("items", "unsharded", "")
	tbl("regions", "reference", "")
	tbl("orders", "sharded", "tenant_id")
	tbl("docs", "sharded", "slug")
	snap.Tables[snapshot.TableKey{Database: "app", SchemaName: "audit", TableName: "events"}] = snapshot.Placement{Placement: "sharded", ShardKey: "tenant_id"}
	snap.Tables[snapshot.TableKey{Database: "app", SchemaName: "public", TableName: "tickets"}] = snapshot.Placement{Placement: "sharded", ShardKey: "tenant_id", SequenceColumns: []string{"id"}}
	snap.Tables[snapshot.TableKey{Database: "app", SchemaName: "public", TableName: "eventlog"}] = snapshot.Placement{Placement: "sharded", ShardKey: "event_id", SequenceColumns: []string{"event_id"}}
	snap.Sequences = map[string]bool{"invoice_numbers": true}
	h := &harness{subs: map[chan snapshot.Change]struct{}{}}
	h.snapp.Store(snap)
	pl := NewPoolers(nil, h.snap, insecure.NewCredentials())
	t.Cleanup(pl.Close)
	sh := &shardedHarness{harness: h, poolers: poolers, snap: snap}
	startHarness(t, h, Config{Snapshot: h.snap, Poolers: pl, Logger: slog.New(slog.DiscardHandler), Scatter: cfg.Scatter, Decisions: cfg.Decisions, Sequences: cfg.Sequences, Planner: cfg.Planner, Migrations: cfg.Migrations,
		Buffering: Buffering{Window: 700 * time.Millisecond, Poll: 20 * time.Millisecond, PerShardCap: 2, Changes: h.subscribe}})
	return sh
}

// dsn connects with pgx's default (prepare-and-describe) mode so parameter
// types come from the backend, as with real drivers.
func (h *shardedHarness) dsn() string {
	return "postgres://app:secret@" + h.addr + "/app?sslmode=disable"
}

func (h *shardedHarness) shardOf(t *testing.T, v any) int {
	t.Helper()
	id, err := placement.KeyspaceID(v)
	if err != nil {
		t.Fatal(err)
	}
	sh, err := h.snap.Locate(DefaultShardSet, id)
	if err != nil {
		t.Fatal(err)
	}
	return int(sh)
}

// twoTenants returns two tenant ids on different shards, the first not on
// the home shard.
func (h *shardedHarness) twoTenants(t *testing.T) (int64, int64) {
	t.Helper()
	var a, b int64 = -1, -1
	for i := int64(1); i < 100; i++ {
		s := h.shardOf(t, i)
		if a < 0 && s != 0 {
			a = i
		} else if a >= 0 && s != h.shardOf(t, a) && s != 0 {
			b = i
			break
		}
	}
	if a < 0 || b < 0 {
		t.Fatal("fixture has no two tenants on distinct non-home shards")
	}
	return a, b
}

func (h *shardedHarness) ranOn(shard int, needle string) bool {
	for _, q := range h.poolers[shard].ran() {
		if strings.Contains(q, needle) {
			return true
		}
	}
	return false
}

func (h *shardedHarness) onlyShardRunning(t *testing.T, needle string) int {
	t.Helper()
	found := -1
	for i := range h.poolers {
		if h.ranOn(i, needle) {
			if found >= 0 {
				t.Fatalf("%q ran on shards %d and %d", needle, found, i)
			}
			found = i
		}
	}
	if found < 0 {
		t.Fatalf("%q ran on no shard", needle)
	}
	return found
}

func expectRefusal(t *testing.T, err error, msg string) *pgconn.PgError {
	t.Helper()
	var pe *pgconn.PgError
	if err == nil || !errors.As(err, &pe) {
		t.Fatalf("expected 0A000 %q, got %v", msg, err)
	}
	if pe.Code != "0A000" || !strings.HasPrefix(pe.Message, msg) {
		t.Fatalf("expected 0A000 %q, got %s %q", msg, pe.Code, pe.Message)
	}
	return pe
}

func TestShardedRoutingSimpleAndExtended(t *testing.T) {
	h := newShardedHarness(t)
	ctx := context.Background()
	a, b := h.twoTenants(t)
	conn := h.connect(t, h.dsn())

	if _, err := conn.Exec(ctx, "insert into orders (tenant_id, id) values ($1, 1)", a); err != nil {
		t.Fatal(err)
	}
	if got := h.onlyShardRunning(t, "insert into orders (tenant_id, id) values ($1, 1)"); got != h.shardOf(t, a) {
		t.Fatalf("tenant %d insert ran on shard %d, want %d", a, got, h.shardOf(t, a))
	}
	if _, err := conn.Exec(ctx, "insert into orders (tenant_id, id) values ($1, 2)", b); err != nil {
		t.Fatal(err)
	}
	if got := h.onlyShardRunning(t, "values ($1, 2)"); got != h.shardOf(t, b) {
		t.Fatalf("tenant %d insert ran on shard %d, want %d", b, got, h.shardOf(t, b))
	}
	var n int
	if err := conn.QueryRow(ctx, "select * from orders where tenant_id = "+itoa(a), pgx.QueryExecModeSimpleProtocol).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if got := h.onlyShardRunning(t, "select * from orders where tenant_id = "+itoa(a)); got != h.shardOf(t, a) {
		t.Fatalf("simple select ran on shard %d, want %d", got, h.shardOf(t, a))
	}
	if err := conn.QueryRow(ctx, "select * from docs where slug = $1", "acme").Scan(&n); err != nil {
		t.Fatal(err)
	}
	if got := h.onlyShardRunning(t, "select * from docs where slug = $1"); got != h.shardOf(t, "acme") {
		t.Fatalf("text key select ran on shard %d, want %d", got, h.shardOf(t, "acme"))
	}
	// The backend's ParameterDescription types an undeclared parameter, so a
	// numeric-looking text key is not ambiguous through a prepared statement.
	if err := conn.QueryRow(ctx, "select * from docs where slug = $1 and id = 1", "123").Scan(&n); err != nil {
		t.Fatalf("numeric-looking text key through describe: %v", err)
	}
	if got := h.onlyShardRunning(t, "select * from docs where slug = $1 and id = 1"); got != h.shardOf(t, "123") {
		t.Fatalf("text key '123' ran on shard %d, want %d", got, h.shardOf(t, "123"))
	}
	if _, err := conn.Exec(ctx, "insert into items values (1)"); err != nil {
		t.Fatal(err)
	}
	if got := h.onlyShardRunning(t, "insert into items"); got != 0 {
		t.Fatalf("unsharded insert ran on shard %d, want home 0", got)
	}
	if err := conn.QueryRow(ctx, "select * from regions").Scan(&n); err != nil {
		t.Fatal(err)
	}
	h.onlyShardRunning(t, "select * from regions")
}

func TestSearchPathRoutesUnqualifiedNames(t *testing.T) {
	h := newShardedHarness(t)
	ctx := context.Background()
	a, _ := h.twoTenants(t)
	conn := h.connect(t, h.dsn())

	if _, err := conn.Exec(ctx, "insert into events (tenant_id, id) values ("+itoa(a)+", 1)"); err != nil {
		t.Fatal(err)
	}
	if got := h.onlyShardRunning(t, "values ("+itoa(a)+", 1)"); got != 0 {
		t.Fatalf("public.events is undeclared and must run on home shard 0, ran on %d", got)
	}
	if _, err := conn.Exec(ctx, "set search_path = audit, public"); err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Exec(ctx, "insert into events (tenant_id, id) values ($1, 2)", a); err != nil {
		t.Fatal(err)
	}
	if got := h.onlyShardRunning(t, "values ($1, 2)"); got != h.shardOf(t, a) {
		t.Fatalf("audit.events insert ran on shard %d, want %d", got, h.shardOf(t, a))
	}
	if _, err := conn.Exec(ctx, "insert into events (tenant_id, id) values ("+itoa(a)+", 3)"); err != nil {
		t.Fatal(err)
	}
	if got := h.onlyShardRunning(t, "values ("+itoa(a)+", 3)"); got != h.shardOf(t, a) {
		t.Fatalf("simple-protocol audit.events insert ran on shard %d, want %d", got, h.shardOf(t, a))
	}
	if _, err := conn.Exec(ctx, "reset search_path"); err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Exec(ctx, "insert into events (tenant_id, id) values ("+itoa(a)+", 4)"); err != nil {
		t.Fatal(err)
	}
	if got := h.onlyShardRunning(t, "values ("+itoa(a)+", 4)"); got != 0 {
		t.Fatalf("after RESET the insert must run on home shard 0, ran on %d", got)
	}
	if _, err := conn.Exec(ctx, "set search_path = audit"); err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Exec(ctx, "reset all"); err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Exec(ctx, "insert into events (tenant_id, id) values ("+itoa(a)+", 5)"); err != nil {
		t.Fatal(err)
	}
	if got := h.onlyShardRunning(t, "values ("+itoa(a)+", 5)"); got != 0 {
		t.Fatalf("after RESET ALL the insert must run on home shard 0, ran on %d", got)
	}
	if _, err := conn.Exec(ctx, "begin"); err != nil {
		t.Fatal(err)
	}
	_, err := conn.Exec(ctx, "set local search_path = audit")
	_ = expectRefusal(t, err, "SET LOCAL search_path")
}

func TestStartupOptionsSearchPath(t *testing.T) {
	h := newShardedHarness(t)
	ctx := context.Background()
	a, _ := h.twoTenants(t)
	conn := h.connect(t, h.dsn()+"&options=-c%20search_path%3Daudit,public")

	if _, err := conn.Exec(ctx, "insert into events (tenant_id, id) values ("+itoa(a)+", 1)"); err != nil {
		t.Fatal(err)
	}
	if got := h.onlyShardRunning(t, "values ("+itoa(a)+", 1)"); got != h.shardOf(t, a) {
		t.Fatalf("startup search_path insert ran on shard %d, want %d", got, h.shardOf(t, a))
	}
	if _, err := conn.Exec(ctx, "set search_path = public"); err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Exec(ctx, "insert into events (tenant_id, id) values ("+itoa(a)+", 2)"); err != nil {
		t.Fatal(err)
	}
	if got := h.onlyShardRunning(t, "values ("+itoa(a)+", 2)"); got != 0 {
		t.Fatalf("after SET the insert must run on home shard 0, ran on %d", got)
	}
	if _, err := conn.Exec(ctx, "reset search_path"); err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Exec(ctx, "insert into events (tenant_id, id) values ("+itoa(a)+", 3)"); err != nil {
		t.Fatal(err)
	}
	if got := h.onlyShardRunning(t, "values ("+itoa(a)+", 3)"); got != h.shardOf(t, a) {
		t.Fatalf("RESET must restore the startup search_path; ran on shard %d, want %d", got, h.shardOf(t, a))
	}
}

func TestStartupSearchPathIsAppliedOnTheBackend(t *testing.T) {
	h := newShardedHarness(t)
	ctx := context.Background()
	conn := h.connect(t, h.dsn()+"&options=-c%20search_path%3Daudit,public")

	setting := func() string {
		var v string
		if err := conn.QueryRow(ctx, "select current_setting('search_path')").Scan(&v); err != nil {
			t.Fatal(err)
		}
		return v
	}
	if got := setting(); got != `"audit", "public"` {
		t.Fatalf("backend search_path = %q, want the startup path the planner routes with", got)
	}
	if _, err := conn.Exec(ctx, "set search_path = public"); err != nil {
		t.Fatal(err)
	}
	if got := setting(); got != "public" {
		t.Fatalf("backend search_path after SET = %q, want public", got)
	}
	if _, err := conn.Exec(ctx, "reset search_path"); err != nil {
		t.Fatal(err)
	}
	if got := setting(); got != `"audit", "public"` {
		t.Fatalf("backend search_path after RESET = %q, want the startup path back", got)
	}
}

func TestShardedPreparedStatementFollowsTheKey(t *testing.T) {
	h := newShardedHarness(t)
	ctx := context.Background()
	a, b := h.twoTenants(t)
	conn := h.connect(t, h.dsn())
	if _, err := conn.Prepare(ctx, "ins", "insert into orders (tenant_id, id) values ($1, $2)"); err != nil {
		t.Fatal(err)
	}
	for i, tenant := range []int64{a, b, a} {
		if _, err := conn.Exec(ctx, "ins", tenant, i); err != nil {
			t.Fatalf("exec %d: %v", i, err)
		}
	}
	for _, tenant := range []int64{a, b} {
		if !h.ranOn(h.shardOf(t, tenant), "insert into orders (tenant_id, id) values ($1, $2)") {
			t.Fatalf("shard %d never ran the prepared insert", h.shardOf(t, tenant))
		}
	}
	runs := 0
	for _, q := range h.poolers[h.shardOf(t, a)].ran() {
		if strings.Contains(q, "values ($1, $2)") {
			runs++
		}
	}
	if runs != 2 {
		t.Fatalf("shard of tenant %d ran the prepared insert %d times, want 2", a, runs)
	}
}

// sidOf returns the session id of the single live session.
func (h *shardedHarness) sidOf(t *testing.T) string {
	t.Helper()
	h.r.mu.Lock()
	defer h.r.mu.Unlock()
	if len(h.r.sessions) != 1 {
		t.Fatalf("%d sessions, want 1", len(h.r.sessions))
	}
	for _, e := range h.r.sessions {
		return e.sid
	}
	return ""
}

func TestShardedTransactionMovesBeforeTouchingAShard(t *testing.T) {
	h := newShardedHarness(t)
	ctx := context.Background()
	a, b := h.twoTenants(t)
	conn := h.connect(t, h.dsn())
	tx, err := conn.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, "set application_name to 'moved'"); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, "insert into orders (tenant_id, id) values ($1, 1)", a); err != nil {
		t.Fatalf("first statement of the transaction must move it to the key's shard: %v", err)
	}
	shard := h.shardOf(t, a)
	if !h.ranOn(shard, "begin") || !h.ranOn(shard, "set application_name to 'moved'") {
		t.Fatalf("BEGIN and SET were not replayed on shard %d: %v", shard, h.poolers[shard].ran())
	}
	if h.poolers[shard].backend(h.sidOf(t)).tx != 'T' {
		t.Fatalf("shard %d backend is not in a transaction", shard)
	}
	_, err = tx.Exec(ctx, "insert into orders (tenant_id, id) values ($1, 2)", b)
	pe := expectRefusal(t, err, "two-phase commit is not available: the router has no decision log")
	if pe.Code != "0A000" {
		t.Fatalf("code %s", pe.Code)
	}
	if err := tx.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	if h.poolers[shard].backend(h.sidOf(t)).tx != 'I' {
		t.Fatalf("shard %d backend still in a transaction after rollback", shard)
	}
	// After the transaction the session moves freely again.
	if _, err := conn.Exec(ctx, "insert into orders (tenant_id, id) values ($1, 3)", b); err != nil {
		t.Fatal(err)
	}
	if !h.ranOn(h.shardOf(t, b), "values ($1, 3)") {
		t.Fatalf("insert after rollback did not reach shard %d", h.shardOf(t, b))
	}
	var v string
	if err := conn.QueryRow(ctx, "select current_setting('application_name')").Scan(&v); err != nil || v != "" {
		t.Fatalf("SET inside a rolled-back transaction must not survive: %q %v", v, err)
	}
}

func TestShardedRefusalsThroughTheWire(t *testing.T) {
	h := newShardedHarness(t)
	ctx := context.Background()
	conn := h.connect(t, h.dsn())
	a, b := h.twoTenants(t)
	cases := []struct {
		sql  string
		args []any
		msg  string
		hint string
	}{
		{sql: "select * from orders where tenant_id in ($1, $2) for update", args: []any{a, b}, msg: "multi-shard SELECT with FOR UPDATE/SHARE is not available yet"},
		{sql: "insert into orders values (1, 2)", msg: "insert requires the shard key", hint: "tenant_id"},
		{sql: "update orders set tenant_id = 2 where tenant_id = 1", msg: "shard key is immutable"},
		{sql: "delete from orders", msg: "scatter DELETE without a shard key predicate is not available yet"},
		{sql: "insert into regions values (1)", msg: "two-phase commit is not available: the router has no decision log"},
		{sql: "create table orders (id int primary key, tenant_id int8)", msg: "primary key or unique constraint (id) on sharded table \"orders\" must include the shard key \"tenant_id\""},
		{sql: "create table orders (id int, tenant_id int8, primary key (tenant_id, id))", msg: "DDL is not available: the router has no migration queue"},
		{sql: "select * from orders o join docs d on o.id = d.id where o.tenant_id = " + itoa(farFrom(t, h, "acme")) + " and d.slug = $1", args: []any{"acme"}, msg: "cross-shard join is not available yet"},
	}
	for _, c := range cases {
		t.Run(c.sql, func(t *testing.T) {
			_, err := conn.Exec(ctx, c.sql, c.args...)
			pe := expectRefusal(t, err, c.msg)
			if c.hint != "" && !strings.Contains(pe.Hint, c.hint) {
				t.Fatalf("hint %q does not mention %q", pe.Hint, c.hint)
			}
			var n int
			if err := conn.QueryRow(ctx, "select 1").Scan(&n); err != nil || n != 1 {
				t.Fatalf("session unusable after refusal: %v", err)
			}
		})
	}
	// An undeclared, untyped parameter that looks numeric is refused rather
	// than guessed (exec mode sends text with no type).
	exec := h.connect(t, h.dsn()+"&default_query_exec_mode=exec")
	_, err := exec.Exec(ctx, "select * from orders where tenant_id = $1", "123")
	pe := expectRefusal(t, err, "parameter $1 cannot be a shard key: value is untyped and looks numeric")
	if !strings.Contains(pe.Hint, "$1::int8") {
		t.Fatalf("hint %q", pe.Hint)
	}
	if _, err := exec.Exec(ctx, "select * from orders where tenant_id = $1::int8", "123"); err != nil {
		t.Fatalf("a cast types the parameter: %v", err)
	}
	if _, err := exec.Exec(ctx, "select * from docs where slug = $1", "acme"); err != nil {
		t.Fatalf("a non-numeric untyped text parameter is a text key: %v", err)
	}
	// A join whose tables resolve to the same shard runs there.
	if _, err := conn.Exec(ctx, "select * from orders o join docs d on o.id = d.id where o.tenant_id = "+itoa(closeTo(t, h, "acme"))+" and d.slug = 'acme'"); err != nil {
		t.Fatalf("co-located join must run: %v", err)
	}
	if got := h.onlyShardRunning(t, "join docs"); got != h.shardOf(t, "acme") {
		t.Fatalf("co-located join ran on shard %d, want %d", got, h.shardOf(t, "acme"))
	}
	for i := range h.poolers {
		for _, q := range h.poolers[i].ran() {
			if strings.Contains(q, "tenant_id = 2 where") || strings.Contains(q, "delete from orders") || strings.Contains(q, "into regions") || strings.Contains(q, "create table") {
				t.Fatalf("refused statement reached shard %d: %q", i, q)
			}
		}
	}
}

// farFrom returns a tenant id whose shard differs from the shard of key.
func farFrom(t *testing.T, h *shardedHarness, key any) int64 {
	t.Helper()
	for i := int64(1); i < 100; i++ {
		if h.shardOf(t, i) != h.shardOf(t, key) {
			return i
		}
	}
	t.Fatal("no tenant off the key's shard")
	return 0
}

// closeTo returns a tenant id on the shard of key.
func closeTo(t *testing.T, h *shardedHarness, key any) int64 {
	t.Helper()
	for i := int64(1); i < 100; i++ {
		if h.shardOf(t, i) == h.shardOf(t, key) {
			return i
		}
	}
	t.Fatal("no tenant on the key's shard")
	return 0
}

func TestShardedBatchMustTargetOneShard(t *testing.T) {
	h := newShardedHarness(t)
	ctx := context.Background()
	a, b := h.twoTenants(t)
	conn, err := pgx.Connect(ctx, h.dsn())
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
	// One Sync, two statements for two shards: refused at the second Bind.
	send(&pgproto3.Parse{Query: "insert into orders (tenant_id, id) values ($1::int8, 1)"},
		&pgproto3.Bind{Parameters: [][]byte{[]byte(itoa(a))}},
		&pgproto3.Execute{},
		&pgproto3.Parse{Query: "insert into orders (tenant_id, id) values ($1::int8, 2)"},
		&pgproto3.Bind{Parameters: [][]byte{[]byte(itoa(b))}},
		&pgproto3.Execute{},
		&pgproto3.Sync{})
	var refusal *pgproto3.ErrorResponse
	for {
		msg, err := fe.Receive()
		if err != nil {
			t.Fatal(err)
		}
		if e, ok := msg.(*pgproto3.ErrorResponse); ok && refusal == nil {
			refusal = e
		}
		if _, ok := msg.(*pgproto3.ReadyForQuery); ok {
			break
		}
	}
	if refusal == nil || refusal.Code != "0A000" || !strings.HasPrefix(refusal.Message, "statements of one batch target different shards") {
		t.Fatalf("expected the batch refusal, got %+v", refusal)
	}
	for i := range h.poolers {
		if h.ranOn(i, "values ($1::int8, 1)") || h.ranOn(i, "values ($1::int8, 2)") {
			t.Fatalf("refused batch reached shard %d", i)
		}
	}
	// Two statements for the same shard in one Sync run there.
	send(&pgproto3.Parse{Query: "insert into orders (tenant_id, id) values ($1::int8, 3)"},
		&pgproto3.Bind{Parameters: [][]byte{[]byte(itoa(a))}},
		&pgproto3.Execute{},
		&pgproto3.Parse{Query: "insert into orders (tenant_id, id) values ($1::int8, 4)"},
		&pgproto3.Bind{Parameters: [][]byte{[]byte(itoa(a))}},
		&pgproto3.Execute{},
		&pgproto3.Sync{})
	completes := 0
	for {
		msg, err := fe.Receive()
		if err != nil {
			t.Fatal(err)
		}
		switch m := msg.(type) {
		case *pgproto3.ErrorResponse:
			t.Fatalf("same-shard batch failed: %s %s", m.Code, m.Message)
		case *pgproto3.CommandComplete:
			completes++
		}
		if _, ok := msg.(*pgproto3.ReadyForQuery); ok {
			break
		}
	}
	if completes != 2 {
		t.Fatalf("%d CommandComplete, want 2", completes)
	}
	if got := h.onlyShardRunning(t, "values ($1::int8, 4)"); got != h.shardOf(t, a) {
		t.Fatalf("same-shard batch ran on shard %d, want %d", got, h.shardOf(t, a))
	}
	send(&pgproto3.Terminate{})
}

func TestShardedInTransactionCannotLeaveTouchedShard(t *testing.T) {
	h := newShardedHarness(t)
	ctx := context.Background()
	a, b := h.twoTenants(t)
	conn := h.connect(t, h.dsn())
	if _, err := conn.Exec(ctx, "begin"); err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Exec(ctx, "insert into items values (1)"); err != nil {
		t.Fatal(err)
	}
	_, err := conn.Exec(ctx, "insert into orders (tenant_id, id) values ($1, 1)", a)
	_ = expectRefusal(t, err, "two-phase commit is not available: the router has no decision log")
	if _, err := conn.Exec(ctx, "commit"); err != nil {
		t.Fatal(err)
	}
	if !h.ranOn(0, "commit") || !h.ranOn(0, "insert into items") {
		t.Fatalf("home shard did not run the transaction: %v", h.poolers[0].ran())
	}
	if _, err := conn.Exec(ctx, "insert into orders (tenant_id, id) values ($1, 1)", b); err != nil {
		t.Fatalf("after commit the session moves again: %v", err)
	}
	if !h.ranOn(h.shardOf(t, b), "values ($1, 1)") {
		t.Fatalf("insert did not reach shard %d", h.shardOf(t, b))
	}
}

func itoa(v int64) string { return strconv.FormatInt(v, 10) }

func TestPreparedStatementReplansOnNewSnapshot(t *testing.T) {
	h := newShardedHarness(t)
	ctx := context.Background()
	a, _ := h.twoTenants(t)
	// Start with the table undeclared: the prepared insert plans onto home.
	declared := h.snap
	undeclared := *declared
	undeclared.Tables = map[snapshot.TableKey]snapshot.Placement{}
	h.setSnap(&undeclared)
	conn := h.connect(t, h.dsn())
	if _, err := conn.Prepare(ctx, "ins", "insert into orders (tenant_id, id) values ($1, $2)"); err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Exec(ctx, "ins", a, 1); err != nil {
		t.Fatal(err)
	}
	if got := h.onlyShardRunning(t, "values ($1, $2)"); got != 0 {
		t.Fatalf("undeclared table ran on shard %d, want home 0", got)
	}
	h.setSnap(declared)
	if _, err := conn.Exec(ctx, "ins", a, 2); err != nil {
		t.Fatal(err)
	}
	if !h.ranOn(h.shardOf(t, a), "values ($1, $2)") {
		t.Fatalf("after the catalog declared the table the cached statement still ran on home; shard %d saw %v", h.shardOf(t, a), h.poolers[h.shardOf(t, a)].ran())
	}
}

func TestPanicInPlanningIsConfinedToTheSession(t *testing.T) {
	planner := NewPlanner()
	planner.before = func(sql string) {
		if strings.Contains(sql, "boom") {
			panic("planner exploded on " + sql)
		}
	}
	h := newShardedHarnessWith(t, Config{Planner: planner})
	ctx := context.Background()
	conn := h.connect(t, h.dsn())

	for _, mode := range []pgx.QueryExecMode{pgx.QueryExecModeSimpleProtocol, pgx.QueryExecModeCacheStatement} {
		_, err := conn.Exec(ctx, "select boom", mode)
		var pe *pgconn.PgError
		if !errors.As(err, &pe) || pe.Code != "XX000" || !strings.Contains(pe.Message, "internal error") {
			t.Fatalf("mode %v: got %v, want XX000 internal error", mode, err)
		}
		var n int
		if err := conn.QueryRow(ctx, "select 1", mode).Scan(&n); err != nil || n != 1 {
			t.Fatalf("mode %v: session unusable after the panic: %v", mode, err)
		}
	}
	other := h.connect(t, h.dsn())
	var n int
	if err := other.QueryRow(ctx, "select 1").Scan(&n); err != nil {
		t.Fatalf("server did not survive the panic: %v", err)
	}
}
