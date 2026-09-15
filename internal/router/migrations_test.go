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
	"github.com/jackc/pgx/v5/pgproto3"

	"github.com/andrew01234567890/pgshard/internal/catalog"
	"github.com/andrew01234567890/pgshard/internal/catalog/snapshot"
)

// fakeQueue completes every migration with the outcome the test scripted,
// after an optional delay.
type fakeQueue struct {
	mu       sync.Mutex
	queued   []catalog.DDLMigration
	outcome  func(m catalog.DDLMigration) catalog.DDLMigration
	delay    time.Duration
	waited   int
	enqueueE error
}

func (q *fakeQueue) Enqueue(_ context.Context, m catalog.DDLMigration) (string, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.enqueueE != nil {
		return "", q.enqueueE
	}
	m.ID = fmt.Sprintf("00000000-0000-0000-0000-%012d", len(q.queued)+1)
	q.queued = append(q.queued, m)
	return m.ID, nil
}

func (q *fakeQueue) Wait(ctx context.Context, id string) (catalog.DDLMigration, error) {
	q.mu.Lock()
	q.waited++
	var m catalog.DDLMigration
	for _, x := range q.queued {
		if x.ID == id {
			m = x
		}
	}
	delay, outcome := q.delay, q.outcome
	q.mu.Unlock()
	if delay > 0 {
		select {
		case <-ctx.Done():
			return m, ctx.Err()
		case <-time.After(delay):
		}
	}
	m.State = catalog.MigrationComplete
	if outcome != nil {
		m = outcome(m)
	}
	return m, nil
}

func (q *fakeQueue) last(t *testing.T) catalog.DDLMigration {
	t.Helper()
	q.mu.Lock()
	defer q.mu.Unlock()
	if len(q.queued) == 0 {
		t.Fatal("nothing was queued")
	}
	return q.queued[len(q.queued)-1]
}

func newDDLHarness(t *testing.T, q *fakeQueue) *shardedHarness {
	t.Helper()
	return newShardedHarnessWith(t, Config{Migrations: q})
}

func TestDDLIsQueuedAndAnsweredWhenComplete(t *testing.T) {
	q := &fakeQueue{delay: 50 * time.Millisecond}
	h := newDDLHarness(t, q)
	ctx := context.Background()
	for _, mode := range []string{"", "&default_query_exec_mode=simple_protocol"} {
		conn := h.connect(t, h.dsn()+mode)
		tag, err := conn.Exec(ctx, "create table orders (tenant_id int8, id int, primary key (tenant_id, id))")
		if err != nil {
			t.Fatalf("mode %q: %v", mode, err)
		}
		if tag.String() != "CREATE TABLE" {
			t.Fatalf("tag = %q", tag)
		}
		m := q.last(t)
		if m.Database != "app" || m.Kind != "CREATE TABLE" || m.Scope != "all" || m.Strategy != "direct" || m.HomeShard != 0 ||
			m.Meta.Object.Name != "orders" || m.Meta.Object.Expect != "present" || !strings.HasPrefix(m.Statement, "create table orders") {
			t.Fatalf("queued %+v", m)
		}
		if _, err := conn.Exec(ctx, "grant select on items to app"); err != nil {
			t.Fatal(err)
		}
		if m := q.last(t); m.Kind != "GRANT" || m.Scope != "home" {
			t.Fatalf("queued %+v", m)
		}
		if _, err := conn.Exec(ctx, "alter table orders alter column note set not null"); err != nil {
			t.Fatal(err)
		}
		if m := q.last(t); m.Strategy != "multistep" || len(m.Meta.Steps) != 2 || m.Meta.Steps[1].Skip.Kind != "notnull_valid" ||
			m.Meta.Steps[1].Skip.Table != "orders" || m.Meta.Steps[1].Skip.Name != "note" || m.Meta.Steps[1].OnFail == "" ||
			!strings.HasPrefix(m.Meta.Steps[0].SQL, `ALTER TABLE "orders" ADD CONSTRAINT "orders_note_not_null" NOT NULL`) {
			t.Fatalf("queued steps %+v", m.Meta.Steps)
		}
		var n int
		if err := conn.QueryRow(ctx, "select 1").Scan(&n); err != nil || n != 1 {
			t.Fatalf("session after DDL: %v", err)
		}
	}
	if q.waited < 6 {
		t.Fatalf("router waited %d times, want 6", q.waited)
	}
	for i := range h.poolers {
		for _, sql := range h.poolers[i].ran() {
			if strings.Contains(strings.ToLower(sql), "create table") || strings.Contains(strings.ToLower(sql), "grant") {
				t.Fatalf("DDL reached shard %d: %q", i, sql)
			}
		}
	}
}

// servingSetRenamed publishes the harness snapshot as a cutover leaves it:
// the shards serve under a new set name and the database's home shard id
// is the new set's.
func (h *shardedHarness) servingSetRenamed(set string, home int32) {
	s := *h.snap
	s.ServingSet = set
	s.ShardSets = map[string][]snapshot.Range{set: h.snap.ShardSets[DefaultShardSet]}
	s.Serving = map[snapshot.ShardKey]snapshot.Serving{}
	for k, v := range h.snap.Serving {
		k.ShardSet = set
		s.Serving[k] = v
	}
	s.Databases = map[string]catalog.Database{"app": {Name: "app", HomeShard: home, DefaultPlacement: "unsharded"}}
	h.setSnap(&s)
}

func TestDDLAfterACutoverIsStillQueued(t *testing.T) {
	q := &fakeQueue{}
	h := newDDLHarness(t, q)
	ctx := context.Background()
	before := h.connect(t, h.dsn())
	h.servingSetRenamed("g2", 1)
	after := h.connect(t, h.dsn())
	for _, conn := range []*pgx.Conn{before, after} {
		for _, sql := range []string{"create index orders_note_idx on orders (note)", "create table notes (id int primary key)", "grant select on items to app"} {
			if _, err := conn.Exec(ctx, sql); err != nil {
				t.Fatalf("%s: %v", sql, err)
			}
			if m := q.last(t); !strings.HasPrefix(m.Statement, sql) || m.HomeShard != 1 {
				t.Fatalf("%s: queued %+v, want it queued with the serving home shard 1", sql, m)
			}
		}
	}
	for i := range h.poolers {
		for _, sql := range h.poolers[i].ran() {
			if l := strings.ToLower(sql); strings.Contains(l, "create") || strings.Contains(l, "grant") {
				t.Fatalf("DDL reached shard %d directly instead of the queue: %q", i, sql)
			}
		}
	}
}

func TestDDLFailureNamesShards(t *testing.T) {
	q := &fakeQueue{outcome: func(m catalog.DDLMigration) catalog.DDLMigration {
		m.State = catalog.MigrationFailed
		m.PerShard = map[string]catalog.ShardMigration{
			"0": {State: catalog.ShardApplied, Attempts: 1},
			"1": {State: catalog.ShardFailed, Attempts: 1, Error: `relation "orders_idx" already exists`, SQLState: "42P07"},
			"2": {State: catalog.ShardApplied, Attempts: 1},
			"3": {State: catalog.ShardApplied, Attempts: 1},
		}
		return m
	}}
	h := newDDLHarness(t, q)
	conn := h.connect(t, h.dsn())
	_, err := conn.Exec(context.Background(), "create index orders_idx on orders (id)")
	var pe *pgconn.PgError
	if !errors.As(err, &pe) {
		t.Fatalf("expected an error, got %v", err)
	}
	if pe.Code != "42P07" || pe.Message != `relation "orders_idx" already exists` {
		t.Fatalf("got %s %q", pe.Code, pe.Message)
	}
	if !strings.Contains(pe.Detail, "failed on shard 1") || !strings.Contains(pe.Detail, "applied on shard 0, 2, 3") || !strings.Contains(pe.Detail, "DEGRADED") {
		t.Fatalf("detail %q", pe.Detail)
	}
}

func TestDDLAsyncReturnsAtOnce(t *testing.T) {
	q := &fakeQueue{delay: time.Hour}
	h := newDDLHarness(t, q)
	ctx := context.Background()
	var notices []string
	cfg, err := pgx.ParseConfig(h.dsn())
	if err != nil {
		t.Fatal(err)
	}
	cfg.OnNotice = func(_ *pgconn.PgConn, n *pgconn.Notice) { notices = append(notices, n.Message) }
	conn, err := pgx.ConnectConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close(ctx) }()
	if _, err := conn.Exec(ctx, "set pgshard.ddl_async = on"); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		tag, err := conn.Exec(ctx, "create index concurrently orders_id on orders (id)")
		if err == nil && tag.String() != "CREATE INDEX" {
			err = fmt.Errorf("tag %q", tag)
		}
		done <- err
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("async DDL blocked")
	}
	if len(notices) != 1 || !strings.Contains(notices[0], q.last(t).ID) {
		t.Fatalf("notices %q", notices)
	}
	if m := q.last(t); m.Strategy != "concurrent" {
		t.Fatalf("queued %+v", m)
	}
	if q.waited != 0 {
		t.Fatal("async DDL must not wait")
	}
	if _, err := conn.Exec(ctx, "reset pgshard.ddl_async"); err != nil {
		t.Fatal(err)
	}
	wctx, cancel := context.WithTimeout(ctx, 300*time.Millisecond)
	defer cancel()
	_, err = conn.Exec(wctx, "drop index orders_id")
	if err == nil {
		t.Fatal("after RESET the router must wait again")
	}
}

func TestDDLRefusedInTransactionAndRewriteClass(t *testing.T) {
	q := &fakeQueue{}
	h := newDDLHarness(t, q)
	ctx := context.Background()
	conn := h.connect(t, h.dsn())
	tx, err := conn.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	_, err = tx.Exec(ctx, "create table orders (tenant_id int8, id int, primary key (tenant_id, id))")
	_ = expectRefusal(t, err, "CREATE TABLE inside a transaction block is not available through the router")
	_ = tx.Rollback(ctx)
	if len(q.queued) != 0 {
		t.Fatal("refused DDL was queued")
	}
	_, err = conn.Exec(ctx, "alter table orders set unlogged")
	_ = expectRefusal(t, err, "ALTER TABLE ... SET UNLOGGED is not supported")
	_, err = conn.Exec(ctx, "drop table items, orders")
	_ = expectRefusal(t, err, "one DDL statement cannot touch both sharded and unsharded tables")
	if len(q.queued) != 0 {
		t.Fatal("refused DDL was queued")
	}
	q.enqueueE = errors.New("catalog down")
	_, err = conn.Exec(ctx, "create table t (id int)")
	pe := expectRefusalCode(t, err, "08006")
	if !strings.Contains(pe.Message, "catalog down") {
		t.Fatalf("message %q", pe.Message)
	}
}

func TestPGMigrationQueueWaitIsBounded(t *testing.T) {
	q := &PGMigrationQueue{Poll: time.Millisecond, MaxWait: 20 * time.Millisecond,
		load: func(context.Context, string) (catalog.DDLMigration, error) {
			return catalog.DDLMigration{State: catalog.MigrationQueued}, nil
		}}
	type result struct {
		err error
	}
	done := make(chan result, 1)
	go func() {
		_, err := q.Wait(context.Background(), "m1")
		done <- result{err}
	}()
	select {
	case r := <-done:
		if r.err == nil || !strings.Contains(r.err.Error(), "controller") {
			t.Fatalf("bounded wait names the controller: %v", r.err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Wait did not return without an applier")
	}
}

func TestPGMigrationQueueWaitResetsOnProgress(t *testing.T) {
	calls := 0
	q := &PGMigrationQueue{Poll: time.Millisecond, MaxWait: 25 * time.Millisecond,
		load: func(context.Context, string) (catalog.DDLMigration, error) {
			calls++
			if calls >= 80 {
				return catalog.DDLMigration{State: catalog.MigrationComplete}, nil
			}
			return catalog.DDLMigration{State: catalog.MigrationRunning,
				PerShard: map[string]catalog.ShardMigration{"0": {State: catalog.ShardRunning, Attempts: calls}}}, nil
		}}
	done := make(chan error, 1)
	go func() {
		m, err := q.Wait(context.Background(), "m1")
		if err == nil && m.State != catalog.MigrationComplete {
			err = fmt.Errorf("state %s", m.State)
		}
		done <- err
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("a migration progressing past MaxWait was aborted: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Wait never finished")
	}
}

// TestAMigrationIsAnsweredOnceTheRoutersSnapshotHasIt (PGS-871): the
// watcher's own reload after a migration can wait out a notification budget
// the migration's per-shard steps drained, and the client's next statement
// was planned without the view it had just created. An applied migration is
// answered only after the router has reloaded; a failed one, or one queued
// asynchronously, is not held for it; and a reload that fails still answers
// the applied DDL, with a warning.
func TestAMigrationIsAnsweredOnceTheRoutersSnapshotHasIt(t *testing.T) {
	ctx := context.Background()
	var (
		mu        sync.Mutex
		refreshes int
		waitedAt  int
		fail      error
	)
	q := &fakeQueue{}
	refresh := func(context.Context) error {
		mu.Lock()
		defer mu.Unlock()
		refreshes++
		q.mu.Lock()
		waitedAt = q.waited
		q.mu.Unlock()
		return fail
	}
	h := newShardedHarnessWith(t, Config{Migrations: q, RefreshSnapshot: func(ctx context.Context) error {
		time.Sleep(200 * time.Millisecond)
		return refresh(ctx)
	}})
	var notices []string
	cfg, err := pgx.ParseConfig(h.dsn())
	if err != nil {
		t.Fatal(err)
	}
	cfg.OnNotice = func(_ *pgconn.PgConn, n *pgconn.Notice) { notices = append(notices, n.Severity+": "+n.Message) }
	conn, err := pgx.ConnectConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close(ctx) }()

	start := time.Now()
	if _, err := conn.Exec(ctx, "create table audit (tenant_id int8, id int, primary key (tenant_id, id))"); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	if refreshes != 1 || waitedAt != 1 || time.Since(start) < 200*time.Millisecond {
		t.Fatalf("an applied migration was answered after %d refreshes (after %d waits) in %s; want it answered after the refresh that followed its wait", refreshes, waitedAt, time.Since(start))
	}
	mu.Unlock()

	q.mu.Lock()
	q.outcome = func(m catalog.DDLMigration) catalog.DDLMigration { m.State = catalog.MigrationFailed; return m }
	q.mu.Unlock()
	if _, err := conn.Exec(ctx, "create index orders_idx on orders (id)"); err == nil {
		t.Fatal("the failed migration was answered as applied")
	}
	if _, err := conn.Exec(ctx, "set pgshard.ddl_async = on"); err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Exec(ctx, "create index concurrently orders_id2 on orders (id)"); err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Exec(ctx, "reset pgshard.ddl_async"); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	if refreshes != 1 {
		t.Fatalf("a failed or asynchronous migration refreshed the snapshot: %d refreshes", refreshes)
	}
	fail = errors.New("catalog unreachable")
	mu.Unlock()

	q.mu.Lock()
	q.outcome = nil
	q.mu.Unlock()
	notices = nil
	if _, err := conn.Exec(ctx, "create table audit2 (tenant_id int8, id int, primary key (tenant_id, id))"); err != nil {
		t.Fatalf("an applied migration whose refresh failed was not answered: %v", err)
	}
	if len(notices) != 1 || !strings.HasPrefix(notices[0], "WARNING: ") || !strings.Contains(notices[0], "catalog unreachable") {
		t.Fatalf("notices after a failed refresh: %q", notices)
	}
}

// TestEachDDLOfAPipelinedBatchRunsItsOwnStatement (PGS-896): the batch
// handler ran the last DDL it saw for every Execute, so a client pipelining
// two before one Sync queued the second twice and never the first. The
// messages go on the wire by hand: pgx's pipeline reader also checks the
// order of the replies, which is PGS-897.
func TestEachDDLOfAPipelinedBatchRunsItsOwnStatement(t *testing.T) {
	q := &fakeQueue{}
	h := newDDLHarness(t, q)
	conn := h.connect(t, h.dsn())
	stmts := []string{"create table notes_a (id int primary key)", "create table notes_b (id int primary key)"}
	for _, named := range []bool{false, true} {
		q.mu.Lock()
		q.queued = nil
		q.mu.Unlock()
		fe := conn.PgConn().Frontend()
		for i, sql := range stmts {
			name := ""
			if named {
				name = fmt.Sprintf("ddl_%d", i)
			}
			fe.Send(&pgproto3.Parse{Name: name, Query: sql})
			fe.Send(&pgproto3.Bind{PreparedStatement: name})
			fe.Send(&pgproto3.Describe{ObjectType: 'P'})
			fe.Send(&pgproto3.Execute{})
		}
		fe.Send(&pgproto3.Sync{})
		if err := fe.Flush(); err != nil {
			t.Fatal(err)
		}
		completed := 0
		for {
			msg, err := fe.Receive()
			if err != nil {
				t.Fatal(err)
			}
			if e, ok := msg.(*pgproto3.ErrorResponse); ok {
				t.Fatalf("named %v: %s", named, e.Message)
			}
			if _, ok := msg.(*pgproto3.CommandComplete); ok {
				completed++
			}
			if _, ok := msg.(*pgproto3.ReadyForQuery); ok {
				break
			}
		}
		q.mu.Lock()
		var got []string
		for _, m := range q.queued {
			got = append(got, m.Statement)
		}
		q.mu.Unlock()
		if completed != 2 || len(got) != 2 || got[0] != stmts[0] || got[1] != stmts[1] {
			t.Fatalf("named %v: %d completions, queued %q, want %q in order", named, completed, got, stmts)
		}
	}
}

// A portal runs the statement it was bound to, even when the name was parsed
// again before its Execute.
func TestADDLPortalRunsTheStatementItWasBoundTo(t *testing.T) {
	q := &fakeQueue{}
	h := newDDLHarness(t, q)
	conn := h.connect(t, h.dsn())
	fe := conn.PgConn().Frontend()
	fe.Send(&pgproto3.Parse{Query: "create table notes_a (id int primary key)"})
	fe.Send(&pgproto3.Bind{DestinationPortal: "p1"})
	fe.Send(&pgproto3.Parse{Query: "create table notes_b (id int primary key)"})
	fe.Send(&pgproto3.Bind{DestinationPortal: "p2"})
	fe.Send(&pgproto3.Execute{Portal: "p1"})
	fe.Send(&pgproto3.Execute{Portal: "p2"})
	fe.Send(&pgproto3.Sync{})
	if err := fe.Flush(); err != nil {
		t.Fatal(err)
	}
	for {
		msg, err := fe.Receive()
		if err != nil {
			t.Fatal(err)
		}
		if e, ok := msg.(*pgproto3.ErrorResponse); ok {
			t.Fatal(e.Message)
		}
		if _, ok := msg.(*pgproto3.ReadyForQuery); ok {
			break
		}
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	if len(q.queued) != 2 || !strings.Contains(q.queued[0].Statement, "notes_a") || !strings.Contains(q.queued[1].Statement, "notes_b") {
		t.Fatalf("queued %+v, want notes_a then notes_b", q.queued)
	}
}

// A DDL statement pipelined with anything else is refused whole: the
// migration and the other statement cannot share a batch's transaction.
func TestADDLBatchWithAnotherStatementIsRefused(t *testing.T) {
	q := &fakeQueue{}
	h := newDDLHarness(t, q)
	conn := h.connect(t, h.dsn())
	for _, order := range [][]string{{"create table notes_c (id int primary key)", "select 1"}, {"select 1", "create table notes_c (id int primary key)"}} {
		fe := conn.PgConn().Frontend()
		for _, sql := range order {
			fe.Send(&pgproto3.Parse{Query: sql})
			fe.Send(&pgproto3.Bind{})
			fe.Send(&pgproto3.Execute{})
		}
		fe.Send(&pgproto3.Sync{})
		if err := fe.Flush(); err != nil {
			t.Fatal(err)
		}
		var refused *pgproto3.ErrorResponse
		for {
			msg, err := fe.Receive()
			if err != nil {
				t.Fatal(err)
			}
			if e, ok := msg.(*pgproto3.ErrorResponse); ok && refused == nil {
				refused = e
			}
			if _, ok := msg.(*pgproto3.ReadyForQuery); ok {
				break
			}
		}
		if refused == nil || refused.Code != "0A000" || !strings.Contains(refused.Message, "only statement of its batch") {
			t.Fatalf("%q: got %+v, want the batch refused", order, refused)
		}
		q.mu.Lock()
		n := len(q.queued)
		q.mu.Unlock()
		if n != 0 {
			t.Fatalf("%q: %d migrations queued from a refused batch", order, n)
		}
	}
}

// TestASequentialDatabaseRunsATransactionsDDLStatementByStatement
// (PGS-869): pgroll sends DDL inside transactions that hold nothing else --
// its version views as one query, "BEGIN; DROP VIEW; CREATE VIEW; COMMIT",
// and batches of ALTER TABLE joined with semicolons. A database declared
// ddl_transactions = 'sequential' runs each such statement as if it had
// been sent on its own, and says so; a transaction that also runs a
// statement on a shard is still refused, whichever comes first.
func TestASequentialDatabaseRunsATransactionsDDLStatementByStatement(t *testing.T) {
	q := &fakeQueue{}
	h := newDDLHarness(t, q)
	app := h.snap.Databases["app"]
	app.DDLTransactions = catalog.DDLTransactionsSequential
	h.snap.Databases["app"] = app
	ctx := context.Background()
	var notices []string
	cfg, err := pgx.ParseConfig(h.dsn())
	if err != nil {
		t.Fatal(err)
	}
	cfg.OnNotice = func(_ *pgconn.PgConn, n *pgconn.Notice) { notices = append(notices, n.Message) }
	conn, err := pgx.ConnectConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close(ctx) }()
	kinds := func(from int) []string {
		q.mu.Lock()
		defer q.mu.Unlock()
		var out []string
		for _, m := range q.queued[from:] {
			out = append(out, m.Kind)
		}
		return out
	}
	idle := func(what string) {
		t.Helper()
		if s := conn.PgConn().TxStatus(); s != 'I' {
			t.Fatalf("after %s the session is in transaction status %q, want idle", what, s)
		}
	}

	results, err := conn.PgConn().Exec(ctx, "BEGIN; DROP VIEW IF EXISTS v; CREATE VIEW v AS SELECT id FROM orders; COMMIT").ReadAll()
	if err != nil {
		t.Fatalf("pgroll's version-view query: %v", err)
	}
	if got := kinds(0); strings.Join(got, ",") != "DROP VIEW,CREATE VIEW" || len(results) != 4 {
		t.Fatalf("queued %q from %d results", got, len(results))
	}
	if len(notices) != 2 || !strings.Contains(notices[0], "applied on its own") {
		t.Fatalf("notices %q", notices)
	}
	idle("the version-view query")

	if _, err := conn.PgConn().Exec(ctx, "ALTER TABLE orders ADD COLUMN a int; ALTER TABLE orders ADD COLUMN b int").ReadAll(); err != nil {
		t.Fatalf("a batch of ALTER TABLE: %v", err)
	}
	if got := kinds(2); strings.Join(got, ",") != "ALTER TABLE,ALTER TABLE" {
		t.Fatalf("queued %q", got)
	}
	idle("a batch of ALTER TABLE")

	tx, err := conn.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, "create table t2 (id int primary key)"); err != nil {
		t.Fatalf("DDL in a transaction: %v", err)
	}
	_, err = tx.Exec(ctx, "select * from items")
	_ = expectRefusal(t, err, "a statement that runs on a shard is not available after DDL in the same transaction")
	_, err = tx.Exec(ctx, "select * from items where id = $1", 1)
	_ = expectRefusal(t, err, "a statement that runs on a shard is not available after DDL in the same transaction")
	if err := tx.Rollback(ctx); err != nil {
		t.Fatal(err)
	}

	tx, err = conn.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, "select * from items"); err != nil {
		t.Fatal(err)
	}
	before := len(kinds(0))
	_, err = tx.Exec(ctx, "create table t3 (id int primary key)")
	_ = expectRefusal(t, err, "CREATE TABLE is not available in a transaction that has already run a statement on a shard")
	_ = tx.Rollback(ctx)
	if len(kinds(0)) != before {
		t.Fatal("DDL after a statement on a shard was queued")
	}

	q.outcome = func(m catalog.DDLMigration) catalog.DDLMigration {
		m.State, m.Error = catalog.MigrationFailed, "relation already exists"
		return m
	}
	tx, err = conn.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, "create table t4 (id int primary key)"); err == nil {
		t.Fatal("a failed migration answered success")
	}
	_, err = tx.Exec(ctx, "create table t5 (id int primary key)")
	var pe *pgconn.PgError
	if !errors.As(err, &pe) || pe.Code != "25P02" {
		t.Fatalf("a statement after the failed DDL: %v, want 25P02", err)
	}
	if err := tx.Commit(ctx); !errors.Is(err, pgx.ErrTxCommitRollback) {
		t.Fatalf("COMMIT of the failed transaction: %v, want it rolled back", err)
	}
	idle("the failed transaction")
}

// TestAnAtomicDatabaseStillRefusesDDLInATransaction: the default is
// unchanged, including for pgroll's version-view query.
func TestAnAtomicDatabaseStillRefusesDDLInATransaction(t *testing.T) {
	q := &fakeQueue{}
	h := newDDLHarness(t, q)
	app := h.snap.Databases["app"]
	app.DDLTransactions = catalog.DDLTransactionsAtomic
	h.snap.Databases["app"] = app
	conn := h.connect(t, h.dsn())
	_, err := conn.PgConn().Exec(context.Background(), "BEGIN; DROP VIEW IF EXISTS v; CREATE VIEW v AS SELECT id FROM orders; COMMIT").ReadAll()
	_ = expectRefusal(t, err, "a transaction control statement is not available inside a multi-statement simple query")
	_, err = conn.PgConn().Exec(context.Background(), "ALTER TABLE orders ADD COLUMN a int; ALTER TABLE orders ADD COLUMN b int").ReadAll()
	_ = expectRefusal(t, err, "ALTER TABLE is not available inside a multi-statement simple query")
	if len(q.queued) != 0 {
		t.Fatalf("queued %d migrations", len(q.queued))
	}
}
