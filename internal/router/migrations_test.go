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
	// attach, when set, answers an enqueue with an existing migration.
	attach func(m catalog.DDLMigration) (catalog.EnqueueResult, bool)
	// blockers is what every waiting migration is told it waits for.
	blockers []catalog.Blocker
	// home and homeQueued answer HomeDDLBlockers.
	home       []catalog.Blocker
	homeQueued bool
	// noQueue makes the catalog one without the operation queue at all,
	// which is a router newer than its catalog mid-rollout.
	noQueue bool
	// waitErr, when set, is what Wait fails with.
	waitErr error
}

func (q *fakeQueue) Enqueue(_ context.Context, m catalog.DDLMigration) (catalog.EnqueueResult, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.enqueueE != nil {
		return catalog.EnqueueResult{}, q.enqueueE
	}
	if q.attach != nil {
		if r, ok := q.attach(m); ok {
			return r, nil
		}
	}
	m.ID = fmt.Sprintf("00000000-0000-0000-0000-%012d", len(q.queued)+1)
	q.queued = append(q.queued, m)
	// As the real one does: a catalog with no queue has nowhere to store a
	// key, whatever the caller asked for.
	return catalog.EnqueueResult{ID: m.ID, State: catalog.MigrationQueued, Deduplicated: !q.noQueue && m.DedupKey != ""}, nil
}

func (q *fakeQueue) HomeDDLBlockers(context.Context, string) ([]catalog.Blocker, bool, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.home, q.homeQueued, nil
}

// QueueSchema answers for a catalog that has the operation queue unless a
// test says otherwise: that is the state the feature is about, and the
// queue-less catalog is the rollout case two tests set explicitly.
func (q *fakeQueue) QueueSchema(context.Context) (bool, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	return !q.noQueue, nil
}

func (q *fakeQueue) Wait(ctx context.Context, id string, waiting func([]catalog.Blocker)) (catalog.DDLMigration, error) {
	q.mu.Lock()
	failWith := q.waitErr
	q.mu.Unlock()
	if failWith != nil {
		return catalog.DDLMigration{ID: id, State: catalog.MigrationQueued}, failWith
	}
	q.mu.Lock()
	q.waited++
	var m catalog.DDLMigration
	for _, x := range q.queued {
		if x.ID == id {
			m = x
		}
	}
	delay, outcome, blockers := q.delay, q.outcome, q.blockers
	q.mu.Unlock()
	if waiting != nil && len(blockers) > 0 {
		waiting(blockers)
	}
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

func newDDLHarness(t *testing.T, q *fakeQueue, opts ...func(*harness)) *shardedHarness {
	t.Helper()
	return newShardedHarnessWith(t, Config{Migrations: q}, opts...)
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
		_, err := q.Wait(context.Background(), "m1", nil)
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
		m, err := q.Wait(context.Background(), "m1", nil)
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

// TestAnIdenticalDDLAttachesAndSaysSoAtOnce (PGS-900): a client whose DDL
// timed out sends it again. The router hands it the migration already
// queued, and says so while it waits rather than when it is done.
func TestAnIdenticalDDLAttachesAndSaysSoAtOnce(t *testing.T) {
	q := &fakeQueue{delay: 400 * time.Millisecond}
	q.attach = func(m catalog.DDLMigration) (catalog.EnqueueResult, bool) {
		q.queued = append(q.queued, catalog.DDLMigration{ID: "00000000-0000-0000-0000-00000000a77a", Statement: m.Statement, DedupKey: m.DedupKey})
		return catalog.EnqueueResult{ID: "00000000-0000-0000-0000-00000000a77a", Attached: true, State: catalog.MigrationRunning}, true
	}
	h := newDDLHarness(t, q)
	ctx := context.Background()
	cfg, err := pgx.ParseConfig(h.dsn())
	if err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	var noticeAt time.Time
	var notices []string
	cfg.OnNotice = func(_ *pgconn.PgConn, n *pgconn.Notice) {
		mu.Lock()
		defer mu.Unlock()
		notices = append(notices, n.Message)
		if noticeAt.IsZero() {
			noticeAt = time.Now()
		}
	}
	conn, err := pgx.ConnectConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close(ctx) }()
	if _, err := conn.Exec(ctx, "create index orders_note_idx on orders (note)"); err != nil {
		t.Fatal(err)
	}
	answered := time.Now()
	mu.Lock()
	defer mu.Unlock()
	if len(notices) != 1 || !strings.Contains(notices[0], "identical migration 00000000-0000-0000-0000-00000000a77a is already running") {
		t.Fatalf("notices %q", notices)
	}
	if answered.Sub(noticeAt) < 200*time.Millisecond {
		t.Fatalf("the notice arrived %s before the answer; it must reach the client while the statement waits", answered.Sub(noticeAt))
	}
	if key := q.queued[0].DedupKey; key == "" {
		t.Fatal("the statement was queued with no dedup key")
	}
}

// TestADDLWaitEndsAtTheSessionStatementTimeout (PGS-900): the router did not
// read statement_timeout, so a client's timeout never ended a DDL wait. It
// now does, and says the migration continues and how to wait for it again.
func TestADDLWaitEndsAtTheSessionStatementTimeout(t *testing.T) {
	q := &fakeQueue{delay: time.Hour}
	h := newDDLHarness(t, q)
	ctx := context.Background()
	for _, c := range []struct{ dsn, set string }{
		{h.dsn(), "set statement_timeout = '150ms'"},
		{h.dsn() + "&options=-c%20statement_timeout%3D150", ""},
	} {
		conn := h.connect(t, c.dsn)
		if c.set != "" {
			if _, err := conn.Exec(ctx, c.set); err != nil {
				t.Fatal(err)
			}
		}
		start := time.Now()
		_, err := conn.Exec(ctx, "create index orders_note_idx on orders (note)")
		var pe *pgconn.PgError
		if !errors.As(err, &pe) || pe.Code != "57014" || pe.Message != "canceling statement due to statement timeout" ||
			!strings.Contains(pe.Detail, q.last(t).ID+" continues in the background") || !strings.Contains(pe.Detail, "Running the same statement again waits for it") {
			t.Fatalf("%q: %v, want 57014 naming the migration that continues", c.dsn, err)
		}
		if took := time.Since(start); took > 5*time.Second {
			t.Fatalf("the wait ended after %s", took)
		}
	}
	// RESET puts the session back to the startup value, so a wait longer
	// than the timeout that was set is no longer cut short.
	slow := &fakeQueue{delay: 400 * time.Millisecond}
	sh := newDDLHarness(t, slow)
	conn := sh.connect(t, sh.dsn())
	for _, sql := range []string{"set statement_timeout = '150ms'", "reset statement_timeout"} {
		if _, err := conn.Exec(ctx, sql); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := conn.Exec(ctx, "create index orders_note_idx on orders (note)"); err != nil {
		t.Fatalf("after RESET statement_timeout: %v", err)
	}
	if _, err := conn.Exec(ctx, "set statement_timeout = '150ms'"); err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Exec(ctx, "create index orders_note_idx on orders (note)"); err == nil {
		t.Fatal("a wait longer than the session's statement_timeout was not cut short")
	}
}

// TestAWaitingDDLSaysWhatItWaitsFor (PGS-900): DDL behind a reshard is
// queued, not refused, and the client is told what holds it.
func TestAWaitingDDLSaysWhatItWaitsFor(t *testing.T) {
	q := &fakeQueue{blockers: []catalog.Blocker{{Kind: catalog.OperationReshard, ID: "00000000-0000-0000-0000-00000000e5a1", Reason: catalog.BlockedByStarted}}}
	h := newDDLHarness(t, q)
	ctx := context.Background()
	cfg, err := pgx.ParseConfig(h.dsn())
	if err != nil {
		t.Fatal(err)
	}
	var notices []string
	cfg.OnNotice = func(_ *pgconn.PgConn, n *pgconn.Notice) { notices = append(notices, n.Message) }
	conn, err := pgx.ConnectConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close(ctx) }()
	if _, err := conn.Exec(ctx, "alter table orders add column extra int"); err != nil {
		t.Fatal(err)
	}
	if len(notices) != 1 || !strings.Contains(notices[0], "waits for reshard 00000000-0000-0000-0000-00000000e5a1 (in progress)") {
		t.Fatalf("notices %q", notices)
	}
}

// TestPgshardDDLDedupOffQueuesWithoutAKey (PGS-900).
func TestPgshardDDLDedupOffQueuesWithoutAKey(t *testing.T) {
	q := &fakeQueue{}
	h := newDDLHarness(t, q)
	ctx := context.Background()
	conn := h.connect(t, h.dsn())
	if _, err := conn.Exec(ctx, "create index a on orders (note)"); err != nil {
		t.Fatal(err)
	}
	if q.last(t).DedupKey == "" {
		t.Fatal("DDL was queued with no dedup key by default")
	}
	if _, err := conn.Exec(ctx, "set pgshard.ddl_dedup = off"); err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Exec(ctx, "create index a on orders (note)"); err != nil {
		t.Fatal(err)
	}
	if q.last(t).DedupKey != "" {
		t.Fatal("pgshard.ddl_dedup = off still queued a key")
	}
}

func TestNormalizedStatementIgnoresSpelling(t *testing.T) {
	a := normalizedStatement("create   index i ON orders (note) -- retry\n")
	if b := normalizedStatement("CREATE INDEX i ON orders(note);"); a != b {
		t.Fatalf("%q != %q", a, b)
	}
	if c := normalizedStatement("CREATE INDEX j ON orders(note)"); c == a {
		t.Fatal("a different index name normalises to the same statement")
	}
}

func TestParseTimeGUC(t *testing.T) {
	for v, want := range map[string]time.Duration{"": 0, "0": 0, "150": 150 * time.Millisecond, "5s": 5 * time.Second, "5 s": 5 * time.Second,
		"2min": 2 * time.Minute, "1h": time.Hour, "1d": 24 * time.Hour, "500us": 500 * time.Microsecond, "1.5s": 1500 * time.Millisecond, "x": 0, "5 years": 0} {
		if got := parseTimeGUC(v); got != want {
			t.Errorf("%q = %s, want %s", v, got, want)
		}
	}
}

// TestAWaitKeepsGoingWhileTheControllerIsAlive (PGS-900): a migration held
// behind a long reshard makes no progress for hours. The wait gave up after
// ten minutes of that; it now gives up only when the controller's heartbeat
// has stopped, and reads through a short catalog outage.
func TestAWaitKeepsGoingWhileTheControllerIsAlive(t *testing.T) {
	queued := catalog.DDLMigration{State: catalog.MigrationQueued}
	var mu sync.Mutex
	beatAge, polls, failFor := time.Duration(0), 0, 0
	q := &PGMigrationQueue{Poll: time.Millisecond, MaxWait: 30 * time.Millisecond, CatalogOutage: time.Hour,
		queue: func(context.Context) (bool, error) { return true, nil },
		load: func(context.Context, string) (catalog.DDLMigration, error) {
			mu.Lock()
			defer mu.Unlock()
			polls++
			if failFor > 0 {
				failFor--
				return queued, errors.New("catalog restarting")
			}
			return queued, nil
		},
		blockers: func(context.Context, string) ([]catalog.Blocker, error) { return nil, nil },
		beat: func(context.Context) (time.Duration, bool, error) {
			mu.Lock()
			defer mu.Unlock()
			return beatAge, true, nil
		},
	}
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	mu.Lock()
	failFor = 20
	mu.Unlock()
	if _, err := q.Wait(ctx, "m1", nil); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("with a fresh heartbeat the wait ended with %v, want it to last until the caller gave up", err)
	}
	mu.Lock()
	beatAge = time.Hour
	mu.Unlock()
	_, err := q.Wait(context.Background(), "m1", nil)
	var quiet errNoApplier
	if !errors.As(err, &quiet) {
		t.Fatalf("with a stale heartbeat the wait ended with %v, want errNoApplier", err)
	}
}

// TestALocalDatabasesDDLWaitsForNothingItWouldOverlap (PGS-900): DDL on a
// local database runs on its home shard at once and cannot wait its turn, so
// it is refused while something it would overlap is unfinished -- read from
// the operation queue, or from the snapshot on a catalog without one.
func TestALocalDatabasesDDLWaitsForNothingItWouldOverlap(t *testing.T) {
	q := &fakeQueue{}
	h := newDDLHarness(t, q)
	local := *h.snap
	local.Databases = map[string]catalog.Database{"app": {Name: "app", HomeShard: 0, DefaultPlacement: "unsharded", LocalOnly: true}}
	h.setSnap(&local)
	ctx := context.Background()
	conn := h.connect(t, h.dsn())

	q.homeQueued = true
	q.home = []catalog.Blocker{{Kind: catalog.OperationReshard, ID: "00000000-0000-0000-0000-00000000e5a2", Reason: catalog.BlockedByStarted}}
	_, err := conn.Exec(ctx, "alter table items add column extra int")
	var pe *pgconn.PgError
	if !errors.As(err, &pe) || pe.Code != "55000" || !strings.Contains(pe.Message, "reshard 00000000-0000-0000-0000-00000000e5a2") {
		t.Fatalf("local DDL behind a reshard: %v, want 55000 naming it", err)
	}

	q.home = nil
	if _, err := conn.Exec(ctx, "alter table items add column extra int"); err != nil && strings.Contains(err.Error(), "not available") {
		t.Fatalf("local DDL with nothing in the queue: %v", err)
	}

	q.homeQueued = false
	resharding := local
	resharding.Serving = map[snapshot.ShardKey]snapshot.Serving{}
	for k, v := range local.Serving {
		resharding.Serving[k] = v
	}
	resharding.Serving[snapshot.ShardKey{ShardSet: "g2", ShardID: 0}] = snapshot.Serving{State: "provisioning"}
	h.setSnap(&resharding)
	if _, err := conn.Exec(ctx, "alter table items add column extra2 int"); err == nil || !strings.Contains(err.Error(), "not available while a reshard is active") {
		t.Fatalf("local DDL during a reshard on a catalog without the queue: %v", err)
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

	tx, err = conn.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, "select bad"); err == nil {
		t.Fatal("the fake pooler's failing statement succeeded")
	}
	before = len(kinds(0))
	_, err = tx.Exec(ctx, "create table t6 (id int primary key)")
	var aborted *pgconn.PgError
	if !errors.As(err, &aborted) || aborted.Code != "25P02" {
		t.Fatalf("DDL in a transaction a statement already failed: %v, want 25P02", err)
	}
	if len(kinds(0)) != before {
		t.Fatal("DDL in an aborted transaction was queued")
	}
	_ = tx.Rollback(ctx)

	// The migration does not wait inside an idle transaction on a backend:
	// the backend's transaction is ended for the wait and opened again.
	sent := func() []int {
		var n []int
		for _, fp := range h.poolers {
			qs, _ := fp.numbered()
			n = append(n, len(qs))
		}
		return n
	}
	mark := sent()
	tx, err = conn.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, "create table t7 (id int primary key)"); err != nil {
		t.Fatal(err)
	}
	if s := conn.PgConn().TxStatus(); s != 'T' {
		t.Fatalf("after DDL in a transaction the session is in status %q, want in transaction", s)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	var got []string
	for i, fp := range h.poolers {
		qs, _ := fp.numbered()
		for _, q := range qs[mark[i]:] {
			got = append(got, strings.ToLower(q.sql))
		}
	}
	if joined := strings.Join(got, ";"); !strings.Contains(joined, "begin;rollback;begin") {
		t.Fatalf("the shards saw %q, want the transaction ended for the wait and reopened", joined)
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

// TestOneStatementIsAnsweredOnce counts the command tags on the wire.
//
// A statement gets exactly one CommandComplete, whatever it did and
// whichever protocol carried it. The async branch used to send its own on
// top of the one the caller sends, so every DDL under pgshard.ddl_async put
// two on the wire -- which PostgreSQL never does, and which desynchronises
// a client that pairs one tag with one Execute. Reading the command tag
// cannot see it: a driver keeps the last one, and both are the same.
func TestOneStatementIsAnsweredOnce(t *testing.T) {
	for _, async := range []bool{false, true} {
		name := "sync"
		if async {
			name = "async"
		}
		t.Run(name, func(t *testing.T) {
			q := &fakeQueue{}
			if async {
				q.delay = time.Hour
			}
			h := newDDLHarness(t, q)
			ctx := context.Background()
			conn := h.connect(t, h.dsn())
			if async {
				if _, err := conn.Exec(ctx, "set pgshard.ddl_async = on"); err != nil {
					t.Fatal(err)
				}
			}
			hj, err := conn.PgConn().Hijack()
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = hj.Conn.Close() }()
			fe := hj.Frontend

			fe.Send(&pgproto3.Parse{Query: "create index concurrently orders_id on orders (id)"})
			fe.Send(&pgproto3.Bind{})
			fe.Send(&pgproto3.Execute{})
			fe.Send(&pgproto3.Sync{})
			if err := fe.Flush(); err != nil {
				t.Fatal(err)
			}
			_ = hj.Conn.SetReadDeadline(time.Now().Add(30 * time.Second))
			var got []string
			for {
				m, err := fe.Receive()
				if err != nil {
					t.Fatalf("receive: %v (got %v)", err, got)
				}
				got = append(got, fmt.Sprintf("%T", m))
				if _, done := m.(*pgproto3.ReadyForQuery); done {
					break
				}
			}
			tags := 0
			for _, m := range got {
				if m == "*pgproto3.CommandComplete" {
					tags++
				}
			}
			if tags != 1 {
				t.Fatalf("%d command tags for one statement: %v", tags, got)
			}
		})
	}
}

// TestFanOutDDLDuringAReshardOnAQueuelessCatalogIsRefusedAtOnce: a router
// newer than its catalog is an ordinary moment in a rollout. Without the
// operation queue there is nowhere for a migration to wait its turn, and
// the applier holds it for the whole copy -- so the router that used to
// refuse it in milliseconds waited ten minutes and then told the client no
// controller had been seen applying migrations, while one was running and
// leading.
func TestFanOutDDLDuringAReshardOnAQueuelessCatalogIsRefusedAtOnce(t *testing.T) {
	q := &fakeQueue{noQueue: true}
	h := newDDLHarness(t, q)
	ctx := context.Background()
	conn := h.connect(t, h.dsn())

	base := *h.snap
	resharding := base
	resharding.Serving = map[snapshot.ShardKey]snapshot.Serving{}
	for k, v := range base.Serving {
		resharding.Serving[k] = v
	}
	resharding.Serving[snapshot.ShardKey{ShardSet: "g2", ShardID: 0}] = snapshot.Serving{State: "provisioning"}
	h.setSnap(&resharding)

	start := time.Now()
	_, err := conn.Exec(ctx, "alter table orders add column extra int")
	if err == nil {
		t.Fatal("fan-out DDL during a reshard on a catalog without the queue was accepted")
	}
	if !strings.Contains(err.Error(), "not available while a reshard is active") {
		t.Fatalf("refused for the wrong reason: %v", err)
	}
	if time.Since(start) > 5*time.Second {
		t.Fatalf("the refusal took %s; it is meant to be immediate", time.Since(start))
	}
	if len(q.queued) != 0 {
		t.Fatalf("the statement was queued into a catalog that has no queue: %d", len(q.queued))
	}

	// With the queue there, the same statement is queued rather than
	// refused: that is the whole point, and this is the control.
	q.noQueue = false
	if _, err := conn.Exec(ctx, "alter table orders add column extra2 int"); err != nil {
		t.Fatalf("with the queue, the statement should be queued and applied: %v", err)
	}
}

// TestATimeoutPromisesDedupOnlyWhereTheCatalogCanDoIt: the DETAIL on a
// timed-out DDL tells the client the migration continues, and -- when the
// statement carries a dedup key the catalog stored -- that sending it again
// waits for that migration rather than queueing another.
//
// The sentence used to be chosen from the router's own request, which says
// only what the router intended. A catalog without the operation queue has
// no dedup_key column, so the enqueue drops the key, and the advised retry
// queued a second migration: the index was built twice, on the advice of
// the error that suggested it.
func TestATimeoutPromisesDedupOnlyWhereTheCatalogCanDoIt(t *testing.T) {
	const promise = "Running the same statement again waits for it"
	for _, c := range []struct{ queue, promised bool }{{true, true}, {false, false}} {
		name := "without the queue"
		if c.queue {
			name = "with the queue"
		}
		t.Run(name, func(t *testing.T) {
			q := &fakeQueue{delay: time.Hour, noQueue: !c.queue}
			h := newDDLHarness(t, q)
			ctx := context.Background()
			conn := h.connect(t, h.dsn())
			if _, err := conn.Exec(ctx, "set statement_timeout = '300ms'"); err != nil {
				t.Fatal(err)
			}
			_, err := conn.Exec(ctx, "create index concurrently orders_note_idx on orders (note)")
			var pgErr *pgconn.PgError
			if !errors.As(err, &pgErr) || pgErr.Code != "57014" {
				t.Fatalf("want a statement timeout, got %v", err)
			}
			if !strings.Contains(pgErr.Detail, "continues in the background") {
				t.Fatalf("detail %q", pgErr.Detail)
			}
			if got := strings.Contains(pgErr.Detail, promise); got != c.promised {
				t.Fatalf("the detail promises dedup = %v, want %v: %q", got, c.promised, pgErr.Detail)
			}
		})
	}
}

// TestWaitAsksWhatAMigrationWaitsForAndSaysSoOnlyWhenItChanges covers the
// half of the waiting notice the fake queue used to stand in for: the
// router's callback was tested against a fake that invoked it itself, so
// the query, the throttle, the queued-only guard and the change detection
// in PGMigrationQueue.Wait had no coverage at all.
func TestWaitAsksWhatAMigrationWaitsForAndSaysSoOnlyWhenItChanges(t *testing.T) {
	reshard := catalog.Blocker{Kind: catalog.OperationReshard, ID: "r1", Reason: catalog.BlockedByStarted}
	earlier := catalog.Blocker{Kind: catalog.OperationDDL, ID: "d1", Reason: catalog.BlockedByEarlier}

	var mu sync.Mutex
	state := catalog.MigrationQueued
	blockers := []catalog.Blocker{reshard, earlier}
	asked, loads := 0, 0
	q := &PGMigrationQueue{
		Poll: time.Millisecond, MaxWait: 10 * time.Second, BlockersEvery: 2 * time.Millisecond,
		load: func(context.Context, string) (catalog.DDLMigration, error) {
			mu.Lock()
			defer mu.Unlock()
			loads++
			return catalog.DDLMigration{State: state}, nil
		},
		blockers: func(context.Context, string) ([]catalog.Blocker, error) {
			mu.Lock()
			defer mu.Unlock()
			asked++
			return blockers, nil
		},
		queue: func(context.Context) (bool, error) { return true, nil },
		beat:  func(context.Context) (time.Duration, bool, error) { return 0, true, nil },
	}
	var told [][]catalog.Blocker
	done := make(chan error, 1)
	go func() {
		_, err := q.Wait(context.Background(), "m1", func(bs []catalog.Blocker) {
			mu.Lock()
			defer mu.Unlock()
			told = append(told, bs)
		})
		done <- err
	}()

	// The same set, polled many times over, is said once.
	waitFor(t, 10*time.Second, func() bool { mu.Lock(); defer mu.Unlock(); return len(told) == 1 && asked > 3 }, "the first blocker set to be reported once")
	// A set that changes is said again.
	mu.Lock()
	blockers = []catalog.Blocker{reshard}
	mu.Unlock()
	waitFor(t, 10*time.Second, func() bool { mu.Lock(); defer mu.Unlock(); return len(told) == 2 }, "a changed blocker set to be reported")
	// Once the migration is running it is no longer waiting for anything,
	// so the session is not told about blockers it no longer has.
	mu.Lock()
	state, blockers, asked = catalog.MigrationRunning, []catalog.Blocker{earlier}, 0
	from := loads
	mu.Unlock()
	// Long enough that a query that was still going to happen has: many
	// polls and several blocker intervals. Asserting "asked == 0" the
	// moment after zeroing it would hold whatever the code did.
	waitFor(t, 10*time.Second, func() bool { mu.Lock(); defer mu.Unlock(); return loads-from > 30 }, "the wait to poll on while the migration runs")
	mu.Lock()
	if asked != 0 {
		t.Errorf("a running migration was asked what it waits for %d times", asked)
	}
	if len(told) != 2 {
		t.Errorf("a running migration was reported as waiting: %v", told)
	}
	state = catalog.MigrationComplete
	mu.Unlock()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(told) != 2 || len(told[0]) != 2 || told[0][0] != reshard || len(told[1]) != 1 {
		t.Fatalf("what the session was told: %v", told)
	}
}

// TestDescribeBlockersNamesEveryKindAndBothReasons: what a waiting session
// is told about what holds it. Every blocker in the suite was a started
// reshard, so collapsing "queued before it" into "in progress" passed --
// and a client waiting behind a migration that has not started would have
// been told it was running.
func TestDescribeBlockersNamesEveryKindAndBothReasons(t *testing.T) {
	got := describeBlockers([]catalog.Blocker{
		{Kind: catalog.OperationReshard, ID: "r1", Reason: catalog.BlockedByStarted},
		{Kind: catalog.OperationUpgrade, ID: "u1", Reason: catalog.BlockedByStarted},
		{Kind: catalog.OperationPlacement, ID: "p1", Reason: catalog.BlockedByStarted},
		{Kind: catalog.OperationDDL, ID: "d1", Reason: catalog.BlockedByEarlier},
		{Kind: "something new", ID: "x1", Reason: catalog.BlockedByEarlier},
	})
	want := "reshard r1 (in progress), major upgrade u1 (in progress), table placement p1 (in progress), " +
		"migration d1 (queued before it), something new x1 (queued before it)"
	if got != want {
		t.Fatalf("describeBlockers:\n got %s\nwant %s", got, want)
	}
	if describeBlockers(nil) != "" {
		t.Errorf("nothing to wait for reads %q", describeBlockers(nil))
	}
}

// TestAStatementAttachedToAFinishedMigrationIsNotRunAgain: the retry window
// answers a statement whose identical migration completed a moment ago
// without running it, and says which. fakeQueue.attach always answered
// "running", so this branch shipped untested.
func TestAStatementAttachedToAFinishedMigrationIsNotRunAgain(t *testing.T) {
	q := &fakeQueue{}
	q.attach = func(catalog.DDLMigration) (catalog.EnqueueResult, bool) {
		return catalog.EnqueueResult{ID: "00000000-0000-0000-0000-00000000c0de", Attached: true,
			State: catalog.MigrationComplete, Deduplicated: true}, true
	}
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
	if _, err := conn.Exec(ctx, "create index concurrently orders_note_idx on orders (note)"); err != nil {
		t.Fatal(err)
	}
	if len(notices) != 1 || !strings.Contains(notices[0], "has just completed; not running the statement again") {
		t.Fatalf("notices %q", notices)
	}
	if len(q.queued) != 0 {
		t.Fatalf("the statement was queued although an identical one had just completed: %d", len(q.queued))
	}
}

// TestAMigrationNoControllerIsDrivingIsReportedAsSuch: 55000, not a
// connection error, and it says the migration is still queued rather than
// that something went wrong with the connection.
func TestAMigrationNoControllerIsDrivingIsReportedAsSuch(t *testing.T) {
	q := &fakeQueue{waitErr: errNoApplier{id: "00000000-0000-0000-0000-00000000dead", state: catalog.MigrationQueued, quiet: 10 * time.Minute}}
	h := newDDLHarness(t, q)
	ctx := context.Background()
	conn := h.connect(t, h.dsn())
	_, err := conn.Exec(ctx, "create index concurrently orders_note_idx on orders (note)")
	var pe *pgconn.PgError
	if !errors.As(err, &pe) {
		t.Fatalf("want a PostgreSQL error, got %v", err)
	}
	if pe.Code != "55000" {
		t.Fatalf("SQLSTATE %s, want 55000: %s", pe.Code, pe.Message)
	}
	if !strings.Contains(pe.Message, "no pgshard controller") || !strings.Contains(pe.Hint, "controller is running") {
		t.Fatalf("message %q hint %q", pe.Message, pe.Hint)
	}
}

// TestAnIdenticalDDLThatHasAlreadyRunIsNotRunAgain (PGS-916): the retry of
// a statement whose migration finished while the client was away is told
// the work is done, not that it is waiting for it.
func TestAnIdenticalDDLThatHasAlreadyRunIsNotRunAgain(t *testing.T) {
	q := &fakeQueue{}
	q.attach = func(m catalog.DDLMigration) (catalog.EnqueueResult, bool) {
		q.queued = append(q.queued, catalog.DDLMigration{ID: "00000000-0000-0000-0000-00000000d0e1", Statement: m.Statement, DedupKey: m.DedupKey})
		return catalog.EnqueueResult{ID: "00000000-0000-0000-0000-00000000d0e1", Attached: true, State: catalog.MigrationComplete}, true
	}
	h := newDDLHarness(t, q)
	ctx := context.Background()
	cfg, err := pgx.ParseConfig(h.dsn())
	if err != nil {
		t.Fatal(err)
	}
	var notices []string
	cfg.OnNotice = func(_ *pgconn.PgConn, n *pgconn.Notice) { notices = append(notices, n.Message) }
	conn, err := pgx.ConnectConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close(ctx) }()
	if _, err := conn.Exec(ctx, "create index orders_note_idx on orders (note)"); err != nil {
		t.Fatal(err)
	}
	if len(notices) != 1 || !strings.Contains(notices[0], "identical migration 00000000-0000-0000-0000-00000000d0e1 has just completed; not running the statement again") {
		t.Fatalf("notices %q, want the client told the work is already done", notices)
	}
}

// TestADDLWaitWithNoApplierSaysTheControllerIsMissing (PGS-916): a queue
// nobody is applying is not a broken connection. The client is told the
// migration is still there and what to look at.
func TestADDLWaitWithNoApplierSaysTheControllerIsMissing(t *testing.T) {
	q := &fakeQueue{waitErr: errNoApplier{id: "00000000-0000-0000-0000-000000000001", state: catalog.MigrationQueued, quiet: 10 * time.Minute}}
	h := newDDLHarness(t, q)
	ctx := context.Background()
	conn := h.connect(t, h.dsn())
	_, err := conn.Exec(ctx, "create index orders_note_idx on orders (note)")
	var pe *pgconn.PgError
	if !errors.As(err, &pe) || pe.Code != "55000" {
		t.Fatalf("%v, want 55000: a quiet queue is not a connection failure", err)
	}
	if !strings.Contains(pe.Message, "no pgshard controller has been seen applying migrations") ||
		!strings.Contains(pe.Detail, q.last(t).ID+" continues in the background") ||
		!strings.Contains(pe.Hint, "controller is running and leading") {
		t.Fatalf("%+v, want the migration named and the controller pointed at", pe)
	}
}

// TestDescribeBlockersNamesWhatItIsAndWhetherItStarted (PGS-916): a client
// waiting behind an operation that has not started is told so. Saying "in
// progress" for a queued one describes a reshard that may never have begun
// as running.
func TestDescribeBlockersNamesWhatItIsAndWhetherItStarted(t *testing.T) {
	for _, c := range []struct {
		blocker catalog.Blocker
		want    string
	}{
		{catalog.Blocker{Kind: catalog.OperationDDL, ID: "a", Reason: catalog.BlockedByStarted}, "migration a (in progress)"},
		{catalog.Blocker{Kind: catalog.OperationDDL, ID: "a", Reason: catalog.BlockedByEarlier}, "migration a (queued before it)"},
		{catalog.Blocker{Kind: catalog.OperationReshard, ID: "b", Reason: catalog.BlockedByStarted}, "reshard b (in progress)"},
		{catalog.Blocker{Kind: catalog.OperationReshard, ID: "b", Reason: catalog.BlockedByEarlier}, "reshard b (queued before it)"},
		{catalog.Blocker{Kind: catalog.OperationUpgrade, ID: "c", Reason: catalog.BlockedByStarted}, "major upgrade c (in progress)"},
		{catalog.Blocker{Kind: catalog.OperationUpgrade, ID: "c", Reason: catalog.BlockedByEarlier}, "major upgrade c (queued before it)"},
		{catalog.Blocker{Kind: catalog.OperationPlacement, ID: "d", Reason: catalog.BlockedByStarted}, "table placement d (in progress)"},
		{catalog.Blocker{Kind: catalog.OperationPlacement, ID: "d", Reason: catalog.BlockedByEarlier}, "table placement d (queued before it)"},
		// A kind this router has not heard of is named, not dropped.
		{catalog.Blocker{Kind: "rekey", ID: "e", Reason: catalog.BlockedByStarted}, "rekey e (in progress)"},
	} {
		if got := describeBlockers([]catalog.Blocker{c.blocker}); got != c.want {
			t.Errorf("%+v = %q, want %q", c.blocker, got, c.want)
		}
	}
	if got, want := describeBlockers([]catalog.Blocker{
		{Kind: catalog.OperationReshard, ID: "b", Reason: catalog.BlockedByStarted},
		{Kind: catalog.OperationDDL, ID: "a", Reason: catalog.BlockedByEarlier},
	}), "reshard b (in progress), migration a (queued before it)"; got != want {
		t.Errorf("two blockers = %q, want %q", got, want)
	}
}

// TestTheBlockerNoticeIsSentOncePerChange (PGS-915): what a queued
// migration waits for is read on a throttle and reported when it changes.
// Reporting it on every poll is a notice every 200ms for as long as a
// reshard runs, and reporting it for a running migration says it is waiting
// when it is not.
func TestTheBlockerNoticeIsSentOncePerChange(t *testing.T) {
	var mu sync.Mutex
	state := catalog.MigrationRunning
	var set []catalog.Blocker
	asked, polls := 0, 0
	var got [][]catalog.Blocker
	q := &PGMigrationQueue{Poll: time.Millisecond, MaxWait: time.Hour, CatalogOutage: time.Hour, BlockersEvery: 200 * time.Millisecond,
		queue: func(context.Context) (bool, error) { return true, nil },
		load: func(context.Context, string) (catalog.DDLMigration, error) {
			mu.Lock()
			defer mu.Unlock()
			polls++
			return catalog.DDLMigration{ID: "m1", State: state}, nil
		},
		blockers: func(context.Context, string) ([]catalog.Blocker, error) {
			mu.Lock()
			defer mu.Unlock()
			asked++
			return append([]catalog.Blocker(nil), set...), nil
		},
		beat: func(context.Context) (time.Duration, bool, error) { return 0, true, nil },
	}
	read := func() (int, int, int) {
		mu.Lock()
		defer mu.Unlock()
		return polls, asked, len(got)
	}
	waitFor := func(what string, ok func(polls, asked, notices int) bool) {
		t.Helper()
		for deadline := time.Now().Add(10 * time.Second); time.Now().Before(deadline); time.Sleep(2 * time.Millisecond) {
			if ok(read()) {
				return
			}
		}
		p, a, n := read()
		t.Fatalf("%s: after %d polls, %d blocker reads and %d notices", what, p, a, n)
	}

	mu.Lock()
	set = []catalog.Blocker{{Kind: catalog.OperationReshard, ID: "e5a1", Reason: catalog.BlockedByStarted}}
	mu.Unlock()
	done := make(chan catalog.DDLMigration, 1)
	go func() {
		m, err := q.Wait(context.Background(), "m1", func(bs []catalog.Blocker) {
			mu.Lock()
			defer mu.Unlock()
			got = append(got, bs)
		})
		if err != nil {
			t.Error(err)
		}
		done <- m
	}()

	// A running migration is not waiting for anything, so the queue is not
	// even asked.
	waitFor("the wait never polled", func(p, _, _ int) bool { return p >= 50 })
	if _, a, n := read(); a != 0 || n != 0 {
		t.Fatalf("a running migration read the blockers %d times and sent %d notices", a, n)
	}

	mu.Lock()
	state = catalog.MigrationQueued
	mu.Unlock()
	waitFor("the queued migration was never told what holds it", func(_, _, n int) bool { return n >= 1 })
	mu.Lock()
	if len(got[0]) != 1 || got[0][0].ID != "e5a1" {
		mu.Unlock()
		t.Fatalf("first notice %+v", got[0])
	}
	mu.Unlock()

	// The same blockers, polled for another while: read on the throttle,
	// reported once.
	start, _, _ := read()
	waitFor("the wait stopped polling", func(p, _, _ int) bool { return p >= start+200 })
	p, a, n := read()
	if n != 1 {
		t.Fatalf("%d notices for one unchanged blocker set", n)
	}
	if a*2 >= p {
		t.Fatalf("the blockers were read %d times in %d polls; the throttle did nothing", a, p)
	}

	mu.Lock()
	set = []catalog.Blocker{{Kind: catalog.OperationUpgrade, ID: "u p 1", Reason: catalog.BlockedByEarlier}}
	mu.Unlock()
	waitFor("a new blocker set was not reported", func(_, _, n int) bool { return n >= 2 })
	mu.Lock()
	if len(got[1]) != 1 || got[1][0].ID != "u p 1" {
		mu.Unlock()
		t.Fatalf("second notice %+v", got[1])
	}
	mu.Unlock()

	mu.Lock()
	set = nil
	mu.Unlock()
	waitFor("an emptied blocker set was not reported", func(_, _, n int) bool { return n >= 3 })
	mu.Lock()
	if len(got[2]) != 0 {
		mu.Unlock()
		t.Fatalf("third notice %+v, want the empty set", got[2])
	}
	mu.Unlock()

	mu.Lock()
	state = catalog.MigrationComplete
	mu.Unlock()
	select {
	case m := <-done:
		if m.State != catalog.MigrationComplete {
			t.Fatalf("the wait ended on %q", m.State)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the wait did not end when the migration completed")
	}
	if _, _, n := read(); n != 3 {
		t.Fatalf("%d notices in all, want one per change", n)
	}
}

// TestARouterLimitOnAWaitingDDLKeepsTheMigrationsDetail (PGS-921): the
// router's own --max-query-duration ends the wait, not the session's
// statement_timeout. The client is still owed the migration id, because the
// migration it queued carries on without it.
func TestARouterLimitOnAWaitingDDLKeepsTheMigrationsDetail(t *testing.T) {
	q := &fakeQueue{delay: time.Hour}
	h := newDDLHarness(t, q, func(h *harness) { h.maxQueryDuration = 200 * time.Millisecond })
	ctx := context.Background()
	conn := h.connect(t, h.dsn())
	start := time.Now()
	_, err := conn.Exec(ctx, "create index orders_note_idx on orders (note)")
	var pe *pgconn.PgError
	if !errors.As(err, &pe) || pe.Code != "57014" || pe.Message != "canceling statement due to statement timeout" {
		t.Fatalf("%v, want 57014 from the router's own limit", err)
	}
	if !strings.Contains(pe.Detail, q.last(t).ID+" continues in the background") ||
		!strings.Contains(pe.Detail, "The router stops a statement after 200ms.") ||
		!strings.Contains(pe.Hint, "pgshard.migrations_public WHERE id = '"+q.last(t).ID+"'") {
		t.Fatalf("detail %q hint %q, want both which clock ran out and the migration that continues", pe.Detail, pe.Hint)
	}
	if took := time.Since(start); took > 5*time.Second {
		t.Fatalf("the wait ended after %s", took)
	}
}

// TestTheQueueProbeIsTakenOnceAndDoesNotBlockOtherSessions (PGS-920): one
// PGMigrationQueue serves every session of a router, and the probe for the
// operation queue ran under its mutex with a failure that cached nothing.
// Against a catalog that was reachable but slow, every 200ms poll of every
// waiting migration re-issued it and queued behind the last.
func TestTheQueueProbeIsTakenOnceAndDoesNotBlockOtherSessions(t *testing.T) {
	var mu sync.Mutex
	probes := 0
	release := make(chan struct{})
	var fail error
	q := &PGMigrationQueue{QueueProbeEvery: 50 * time.Millisecond,
		queue: func(ctx context.Context) (bool, error) {
			mu.Lock()
			probes++
			hold, err := release, fail
			mu.Unlock()
			if hold != nil {
				select {
				case <-hold:
				case <-ctx.Done():
					return false, ctx.Err()
				}
			}
			return err == nil, err
		}}
	count := func() int {
		mu.Lock()
		defer mu.Unlock()
		return probes
	}

	// One session is in the probe. Another, with a budget of its own, must
	// come back on that budget rather than on the first one's progress: a
	// sync.Mutex is not cancellable, so a session parked in it could not be
	// cut short by its own deadline or a client's cancel.
	first := make(chan error, 1)
	go func() {
		_, err := q.queued(context.Background())
		first <- err
	}()
	for count() == 0 {
		time.Sleep(time.Millisecond)
	}
	short, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	start := time.Now()
	if _, err := q.queued(short); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("a second session waiting on the probe ended with %v, want its own deadline", err)
	}
	if took := time.Since(start); took > 3*time.Second {
		t.Fatalf("the second session waited %s for the first session's probe", took)
	}
	// And it waited for the probe rather than starting one of its own.
	if n := count(); n != 1 {
		t.Fatalf("%d probes while one was in flight", n)
	}

	// Many sessions arriving together share the one probe.
	var wg sync.WaitGroup
	for range 20 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := q.queued(context.Background()); err != nil {
				t.Error(err)
			}
		}()
	}
	close(release)
	if err := <-first; err != nil {
		t.Fatal(err)
	}
	wg.Wait()
	if n := count(); n != 1 {
		t.Fatalf("%d probes for one answer; the sessions did not share it", n)
	}

	// A catalog that has the queue is never asked again.
	time.Sleep(80 * time.Millisecond)
	if ok, err := q.queued(context.Background()); !ok || err != nil {
		t.Fatalf("queued = %v, %v", ok, err)
	}
	if n := count(); n != 1 {
		t.Fatalf("%d probes; the queue's presence is permanent", n)
	}

	// A probe that fails is remembered too, so a slow catalog is not
	// re-probed by every session on every poll -- but for a window of its
	// own, far shorter than the one an answer earns, so a catalog that has
	// come back does not wait the answer's window out. This queue trusts an
	// answer for ten seconds; the failure must be re-read long before that.
	var fmu sync.Mutex
	fprobes := 0
	var ferr error
	ferr = errors.New("catalog restarting")
	fq := &PGMigrationQueue{QueueProbeEvery: 10 * time.Second,
		queue: func(context.Context) (bool, error) {
			fmu.Lock()
			defer fmu.Unlock()
			fprobes++
			return ferr == nil, ferr
		}}
	fcount := func() int {
		fmu.Lock()
		defer fmu.Unlock()
		return fprobes
	}
	for range 5 {
		if _, err := fq.queued(context.Background()); err == nil {
			t.Fatal("a failing probe reported success")
		}
	}
	if n := fcount(); n != 1 {
		t.Fatalf("%d probes for one failure; it was not remembered", n)
	}
	time.Sleep(queueProbeErrorEvery + 100*time.Millisecond)
	fmu.Lock()
	ferr = nil
	fmu.Unlock()
	if ok, err := fq.queued(context.Background()); !ok || err != nil {
		t.Fatalf("a catalog that came back answered %v, %v; the failure was kept for the whole answer window", ok, err)
	}
}
