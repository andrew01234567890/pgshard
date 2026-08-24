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

	"github.com/andrew01234567890/pgshard/internal/catalog"
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
	_ = expectRefusal(t, err, "rewrite-class DDL is not available yet")
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
