package router

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/andrew01234567890/pgshard/internal/pgwire"
	"github.com/andrew01234567890/pgshard/internal/router/plan"
)

// TestASessionPinnedToARetiringSetIsToldWhy (PGS-927): after a reshard
// switches, a session that is pinned -- an open transaction, a prepared
// statement on a reserved backend -- still holds a pooler connection to a
// primary in the old set. When the set retires, that connection goes. The
// client is owed an error it can act on, in a session it can still end,
// rather than its connection dropping under it.
//
// retireOldGroupsAfter: 0 makes this the normal case rather than a rare
// accident: every session still pinned to the old set meets it at the
// switch.
func TestASessionPinnedToARetiringSetIsToldWhy(t *testing.T) {
	h := newShardedHarnessWith(t, Config{})
	ctx := context.Background()
	var mu sync.Mutex
	var warnings []string
	cfg, err := pgx.ParseConfig(h.dsn())
	if err != nil {
		t.Fatal(err)
	}
	cfg.OnNotice = func(_ *pgconn.PgConn, n *pgconn.Notice) {
		mu.Lock()
		defer mu.Unlock()
		warnings = append(warnings, n.Message)
	}
	conn, err := pgx.ConnectConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close(ctx) }()
	tenant, _ := h.twoTenants(t)
	shard := h.shardOf(t, tenant)

	if _, err := conn.Exec(ctx, "begin"); err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Exec(ctx, "insert into orders (tenant_id, id) values ($1, 1)", tenant); err != nil {
		t.Fatalf("the write that pins the session: %v", err)
	}

	// The old set goes while the session still holds it.
	h.poolers[shard].gone.Store(true)

	_, err = conn.Exec(ctx, "insert into orders (tenant_id, id) values ($1, 2)", tenant)
	if err == nil {
		t.Fatal("a write to a retired shard was accepted")
	}
	var pe *pgconn.PgError
	if !errors.As(err, &pe) {
		t.Fatalf("the client saw %v, not an error it can read", err)
	}
	t.Logf("client got SQLSTATE %s: %s (detail %q hint %q)", pe.Code, pe.Message, pe.Detail, pe.Hint)
	if pe.Code == "" || strings.Contains(strings.ToLower(pe.Message), "conn closed") {
		t.Fatalf("the connection dropped rather than answering: %+v", pe)
	}

	// And the session is still there to end. The shard that would have been
	// asked is the one that has just gone, so the answer has to come from
	// the router: PostgreSQL's own warning and the statement's own tag.
	tag, err := conn.Exec(ctx, "rollback")
	if err != nil {
		t.Fatalf("the session could not be ended after the shard went: %v", err)
	}
	if tag.String() != "ROLLBACK" {
		t.Errorf("rollback answered %q", tag)
	}
	mu.Lock()
	got := append([]string(nil), warnings...)
	mu.Unlock()
	if len(got) != 1 || got[0] != "there is no transaction in progress" {
		t.Errorf("warnings %q, want PostgreSQL's own words for ending nothing", got)
	}
}

// TestCommitWithNoTransactionIsAnsweredByTheRouter (PGS-927): the same
// answer when nothing has gone wrong at all. It used to cost a round trip
// to a shard to be told what the session already knew.
func TestCommitWithNoTransactionIsAnsweredByTheRouter(t *testing.T) {
	h := newShardedHarnessWith(t, Config{})
	ctx := context.Background()
	var mu sync.Mutex
	var warnings []string
	cfg, err := pgx.ParseConfig(h.dsn())
	if err != nil {
		t.Fatal(err)
	}
	cfg.OnNotice = func(_ *pgconn.PgConn, n *pgconn.Notice) {
		mu.Lock()
		defer mu.Unlock()
		warnings = append(warnings, n.Message)
	}
	conn, err := pgx.ConnectConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close(ctx) }()
	for _, c := range []struct{ sql, tag string }{{"commit", "COMMIT"}, {"rollback", "ROLLBACK"}} {
		tag, err := conn.Exec(ctx, c.sql)
		if err != nil {
			t.Fatalf("%s outside a transaction: %v", c.sql, err)
		}
		if tag.String() != c.tag {
			t.Errorf("%s answered %q, want %q", c.sql, tag, c.tag)
		}
	}
	mu.Lock()
	defer mu.Unlock()
	if len(warnings) != 2 {
		t.Fatalf("warnings %q, want one for each", warnings)
	}
	for _, w := range warnings {
		if w != "there is no transaction in progress" {
			t.Errorf("warning %q", w)
		}
	}
	// Nothing was asked of any shard.
	for i := range h.poolers {
		for _, sql := range h.poolers[i].ran() {
			if s := strings.ToLower(sql); strings.Contains(s, "commit") || strings.Contains(s, "rollback") {
				t.Errorf("shard %d was asked to end a transaction that was never open: %q", i, sql)
			}
		}
	}
	// A transaction that is open but has not touched a shard yet is still a
	// transaction: ending it is not ending nothing, and it must not be
	// warned about.
	warnings = nil
	mu.Unlock()
	for _, sql := range []string{"begin", "commit"} {
		if _, err := conn.Exec(ctx, sql); err != nil {
			t.Fatalf("%s: %v", sql, err)
		}
	}
	mu.Lock()
	if len(warnings) != 0 {
		t.Errorf("ending an open transaction warned %q", warnings)
	}
}

// TestEndingNothingNeedsThereToBeNothing (PGS-927): the guard that keeps the
// router's local answer to the state it is an answer for.
//
// Nothing reaches endNoTxn today with a transaction open and no backend --
// a BEGIN takes one -- so the guard is asserted where it lives rather than
// through a session. Removing it would answer COMMIT to a client whose
// transaction was never committed anywhere, which is the worst answer this
// router can give.
func TestEndingNothingNeedsThereToBeNothing(t *testing.T) {
	for _, tx := range []pgwire.TxStatus{pgwire.TxInBlock, pgwire.TxFailed} {
		e := &Executor{tx: tx}
		if handled, err := e.endNoTxn(StmtClass{Txn: plan.TxnCommit}, discardWriter{}); handled || err != nil {
			t.Errorf("with a transaction %q open, COMMIT was answered without one: handled=%v err=%v", tx, handled, err)
		}
	}
	e := &Executor{tx: pgwire.TxIdle}
	if handled, err := e.endNoTxn(StmtClass{Txn: plan.TxnCommit}, discardWriter{}); !handled || err != nil {
		t.Errorf("with nothing open, COMMIT was not answered: handled=%v err=%v", handled, err)
	}
	// A statement that is not an end is never this function's business.
	if handled, _ := e.endNoTxn(StmtClass{Txn: plan.TxnBegin}, discardWriter{}); handled {
		t.Error("BEGIN was answered as the end of a transaction")
	}
}
