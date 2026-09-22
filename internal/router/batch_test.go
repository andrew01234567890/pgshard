package router

import (
	"context"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// A batch runs in a transaction the client never opened, so the shard sees
// a BEGIN and a COMMIT around statements it was sent one at a time before.
func TestABatchIsOneTransactionOnTheShard(t *testing.T) {
	h := newHarness(t)
	conn := h.connect(t, h.dsn("app", "secret", "app"))
	ctx := context.Background()
	if _, err := conn.Exec(ctx, "select 1; select 1", pgx.QueryExecModeSimpleProtocol); err != nil {
		t.Fatal(err)
	}
	h.fp.mu.Lock()
	got := strings.Join(h.fp.executed, "|")
	h.fp.mu.Unlock()
	if got != "begin|select 1|select 1|commit" {
		t.Fatalf("shard ran %q, want the batch wrapped in one transaction", got)
	}
}

func (h *harness) shardRan() string {
	h.fp.mu.Lock()
	defer h.fp.mu.Unlock()
	got := strings.Join(h.fp.executed, "|")
	h.fp.executed = nil
	return got
}

func batchConn(t *testing.T, h *harness) (*pgx.Conn, *[]*pgconn.Notice) {
	t.Helper()
	cfg, err := pgx.ParseConfig(h.dsn("app", "secret", "app"))
	if err != nil {
		t.Fatal(err)
	}
	var notices []*pgconn.Notice
	cfg.OnNotice = func(_ *pgconn.PgConn, n *pgconn.Notice) { notices = append(notices, n) }
	conn, err := pgx.ConnectConfig(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close(context.Background()) })
	return conn, &notices
}

// A COMMIT inside a batch commits what ran before it, with PostgreSQL's
// warning that the client had no transaction open, and the statements after
// it run in a transaction of their own.
func TestACommitInsideABatchEndsItsTransactionAndOpensANewOne(t *testing.T) {
	h := newHarness(t)
	conn, notices := batchConn(t, h)
	ctx := context.Background()
	h.shardRan()
	if _, err := conn.Exec(ctx, "select 1; commit; select 1", pgx.QueryExecModeSimpleProtocol); err != nil {
		t.Fatal(err)
	}
	if got := h.shardRan(); got != "begin|select 1|commit|begin|select 1|commit" {
		t.Fatalf("shard ran %q, want two transactions split at the COMMIT", got)
	}
	if len(*notices) != 1 || (*notices)[0].Code != "25P01" || (*notices)[0].Severity != "WARNING" {
		t.Fatalf("notices = %+v, want one 25P01 WARNING", *notices)
	}
	if st := conn.PgConn().TxStatus(); st != 'I' {
		t.Fatalf("status after the batch = %c, want I", st)
	}
}

func TestARollbackInsideABatchUndoesWhatRanBeforeIt(t *testing.T) {
	h := newHarness(t)
	conn, notices := batchConn(t, h)
	ctx := context.Background()
	h.shardRan()
	if _, err := conn.Exec(ctx, "select 1; rollback; select 1", pgx.QueryExecModeSimpleProtocol); err != nil {
		t.Fatal(err)
	}
	if got := h.shardRan(); got != "begin|select 1|rollback|begin|select 1|commit" {
		t.Fatalf("shard ran %q, want the first transaction rolled back", got)
	}
	if len(*notices) != 1 || (*notices)[0].Code != "25P01" {
		t.Fatalf("notices = %+v, want one 25P01 WARNING", *notices)
	}
}

// A BEGIN inside a batch takes the batch's transaction over: nothing is
// committed when the batch ends, and the client's own COMMIT later commits
// the statements from before its BEGIN too.
func TestABeginInsideABatchAdoptsItsTransaction(t *testing.T) {
	h := newHarness(t)
	conn, notices := batchConn(t, h)
	ctx := context.Background()
	h.shardRan()
	if _, err := conn.Exec(ctx, "select 1; begin; select 1", pgx.QueryExecModeSimpleProtocol); err != nil {
		t.Fatal(err)
	}
	if got := h.shardRan(); got != "begin|select 1|select 1" {
		t.Fatalf("shard ran %q, want the transaction left open", got)
	}
	if st := conn.PgConn().TxStatus(); st != 'T' {
		t.Fatalf("status after the batch = %c, want T", st)
	}
	if _, err := conn.Exec(ctx, "commit", pgx.QueryExecModeSimpleProtocol); err != nil {
		t.Fatal(err)
	}
	if got := h.shardRan(); got != "commit" {
		t.Fatalf("shard ran %q at the client's COMMIT", got)
	}
	if len(*notices) != 0 {
		t.Fatalf("notices = %+v, want none: PostgreSQL warns about neither statement", *notices)
	}
}

func TestABatchWrappedInBeginAndCommitIsOneTransaction(t *testing.T) {
	h := newHarness(t)
	conn, notices := batchConn(t, h)
	ctx := context.Background()
	h.shardRan()
	if _, err := conn.Exec(ctx, "begin; select 1; select 1; commit", pgx.QueryExecModeSimpleProtocol); err != nil {
		t.Fatal(err)
	}
	if got := h.shardRan(); got != "begin|select 1|select 1|commit" {
		t.Fatalf("shard ran %q, want one transaction", got)
	}
	if len(*notices) != 0 || conn.PgConn().TxStatus() != 'I' {
		t.Fatalf("notices = %+v, status %c", *notices, conn.PgConn().TxStatus())
	}
}

// An error after the adopting BEGIN leaves the client's transaction failed,
// not rolled back: it is the client's to end.
func TestAnErrorAfterAnAdoptingBeginLeavesTheTransactionFailed(t *testing.T) {
	h := newHarness(t)
	conn, _ := batchConn(t, h)
	ctx := context.Background()
	h.shardRan()
	if _, err := conn.Exec(ctx, "begin; select 1; select bad", pgx.QueryExecModeSimpleProtocol); err == nil {
		t.Fatal("the failing statement succeeded")
	}
	if got := h.shardRan(); got != "begin|select 1|select bad" {
		t.Fatalf("shard ran %q, want nothing ended", got)
	}
	if st := conn.PgConn().TxStatus(); st != 'E' {
		t.Fatalf("status after the batch = %c, want E", st)
	}
}

func TestABeginWithModesFirstInABatchIsTheTransaction(t *testing.T) {
	h := newHarness(t)
	conn, _ := batchConn(t, h)
	ctx := context.Background()
	h.shardRan()
	if _, err := conn.Exec(ctx, "begin isolation level serializable; select 1; commit", pgx.QueryExecModeSimpleProtocol); err != nil {
		t.Fatal(err)
	}
	if got := h.shardRan(); got != "begin|rollback|begin isolation level serializable|select 1|commit" {
		t.Fatalf("shard ran %q, want the batch's empty transaction replaced by the client's", got)
	}
}

func TestTransactionControlInsideABatchThatPostgreSQLRefusesIsRefused(t *testing.T) {
	h := newHarness(t)
	conn, _ := batchConn(t, h)
	ctx := context.Background()
	for sql, code := range map[string]string{
		"select 1; savepoint s":             "25P01",
		"select 1; release savepoint s":     "25P01",
		"select 1; rollback to savepoint s": "25P01",
		"select 1; commit and chain":        "25P01",
	} {
		_, err := conn.Exec(ctx, sql, pgx.QueryExecModeSimpleProtocol)
		if sqlstate(err) != code {
			t.Fatalf("%q: err = %v, want %s", sql, err, code)
		}
		if st := conn.PgConn().TxStatus(); st != 'I' {
			t.Fatalf("%q: status = %c, want the batch rolled back", sql, st)
		}
	}
}

// The client's own transaction control still works: what is refused is a
// control statement inside a batch this router wrapped, not one the client
// sent on its own.
func TestTransactionControlOutsideABatchStillWorks(t *testing.T) {
	h := newHarness(t)
	conn := h.connect(t, h.dsn("app", "secret", "app"))
	ctx := context.Background()
	for _, sql := range []string{"begin", "select 1", "commit"} {
		if _, err := conn.Exec(ctx, sql, pgx.QueryExecModeSimpleProtocol); err != nil {
			t.Fatalf("%q: %v", sql, err)
		}
	}
}

// The transaction a batch runs in is the router's, so a client that sent no
// BEGIN must not be told to run its DDL outside one. What it sent is a
// batch, and that is what the refusal has to name.
func TestDDLInsideABatchIsRefusedInTermsOfTheBatch(t *testing.T) {
	h := newHarness(t)
	conn := h.connect(t, h.dsn("app", "secret", "app"))
	ctx := context.Background()
	_, err := conn.Exec(ctx, "select 1; create table t (a int)", pgx.QueryExecModeSimpleProtocol)
	if sqlstate(err) != "0A000" {
		t.Fatalf("err = %v, want 0A000", err)
	}
	if !strings.Contains(err.Error(), "multi-statement simple query") {
		t.Fatalf("err = %v, want the batch named", err)
	}
	if strings.Contains(err.Error(), "BEGIN/COMMIT") {
		t.Fatalf("err = %v, blames a transaction block the client never opened", err)
	}
}

// After other statements PostgreSQL adopts the transaction and applies the
// modes as SET TRANSACTION would, so the shard decides what may still
// change: READ ONLY may, an isolation level after a query may not (25001).
func TestABeginWithModesAfterOtherStatementsSetsThemOnTheTransaction(t *testing.T) {
	h := newHarness(t)
	conn, _ := batchConn(t, h)
	ctx := context.Background()
	h.shardRan()
	if _, err := conn.Exec(ctx, "select 1; begin read only; select 1", pgx.QueryExecModeSimpleProtocol); err != nil {
		t.Fatal(err)
	}
	if got := h.shardRan(); got != "begin|select 1|set transaction read only|select 1" {
		t.Fatalf("shard ran %q, want the modes set on the adopted transaction", got)
	}
	if st := conn.PgConn().TxStatus(); st != 'T' {
		t.Fatalf("status %c, want the adopted transaction open", st)
	}
}
