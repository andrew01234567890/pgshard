package router

import (
	"context"
	"testing"
	"time"

	"github.com/andrew01234567890/pgshard/internal/pgwire"
)

// TestARecoveredTransactionTakesItsWritePauseVerdictFromItsNewBegin
// (PGS-959 item 3): a transaction that began before the write pause may
// still write, because PostgreSQL read default_transaction_read_only at its
// BEGIN. One recovered by ROLLBACK TO after a failover killed its backend
// is re-opened by a replayed BEGIN on a fresh backend -- under the pause --
// but kept the verdict of the BEGIN that died. Its first write went
// straight to a backend whose transaction is read-only, where PostgreSQL
// answers a raw 25006, instead of waiting at the fence like any other.
func TestARecoveredTransactionTakesItsWritePauseVerdictFromItsNewBegin(t *testing.T) {
	h := newDDLHarness(t, &fakeQueue{})
	ctx := context.Background()
	conn := h.connect(t, h.dsn())
	for _, sql := range []string{"begin", "savepoint sp"} {
		if _, err := conn.Exec(ctx, sql); err != nil {
			t.Fatalf("%s: %v", sql, err)
		}
	}
	serving := h.fence("fenced")
	if _, err := conn.Exec(ctx, "insert into items (id) values (9591)"); sqlstate(err) != "40001" {
		t.Fatalf("the killed statement reported %v, want 40001", err)
	}
	paused := *serving
	paused.WriteFence = true
	h.setSnap(&paused)
	if _, err := conn.Exec(ctx, "rollback to savepoint sp"); err != nil {
		t.Fatalf("recovering the untouched transaction: %v", err)
	}

	done := make(chan error, 1)
	go func() {
		_, err := conn.Exec(ctx, "insert into items (id) values (9592)")
		done <- err
	}()
	select {
	case err := <-done:
		t.Fatalf("the recovered transaction's write was not held by the pause (err %v): its BEGIN was replayed after the pause went up, so it cannot write", err)
	case <-time.After(300 * time.Millisecond):
	}
	if n := h.r.FenceWaiting(); n != 1 {
		t.Fatalf("fence waiting = %d, want 1", n)
	}
	h.fenced(false)
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("the held write after the pause lifted: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the held write never completed after the pause lifted")
	}
}

// TestAPanicForgetsTheTransactionsSavepoints (PGS-959 item 4): the panic
// handler cleared everything recorded about the open transaction except its
// savepoints, so a later transaction could find a name set in an earlier one
// and be recovered to a savepoint that does not exist.
func TestAPanicForgetsTheTransactionsSavepoints(t *testing.T) {
	h := newShardedHarness(t)
	e := newExecutor(h.r, pgwire.SessionInfo{ID: 1, Database: "app", User: "app", Auth: &pgwire.AuthResult{SCRAM: &pgwire.SCRAMKeys{}}}, Shard{Set: DefaultShardSet, ID: 0})
	e.savepoints = []savepointMark{{name: "sp"}}
	e.txnPrelude = []string{"savepoint sp"}
	_ = e.guard("SimpleQuery", func() error { panic("boom") })
	if e.savepointIndex("sp") >= 0 {
		t.Fatal("a savepoint of the transaction the panic reset is still recorded")
	}
}

// TestAFailedBatchRecordsTheStatementsTheClientSawSucceed (PGS-959 item 1):
// an extended batch recorded its statements' effect on the transaction only
// when the whole batch succeeded, while the pump had already relayed each
// statement's CommandComplete as it came. A batch whose last statement
// failed left no record of the SAVEPOINT and the write before it, although
// the client had watched both succeed -- so the transaction read as
// untouched, with no savepoint.
func TestAFailedBatchRecordsTheStatementsTheClientSawSucceed(t *testing.T) {
	h := newShardedHarness(t)
	keys := &pgwire.SCRAMKeys{ClientKey: make([]byte, 32), ServerKey: make([]byte, 32)}
	e := newExecutor(h.r, pgwire.SessionInfo{ID: 1, Database: "app", User: "app", Auth: &pgwire.AuthResult{SCRAM: keys}}, Shard{Set: DefaultShardSet, ID: 0})
	ctx := context.Background()
	w := discardWriter{}
	if err := e.SimpleQuery(ctx, "begin", w); err != nil {
		t.Fatal(err)
	}
	for i, sql := range []string{"savepoint sp", "insert into items (id) values (9593)", "select bad"} {
		name := string(rune('a' + i))
		if err := e.Parse(ctx, name, sql, nil, w); err != nil {
			t.Fatalf("parse %q: %v", sql, err)
		}
		if err := e.Bind(ctx, name, name, nil, nil, nil, w); err != nil {
			t.Fatalf("bind %q: %v", sql, err)
		}
		if err := e.Execute(ctx, name, 0, w); err != nil {
			t.Fatalf("execute %q: %v", sql, err)
		}
	}
	if err := e.Sync(ctx); err == nil {
		t.Fatal("the batch's last statement must fail, or this test is not about a failed batch")
	}
	if e.savepointIndex("sp") < 0 {
		t.Error("the SAVEPOINT the client saw succeed is not recorded")
	}
	if !e.txnTouched {
		t.Error("the write the client saw succeed did not mark the transaction as touching its shard")
	}
}
