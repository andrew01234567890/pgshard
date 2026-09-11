package router

import (
	"context"
	"strconv"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgproto3"

	"github.com/andrew01234567890/pgshard/internal/pgwire"
)

// TestABeginAndAWriteInOneBatchRecordTheWrite.
//
// noteWrite returned early while e.tx was Idle, and a batch carrying BEGIN
// and a write in ONE Sync is classified before the BEGIN has run -- so the
// write went unrecorded. The consequences were all downstream of that:
// with transaction_mode=single a second writable shard was admitted and
// refused only at COMMIT, checkPreparedCapacity ran at COMMIT instead of
// when the write was admitted, and txnWrote() reported false for a
// transaction that had written. Only the hidden-writer probe kept the
// outcome correct, and a backstop is not where this belongs.
func TestABeginAndAWriteInOneBatchRecordTheWrite(t *testing.T) {
	h := newShardedHarness(t)
	ctx := context.Background()

	// Outside a transaction, a write records nothing: that is the guard
	// this fix must not remove.
	e := newExecutor(h.r, pgwire.SessionInfo{ID: 1, Database: "app", User: "app",
		Auth: &pgwire.AuthResult{SCRAM: &pgwire.SCRAMKeys{}}}, Shard{Set: DefaultShardSet, ID: 0})
	if err := e.noteWriteIn(ctx, false); err != nil {
		t.Fatal(err)
	}
	if e.wroteHere {
		t.Fatal("a write outside a transaction was recorded as one inside it")
	}

	// A BEGIN in the same batch opens the transaction the write belongs to,
	// even though e.tx still says Idle when the batch is classified.
	if err := e.noteWriteIn(ctx, true); err != nil {
		t.Fatal(err)
	}
	if !e.wroteHere {
		t.Fatal("a write that follows a BEGIN in the same Sync was not recorded; the transaction reports it never wrote")
	}
	if e.tx != pgwire.TxIdle {
		t.Fatalf("the premise of this test is that e.tx has not moved yet: %v", e.tx)
	}
}

// TestABatchOpeningATransactionRecordsItsWriteAtOnce drives the real batch
// path, which the helper test above cannot: it is the batch walk that
// decides whether a transaction is open, and a mutation of that walk left
// the helper test passing.
//
// In single mode the second writable shard must be refused WHERE IT IS
// WRITTEN. Without the write recorded, it was admitted and refused only at
// COMMIT -- and checkPreparedCapacity ran there too, long after the point
// it was meant to guard.
func TestABatchOpeningATransactionRecordsItsWriteAtOnce(t *testing.T) {
	h := newTxnHarness(t)
	ctx := context.Background()
	a, b := h.twoTenants(t)
	conn := h.connect(t, h.dsn())
	if _, err := conn.Exec(ctx, "set pgshard.transaction_mode = single"); err != nil {
		t.Fatal(err)
	}
	hj, err := conn.PgConn().Hijack()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = hj.Conn.Close() })
	fe := hj.Frontend
	send := func(msgs ...pgproto3.FrontendMessage) {
		t.Helper()
		for _, m := range msgs {
			fe.Send(m)
		}
		if err := fe.Flush(); err != nil {
			t.Fatal(err)
		}
	}
	drain := func() *pgproto3.ErrorResponse {
		t.Helper()
		var first *pgproto3.ErrorResponse
		for {
			msg, err := fe.Receive()
			if err != nil {
				t.Fatal(err)
			}
			switch m := msg.(type) {
			case *pgproto3.ErrorResponse:
				if first == nil {
					first = m
				}
			case *pgproto3.ReadyForQuery:
				return first
			}
		}
	}

	// BEGIN and the first write in ONE Sync, through the EXTENDED protocol:
	// the batch is classified before the BEGIN has run, which is the case
	// that went unrecorded. (A multi-statement SIMPLE query carrying BEGIN
	// is refused outright, so this shape is the only way to reach it.)
	send(
		&pgproto3.Parse{Query: "begin"}, &pgproto3.Bind{}, &pgproto3.Execute{},
		&pgproto3.Parse{Query: "insert into orders (tenant_id, id) values (" + itoa64(a) + ", 1)"},
		&pgproto3.Bind{}, &pgproto3.Execute{},
		&pgproto3.Sync{})
	if e := drain(); e != nil {
		t.Fatalf("begin and write in one batch: %s %s", e.Code, e.Message)
	}

	// The second writable shard must be refused HERE.
	send(
		&pgproto3.Parse{Query: "insert into orders (tenant_id, id) values (" + itoa64(b) + ", 2)"},
		&pgproto3.Bind{}, &pgproto3.Execute{}, &pgproto3.Sync{})
	e := drain()
	if e == nil {
		t.Fatal("single mode admitted a second writable shard: the first write was never recorded against the transaction")
	}
	if !strings.Contains(e.Message, "transaction already writes to shard") {
		t.Fatalf("refused for another reason: %s %s", e.Code, e.Message)
	}
	send(&pgproto3.Parse{Query: "rollback"}, &pgproto3.Bind{}, &pgproto3.Execute{}, &pgproto3.Sync{})
	_ = drain()
}

func itoa64(v int64) string { return strconv.FormatInt(v, 10) }
