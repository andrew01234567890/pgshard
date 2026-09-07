package pgwire

import (
	"context"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgproto3"
)

// A batch used to be refused outright, which is what psql -f, a dump and
// every migration tool send. PostgreSQL runs one, in a transaction the
// client never opened, and stops at the first error with all of it undone.
func TestASimpleQueryBatchRunsInOneImplicitTransaction(t *testing.T) {
	ts := startServer(t, Config{})
	var seen []string
	ts.newExec = func(SessionInfo) (Executor, error) {
		e := NewFakeExecutor()
		e.Delay = func(context.Context) error { return nil }
		return &recordingExecutor{Executor: e, seen: &seen}, nil
	}
	c := dialRaw(t, ts.addr)
	c.startup(ProtocolVersion30)
	c.send(&pgproto3.Query{String: "select 1; select 1"})

	// One result per statement, then a single ReadyForQuery for the batch.
	for i := 0; i < 2; i++ {
		if _, ok := c.recv().(*pgproto3.RowDescription); !ok {
			t.Fatalf("statement %d: want its own row description", i)
		}
		c.recv() // DataRow
		if _, ok := c.recv().(*pgproto3.CommandComplete); !ok {
			t.Fatalf("statement %d: want its own command complete", i)
		}
	}
	if _, ok := c.recv().(*pgproto3.ReadyForQuery); !ok {
		t.Fatal("a batch sends one ReadyForQuery, after all of it")
	}
	if got := strings.Join(seen, "|"); got != "BEGIN|select 1|select 1|COMMIT" {
		t.Fatalf("executed %q, want the statements wrapped in one transaction", got)
	}
}

// The whole point of the implicit transaction: a batch that fails halfway
// must not leave its first half applied.
func TestAFailedBatchRollsBackAndRunsNothingAfterTheError(t *testing.T) {
	ts := startServer(t, Config{})
	var seen []string
	ts.newExec = func(SessionInfo) (Executor, error) {
		return &recordingExecutor{Executor: NewFakeExecutor(), seen: &seen}, nil
	}
	c := dialRaw(t, ts.addr)
	c.startup(ProtocolVersion30)
	// The fake executor answers "select 1" and refuses everything else.
	c.send(&pgproto3.Query{String: "select 1; select 2; select 1"})

	if _, ok := c.recv().(*pgproto3.RowDescription); !ok {
		t.Fatal("the first statement runs")
	}
	c.recv()
	c.recv()
	if er, ok := c.recv().(*pgproto3.ErrorResponse); !ok {
		t.Fatalf("want the second statement's error, got %+v", er)
	}
	if _, ok := c.recv().(*pgproto3.ReadyForQuery); !ok {
		t.Fatal("want one ReadyForQuery after the error")
	}
	if got := strings.Join(seen, "|"); got != "BEGIN|select 1|select 2|ROLLBACK" {
		t.Fatalf("executed %q, want the batch rolled back and the third statement never reached", got)
	}
}

// A client that opened its own transaction already has atomicity, and the
// COMMIT is its to send: opening a second one here would be wrong and
// committing it at the end of the batch would end the client's.
func TestABatchInsideTheClientsOwnTransactionOpensNothing(t *testing.T) {
	ts := startServer(t, Config{})
	var seen []string
	ts.newExec = func(SessionInfo) (Executor, error) {
		return &recordingExecutor{Executor: NewFakeExecutor(), seen: &seen}, nil
	}
	c := dialRaw(t, ts.addr)
	c.startup(ProtocolVersion30)
	c.send(&pgproto3.Query{String: "begin"})
	for {
		if _, ok := c.recv().(*pgproto3.ReadyForQuery); ok {
			break
		}
	}
	seen = nil
	c.send(&pgproto3.Query{String: "select 1; select 1"})
	for {
		if _, ok := c.recv().(*pgproto3.ReadyForQuery); ok {
			break
		}
	}
	if got := strings.Join(seen, "|"); got != "select 1|select 1" {
		t.Fatalf("executed %q, want no transaction of our own around a batch the client already wrapped", got)
	}
}

// recordingExecutor notes the SQL each statement runs as, including the
// implicit transaction's own, which no client ever sees.
type recordingExecutor struct {
	Executor
	seen *[]string
}

func (r *recordingExecutor) SimpleQuery(ctx context.Context, sql string, w ResultWriter) error {
	*r.seen = append(*r.seen, sql)
	return r.Executor.SimpleQuery(ctx, sql, w)
}

func (r *recordingExecutor) BeginImplicit(ctx context.Context) error {
	*r.seen = append(*r.seen, "BEGIN")
	return r.Executor.BeginImplicit(ctx)
}

func (r *recordingExecutor) EndImplicit(ctx context.Context, commit bool) error {
	if commit {
		*r.seen = append(*r.seen, "COMMIT")
	} else {
		*r.seen = append(*r.seen, "ROLLBACK")
	}
	return r.Executor.EndImplicit(ctx, commit)
}

// PostgreSQL ends the implicit transaction before it reports the last
// statement, and says why in its own source: a client expects either a
// command completion or an error, not one and then the other. A commit can
// still fail -- a serialization failure, a deferred constraint, a prepare
// that no shard accepted -- and a client walking results one at a time would
// otherwise have been told the batch finished before hearing it did not.
func TestACommitThatFailsReplacesTheLastStatementsCompletion(t *testing.T) {
	ts := startServer(t, Config{})
	ts.newExec = func(SessionInfo) (Executor, error) {
		return &failingCommitExecutor{Executor: NewFakeExecutor()}, nil
	}
	c := dialRaw(t, ts.addr)
	c.startup(ProtocolVersion30)
	c.send(&pgproto3.Query{String: "select 1; select 1"})

	// The first statement is finished and cannot be taken back.
	if _, ok := c.recv().(*pgproto3.RowDescription); !ok {
		t.Fatal("first statement: row description")
	}
	c.recv()
	if _, ok := c.recv().(*pgproto3.CommandComplete); !ok {
		t.Fatal("first statement: command complete")
	}
	// The last statement's rows are already out; only its completion waits.
	if _, ok := c.recv().(*pgproto3.RowDescription); !ok {
		t.Fatal("last statement: row description")
	}
	c.recv()
	switch m := c.recv().(type) {
	case *pgproto3.ErrorResponse:
		if !strings.Contains(m.Message, "commit refused") {
			t.Fatalf("want the commit's own error, got %+v", m)
		}
	case *pgproto3.CommandComplete:
		t.Fatal("the last statement was reported complete before the commit that failed")
	default:
		t.Fatalf("got %T", m)
	}
	if _, ok := c.recv().(*pgproto3.ReadyForQuery); !ok {
		t.Fatal("want ReadyForQuery after the failed commit")
	}
}

type failingCommitExecutor struct {
	Executor
}

func (f *failingCommitExecutor) EndImplicit(ctx context.Context, commit bool) error {
	if commit {
		return Errorf("40001", "commit refused")
	}
	return f.Executor.EndImplicit(ctx, commit)
}

// A leading comment is part of what the client sent, and a shard reads some
// of them: a planner hint is a comment by design. Splitting a single
// statement out of its own text used to drop everything before its first
// token.
func TestASingleStatementKeepsWhatPrecedesIt(t *testing.T) {
	ts := startServer(t, Config{})
	var seen []string
	ts.newExec = func(SessionInfo) (Executor, error) {
		return &recordingExecutor{Executor: NewFakeExecutor(), seen: &seen}, nil
	}
	c := dialRaw(t, ts.addr)
	c.startup(ProtocolVersion30)
	c.send(&pgproto3.Query{String: "/*+ SeqScan(t) */ select 1"})
	for {
		if _, ok := c.recv().(*pgproto3.ReadyForQuery); ok {
			break
		}
	}
	if len(seen) != 1 || seen[0] != "/*+ SeqScan(t) */ select 1" {
		t.Fatalf("executor saw %q, want the text the client sent", seen)
	}
}
