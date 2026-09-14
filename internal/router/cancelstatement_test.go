package router

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

// TestACancelForAFinishedStatementDoesNotReachTheNext.
//
// A cancel request is handled on its own goroutine: pgwire cancels the
// statement's context, and the router then forwards a Cancel to the poolers.
// The statement's own pump forwards one too, as soon as it sees the context
// end, and that one is what stops it. The cancel request's goroutine can
// therefore still be on its way when the statement has ended and the client
// has sent the next one -- and a Cancel sent then interrupted that next
// statement, which nobody cancelled (PGS-791; seen in CI as a 57014 on the
// "select 1" after a cancelled pg_sleep).
func TestACancelForAFinishedStatementDoesNotReachTheNext(t *testing.T) {
	fp := newFakePooler()
	h := newHarnessWith(t, fp, startFakePooler(t, fp), nil)
	conn := h.connect(t, h.dsn("app", "secret", "app"))
	ctx := context.Background()

	reached, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	h.r.mu.Lock()
	h.r.localCancelled = func() {
		once.Do(func() { close(reached) })
		<-release
	}
	h.r.mu.Unlock()
	asleep := func() bool {
		fp.mu.Lock()
		defer fp.mu.Unlock()
		return len(fp.sleeping) > 0
	}

	first := make(chan error, 1)
	go func() {
		_, err := conn.Exec(ctx, "select pg_sleep(10)", pgx.QueryExecModeSimpleProtocol)
		first <- err
	}()
	waitFor(t, 5*time.Second, asleep, "the first statement never started")
	cancelled := make(chan error, 1)
	go func() { cancelled <- conn.PgConn().CancelRequest(ctx) }()
	<-reached
	if err := <-first; sqlstate(err) != "57014" {
		t.Fatalf("the first statement was not cancelled: %v", err)
	}

	second := make(chan error, 1)
	go func() {
		_, err := conn.Exec(ctx, "select pg_sleep(10)", pgx.QueryExecModeSimpleProtocol)
		second <- err
	}()
	waitFor(t, 5*time.Second, asleep, "the second statement never started")
	before := len(fp.cancelled())
	close(release)
	if err := <-cancelled; err != nil {
		t.Fatal(err)
	}
	if n := len(fp.cancelled()) - before; n != 0 {
		t.Errorf("%d Cancel(s) sent after the cancelled statement had ended and the next had begun; they interrupt that one", n)
	}
	select {
	case err := <-second:
		t.Fatalf("the second statement, which nobody cancelled, ended: %v", err)
	default:
	}

	if err := conn.PgConn().CancelRequest(ctx); err != nil {
		t.Fatal(err)
	}
	if err := <-second; sqlstate(err) != "57014" {
		t.Fatalf("the second statement did not take its own cancel: %v", err)
	}
}

// TestStatementsAreNumberedOnTheWire: the pooler can only ignore a cancel
// for an earlier statement if every request says which statement it belongs
// to and the Cancel says which one it was fired for.
func TestStatementsAreNumberedOnTheWire(t *testing.T) {
	fp := newFakePooler()
	h := newHarnessWith(t, fp, startFakePooler(t, fp), nil)
	conn := h.connect(t, h.dsn("app", "secret", "app"))
	ctx := context.Background()

	for range 2 {
		if _, err := conn.Exec(ctx, "select 1", pgx.QueryExecModeSimpleProtocol); err != nil {
			t.Fatal(err)
		}
	}
	go func() {
		deadline := time.Now().Add(5 * time.Second)
		for time.Now().Before(deadline) {
			fp.mu.Lock()
			asleep := len(fp.sleeping) > 0
			fp.mu.Unlock()
			if asleep {
				_ = conn.PgConn().CancelRequest(ctx)
				return
			}
			time.Sleep(5 * time.Millisecond)
		}
	}()
	if _, err := conn.Exec(ctx, "select pg_sleep(10)", pgx.QueryExecModeSimpleProtocol); sqlstate(err) != "57014" {
		t.Fatalf("cancel: %v", err)
	}

	queries, cancels := fp.numbered()
	var ones []uint64
	var sleep uint64
	for _, q := range queries {
		switch q.sql {
		case "select 1":
			ones = append(ones, q.statement)
		case "select pg_sleep(10)":
			sleep = q.statement
		}
	}
	if len(ones) != 2 || ones[0] == 0 || ones[1] <= ones[0] || sleep <= ones[1] {
		t.Fatalf("statement numbers %v then %d: every statement must carry a number above the one before it", ones, sleep)
	}
	if len(cancels) != 1 || cancels[0] != sleep {
		t.Fatalf("Cancel carried %v, want the number of the statement it cancelled, %d", cancels, sleep)
	}
}
