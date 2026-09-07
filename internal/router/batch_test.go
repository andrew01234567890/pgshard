package router

import (
	"context"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
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

// PostgreSQL lets a BEGIN inside a batch adopt the implicit transaction and
// a COMMIT end it. Nothing here implements that handover, and running the
// statement anyway would commit a transaction the client cannot see, so it
// is refused by name rather than silently doing the wrong thing.
func TestTransactionControlInsideABatchIsRefusedByName(t *testing.T) {
	h := newHarness(t)
	conn := h.connect(t, h.dsn("app", "secret", "app"))
	ctx := context.Background()
	for _, sql := range []string{"select 1; commit", "select 1; begin", "select 1; savepoint s"} {
		_, err := conn.Exec(ctx, sql, pgx.QueryExecModeSimpleProtocol)
		if sqlstate(err) != "0A000" || !strings.Contains(err.Error(), "multi-statement simple query") {
			t.Fatalf("%q: err = %v, want a 0A000 naming the batch", sql, err)
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
