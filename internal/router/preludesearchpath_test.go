package router

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/andrew01234567890/pgshard/internal/catalog"
)

// TestThePreludeReplayKeepsTheStartupSearchPath (PGS-883 item 8): a
// transaction that gave its backend up and got it back ran on with the
// wrong search_path.
//
// RESET search_path restores the STARTUP path on this session and the
// SERVER default on a backend that never saw the client's startup options
// -- which is why reapplyStartupSearchPath exists. But what it sends is not
// a statement the client issued, so txnPrelude does not record it, and the
// ROLLBACK that releaseUntouchedTxn sends before the wait had already undone
// it. Replaying the prelude therefore reran the RESET and stopped there.
//
// Measured before the fix, with a client connected as search_path=audit,public:
//
//	BEGIN; RESET search_path; <write held by the cluster write pause>
//	-> current_setting('search_path') = ""
//
// Empty, where the session and the planner both say "audit, public". Every
// unqualified name after that resolves somewhere else.
func TestThePreludeReplayKeepsTheStartupSearchPath(t *testing.T) {
	t.Run("AcrossAWritePause", func(t *testing.T) {
		h := newTxnHarness(t)
		ctx := context.Background()
		a, _ := h.twoTenants(t)
		conn := h.connect(t, h.dsn()+"&search_path=audit,public")

		var v string
		if err := conn.QueryRow(ctx, "select current_setting('search_path')").Scan(&v); err != nil || v != "audit, public" {
			t.Fatalf("baseline search_path = %q (%v)", v, err)
		}
		// The pause has to be up before the transaction opens, or
		// txnPreFence sends gateWrite down the refusal branch instead of
		// the release-and-replay one this is about.
		h.fenced(true)
		for _, sql := range []string{"begin", "reset search_path"} {
			if _, err := conn.Exec(ctx, sql); err != nil {
				t.Fatalf("%s: %v", sql, err)
			}
		}
		done := make(chan error, 1)
		go func() {
			_, err := conn.Exec(ctx, "insert into orders (tenant_id, id) values ($1, 991)", a)
			done <- err
		}()
		select {
		case err := <-done:
			t.Fatalf("the write returned while the pause was up: %v", err)
		case <-time.After(150 * time.Millisecond):
		}
		h.fenced(false)
		if err := <-done; err != nil {
			t.Fatalf("the held write: %v", err)
		}
		if err := conn.QueryRow(ctx, "select current_setting('search_path')").Scan(&v); err != nil {
			t.Fatalf("after the replay: %v", err)
		}
		if v != "audit, public" {
			t.Errorf("search_path = %q after the transaction got its backend back, want \"audit, public\": the rest of the transaction resolves unqualified names somewhere the planner did not route them", v)
		}
		_, _ = conn.Exec(ctx, "rollback")
	})

	// The other replay: a sequential DDL releases the transaction for the
	// migration wait and reopens it afterwards. Its shard statements are
	// refused after the DDL, so the observation is what the shard was
	// actually sent.
	t.Run("AcrossASequentialDDL", func(t *testing.T) {
		h := newDDLHarness(t, &fakeQueue{})
		app := h.snap.Databases["app"]
		app.DDLTransactions = catalog.DDLTransactionsSequential
		h.snap.Databases["app"] = app
		ctx := context.Background()
		conn := h.connect(t, h.dsn()+"&search_path=audit,public")

		for _, sql := range []string{"begin", "reset search_path"} {
			if _, err := conn.Exec(ctx, sql); err != nil {
				t.Fatalf("%s: %v", sql, err)
			}
		}
		if _, err := conn.Exec(ctx, "create table t12 (id int primary key)"); err != nil {
			t.Fatalf("DDL: %v", err)
		}
		var after []string
		for i := range h.poolers {
			ran := h.poolers[i].ran()
			for n, q := range ran {
				if strings.EqualFold(strings.TrimSpace(q), "reset search_path") {
					after = ran[n+1:]
				}
			}
		}
		if after == nil {
			t.Fatal("no shard was sent the RESET, so this test never reached the replay")
		}
		var restored bool
		for _, q := range after {
			if strings.Contains(q, "set_config('search_path', 'audit, public'") {
				restored = true
			}
		}
		if !restored {
			t.Errorf("after the replayed RESET the shard was never given the startup search_path back; it ran %q", after)
		}
	})

	// The contrast: a transaction that SET a path rather than resetting one
	// keeps that path, not the startup one. Re-asserting the effective path
	// must not undo the client's own SET.
	t.Run("ASetIsNotUndone", func(t *testing.T) {
		h := newTxnHarness(t)
		ctx := context.Background()
		a, _ := h.twoTenants(t)
		conn := h.connect(t, h.dsn()+"&search_path=audit,public")
		h.fenced(true)
		for _, sql := range []string{"begin", "set search_path to reports"} {
			if _, err := conn.Exec(ctx, sql); err != nil {
				t.Fatalf("%s: %v", sql, err)
			}
		}
		done := make(chan error, 1)
		go func() {
			_, err := conn.Exec(ctx, "insert into orders (tenant_id, id) values ($1, 992)", a)
			done <- err
		}()
		select {
		case err := <-done:
			t.Fatalf("the write returned while the pause was up: %v", err)
		case <-time.After(150 * time.Millisecond):
		}
		h.fenced(false)
		if err := <-done; err != nil {
			t.Fatalf("the held write: %v", err)
		}
		var v string
		if err := conn.QueryRow(ctx, "select current_setting('search_path')").Scan(&v); err != nil {
			t.Fatalf("after the replay: %v", err)
		}
		if v != "reports" {
			t.Errorf("search_path = %q, want \"reports\": the replay overwrote the transaction's own SET", v)
		}
		_, _ = conn.Exec(ctx, "rollback")
	})
}
