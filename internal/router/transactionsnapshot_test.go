package router

import (
	"context"
	"strings"
	"testing"
)

// TestSetTransactionSnapshotIsRefused (PGS-883 item 8, snapshot half): the
// router used to relay SET TRANSACTION SNAPSHOT verbatim and accept it.
//
// A snapshot belongs to one backend. Its id comes from
// pg_export_snapshot(), which pgshard routes to a single shard, so the id
// is only meaningful on that shard -- and the client has no way to know
// which one it was. Accepted, it pinned a snapshot on the session's current
// shard and left every other shard the transaction reached on its own: the
// same missing guarantee that makes a multi-shard REPEATABLE READ refused.
//
// It also entered the transaction prelude, which the router replays onto a
// fresh backend whenever it has to give one up -- a cluster write pause or
// a sequential DDL -- by which time the exporting transaction has ended and
// the replay fails with PostgreSQL's own "invalid snapshot identifier",
// from a statement the client sent long before.
func TestSetTransactionSnapshotIsRefused(t *testing.T) {
	h := newTxnHarness(t)
	ctx := context.Background()
	conn := h.connect(t, h.dsn())
	if _, err := conn.Exec(ctx, "begin isolation level repeatable read"); err != nil {
		t.Fatal(err)
	}
	_, err := conn.Exec(ctx, "set transaction snapshot '00000003-0000006F-1'")
	if err == nil {
		t.Fatal("SET TRANSACTION SNAPSHOT was accepted: it pins a snapshot on one shard while the transaction may reach others, and it breaks the prelude replay")
	}
	if sqlstate(err) != "0A000" {
		t.Errorf("refused with %s, want 0A000: %v", sqlstate(err), err)
	}
	for _, want := range []string{"snapshot", "shard"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("the refusal does not mention %q: %v", want, err)
		}
	}
	for i := range h.poolers {
		for _, q := range h.poolers[i].ran() {
			if strings.Contains(strings.ToLower(q), "transaction snapshot") {
				t.Errorf("shard %d was still sent the statement: %q", i, q)
			}
		}
	}
	_, _ = conn.Exec(ctx, "rollback")

	// The contrast: the other two VAR_SET_MULTI forms are untouched. Both
	// share the parser kind and differ only by name, so a refusal keyed on
	// the kind would have taken these with it.
	for _, sql := range []string{
		"set transaction isolation level repeatable read",
		"set transaction read only",
		"set session characteristics as transaction read only",
	} {
		if _, err := conn.Exec(ctx, sql); err != nil {
			t.Errorf("%s: %v", sql, err)
		}
	}
}
