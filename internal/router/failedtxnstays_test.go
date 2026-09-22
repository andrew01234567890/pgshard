package router

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
)

// A failed transaction whose backend is still there refuses a statement for
// another shard too. The backend would have answered 25P02 for a statement
// sent to it; one routed elsewhere moved the session and ran.
func TestAFailedTransactionDoesNotMoveToAnotherShard(t *testing.T) {
	h := newShardedHarness(t)
	ctx := context.Background()
	a, b := h.twoTenants(t)
	conn := h.connect(t, h.dsn())
	if _, err := conn.Exec(ctx, "begin"); err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Exec(ctx, "insert into items values (1)"); err != nil {
		t.Fatal(err)
	}
	_, err := conn.Exec(ctx, "insert into orders (tenant_id, id) values ($1, 1)", a)
	_ = expectRefusal(t, err, "two-phase commit is not available")
	other := h.shardOf(t, b)
	before := len(h.poolers[other].ran())
	_, err = conn.Exec(ctx, "select id from orders where tenant_id = $1", b)
	if sqlstate(err) != "25P02" {
		t.Fatalf("a read for another shard in the failed transaction: %v, want 25P02", err)
	}
	if ran := h.poolers[other].ran()[before:]; len(ran) != 0 {
		t.Fatalf("shard %d ran %v inside the failed transaction", other, ran)
	}
	tag, err := conn.Exec(ctx, "commit")
	if err != nil || tag.String() != "ROLLBACK" {
		t.Fatalf("COMMIT: %q %v, want ROLLBACK", tag, err)
	}
}

// A pipelined batch that recovers to a savepoint and carries on is what
// PostgreSQL runs: the ROLLBACK TO executes before the statements after it.
func TestAPipelinedBatchRecoversToASavepointAndCarriesOn(t *testing.T) {
	h := newHarness(t)
	pc := h.connect(t, h.dsn("app", "secret", "app")).PgConn()
	ctx := context.Background()
	for _, sql := range []string{"begin", "select 1", "savepoint sp"} {
		if _, err := pc.Exec(ctx, sql).ReadAll(); err != nil {
			t.Fatalf("%q: %v", sql, err)
		}
	}
	if _, err := pc.Exec(ctx, "select bad").ReadAll(); err == nil {
		t.Fatal("select bad succeeded")
	}
	if pc.TxStatus() != 'E' {
		t.Fatalf("status %c, want E", pc.TxStatus())
	}
	p := pc.StartPipeline(ctx)
	p.SendQueryParams("rollback to savepoint sp", nil, nil, nil, nil)
	p.SendQueryParams("select 1", nil, nil, nil, nil)
	if err := p.Sync(); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		res, err := p.GetResults()
		if err != nil {
			t.Fatalf("result %d: %v", i, err)
		}
		if rr, ok := res.(*pgconn.ResultReader); ok {
			if _, err := rr.Close(); err != nil {
				t.Fatalf("statement %d of the recovering batch: %v", i, err)
			}
		}
	}
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
	if pc.TxStatus() != 'T' {
		t.Fatalf("status after the recovering batch %c, want T", pc.TxStatus())
	}
}
