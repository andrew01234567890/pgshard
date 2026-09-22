package router

import (
	"context"
	"fmt"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// A shard parked earlier in the transaction does not replay the prelude when
// it is revived, so a SET TRANSACTION or SET LOCAL has to reach it directly:
// a READ ONLY that missed it left a write there free to commit.
func TestATransactionScopedSetReachesEveryShardOfTheTransaction(t *testing.T) {
	for _, c := range []struct {
		set      string
		extended bool
	}{
		{"set transaction read only", false},
		{"set local statement_timeout = 1234", false},
		{"set transaction read only", true},
		{"set local statement_timeout = 1234", true},
	} {
		set, extended := c.set, c.extended
		t.Run(fmt.Sprintf("%s/extended=%v", set, extended), func(t *testing.T) {
			h := newShardedHarness(t)
			conn := h.connect(t, h.dsn())
			ctx := context.Background()
			a, b := h.twoTenants(t)
			for _, sql := range []string{"begin",
				fmt.Sprintf("select * from orders where tenant_id = %d", a),
				fmt.Sprintf("select * from orders where tenant_id = %d", b)} {
				if _, err := conn.Exec(ctx, sql, pgx.QueryExecModeSimpleProtocol); err != nil {
					t.Fatalf("%s: %v", sql, err)
				}
			}
			if extended {
				// Parse, Bind, Execute and Sync, as pgjdbc and npgsql send it.
				if rr := conn.PgConn().ExecParams(ctx, set, nil, nil, nil, nil).Read(); rr.Err != nil {
					t.Fatalf("%s: %v", set, rr.Err)
				}
			} else if _, err := conn.Exec(ctx, set, pgx.QueryExecModeSimpleProtocol); err != nil {
				t.Fatalf("%s: %v", set, err)
			}
			for _, k := range []int64{a, b} {
				if sh := h.shardOf(t, k); !h.ranOn(sh, set) {
					t.Fatalf("shard %d of the transaction never ran %q: %v", sh, set, h.poolers[sh].ran())
				}
			}
		})
	}
}

// A pipeline can have the SET answered at a Flush, which runs the batch
// through its own path.
func TestATransactionScopedSetAnsweredAtAFlushReachesEveryShard(t *testing.T) {
	h := newShardedHarness(t)
	conn := h.connect(t, h.dsn())
	ctx := context.Background()
	a, b := h.twoTenants(t)
	for _, sql := range []string{"begin",
		fmt.Sprintf("select * from orders where tenant_id = %d", a),
		fmt.Sprintf("select * from orders where tenant_id = %d", b)} {
		if _, err := conn.Exec(ctx, sql, pgx.QueryExecModeSimpleProtocol); err != nil {
			t.Fatalf("%s: %v", sql, err)
		}
	}
	const set = "set local statement_timeout = 1234"
	p := conn.PgConn().StartPipeline(ctx)
	p.SendQueryParams(set, nil, nil, nil, nil)
	p.SendFlushRequest()
	if err := p.Flush(); err != nil {
		t.Fatal(err)
	}
	res, err := p.GetResults()
	if err != nil {
		t.Fatal(err)
	}
	if rr, ok := res.(*pgconn.ResultReader); ok {
		if _, err := rr.Close(); err != nil {
			t.Fatal(err)
		}
	}
	if err := p.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
	for _, k := range []int64{a, b} {
		if sh := h.shardOf(t, k); !h.ranOn(sh, set) {
			t.Fatalf("shard %d of the transaction never ran %q: %v", sh, set, h.poolers[sh].ran())
		}
	}
}
