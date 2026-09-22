package router

import (
	"context"
	"fmt"
	"testing"

	"github.com/jackc/pgx/v5"
)

// A shard parked earlier in the transaction does not replay the prelude when
// it is revived, so a SET TRANSACTION or SET LOCAL has to reach it directly:
// a READ ONLY that missed it left a write there free to commit.
func TestATransactionScopedSetReachesEveryShardOfTheTransaction(t *testing.T) {
	for _, set := range []string{"set transaction read only", "set local statement_timeout = 1234"} {
		t.Run(set, func(t *testing.T) {
			h := newShardedHarness(t)
			conn := h.connect(t, h.dsn())
			ctx := context.Background()
			a, b := h.twoTenants(t)
			for _, sql := range []string{"begin",
				fmt.Sprintf("select * from orders where tenant_id = %d", a),
				fmt.Sprintf("select * from orders where tenant_id = %d", b),
				set} {
				if _, err := conn.Exec(ctx, sql, pgx.QueryExecModeSimpleProtocol); err != nil {
					t.Fatalf("%s: %v", sql, err)
				}
			}
			for _, k := range []int64{a, b} {
				if sh := h.shardOf(t, k); !h.ranOn(sh, set) {
					t.Fatalf("shard %d of the transaction never ran %q: %v", sh, set, h.poolers[sh].ran())
				}
			}
		})
	}
}
