package router

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5"
)

// BenchmarkUnshardedSelectThroughRouter measures a whole statement through
// the router and its pooler stream: plan, send, and relay the responses
// back. It is the path the stream's receive sits on, which the raw gRPC
// benchmark in test/perf does not touch -- that one drives the generated
// client directly and never builds a poolerStream.
func BenchmarkUnshardedSelectThroughRouter(b *testing.B) {
	h := newShardedHarness(b)
	conn, err := pgx.Connect(context.Background(), h.dsn())
	if err != nil {
		b.Fatal(err)
	}
	ctx := context.Background()
	for _, fp := range h.poolers {
		fp.script("select 1 from items", int4Rows("1"))
	}
	if _, err := conn.Exec(ctx, "select 1 from items"); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	for b.Loop() {
		if _, err := conn.Exec(ctx, "select 1 from items"); err != nil {
			b.Fatal(err)
		}
	}
}
