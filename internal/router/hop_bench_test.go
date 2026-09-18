package router

import (
	"context"
	"fmt"
	"strconv"
	"strings"
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

// BenchmarkScatterRowsThroughRouter measures the path a row takes from the
// pooler stream to the client on a multi-shard read, where the response
// count -- not the statement count -- is what the transport costs. It runs
// at two widths because the per-column cost and the per-row cost are
// different things: a one-column row is dominated by the message around
// it, and only a wide one shows what each column costs to carry.
func BenchmarkScatterRowsThroughRouter(b *testing.B) {
	for _, cols := range []int{1, 16} {
		b.Run(fmt.Sprintf("cols=%d", cols), func(b *testing.B) { benchScatterRows(b, cols, 0) })
	}
	// A wide row, where a batch is far past the kilobyte at which pgproto3
	// throws its write buffer away. That is the case the row slab exists
	// for: without it every batch grew and copied its way back up from
	// 1 KiB, on every read, forever.
	b.Run("wide", func(b *testing.B) { benchScatterRows(b, 4, 4096) })
}

// benchScatterRows reads 250 rows of cols columns; width, when non-zero, is
// the bytes in each column.
func benchScatterRows(b *testing.B, cols, width int) {
	h := newShardedHarness(b)
	conn, err := pgx.Connect(context.Background(), h.dsn()+"&default_query_exec_mode=simple_protocol")
	if err != nil {
		b.Fatal(err)
	}
	ctx := context.Background()
	sc := script{}
	for i := range cols {
		sc.cols = append(sc.cols, scriptCol{name: "c" + strconv.Itoa(i), oid: 25})
	}
	pad := strings.Repeat("x", max(width-8, 0))
	for i := range 250 {
		row := make([]string, cols)
		for j := range row {
			row[j] = strconv.Itoa(i*cols+j) + pad
		}
		sc.rows = append(sc.rows, row)
	}
	for _, fp := range h.poolers {
		fp.script("select * from orders", sc)
	}
	read := func() {
		rows, err := conn.Query(ctx, "select * from orders")
		if err != nil {
			b.Fatal(err)
		}
		for rows.Next() {
		}
		if rows.Err() != nil {
			b.Fatal(rows.Err())
		}
	}
	read()
	b.ReportAllocs()
	for b.Loop() {
		read()
	}
}

// BenchmarkScatterStatementThroughRouter measures a scatter whose result is
// ONE ROW PER SHARD, so what moves the number is the per-statement fan-out
// -- a pooler stream opened, a goroutine started and torn down for every
// participant -- and not the rows. Its neighbour above deliberately measures
// the other thing and says so: "the response count -- not the statement
// count -- is what the transport costs". At 250 rows and ~20k allocations a
// handful of stream opens is invisible there, so it cannot show a change to
// them either way.
//
// It runs across shard counts because the SLOPE is the answer: the gap
// between one shard and eight, divided by seven, is what one participant
// costs. That is the number PGS-616 is about, and nothing measured it.
func BenchmarkScatterStatementThroughRouter(b *testing.B) {
	for _, shards := range []int{1, 2, 4, 8} {
		b.Run(fmt.Sprintf("shards=%d", shards), func(b *testing.B) { benchScatterStatement(b, shards) })
	}
}

func benchScatterStatement(b *testing.B, shards int) {
	h := newShardedHarnessShards(b, Config{}, shards)
	conn, err := pgx.Connect(context.Background(), h.dsn()+"&default_query_exec_mode=simple_protocol")
	if err != nil {
		b.Fatal(err)
	}
	ctx := context.Background()
	for i, fp := range h.poolers {
		fp.script("select * from orders", script{
			cols: []scriptCol{{name: "c0", oid: 25}},
			rows: [][]string{{strconv.Itoa(i)}},
		})
	}
	read := func() {
		rows, err := conn.Query(ctx, "select * from orders")
		if err != nil {
			b.Fatal(err)
		}
		for rows.Next() {
		}
		if rows.Err() != nil {
			b.Fatal(rows.Err())
		}
	}
	read()
	b.ReportAllocs()
	for b.Loop() {
		read()
	}
}
