package controller

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

// ratePasses is how many times each side of a comparison runs. One
// scheduler or GC stall inside a thirty-millisecond window halves the rate
// that pass reports, and the two sides are compared against each other, so
// the best of a few passes is what the comparison can stand on.
const ratePasses = 3

// rateCase is one side of a comparison: a fresh table each pass, so no pass
// is helped by the previous one's rows already being in the index, and
// whatever has to exist before the timed work does.
type rateCase struct {
	table string
	shape func(table string) rowShape
	setup func(table string)
	ops   func(table string) []applyOp
}

func (c rateCase) rate(t *testing.T, raw *pgx.Conn, conn ShardConn) float64 {
	t.Helper()
	best := 0.0
	for pass := range ratePasses {
		table := fmt.Sprintf("%s_%d", c.table, pass)
		mustExec(t, raw, `CREATE TABLE `+table+` (id bigint PRIMARY KEY, tenant_id bigint, note text)`)
		if c.setup != nil {
			c.setup(table)
		}
		ops := c.ops(table)
		start := time.Now()
		if err := applyOps(context.Background(), targetConns{0: conn}, c.shape(table), table, ops); err != nil {
			t.Fatal(err)
		}
		best = max(best, float64(len(ops))/time.Since(start).Seconds())
	}
	return best
}

func rateShape(table string) rowShape {
	return rowShape{Schema: "public", Name: table, Columns: []string{"id", "tenant_id", "note"}, PK: []string{"id"}}
}

const rateRows = 4000

func rateRow(i int) *Tuple {
	return &Tuple{
		Values:    []*string{s(itoa(int64(i))), s("1"), s(strings.Repeat("x", 64))},
		Unchanged: []bool{false, false, false},
	}
}

// PGS-355 asks for applied operations a second, measured, rather than an
// argument that fewer statements must be faster. This applies the same
// rows to the same PostgreSQL both ways -- one statement per row, the way
// catch-up did it, and coalesced into multi-row statements -- and requires
// the second to be meaningfully faster.
//
// The assertion is a RATIO measured in the same run on the same machine,
// never a rate: a rate on a shared runner says as much about the runner as
// about the code.
func TestCatchUpAppliesMoreOperationsASecondWhenItCoalescesThem(t *testing.T) {
	parallelPG(t)
	raw := connect(t, startPostgres(t))
	conn := pgxShardConn{raw}

	perRow := rateCase{table: "per_row", shape: rateShape, ops: func(table string) []applyOp {
		var ops []applyOp
		for i := range rateRows {
			ops = append(ops, applyOp{shard: 0, sql: rateShape(table).UpsertSQL(table, []*Tuple{rateRow(i)})[0]})
		}
		return ops
	}}.rate(t, raw, conn)

	coalesced := rateCase{table: "coalesced", shape: rateShape, ops: func(string) []applyOp {
		var ops []applyOp
		for i := range rateRows {
			ops = append(ops, applyOp{shard: 0, up: rateRow(i)})
		}
		return ops
	}}.rate(t, raw, conn)

	t.Logf("one statement per row: %8.0f operations/s", perRow)
	t.Logf("coalesced:             %8.0f operations/s", coalesced)
	t.Logf("coalescing is %.1fx", coalesced/perRow)

	// The floor sits between what a regression measures and what a bad
	// runner measures: coalescing off is 1.2x, and best-of-three with it
	// on has not been seen below 2.4x.
	if coalesced < 1.5*perRow {
		t.Errorf("coalescing is only %.1fx one statement per row; it was 2.9x to 3.7x when written", coalesced/perRow)
	}
	assertRowsLanded(t, raw, "coalesced", rateRows)
}

// The other half of PGS-355's batching, measured the same way, plus the
// part that cannot be argued from the Go side: that a row-constructor IN
// list deletes exactly the composite keys it names on real PostgreSQL.
func TestCatchUpAppliesMoreDeletesASecondWhenItBatchesThem(t *testing.T) {
	parallelPG(t)
	raw := connect(t, startPostgres(t))
	conn := pgxShardConn{raw}

	// Every pass deletes rows that have to be there first, and seeding
	// them is not part of what is being timed.
	seed := func(table string) {
		var ops []applyOp
		for i := range rateRows {
			ops = append(ops, applyOp{shard: 0, up: rateRow(i)})
		}
		if err := applyOps(context.Background(), targetConns{0: conn}, rateShape(table), table, ops); err != nil {
			t.Fatal(err)
		}
	}

	perRow := rateCase{table: "del_per_row", shape: rateShape, setup: seed, ops: func(table string) []applyOp {
		var ops []applyOp
		for i := range rateRows {
			ops = append(ops, applyOp{shard: 0, sql: rateShape(table).DeleteSQL(table, rateRow(i))})
		}
		return ops
	}}.rate(t, raw, conn)

	batched := rateCase{table: "del_batched", shape: rateShape, setup: seed, ops: func(string) []applyOp {
		var ops []applyOp
		for i := range rateRows {
			ops = append(ops, applyOp{shard: 0, del: rateRow(i)})
		}
		return ops
	}}.rate(t, raw, conn)

	t.Logf("one delete per statement: %8.0f operations/s", perRow)
	t.Logf("one IN list:              %8.0f operations/s", batched)
	t.Logf("batching deletes is %.1fx", batched/perRow)

	if batched < 1.5*perRow {
		t.Errorf("batching deletes is only %.1fx one at a time; it was over 10x when written", batched/perRow)
	}
	for _, table := range []string{"del_per_row", "del_batched"} {
		assertRowsLanded(t, raw, table, 0)
	}
	assertCompositeDeleteMatchesPairs(t, raw, conn)
}

func assertRowsLanded(t *testing.T, raw *pgx.Conn, table string, want int64) {
	t.Helper()
	for pass := range ratePasses {
		var got int64
		name := fmt.Sprintf("%s_%d", table, pass)
		if err := raw.QueryRow(context.Background(), `SELECT count(*) FROM `+name).Scan(&got); err != nil {
			t.Fatal(err)
		}
		if got != want {
			t.Fatalf("%s holds %d rows, want %d", name, got, want)
		}
	}
}

// A composite key goes out as a row constructor, which PostgreSQL compares
// element by element. The list must therefore delete the pairs it names
// and not the cross product of their columns: asked for (1,2) and (3,9) it
// has to leave (1,9) and (3,2) alone. An IN list per column, ANDed --
// which is the natural wrong way to write this -- deletes all four.
func assertCompositeDeleteMatchesPairs(t *testing.T, raw *pgx.Conn, conn ShardConn) {
	t.Helper()
	mustExec(t, raw, `CREATE TABLE pairs (a bigint, b bigint, note text, PRIMARY KEY (a, b))`)
	for _, v := range [][2]string{{"1", "2"}, {"3", "9"}, {"1", "9"}, {"3", "2"}} {
		mustExec(t, raw, `INSERT INTO pairs VALUES ($1, $2, 'n')`, v[0], v[1])
	}
	shape := rowShape{Schema: "public", Name: "pairs", Columns: []string{"a", "b", "note"}, PK: []string{"a", "b"}}
	pair := func(a, b string) *Tuple {
		return &Tuple{Values: []*string{s(a), s(b), s("n")}, Unchanged: []bool{false, false, false}}
	}
	if err := applyOps(context.Background(), targetConns{0: conn}, shape, "pairs",
		[]applyOp{{shard: 0, del: pair("1", "2")}, {shard: 0, del: pair("3", "9")}}); err != nil {
		t.Fatal(err)
	}
	rows, err := raw.Query(context.Background(), `SELECT a || ',' || b FROM pairs ORDER BY a, b`)
	if err != nil {
		t.Fatal(err)
	}
	var left []string
	for rows.Next() {
		var v string
		if err := rows.Scan(&v); err != nil {
			t.Fatal(err)
		}
		left = append(left, v)
	}
	if got := strings.Join(left, " "); got != "1,9 3,2" {
		t.Fatalf("the row-constructor list left %q, want \"1,9 3,2\": it is not matching pairs element by element", got)
	}
	assertANullKeyDeletesWhatItAlwaysDid(t, raw)
}

// Batching must not change what a delete removes when a key value is
// NULL. A real primary key cannot hold one, so this is about the forms
// being equivalent rather than about a case that occurs: a NULL matches
// nothing in either, and an entry carrying one does not stop the other
// entries in the same list from matching.
//
// Asserted against PostgreSQL rather than reasoned about, because row
// constructors and NULL are exactly where reasoning goes wrong.
func assertANullKeyDeletesWhatItAlwaysDid(t *testing.T, raw *pgx.Conn) {
	t.Helper()
	mustExec(t, raw, `CREATE TABLE nullable (a bigint, b bigint)`)
	mustExec(t, raw, `INSERT INTO nullable VALUES (1, 2), (1, NULL), (3, 9)`)
	for _, c := range []struct {
		what, where string
		want        int64
	}{
		{"the equality form a delete has always used", `"a" = 1 AND "b" = NULL`, 0},
		{"one row constructor carrying a NULL", `("a", "b") IN ((1, NULL))`, 0},
		{"a NULL entry beside a real pair", `("a", "b") IN ((1, NULL), (3, 9))`, 1},
		{"a NULL in a single-column list", `"a" IN (1, NULL)`, 2},
	} {
		var got int64
		if err := raw.QueryRow(context.Background(), `SELECT count(*) FROM nullable WHERE `+c.where).Scan(&got); err != nil {
			t.Fatal(err)
		}
		if got != c.want {
			t.Errorf("%s matched %d rows, want %d (%s)", c.what, got, c.want, c.where)
		}
	}
}
