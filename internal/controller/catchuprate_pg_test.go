package controller

import (
	"context"
	"strings"
	"testing"
	"time"
)

// applyRate applies ops to a real PostgreSQL through applyOps and reports
// how many operations a second went through, and how many transactions the
// server committed doing it.
func applyRate(t *testing.T, conn ShardConn, shape rowShape, table string, ops []applyOp) (perSec float64, commits int64) {
	t.Helper()
	ctx := context.Background()
	before := xactCommits(t, conn)
	start := time.Now()
	if err := applyOps(ctx, targetConns{0: conn}, shape, table, ops); err != nil {
		t.Fatal(err)
	}
	elapsed := time.Since(start)
	return float64(len(ops)) / elapsed.Seconds(), xactCommits(t, conn) - before
}

func xactCommits(t *testing.T, conn ShardConn) int64 {
	t.Helper()
	// Statistics reach pg_stat_database on the backend's own schedule, so
	// reading it without this returns the same number twice and the
	// difference is a zero that asserts nothing.
	rows, err := conn.Query(context.Background(),
		`SELECT pg_stat_force_next_flush(), (SELECT xact_commit::bigint FROM pg_stat_database WHERE datname = current_database())`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var n int64
	if rows.Next() {
		var flushed any
		if err := rows.Scan(&flushed, &n); err != nil {
			t.Fatal(err)
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return n
}

// PGS-355 asks for applied operations a second, measured, rather than an
// argument that fewer statements must be faster. This applies the same
// rows to the same PostgreSQL twice -- once as one statement per row, the
// way catch-up did it, and once coalesced into multi-row statements -- and
// requires the second to be meaningfully faster than the first.
//
// The assertion is a RATIO measured in the same run on the same machine,
// not a rate, because a rate measured on a shared CI runner says as much
// about the runner as about the code.
func TestCatchUpAppliesMoreOperationsASecondWhenItCoalescesThem(t *testing.T) {
	parallelPG(t)
	dsn := startPostgres(t)
	raw := connect(t, dsn)
	conn := pgxShardConn{raw}
	// A table each, so neither run pays for the other's rows already
	// being in the index.
	for _, name := range []string{"per_row", "coalesced"} {
		mustExec(t, raw, `CREATE TABLE `+name+` (id bigint PRIMARY KEY, tenant_id bigint, note text)`)
	}
	shape := func(table string) rowShape {
		return rowShape{Schema: "public", Name: table, Columns: []string{"id", "tenant_id", "note"}, PK: []string{"id"}}
	}
	const rows = 4000
	note := strings.Repeat("x", 64)

	build := func(table string, carry bool) []applyOp {
		var ops []applyOp
		for i := range rows {
			row := &Tuple{
				Values:    []*string{s(itoa(int64(i))), s("1"), s(note)},
				Unchanged: []bool{false, false, false},
			}
			if carry {
				ops = append(ops, applyOp{shard: 0, up: row})
				continue
			}
			// What routeChange used to produce: every row rendered on its own.
			ops = append(ops, applyOp{shard: 0, sql: shape(table).UpsertSQL(table, []*Tuple{row})[0]})
		}
		return ops
	}

	perRow, perRowCommits := applyRate(t, conn, shape("per_row"), "per_row", build("per_row", false))
	coalesced, coalescedCommits := applyRate(t, conn, shape("coalesced"), "coalesced", build("coalesced", true))

	t.Logf("one statement per row: %8.0f operations/s, %d commits", perRow, perRowCommits)
	t.Logf("coalesced:             %8.0f operations/s, %d commits", coalesced, coalescedCommits)
	t.Logf("coalescing is %.1fx", coalesced/perRow)

	if coalesced <= perRow {
		t.Fatalf("coalescing applied %.0f operations/s against %.0f for one statement per row: it is not buying anything",
			coalesced, perRow)
	}
	// The floor sits between what a regression measures and what a bad
	// runner measures: coalescing off is 1.2x, and the worst seen with
	// coalescing on -- under -race, on a loaded machine -- is 2.4x.
	if coalesced < 1.5*perRow {
		t.Errorf("coalescing is only %.1fx one statement per row; it was 2.9x to 3.7x when written",
			coalesced/perRow)
	}
	// Both go out in the same few round trips, so neither should be paying
	// a commit per row -- that is what applyToTarget's batching is for.
	for name, n := range map[string]int64{"per-row": perRowCommits, "coalesced": coalescedCommits} {
		if n > rows/10 {
			t.Errorf("%s applied %d rows in %d commits; the batching is not holding", name, rows, n)
		}
	}
	for _, table := range []string{"per_row", "coalesced"} {
		var got int64
		if err := raw.QueryRow(context.Background(), `SELECT count(*) FROM `+table).Scan(&got); err != nil {
			t.Fatal(err)
		}
		if got != rows {
			t.Fatalf("%s: %d rows landed, want %d", table, got, rows)
		}
	}
}

// The other half of PGS-355's batching, measured the same way, plus the
// part that cannot be argued from the Go side: that a row-constructor IN
// list deletes exactly the composite keys it names on real PostgreSQL.
func TestCatchUpAppliesMoreDeletesASecondWhenItBatchesThem(t *testing.T) {
	parallelPG(t)
	dsn := startPostgres(t)
	raw := connect(t, dsn)
	conn := pgxShardConn{raw}
	const rows = 4000
	note := strings.Repeat("x", 64)

	shape := func(table string) rowShape {
		return rowShape{Schema: "public", Name: table, Columns: []string{"id", "tenant_id", "note"}, PK: []string{"id"}}
	}
	seed := func(table string) []applyOp {
		mustExec(t, raw, `CREATE TABLE `+table+` (id bigint PRIMARY KEY, tenant_id bigint, note text)`)
		var ups, dels []applyOp
		for i := range rows {
			row := &Tuple{Values: []*string{s(itoa(int64(i))), s("1"), s(note)}, Unchanged: []bool{false, false, false}}
			ups = append(ups, applyOp{shard: 0, up: row})
			dels = append(dels, applyOp{shard: 0, del: row})
		}
		if err := applyOps(context.Background(), targetConns{0: conn}, shape(table), table, ups); err != nil {
			t.Fatal(err)
		}
		return dels
	}

	oneAtATime := seed("del_per_row")
	for i, op := range oneAtATime {
		oneAtATime[i] = applyOp{shard: 0, sql: shape("del_per_row").DeleteSQL("del_per_row", op.del)}
	}
	batched := seed("del_batched")

	perRow, perRowCommits := applyRate(t, conn, shape("del_per_row"), "del_per_row", oneAtATime)
	inList, inListCommits := applyRate(t, conn, shape("del_batched"), "del_batched", batched)

	t.Logf("one delete per statement: %8.0f operations/s, %d commits", perRow, perRowCommits)
	t.Logf("one IN list:              %8.0f operations/s, %d commits", inList, inListCommits)
	t.Logf("batching deletes is %.1fx", inList/perRow)

	if inList <= perRow {
		t.Fatalf("batched deletes ran at %.0f operations/s against %.0f one at a time", inList, perRow)
	}
	for _, table := range []string{"del_per_row", "del_batched"} {
		var left int64
		if err := raw.QueryRow(context.Background(), `SELECT count(*) FROM `+table).Scan(&left); err != nil {
			t.Fatal(err)
		}
		if left != 0 {
			t.Errorf("%s: %d of %d rows survived the delete", table, left, rows)
		}
	}

	// Composite keys go out as a row constructor. PostgreSQL compares
	// those element by element, so the list must delete the pairs it
	// names and nothing else -- including leaving (1,9) alone when the
	// list holds (1,2) and (3,9).
	mustExec(t, raw, `CREATE TABLE pairs (a bigint, b bigint, note text, PRIMARY KEY (a, b))`)
	for _, v := range [][2]string{{"1", "2"}, {"3", "9"}, {"1", "9"}, {"3", "2"}} {
		mustExec(t, raw, `INSERT INTO pairs VALUES ($1, $2, 'n')`, v[0], v[1])
	}
	comp := rowShape{Schema: "public", Name: "pairs", Columns: []string{"a", "b", "note"}, PK: []string{"a", "b"}}
	pair := func(a, b string) *Tuple {
		return &Tuple{Values: []*string{s(a), s(b), s("n")}, Unchanged: []bool{false, false, false}}
	}
	if err := applyOps(context.Background(), targetConns{0: conn}, comp, "pairs",
		[]applyOp{{shard: 0, del: pair("1", "2")}, {shard: 0, del: pair("3", "9")}}); err != nil {
		t.Fatal(err)
	}
	var left []string
	rowsLeft, err := raw.Query(context.Background(), `SELECT a || ',' || b FROM pairs ORDER BY a, b`)
	if err != nil {
		t.Fatal(err)
	}
	for rowsLeft.Next() {
		var v string
		if err := rowsLeft.Scan(&v); err != nil {
			t.Fatal(err)
		}
		left = append(left, v)
	}
	if got := strings.Join(left, " "); got != "1,9 3,2" {
		t.Fatalf("the row-constructor list left %q, want \"1,9 3,2\": it is not matching pairs element by element", got)
	}
}
