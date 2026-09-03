package controller

import (
	"context"
	"fmt"
	"math"
	"testing"

	"github.com/andrew01234567890/pgshard/internal/placement"
)

// TestBucketDigestsAgreeWithAScanPerRange pins the replacement to what it
// replaced: the grouped scan must return, for every range, exactly the
// digest a filtered scan of that range returns. The whole point of the
// change is that verification stops paying a scan per target, so an
// equivalence test is the one that matters.
func TestBucketDigestsAgreeWithAScanPerRange(t *testing.T) {
	parallelPG(t)
	f := newPlacementFixture(t)
	ctx := context.Background()
	app := f.app(0)
	conn := pgxShardConn{app}
	mustExec(t, app, `CREATE TABLE widgets (id bigint PRIMARY KEY, note text)`)
	for i := range int64(3000) {
		mustExec(t, app, `INSERT INTO widgets VALUES ($1, $2)`, i*7919+13, fmt.Sprintf("n%d", i))
	}
	hash, err := KeyHashExpr("id", "int8")
	if err != nil {
		t.Fatal(err)
	}
	// Four uneven ranges that between them cover the keyspace, plus a
	// deliberate hole, so a row outside every range has to be dropped by
	// both paths rather than counted somewhere.
	ranges := []placement.Range{
		{Start: math.MinInt64, End: -(1 << 62)},
		{Start: -(1 << 62) + 1, End: 0},
		{Start: 1, End: 1 << 61},
		{Start: (1 << 61) + 1, End: (1 << 62)},
	}
	scans := func() int64 {
		mustExec(t, app, `SELECT pg_stat_force_next_flush()`)
		return queryOne[int64](t, app, `SELECT seq_scan FROM pg_stat_user_tables WHERE relname = 'widgets'`)
	}

	before := scans()
	buckets, err := bucketDigests(ctx, conn, "public", "widgets", hash, ranges)
	if err != nil {
		t.Fatal(err)
	}
	if grouped := scans() - before; grouped != 1 {
		t.Fatalf("the grouped scan read the table %d times, want 1", grouped)
	}
	total, perRange := int64(0), scans()
	for i, r := range ranges {
		want, err := digest(ctx, conn, "public", "widgets", RangeFilter(hash, r))
		if err != nil {
			t.Fatal(err)
		}
		if buckets[i] != want {
			t.Fatalf("range %d: grouped scan gave %+v, a scan of that range alone gives %+v", i, buckets[i], want)
		}
		total += want.Rows
	}
	// The point of the change, measured rather than argued: the old shape
	// read the table once per target range, so the fence grew with the
	// number of targets as well as with the table.
	if n := scans() - perRange; n != int64(len(ranges)) {
		t.Fatalf("a scan per range read the table %d times for %d ranges", n, len(ranges))
	}
	if total == 0 {
		t.Fatal("every range was empty, so the comparison proved nothing")
	}
	all, err := digest(ctx, conn, "public", "widgets", "")
	if err != nil {
		t.Fatal(err)
	}
	if total >= all.Rows {
		t.Fatalf("the ranges hold %d of %d rows; the hole outside them was meant to leave some out", total, all.Rows)
	}
}
