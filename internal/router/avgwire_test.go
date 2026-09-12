package router

import (
	"context"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
)

// TestAvgOverIntegersIsDescribedAsNumeric: the shards are asked for sum(x),
// so for an int2 or int4 column they describe an int8 -- and the router then
// answers a numeric under that description. A client reads the description:
// in the simple protocol it parses "2.5" as an int8 and fails, and in the
// extended protocol it asks for binary int8 and is handed a numeric.
//
// The column's NAME matters for the same reason and is fixed in the shard
// query rather than here, so that an alias the client did write still wins.
func TestAvgOverIntegersIsDescribedAsNumeric(t *testing.T) {
	h := newShardedHarness(t)
	for _, fp := range h.poolers {
		// sum(qty) of an int2 column comes back as int8, one row per shard.
		fp.script("SELECT pg_catalog.sum(qty) AS avg, pg_catalog.count(qty) FROM orders", script{
			cols: []scriptCol{{name: "avg", oid: 20}, {name: "count", oid: 20}},
			rows: [][]string{{"10", "4"}},
		})
		// The extended protocol describes the statement the CLIENT wrote,
		// on a shard, before the scatter runs -- and a real PostgreSQL
		// answers that one "avg", numeric, because it still says avg().
		// The simple protocol has no such describe and takes its answer
		// from the scatter, which is where the correction is needed.
		fp.script("select avg(qty) from orders", script{
			cols: []scriptCol{{name: "avg", oid: 1700}},
		})
	}
	for _, mode := range []string{"simple_protocol", "cache_statement", "cache_describe"} {
		conn := h.connect(t, h.dsn()+"&default_query_exec_mode="+mode)
		rows, err := conn.Query(context.Background(), "select avg(qty) from orders")
		if err != nil {
			t.Fatalf("%s: %v", mode, err)
		}
		fds := rows.FieldDescriptions()
		if len(fds) != 1 {
			t.Fatalf("%s: %d columns, want 1", mode, len(fds))
		}
		if fds[0].Name != "avg" {
			t.Errorf("%s: column named %q, want avg -- the client asked for an average, not a sum", mode, fds[0].Name)
		}
		if fds[0].DataTypeOID != 1700 {
			t.Errorf("%s: column type oid %d, want 1700 (numeric): the value the router sends is a numeric", mode, fds[0].DataTypeOID)
		}
		var got string
		n := 0
		for rows.Next() {
			if err := rows.Scan(&got); err != nil {
				rows.Close()
				t.Fatalf("%s: scan: %v", mode, err)
			}
			n++
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			t.Fatalf("%s: %v", mode, err)
		}
		// Four shards, each 10 over 4, so 40 over 16 -- and the scale is
		// PostgreSQL's for a numeric quotient, not a bare "2.5".
		if n != 1 || got != "2.5000000000000000" {
			t.Errorf("%s: %d rows, avg %q, want one row of 2.5000000000000000", mode, n, got)
		}
	}
}

// An alias the client wrote is the client's, not ours.
func TestAvgKeepsAnAliasTheClientWrote(t *testing.T) {
	h := newShardedHarness(t)
	for _, fp := range h.poolers {
		fp.script("SELECT pg_catalog.sum(qty) AS mean, pg_catalog.count(qty) FROM orders", script{
			cols: []scriptCol{{name: "mean", oid: 20}, {name: "count", oid: 20}},
			rows: [][]string{{"10", "4"}},
		})
		fp.script("select avg(qty) as mean from orders", script{cols: []scriptCol{{name: "mean", oid: 1700}}})
	}
	conn := h.connect(t, h.dsn())
	rows, err := conn.Query(context.Background(), "select avg(qty) as mean from orders", pgx.QueryExecModeSimpleProtocol)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	if name := rows.FieldDescriptions()[0].Name; name != "mean" {
		t.Errorf("column named %q, want the client's alias", name)
	}
}

// avg() over a float8 column is a float8, and the shard already said so.
func TestAvgOverFloatKeepsTheFloatDescription(t *testing.T) {
	h := newShardedHarness(t)
	for _, fp := range h.poolers {
		fp.script("SELECT pg_catalog.sum(price) AS avg, pg_catalog.count(price) FROM orders", script{
			cols: []scriptCol{{name: "avg", oid: 701}, {name: "count", oid: 20}},
			rows: [][]string{{"10", "4"}},
		})
		fp.script("select avg(price) from orders", script{cols: []scriptCol{{name: "avg", oid: 701}}})
	}
	conn := h.connect(t, h.dsn())
	rows, err := conn.Query(context.Background(), "select avg(price) from orders", pgx.QueryExecModeSimpleProtocol)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var got float64
	if !rows.Next() {
		t.Fatalf("no row: %v", rows.Err())
	}
	if oid := rows.FieldDescriptions()[0].DataTypeOID; oid != 701 {
		t.Errorf("column type oid %d, want 701 (float8)", oid)
	}
	if err := rows.Scan(&got); err != nil {
		t.Fatal(err)
	}
	if got != 2.5 {
		t.Errorf("avg %v, want 2.5", got)
	}
}

// A real column is refused, because PostgreSQL accumulates avg(real) in
// double precision while its sum(real) accumulates in real.
func TestAvgOverRealIsRefused(t *testing.T) {
	h := newShardedHarness(t)
	for _, fp := range h.poolers {
		fp.script("SELECT pg_catalog.sum(rate) AS avg, pg_catalog.count(rate) FROM orders", script{
			cols: []scriptCol{{name: "avg", oid: 700}, {name: "count", oid: 20}},
			rows: [][]string{{"10", "4"}},
		})
	}
	conn := h.connect(t, h.dsn())
	_, err := conn.Exec(context.Background(), "select avg(rate) from orders", pgx.QueryExecModeSimpleProtocol)
	if err == nil || !strings.Contains(err.Error(), "avg() over a real column") {
		t.Fatalf("want the real refusal, got %v", err)
	}
}
