//go:build integration

package router

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

// PGS-778: COPY FROM STDIN into a sharded table through the router stores
// exactly what one PostgreSQL stores from the same stream, and every row on
// the shard its key belongs to.
func TestRouterShardedCopyMatchesOneNode(t *testing.T) {
	// Two-phase commit: a load that spans shards commits on all or none.
	s := startScatterStackWith(t, []string{"-c", "max_prepared_transactions=20"})
	ctx := context.Background()
	oracle := s.appConn(t, s.oracleDSN)
	conn := s.connect(t)

	// Escapes, NULLs in every column but the key, an empty string, and the
	// end-of-data marker with a line after it that must be ignored.
	var in strings.Builder
	for i := 0; i < 3000; i++ {
		tenant := int64(i%97 - 20)
		sku := fmt.Sprintf("sku-%d", i)
		switch i % 7 {
		case 0:
			sku = `\N`
		case 1:
			sku = `tab\there`
		case 2:
			sku = `back\\slash`
		case 3:
			sku = ""
		case 4:
			sku = `new\nline`
		case 5:
			// A backslash before a RAW newline keeps it in the value: the
			// row does not end there.
			sku = "raw\\\nnewline"
		case 6:
			sku = `oct\101hex\x42`
		}
		units := fmt.Sprint(i % 13)
		if i%11 == 0 {
			units = `\N`
		}
		fmt.Fprintf(&in, "%d\t%d\t%d\t%s\t%s\n", tenant, i, i%5, sku, units)
	}
	in.WriteString("\\.\n")
	data := in.String()
	const copySQL = "copy event_lines (tenant_id, id, line, sku, units) from stdin"

	if _, err := oracle.PgConn().CopyFrom(ctx, strings.NewReader(data), copySQL); err != nil {
		t.Fatalf("oracle: %v", err)
	}
	// The router routes a COPY once the controller has recorded the key
	// column's type, which happens shortly after the stack starts.
	deadline := time.Now().Add(60 * time.Second)
	for {
		tag, err := conn.PgConn().CopyFrom(ctx, strings.NewReader(data), copySQL)
		if err == nil {
			if tag.RowsAffected() != 3000 {
				t.Fatalf("router COPY tag %q, want 3000 rows", tag)
			}
			break
		}
		if !strings.Contains(err.Error(), "has been inspected") || time.Now().After(deadline) {
			t.Fatalf("router COPY: %v\nrouter log:\n%s", err, s.routerLog.String())
		}
		time.Sleep(300 * time.Millisecond)
	}

	const all = `select tenant_id, id, line, coalesce(sku, '<null>'), coalesce(units::text, '<null>') from event_lines order by tenant_id, id, line`
	want := resultOf(t, oracle, all, pgx.QueryExecModeSimpleProtocol)
	got := resultOf(t, conn, all, pgx.QueryExecModeSimpleProtocol)
	if strings.Join(got, "\n") != strings.Join(want, "\n") {
		t.Fatalf("router and oracle differ (%d vs %d rows)\nrouter: %s\noracle: %s", len(got), len(want), firstDiff(got, want), firstDiff(want, got))
	}
	// Placement: every shard holds only the keys it owns.
	for i, dsn := range s.shardDSNs {
		shard := s.appConn(t, dsn)
		rows, err := shard.Query(ctx, "select distinct tenant_id from event_lines")
		if err != nil {
			t.Fatal(err)
		}
		for rows.Next() {
			var k int64
			if err := rows.Scan(&k); err != nil {
				t.Fatal(err)
			}
			if owner := s.shardOf(t, k); owner != i {
				t.Fatalf("key %d stored on shard %d, owned by shard %d", k, i, owner)
			}
		}
		rows.Close()
	}

	// A load with a row that cannot be placed leaves nothing behind.
	var before int64
	if err := conn.QueryRow(ctx, "select count(*) from event_lines", pgx.QueryExecModeSimpleProtocol).Scan(&before); err != nil {
		t.Fatal(err)
	}
	if _, err := conn.PgConn().CopyFrom(ctx, strings.NewReader("1\t90001\t0\tx\t1\nnot-a-key\t90002\t0\tx\t1\n"), copySQL); err == nil {
		t.Fatal("a load with an unreadable key succeeded")
	}
	var after int64
	if err := conn.QueryRow(ctx, "select count(*) from event_lines", pgx.QueryExecModeSimpleProtocol).Scan(&after); err != nil {
		t.Fatal(err)
	}
	if after != before {
		t.Fatalf("a failed load left %d rows behind", after-before)
	}

	// A stream whose rows end in a bare carriage return, which PostgreSQL
	// accepts, loads the same through the router as into one node.
	var cr strings.Builder
	for i := 0; i < 200; i++ {
		fmt.Fprintf(&cr, "%d\t%d\t9\tcr-%d\t%d\r", int64(i%31-7), 100000+i, i, i)
	}
	if _, err := oracle.PgConn().CopyFrom(ctx, strings.NewReader(cr.String()), copySQL); err != nil {
		t.Fatalf("oracle CR load: %v", err)
	}
	if _, err := conn.PgConn().CopyFrom(ctx, strings.NewReader(cr.String()), copySQL); err != nil {
		t.Fatalf("router CR load: %v", err)
	}
	want = resultOf(t, oracle, all, pgx.QueryExecModeSimpleProtocol)
	got = resultOf(t, conn, all, pgx.QueryExecModeSimpleProtocol)
	if strings.Join(got, "\n") != strings.Join(want, "\n") {
		t.Fatalf("after the CR load router and oracle differ (%d vs %d rows)\nrouter: %s\noracle: %s", len(got), len(want), firstDiff(got, want), firstDiff(want, got))
	}
}
