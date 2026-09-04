package pooler

import (
	"context"
	"fmt"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	pgshardv1 "github.com/andrew01234567890/pgshard/internal/gen/pgshard/v1"
)

// copied is what one CopyTables call delivered.
type copied struct {
	snapshot  *pgshardv1.CopyTablesResponse_Snapshot
	tables    []string
	byCtid    map[string]bool
	rows      map[string][]string
	batches   map[string][][]byte
	completed []string
	done      bool
}

func collectCopy(t *testing.T, st pgshardv1.Pooler_CopyTablesClient, stopAfterBatches int) *copied {
	t.Helper()
	c := &copied{byCtid: map[string]bool{}, rows: map[string][]string{}, batches: map[string][][]byte{}}
	cur := ""
	n := 0
	for {
		m, err := st.Recv()
		if err != nil {
			t.Fatalf("recv: %v", err)
		}
		switch r := m.GetResponse().(type) {
		case *pgshardv1.CopyTablesResponse_Snapshot_:
			c.snapshot = r.Snapshot
		case *pgshardv1.CopyTablesResponse_TableBegin_:
			cur = r.TableBegin.GetRelation().GetSchema() + "." + r.TableBegin.GetRelation().GetTable()
			c.tables = append(c.tables, cur)
			c.byCtid[cur] = r.TableBegin.GetByCtid()
		case *pgshardv1.CopyTablesResponse_Rows_:
			for _, row := range r.Rows.GetRows() {
				var vals []string
				for _, v := range row.GetValues() {
					if v.GetNull() {
						vals = append(vals, "NULL")
					} else {
						vals = append(vals, string(v.GetData()))
					}
				}
				c.rows[cur] = append(c.rows[cur], strings.Join(vals, "|"))
			}
			c.batches[cur] = append(c.batches[cur], r.Rows.GetLastpk())
			n++
			if stopAfterBatches > 0 && n >= stopAfterBatches {
				return c
			}
		case *pgshardv1.CopyTablesResponse_TableDone_:
			c.completed = append(c.completed, r.TableDone.GetSchema()+"."+r.TableDone.GetTable())
		case *pgshardv1.CopyTablesResponse_Done_:
			c.done = true
			return c
		}
	}
}

// testCopyWideRowsChunksOnBytes: a page of the initial copy went out as one
// message bounded by the ROW COUNT alone. A row is anything from a few
// bytes to a megabyte, so a page of wide rows built a message past the
// gRPC limit both sides enforce, and the copy failed on a table whose only
// fault was wide columns.
func (h *pgHarness) testCopyWideRowsChunksOnBytes(t *testing.T) {
	ctx := context.Background()
	for _, sql := range []string{
		"CREATE TABLE wide (id int primary key, body text)",
		// 40 rows of 256 KiB is 10 MiB in one page at BatchRows: 100 --
		// comfortably past the 4 MiB message limit, and the point is that
		// it now arrives rather than failing.
		"INSERT INTO wide SELECT g, repeat('x', 256 * 1024) FROM generate_series(1, 40) g",
	} {
		if _, err := h.admin.Exec(ctx, sql); err != nil {
			t.Fatalf("%s: %v", sql, err)
		}
	}
	t.Cleanup(func() {
		for _, sql := range []string{"SELECT pg_drop_replication_slot('pgshard_wide_shard0')", "DROP TABLE wide", "DROP PUBLICATION pgshard_wide_all"} {
			_, _ = h.admin.Exec(ctx, sql)
		}
	})
	if _, err := h.admin.Exec(ctx, "CREATE PUBLICATION pgshard_wide_all FOR TABLE wide"); err != nil {
		t.Fatal(err)
	}

	st, err := h.client.CopyTables(ctx, &pgshardv1.CopyTablesRequest{Stream: "wide", BatchRows: 100, Publication: "pgshard_wide_all"})
	if err != nil {
		t.Fatal(err)
	}
	got := collectCopy(t, st, 0)
	if n := len(got.rows["public.wide"]); n != 40 {
		t.Fatalf("copied %d rows of 40", n)
	}
	// One page, so one message before the byte bound. Several after it,
	// and every one of them carries a resume point: a chunk that was
	// delivered is a chunk the copy does not have to send again.
	batches := got.batches["public.wide"]
	if len(batches) < 4 {
		t.Fatalf("40 wide rows went out in %d messages; the byte bound did not split the page", len(batches))
	}
	for i, b := range batches {
		if len(b) == 0 {
			t.Fatalf("chunk %d carries no resume point", i)
		}
	}
	// The resume points advance: each names its own chunk's last row, not
	// the page's.
	if string(batches[0]) == string(batches[len(batches)-1]) {
		t.Fatalf("every chunk reported the same resume point %q", batches[0])
	}
}

func (h *pgHarness) testCopyTables(t *testing.T) {
	ctx := context.Background()
	for _, sql := range []string{
		"CREATE PUBLICATION pgshard_all FOR ALL TABLES",
		"CREATE TABLE citems (id int primary key, name text)",
		"CREATE TABLE pairs (a int, b text, primary key (b, a))",
		"CREATE TABLE nopk (x int, y text)",
		"INSERT INTO citems SELECT g, 'item ' || g FROM generate_series(1, 25) g",
		"INSERT INTO pairs SELECT g / 3, 'k' || (g % 3) FROM generate_series(1, 10) g",
		"INSERT INTO nopk SELECT g, CASE WHEN g % 2 = 0 THEN NULL ELSE 'n' || g END FROM generate_series(1, 7) g",
	} {
		if _, err := h.admin.Exec(ctx, sql); err != nil {
			t.Fatalf("%s: %v", sql, err)
		}
	}
	t.Cleanup(func() {
		for _, sql := range []string{"SELECT pg_drop_replication_slot('pgshard_copyt_shard0')", "DROP TABLE citems, pairs, nopk", "DROP PUBLICATION pgshard_all"} {
			_, _ = h.admin.Exec(ctx, sql)
		}
	})

	if st, err := h.client.CopyTables(ctx, &pgshardv1.CopyTablesRequest{Stream: "bad name"}); err == nil {
		if _, err := st.Recv(); err == nil {
			t.Fatal("invalid stream name accepted")
		}
	}

	st, err := h.client.CopyTables(ctx, &pgshardv1.CopyTablesRequest{Stream: "copyt", BatchRows: 10})
	if err != nil {
		t.Fatal(err)
	}
	full := collectCopy(t, st, 0)
	if full.snapshot == nil || !full.snapshot.GetStreamSlot() || full.snapshot.GetSlot() != "pgshard_copyt_shard0" || full.snapshot.GetConsistentPoint() == 0 || full.snapshot.GetSnapshotName() == "" {
		t.Fatalf("snapshot: %v", full.snapshot)
	}
	if want := []string{"public.citems", "public.items", "public.nopk", "public.pairs", "public.secret_t"}; strings.Join(full.tables, ",") != strings.Join(want, ",") || strings.Join(full.completed, ",") != strings.Join(want, ",") || !full.done {
		t.Fatalf("tables %v completed %v done %v", full.tables, full.completed, full.done)
	}
	if len(full.rows["public.citems"]) != 25 || full.rows["public.citems"][0] != "1|item 1" || full.rows["public.citems"][24] != "25|item 25" {
		t.Fatalf("items: %v", full.rows["public.citems"])
	}
	if b := full.batches["public.citems"]; len(b) != 3 || string(b[0]) != `["10"]` || string(b[1]) != `["20"]` || string(b[2]) != `["25"]` {
		t.Fatalf("items batches: %q", b)
	}
	if r := full.rows["public.pairs"]; len(r) != 10 || r[0] != "1|k0" || r[1] != "2|k0" || r[2] != "3|k0" || r[3] != "0|k1" || r[9] != "2|k2" {
		t.Fatalf("pairs: %v", r)
	}
	if b := full.batches["public.pairs"]; len(b) != 1 || string(b[0]) != `["k2","2"]` {
		t.Fatalf("pairs batches: %q", b)
	}
	if r := full.rows["public.nopk"]; len(r) != 7 || r[0] != "1|n1" || r[1] != "2|NULL" || !full.byCtid["public.nopk"] || full.byCtid["public.pairs"] {
		t.Fatalf("nopk: %v by ctid %v", r, full.byCtid)
	}
	if b := full.batches["public.nopk"]; len(b) != 1 || !strings.HasPrefix(string(b[0]), `["(0,7)`) {
		t.Fatalf("nopk batches: %q", b)
	}
	// Rows committed after the snapshot are invisible to a copy that is
	// still running, and a resume skips done tables and rows at or below the
	// checkpoint.
	if _, err := h.admin.Exec(ctx, "INSERT INTO citems VALUES (26, 'late')"); err != nil {
		t.Fatal(err)
	}
	sctx, cancel := context.WithCancel(ctx)
	st, err = h.client.CopyTables(sctx, &pgshardv1.CopyTablesRequest{Stream: "copyt", BatchRows: 4})
	if err != nil {
		t.Fatal(err)
	}
	partial := collectCopy(t, st, 3)
	cancel()
	if partial.snapshot.GetStreamSlot() || !strings.HasPrefix(partial.snapshot.GetSlot(), "pgshard_copy_") || len(partial.rows["public.citems"]) != 12 {
		t.Fatalf("partial: %v %v", partial.snapshot, partial.rows["public.citems"])
	}
	st, err = h.client.CopyTables(ctx, &pgshardv1.CopyTablesRequest{Stream: "copyt", BatchRows: 4, ResumeSchema: "public", ResumeTable: "citems",
		ResumeLastpk: partial.batches["public.citems"][2], DoneTables: []string{"public.items", "public.secret_t", "public.nopk"}})
	if err != nil {
		t.Fatal(err)
	}
	resumed := collectCopy(t, st, 0)
	if got := resumed.rows["public.citems"]; len(got) != 14 || got[0] != "13|item 13" || got[13] != "26|late" {
		t.Fatalf("resumed items: %v", got)
	}
	if strings.Join(resumed.tables, ",") != "public.citems,public.pairs" || len(resumed.rows["public.pairs"]) != 10 || !resumed.done {
		t.Fatalf("resumed tables %v", resumed.tables)
	}
	// A no-PK table paginates on ctid, and a ctid does not survive a heap
	// rewrite: VACUUM FULL or CLUSTER between the snapshot a checkpoint
	// came from and this one can move an untouched row below it, and a
	// physical rewrite is not reported as row DML, so resuming there would
	// silently drop the row. The table starts again instead, whatever
	// checkpoint the caller kept -- the copy is at-least-once, so repeating
	// rows is within the contract and losing one is not.
	st, err = h.client.CopyTables(ctx, &pgshardv1.CopyTablesRequest{Stream: "copyt", BatchRows: 100, ResumeSchema: "public", ResumeTable: "nopk",
		ResumeLastpk: []byte(`["(0,3)"]`), DoneTables: []string{"public.citems", "public.items", "public.pairs", "public.secret_t"}})
	if err != nil {
		t.Fatal(err)
	}
	byCtid := collectCopy(t, st, 0)
	if got := byCtid.rows["public.nopk"]; len(got) != 7 || got[0] != "1|n1" {
		t.Fatalf("a no-PK table must restart rather than resume from a ctid: %v", got)
	}
	deadline := time.Now().Add(10 * time.Second)
	for {
		var slots int
		if err := h.admin.QueryRow(ctx, fmt.Sprintf("select count(*) from pg_replication_slots where slot_name like %s", "'pgshard\\_copy\\_%'")).Scan(&slots); err != nil {
			t.Fatal(err)
		}
		if slots == 0 {
			break
		}
		if time.Now().After(deadline) {
			var info string
			_ = h.admin.QueryRow(ctx, "select string_agg(slot_name || ' active=' || active || ' pid=' || coalesce(active_pid::text, '-') || ' temp=' || temporary, ', ') from pg_replication_slots").Scan(&info)
			t.Fatalf("temporary slots left: %d: %s", slots, info)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// BenchmarkCopyWideTable measures what an initial copy allocates per page
// of a wide table. It is the evidence PGS-214 asked for and PGS-626
// inherited: the change it justifies is allocation behaviour, not a visible
// failure, so without a number there is nothing to argue from.
//
// copyRows used to call ExecParams(...).Read(), which materialises the whole
// page before any of it is converted -- so at BatchRows rows of a wide table
// the page's bytes and the converted Values coexist. Iterating the reader
// instead makes what is held a function of the chunk rather than the page.
//
// Run it with -benchmem; B/op is the figure that moves.
func BenchmarkCopyWideTable(b *testing.B) {
	ctx := context.Background()
	if _, err := exec.LookPath("docker"); err != nil {
		b.Skip("docker not on PATH")
	}
	if err := exec.Command("docker", "info").Run(); err != nil {
		b.Skip("docker daemon unavailable")
	}
	_, dsn := startPostgres(b, pgImages[0].name)
	conn, err := pgconn.Connect(ctx, dsn)
	if err != nil {
		b.Skipf("postgres unavailable: %v", err)
	}
	defer func() { _ = conn.Close(ctx) }()
	for _, sql := range []string{
		"DROP TABLE IF EXISTS benchwide",
		"CREATE TABLE benchwide (id int primary key, body text)",
		"INSERT INTO benchwide SELECT g, repeat('x', 256 * 1024) FROM generate_series(1, 100) g",
	} {
		if err := conn.Exec(ctx, sql).Close(); err != nil {
			b.Fatalf("%s: %v", sql, err)
		}
	}
	tbl := copyTable{schema: "public", table: "benchwide", keyNames: []string{pgx.Identifier{"id"}.Sanitize()}, keyTypes: []string{"int4"},
		relation: &pgshardv1.ChangeEvent_Relation{Columns: []*pgshardv1.ChangeEvent_Relation_Column{{Name: "id"}, {Name: "body"}}}}
	sent := 0
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		if err := copyRows(ctx, conn, tbl, nil, 100, func(*pgshardv1.CopyTablesResponse) error {
			sent++
			return nil
		}); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	if sent == 0 {
		b.Fatal("the benchmark sent nothing; it is measuring the wrong thing")
	}
}
