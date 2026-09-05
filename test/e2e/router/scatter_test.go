//go:build integration

package router

import (
	"context"
	"fmt"
	"math/rand"
	"net"
	"os/exec"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/andrew01234567890/pgshard/internal/placement"
	"github.com/andrew01234567890/pgshard/internal/router"
)

// scatterStack is a stack with three shards and an oracle PostgreSQL that
// holds every row of the sharded table.
type scatterStack struct {
	*stack
	shardDSNs []string
	oracleDSN string
	ranges    placement.RangeSet
}

const scatterTable = `create table events (
	tenant_id int8 not null, id int not null, amount numeric(12,3), price float8, qty int2,
	name text, ts timestamptz, d date, ok bool, u uuid, region int4, primary key (tenant_id, id))`

// event_lines is sharded on the same key as events, so a join between them
// on that key matches only rows that already live on the same shard.
// regions is a reference table, present in full on every shard.
const joinTables = `create table event_lines (
	tenant_id int8 not null, id int not null, line int4 not null, sku text, units int4,
	primary key (tenant_id, id, line));
create table regions (id int4 primary key, name text not null)`

func startScatterStack(tb testing.TB) *scatterStack {
	tb.Helper()
	s := &scatterStack{stack: startStack(tb)}
	s.shardDSNs = []string{s.shardDSN}
	poolerBin, _ := buildBinaries(tb)
	for id := int32(1); id <= 2; id++ {
		addr, dsn := startPostgres(tb, fmt.Sprintf("shard%d", id))
		s.shardDSNs = append(s.shardDSNs, dsn)
		// Also on the embedded stack, whose map is what the controller is
		// started with. Without it the controller knows shard 0 only, and
		// anything that has to ask every shard -- the shard-key check, the
		// resolver -- fails on the two it cannot dial and retries for ever.
		s.stack.shardDSNs[int(id)] = dsn
		pooler := fmt.Sprintf("127.0.0.1:%d", freePort(tb))
		err := router.DevBootstrap{CatalogDSN: s.catalogDSN, ShardDSN: dsn, ShardID: id, Database: appDatabase, Role: appRole,
			Password: appPassword, PoolerEndpoint: pooler, Epoch: 1}.Run(context.Background())
		if err != nil {
			tb.Fatalf("bootstrap shard %d: %v", id, err)
		}
		host, port, _ := net.SplitHostPort(addr)
		startProcess(tb, &logBuffer{}, "listening on", poolerBin, "run", "--insecure-dev", "--listen", pooler,
			"--pg-host", host, "--pg-port", port, "--pg-database", appDatabase,
			"--catalog-dsn", s.catalogDSN, "--shard-set", router.DefaultShardSet, "--shard-id", fmt.Sprint(id), "--drain-timeout", "5s")
	}
	_, s.oracleDSN = startPostgres(tb, "oracle")
	ranges, err := placement.Split(3)
	if err != nil {
		tb.Fatal(err)
	}
	s.ranges = ranges
	ctx := context.Background()
	cat, err := pgx.Connect(ctx, s.catalogDSN)
	if err != nil {
		tb.Fatal(err)
	}
	defer func() { _ = cat.Close(ctx) }()
	tx, err := cat.Begin(ctx)
	if err != nil {
		tb.Fatal(err)
	}
	for i, r := range ranges {
		lo, hi := fmt.Sprint(r.Start), ""
		if i == 0 {
			lo = ""
		}
		if i < len(ranges)-1 {
			hi = fmt.Sprint(r.End + 1)
		}
		if _, err := tx.Exec(ctx, `INSERT INTO pgshard.shard_ranges (shard_set, shard_id, range) VALUES ('default', $1, $2::int8range)`, i, "["+lo+","+hi+")"); err != nil {
			tb.Fatalf("shard range %d: %v", i, err)
		}
	}
	// Restart it now that every shard is registered: startStack started one
	// knowing shard 0 only.
	s.startController(tb)
	for _, sql := range []string{
		`INSERT INTO pgshard.tables (database, schema_name, table_name, placement, shard_key) VALUES ('app', 'public', 'events', 'sharded', 'tenant_id')`,
		`INSERT INTO pgshard.table_status (database, schema_name, table_name, effective_placement, effective_shard_key) VALUES ('app', 'public', 'events', 'sharded', 'tenant_id')`,
		`INSERT INTO pgshard.tables (database, schema_name, table_name, placement, shard_key) VALUES ('app', 'public', 'event_lines', 'sharded', 'tenant_id')`,
		`INSERT INTO pgshard.table_status (database, schema_name, table_name, effective_placement, effective_shard_key) VALUES ('app', 'public', 'event_lines', 'sharded', 'tenant_id')`,
		`INSERT INTO pgshard.tables (database, schema_name, table_name, placement) VALUES ('app', 'public', 'regions', 'reference')`,
		`INSERT INTO pgshard.table_status (database, schema_name, table_name, effective_placement) VALUES ('app', 'public', 'regions', 'reference')`,
	} {
		if _, err := tx.Exec(ctx, sql); err != nil {
			tb.Fatalf("%s: %v", sql, err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		tb.Fatal(err)
	}
	for _, dsn := range s.shardDSNs {
		app := s.appConn(tb, dsn)
		if _, err := app.Exec(ctx, scatterTable+"; "+joinTables+"; grant all on events, event_lines, regions to "+appRole); err != nil {
			tb.Fatal(err)
		}
	}
	admin, err := pgx.Connect(ctx, s.oracleDSN)
	if err != nil {
		tb.Fatal(err)
	}
	if _, err := admin.Exec(ctx, "create database "+appDatabase); err != nil {
		tb.Fatal(err)
	}
	_ = admin.Close(ctx)
	if _, err := s.appConn(tb, s.oracleDSN).Exec(ctx, scatterTable+"; "+joinTables); err != nil {
		tb.Fatal(err)
	}
	return s
}

func (s *scatterStack) appConn(tb testing.TB, adminDSN string) *pgx.Conn {
	tb.Helper()
	conn, err := pgx.Connect(context.Background(), strings.Replace(adminDSN, "/postgres?", "/"+appDatabase+"?", 1))
	if err != nil {
		tb.Fatal(err)
	}
	tb.Cleanup(func() { _ = conn.Close(context.Background()) })
	return conn
}

func (s *scatterStack) shardOf(tb testing.TB, tenant int64) int {
	id, err := placement.KeyspaceID(tenant)
	if err != nil {
		tb.Fatal(err)
	}
	for i, r := range s.ranges {
		if r.Contains(id) {
			return i
		}
	}
	tb.Fatalf("keyspace id %d outside every range", id)
	return -1
}

// loadRows generates n deterministic rows with NULLs and duplicates, loads
// them all into the oracle and each into its shard.
func (s *scatterStack) loadRows(tb testing.TB, n int) {
	tb.Helper()
	rng := rand.New(rand.NewSource(42))
	cols := []string{"tenant_id", "id", "amount", "price", "qty", "name", "ts", "d", "ok", "u", "region"}
	perShard := make([][][]any, len(s.shardDSNs))
	var all [][]any
	lineCols := []string{"tenant_id", "id", "line", "sku", "units"}
	linesPerShard := make([][][]any, len(s.shardDSNs))
	var allLines [][]any
	names := []string{"alpha", "Bravo", "charlie", "delta", "Echo", "foxtrot", "golf", "hotel", "india", "Juliet", "kilo", "lima"}
	base := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	for i := 0; i < n; i++ {
		tenant := int64(rng.Intn(400)) - 50
		row := []any{tenant, i, nil, nil, nil, nil, nil, nil, nil, nil, nil}
		if rng.Intn(10) != 0 {
			row[2] = fmt.Sprintf("%d.%03d", rng.Intn(20000)-10000, rng.Intn(1000))
		}
		if rng.Intn(10) != 0 {
			row[3] = float64(rng.Intn(1000000))/128 - 3000
		}
		if rng.Intn(10) != 0 {
			row[4] = int16(rng.Intn(200) - 100)
		}
		if rng.Intn(10) != 0 {
			row[5] = names[rng.Intn(len(names))]
		}
		if rng.Intn(10) != 0 {
			row[6] = base.Add(time.Duration(rng.Intn(1<<31)) * time.Second)
		}
		if rng.Intn(10) != 0 {
			row[7] = base.AddDate(0, 0, rng.Intn(3000)-1000)
		}
		if rng.Intn(10) != 0 {
			row[8] = rng.Intn(2) == 0
		}
		if rng.Intn(10) != 0 {
			row[9] = fmt.Sprintf("%08x-%04x-4%03x-8%03x-%012x", rng.Uint32(), rng.Intn(1<<16), rng.Intn(1<<12), rng.Intn(1<<12), rng.Int63n(1<<48))
		}
		// A tenth of the rows have no region, so an outer join to the
		// reference table has unmatched rows to preserve.
		if rng.Intn(10) != 0 {
			row[10] = int32(rng.Intn(len(regionNames)))
		}
		all = append(all, row)
		sh := s.shardOf(tb, tenant)
		perShard[sh] = append(perShard[sh], row)
		// Zero, one or two lines per event, so a join drops rows, keeps
		// them and multiplies them within the same query.
		for line := 0; line < rng.Intn(3); line++ {
			l := []any{tenant, i, line, names[rng.Intn(len(names))], rng.Intn(50)}
			allLines = append(allLines, l)
			linesPerShard[sh] = append(linesPerShard[sh], l)
		}
	}
	load := func(dsn, table string, columns []string, rows [][]any) {
		conn := s.appConn(tb, dsn)
		if _, err := conn.CopyFrom(context.Background(), pgx.Identifier{table}, columns, pgx.CopyFromRows(rows)); err != nil {
			tb.Fatalf("load %s into %s: %v", dsn, table, err)
		}
	}
	regionCols := []string{"id", "name"}
	var regions [][]any
	for i, n := range regionNames {
		regions = append(regions, []any{int32(i), n})
	}
	load(s.oracleDSN, "events", cols, all)
	load(s.oracleDSN, "event_lines", lineCols, allLines)
	// The reference table is loaded whole everywhere, which is what makes
	// it join in place on each shard.
	load(s.oracleDSN, "regions", regionCols, regions)
	for i, rows := range perShard {
		if len(rows) == 0 {
			tb.Fatalf("shard %d received no rows; the fixture must cover every shard", i)
		}
		load(s.shardDSNs[i], "events", cols, rows)
		load(s.shardDSNs[i], "event_lines", lineCols, linesPerShard[i])
		load(s.shardDSNs[i], "regions", regionCols, regions)
	}
}

var regionNames = []string{"north", "south", "east", "west", "central"}

// resultOf runs sql and renders every row as text, NULL as "NULL".
func resultOf(tb testing.TB, conn *pgx.Conn, sql string, mode pgx.QueryExecMode) []string {
	tb.Helper()
	rows, err := conn.Query(context.Background(), sql, mode)
	if err != nil {
		tb.Fatalf("%s: %v", sql, err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		vals, err := rows.Values()
		if err != nil {
			tb.Fatal(err)
		}
		parts := make([]string, len(vals))
		for i, v := range vals {
			switch x := v.(type) {
			case nil:
				parts[i] = "NULL"
			case time.Time:
				parts[i] = x.UTC().Format(time.RFC3339Nano)
			case [16]byte:
				parts[i] = fmt.Sprintf("%x", x)
			default:
				parts[i] = fmt.Sprint(v)
			}
		}
		out = append(out, strings.Join(parts, "|"))
	}
	if rows.Err() != nil {
		tb.Fatalf("%s: %v", sql, rows.Err())
	}
	return out
}

type corpusQuery struct {
	sql     string
	ordered bool
}

var scatterCorpus = []corpusQuery{
	{`select tenant_id, id, amount, price, qty, name, ts, d, ok, u from events`, false},
	{`select id, name from events where qty > 50`, false},
	{`select id from events where name is null`, false},
	{`select tenant_id, id from events order by id`, true},
	{`select id, amount from events order by amount, id`, true},
	{`select id, amount from events order by amount desc, id`, true},
	{`select id, amount from events order by amount nulls first, id`, true},
	{`select id, amount from events order by amount desc nulls last, id desc`, true},
	{`select id, price from events order by price, id`, true},
	{`select id, price from events order by price desc nulls first, id`, true},
	{`select id, qty from events order by qty nulls first, id`, true},
	{`select id, name from events order by name collate "C", id`, true},
	{`select id, name from events order by name collate "C" desc nulls last, id`, true},
	{`select id, ts from events order by ts, id`, true},
	{`select id, ts from events order by ts desc nulls last, id`, true},
	{`select id, d from events order by d, id`, true},
	{`select id, ok from events order by ok, id`, true},
	{`select id, ok from events order by ok desc nulls first, id desc`, true},
	{`select id, u from events order by u, id`, true},
	{`select id, u from events order by u desc, id`, true},
	{`select id from events order by qty desc nulls last, amount, id`, true},
	{`select id from events order by 1 desc`, true},
	{`select id as k, qty from events order by k limit 100`, true},
	{`select id from events order by id limit 10`, true},
	{`select id from events order by id limit 10 offset 4990`, true},
	{`select id from events order by id limit 0`, true},
	{`select id from events order by id offset 4995`, true},
	{`select id from events order by id desc limit 7 offset 3`, true},
	{`select id, amount from events order by amount desc nulls last, id limit 25 offset 25`, true},
	{`select id, price from events order by price, id limit 3`, true},
	{`select id from events order by ts desc nulls last, id limit 50 offset 10`, true},
	{`select tenant_id, id from events where qty < 0 order by tenant_id, id`, true},
	{`select tenant_id, id from events where tenant_id in (1, 2, 3, 4, 5, 6, 7, 8, 9, 10) order by id`, true},
	{`select id from events where id < 100 limit 100`, false},
	{`select count(*) from events`, true},
	{`select count(*), count(amount), count(name) from events`, true},
	{`select sum(qty), sum(id) from events`, true},
	{`select sum(amount) from events`, true},
	{`select min(amount), max(amount) from events`, true},
	{`select min(id), max(id), min(qty), max(qty) from events`, true},
	{`select min(ts), max(ts), min(d), max(d) from events`, true},
	{`select min(price), max(price), min(d), max(d) from events`, true},
	{`select count(*) from events where qty > 1000`, true},
	{`select sum(amount), max(amount) from events where qty > 1000`, true},
	{`select count(*), sum(qty) from events where tenant_id in (1, 2, 3, 4, 5, 6, 7, 8, 9, 10)`, true},
	{`select tenant_id, count(*), sum(qty) from events group by tenant_id order by tenant_id`, true},
	{`select tenant_id, max(id) from events group by tenant_id having count(*) > 12 order by 2 desc limit 20`, true},
	{`select distinct tenant_id, ok from events order by tenant_id, ok`, true},
	{`select distinct on (tenant_id) tenant_id, id from events order by tenant_id, id desc`, true},
	{`select tenant_id, avg(qty)::text from events group by tenant_id order by 1 limit 15`, true},
	// Two client columns ordered by a third: the shard statement carries
	// a hidden sort column, so the client's per-column result formats no
	// longer match its column count. Every other ordered query here asks
	// for one column or sorts by one it selects, which is why nothing
	// caught this.
	{`select id, qty from events order by price, id`, true},
}

// joinCorpus is PGS-614: joins whose sharded relations are joined on the
// shard key, plus reference-table joins. Every one of them must produce
// exactly what one PostgreSQL holding all the rows produces.
//
// The reference table is on the null-supplying side throughout. Preserved
// by an outer join it would be planned differently -- a reference row that
// matches nothing is emitted NULL-extended by every shard -- and the
// refusal for that shape is asserted in the plan tests.
var joinCorpus = []corpusQuery{
	{`select count(*) from events e join event_lines l on e.tenant_id = l.tenant_id and e.id = l.id`, true},
	{`select e.id, l.line, l.units from events e join event_lines l on e.tenant_id = l.tenant_id and e.id = l.id order by e.id, l.line`, true},
	{`select e.id, l.line from events e left join event_lines l on e.tenant_id = l.tenant_id and e.id = l.id order by e.id, l.line nulls first`, true},
	{`select e.id, l.line from events e right join event_lines l on e.tenant_id = l.tenant_id and e.id = l.id order by e.id, l.line`, true},
	{`select count(*) from events e full join event_lines l on e.tenant_id = l.tenant_id and e.id = l.id`, true},
	{`select e.id, l.line from events e join event_lines l using (tenant_id) where l.line = 0 order by e.id, l.id`, true},
	{`select count(*), sum(l.units) from events e join event_lines l on e.tenant_id = l.tenant_id and e.id = l.id where e.qty > 0`, true},
	{`select e.tenant_id, count(*) from events e join event_lines l on e.tenant_id = l.tenant_id and e.id = l.id group by e.tenant_id order by 1 limit 20`, true},
	{`select e.id from events e join event_lines l on e.tenant_id = l.tenant_id and e.id = l.id order by e.id limit 30 offset 15`, true},
	{`select distinct e.tenant_id from events e join event_lines l on e.tenant_id = l.tenant_id and e.id = l.id order by 1 limit 25`, true},
	// A sharded table joined to a reference table: the reference side is
	// complete on every shard, so each shard answers for its own rows.
	{`select count(*) from events e join regions r on r.id = e.region`, true},
	{`select e.id, r.name from events e join regions r on r.id = e.region order by e.id limit 40`, true},
	{`select e.id, r.name from events e left join regions r on r.id = e.region order by e.id limit 40`, true},
	// Three relations at once, two sharded and one reference.
	{`select count(*) from events e join event_lines l on e.tenant_id = l.tenant_id and e.id = l.id join regions r on r.id = e.region`, true},
}

func TestRouterScatterDifferential(t *testing.T) {
	s := startScatterStack(t)
	s.loadRows(t, 5000)
	ctx := context.Background()
	oracle := s.appConn(t, s.oracleDSN)
	conn := s.connect(t)
	deadline := time.Now().Add(30 * time.Second)
	for {
		var n int64
		err := conn.QueryRow(ctx, "select count(*) from events", pgx.QueryExecModeSimpleProtocol).Scan(&n)
		if err == nil && n == 5000 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("router never served the sharded count (last: %d %v)\nrouter log:\n%s", n, err, s.routerLog.String())
		}
		time.Sleep(200 * time.Millisecond)
	}
	// And the join separately. Serving `events` says the controller
	// inspected THAT table's shard key; a join is only plannable once it
	// has inspected both, because the two key types have to be compared.
	// Waiting on one and asking about two is a race the corpus loses about
	// as often as it wins.
	deadline = time.Now().Add(60 * time.Second)
	for {
		var n int64
		err := conn.QueryRow(ctx, "select count(*) from events e join event_lines l on e.tenant_id = l.tenant_id and e.id = l.id",
			pgx.QueryExecModeSimpleProtocol).Scan(&n)
		if err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("router never planned the colocated join (%v)\nrouter log:\n%s", err, s.routerLog.String())
		}
		time.Sleep(200 * time.Millisecond)
	}
	if len(scatterCorpus) < 40 {
		t.Fatalf("corpus has %d queries, want at least 40", len(scatterCorpus))
	}
	for _, q := range append(append([]corpusQuery(nil), scatterCorpus...), joinCorpus...) {
		want := resultOf(t, oracle, q.sql, pgx.QueryExecModeSimpleProtocol)
		for _, mode := range []pgx.QueryExecMode{pgx.QueryExecModeSimpleProtocol, pgx.QueryExecModeCacheStatement} {
			t.Run(fmt.Sprintf("%s/%v", q.sql, mode), func(t *testing.T) {
				got := resultOf(t, conn, q.sql, mode)
				g, w := append([]string(nil), got...), append([]string(nil), want...)
				if !q.ordered {
					sort.Strings(g)
					sort.Strings(w)
				}
				if strings.Join(g, "\n") != strings.Join(w, "\n") {
					t.Fatalf("router and oracle differ (%d vs %d rows)\nrouter: %s\noracle: %s", len(g), len(w), firstDiff(g, w), firstDiff(w, g))
				}
			})
		}
	}

	t.Run("cancel_reaches_every_shard", func(t *testing.T) {
		before := s.canceledCount(t)
		go func() {
			time.Sleep(1500 * time.Millisecond)
			_ = conn.PgConn().CancelRequest(ctx)
		}()
		start := time.Now()
		_, err := conn.Exec(ctx, "select count(*) from events where pg_sleep(20) is not null", pgx.QueryExecModeSimpleProtocol)
		if sqlstate(err) != "57014" || time.Since(start) > 15*time.Second {
			t.Fatalf("cancel: %v after %s", err, time.Since(start))
		}
		deadline := time.Now().Add(10 * time.Second)
		for s.canceledCount(t)-before < len(s.shardDSNs) {
			if time.Now().After(deadline) {
				t.Fatalf("only %d shards reported a cancel", s.canceledCount(t)-before)
			}
			time.Sleep(200 * time.Millisecond)
		}
		var n int64
		if err := conn.QueryRow(ctx, "select count(*) from events where id < 3", pgx.QueryExecModeSimpleProtocol).Scan(&n); err != nil || n != 3 {
			t.Fatalf("session after cancel: %d %v", n, err)
		}
	})

	t.Run("refusals", func(t *testing.T) {
		for _, c := range []struct{ sql, msg string }{
			{`select id, name from events order by name`, `multi-shard ORDER BY on a text column needs an explicit COLLATE "C"`},
			{`select avg(qty) from events`, "multi-shard avg() is not available yet"},
			{`select max(name) from events`, "multi-shard min()/max() over a text column is not available yet"},
			{`select id from events order by id limit $1`, "multi-shard LIMIT must be an integer constant"},
			{`select ok, count(*) from events group by ok`, "multi-shard GROUP BY without the shard key"},
			// A colocated join is planned per shard; what it may compute
			// there is unchanged. Grouping by a reference table's column
			// spreads one group over every shard, so it is refused for the
			// same reason grouping by any non-key column is.
			{`select r.name, count(*) from events e join regions r on r.id = e.region group by r.name`, "multi-shard GROUP BY without the shard key"},
			{`select e.id from events e join event_lines l on e.id = l.id`, "cross-shard join is not available yet"},
			{`select r.name from regions r left join events e on r.id = e.region`, "cross-shard join is not available yet"},
		} {
			_, err := conn.Exec(ctx, c.sql, pgx.QueryExecModeSimpleProtocol)
			if sqlstate(err) != "0A000" || !strings.Contains(err.Error(), c.msg) {
				t.Errorf("%s: got %v, want 0A000 %q", c.sql, err, c.msg)
			}
		}
	})
}

// canceledCount counts the "canceling statement due to user request"
// errors in every shard's PostgreSQL log.
func (s *scatterStack) canceledCount(tb testing.TB) int {
	tb.Helper()
	total := 0
	for _, dsn := range s.shardDSNs {
		cname := containerAt(tb, dsn)
		out, err := exec.Command("docker", "logs", cname).CombinedOutput()
		if err != nil {
			tb.Fatalf("docker logs %s: %v: %s", cname, err, out)
		}
		total += strings.Count(string(out), "canceling statement due to user request")
	}
	return total
}

func firstDiff(a, b []string) string {
	for i := range a {
		if i >= len(b) || a[i] != b[i] {
			return fmt.Sprintf("row %d: %s", i, a[i])
		}
	}
	return "(prefix equal)"
}
