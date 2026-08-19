//go:build integration

package router

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os/exec"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	pgshardv1 "github.com/andrew01234567890/pgshard/internal/gen/pgshard/v1"
	"github.com/andrew01234567890/pgshard/internal/router"
)

// vstreamStack adds a controller (stream creation) and a router serving
// VStream to the sharded stack.
type vstreamStack struct {
	*shardedStack
	client pgshardv1.VStreamClient
}

func startVStreamStack(tb testing.TB) *vstreamStack {
	tb.Helper()
	s := &vstreamStack{shardedStack: startShardedStackWith(tb, []string{preparedXacts}, []string{preparedXacts})}
	controllerAddr := fmt.Sprintf("127.0.0.1:%d", freePort(tb))
	startProcess(tb, &logBuffer{}, "listening on", s.controllerBin, "run", "--insecure-dev", "--listen", controllerAddr,
		"--catalog-dsn", s.catalogDSN, "--resolve-interval", "1h",
		"--shard-dsn", "default/0="+s.appDSN(0), "--shard-dsn", "default/1="+s.appDSN(1))
	vstreamAddr := fmt.Sprintf("127.0.0.1:%d", freePort(tb))
	s.startRouter(tb, 7, nil, "--vstream-listen", vstreamAddr, "--controller", controllerAddr)
	cc, err := grpc.NewClient(vstreamAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		tb.Fatal(err)
	}
	tb.Cleanup(func() { _ = cc.Close() })
	s.client = pgshardv1.NewVStreamClient(cc)
	return s
}

func (s *vstreamStack) slotRows(tb testing.TB, shard int) []string {
	tb.Helper()
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, s.appDSN(shard))
	if err != nil {
		tb.Fatal(err)
	}
	defer func() { _ = conn.Close(ctx) }()
	rows, err := conn.Query(ctx, "select slot_name || ' failover=' || failover || ' two_phase=' || two_phase from pg_replication_slots order by slot_name")
	if err != nil {
		tb.Fatal(err)
	}
	out, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		tb.Fatal(err)
	}
	return out
}

func (s *vstreamStack) confirmedFlush(tb testing.TB, shard int) uint64 {
	tb.Helper()
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, s.appDSN(shard))
	if err != nil {
		tb.Fatal(err)
	}
	defer func() { _ = conn.Close(ctx) }()
	var lsn int64
	if err := conn.QueryRow(ctx, "select confirmed_flush_lsn - '0/0'::pg_lsn from pg_replication_slots where slot_name like 'pgshard_orders_%'").Scan(&lsn); err != nil {
		tb.Fatal(err)
	}
	return uint64(lsn)
}

func (s *vstreamStack) walEnd(tb testing.TB, shard int) uint64 {
	tb.Helper()
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, s.appDSN(shard))
	if err != nil {
		tb.Fatal(err)
	}
	defer func() { _ = conn.Close(ctx) }()
	var lsn int64
	if err := conn.QueryRow(ctx, "select pg_current_wal_lsn() - '0/0'::pg_lsn").Scan(&lsn); err != nil {
		tb.Fatal(err)
	}
	return uint64(lsn)
}

// consumed is what one Stream call delivered: row ids per transaction in
// delivery order, the gids seen in Prepare/CommitPrepared events and the
// last VGtid.
type consumed struct {
	txns      [][]int
	prepared  []string
	committed []string
	last      *pgshardv1.VPosition
	shards    []string
}

// consume reads from the stream until stop returns true after a VGtid, or
// the stream ends.
func consume(tb testing.TB, st pgshardv1.VStream_StreamClient, stop func(c *consumed) bool) *consumed {
	tb.Helper()
	c := &consumed{}
	var cur []int
	var curShard string
	for {
		ev, err := st.Recv()
		if errors.Is(err, io.EOF) {
			return c
		}
		if err != nil {
			tb.Fatalf("recv: %v", err)
		}
		switch e := ev.GetEvent().(type) {
		case *pgshardv1.VEvent_Begin_:
			cur = []int{}
			curShard = fmt.Sprint(e.Begin.GetShard().GetShardId())
		case *pgshardv1.VEvent_Row_:
			if got := fmt.Sprint(e.Row.GetShard().GetShardId()); got != curShard {
				tb.Fatalf("row of shard %s inside a transaction of shard %s", got, curShard)
			}
			if e.Row.GetKind() != pgshardv1.VEvent_Row_KIND_INSERT {
				continue
			}
			id, err := strconv.Atoi(string(e.Row.GetNew().GetColumns()[1].GetValue()))
			if err != nil {
				tb.Fatal(err)
			}
			cur = append(cur, id)
		case *pgshardv1.VEvent_Commit_, *pgshardv1.VEvent_Prepare_:
			if cur == nil {
				tb.Fatalf("%T without begin", e)
			}
			c.txns = append(c.txns, cur)
			c.shards = append(c.shards, curShard)
			cur = nil
			if p, ok := e.(*pgshardv1.VEvent_Prepare_); ok {
				c.prepared = append(c.prepared, p.Prepare.GetGid())
			}
		case *pgshardv1.VEvent_CommitPrepared_:
			c.committed = append(c.committed, e.CommitPrepared.GetGid())
		case *pgshardv1.VEvent_Vgtid:
			c.last = e.Vgtid.GetPosition()
			if stop(c) {
				return c
			}
		case *pgshardv1.VEvent_Error_:
			tb.Fatalf("stream error: %v", e.Error)
		}
	}
}

func flatten(c *consumed) []int {
	var ids []int
	for _, tx := range c.txns {
		ids = append(ids, tx...)
	}
	return ids
}

func TestRouterVStream(t *testing.T) {
	s := startVStreamStack(t)
	ctx := context.Background()
	t0, t1 := twoTenants(t)
	conn := s.connect(t)
	s.awaitSharded(t, conn)

	created, err := s.client.Create(ctx, &pgshardv1.CreateVStreamRequest{Stream: "orders", Database: appDatabase, TwoPhase: true})
	if err != nil || created.GetError() != nil || len(created.GetSlots()) != 2 {
		t.Fatalf("create: %v %v", created, err)
	}
	for shard := range 2 {
		want := fmt.Sprintf("pgshard_orders_shard%d failover=true two_phase=true", shard)
		if got := s.slotRows(t, shard); len(got) != 1 || got[0] != want {
			t.Fatalf("shard %d slots = %v, want %q", shard, got, want)
		}
	}
	list, err := s.client.List(ctx, &pgshardv1.ListVStreamsRequest{})
	if err != nil || len(list.GetStreams()) != 1 || list.GetStreams()[0].GetState() != "active" || !list.GetStreams()[0].GetTwoPhase() {
		t.Fatalf("list: %v %v", list, err)
	}

	// Sequence oracle: ids 1..n, each in one transaction on one shard, plus
	// one cross-shard transaction committed through 2PC.
	const n = 40
	id := 0
	for id < n {
		id++
		tenant := t0
		if id%3 == 0 {
			tenant = t1
		}
		if _, err := conn.Exec(ctx, fmt.Sprintf("insert into orders (tenant_id, id) values (%d, %d)", tenant, id), pgx.QueryExecModeSimpleProtocol); err != nil {
			t.Fatal(err)
		}
	}
	tx, err := conn.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, fmt.Sprintf("insert into orders (tenant_id, id) values (%d, %d)", t0, n+1), pgx.QueryExecModeSimpleProtocol); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, fmt.Sprintf("insert into orders (tenant_id, id) values (%d, %d)", t1, n+2), pgx.QueryExecModeSimpleProtocol); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	total := n + 2

	open := func(pos *pgshardv1.VPosition) (pgshardv1.VStream_StreamClient, context.CancelFunc) {
		t.Helper()
		sctx, cancel := context.WithCancel(ctx)
		st, err := s.client.Stream(sctx)
		if err != nil {
			t.Fatal(err)
		}
		if err := st.Send(&pgshardv1.VStreamRequest{Request: &pgshardv1.VStreamRequest_Start_{Start: &pgshardv1.VStreamRequest_Start{
			Stream: "orders", Position: pos, Options: &pgshardv1.VStreamOptions{TwoPhase: true, HeartbeatIntervalMs: 500}}}}); err != nil {
			t.Fatal(err)
		}
		return st, cancel
	}

	// First consumer dies after a dozen rows; the second resumes from its
	// last VGtid. Together they see every id exactly once.
	st, cancel := open(nil)
	first := consume(t, st, func(c *consumed) bool { return len(flatten(c)) >= 12 })
	cancel()
	if first.last == nil || len(first.last.GetShards()) == 0 {
		t.Fatalf("no vgtid after %d rows", len(flatten(first)))
	}
	st, cancel = open(first.last)
	second := consume(t, st, func(c *consumed) bool {
		return len(flatten(first))+len(flatten(c)) >= total && len(first.committed)+len(c.committed) >= 2
	})
	ids := append(flatten(first), flatten(second)...)
	sort.Ints(ids)
	for i, v := range ids {
		if v != i+1 {
			t.Fatalf("ids after resume: %v (first %v, second %v)", ids, flatten(first), flatten(second))
		}
	}
	if len(ids) != total {
		t.Fatalf("saw %d ids, want %d", len(ids), total)
	}
	all := append(append([]string(nil), first.prepared...), second.prepared...)
	allCommitted := append(append([]string(nil), first.committed...), second.committed...)
	if len(all) != 2 || all[0] != all[1] || !strings.HasPrefix(all[0], "pgshard-") {
		t.Fatalf("prepare events = %v, want the 2PC gid on both shards", all)
	}
	if len(allCommitted) != 2 || allCommitted[0] != all[0] || allCommitted[1] != all[0] {
		t.Fatalf("commit prepared events = %v, want %s twice", allCommitted, all[0])
	}
	for i, tx := range second.txns {
		if len(tx) > 1 {
			t.Fatalf("transaction %d (%s) carries %v: transactions must not merge", i, second.shards[i], tx)
		}
	}

	// Acking the final vector advances confirmed_flush_lsn on both shards.
	final := second.last
	if err := st.Send(&pgshardv1.VStreamRequest{Request: &pgshardv1.VStreamRequest_Ack{Ack: final}}); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(15 * time.Second)
	for {
		ok := true
		for _, p := range final.GetShards() {
			if s.confirmedFlush(t, int(p.GetShard().GetShardId())) < p.GetLsn() {
				ok = false
			}
		}
		if ok {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("confirmed_flush_lsn did not reach %v (shard0 %d, shard1 %d)", final.GetShards(), s.confirmedFlush(t, 0), s.confirmedFlush(t, 1))
		}
		time.Sleep(100 * time.Millisecond)
	}

	// An ack past what was delivered is clamped at the pooler: confirmed_flush
	// stays at or below the WAL end, never at the bogus position.
	const overAck = uint64(1) << 62
	var overPos pgshardv1.VPosition
	for _, p := range final.GetShards() {
		overPos.Shards = append(overPos.Shards, &pgshardv1.VPosition_Shard{Shard: p.GetShard(), Lsn: overAck})
	}
	if ack, err := s.client.Ack(ctx, &pgshardv1.VStreamAckRequest{Stream: "orders", Position: &overPos}); err != nil || ack.GetError() != nil {
		t.Fatalf("over-ack through the router: %v %v", ack, err)
	}
	for _, p := range final.GetShards() {
		id := int(p.GetShard().GetShardId())
		if got := s.confirmedFlush(t, id); got >= overAck || got > s.walEnd(t, id) {
			t.Fatalf("shard %d: over-ack moved confirmed_flush_lsn to %d", id, got)
		}
	}
	cancel()

	// A stream cannot be opened twice on the same slots while the first
	// reader is still attached, but a fresh one after cancel is fine; it
	// sees nothing new and heartbeats.
	st, cancel = open(final)
	hbCtx, hbCancel := context.WithTimeout(ctx, 10*time.Second)
	defer hbCancel()
	for {
		ev, err := st.Recv()
		if err != nil {
			t.Fatalf("fresh stream: %v", err)
		}
		if ev.GetHeartbeat() != nil {
			if len(ev.GetHeartbeat().GetPosition().GetShards()) != 2 {
				t.Fatalf("heartbeat position: %v", ev.GetHeartbeat().GetPosition())
			}
			break
		}
		if ev.GetBegin() != nil {
			t.Fatalf("resumed stream replayed a transaction: %v", ev)
		}
		if hbCtx.Err() != nil {
			t.Fatal("no heartbeat")
		}
	}
	cancel()

	dropped, err := s.client.Drop(ctx, &pgshardv1.DropVStreamRequest{Stream: "orders"})
	if err != nil || dropped.GetError() != nil {
		t.Fatalf("drop: %v %v", dropped, err)
	}
	for shard := range 2 {
		if got := s.slotRows(t, shard); len(got) != 0 {
			t.Fatalf("shard %d slots left after drop: %v", shard, got)
		}
	}
	if list, err := s.client.List(ctx, &pgshardv1.ListVStreamsRequest{}); err != nil || len(list.GetStreams()) != 0 {
		t.Fatalf("list after drop: %v %v", list, err)
	}
}

// containerOf is the docker container name startPostgres gave the server
// listening on addr.
func containerOf(name, addr string) string {
	_, port, _ := net.SplitHostPort(addr)
	return fmt.Sprintf("pgshard-router-e2e-%s-%s", name, port)
}

func hostPortOf(tb testing.TB, dsn string) string {
	tb.Helper()
	cfg, err := pgx.ParseConfig(dsn)
	if err != nil {
		tb.Fatal(err)
	}
	return net.JoinHostPort(cfg.Host, fmt.Sprint(cfg.Port))
}

// startStandby clones primary (a startPostgres container) into a hot
// standby that synchronizes failover slots, and returns its address and
// admin DSN.
func startStandby(tb testing.TB, primaryContainer, primaryAdminDSN string, opts ...string) (addr, adminDSN string) {
	tb.Helper()
	ctx := context.Background()
	out, err := exec.Command("docker", "exec", primaryContainer, "sh", "-ec",
		`echo "host replication all all trust" >> /tmp/pgdata/pg_hba.conf`).CombinedOutput()
	if err != nil {
		tb.Fatalf("hba: %v: %s", err, out)
	}
	admin, err := pgx.Connect(ctx, primaryAdminDSN)
	if err != nil {
		tb.Fatal(err)
	}
	if _, err := admin.Exec(ctx, "select pg_reload_conf()"); err != nil {
		tb.Fatal(err)
	}
	_ = admin.Close(ctx)
	ip, err := exec.Command("docker", "inspect", "-f", "{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}", primaryContainer).Output()
	if err != nil {
		tb.Fatalf("inspect: %v", err)
	}
	primaryIP := strings.TrimSpace(string(ip))
	port := freePort(tb)
	script := `pg_basebackup -h ` + primaryIP + ` -p 5432 -U postgres -D /tmp/pgdata -X stream -C -S standby1 -R >/dev/null &&
		 exec postgres -D /tmp/pgdata -c listen_addresses='*' -c wal_level=logical -c hot_standby_feedback=on \
		   -c sync_replication_slots=on -c primary_slot_name=standby1 \
		   -c "primary_conninfo=host=` + primaryIP + ` port=5432 user=postgres dbname=` + appDatabase + `" ` + strings.Join(opts, " ")
	cname := fmt.Sprintf("pgshard-router-e2e-standby-%d", port)
	out, err = exec.Command("docker", "run", "-d", "--rm", "--name", cname, "-p", fmt.Sprintf("127.0.0.1:%d:5432", port), "--entrypoint", "sh", pgImage(), "-ec", script).CombinedOutput()
	if err != nil {
		tb.Fatalf("docker run: %v: %s", err, out)
	}
	tb.Cleanup(func() { _ = exec.Command("docker", "rm", "-f", cname).Run() })
	addr = fmt.Sprintf("127.0.0.1:%d", port)
	adminDSN = fmt.Sprintf("postgres://postgres@%s/postgres?sslmode=disable", addr)
	deadline := time.Now().Add(120 * time.Second)
	for time.Now().Before(deadline) {
		cctx, cancel := context.WithTimeout(ctx, 2*time.Second)
		conn, err := pgx.Connect(cctx, adminDSN)
		cancel()
		if err == nil {
			var recovery bool
			err = conn.QueryRow(ctx, "select pg_is_in_recovery()").Scan(&recovery)
			_ = conn.Close(ctx)
			if err == nil && recovery {
				return addr, adminDSN
			}
		}
		time.Sleep(300 * time.Millisecond)
	}
	logs, _ := exec.Command("docker", "logs", cname).CombinedOutput()
	tb.Fatalf("standby did not become ready:\n%s", logs)
	return "", ""
}

func TestRouterVStreamFailoverContinuity(t *testing.T) {
	s := startVStreamStack(t)
	ctx := context.Background()
	t0, t1 := twoTenants(t)
	conn := s.connect(t)
	s.awaitSharded(t, conn)

	if created, err := s.client.Create(ctx, &pgshardv1.CreateVStreamRequest{Stream: "orders", Database: appDatabase}); err != nil || created.GetError() != nil {
		t.Fatalf("create: %v %v", created, err)
	}
	standbyAddr, standbyDSN := startStandby(t, containerOf("shard1", hostPortOf(t, s.shard1DSN)), s.shard1DSN, preparedXacts)
	standby, err := pgx.Connect(ctx, streamDSN(standbyDSN))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = standby.Close(ctx) }()

	insert := func(tenant int64, id int) {
		t.Helper()
		if _, err := conn.Exec(ctx, fmt.Sprintf("insert into orders (tenant_id, id) values (%d, %d)", tenant, id), pgx.QueryExecModeSimpleProtocol); err != nil {
			t.Fatalf("insert %d: %v", id, err)
		}
	}
	for id := 1; id <= 10; id++ {
		insert(t1, id)
		insert(t0, 100+id)
	}
	sctx, cancel := context.WithCancel(ctx)
	defer cancel()
	st, err := s.client.Stream(sctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := st.Send(&pgshardv1.VStreamRequest{Request: &pgshardv1.VStreamRequest_Start_{Start: &pgshardv1.VStreamRequest_Start{Stream: "orders"}}}); err != nil {
		t.Fatal(err)
	}
	before := consume(t, st, func(c *consumed) bool { return len(flatten(c)) >= 20 })
	if err := st.Send(&pgshardv1.VStreamRequest{Request: &pgshardv1.VStreamRequest_Ack{Ack: before.last}}); err != nil {
		t.Fatal(err)
	}

	// The failover slot must be synchronized (and persistent: a synced slot
	// stays temporary until the standby has replayed past its positions)
	// on the standby before the promotion, and the standby caught up.
	primary, err := pgx.Connect(ctx, s.appDSN(1))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = primary.Close(ctx) }()
	deadline := time.Now().Add(120 * time.Second)
	for {
		var synced bool
		var replay, current int64
		_ = standby.QueryRow(ctx, "select coalesce((select synced and not temporary from pg_replication_slots where slot_name = 'pgshard_orders_shard1'), false)").Scan(&synced)
		_ = standby.QueryRow(ctx, "select coalesce(pg_last_wal_replay_lsn() - '0/0'::pg_lsn, 0)").Scan(&replay)
		_ = primary.QueryRow(ctx, "select pg_current_wal_lsn() - '0/0'::pg_lsn").Scan(&current)
		if synced && replay >= current {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("slot not synchronized in time (synced=%t replay=%d current=%d)", synced, replay, current)
		}
		time.Sleep(500 * time.Millisecond)
	}

	if _, err := standby.Exec(ctx, "select pg_promote()"); err != nil {
		t.Fatal(err)
	}
	poolerBin, _ := buildBinaries(t)
	newPooler := fmt.Sprintf("127.0.0.1:%d", freePort(t))
	host, port, _ := net.SplitHostPort(standbyAddr)
	startProcess(t, &logBuffer{}, "listening on", poolerBin, "run", "--insecure-dev", "--listen", newPooler,
		"--pg-host", host, "--pg-port", port, "--pg-database", appDatabase, "--stream-dsn", streamDSN(standbyDSN),
		"--catalog-dsn", s.catalogDSN, "--shard-set", router.DefaultShardSet, "--shard-id", "1", "--drain-timeout", "5s")
	err = router.DevBootstrap{CatalogDSN: s.catalogDSN, ShardDSN: standbyDSN, ShardID: 1, Database: appDatabase, Role: appRole,
		Password: appPassword, PoolerEndpoint: newPooler, Epoch: 2}.Run(ctx)
	if err != nil {
		t.Fatalf("publish new primary: %v", err)
	}

	// Writes to shard 1 now land on the promoted standby once the router
	// has seen the new endpoint; retry until they do.
	deadline = time.Now().Add(30 * time.Second)
	for {
		var n int
		_ = standby.QueryRow(ctx, "select count(*) from orders").Scan(&n)
		fresh := s.connect(t)
		_, err := fresh.Exec(ctx, fmt.Sprintf("insert into orders (tenant_id, id) values (%d, 999)", t1), pgx.QueryExecModeSimpleProtocol)
		var after int
		_ = standby.QueryRow(ctx, "select count(*) from orders").Scan(&after)
		if err == nil && after == n+1 {
			conn = fresh
			break
		}
		if err == nil {
			if _, err := fresh.Exec(ctx, fmt.Sprintf("delete from orders where tenant_id = %d and id = 999", t1), pgx.QueryExecModeSimpleProtocol); err != nil {
				t.Fatal(err)
			}
		}
		if time.Now().After(deadline) {
			var oldN int
			_ = primary.QueryRow(ctx, "select count(*) from orders").Scan(&oldN)
			var status string
			cat, _ := pgx.Connect(ctx, s.catalogDSN)
			_ = cat.QueryRow(ctx, "select primary_endpoint || ' epoch=' || primary_epoch from pgshard.shard_status where shard_id = 1").Scan(&status)
			t.Fatalf("router did not move shard 1 to the promoted standby (last err %v; old primary rows %d, standby rows %d, shard_status %s, new pooler %s)\nrouter log:\n%s",
				err, oldN, after, status, newPooler, s.routerLog.String())
		}
		time.Sleep(500 * time.Millisecond)
	}
	for id := 11; id <= 20; id++ {
		insert(t1, id)
	}
	// Probe rows (999) may have been written to, and streamed from, the old
	// primary before the router and the stream followed the promotion; the
	// ids 11..20 only ever exist on the promoted standby.
	after := consume(t, st, func(c *consumed) bool {
		n := 0
		for _, id := range flatten(c) {
			if id != 999 {
				n++
			}
		}
		return n >= 10
	})
	var ids []int
	for _, id := range flatten(after) {
		if id != 999 {
			ids = append(ids, id)
		}
	}
	sort.Ints(ids)
	for i, v := range ids {
		if v != 11+i {
			t.Fatalf("after failover: %v (all %v)", ids, flatten(after))
		}
	}
	if len(ids) != 10 {
		t.Fatalf("after failover saw %v, want 11..20", ids)
	}
	var synced bool
	if err := standby.QueryRow(ctx, "select synced from pg_replication_slots where slot_name = 'pgshard_orders_shard1'").Scan(&synced); err != nil || !synced {
		t.Fatalf("promoted standby slot: synced=%t %v", synced, err)
	}
}
