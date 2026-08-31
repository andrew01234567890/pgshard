//go:build integration

package router

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/andrew01234567890/pgshard/internal/catalog"
	"github.com/andrew01234567890/pgshard/internal/controller"
	"github.com/andrew01234567890/pgshard/internal/placement"
	"github.com/andrew01234567890/pgshard/internal/router"
	"github.com/andrew01234567890/pgshard/test/e2e/oracle"
)

const accounts = 64

// cutoverStack is a catalog, one serving source shard, two provisioning
// targets, a pooler per shard and a router; the shards share a docker
// network so subscriptions reach each other by container name.
type cutoverStack struct {
	*stack
	net     string
	names   map[controller.ShardRef]string
	dsns    map[controller.ShardRef]string
	pool    *pgxpool.Pool
	catalog *pgx.Conn
}

func startCutoverStack(t *testing.T) *cutoverStack {
	t.Helper()
	requireDocker(t)
	poolerBin, routerBin := buildBinaries(t)
	s := &cutoverStack{stack: &stack{routerLog: &logBuffer{}, poolerLog: &logBuffer{}, routerBin: routerBin},
		net:   fmt.Sprintf("pgshard-cutover-%d", time.Now().UnixNano()%1_000_000),
		names: map[controller.ShardRef]string{}, dsns: map[controller.ShardRef]string{}}
	if out, err := exec.Command("docker", "network", "create", s.net).CombinedOutput(); err != nil {
		t.Fatalf("docker network create: %v: %s", err, out)
	}
	t.Cleanup(func() { _ = exec.Command("docker", "network", "rm", s.net).Run() })
	ctx := context.Background()
	_, s.catalogDSN = startPostgres(t, "catalog")
	opts := []string{"-c max_prepared_transactions=16", "-c max_replication_slots=16", "-c max_wal_senders=16",
		"-c max_logical_replication_workers=16", "-c max_worker_processes=32"}
	shards := []controller.ShardRef{{Set: "default", ID: 0}, {Set: "g2", ID: 0}, {Set: "g2", ID: 1}}
	addrs := map[controller.ShardRef]string{}
	for _, ref := range shards {
		s.names[ref] = fmt.Sprintf("%s-%s-%d", s.net, ref.Set, ref.ID)
		addrs[ref], s.dsns[ref] = startNetPostgres(t, s.net, s.names[ref], opts...)
	}
	s.shardAddr, s.shardDSN = addrs[shards[0]], s.dsns[shards[0]]
	s.poolerAddr = fmt.Sprintf("127.0.0.1:%d", freePort(t))
	err := router.DevBootstrap{CatalogDSN: s.catalogDSN, ShardDSN: s.shardDSN, Database: appDatabase, Role: appRole,
		Password: appPassword, PoolerEndpoint: s.poolerAddr, Epoch: 1}.Run(ctx)
	if err != nil {
		t.Fatalf("bootstrap: %v", err)
	}
	s.catalog = connectDSN(t, s.catalogDSN)
	if s.pool, err = pgxpool.New(ctx, s.catalogDSN); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(s.pool.Close)
	var verifier string
	if err := s.catalog.QueryRow(ctx, `SELECT verifier FROM pgshard.roles WHERE rolname = $1`, appRole).Scan(&verifier); err != nil {
		t.Fatal(err)
	}
	tgtRanges, _ := placement.Split(2)
	tx, err := s.catalog.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	one, _ := placement.Split(1)
	if err := catalog.MaterializeShardSet(ctx, tx, "default", 1, catalog.ShardSetServing, one, 0); err != nil {
		t.Fatal(err)
	}
	if err := catalog.MaterializeShardSet(ctx, tx, "g2", 2, catalog.ShardSetProvisioning, tgtRanges, 0); err != nil {
		t.Fatal(err)
	}
	for _, sql := range []string{
		`INSERT INTO pgshard.tables (database, schema_name, table_name, placement, shard_key) VALUES ('app', 'public', 'accounts', 'sharded', 'id')`,
		`INSERT INTO pgshard.table_status (database, schema_name, table_name, effective_placement, effective_shard_key) VALUES ('app', 'public', 'accounts', 'sharded', 'id')`,
	} {
		if _, err := tx.Exec(ctx, sql); err != nil {
			t.Fatalf("%s: %v", sql, err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	for _, ref := range shards {
		host, port, _ := net.SplitHostPort(addrs[ref])
		poolerAddr := s.poolerAddr
		if ref.Set != "default" {
			poolerAddr = fmt.Sprintf("127.0.0.1:%d", freePort(t))
			conn := connectDSN(t, s.dsns[ref])
			if _, err := conn.Exec(ctx, fmt.Sprintf("CREATE ROLE %s LOGIN PASSWORD '%s'", appRole, verifier)); err != nil {
				t.Fatal(err)
			}
			if _, err := s.catalog.Exec(ctx, `INSERT INTO pgshard.shard_status (shard_set, shard_id, group_name, serving_state, primary_epoch, primary_endpoint)
				VALUES ($1, $2, $3, 'provisioning', 1, $4)`, ref.Set, ref.ID, s.names[ref], poolerAddr); err != nil {
				t.Fatal(err)
			}
		}
		startProcess(t, s.poolerLog, "listening on", poolerBin, "run", "--insecure-dev", "--listen", poolerAddr,
			"--pg-host", host, "--pg-port", port, "--pg-database", appDatabase,
			"--catalog-dsn", s.catalogDSN, "--shard-set", ref.Set, "--shard-id", fmt.Sprint(ref.ID), "--drain-timeout", "5s")
	}
	src := connectDSN(t, s.appDSN(shards[0]))
	if _, err := src.Exec(ctx, `CREATE TABLE accounts (id bigint PRIMARY KEY, balance bigint NOT NULL, ops bigint NOT NULL DEFAULT 0);
		INSERT INTO accounts (id, balance) SELECT g, 1000 FROM generate_series(1, `+fmt.Sprint(accounts)+`) g;
		GRANT ALL ON accounts TO `+appRole); err != nil {
		t.Fatal(err)
	}
	s.routerPort = fmt.Sprint(freePort(t))
	s.routerAddr = "127.0.0.1:" + s.routerPort
	s.peerAddr = fmt.Sprintf("127.0.0.1:%d", freePort(t))
	s.healthAddr = fmt.Sprintf("127.0.0.1:%d", freePort(t))
	startProcess(t, s.routerLog, "listening on", routerBin, "serve", "--insecure-dev", "--listen", "0.0.0.0:"+s.routerPort,
		"--catalog-dsn", s.catalogDSN, "--drain-timeout", "5s", "--drain-delay", "1s", "--buffer-window", "20s",
		"--instance-id", "1", "--peer-cancel-listen", s.peerAddr, "--health-listen", s.healthAddr)
	return s
}

func startNetPostgres(tb testing.TB, network, name string, opts ...string) (addr, adminDSN string) {
	tb.Helper()
	port := freePort(tb)
	script := `initdb -D /tmp/pgdata --auth=trust -U postgres >/dev/null &&
		 printf 'host all postgres all trust\nhost all all all scram-sha-256\n' >> /tmp/pgdata/pg_hba.conf &&
		 exec postgres -D /tmp/pgdata -c listen_addresses='*' -c wal_level=logical ` + strings.Join(opts, " ")
	out, err := exec.Command("docker", "run", "-d", "--rm", "--name", name, "--network", network, "-p", fmt.Sprintf("127.0.0.1:%d:5432", port),
		"--entrypoint", "sh", pgImage(), "-ec", script).CombinedOutput()
	if err != nil {
		tb.Fatalf("docker run: %v: %s", err, out)
	}
	tb.Cleanup(func() { _ = exec.Command("docker", "rm", "-f", name).Run() })
	addr = fmt.Sprintf("127.0.0.1:%d", port)
	adminDSN = fmt.Sprintf("postgres://postgres@%s/postgres?sslmode=disable", addr)
	deadline := time.Now().Add(90 * time.Second)
	for time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		conn, err := pgx.Connect(ctx, adminDSN)
		cancel()
		if err == nil {
			_ = conn.Close(context.Background())
			return addr, adminDSN
		}
		time.Sleep(300 * time.Millisecond)
	}
	logs, _ := exec.Command("docker", "logs", name).CombinedOutput()
	tb.Fatalf("%s did not become ready:\n%s", name, logs)
	return "", ""
}

func connectDSN(tb testing.TB, dsn string) *pgx.Conn {
	tb.Helper()
	conn, err := pgx.Connect(context.Background(), dsn)
	if err != nil {
		tb.Fatalf("connect to %s: %v", dsn, err)
	}
	tb.Cleanup(func() { _ = conn.Close(context.Background()) })
	return conn
}

func (s *cutoverStack) appDSN(ref controller.ShardRef) string {
	return strings.Replace(s.dsns[ref], "/postgres?", "/"+appDatabase+"?", 1)
}

func (s *cutoverStack) connInfo(_ context.Context, ref controller.ShardRef, database string) (string, error) {
	return fmt.Sprintf("host=%s port=5432 user=postgres dbname=%s", s.names[ref], database), nil
}

// newCopier builds a fresh controller over the catalog; every pass of the
// test uses a new one, so the workflow record alone carries the state
// between passes, as it does across controller restarts.
func (s *cutoverStack) newCopier(t *testing.T) *controller.Copier {
	dir := t.TempDir()
	for _, bin := range []string{"pg_dump", "psql"} {
		script := fmt.Sprintf("#!/bin/sh\nexec docker exec -i %s %s \"$@\"\n", s.names[controller.ShardRef{Set: "default"}], bin)
		if err := os.WriteFile(filepath.Join(dir, bin), []byte(script), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	return &controller.Copier{Pool: s.pool, Shards: &controller.PgxShardDialer{Pool: s.pool, DSNs: s.dsns},
		Schema: &controller.ExecMaterializer{BinDir: dir, TargetConnInfo: s.connInfo}, SourceConnInfo: s.connInfo,
		LagBytes: 1 << 20, PreparedWait: time.Minute, LockTimeout: 2 * time.Second, CutoverTimeout: 30 * time.Second}
}

func (s *cutoverStack) workflow(t *testing.T, id string) (state, stage, status string) {
	t.Helper()
	if err := s.catalog.QueryRow(context.Background(), `SELECT state, coalesce(status->>'stage', ''), status::text FROM pgshard.workflows WHERE id = $1::uuid`, id).Scan(&state, &stage, &status); err != nil {
		t.Fatal(err)
	}
	return state, stage, status
}

func (s *cutoverStack) driveUntil(t *testing.T, id, wantStage string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		if _, err := s.newCopier(t).Pass(context.Background()); err != nil {
			t.Fatal(err)
		}
		state, stage, status := s.workflow(t, id)
		if stage == wantStage {
			return
		}
		if state == "failed" || time.Now().After(deadline) {
			t.Fatalf("waiting for %s: workflow %s/%s %s\nrouter log:\n%s", wantStage, state, stage, status, s.routerLog.String())
		}
		time.Sleep(100 * time.Millisecond)
	}
}

type transferLoad struct {
	commits atomic.Int64
	fails   atomic.Int64
	// paused counts transfers the router refused with 57P03 and the
	// worker retried, which is what a client is told to do.
	paused atomic.Int64
	mu     sync.Mutex
	errs   map[string]int
	errAt  []time.Time
	stop   chan struct{}
	wg     sync.WaitGroup
}

// isWritePause reports the router's own answer to a write that met a
// cluster write pause: 57P03, cannot_connect_now, which carries a hint to
// retry. PostgreSQL's 25006 is not it -- that is a pause reaching a backend
// the router thought was writable, and it tells the client nothing about
// retrying, so it stays a failure here.
func isWritePause(err error) bool {
	var pe *pgconn.PgError
	return errors.As(err, &pe) && pe.Code == "57P03"
}

func (s *cutoverStack) startLoad(t *testing.T, workers int) *transferLoad {
	t.Helper()
	l := &transferLoad{stop: make(chan struct{}), errs: map[string]int{}}
	for w := range workers {
		conn := s.connect(t)
		l.wg.Add(1)
		go func() {
			defer l.wg.Done()
			ctx := context.Background()
			for i := 0; ; i++ {
				select {
				case <-l.stop:
					return
				default:
				}
				from, to := int64((w*7+i)%accounts+1), int64((w*11+i*3)%accounts+1)
				if from == to {
					to = to%accounts + 1
				}
				tx, err := conn.Begin(ctx)
				if err == nil {
					_, err = tx.Exec(ctx, `UPDATE accounts SET balance = balance - 1, ops = ops + 1 WHERE id = $1`, from)
					if err == nil {
						_, err = tx.Exec(ctx, `UPDATE accounts SET balance = balance + 1, ops = ops + 1 WHERE id = $1`, to)
					}
					if err == nil {
						err = tx.Commit(ctx)
					} else {
						_ = tx.Rollback(ctx)
					}
				}
				if err != nil {
					// A transfer writes to two accounts, so it spans two
					// shards. A cutover pause that arrives between them
					// refuses the second shard at once rather than
					// buffering: buffering inside an open transaction
					// would hold it in front of the drain the cutover is
					// waiting on. The router says so with 57P03 and a
					// hint to retry, and a client that retries loses
					// nothing -- so the workload retries, and a lost
					// transfer still counts as a failure.
					if isWritePause(err) {
						l.paused.Add(1)
						i--
						time.Sleep(50 * time.Millisecond)
						continue
					}
					l.fails.Add(1)
					l.mu.Lock()
					l.errs[err.Error()]++
					l.errAt = append(l.errAt, time.Now())
					l.mu.Unlock()
					time.Sleep(50 * time.Millisecond)
					continue
				}
				l.commits.Add(1)
			}
		}()
	}
	return l
}

func (l *transferLoad) halt() {
	close(l.stop)
	l.wg.Wait()
}

func (s *cutoverStack) balances(conn *pgx.Conn) func(ctx context.Context) (map[string]int64, error) {
	return func(ctx context.Context) (map[string]int64, error) {
		rows, err := conn.Query(ctx, `SELECT id, balance FROM accounts`)
		if err != nil {
			return nil, err
		}
		defer rows.Close()
		out := map[string]int64{}
		for rows.Next() {
			var id, b int64
			if err := rows.Scan(&id, &b); err != nil {
				return nil, err
			}
			out[fmt.Sprint(id)] = b
		}
		return out, rows.Err()
	}
}

func queryInt(t *testing.T, conn *pgx.Conn, sql string, args ...any) int64 {
	t.Helper()
	var n int64
	if err := conn.QueryRow(context.Background(), sql, args...).Scan(&n); err != nil {
		t.Fatalf("%s: %v", sql, err)
	}
	return n
}

// TestReshardCutoverUnderLoad grows one shard to two through the router
// while transfers run: no transfer fails, the ledger balances, every
// acknowledged commit is on the new shards, rows sit where the new map
// places them, the journal is on the source, writes after the switch flow
// back to the source, and the pause stays under two seconds.
func TestReshardCutoverUnderLoad(t *testing.T) {
	s := startCutoverStack(t)
	ctx := context.Background()
	ranges, _ := placement.Split(2)
	spec := map[string]any{"shard_set": "g2", "generation": 2, "source_set": "default", "retire_after_seconds": 1,
		"ranges": []map[string]any{{"shard_id": 0, "lower": ranges[0].Start, "upper": ranges[0].End}, {"shard_id": 1, "lower": ranges[1].Start, "upper": ranges[1].End}}}
	var id string
	if err := s.catalog.QueryRow(ctx, `INSERT INTO pgshard.workflows (id, kind, state, spec, status) VALUES (gen_random_uuid(), 'reshard', 'running', $1, '{"stage": "ready_for_copy"}') RETURNING id::text`, spec).Scan(&id); err != nil {
		t.Fatal(err)
	}
	client := s.connect(t)
	ledger := &oracle.Ledger{Expected: accounts * 1000, Balances: s.balances(client)}
	load := s.startLoad(t, 4)
	s.driveUntil(t, id, "awaiting_switch_writes", 3*time.Minute)
	before := load.commits.Load()
	s.driveUntil(t, id, "switched", 2*time.Minute)
	time.Sleep(2 * time.Second)
	load.halt()
	if load.commits.Load() <= before {
		t.Fatal("no transfer committed during the switch")
	}
	if f := load.fails.Load(); f != 0 {
		t.Errorf("%d transfers failed during the cutover: %v\nrouter log:\n%s", f, load.errs, s.routerLog.String())
	}
	if v, err := ledger.Check(ctx); err != nil || len(v) > 0 {
		t.Fatalf("ledger through the router: %v %v", v, err)
	}
	if ops := queryInt(t, client, `SELECT sum(ops) FROM accounts`); ops != 2*load.commits.Load() {
		t.Fatalf("ops %d, want %d (every acknowledged commit on the new shards)", ops, 2*load.commits.Load())
	}
	_, _, status := s.workflow(t, id)
	pause := queryInt(t, s.catalog, `SELECT (status->'cutover'->>'pause_ms')::bigint FROM pgshard.workflows WHERE id = $1::uuid`, id)
	t.Logf("cutover pause %dms, %d transfers retried through it; %s", pause, load.paused.Load(), status)
	for _, at := range load.errAt {
		t.Logf("error at %s", at.Format(time.RFC3339Nano))
	}
	if pause <= 0 || pause >= 2000 {
		t.Errorf("cutover pause %dms, want (0, 2000)", pause)
	}
	hash, err := controller.KeyHashExpr("id", "bigint")
	if err != nil {
		t.Fatal(err)
	}
	var total int64
	for i, r := range ranges {
		conn := connectDSN(t, s.appDSN(controller.ShardRef{Set: "g2", ID: int32(i)}))
		if stray := queryInt(t, conn, `SELECT count(*) FROM accounts WHERE NOT (`+controller.RangeFilter(hash, r)+`)`); stray != 0 {
			t.Errorf("target %d holds %d rows outside its range", i, stray)
		}
		total += queryInt(t, conn, `SELECT count(*) FROM accounts`)
	}
	if total != accounts {
		t.Errorf("targets hold %d rows, want %d", total, accounts)
	}
	src := connectDSN(t, s.appDSN(controller.ShardRef{Set: "default"}))
	if n := queryInt(t, src, `SELECT count(*) FROM pgshard_journal.resharding_journal WHERE generation = 2`); n != 1 {
		t.Errorf("journal rows on the source: %d", n)
	}
	if n := queryInt(t, s.catalog, `SELECT count(*) FROM pgshard.resharding_journal WHERE shard_set = 'g2'`); n != 1 {
		t.Errorf("catalog journal rows: %d", n)
	}
	if got := queryInt(t, s.catalog, `SELECT count(*) FROM pgshard.shard_status WHERE migrating`); got != 0 {
		t.Errorf("fence left raised on %d shards", got)
	}
	var states string
	if err := s.catalog.QueryRow(ctx, `SELECT string_agg(shard_set || ':' || state, ',' ORDER BY generation) FROM pgshard.shard_sets`).Scan(&states); err != nil || states != "default:retired,g2:serving" {
		t.Errorf("shard sets %q %v", states, err)
	}
	if _, err := client.Exec(ctx, `INSERT INTO accounts (id, balance) VALUES ($1, 0)`, int64(accounts+1)); err != nil {
		t.Fatalf("write after the switch: %v", err)
	}
	deadline := time.Now().Add(time.Minute)
	for queryInt(t, src, `SELECT count(*) FROM accounts WHERE id = $1`, int64(accounts+1)) == 0 {
		if time.Now().After(deadline) {
			t.Fatal("reverse replication never delivered the post-switch row to the source")
		}
		time.Sleep(200 * time.Millisecond)
	}
	s.driveUntil(t, id, "completed", 2*time.Minute)
	if n := queryInt(t, src, `SELECT count(*) FROM pg_subscription`) + queryInt(t, src, `SELECT count(*) FROM pg_replication_slots`) +
		queryInt(t, src, `SELECT count(*) FROM pg_publication`); n != 0 {
		t.Errorf("replication objects left on the source: %d", n)
	}
	for i := range 2 {
		conn := connectDSN(t, s.appDSN(controller.ShardRef{Set: "g2", ID: int32(i)}))
		if n := queryInt(t, conn, `SELECT count(*) FROM pg_subscription`) + queryInt(t, conn, `SELECT count(*) FROM pg_replication_slots`) +
			queryInt(t, conn, `SELECT count(*) FROM pg_publication`); n != 0 {
			t.Errorf("replication objects left on target %d: %d", i, n)
		}
	}
	if v, err := ledger.Check(ctx); err != nil || len(v) > 0 {
		t.Fatalf("ledger after complete: %v %v", v, err)
	}
}
