package controller

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/andrew01234567890/pgshard/internal/catalog"
	pgshardv1 "github.com/andrew01234567890/pgshard/internal/gen/pgshard/v1"
)

const pgImage = "ghcr.io/andrew01234567890/pgshard-postgres:18"

func startPostgres(t *testing.T) string {
	t.Helper()
	return startPostgresWith(t)
}

// startPostgresWith starts PostgreSQL with extra server options.
func startPostgresWith(t *testing.T, opts ...string) string {
	t.Helper()
	return startPostgresImage(t, pgImage, nil, opts...)
}

// startPostgresImage starts image with extra docker run arguments and
// server options and returns the host-side DSN.
func startPostgresImage(t *testing.T, image string, dockerArgs []string, opts ...string) string {
	t.Helper()
	if err := exec.Command("docker", "info").Run(); err != nil {
		t.Skip("docker unavailable; skipping controller integration tests")
	}
	if exec.Command("docker", "image", "inspect", image).Run() != nil {
		if out, err := exec.Command("docker", "pull", image).CombinedOutput(); err != nil {
			t.Skipf("image %s unavailable: %v: %s", image, err, out)
		}
	}
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := l.Addr().(*net.TCPAddr).Port
	_ = l.Close()
	args := append([]string{"run", "-d", "--rm", "-p", fmt.Sprintf("127.0.0.1:%d:5432", port)}, dockerArgs...)
	args = append(args, "--entrypoint", "sh", image, "-ec",
		`initdb -D /tmp/pgdata --auth=trust -U postgres >/dev/null &&
		 echo "host all all all trust" >> /tmp/pgdata/pg_hba.conf &&
		 exec postgres -D /tmp/pgdata -c listen_addresses='*' `+strings.Join(opts, " "))
	out, err := exec.Command("docker", args...).CombinedOutput()
	if err != nil {
		t.Fatalf("docker run: %v: %s", err, out)
	}
	id := strings.TrimSpace(string(out))
	t.Cleanup(func() { _ = exec.Command("docker", "rm", "-f", id).Run() })
	dsn := fmt.Sprintf("postgres://postgres@127.0.0.1:%d/postgres?sslmode=disable", port)
	deadline := time.Now().Add(90 * time.Second)
	for time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		conn, err := pgx.Connect(ctx, dsn)
		cancel()
		if err == nil {
			_ = conn.Close(context.Background())
			return dsn
		}
		time.Sleep(300 * time.Millisecond)
	}
	t.Fatal("postgres did not become ready")
	return ""
}

func connect(t *testing.T, dsn string) *pgx.Conn {
	t.Helper()
	conn, err := pgx.Connect(context.Background(), dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close(context.Background()) })
	return conn
}

func mustExec(t *testing.T, conn *pgx.Conn, sql string, args ...any) {
	t.Helper()
	if _, err := conn.Exec(context.Background(), sql, args...); err != nil {
		t.Fatalf("%s: %v", sql, err)
	}
}

func queryOne[T any](t *testing.T, conn *pgx.Conn, sql string, args ...any) T {
	t.Helper()
	var v T
	if err := conn.QueryRow(context.Background(), sql, args...).Scan(&v); err != nil {
		t.Fatalf("%s: %v", sql, err)
	}
	return v
}

func reconcile(t *testing.T, conn *pgx.Conn) Result {
	t.Helper()
	res, err := Reconcile(context.Background(), conn, nil)
	if err != nil {
		t.Fatal(err)
	}
	return res
}

func TestController(t *testing.T) {
	dsn := startPostgres(t)
	ctx := context.Background()
	conn := connect(t, dsn)
	if err := catalog.Migrate(ctx, conn); err != nil {
		t.Fatal(err)
	}
	mustExec(t, conn, `INSERT INTO pgshard.databases (name) VALUES ('app')`)

	t.Run("new_sharded_table_is_effective_immediately", func(t *testing.T) {
		mustExec(t, conn, `INSERT INTO pgshard.tables (database, schema_name, table_name, placement, shard_key)
			VALUES ('app', 'public', 'orders', 'sharded', 'customer_id')`)
		res := reconcile(t, conn)
		if res.TablesMadeEffective != 1 || res.WorkflowsCreated != 0 {
			t.Fatalf("result %+v", res)
		}
		st, err := catalog.ListTableStatus(ctx, conn, "app")
		if err != nil {
			t.Fatal(err)
		}
		if len(st) != 1 || st[0].EffectivePlacement == nil || *st[0].EffectivePlacement != "sharded" ||
			st[0].EffectiveShardKey == nil || *st[0].EffectiveShardKey != "customer_id" || st[0].WorkflowID != nil {
			t.Fatalf("table_status %+v", st)
		}
		desired := queryOne[int64](t, conn, `SELECT desired_generation FROM pgshard.tables WHERE table_name = 'orders'`)
		if st[0].EffectiveGeneration != desired {
			t.Fatalf("effective_generation %d, desired %d", st[0].EffectiveGeneration, desired)
		}
		if res := reconcile(t, conn); res.TablesMadeEffective != 0 || res.WorkflowsCreated != 0 {
			t.Fatalf("second pass not idempotent: %+v", res)
		}
	})

	t.Run("shard_key_change_needs_workflow", func(t *testing.T) {
		mustExec(t, conn, `UPDATE pgshard.tables SET shard_key = 'region_id' WHERE table_name = 'orders'`)
		res := reconcile(t, conn)
		if res.WorkflowsCreated != 1 || res.TablesMadeEffective != 0 {
			t.Fatalf("result %+v", res)
		}
		st, _ := catalog.ListTableStatus(ctx, conn, "app")
		if *st[0].EffectiveShardKey != "customer_id" || st[0].WorkflowID == nil {
			t.Fatalf("table_status %+v", st[0])
		}
		kind := queryOne[string](t, conn, `SELECT kind || ':' || state || ':' || (spec->'to'->>'shard_key') FROM pgshard.workflows WHERE id = $1::uuid`, *st[0].WorkflowID)
		if kind != "table_rekey:pending:region_id" {
			t.Fatalf("workflow %s", kind)
		}
		if res := reconcile(t, conn); res.WorkflowsCreated != 0 {
			t.Fatalf("duplicate workflow created: %+v", res)
		}
		if n := queryOne[int64](t, conn, `SELECT count(*) FROM pgshard.workflows`); n != 1 {
			t.Fatalf("%d workflows", n)
		}
	})

	t.Run("invalid_hash_version_is_skipped", func(t *testing.T) {
		mustExec(t, conn, `INSERT INTO pgshard.hash_versions VALUES (9, 'future')`)
		mustExec(t, conn, `INSERT INTO pgshard.tables (database, schema_name, table_name, placement, shard_key, hash_version)
			VALUES ('app', 'public', 'future', 'sharded', 'id', 9)`)
		res := reconcile(t, conn)
		if len(res.Invalid) != 1 || res.TablesMadeEffective != 0 || !strings.Contains(res.Invalid[0], "hash_version 9") {
			t.Fatalf("result %+v", res)
		}
		mustExec(t, conn, `DELETE FROM pgshard.tables WHERE table_name = 'future'`)
	})

	t.Run("shard_ranges_initial_population", func(t *testing.T) {
		before := queryOne[int64](t, conn, `SELECT generation FROM pgshard.shard_map_generation`)
		mustExec(t, conn, `INSERT INTO pgshard.shard_ranges (shard_set, shard_id, range) VALUES
			('default', 0, '[,0)'), ('default', 1, '[0,)')`)
		res := reconcile(t, conn)
		if res.ShardSetsPopulated != 1 || !res.GenerationBumped || res.WorkflowsCreated != 0 {
			t.Fatalf("result %+v", res)
		}
		st, err := catalog.ListShardStatus(ctx, conn, "default")
		if err != nil {
			t.Fatal(err)
		}
		if len(st) != 2 || st[0].ShardID != 0 || st[1].ShardID != 1 || st[0].ServingState != ServingProvisioning || st[1].GroupName != "shard1" {
			t.Fatalf("shard_status %+v", st)
		}
		if g := queryOne[int64](t, conn, `SELECT generation FROM pgshard.shard_map_generation`); g != before+1 {
			t.Fatalf("shard_map_generation %d, want %d", g, before+1)
		}
		if g := queryOne[int64](t, conn, `SELECT generation FROM pgshard.serving WHERE shard_set = 'default'`); g <= 0 {
			t.Fatalf("serving generation %d", g)
		}
		if res := reconcile(t, conn); res.GenerationBumped || res.ShardSetsPopulated != 0 {
			t.Fatalf("second pass not idempotent: %+v", res)
		}
	})

	t.Run("published_endpoint_flips_provisioning_to_serving", func(t *testing.T) {
		before := queryOne[int64](t, conn, `SELECT generation FROM pgshard.shard_map_generation`)
		mustExec(t, conn, `UPDATE pgshard.shard_status SET primary_endpoint = 'shard0:6432', primary_epoch = 1 WHERE shard_id = 0`)
		res := reconcile(t, conn)
		if res.ShardsMadeServing != 1 || !res.GenerationBumped {
			t.Fatalf("result %+v", res)
		}
		states := queryOne[string](t, conn, `SELECT string_agg(serving_state, ',' ORDER BY shard_id) FROM pgshard.shard_status`)
		if states != "serving,provisioning" {
			t.Fatalf("states %s", states)
		}
		if g := queryOne[int64](t, conn, `SELECT generation FROM pgshard.shard_map_generation`); g != before+1 {
			t.Fatalf("shard_map_generation %d", g)
		}
	})

	t.Run("range_change_needs_reshard_workflow", func(t *testing.T) {
		mustExec(t, conn, `BEGIN;
			UPDATE pgshard.shard_ranges SET range = '[0,100)' WHERE shard_set = 'default' AND shard_id = 1;
			INSERT INTO pgshard.shard_ranges (shard_set, shard_id, range) VALUES ('default', 2, '[100,)');
			COMMIT`)
		res := reconcile(t, conn)
		if res.WorkflowsCreated != 1 || res.ShardSetsPopulated != 0 || res.GenerationBumped {
			t.Fatalf("result %+v", res)
		}
		if n := queryOne[int64](t, conn, `SELECT count(*) FROM pgshard.shard_status`); n != 2 {
			t.Fatalf("%d shard_status rows; new shard must wait for the workflow", n)
		}
		spec := queryOne[string](t, conn, `SELECT kind || ':' || state || ':' || (spec->>'shard_set') || ':' || jsonb_array_length(spec->'ranges')
			FROM pgshard.workflows WHERE kind = 'reshard'`)
		if spec != "reshard:pending:default:3" {
			t.Fatalf("workflow %s", spec)
		}
		if res := reconcile(t, conn); res.WorkflowsCreated != 0 {
			t.Fatalf("duplicate reshard workflow: %+v", res)
		}
	})

	t.Run("overlapping_ranges_refused_by_catalog", func(t *testing.T) {
		_, err := conn.Exec(ctx, `INSERT INTO pgshard.shard_ranges (shard_set, shard_id, range) VALUES ('default', 3, '[50,60)')`)
		var pgErr *pgconn.PgError
		if !errors.As(err, &pgErr) || pgErr.Code != "23P01" {
			t.Fatalf("expected exclusion violation, got %v", err)
		}
	})

	t.Run("workflow_rpcs", func(t *testing.T) {
		pool, err := pgxpool.New(ctx, dsn)
		if err != nil {
			t.Fatal(err)
		}
		defer pool.Close()
		srv := &Server{Pool: pool}
		all, err := srv.ListWorkflows(ctx, &pgshardv1.ListWorkflowsRequest{})
		if err != nil || len(all.Workflows) != 2 {
			t.Fatalf("list: %v %v", all, err)
		}
		rekeys, err := srv.ListWorkflows(ctx, &pgshardv1.ListWorkflowsRequest{Kind: pgshardv1.WorkflowKind_WORKFLOW_KIND_REKEY})
		if err != nil || len(rekeys.Workflows) != 1 || rekeys.Workflows[0].Kind != pgshardv1.WorkflowKind_WORKFLOW_KIND_REKEY {
			t.Fatalf("list rekey: %v %v", rekeys, err)
		}
		id := rekeys.Workflows[0].Id
		got, err := srv.GetWorkflow(ctx, &pgshardv1.GetWorkflowRequest{Id: id})
		if err != nil || got.Workflow.State != pgshardv1.WorkflowState_WORKFLOW_STATE_PENDING {
			t.Fatalf("get: %v %v", got, err)
		}
		if _, err := srv.GetWorkflow(ctx, &pgshardv1.GetWorkflowRequest{Id: "00000000-0000-0000-0000-000000000000"}); status.Code(err) != codes.NotFound {
			t.Fatalf("missing workflow: %v", err)
		}
		paused, err := srv.PauseWorkflow(ctx, &pgshardv1.PauseWorkflowRequest{Id: id})
		if err != nil || paused.Workflow.State != pgshardv1.WorkflowState_WORKFLOW_STATE_PAUSED {
			t.Fatalf("pause: %v %v", paused, err)
		}
		if _, err := srv.PauseWorkflow(ctx, &pgshardv1.PauseWorkflowRequest{Id: id}); status.Code(err) != codes.FailedPrecondition {
			t.Fatalf("double pause: %v", err)
		}
		if res := reconcile(t, conn); res.WorkflowsCreated != 0 {
			t.Fatalf("paused workflow not treated as active: %+v", res)
		}
		if l, _ := srv.ListWorkflows(ctx, &pgshardv1.ListWorkflowsRequest{State: pgshardv1.WorkflowState_WORKFLOW_STATE_PAUSED}); len(l.Workflows) != 1 {
			t.Fatalf("paused filter: %v", l)
		}
		resumed, err := srv.ResumeWorkflow(ctx, &pgshardv1.ResumeWorkflowRequest{Id: id})
		if err != nil || resumed.Workflow.State != pgshardv1.WorkflowState_WORKFLOW_STATE_PENDING {
			t.Fatalf("resume: %v %v", resumed, err)
		}
		if _, err := srv.ResumeWorkflow(ctx, &pgshardv1.ResumeWorkflowRequest{Id: id}); status.Code(err) != codes.FailedPrecondition {
			t.Fatalf("double resume: %v", err)
		}
		if s := queryOne[string](t, conn, `SELECT status::text FROM pgshard.workflows WHERE id::text = $1`, id); s != "{}" {
			t.Fatalf("status after resume %s", s)
		}
		mustExec(t, conn, `UPDATE pgshard.workflows SET state = 'running' WHERE id::text = $1`, id)
		if _, err := srv.PauseWorkflow(ctx, &pgshardv1.PauseWorkflowRequest{Id: id}); err != nil {
			t.Fatal(err)
		}
		resumed, err = srv.ResumeWorkflow(ctx, &pgshardv1.ResumeWorkflowRequest{Id: id})
		if err != nil || resumed.Workflow.State != pgshardv1.WorkflowState_WORKFLOW_STATE_RUNNING {
			t.Fatalf("resume from running: %v %v", resumed, err)
		}
		mustExec(t, conn, `UPDATE pgshard.workflows SET state = 'pending' WHERE id::text = $1`, id)
	})

	t.Run("reshard_pending_set_state_machine", func(t *testing.T) {
		mustExec(t, conn, `INSERT INTO pgshard.shard_ranges (shard_set, shard_id, range) VALUES ('g2', 0, '[,0)'), ('g2', 1, '[0,)')`)
		res := reconcile(t, conn)
		if res.WorkflowsCreated != 1 || res.ShardSetsPopulated != 0 || res.GenerationBumped {
			t.Fatalf("pending set must only create a provisioning workflow: %+v", res)
		}
		if n := queryOne[int64](t, conn, `SELECT count(*) FROM pgshard.shard_status WHERE shard_set = 'g2'`); n != 0 {
			t.Fatalf("%d shard_status rows for the pending set; the operator publishes them", n)
		}
		if st := queryOne[string](t, conn, `SELECT state FROM pgshard.shard_sets WHERE shard_set = 'g2'`); st != catalog.ShardSetProvisioning {
			t.Fatalf("shard set state %s", st)
		}
		wf := queryOne[string](t, conn, `SELECT state || ':' || (status->>'stage') || ':' || (spec->>'shard_set') || ':' || jsonb_array_length(spec->'ranges')
			FROM pgshard.workflows WHERE kind = 'reshard' AND spec->>'shard_set' = 'g2'`)
		if wf != "provisioning:provisioning:g2:2" {
			t.Fatalf("workflow %s", wf)
		}
		if res := reconcile(t, conn); res.WorkflowsCreated != 0 || res.ReshardsAdvanced != 0 {
			t.Fatalf("idempotent pass: %+v", res)
		}
		if n := queryOne[int64](t, conn, `SELECT count(*) FROM pgshard.serving WHERE shard_set = 'g2'`); n != 0 {
			t.Fatal("a pending set must never be published as serving")
		}
		mustExec(t, conn, `INSERT INTO pgshard.shard_status (shard_set, shard_id, group_name, serving_state, primary_endpoint)
			VALUES ('g2', 0, 'shard-0-g2', 'provisioning', 'shard-0-g2:5432')`)
		if res := reconcile(t, conn); res.ReshardsAdvanced != 0 || res.ShardsMadeServing != 0 {
			t.Fatalf("one target of two must not advance nor become serving: %+v", res)
		}
		mustExec(t, conn, `INSERT INTO pgshard.shard_status (shard_set, shard_id, group_name, serving_state, primary_endpoint)
			VALUES ('g2', 1, 'shard-1-g2', 'provisioning', 'shard-1-g2:5432')`)
		res = reconcile(t, conn)
		if res.ReshardsAdvanced != 1 || res.ShardsMadeServing != 0 || res.GenerationBumped {
			t.Fatalf("all targets ready must hand over to the copy stage: %+v", res)
		}
		wf = queryOne[string](t, conn, `SELECT state || ':' || (status->>'stage') FROM pgshard.workflows WHERE kind = 'reshard' AND spec->>'shard_set' = 'g2'`)
		if wf != "running:ready_for_copy" {
			t.Fatalf("workflow %s", wf)
		}
		states := queryOne[string](t, conn, `SELECT string_agg(DISTINCT serving_state, ',') FROM pgshard.shard_status WHERE shard_set = 'g2'`)
		if states != "provisioning" {
			t.Fatalf("target shard_status must stay provisioning, got %s", states)
		}

		mustExec(t, conn, `UPDATE pgshard.workflows SET state = 'provisioning' WHERE kind = 'reshard' AND spec->>'shard_set' = 'g2'`)
		mustExec(t, conn, `BEGIN; DELETE FROM pgshard.shard_ranges WHERE shard_set = 'g2'; DELETE FROM pgshard.shard_sets WHERE shard_set = 'g2'; COMMIT`)
		res = reconcile(t, conn)
		if res.ReshardsCancelled != 1 {
			t.Fatalf("vanished set must cancel: %+v", res)
		}
		wf = queryOne[string](t, conn, `SELECT state || ':' || (status->>'stage') FROM pgshard.workflows WHERE kind = 'reshard' AND spec->>'shard_set' = 'g2'`)
		if wf != "cancelled:cancelled" {
			t.Fatalf("workflow %s", wf)
		}
		if n := queryOne[int64](t, conn, `SELECT count(*) FROM pgshard.shard_status WHERE shard_set = 'g2'`); n != 0 {
			t.Fatalf("%d stale status rows after cancel", n)
		}
		mustExec(t, conn, `DELETE FROM pgshard.workflows WHERE kind = 'reshard' AND spec->>'shard_set' = 'g2'`)
	})

	t.Run("leader_election_and_notify", func(t *testing.T) {
		mustExec(t, conn, `DELETE FROM pgshard.tables; DELETE FROM pgshard.table_status; DELETE FROM pgshard.workflows`)
		type event struct {
			who    string
			leader bool
		}
		events := make(chan event, 16)
		results := make(chan Result, 16)
		newRec := func(name string) *Reconciler {
			return &Reconciler{DSN: dsn, Interval: time.Hour, RetryInterval: 200 * time.Millisecond,
				OnLeader: func(l bool) { events <- event{name, l} },
				OnResult: func(r Result) {
					if name == "a" {
						results <- r
					}
				}}
		}
		ctxA, cancelA := context.WithCancel(ctx)
		defer cancelA()
		go func() { _ = newRec("a").Run(ctxA) }()
		expect := func(want event) {
			t.Helper()
			select {
			case got := <-events:
				if got != want {
					t.Fatalf("event %+v, want %+v", got, want)
				}
			case <-time.After(15 * time.Second):
				t.Fatalf("no event %+v", want)
			}
		}
		expect(event{"a", true})
		<-results
		ctxB, cancelB := context.WithCancel(ctx)
		defer cancelB()
		go func() { _ = newRec("b").Run(ctxB) }()
		select {
		case e := <-events:
			t.Fatalf("unexpected event %+v while a leads", e)
		case <-time.After(time.Second):
		}
		mustExec(t, conn, `INSERT INTO pgshard.tables (database, schema_name, table_name, placement) VALUES ('app', 'public', 'notified', 'reference')`)
		select {
		case r := <-results:
			if r.TablesMadeEffective != 1 {
				t.Fatalf("notified pass %+v", r)
			}
		case <-time.After(15 * time.Second):
			t.Fatal("leader did not react to pgshard_desired")
		}
		cancelA()
		expect(event{"a", false})
		expect(event{"b", true})
	})

	t.Run("periodic_pass_keeps_leadership", func(t *testing.T) {
		events := make(chan bool, 16)
		passes := make(chan Result, 64)
		rctx, cancel := context.WithCancel(ctx)
		defer cancel()
		go func() {
			_ = (&Reconciler{DSN: dsn, LockKey: 4242, Interval: 200 * time.Millisecond, RetryInterval: 100 * time.Millisecond,
				OnLeader: func(l bool) { events <- l }, OnResult: func(r Result) { passes <- r }}).Run(rctx)
		}()
		if l := <-events; !l {
			t.Fatal("first event was not leadership")
		}
		deadline := time.After(10 * time.Second)
		for n := 0; n < 4; {
			select {
			case <-passes:
				n++
			case l := <-events:
				t.Fatalf("leadership event %v during periodic passes", l)
			case <-deadline:
				t.Fatalf("only %d passes", n)
			}
		}
	})
}
