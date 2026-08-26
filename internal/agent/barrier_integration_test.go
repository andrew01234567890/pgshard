//go:build integration

package agent

import (
	"context"
	"github.com/andrew01234567890/pgshard/internal/dockertest"
	"os"
	"os/exec"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/andrew01234567890/pgshard/internal/catalog"
	pgshardv1 "github.com/andrew01234567890/pgshard/internal/gen/pgshard/v1"
)

// TestRestoreReconciliationMatrix restores one shard to restore points
// taken before PREPARE, between PREPARE and the decision, and after the
// decision, and checks the reconciliation the agent applies against the
// decision log at each point. It also exercises the catalog-side RPCs.
func TestRestoreReconciliationMatrix(t *testing.T) {
	if err := exec.Command("docker", "info").Run(); err != nil {
		dockertest.Unavailable(t, "docker unavailable")
	}
	image := integrationImages[0]
	if img := os.Getenv("PGSHARD_POSTGRES_IMAGE"); img != "" {
		image = img
	}
	if exec.Command("docker", "image", "inspect", image).Run() != nil {
		t.Skipf("image %s not present", image)
	}
	h := newHarness(t, image, buildAgent(t))
	h.extra = map[string]any{
		"backup":   h.startMinIO("it-s0-pg18"),
		"postgres": map[string]any{"parameters": map[string]string{"max_prepared_transactions": "16"}},
	}
	p := h.start("s0-0", RolePrimary, "s0-0", nil)
	p.waitHTTP("/startz", 200, 90*time.Second)
	p.waitHTTP("/readyz", 200, 60*time.Second)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	waitInfo := func(cond func(*pgshardv1.RestoreInfoResponse) bool, what string) {
		t.Helper()
		deadline := time.Now().Add(2 * time.Minute)
		var last *pgshardv1.RestoreInfoResponse
		for time.Now().Before(deadline) {
			last, _ = p.grpc.RestoreInfo(ctx, &pgshardv1.RestoreInfoRequest{})
			if last.GetError() == nil && cond(last) {
				return
			}
			time.Sleep(time.Second)
		}
		t.Fatalf("timed out waiting for %s; last %v\n%s", what, last, p.logs())
	}
	waitInfo(func(r *pgshardv1.RestoreInfoResponse) bool {
		return r.GetStatusCode() != 0 || r.GetStanza() == "it-s0-pg18"
	}, "stanza")
	full, err := p.grpc.Backup(ctx, &pgshardv1.BackupRequest{Type: pgshardv1.BackupRequest_TYPE_FULL})
	if err != nil || full.GetError() != nil {
		t.Fatalf("full backup: %v %v\n%s", err, full.GetError(), p.logs())
	}

	// The catalog schema on this instance stands in for the catalog group.
	migrations, err := catalog.Migrations()
	if err != nil {
		t.Fatal(err)
	}
	p.psql("CREATE SCHEMA pgshard; CREATE TABLE pgshard.schema_migrations (version integer PRIMARY KEY, applied_at timestamptz NOT NULL DEFAULT now(), checksum text NOT NULL)")
	for _, m := range migrations {
		p.psql(m.SQL)
	}
	p.psql("CREATE TABLE t (v text)")
	// prepare leaves a prepared transaction and returns its transaction id
	// (the cluster is young, so the 32-bit id equals the xid8 the router
	// would have recorded).
	prepare := func(gid, v string) string {
		t.Helper()
		p.psql("BEGIN; INSERT INTO t VALUES ('" + v + "'); PREPARE TRANSACTION '" + gid + "'")
		return p.psql("SELECT transaction FROM pg_prepared_xacts WHERE gid = '" + gid + "'")
	}
	if got := p.psql("SELECT count(*) FROM pg_prepared_xacts"); got != "0" {
		t.Fatalf("prepared before the test: %s", got)
	}
	p.psql("SELECT pg_create_restore_point('pgshard-b0')")
	xidX := prepare("pgshard-x", "x")
	prepare("pgshard-y", "y")
	prepare("pgshard-z", "z")
	// psql exits with the transaction open, so w rolls back.
	xidW := p.psql("BEGIN; INSERT INTO t VALUES ('w'); SELECT pg_current_xact_id()")
	xidW = xidW[strings.LastIndex(xidW, "\n")+1:]
	p.psql("SELECT pg_create_restore_point('pgshard-b1')")
	listed, err := p.grpc.ListPreparedTransactions(ctx, &pgshardv1.ListPreparedTransactionsRequest{})
	if err != nil || listed.GetError() != nil || len(listed.GetPrepared()) != 3 || listed.GetPrepared()[0].GetGid() != "pgshard-x" || listed.GetPrepared()[0].GetDatabase() != "postgres" || listed.GetPrepared()[2].GetGid() != "pgshard-z" {
		t.Fatalf("ListPreparedTransactions: %v %v", err, listed)
	}
	p.psql("COMMIT PREPARED 'pgshard-x'")
	p.psql("ROLLBACK PREPARED 'pgshard-y'")
	p.psql("ROLLBACK PREPARED 'pgshard-z'")
	p.psql("SELECT pg_create_restore_point('pgshard-b2')")
	seg := p.psql("SELECT pg_walfile_name(pg_switch_wal())")
	waitInfo(func(r *pgshardv1.RestoreInfoResponse) bool { return r.GetArchiveMax() >= seg }, "archived WAL "+seg)

	// Catalog-side RPCs on the source: the decision log and the fence.
	p.psql("INSERT INTO pgshard.xact_decisions (gid, state, participants, participant_xids) VALUES ('pgshard-x', 'commit', '{0}', '{" + xidX + "}'), ('pgshard-z', 'abort', '{0,1}', '{" + xidW + ",7}')")
	decisions, err := p.grpc.ListTransactionDecisions(ctx, &pgshardv1.ListTransactionDecisionsRequest{})
	if err != nil || decisions.GetError() != nil || len(decisions.GetDecisions()) != 2 {
		t.Fatalf("ListTransactionDecisions: %v %v", err, decisions)
	}
	if d := decisions.GetDecisions()[1]; d.GetGid() != "pgshard-z" || d.GetState() != "abort" || !slices.Equal(d.GetParticipants(), []int32{0, 1}) || !slices.Equal(d.GetParticipantXids(), []string{xidW, "7"}) {
		t.Fatalf("decision %v", d)
	}
	epoch := p.status().GetEpoch()
	if resp, err := p.grpc.SetWriteFence(ctx, &pgshardv1.SetWriteFenceRequest{Epoch: epoch, Active: true, Reason: "test"}); err != nil || resp.GetError() != nil {
		t.Fatalf("SetWriteFence: %v %v", err, resp)
	}
	if got := p.psql("SELECT write_fence || ':' || write_fence_reason FROM pgshard.shard_map_generation"); got != "true:test" {
		t.Fatalf("fence row %s", got)
	}
	if resp, _ := p.grpc.SetWriteFence(ctx, &pgshardv1.SetWriteFenceRequest{Epoch: epoch + 1, Active: false}); resp.GetError() == nil || resp.GetError().GetSqlstate() != "55000" {
		t.Fatalf("stale epoch must be refused: %v", resp)
	}
	if resp, err := p.grpc.SetWriteFence(ctx, &pgshardv1.SetWriteFenceRequest{Epoch: epoch, Active: false}); err != nil || resp.GetError() != nil {
		t.Fatalf("SetWriteFence release: %v %v", err, resp)
	}
	if got := p.psql("SELECT write_fence || ':' || write_fence_reason || ':' || coalesce(write_fenced_at::text, '-') FROM pgshard.shard_map_generation"); got != "false::-" {
		t.Fatalf("fence row after release %s", got)
	}

	log := []*pgshardv1.TransactionDecision{
		{Gid: "pgshard-x", State: "commit", Participants: []int32{0}, ParticipantXids: []string{xidX}},
		{Gid: "pgshard-z", State: "abort", Participants: []int32{0}},
		{Gid: "pgshard-w", State: "commit", Participants: []int32{0}, ParticipantXids: []string{xidW}},
		{Gid: "pgshard-elsewhere", State: "commit", Participants: []int32{1}, ParticipantXids: []string{"5"}},
	}
	reconcile := func(n *node) *pgshardv1.ReconcilePreparedTransactionsResponse {
		t.Helper()
		resp, err := n.grpc.ReconcilePreparedTransactions(ctx, &pgshardv1.ReconcilePreparedTransactionsRequest{Epoch: n.status().GetEpoch(), Decisions: log, ShardId: 0})
		if err != nil || resp.GetError() != nil {
			t.Fatalf("%s reconcile: %v %v\n%s", n.name, err, resp.GetError(), n.logs())
		}
		return resp
	}
	restore := func(member, point string) *node {
		t.Helper()
		return h.restoreFrom(member, map[string]any{"stanza": "it-s0-pg18", "type": "name", "target": point, "backupId": full.GetBackupRef()})
	}

	// Before PREPARE: nothing is prepared and the commit-decided x never
	// happened here; w never committed either. Both ids lie in this
	// restore's future, so their status is unavailable rather than aborted.
	b0 := restore("b0", "pgshard-b0")
	if got := b0.psql("SELECT count(*) FROM pg_prepared_xacts"); got != "0" {
		t.Fatalf("b0 prepared: %s", got)
	}
	r0 := reconcile(b0)
	if r0.GetCommitted() != 0 || r0.GetRolledBack() != 0 || len(r0.GetContradictions()) != 0 || !slices.Equal(r0.GetUnverifiable(), []string{"pgshard-x", "pgshard-w"}) {
		t.Fatalf("b0 outcome %v", r0)
	}
	if got := b0.psql("SELECT count(*) FROM t"); got != "0" {
		t.Fatalf("b0 rows %s", got)
	}

	// Between PREPARE and the decision: x is committed, z rolled back by
	// its decision, y rolled back as undecided; w is still a contradiction.
	b1 := restore("b1", "pgshard-b1")
	if got := b1.psql("SELECT string_agg(gid, ',' ORDER BY gid) FROM pg_prepared_xacts"); got != "pgshard-x,pgshard-y,pgshard-z" {
		t.Fatalf("b1 prepared: %s", got)
	}
	r1 := reconcile(b1)
	if r1.GetCommitted() != 1 || r1.GetRolledBack() != 2 || !slices.Equal(r1.GetContradictions(), []string{"pgshard-w"}) {
		t.Fatalf("b1 outcome %v", r1)
	}
	if got := b1.psql("SELECT count(*) FROM pg_prepared_xacts"); got != "0" {
		t.Fatalf("b1 prepared after reconcile: %s", got)
	}
	if got := b1.psql("SELECT string_agg(v, ',') FROM t"); got != "x" {
		t.Fatalf("b1 rows %s", got)
	}
	// Reconciling again is a no-op: x now reads as committed.
	if again := reconcile(b1); again.GetCommitted() != 0 || again.GetRolledBack() != 0 || !slices.Equal(again.GetContradictions(), []string{"pgshard-w"}) {
		t.Fatalf("b1 second reconcile %v", again)
	}

	// After the decision: x already committed (its id reads committed), z
	// already rolled back; only w contradicts.
	b2 := restore("b2", "pgshard-b2")
	if got := b2.psql("SELECT count(*) FROM pg_prepared_xacts"); got != "0" {
		t.Fatalf("b2 prepared: %s", got)
	}
	r2 := reconcile(b2)
	if r2.GetCommitted() != 0 || r2.GetRolledBack() != 0 || !slices.Equal(r2.GetContradictions(), []string{"pgshard-w"}) {
		t.Fatalf("b2 outcome %v", r2)
	}
	if got := b2.psql("SELECT string_agg(v, ',') FROM t"); got != "x" {
		t.Fatalf("b2 rows %s", got)
	}
	if resp, _ := b2.grpc.ReconcilePreparedTransactions(ctx, &pgshardv1.ReconcilePreparedTransactionsRequest{Epoch: b2.status().GetEpoch() + 1, Decisions: log}); resp.GetError() == nil || resp.GetError().GetSqlstate() != "55000" {
		t.Fatalf("stale epoch must be refused: %v", resp)
	}
}
