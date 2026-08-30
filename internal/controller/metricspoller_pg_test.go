package controller

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/andrew01234567890/pgshard/internal/catalog"
	"github.com/andrew01234567890/pgshard/internal/metrics"
)

// TestMetricsPollerReportsWorkflowProgress guards a refresh that failed on
// every tick: it averaged pgshard.table_status.progress, which is jsonb and has
// no avg(), and which nothing ever writes. Progress lives on the workflow.
func TestMetricsPollerReportsWorkflowProgress(t *testing.T) {
	parallelPG(t)
	dsn := startPostgres(t)
	ctx := context.Background()
	conn := connect(t, dsn)
	if err := catalog.Migrate(ctx, conn); err != nil {
		t.Fatal(err)
	}
	mustExec(t, conn, `INSERT INTO pgshard.workflows (id, kind, state, spec, status) VALUES
		('99999999-9999-9999-9999-999999999999', 'reshard', 'running', '{}'::jsonb,
		 '{"progress": {"tables_total": 4, "tables_ready": 1}}'::jsonb),
		('aaaaaaaa-9999-9999-9999-999999999999', 'upgrade', 'paused', '{}'::jsonb,
		 '{"progress": {"tables_total": 0, "tables_ready": 0}}'::jsonb),
		('bbbbbbbb-9999-9999-9999-999999999999', 'reshard', 'completed', '{}'::jsonb,
		 '{"progress": {"tables_total": 2, "tables_ready": 2}}'::jsonb)`)

	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)

	reg := prometheus.NewRegistry()
	p := &MetricsPoller{Pool: pool, Metrics: metrics.NewController(reg)}
	if err := p.Refresh(ctx); err != nil {
		t.Fatalf("refresh: %v", err)
	}

	got := map[string]float64{}
	families, err := reg.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, f := range families {
		if f.GetName() != "pgshard_controller_workflow_progress" {
			continue
		}
		for _, m := range f.GetMetric() {
			var kind, id string
			for _, l := range m.GetLabel() {
				switch l.GetName() {
				case "kind":
					kind = l.GetValue()
				case "id":
					id = l.GetValue()
				}
			}
			got[kind+"/"+id] = m.GetGauge().GetValue()
		}
	}

	if len(got) != 2 {
		t.Fatalf("want one sample per running or paused workflow, got %v", got)
	}
	if v := got["reshard/99999999-9999-9999-9999-999999999999"]; v != 0.25 {
		t.Errorf("progress fraction = %v, want 0.25", v)
	}
	if v, ok := got["upgrade/aaaaaaaa-9999-9999-9999-999999999999"]; !ok || v != 0 {
		t.Errorf("a workflow with no tables yet must report 0, got %v", v)
	}
}

// TestDecidedTransactionsAreCountedSeparately: a decision row is deleted
// when the resolver finishes it, so one that outlives its decision is a
// transaction still holding locks, WAL and a vacuum horizon on every
// participant. The gauges counted only undecided rows, so nothing said so.
func TestDecidedTransactionsAreCountedSeparately(t *testing.T) {
	parallelPG(t)
	dsn := startPostgres(t)
	ctx := context.Background()
	conn := connect(t, dsn)
	if err := catalog.Migrate(ctx, conn); err != nil {
		t.Fatal(err)
	}
	mustExec(t, conn, `INSERT INTO pgshard.xact_decisions (gid, state, participants, created_at, decided_at) VALUES
		('pgshard-a', 'preparing', '{0}', now() - interval '10 minutes', NULL),
		('pgshard-b', 'commit',    '{0}', now() - interval '2 hours',    now() - interval '1 hour'),
		('pgshard-c', 'commit',    '{0}', now() - interval '2 hours',    now() - interval '30 minutes'),
		('pgshard-d', 'abort',     '{0}', now() - interval '2 hours',    now() - interval '15 minutes')`)

	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	reg := prometheus.NewRegistry()
	p := &MetricsPoller{Pool: pool, Metrics: metrics.NewController(reg)}
	if err := p.Refresh(ctx); err != nil {
		t.Fatalf("refresh: %v", err)
	}

	got := map[string]float64{}
	families, err := reg.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, f := range families {
		switch f.GetName() {
		case "pgshard_controller_decided_transactions", "pgshard_controller_decided_oldest_age_seconds",
			"pgshard_controller_in_doubt_transactions", "pgshard_controller_in_doubt_oldest_age_seconds":
		default:
			continue
		}
		for _, m := range f.GetMetric() {
			key := f.GetName()
			for _, l := range m.GetLabel() {
				key += "/" + l.GetValue()
			}
			got[key] = m.GetGauge().GetValue()
		}
	}

	if got["pgshard_controller_decided_transactions/commit"] != 2 || got["pgshard_controller_decided_transactions/abort"] != 1 {
		t.Fatalf("decided rows are not counted by decision: %v", got)
	}
	// Aged from the decision, not from when the transaction started.
	if age := got["pgshard_controller_decided_oldest_age_seconds/commit"]; age < 3500 || age > 3700 {
		t.Fatalf("oldest commit decision aged %v seconds, want about one hour since it was decided", age)
	}
	if age := got["pgshard_controller_decided_oldest_age_seconds/abort"]; age < 800 || age > 1000 {
		t.Fatalf("oldest abort decision aged %v seconds, want about fifteen minutes", age)
	}
	if got["pgshard_controller_in_doubt_transactions"] != 1 {
		t.Fatalf("the undecided count must not include decided rows: %v", got)
	}
	if age := got["pgshard_controller_in_doubt_oldest_age_seconds"]; age < 550 || age > 700 {
		t.Fatalf("oldest undecided aged %v seconds, want about ten minutes", age)
	}
}
