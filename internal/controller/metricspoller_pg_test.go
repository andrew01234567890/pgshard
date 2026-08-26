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
