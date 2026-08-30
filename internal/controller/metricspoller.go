package controller

import (
	"context"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/andrew01234567890/pgshard/internal/metrics"
)

// MetricsPoller keeps the controller's catalog-derived gauges current.
type MetricsPoller struct {
	Pool    *pgxpool.Pool
	Metrics *metrics.Controller
	Logger  *slog.Logger
}

// Run refreshes the gauges every interval until ctx ends.
func (p *MetricsPoller) Run(ctx context.Context, every time.Duration) {
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		if err := p.Refresh(ctx); err != nil && ctx.Err() == nil && p.Logger != nil {
			p.Logger.Warn("metrics refresh failed", "err", err)
		}
		select {
		case <-t.C:
		case <-ctx.Done():
			return
		}
	}
}

// Refresh reloads every gauge from the catalog.
func (p *MetricsPoller) Refresh(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	m := p.Metrics

	rows, err := p.Pool.Query(ctx, `SELECT kind, state, count(*) FROM pgshard.workflows GROUP BY kind, state`)
	if err != nil {
		return err
	}
	m.Workflows.Reset()
	for rows.Next() {
		var kind, state string
		var n int64
		if err := rows.Scan(&kind, &state, &n); err != nil {
			rows.Close()
			return err
		}
		m.Workflows.WithLabelValues(kind, state).Set(float64(n))
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return err
	}

	// A configured cutover pause holds a workflow that is still running --
	// only an operator pauses the workflow itself -- so counting the ones
	// in state paused reported a manual pause at any stage and never the
	// automatic gate this is named for. A workflow that reaches a pause is
	// one line here, carrying enough to say which one it is.
	rows, err = p.Pool.Query(ctx, `SELECT kind, coalesce(spec->>'shard_set', ''), id::text, status->'cutover'->>'pause'
		FROM pgshard.workflows WHERE state = $1 AND status->'cutover'->>'pause' IS NOT NULL`, StateRunning)
	if err != nil {
		return err
	}
	m.CutoverPaused.Reset()
	for rows.Next() {
		var kind, set, id, pause string
		if err := rows.Scan(&kind, &set, &id, &pause); err != nil {
			rows.Close()
			return err
		}
		m.CutoverPaused.WithLabelValues(kind, set, id, pause).Set(1)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return err
	}

	rows, err = p.Pool.Query(ctx, `SELECT kind, id::text,
		       CASE WHEN coalesce((status->'progress'->>'tables_total')::float8, 0) > 0
		            THEN least(1, coalesce((status->'progress'->>'tables_ready')::float8, 0)
		                          / (status->'progress'->>'tables_total')::float8)
		            ELSE 0 END
		FROM pgshard.workflows
		WHERE state IN ($1, $2)`, StateRunning, StatePaused)
	if err != nil {
		return err
	}
	m.WorkflowProgress.Reset()
	for rows.Next() {
		var kind, id string
		var progress float64
		if err := rows.Scan(&kind, &id, &progress); err != nil {
			rows.Close()
			return err
		}
		m.WorkflowProgress.WithLabelValues(kind, id).Set(progress)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return err
	}

	// A row survives its decision only while the resolver cannot finish it:
	// finishing deletes it. So a decided row is not history, it is a
	// transaction still holding locks, WAL and a vacuum horizon on every
	// participant -- aged from the decision, since that is when finishing
	// it became the only thing left to do.
	rows, err = p.Pool.Query(ctx, `SELECT state, count(*), extract(epoch FROM now() - min(coalesce(decided_at, created_at)))
		FROM pgshard.xact_decisions GROUP BY state`)
	if err != nil {
		return err
	}
	m.Decided.Reset()
	m.DecidedOldestAge.Reset()
	var inDoubt int64
	var inDoubtOldest float64
	for rows.Next() {
		var state string
		var n int64
		var oldest *float64
		if err := rows.Scan(&state, &n, &oldest); err != nil {
			rows.Close()
			return err
		}
		age := 0.0
		if oldest != nil {
			age = *oldest
		}
		if state == "preparing" {
			inDoubt, inDoubtOldest = n, age
			continue
		}
		m.Decided.WithLabelValues(state).Set(float64(n))
		m.DecidedOldestAge.WithLabelValues(state).Set(age)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return err
	}
	m.InDoubt.Set(float64(inDoubt))
	m.InDoubtOldestAge.Set(inDoubtOldest)

	rows, err = p.Pool.Query(ctx, `SELECT state, count(*) FROM pgshard.migrations GROUP BY state`)
	if err != nil {
		return err
	}
	m.Migrations.Reset()
	for rows.Next() {
		var state string
		var n int64
		if err := rows.Scan(&state, &n); err != nil {
			rows.Close()
			return err
		}
		m.Migrations.WithLabelValues(state).Set(float64(n))
	}
	rows.Close()
	return rows.Err()
}
