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
	var paused float64
	for rows.Next() {
		var kind, state string
		var n int64
		if err := rows.Scan(&kind, &state, &n); err != nil {
			rows.Close()
			return err
		}
		m.Workflows.WithLabelValues(kind, state).Set(float64(n))
		if state == StatePaused {
			paused += float64(n)
		}
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return err
	}
	m.CutoverPaused.Set(paused)

	rows, err = p.Pool.Query(ctx, `SELECT w.kind, w.id::text, coalesce(avg(ts.progress), 0)
		FROM pgshard.workflows w LEFT JOIN pgshard.table_status ts ON ts.workflow_id = w.id
		WHERE w.state IN ($1, $2) GROUP BY w.kind, w.id`, StateRunning, StatePaused)
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

	var inDoubt int64
	var oldest *float64
	if err := p.Pool.QueryRow(ctx, `SELECT count(*), extract(epoch FROM now() - min(created_at))
		FROM pgshard.xact_decisions WHERE state = 'preparing'`).Scan(&inDoubt, &oldest); err != nil {
		return err
	}
	m.InDoubt.Set(float64(inDoubt))
	if oldest != nil {
		m.InDoubtOldestAge.Set(*oldest)
	} else {
		m.InDoubtOldestAge.Set(0)
	}

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
