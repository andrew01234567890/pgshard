package agent

import (
	"context"
	"time"

	"github.com/andrew01234567890/pgshard/internal/metrics"
)

// metricsPollInterval spaces the slot and backup metric refreshes.
const metricsPollInterval = time.Minute

// pollMetrics refreshes the slot wal_status and pgBackRest backup gauges
// until ctx ends. The primary owns backups and slots; a standby only clears
// nothing and keeps its scrape-time gauges.
func pollMetrics(ctx context.Context, inst *Instance, m *metrics.Agent) {
	t := time.NewTicker(metricsPollInterval)
	defer t.Stop()
	for {
		refreshMetrics(ctx, inst, m)
		select {
		case <-t.C:
		case <-ctx.Done():
			return
		}
	}
}

func refreshMetrics(ctx context.Context, inst *Instance, m *metrics.Agent) {
	// Metrics from a member whose role cannot be read would be wrong in a
	// way nothing downstream could tell, so the pass is skipped.
	if primary, err := inst.IsPrimary(); err != nil || !primary {
		return
	}
	sctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if conn, err := inst.Connect(sctx); err == nil {
		rows, err := conn.Query(sctx, `SELECT slot_name, coalesce(wal_status, '') FROM pg_replication_slots`)
		if err == nil {
			m.SlotWALStatus.Reset()
			for rows.Next() {
				var slot, status string
				if rows.Scan(&slot, &status) == nil {
					m.SlotWALStatus.WithLabelValues(slot, status).Set(1)
				}
			}
			rows.Close()
		}
		_ = conn.Close(sctx)
	}
	r, err := inst.backupRunner()
	if err != nil {
		return
	}
	stanza, err := r.Info(sctx)
	if err != nil {
		m.BackupLastResult.Reset()
		m.BackupLastResult.WithLabelValues("error").Set(1)
		return
	}
	m.BackupLastResult.Reset()
	if stanza.StatusCode == 0 && len(stanza.Backups) > 0 {
		last := stanza.Backups[len(stanza.Backups)-1]
		m.BackupLastResult.WithLabelValues("ok").Set(1)
		m.BackupLastAge.Set(time.Since(time.Unix(last.FinishedAt, 0)).Seconds())
		m.BackupLastSize.Set(float64(last.SizeBytes))
	} else {
		m.BackupLastResult.WithLabelValues("none").Set(1)
	}
}
