package agent

import (
	"context"
	"time"
)

// StandbySnapshotEvery is how often a primary holding failover slots writes
// a running-xacts record so that standbys can finish synchronising them.
//
// It is deliberately close to the bgwriter's own LOG_SNAPSHOT_INTERVAL_MS:
// on a busy cluster these records already appear at that rate and this
// changes nothing, and on a quiet one it supplies the only ones there are.
const StandbySnapshotEvery = 15 * time.Second

// needsStandbySnapshot reports whether this instance should write one.
//
// Only a primary can: the record is WAL, and a standby writes none. And
// only a primary that holds a FAILOVER logical slot needs to, because that
// is the only thing waiting on it.
//
// The wait is real and cannot be won by waiting longer. A slot synced to a
// standby is created temporary and persists only once the remote slot has
// caught up to the position the standby reserved locally; PostgreSQL's
// update_local_synced_slot declines while the remote catalog_xmin precedes
// the local one. The primary's slot advances that only on a running-xacts
// record, and the bgwriter writes those every fifteen seconds AND ONLY
// WHILE THERE IS WAL ACTIVITY. So on a quiet cluster there may be none at
// all, the standby's synced slots stay temporary for as long as the
// cluster stays quiet, and a promotion then loses every change stream's
// position -- which is the whole of what failover slots are for.
//
// PostgreSQL's own failover-slot tests force the record for this reason:
// src/test/recovery/t/040_standby_failover_slots_sync.pl, "Create
// xl_running_xacts on the primary to speed up restart_lsn advancement".
func needsStandbySnapshot(inRecovery bool, failoverSlots int) bool {
	return !inRecovery && failoverSlots > 0
}

// runStandbySnapshots writes a running-xacts record whenever this instance
// is a primary holding failover slots, until ctx ends.
//
// Failures are logged and retried on the next tick rather than escalated:
// the record is an optimisation on a cluster that is otherwise working,
// and an instance that cannot be asked is one that has larger problems
// already being reported elsewhere.
func (s *Server) runStandbySnapshots(ctx context.Context, every time.Duration) {
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		if err := s.logStandbySnapshot(ctx); err != nil && ctx.Err() == nil {
			s.log.Info("could not write a running-xacts record for the standbys' slot sync", "err", err.Error())
		}
	}
}

// logStandbySnapshot writes one record if this instance needs to.
func (s *Server) logStandbySnapshot(ctx context.Context) error {
	return s.withConn(ctx, func(q querier) error {
		var inRecovery bool
		var failover int
		if err := q.QueryRow(ctx, `SELECT pg_is_in_recovery(),
			(SELECT count(*) FROM pg_replication_slots WHERE slot_type = 'logical' AND failover)`).
			Scan(&inRecovery, &failover); err != nil {
			return err
		}
		if !needsStandbySnapshot(inRecovery, failover) {
			return nil
		}
		_, err := q.Exec(ctx, `SELECT pg_log_standby_snapshot()`)
		return err
	})
}
