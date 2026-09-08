package agent

import (
	"context"
	"time"
)

// standbySnapshotEvery is how often a primary holding an active failover
// slot writes a running-xacts record so that standbys can finish
// synchronising it.
//
// It is deliberately close to the bgwriter's own LOG_SNAPSHOT_INTERVAL_MS:
// on a busy cluster these records already appear at that rate and this
// changes nothing, and on a quiet one there may be none between them.
const standbySnapshotEvery = 15 * time.Second

// needsStandbySnapshot reports whether this instance should write one.
//
// Three conditions, and each excludes a case where the record would be WAL
// for nothing.
//
// A STANDBY cannot: the record is WAL and a standby writes none.
//
// A FAILOVER logical slot has to exist, because a slot synced to a standby
// is what waits on this. The copy is created temporary and persists only
// once the remote slot has caught up to the position the standby reserved
// locally; PostgreSQL's update_local_synced_slot declines while the remote
// catalog_xmin precedes the local one.
//
// And the slot has to be ACTIVE. The primary's slot advances its
// catalog_xmin only through LogicalConfirmReceivedLocation -- a walsender
// decoding the record and the consumer confirming past it. A slot nobody
// is reading never advances however many records are written for it, so
// for that slot this would be WAL that changes nothing.
//
// What this does NOT rescue, therefore: a late copy of a slot with no
// consumer attached. Nothing the primary can do reaches that case.
//
// Note the slot's own creation already logs one of these records
// (ReplicationSlotReserveWal, slot.c), so a copy synced while that record
// is current persists without help. The exposure is a copy created LATER
// -- a member added, re-cloned or resynced -- on a cluster that has gone
// quiet since, which is the state PGS-712 observed: the standby's copy
// reserving ahead of the primary slot's stale catalog_xmin.
//
// PostgreSQL's own failover-slot tests force the record for this reason:
// src/test/recovery/t/040_standby_failover_slots_sync.pl, "Create
// xl_running_xacts on the primary to speed up restart_lsn advancement".
func needsStandbySnapshot(inRecovery bool, activeFailoverSlots int) bool {
	return !inRecovery && activeFailoverSlots > 0
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
			// Debug, not Info: PostgreSQL is down for minutes during a
			// rewind or a re-clone, and one line every fifteen seconds
			// through that is noise about a record nothing is waiting for
			// while there is no server to hold slots anyway.
			s.log.Debug("could not write a running-xacts record for the standbys' slot sync", "err", err.Error())
		}
	}
}

// logStandbySnapshot writes one record if this instance needs to.
func (s *Server) logStandbySnapshot(ctx context.Context) error {
	return s.withConn(ctx, func(q querier) error {
		var inRecovery bool
		var active int
		if err := q.QueryRow(ctx, `SELECT pg_is_in_recovery(),
			(SELECT count(*) FROM pg_replication_slots WHERE slot_type = 'logical' AND failover AND active)`).
			Scan(&inRecovery, &active); err != nil {
			return err
		}
		if !needsStandbySnapshot(inRecovery, active) {
			return nil
		}
		_, err := q.Exec(ctx, `SELECT pg_log_standby_snapshot()`)
		return err
	})
}
