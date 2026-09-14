package controller

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/andrew01234567890/pgshard/internal/catalog"
)

// StreamMonitor copies the state of every change stream's slot, on the
// shards of that stream's own set, into pgshard.stream_status -- and marks
// a stream lost once one of those slots has been unresumable, invalidated
// or gone, on two consecutive sweeps.
type StreamMonitor struct {
	Pool   *pgxpool.Pool
	Shards ShardDialer
	Logger *slog.Logger
}

// Sweep runs one pass and returns how many (stream, shard) rows it wrote.
func (m *StreamMonitor) Sweep(ctx context.Context) (int, error) {
	streams, err := catalog.ListStreams(ctx, m.Pool)
	if err != nil {
		return 0, fmt.Errorf("streams: %w", err)
	}
	if len(streams) == 0 {
		return 0, nil
	}
	// Per shard SET, not every shard in the cluster. A stream's slots are
	// made on one set and nowhere else, so sweeping the rest reported a
	// row per stream per foreign shard with no slot behind it -- which is
	// what a reshard's freshly provisioned targets look like, and is
	// indistinguishable in the result from a slot that has gone.
	var groups map[ShardRef]string
	if groups, err = m.groupNames(ctx); err != nil {
		return 0, err
	}
	bySet := map[string][]catalog.Stream{}
	for _, st := range streams {
		bySet[st.ShardSet] = append(bySet[st.ShardSet], st)
	}
	written := 0
	var firstErr error
	for set, inSet := range bySet {
		if set == "" {
			// listShards("") is every shard in the cluster, which is the
			// behaviour this is replacing -- so an unset row must not
			// reach it. The migration backfills and CreateStream always
			// records one, so this is a row written by something that
			// forgot; it is resolved rather than skipped so the stream
			// keeps being reported, and said out loud because nothing
			// else would ever mention it.
			serving, err := catalog.ServingShardSet(ctx, m.Pool)
			if err != nil {
				if firstErr == nil {
					firstErr = err
				}
				continue
			}
			if m.Logger != nil {
				m.Logger.Warn("stream has no shard set recorded; sweeping the serving set",
					"streams", len(inSet), "serving", serving)
			}
			set = serving
		}
		shards, err := (&Resolver{Pool: m.Pool}).listShards(ctx, set)
		if err != nil {
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		if len(shards) == 0 {
			// The set was dropped under the stream -- a cancelled reshard,
			// or a retired set finally removed. Sweeping nothing leaves
			// every stream_status row frozen at whatever it last said,
			// which for a monitor is worse than saying nothing at all:
			// the rows go on looking current. Nothing here can repair it,
			// so it is reported rather than hidden.
			if m.Logger != nil {
				m.Logger.Warn("stream's shard set has no shards; its status will not be updated",
					"shard_set", set, "streams", len(inSet))
			}
			continue
		}
		for _, sh := range shards {
			conn, err := m.Shards.Dial(ctx, sh.Set, sh.ID)
			if err != nil {
				if firstErr == nil {
					firstErr = err
				}
				continue
			}
			for _, st := range inSet {
				slot := catalog.StreamSlotName(st.Name, groups[sh])
				row, err := slotStatus(ctx, conn, slot)
				if err != nil {
					if firstErr == nil {
						firstErr = fmt.Errorf("shard %s/%d slot %s: %w", sh.Set, sh.ID, slot, err)
					}
					continue
				}
				row.Stream, row.ShardSet, row.ShardID = st.Name, sh.Set, sh.ID
				if err := catalog.UpsertStreamStatus(ctx, m.Pool, row); err != nil {
					_ = conn.Close(ctx)
					return written, err
				}
				written++
			}
			_ = conn.Close(ctx)
		}
	}
	return written, firstErr
}

func (m *StreamMonitor) groupNames(ctx context.Context) (map[ShardRef]string, error) {
	rows, err := m.Pool.Query(ctx, `SELECT shard_set, shard_id, group_name FROM pgshard.shard_status`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[ShardRef]string{}
	for rows.Next() {
		var ref ShardRef
		var group string
		if err := rows.Scan(&ref.Set, &ref.ID, &group); err != nil {
			return nil, err
		}
		out[ref] = group
	}
	return out, rows.Err()
}

// slotStatus reads one slot; a missing slot reports wal_status "missing".
func slotStatus(ctx context.Context, conn ShardConn, slot string) (catalog.StreamStatus, error) {
	st := catalog.StreamStatus{Slot: slot, WALStatus: "missing"}
	rows, err := conn.Query(ctx, `SELECT coalesce(wal_status, ''), coalesce(invalidation_reason, ''),
		coalesce(confirmed_flush_lsn - '0/0'::pg_lsn, 0), coalesce(restart_lsn - '0/0'::pg_lsn, 0),
		greatest(coalesce(CASE WHEN pg_is_in_recovery() THEN pg_last_wal_replay_lsn() ELSE pg_current_wal_lsn() END - restart_lsn, 0), 0),
		active, synced, failover
		FROM pg_replication_slots WHERE slot_name = $1`, slot)
	if err != nil {
		return st, err
	}
	defer rows.Close()
	if rows.Next() {
		var confirmed, restart int64
		if err := rows.Scan(&st.WALStatus, &st.InvalidationReason, &confirmed, &restart, &st.RetainedBytes, &st.Active, &st.Synced, &st.Failover); err != nil {
			return st, err
		}
		st.ConfirmedFlushLSN, st.RestartLSN = uint64(confirmed), uint64(restart)
	}
	return st, rows.Err()
}

// Run sweeps stream status on every tick while this replica is the leader.
func (m *StreamMonitor) Run(ctx context.Context, interval time.Duration, leader func() bool) {
	runLoopStoppable(ctx, interval, leader, func() *slog.Logger { return m.Logger }, "stream status sweep", func(ctx context.Context) {
		if _, err := m.Sweep(ctx); err != nil && m.Logger != nil {
			m.Logger.Warn("stream status sweep failed", "err", err)
		}
	})
}
