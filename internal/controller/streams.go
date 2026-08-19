package controller

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/andrew01234567890/pgshard/internal/catalog"
)

// StreamMonitor copies the state of every change stream's slot on every
// shard into pgshard.stream_status and marks a stream lost once one of its
// slots was invalidated (wal_status 'lost').
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
	shards, err := (&Resolver{Pool: m.Pool}).listShards(ctx, "")
	if err != nil {
		return 0, err
	}
	var groups map[ShardRef]string
	if groups, err = m.groupNames(ctx); err != nil {
		return 0, err
	}
	written := 0
	var firstErr error
	for _, sh := range shards {
		conn, err := m.Shards.Dial(ctx, sh.Set, sh.ID)
		if err != nil {
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		for _, st := range streams {
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
		coalesce(confirmed_flush_lsn - '0/0'::pg_lsn, 0), coalesce(restart_lsn - '0/0'::pg_lsn, 0), active
		FROM pg_replication_slots WHERE slot_name = $1`, slot)
	if err != nil {
		return st, err
	}
	defer rows.Close()
	if rows.Next() {
		var confirmed, restart int64
		if err := rows.Scan(&st.WALStatus, &st.InvalidationReason, &confirmed, &restart, &st.Active); err != nil {
			return st, err
		}
		st.ConfirmedFlushLSN, st.RestartLSN = uint64(confirmed), uint64(restart)
	}
	return st, rows.Err()
}

// Run sweeps every interval until ctx ends.
func (m *StreamMonitor) Run(ctx context.Context, interval time.Duration) {
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		if _, err := m.Sweep(ctx); err != nil && m.Logger != nil {
			m.Logger.Warn("stream status sweep failed", "err", err)
		}
	}
}
