package catalog

import (
	"context"
	"fmt"
	"regexp"
	"time"

	"github.com/jackc/pgx/v5"
)

// GroupName is the name of the operator group (and agent shard) serving a
// shard: shardN for the default set, <set>-shardN otherwise.
func GroupName(set string, id int32) string {
	if set == "default" {
		return fmt.Sprintf("shard%d", id)
	}
	return fmt.Sprintf("%s-shard%d", set, id)
}

var streamNameRE = regexp.MustCompile(`^[a-z][a-z0-9_]{0,31}$`)

// ValidStreamName reports whether name can be embedded in a slot name.
func ValidStreamName(name string) bool { return streamNameRE.MatchString(name) }

// StreamSlotName is the logical slot of a stream on one shard group:
// pgshard_<stream>_<group>, with characters outside [a-z0-9_] folded to '_'.
func StreamSlotName(stream, group string) string {
	b := []byte("pgshard_" + stream + "_")
	for i := 0; i < len(group); i++ {
		c := group[i]
		switch {
		case c >= 'a' && c <= 'z', c >= '0' && c <= '9', c == '_':
			b = append(b, c)
		case c >= 'A' && c <= 'Z':
			b = append(b, c+('a'-'A'))
		default:
			b = append(b, '_')
		}
	}
	return string(b)
}

// Stream is one row of pgshard.streams.
type Stream struct {
	Name      string
	Database  string
	TwoPhase  bool
	State     string
	CreatedAt time.Time
}

// Stream states.
const (
	StreamCreating = "creating"
	StreamActive   = "active"
	StreamLost     = "lost"
)

// StreamStatus is one row of pgshard.stream_status.
type StreamStatus struct {
	Stream             string
	ShardSet           string
	ShardID            int32
	Slot               string
	WALStatus          string
	InvalidationReason string
	ConfirmedFlushLSN  uint64
	RestartLSN         uint64
	RetainedBytes      int64
	Active             bool
	Synced             bool
	Failover           bool
	UpdatedAt          time.Time
}

// CreateStream inserts a stream row; it is an error if the name is taken.
func CreateStream(ctx context.Context, q Execer, s Stream) error {
	if !ValidStreamName(s.Name) {
		return fmt.Errorf("catalog: invalid stream name %q", s.Name)
	}
	state := s.State
	if state == "" {
		state = StreamCreating
	}
	_, err := q.Exec(ctx, `INSERT INTO pgshard.streams (name, database, two_phase, state) VALUES ($1, $2, $3, $4)`,
		s.Name, s.Database, s.TwoPhase, state)
	return err
}

// ListStreams returns every stream by name.
func ListStreams(ctx context.Context, q Querier) ([]Stream, error) {
	rows, err := q.Query(ctx, `SELECT name, database, two_phase, state, created_at FROM pgshard.streams ORDER BY name`)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, pgx.RowToStructByPos[Stream])
}

// SetStreamState updates a stream's state.
func SetStreamState(ctx context.Context, q Execer, name, state string) error {
	_, err := q.Exec(ctx, `UPDATE pgshard.streams SET state = $2 WHERE name = $1`, name, state)
	return err
}

// DeleteStream removes a stream and its status rows.
func DeleteStream(ctx context.Context, q Execer, name string) error {
	_, err := q.Exec(ctx, `DELETE FROM pgshard.streams WHERE name = $1`, name)
	return err
}

// UpsertStreamStatus records the slot state of a stream on one shard; a
// lost slot also marks the stream lost.
func UpsertStreamStatus(ctx context.Context, q Execer, st StreamStatus) error {
	_, err := q.Exec(ctx, `INSERT INTO pgshard.stream_status
		(stream, shard_set, shard_id, slot, wal_status, invalidation_reason, confirmed_flush_lsn, restart_lsn, retained_bytes, active, synced, failover, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, now())
		ON CONFLICT (stream, shard_set, shard_id) DO UPDATE SET slot = EXCLUDED.slot, wal_status = EXCLUDED.wal_status,
		invalidation_reason = EXCLUDED.invalidation_reason, confirmed_flush_lsn = EXCLUDED.confirmed_flush_lsn,
		restart_lsn = EXCLUDED.restart_lsn, retained_bytes = EXCLUDED.retained_bytes, active = EXCLUDED.active,
		synced = EXCLUDED.synced, failover = EXCLUDED.failover, updated_at = now()`,
		st.Stream, st.ShardSet, st.ShardID, st.Slot, st.WALStatus, st.InvalidationReason, int64(st.ConfirmedFlushLSN), int64(st.RestartLSN),
		st.RetainedBytes, st.Active, st.Synced, st.Failover)
	if err != nil {
		return err
	}
	if st.WALStatus == "lost" {
		return SetStreamState(ctx, q, st.Stream, StreamLost)
	}
	return nil
}

// ListStreamStatus returns the per-shard rows of one stream ("" for all).
func ListStreamStatus(ctx context.Context, q Querier, stream string) ([]StreamStatus, error) {
	rows, err := q.Query(ctx, `SELECT stream, shard_set, shard_id, slot, wal_status, invalidation_reason, confirmed_flush_lsn, restart_lsn,
		retained_bytes, active, synced, failover, updated_at
		FROM pgshard.stream_status WHERE ($1 = '' OR stream = $1) ORDER BY stream, shard_set, shard_id`, stream)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, pgx.RowToStructByPos[StreamStatus])
}
