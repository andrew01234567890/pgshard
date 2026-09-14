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
	// ShardSet is where this stream's slots were made. The set a stream
	// READS is decided per call and follows the serving topology when it
	// is not named; this is the durable half, and it is what lets the
	// monitor ask the shards a stream actually has slots on instead of
	// every shard in the cluster.
	ShardSet string
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
	_, err := q.Exec(ctx, `INSERT INTO pgshard.streams (name, database, two_phase, state, shard_set) VALUES ($1, $2, $3, $4, $5)`,
		s.Name, s.Database, s.TwoPhase, state, s.ShardSet)
	return err
}

// ListStreams returns every stream by name.
func ListStreams(ctx context.Context, q Querier) ([]Stream, error) {
	rows, err := q.Query(ctx, `SELECT name, database, two_phase, state, created_at, shard_set FROM pgshard.streams ORDER BY name`)
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

// UpsertStreamStatus records the slot state of a stream on one shard. A
// slot that cannot be resumed from -- invalidated, or gone -- also marks
// the stream lost, on the second consecutive sighting: see markStreamLost.
func UpsertStreamStatus(ctx context.Context, q Execer, st StreamStatus) error {
	if Unresumable(st.WALStatus) {
		if err := markStreamLost(ctx, q, st); err != nil {
			return err
		}
	}
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
	return nil
}

// Unresumable reports whether a slot's wal_status leaves its stream nothing
// to resume from.
//
// "lost" is PostgreSQL's: the slot is there and the WAL it needed is gone.
// "missing" is ours, written by the monitor when pg_replication_slots has
// no row -- a slot dropped, or one that did not survive a promotion
// because it was never synchronised to the member that got promoted. That
// is strictly worse than invalidated: an invalidated slot at least records
// why it died, where a vanished one leaves nothing to read a position
// from. Both mean the consumer must re-baseline.
func Unresumable(walStatus string) bool { return walStatus == "lost" || walStatus == "missing" }

// markStreamLost moves a stream to lost. Called BEFORE the upsert that
// overwrites this shard's last report, because that report is the evidence.
//
// TWO consecutive sightings, for "lost" as well as "missing", and the
// stored row is the first. Both can be transient on a standby: a -rw
// Service flip can land a sweep on a member whose synced copy of the slot
// has not caught up ("missing"), and slotsync can invalidate a synced slot
// on the standby -- its own max_slot_wal_keep_size, or primary_slot_name
// reset -- while the slot is perfectly valid on the primary ("lost"), then
// drop and recreate it on the next cycle. So "the slot is there and says
// its WAL is gone" is true of a primary and not of a standby, and a sweep
// cannot tell which member answered it.
//
// Making a stream lost is not reversible -- only Create writes active, and
// CreateStream refuses a duplicate name, so recovery costs the consumer
// its position. One glimpse is not evidence enough for that.
//
// A stream still being created is exempt: its slots do not exist until
// CreateStream has made them. Create clears the rows that phase leaves
// behind, so they cannot serve as the first sighting afterwards.
func markStreamLost(ctx context.Context, q Execer, st StreamStatus) error {
	_, err := q.Exec(ctx, `UPDATE pgshard.streams SET state = $4
		WHERE name = $1 AND state <> $5
		  AND EXISTS (SELECT 1 FROM pgshard.stream_status
		        WHERE stream = $1 AND shard_set = $2 AND shard_id = $3
		          AND wal_status IN ('missing', 'lost'))`,
		st.Stream, st.ShardSet, st.ShardID, StreamLost, StreamCreating)
	return err
}

// ClearUnresumableStatus forgets the unresumable rows a stream accumulated
// before it was fully created.
//
// The sweep runs while Create is still making slots, so it records every
// shard it has not reached yet as "missing". Those rows are suppressed as
// a reason to condemn the stream while it is creating -- but they stay in
// the table, and once the stream goes active they would serve as the first
// of the two sightings the debounce needs. One real sighting would then be
// enough, which is the thing the debounce exists to prevent.
func ClearUnresumableStatus(ctx context.Context, q Execer, stream string) error {
	_, err := q.Exec(ctx, `DELETE FROM pgshard.stream_status
		WHERE stream = $1 AND wal_status IN ('missing', 'lost')`, stream)
	return err
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
